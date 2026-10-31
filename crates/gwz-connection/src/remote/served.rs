//! One binding as the client serves it: the endpoint half of the transport session (`Mux::endpoint`) on the core's
//! session identifier, and the client's own `Operation` on its owner, whose exchanges carry the bytes.
//!
//! The owner registers its operation under its own host-minted session name, so the core's identifier cannot route into
//! the owner's binding (`HostShared::route` compares names); the receiver keeps the core-facing session itself and uses
//! the owner only for what it owns: admission, the pool and its connections (reported at step 2.2c).
use super::{
    admission::{
        AllowedSet, CanonError, Canonical, ServiceClass, admit_service, class_refusal_text,
        tuple_refusal_text,
    },
    core::stream_config,
    receiver::{Invocation, NamedKey, OUTBOX_ROOM, rejection},
    table::{self, crate_service},
};
use crate::{
    Acquire, AdmissionError, EndpointRoute, Exchange, ExchangeFinish, ExchangeSpec, OpenFailure,
    OpenWait, Operation, Registration,
};
use gwz_transport::{
    mux::{self, Attachment, Mux, Phase},
    pool::{Identity as PoolIdentity, Key},
    protocol::{
        Disposition, Effect, Envelope, ErrorCode, Facts, Failure, GitService, IdentityMode,
        MessageKind, Opened, Scheme,
    },
    stream::{self, Side, StreamMachine},
};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    io::ErrorKind,
    pin::Pin,
    task::{Context, Poll, Waker},
};

struct Rules {
    class: ServiceClass,
    fetch: AllowedSet,
    push: AllowedSet,
    named_keys: Vec<NamedKey>,
    path_base: Option<String>,
    ssh_user: Option<String>,
}

enum Stage {
    Admitting {
        acquire: Pin<Box<Acquire>>,
        bind: Box<Attachment>,
    },
    Bound {
        mux: Box<Mux>,
        op: Option<Operation>,
    },
    Dead,
}

struct Running {
    machine: StreamMachine,
    exchange: Option<Exchange>,
    service: GitService,
    /// Request bytes read from the machine and not yet written to the exchange.
    to_exchange: Vec<u8>,
    /// Response bytes read from the exchange and not yet written to the machine.
    to_machine: Vec<u8>,
    forwarded: u64,
    request_done: bool,
    response_done: bool,
    finishing: Option<ExchangeFinish>,
    disposition: Option<Disposition>,
}
enum Ex {
    Opening { wait: OpenWait, service: GitService },
    Open(Box<Running>),
}

/// Streams the client's endpoint half of a binding holds at once (the mux's default bound), which the carrier holds the
/// binding to as well (`BindBounds::client`).
pub(super) const STREAMS: usize = 64;

/// Why a frame cannot be answered per stream: the binding is faulted (contract section 6).
struct Fault;

pub(super) struct Served {
    request: String,
    session: String,
    rules: Rules,
    route: EndpointRoute,
    stage: Stage,
    exchanges: BTreeMap<i64, Ex>,
    /// Messages built outside the mux (a faulted binding's terminals), flushed with the next drive.
    outq: VecDeque<Attachment>,
    dying: bool,
    /// Admission refusals in the words the command prints (section 5.6), taken by the receiver.
    pub(super) notes: Vec<String>,
}

impl Served {
    pub(super) fn admitting(
        request: String,
        invocation: Invocation,
        acquire: Pin<Box<Acquire>>,
        bind: Attachment,
    ) -> Self {
        Self {
            session: bind.1.session_id.clone(),
            request,
            route: invocation.prepared.route.clone(),
            rules: Rules {
                class: invocation.class,
                fetch: invocation.fetch,
                push: invocation.push,
                named_keys: invocation.named_keys,
                path_base: invocation.path_base,
                ssh_user: invocation.ssh_user,
            },
            stage: Stage::Admitting {
                acquire,
                bind: Box::new(bind),
            },
            exchanges: BTreeMap::new(),
            outq: VecDeque::new(),
            dying: false,
            notes: Vec::new(),
        }
    }
    pub(super) fn dead(&self) -> bool {
        matches!(self.stage, Stage::Dead) && self.outq.is_empty()
    }

    /// One arrival for this binding. A name other than the call's, or anything the mux refuses, faults this binding
    /// only.
    pub(super) fn receive(&mut self, message: Attachment) {
        let stream = message.1.stream_id;
        let Stage::Bound { mux, .. } = &mut self.stage else {
            return;
        };
        if message.0 != self.request || mux.receive(&message).is_err() {
            self.fault(Some(stream));
        }
    }

    /// The binding is over: its live streams and the offending one (if any) end with `code` and `Effect::None`
    /// for what never started, its operation is dropped (which cancels it), and only this binding is affected.
    fn fault(&mut self, offending: Option<i64>) {
        self.terminate(ErrorCode::Protocol, offending);
    }
    /// The carrier faulted the binding: its streams end `protocol` and it goes, alone.
    pub(super) fn lane_fault(&mut self) {
        self.fault(None);
    }
    /// The call ended on the client: every stream of the binding ends `cancelled` and the binding goes.
    pub(super) fn end_call(&mut self) {
        self.terminate(ErrorCode::Cancelled, None);
    }
    fn terminate(&mut self, code: ErrorCode, offending: Option<i64>) {
        // Each running exchange's effect is what `cancel_stream` reports: a push whose bytes already crossed the owner's
        // stream may have changed refs at the remote.
        let mut streams: Vec<(i64, bool, Effect)> = self
            .exchanges
            .iter()
            .map(|(id, ex)| match ex {
                Ex::Opening { .. } => (*id, true, Effect::None),
                Ex::Open(run) => (*id, false, effect_after(run.service, run.forwarded)),
            })
            .collect();
        if let Some(id) = offending
            && id > 0
            && !streams.iter().any(|(known, _, _)| *known == id)
        {
            streams.push((id, true, Effect::None));
        }
        for (id, opening, effect) in streams {
            let kind = if opening {
                MessageKind::OpenFailed
            } else {
                MessageKind::Failed
            };
            let failure = Failure {
                code,
                effect,
                ..Default::default()
            };
            let mut envelope = Envelope {
                version: 2,
                session_id: self.session.clone(),
                stream_id: id,
                kind,
                ..Default::default()
            };
            if opening {
                envelope.open_failed = Some(failure);
            } else {
                envelope.failed = Some(failure);
            }
            self.outq.push_back((self.request.clone(), envelope));
        }
        self.exchanges.clear();
        self.dying = true;
        if let Stage::Bound { mux, .. } = &mut self.stage {
            mux.disconnect();
        }
    }

    pub(super) fn drive(&mut self, now: u64, waker: &Waker, outbox: &mut VecDeque<Attachment>) {
        let mut cx = Context::from_waker(waker);
        if let Stage::Admitting { acquire, .. } = &mut self.stage {
            match acquire.as_mut().poll(&mut cx) {
                Poll::Pending => return,
                Poll::Ready(result) => self.admitted(result, outbox),
            }
        }
        if matches!(self.stage, Stage::Bound { .. }) {
            self.drive_bound(now, &mut cx, outbox);
        }
        outbox.extend(self.outq.drain(..));
        if self.dying {
            self.stage = Stage::Dead;
        }
    }

    fn admitted(
        &mut self,
        result: Result<crate::AdmissionPermit, AdmissionError>,
        outbox: &mut VecDeque<Attachment>,
    ) {
        let Stage::Admitting { bind, .. } = std::mem::replace(&mut self.stage, Stage::Dead) else {
            return;
        };
        let reject = |outbox: &mut VecDeque<Attachment>, code| {
            outbox.push_back(rejection(&bind.0, &bind.1.session_id, code));
        };
        let permit = match result {
            Ok(permit) => permit,
            Err(error) => {
                let code = match error {
                    AdmissionError::Closing(_) => ErrorCode::Cancelled,
                    _ => ErrorCode::Capacity,
                };
                reject(outbox, code);
                return;
            }
        };
        let op = permit.register();
        if !matches!(op.registration(), Registration::Registered(_)) {
            reject(outbox, ErrorCode::Cancelled);
            return;
        }
        let config = mux::Config {
            limits: crate::binding::limits(),
            max_requests: 4,
            max_streams: STREAMS,
            ..Default::default()
        };
        let built = Mux::endpoint(
            &self.session,
            self.route.endpoint_config(&self.session),
            config,
        )
        .and_then(|mut m| {
            m.register(&self.request, None)?;
            m.receive(&bind)?;
            Ok(m)
        });
        match built {
            Ok(mux) => {
                self.stage = Stage::Bound {
                    mux: Box::new(mux),
                    op: Some(op),
                };
            }
            Err(_) => reject(outbox, ErrorCode::InvalidRequest),
        }
    }

    fn drive_bound(&mut self, now: u64, cx: &mut Context<'_>, outbox: &mut VecDeque<Attachment>) {
        let Self {
            request,
            session,
            rules,
            stage,
            exchanges,
            notes,
            ..
        } = self;
        let Stage::Bound { mux, op } = stage else {
            return;
        };
        mux.advance(now);
        let mut fault = None;
        while let Some((_, message)) = mux.next_action() {
            let id = message.stream_id;
            let result = match message.kind {
                MessageKind::Open => {
                    open(mux, op.as_ref(), rules, request, exchanges, notes, message)
                }
                MessageKind::CheckIdentity => {
                    check_identity(mux, rules, request, message);
                    Ok(())
                }
                MessageKind::Cancel => {
                    cancel_stream(mux, request, exchanges, id);
                    Ok(())
                }
                _ => {
                    if let Some(Ex::Open(run)) = exchanges.get_mut(&id) {
                        // A message the machine refuses ends that exchange: its own error reports it.
                        let _ = run.machine.receive(message);
                    }
                    Ok(())
                }
            };
            if let Err(Fault) = result {
                fault = Some(id);
                break;
            }
        }
        if fault.is_none() {
            poll_openings(mux, request, session, exchanges, cx);
            let mut budget = 16;
            let finished: Vec<i64> = exchanges
                .iter_mut()
                .filter_map(|(id, ex)| match ex {
                    Ex::Open(run) => {
                        pump_running(run, mux, request, now, cx, &mut budget).then_some(*id)
                    }
                    Ex::Opening { .. } => None,
                })
                .collect();
            for id in finished {
                exchanges.remove(&id);
            }
        }
        while outbox.len() < OUTBOX_ROOM {
            let Some(message) = mux.next_message() else {
                break;
            };
            outbox.push_back(message);
        }
        let closed = mux.phase() == Phase::Closed;
        if let Some(id) = fault {
            self.fault(Some(id));
        } else if closed {
            self.exchanges.clear();
            self.stage = Stage::Dead;
        }
    }
}

/// What an exchange's end may have left at the remote: a push (`receive-pack`) whose bytes already crossed the owner's
/// stream may have changed refs; anything else changed nothing. The one rule for every way an exchange ends before its
/// orderly close (a cancel, the call's end, a fault of the binding, a failure of the owner's stream).
fn effect_after(service: GitService, forwarded: u64) -> Effect {
    if service == GitService::ReceivePackExchange && forwarded > 0 {
        Effect::Possible
    } else {
        Effect::None
    }
}

fn send(
    mux: &mut Mux,
    request: &str,
    id: i64,
    kind: MessageKind,
    fill: impl FnOnce(&mut Envelope),
) {
    if let Ok(mut envelope) = mux.message(id, kind) {
        fill(&mut envelope);
        let _ = mux.send(request, &envelope);
    }
}
/// The initiator cancelled a stream (GwzSplitCommandChannel 3.3): the exchange is aborted, its connection discarded, and
/// the endpoint answers the terminal that retires the stream's slot at both ends. The transport's stream machine sends
/// none for a peer's `cancel`, and a route lives until a terminal passes it, so without this answer every cancelled
/// exchange held one of the binding's streams until the binding ended (step 2.2c rework; old-lane report item 10).
fn cancel_stream(mux: &mut Mux, request: &str, exchanges: &mut BTreeMap<i64, Ex>, id: i64) {
    match exchanges.remove(&id) {
        Some(Ex::Opening { .. }) => open_failed(mux, request, id, ErrorCode::Cancelled),
        Some(Ex::Open(mut run)) => {
            let effect = effect_after(run.service, run.forwarded);
            if let Some(exchange) = run.exchange.take() {
                exchange.abort();
            }
            send(mux, request, id, MessageKind::Failed, |envelope| {
                envelope.failed = Some(Failure {
                    code: ErrorCode::Cancelled,
                    effect,
                    ..Default::default()
                });
            });
        }
        None => {}
    }
}
fn open_failed(mux: &mut Mux, request: &str, id: i64, code: ErrorCode) {
    send(mux, request, id, MessageKind::OpenFailed, |envelope| {
        envelope.open_failed = Some(Failure {
            code,
            effect: Effect::None,
            ..Default::default()
        });
    });
}

/// Section 5 on an `Open`: shape (a fault), tuple, service class and allowance, identity, username; then the owner's
/// own `open`. An admission refusal answers `open_failed(invalid_request)` with `Effect::None` and no facts, and
/// touches no agent, key file, helper or connection.
fn open(
    mux: &mut Mux,
    op: Option<&Operation>,
    rules: &Rules,
    request: &str,
    exchanges: &mut BTreeMap<i64, Ex>,
    notes: &mut Vec<String>,
    message: Envelope,
) -> Result<(), Fault> {
    let id = message.stream_id;
    let open = message.open.ok_or(Fault)?;
    let scheme = open.destination.scheme;
    table::check_shape(scheme, open.service).map_err(|_| Fault)?;
    let refuse = |mux: &mut Mux| open_failed(mux, request, id, ErrorCode::InvalidRequest);
    let tuple: Result<Canonical, CanonError> = Canonical::from_destination(&open.destination);
    let tuple = match tuple {
        Ok(tuple) => tuple,
        Err(error) => {
            notes.push(tuple_refusal_text(&open.destination, open.service, error));
            refuse(mux);
            return Ok(());
        }
    };
    let allowance =
        match admit_service(rules.class, open.service, &tuple, &rules.fetch, &rules.push) {
            Ok(allowance) => allowance,
            Err(_) => {
                notes.push(class_refusal_text(&tuple, open.service));
                refuse(mux);
                return Ok(());
            }
        };
    let username = match scheme {
        Scheme::Ssh => {
            let choice = allowance
                .user()
                .map(str::to_owned)
                .or_else(|| rules.ssh_user.clone())
                .unwrap_or_else(|| "git".to_owned());
            if open.destination.https_username.is_some()
                || open
                    .destination
                    .ssh_username
                    .as_ref()
                    .is_some_and(|given| *given != choice)
            {
                refuse(mux);
                return Ok(());
            }
            Some(choice)
        }
        Scheme::Https => {
            let choice = allowance.user().map(str::to_owned);
            if open.destination.ssh_username.is_some()
                || open
                    .destination
                    .https_username
                    .as_ref()
                    .is_some_and(|given| Some(given) != choice.as_ref())
            {
                refuse(mux);
                return Ok(());
            }
            choice
        }
    };
    let identity = match (scheme, open.identity.mode) {
        (Scheme::Ssh, IdentityMode::Ambient) if rules.named_keys.is_empty() => {
            Some(PoolIdentity::Ambient)
        }
        (Scheme::Ssh, IdentityMode::ExplicitKey) => {
            open.identity.key_path.as_ref().and_then(|path| {
                let named = rules.named_keys.iter().find(|key| key.key_path == *path)?;
                (open.identity.path_base == rules.path_base)
                    .then(|| PoolIdentity::Explicit(named.proof.clone()))
            })
        }
        (Scheme::Https, IdentityMode::Ambient | IdentityMode::CredentialsDisabled) => {
            Some(PoolIdentity::Https)
        }
        _ => None,
    };
    let key = match scheme {
        Scheme::Ssh => Key::ssh(username.unwrap_or_default(), tuple.host(), tuple.port()),
        Scheme::Https => Key::https(tuple.host(), tuple.port()),
    };
    let service = crate_service(open.service);
    let spec = identity.and_then(|identity| {
        ExchangeSpec::git(key, identity, service, &open.destination.path).ok()
    });
    let (Some(spec), Some(op)) = (spec, op) else {
        refuse(mux);
        return Ok(());
    };
    match op.open(spec) {
        Ok(opening) => {
            exchanges.insert(
                id,
                Ex::Opening {
                    wait: opening.wait(),
                    service: open.service,
                },
            );
        }
        Err(crate::OpenError::BindingLimit) => open_failed(mux, request, id, ErrorCode::Capacity),
        Err(_) => open_failed(mux, request, id, ErrorCode::Cancelled),
    }
    Ok(())
}

/// Section 5.4 on a `check_identity`: a key path the invocation did not name, or a path base other than the one it
/// sent, is `identity_check_failed(invalid_request)`. Performing the check is the SSH endpoint's work (step 2.3); the
/// owner exposes none, so an admitted request answers `unavailable`, `Effect::None`.
fn check_identity(mux: &mut Mux, rules: &Rules, request: &str, message: Envelope) {
    let id = message.stream_id;
    let admitted = message.check_identity.as_ref().is_some_and(|check| {
        check
            .identity
            .key_path
            .as_ref()
            .is_some_and(|path| rules.named_keys.iter().any(|k| k.key_path == *path))
            && check.identity.path_base == rules.path_base
    });
    let code = if admitted {
        ErrorCode::Unavailable
    } else {
        ErrorCode::InvalidRequest
    };
    send(
        mux,
        request,
        id,
        MessageKind::IdentityCheckFailed,
        |envelope| {
            envelope.identity_check_failed = Some(Failure {
                code,
                effect: Effect::None,
                ..Default::default()
            });
        },
    );
}

fn poll_openings(
    mux: &mut Mux,
    request: &str,
    session: &str,
    exchanges: &mut BTreeMap<i64, Ex>,
    cx: &mut Context<'_>,
) {
    let ids: Vec<i64> = exchanges
        .iter()
        .filter(|(_, ex)| matches!(ex, Ex::Opening { .. }))
        .map(|(id, _)| *id)
        .collect();
    for id in ids {
        let Some(Ex::Opening { wait, service }) = exchanges.get_mut(&id) else {
            continue;
        };
        let service = *service;
        let result = match Pin::new(wait).poll(cx) {
            Poll::Pending => continue,
            Poll::Ready(result) => result,
        };
        exchanges.remove(&id);
        match result {
            Ok(exchange) => {
                let Some(binding) = mux.binding() else {
                    continue;
                };
                let limits = binding.limits().clone();
                let (endpoint_id, trust_owner) = (
                    binding.endpoint_id().to_owned(),
                    binding.trust_owner().to_owned(),
                );
                let Ok(machine) =
                    StreamMachine::new(stream_config(session, id, Side::Endpoint, &limits))
                else {
                    exchange.abort();
                    open_failed(mux, request, id, ErrorCode::Protocol);
                    continue;
                };
                let reused = exchange.reused();
                send(mux, request, id, MessageKind::Opened, |envelope| {
                    envelope.opened = Some(Opened {
                        connection_id: format!("c{id}"),
                        reused,
                        endpoint_id,
                        trust_owner,
                        facts: Facts::default(),
                        receive_limits: limits,
                    });
                });
                exchanges.insert(
                    id,
                    Ex::Open(Box::new(Running {
                        machine,
                        exchange: Some(exchange),
                        service,
                        to_exchange: Vec::new(),
                        to_machine: Vec::new(),
                        forwarded: 0,
                        request_done: false,
                        response_done: false,
                        finishing: None,
                        disposition: None,
                    })),
                );
            }
            Err(failure) => {
                let code = match failure {
                    OpenFailure::Pool(_) | OpenFailure::NoFittingConnection => {
                        ErrorCode::Unavailable
                    }
                    OpenFailure::Io(_) => ErrorCode::Io,
                    _ => ErrorCode::Cancelled,
                };
                open_failed(mux, request, id, code);
            }
        }
    }
}

/// Moves one exchange's bytes both ways between its machine and the owner's stream, and its machine's messages into
/// the mux. Returns whether the exchange is over and drained.
fn pump_running(
    run: &mut Running,
    mux: &mut Mux,
    request: &str,
    now: u64,
    cx: &mut Context<'_>,
    budget: &mut usize,
) -> bool {
    run.machine.advance(now);
    let mut buffer = [0u8; 16 * 1024];
    if run.exchange.is_some() {
        // The request: machine to the owner's stream.
        for _ in 0..8 {
            if run.to_exchange.is_empty() && !run.request_done {
                match run.machine.read(&mut buffer) {
                    Ok(0) => run.request_done = true,
                    Ok(count) => run.to_exchange.extend_from_slice(&buffer[..count]),
                    Err(_) => break,
                }
            }
            if run.to_exchange.is_empty() {
                break;
            }
            let Some(exchange) = run.exchange.as_mut() else {
                break;
            };
            match exchange.stream().try_write(&run.to_exchange) {
                Ok(count) => {
                    run.forwarded += count as u64;
                    run.to_exchange.drain(..count);
                }
                Err(crate::StreamError::Io(error)) if error.kind() == ErrorKind::WouldBlock => {
                    break;
                }
                Err(error) => {
                    fail(run, &error);
                    break;
                }
            }
        }
        // The response: the owner's stream to the machine. `Ok(0)` is the end only once the request is complete.
        for _ in 0..8 {
            let Some(exchange) = run.exchange.as_mut() else {
                break;
            };
            if run.to_machine.is_empty() && !run.response_done {
                match exchange.stream().try_read(&mut buffer) {
                    Ok(0) if run.request_done && run.to_exchange.is_empty() => {
                        let _ = run.machine.end_write();
                        run.response_done = true;
                    }
                    Ok(0) => break,
                    Ok(count) => run.to_machine.extend_from_slice(&buffer[..count]),
                    Err(crate::StreamError::Io(error)) if error.kind() == ErrorKind::WouldBlock => {
                        break;
                    }
                    Err(error) => {
                        fail(run, &error);
                        break;
                    }
                }
            }
            if run.to_machine.is_empty() {
                break;
            }
            match run.machine.write(&run.to_machine) {
                Ok(count) => {
                    run.to_machine.drain(..count);
                }
                Err(stream::Error::WouldBlock) => break,
                Err(_) => break,
            }
        }
        if run.request_done
            && run.response_done
            && run.to_exchange.is_empty()
            && run.to_machine.is_empty()
            && run.finishing.is_none()
        {
            run.finishing = run.exchange.take().map(Exchange::finish);
        }
    }
    if let Some(finishing) = run.finishing.as_mut()
        && let Poll::Ready(end) = Pin::new(finishing).poll(cx)
    {
        run.disposition = Some(end.disposition);
        run.finishing = None;
    }
    if let Some(disposition) = run.disposition
        && run
            .machine
            .complete_close(disposition, Facts::default())
            .is_ok()
    {
        run.disposition = None;
    }
    // A machine the core ended (cancel) or that failed: the owner's exchange is aborted.
    if run
        .machine
        .close_result()
        .is_err_and(|error| error != stream::Error::WouldBlock)
    {
        if let Some(exchange) = run.exchange.take() {
            exchange.abort();
        }
        run.finishing = None;
    }
    let mut quiet = false;
    while *budget > 0 {
        let Some(message) = run.machine.next_message() else {
            quiet = true;
            break;
        };
        *budget -= 1;
        let _ = mux.send(request, &message);
    }
    quiet && run.machine.stats().terminal
}

/// The owner's stream failed: the exchange ends with a terminal failure the core receives.
fn fail(run: &mut Running, error: &crate::StreamError) {
    let code = match error {
        crate::StreamError::Cancelled => ErrorCode::Cancelled,
        _ => ErrorCode::Io,
    };
    let effect = effect_after(run.service, run.forwarded);
    let _ = run.machine.fail_terminal(Failure {
        code,
        effect,
        ..Default::default()
    });
    if let Some(exchange) = run.exchange.take() {
        exchange.abort();
    }
}
