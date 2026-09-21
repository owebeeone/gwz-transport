//! Bounded request/session routing over application messages, without a carrier.
//! The host supplies monotonic time, delivery, and endpoint work. This module
//! owns no socket, credential, Git repository, executor or physical pool lease.
use crate::{
    binding::{self, Binding, EndpointConfig},
    codec,
    protocol::*,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

mod asynchronous;
mod routing;
pub use asynchronous::{Owner, Port};
/// Existing host request id and shared payload; not a new serialized wrapper.
pub type Attachment = (String, Envelope);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidRequest,
    Capacity,
    WouldBlock,
    Protocol,
    Rejected,
    Closed,
    WrongState,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Phase {
    Unbound,
    Binding,
    Rejecting,
    Ready,
    Closed,
}
#[derive(Clone, Debug)]
pub struct Config {
    pub limits: Limits,
    pub role: EndpointRole,
    pub max_requests: usize,
    pub max_streams: usize,
    pub bootstrap_timeout_ms: u64,
    pub cleanup_timeout_ms: u64,
    pub max_check_ms: u64,
}
impl Default for Config {
    fn default() -> Self {
        Self {
            limits: binding::default_limits(),
            role: EndpointRole::Driver,
            max_requests: 256,
            max_streams: 64,
            bootstrap_timeout_ms: 5000,
            cleanup_timeout_ms: 5000,
            max_check_ms: 120_000,
        }
    }
}
struct Request {
    operation: Option<String>,
    sealed: bool,
    cleanup_deadline: Option<u64>,
}
#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Check,
    Opening,
    Stream,
}
struct Route {
    request: String,
    kind: Kind,
    deadline: Option<u64>,
    cancel: Option<ErrorCode>,
}
struct Queued {
    attachment: Attachment,
    charge: usize,
    control: bool,
}
#[derive(Default)]
struct Queue {
    items: VecDeque<Queued>,
    bytes: usize,
    data_bytes: usize,
    data_frames: usize,
}
impl Queue {
    fn can_push(&self, charge: usize, control: bool, limits: &Limits) -> bool {
        self.bytes + charge <= limits.queued_bytes as usize
            && self.items.len() < limits.queued_frames as usize
            && (control
                || (self.data_bytes + charge
                    <= (limits.queued_bytes - limits.control_reserve_bytes) as usize
                    && self.data_frames
                        < (limits.queued_frames - limits.control_reserve_frames) as usize))
    }
    fn push(&mut self, item: &Attachment, charge: usize, control: bool) {
        // Clone only after admission and reservation, compacting caller capacity.
        self.items.push_back(Queued {
            attachment: item.clone(),
            charge,
            control,
        });
        self.bytes += charge;
        if !control {
            self.data_bytes += charge;
            self.data_frames += 1;
        }
    }
    fn pop(&mut self) -> Option<Attachment> {
        self.items.pop_front().map(|entry| {
            self.bytes -= entry.charge;
            if !entry.control {
                self.data_bytes -= entry.charge;
                self.data_frames -= 1;
            }
            entry.attachment
        })
    }
    fn has_terminal(&self, request: &str) -> bool {
        self.items
            .iter()
            .any(|q| q.attachment.0 == request && terminal(q.attachment.1.kind))
    }
    fn discard(&mut self, request: &str) {
        self.items.retain(|entry| {
            entry.attachment.0 != request
                || terminal(entry.attachment.1.kind)
                || matches!(
                    entry.attachment.1.kind,
                    MessageKind::Open
                        | MessageKind::CheckIdentity
                        | MessageKind::Bind
                        | MessageKind::Bound
                        | MessageKind::BindRejected
                )
        });
        self.bytes = self.items.iter().map(|e| e.charge).sum();
        self.data_bytes = self
            .items
            .iter()
            .filter(|e| !e.control)
            .map(|e| e.charge)
            .sum();
        self.data_frames = self.items.iter().filter(|e| !e.control).count();
    }
}
/// Deterministic mux. Queue capacities apply independently in each direction;
/// each includes full typed/decode allocation charges and a control reserve.
pub struct Mux {
    config: Config,
    session: String,
    endpoint: Option<EndpointConfig>,
    phase: Phase,
    binding: Option<Binding>,
    offer: Option<Attachment>,
    acknowledgement: Option<Attachment>,
    rejection: Option<Failure>,
    bootstrap_deadline: Option<u64>,
    requests: BTreeMap<String, Request>,
    used_requests: BTreeSet<String>,
    routes: BTreeMap<i64, Route>,
    highest_id: i64,
    outbound: Queue,
    actions: Queue,
    now: u64,
}
impl Mux {
    pub fn initiator(session: &str, config: Config) -> Result<Self, Error> {
        Self::new(session, None, config)
    }
    pub fn endpoint(
        session: &str,
        endpoint: EndpointConfig,
        config: Config,
    ) -> Result<Self, Error> {
        Self::new(session, Some(endpoint), config)
    }
    fn new(session: &str, endpoint: Option<EndpointConfig>, config: Config) -> Result<Self, Error> {
        if !identifier(session)
            || config.max_requests == 0
            || config.max_requests > 4096
            || config.max_streams == 0
            || config.max_streams > 1024
            || config.bootstrap_timeout_ms == 0
            || config.cleanup_timeout_ms == 0
            || config.max_check_ms == 0
            || config.max_check_ms > 120_000
            || binding::usable(&config.limits).is_err()
            || codec::validate_limits(&config.limits).is_err()
            || endpoint.as_ref().is_some_and(|e| {
                e.role != config.role || !binding::no_greater(&e.limits, &config.limits)
            })
        {
            return Err(Error::InvalidRequest);
        }
        Ok(Self {
            config,
            session: session.into(),
            endpoint,
            phase: Phase::Unbound,
            binding: None,
            offer: None,
            acknowledgement: None,
            rejection: None,
            bootstrap_deadline: None,
            requests: BTreeMap::new(),
            used_requests: BTreeSet::new(),
            routes: BTreeMap::new(),
            highest_id: 0,
            outbound: Queue::default(),
            actions: Queue::default(),
            now: 0,
        })
    }
    pub fn phase(&self) -> Phase {
        self.phase
    }
    /// Negotiated settings; the mux still checks live authority on every call.
    pub fn binding(&self) -> Option<&Binding> {
        self.binding.as_ref()
    }
    /// Retained typed bootstrap outcome, including after local port retirement.
    pub fn bootstrap_failure(&self) -> Option<&Failure> {
        self.rejection.as_ref()
    }
    pub fn active_streams(&self) -> usize {
        self.routes.len()
    }
    pub fn active_requests(&self) -> usize {
        self.requests.len()
    }
    pub fn queued_bytes(&self) -> (usize, usize) {
        (self.outbound.bytes, self.actions.bytes)
    }
    pub fn register(&mut self, request: &str, operation: Option<String>) -> Result<(), Error> {
        self.live()?;
        if self.phase == Phase::Rejecting {
            return Err(Error::Rejected);
        }
        if !identifier(request)
            || self.used_requests.contains(request)
            || operation.as_ref().is_some_and(|o| !identifier(o))
            || (self.endpoint.is_none() && operation.is_none())
        {
            return Err(Error::InvalidRequest);
        }
        // Retain request tombstones: an old delayed Open cannot claim a new
        // registration using the same arbitrary host request id.
        if self.used_requests.len() == self.config.max_requests {
            return Err(Error::Capacity);
        }
        self.used_requests.insert(request.into());
        self.requests.insert(
            request.into(),
            Request {
                operation,
                sealed: false,
                cleanup_deadline: None,
            },
        );
        Ok(())
    }
    pub fn begin(&mut self, request: &str) -> Result<(), Error> {
        self.live_request(request)?;
        if self.endpoint.is_some() {
            return Err(Error::WrongState);
        }
        match self.phase {
            Phase::Unbound => {
                let mut offer = binding::offer(&self.session, self.config.role);
                let body = offer.bind.as_mut().expect("offer body");
                body.versions = vec![2];
                body.schemes = vec![Scheme::Ssh];
                body.policies = vec![AuthPolicy::SshAmbient, AuthPolicy::SshExplicit];
                body.receive_limits = self.config.limits.clone();
                let item = (request.into(), offer);
                self.enqueue(&item, true)?;
                self.offer = Some(item);
                self.phase = Phase::Binding;
                self.bootstrap_deadline =
                    Some(self.now.saturating_add(self.config.bootstrap_timeout_ms));
                Ok(())
            }
            Phase::Binding | Phase::Ready => Ok(()),
            Phase::Rejecting => Err(Error::Rejected),
            Phase::Closed => Err(Error::Closed),
        }
    }
    pub fn message(&self, id: i64, kind: MessageKind) -> Result<Envelope, Error> {
        self.live()?;
        if self.phase != Phase::Ready {
            return Err(Error::WrongState);
        }
        Ok(Envelope {
            version: 2,
            session_id: self.session.clone(),
            stream_id: id,
            kind,
            ..Default::default()
        })
    }
    pub fn check_identity(
        &mut self,
        request: &str,
        identity: Identity,
        timeout_ms: u64,
    ) -> Result<i64, Error> {
        self.live_request(request)?;
        if timeout_ms == 0 || timeout_ms > self.config.max_check_ms {
            return Err(Error::InvalidRequest);
        }
        let binding = self.binding.as_ref().ok_or(Error::WrongState)?;
        let body = CheckIdentity {
            endpoint_id: binding.endpoint_id().into(),
            operation_id: self.requests[request]
                .operation
                .clone()
                .ok_or(Error::WrongState)?,
            identity,
            timeout_ms: timeout_ms as i64,
        };
        self.start(request, Kind::Check, Some(timeout_ms), |m| {
            m.kind = MessageKind::CheckIdentity;
            m.check_identity = Some(body);
        })
    }
    pub fn open(&mut self, request: &str, open: Open) -> Result<i64, Error> {
        self.live_request(request)?;
        if self.requests[request].operation.as_deref() != Some(&open.operation_id) {
            return Err(Error::InvalidRequest);
        }
        self.start(request, Kind::Opening, None, |m| {
            m.open = Some(open);
        })
    }
    fn start(
        &mut self,
        request: &str,
        kind: Kind,
        timeout: Option<u64>,
        fill: impl FnOnce(&mut Envelope),
    ) -> Result<i64, Error> {
        if self.endpoint.is_some() || self.phase != Phase::Ready {
            return Err(Error::WrongState);
        }
        if self.routes.len() >= self.config.max_streams {
            return Err(Error::Capacity);
        }
        let id = self.highest_id.checked_add(1).ok_or(Error::Capacity)?;
        let mut message = self.message(id, MessageKind::Open)?;
        fill(&mut message);
        self.validate_open(&message)?;
        self.enqueue(&(request.into(), message), true)?;
        self.highest_id = id;
        self.routes.insert(
            id,
            Route {
                request: request.into(),
                kind,
                deadline: timeout.map(|t| self.now.saturating_add(t)),
                cancel: None,
            },
        );
        Ok(id)
    }
    pub fn send(&mut self, request: &str, message: &Envelope) -> Result<(), Error> {
        self.live()?;
        let route = self
            .routes
            .get(&message.stream_id)
            .ok_or(Error::InvalidRequest)?;
        if route.request != request || message.session_id != self.session || message.version != 2 {
            return Err(Error::InvalidRequest);
        }
        self.validate_transition(route.kind, message.kind, true)?;
        self.validate_opened(message)?;
        codec::admit_limited(
            message,
            self.binding.as_ref().ok_or(Error::WrongState)?.limits(),
        )
        .map_err(|_| Error::Protocol)?;
        self.enqueue(&(request.into(), message.clone()), true)?;
        self.transition(message);
        Ok(())
    }
    pub fn next_action(&mut self) -> Option<Attachment> {
        self.actions.pop()
    }
    pub fn next_message(&mut self) -> Option<Attachment> {
        if self.phase == Phase::Rejecting {
            let reply = self.outbound.pop();
            self.disconnect();
            return reply;
        }
        // Cancellation uses its reserved route slot and must not wait for an
        // unrelated producer to empty the bulk queue. Preserve creation order
        // for this stream, and never overtake bootstrap.
        if self
            .outbound
            .items
            .front()
            .is_some_and(|q| q.attachment.1.stream_id == 0)
        {
            return self.outbound.pop();
        }
        let pending = self.routes.iter().find(|(id, r)| {
            r.cancel.is_some()
                && !self.outbound.items.iter().any(|q| {
                    q.attachment.1.stream_id == **id
                        && matches!(
                            q.attachment.1.kind,
                            MessageKind::Open | MessageKind::CheckIdentity
                        )
                })
        });
        let Some((&id, route)) = pending else {
            return self.outbound.pop();
        };
        let req = route.request.clone();
        let reason = route.cancel?;
        let kind = route.kind;
        let mut message = self.message(id, MessageKind::Cancel).ok()?;
        if self.endpoint.is_none() {
            message.cancel = Some(Cancel { reason });
            self.routes.get_mut(&id)?.cancel = None;
        } else {
            let failure = Failure {
                code: reason,
                effect: if kind == Kind::Check {
                    Effect::None
                } else {
                    Effect::Possible
                },
                facts: None,
            };
            match kind {
                Kind::Check => {
                    message.kind = MessageKind::IdentityCheckFailed;
                    message.identity_check_failed = Some(failure);
                }
                Kind::Opening => {
                    message.kind = MessageKind::OpenFailed;
                    message.open_failed = Some(failure);
                }
                Kind::Stream => {
                    message.kind = MessageKind::Failed;
                    message.failed = Some(failure);
                }
            }
            self.routes.remove(&id);
        }
        Some((req, message))
    }
    pub fn cancel(&mut self, request: &str) -> Result<(), Error> {
        self.live()?;
        if self.phase == Phase::Binding && self.offer.as_ref().is_some_and(|v| v.0 == request) {
            self.disconnect();
            return Ok(());
        }
        let row = self
            .requests
            .get_mut(request)
            .ok_or(Error::InvalidRequest)?;
        if row.sealed {
            return Ok(());
        }
        row.sealed = true;
        row.cleanup_deadline = Some(self.now.saturating_add(self.config.cleanup_timeout_ms));
        self.outbound.discard(request);
        if self.endpoint.is_some() {
            let ids: Vec<_> = self
                .routes
                .iter()
                .filter(|(_, r)| r.request == request)
                .map(|(id, _)| *id)
                .collect();
            self.actions
                .items
                .retain(|q| q.attachment.0 != request || terminal(q.attachment.1.kind));
            self.actions.bytes = self.actions.items.iter().map(|q| q.charge).sum();
            self.actions.data_bytes = self
                .actions
                .items
                .iter()
                .filter(|q| !q.control)
                .map(|q| q.charge)
                .sum();
            self.actions.data_frames = self.actions.items.iter().filter(|q| !q.control).count();
            for id in ids {
                let mut cancel = self.message(id, MessageKind::Cancel)?;
                cancel.cancel = Some(Cancel {
                    reason: ErrorCode::Cancelled,
                });
                if self.enqueue(&(request.into(), cancel), false).is_err() {
                    self.disconnect();
                    return Err(Error::Closed);
                }
            }
        }
        for route in self.routes.values_mut().filter(|r| r.request == request) {
            route.cancel = Some(ErrorCode::Cancelled);
            route.deadline = Some(self.now.saturating_add(self.config.cleanup_timeout_ms));
        }
        Ok(())
    }
    pub fn finish(&mut self, request: &str) -> Result<(), Error> {
        self.cancel(request)?;
        if self.routes.values().any(|r| r.request == request)
            || self.outbound.has_terminal(request)
            || self.actions.has_terminal(request)
        {
            return Err(Error::WouldBlock);
        }
        self.requests.remove(request);
        Ok(())
    }
    pub fn advance(&mut self, now: u64) {
        self.now = self.now.max(now);
        if self.requests.iter().any(|(id, request)| {
            request.cleanup_deadline.is_some_and(|t| t <= self.now)
                && (self.outbound.has_terminal(id) || self.actions.has_terminal(id))
        }) {
            self.disconnect();
            return;
        }
        if self.bootstrap_deadline.is_some_and(|t| {
            matches!(self.phase, Phase::Binding | Phase::Rejecting) && t <= self.now
        }) {
            self.disconnect();
            return;
        }
        let expired: Vec<_> = self
            .routes
            .iter()
            .filter(|(_, r)| r.deadline.is_some_and(|t| t <= self.now))
            .map(|(id, _)| *id)
            .collect();
        for id in expired {
            let route = &self.routes[&id];
            let req = route.request.clone();
            let code = if self.requests.get(&req).is_some_and(|r| r.sealed) {
                ErrorCode::Cancelled
            } else {
                ErrorCode::Timeout
            };
            let mut terminal = match self.message(id, MessageKind::Failed) {
                Ok(m) => m,
                Err(_) => {
                    return;
                }
            };
            let failure = Failure {
                code,
                effect: if route.kind == Kind::Check {
                    Effect::None
                } else {
                    Effect::Possible
                },
                facts: None,
            };
            if route.kind == Kind::Check {
                terminal.kind = MessageKind::IdentityCheckFailed;
                terminal.identity_check_failed = Some(failure);
            } else if route.kind == Kind::Opening {
                terminal.kind = MessageKind::OpenFailed;
                terminal.open_failed = Some(failure);
            } else {
                terminal.failed = Some(failure);
            }
            let mut cancel = self
                .message(id, MessageKind::Cancel)
                .expect("ready session");
            cancel.cancel = Some(Cancel { reason: code });
            // Endpoint reports the timeout to its peer and wakes its worker.
            // Initiator reports it locally and cancels the endpoint operation.
            let endpoint = self.endpoint.is_some();
            if self.enqueue(&(req.clone(), terminal), endpoint).is_err()
                || self.enqueue(&(req, cancel), !endpoint).is_err()
            {
                self.disconnect();
                return;
            }
            self.routes.remove(&id);
        }
    }

    pub fn disconnect(&mut self) {
        self.phase = Phase::Closed;
        self.binding = None;
        self.offer = None;
        self.acknowledgement = None;
        self.requests.clear();
        self.routes.clear();
        self.outbound = Queue::default();
        self.actions = Queue::default();
    }
    fn live(&self) -> Result<(), Error> {
        if self.phase == Phase::Closed {
            Err(Error::Closed)
        } else {
            Ok(())
        }
    }
    fn live_request(&self, request: &str) -> Result<(), Error> {
        self.live()?;
        if self.requests.get(request).is_none_or(|r| r.sealed) {
            Err(Error::InvalidRequest)
        } else {
            Ok(())
        }
    }
    fn enqueue(&mut self, item: &Attachment, outgoing: bool) -> Result<(), Error> {
        if !identifier(&item.0) {
            return Err(Error::InvalidRequest);
        }
        let limits = self
            .binding
            .as_ref()
            .map_or(&self.config.limits, |b| b.limits());
        let charge = codec::allocation_charge(&item.1, limits).map_err(|_| Error::Protocol)?
            + item.0.len()
            + 128;
        let control = !matches!(
            item.1.kind,
            MessageKind::Data | MessageKind::Open | MessageKind::CheckIdentity
        );
        let queue = if outgoing {
            &mut self.outbound
        } else {
            &mut self.actions
        };
        if !queue.can_push(charge, control, limits) {
            return Err(Error::WouldBlock);
        }
        queue.push(item, charge, control);
        Ok(())
    }
}
fn identifier(value: &str) -> bool {
    !value.is_empty() && value.len() <= 128 && !value.chars().any(char::is_control)
}

fn terminal(kind: MessageKind) -> bool {
    matches!(
        kind,
        MessageKind::BindRejected
            | MessageKind::OpenFailed
            | MessageKind::IdentityChecked
            | MessageKind::IdentityCheckFailed
            | MessageKind::Failed
            | MessageKind::Closed
    )
}
