//! The client's receiver (GwzSplitCommandChannel section 3.4 and 5): it resolves each `bind`'s `request_id` to the
//! call this client sent, builds the `PreparedOperation` itself from that call's captured [`Invocation`], runs its own
//! owner's `admit -> acquire -> register`, and answers `bound` or `bind_rejected`. On every `open` and
//! `check_identity` it runs section 5's admission before the owner sees anything. The core supplies no profile,
//! authentication context, policy or capacity: the one place it names anything (an `Open`) is checked against the
//! client's own sets.
use super::{
    admission::{AllowedSet, ServiceClass},
    lane::ClientSession,
    served::Served,
};
use crate::{HostHandle, PreparedOperation, Sleep};
use gwz_transport::{
    mux::Attachment,
    protocol::{ErrorCode, Failure, MessageKind},
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
    time::Duration,
};

/// A key the invocation named (`--identity`, a `--remote-identity` value): the path the core must echo, and the
/// opaque proof the client's endpoint resolves it by. The proof never crosses the carrier.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedKey {
    pub key_path: String,
    pub proof: String,
}

/// What the client captured from the call it sent (section 3.4): everything its owner needs, none of it from the core.
#[derive(Clone, Debug)]
pub struct Invocation {
    /// Route, profile, authentication context and policy: built by the client from its own invocation and capture.
    pub prepared: PreparedOperation,
    /// The request type's class (section 4.5), never the core's.
    pub class: ServiceClass,
    /// The fetch allowance, and the separate push allowance (section 5.3).
    pub fetch: AllowedSet,
    pub push: AllowedSet,
    /// The keys the invocation named, and the `endpoint_path_base` it sent. An `ambient` identity is admitted only
    /// when `named_keys` is empty.
    pub named_keys: Vec<NamedKey>,
    pub path_base: Option<String>,
    /// The SSH username when the matching allowance names none: the client's SSH configuration for the host, else
    /// `git` (section 5.4).
    pub ssh_user: Option<String>,
}
impl Invocation {
    /// An invocation that allows nothing: every `open` is refused until the sets say otherwise.
    pub fn new(prepared: PreparedOperation, class: ServiceClass) -> Self {
        Self {
            prepared,
            class,
            fetch: AllowedSet::default(),
            push: AllowedSet::default(),
            named_keys: Vec::new(),
            path_base: None,
            ssh_user: None,
        }
    }
}

/// The calls this client has sent on the carrier. `take` yields a call's invocation once: a second `bind` for the
/// same `request_id`, or one for a call that finished or never existed, finds nothing.
pub trait Invocations: Send + Sync + 'static {
    fn take(&self, request_id: &str) -> Option<Invocation>;
}

/// An admission refusal in the words the command prints (section 5.6), for the call whose core asked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefusalNote {
    pub request: String,
    pub text: String,
}
/// The most refusals kept for the command.
const NOTES: usize = 64;
/// The most request names remembered as already bound (an evicted name is still refused: `take` yields once).
const SEEN: usize = 1024;
/// Messages the outbox holds before bindings stop producing.
pub(super) const OUTBOX_ROOM: usize = 64;

pub(super) struct RState {
    pub(super) served: BTreeMap<String, Served>,
    pub(super) calls: BTreeMap<String, String>,
    seen: VecDeque<String>,
    seen_set: BTreeSet<String>,
    pub(super) outbox: VecDeque<Attachment>,
    /// The latest admission refusals, for the command to print (section 5.6).
    notes: VecDeque<RefusalNote>,
    pub(super) ended: bool,
    pub(super) discarded: usize,
    pub(super) waker: Waker,
}
struct RShared {
    host: HostHandle,
    session: Arc<dyn ClientSession>,
    invocations: Arc<dyn Invocations>,
    state: Mutex<RState>,
}

/// The client's side of the transport lane. `Clone + Send + Sync`.
#[derive(Clone)]
pub struct Receiver {
    shared: Arc<RShared>,
}
/// Serves the bindings the core binds on `session`'s lane against `host`, the client's only `HostOwner`'s handle. The
/// receiver's driver is spawned on the session's executor, so bindings progress with no application call.
pub fn serve_bindings(
    host: &HostHandle,
    session: Arc<dyn ClientSession>,
    invocations: Arc<dyn Invocations>,
) -> Receiver {
    let receiver = Receiver {
        shared: Arc::new(RShared {
            host: host.clone(),
            session,
            invocations,
            state: Mutex::new(RState {
                served: BTreeMap::new(),
                calls: BTreeMap::new(),
                seen: VecDeque::new(),
                seen_set: BTreeSet::new(),
                outbox: VecDeque::new(),
                notes: VecDeque::new(),
                ended: false,
                discarded: 0,
                waker: Waker::noop().clone(),
            }),
        }),
    };
    let _ = receiver
        .shared
        .session
        .progress()
        .spawn(Box::pin(ReceiverDriver {
            shared: Arc::downgrade(&receiver.shared),
            tick: None,
        }));
    receiver
}

impl Receiver {
    /// Moves whatever can move now, with `waker` for the owner's futures.
    pub fn pump_with(&self, waker: &Waker) {
        self.step(Some(waker));
    }
    pub fn pump(&self) {
        self.step(None);
    }
    fn step(&self, waker: Option<&Waker>) {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(waker) = waker {
            state.waker = waker.clone();
        }
        self.pump_locked(&mut state);
    }
    /// The admission refusals since the last call, oldest first, at most the latest sixty-four: what the command
    /// prints for each (section 5.6). The core is told only `invalid_request`, never these.
    pub fn take_refusals(&self) -> Vec<RefusalNote> {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        state.notes.drain(..).collect()
    }
    /// The call finished or was cancelled on the client: its binding ends (its operation is cancelled, its exchanges
    /// aborted, each live stream told `cancelled`) and its name stays bound.
    pub fn end_call(&self, request_id: &str) {
        let mut state = self.shared.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(session) = state.calls.remove(request_id)
            && let Some(served) = state.served.get_mut(&session)
        {
            served.end_call();
        }
        self.pump_locked(&mut state);
    }
    fn pump_locked(&self, state: &mut RState) {
        if state.ended {
            return;
        }
        let session = &self.shared.session;
        let now = session.clock().now().get();
        loop {
            // A core that sends binds and does not read costs this end no more than the outbox's room: it stops taking
            // while the outbox is full, and what it has not taken waits in the lane.
            if state.outbox.len() >= OUTBOX_ROOM {
                break;
            }
            match session.recv() {
                Ok(Some(message)) => self.route_in(state, message),
                Ok(None) => break,
                Err(_) => {
                    state.ended = true;
                    state.served.clear();
                    state.calls.clear();
                    state.outbox.clear();
                    return;
                }
            }
        }
        let waker = state.waker.clone();
        let mut outbox = std::mem::take(&mut state.outbox);
        let mut notes = Vec::new();
        for (key, served) in &mut state.served {
            // A binding the carrier faulted ends alone (contract section 6).
            if !served.dead() && session.faulted(key) {
                served.lane_fault();
            }
        }
        for (session, served) in &mut state.served {
            served.drive(now, &waker, &mut outbox);
            notes.extend(served.notes.drain(..).map(|text| (session.clone(), text)));
        }
        state.outbox = outbox;
        for (session, text) in notes {
            let request = state
                .calls
                .iter()
                .find(|(_, bound)| **bound == session)
                .map(|(request, _)| request.clone())
                .unwrap_or_default();
            if state.notes.len() == NOTES {
                state.notes.pop_front();
            }
            state.notes.push_back(RefusalNote { request, text });
        }
        let dead: Vec<String> = state
            .served
            .iter()
            .filter(|(_, served)| served.dead())
            .map(|(session, _)| session.clone())
            .collect();
        for key in dead {
            state.served.remove(&key);
            session.retire(&key);
        }
        let live: BTreeSet<String> = state.served.keys().cloned().collect();
        state.calls.retain(|_, session| live.contains(session));
        while let Some(message) = state.outbox.pop_front() {
            match session.send(message) {
                Ok(()) => {}
                Err(super::lane::LaneSend::Full(message)) => {
                    state.outbox.push_front(*message);
                    break;
                }
                Err(super::lane::LaneSend::Closed(_)) => {
                    state.ended = true;
                    state.served.clear();
                    state.calls.clear();
                    state.outbox.clear();
                    return;
                }
            }
        }
    }

    fn route_in(&self, state: &mut RState, message: Attachment) {
        let session = message.1.session_id.clone();
        if let Some(served) = state.served.get_mut(&session) {
            served.receive(message);
            return;
        }
        if message.1.kind == MessageKind::Bind && message.1.stream_id == 0 {
            self.start_bind(state, message);
            return;
        }
        // A frame for a binding that is absent or retired: discarded, and nothing made for it.
        state.discarded += 1;
    }

    /// A `bind`: resolve the call, build and admit the operation on the owner. Any refusal is `bind_rejected` with
    /// `Effect::None` and no state (section 3.4).
    fn start_bind(&self, state: &mut RState, message: Attachment) {
        let (request, envelope) = (message.0.clone(), message.1.clone());
        let reject = |state: &mut RState, code: ErrorCode| {
            state
                .outbox
                .push_back(rejection(&request, &envelope.session_id, code));
            // The carrier admitted the bind to ask; a refused one keeps nothing there.
            self.shared.session.retire(&envelope.session_id);
        };
        if state.calls.contains_key(&request) || state.seen_set.contains(&request) {
            reject(state, ErrorCode::InvalidRequest);
            return;
        }
        if state.seen.len() == SEEN
            && let Some(oldest) = state.seen.pop_front()
        {
            state.seen_set.remove(&oldest);
        }
        state.seen.push_back(request.clone());
        state.seen_set.insert(request.clone());
        let Some(invocation) = self.shared.invocations.take(&request) else {
            reject(state, ErrorCode::InvalidRequest);
            return;
        };
        if invocation.class == ServiceClass::NoTransport {
            reject(state, ErrorCode::InvalidRequest);
            return;
        }
        let admission = match self.shared.host.admit(invocation.prepared.clone()) {
            Ok(admission) => admission,
            Err(error) => {
                reject(state, rejection_code(&error));
                return;
            }
        };
        let served = Served::admitting(
            request.clone(),
            invocation,
            Box::pin(admission.acquire()),
            message,
        );
        state.calls.insert(request, envelope.session_id.clone());
        state.served.insert(envelope.session_id, served);
    }
}

fn rejection_code(error: &crate::AdmissionError) -> ErrorCode {
    match error {
        crate::AdmissionError::Closing(_) => ErrorCode::Cancelled,
        _ => ErrorCode::Capacity,
    }
}

/// A `bind_rejected` for `request`'s binding: `Effect::None`, no facts (section 3.4).
pub(super) fn rejection(request: &str, session: &str, code: ErrorCode) -> Attachment {
    (
        request.to_owned(),
        gwz_transport::protocol::Envelope {
            version: 1,
            session_id: session.to_owned(),
            stream_id: 0,
            kind: MessageKind::BindRejected,
            bind_rejected: Some(Failure {
                code,
                effect: gwz_transport::protocol::Effect::None,
                ..Default::default()
            }),
            ..Default::default()
        },
    )
}

/// The receiver's driver: resolves when the carrier has ended or every receiver is gone.
struct ReceiverDriver {
    shared: Weak<RShared>,
    tick: Option<Sleep>,
}
impl Future for ReceiverDriver {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let Some(shared) = self.shared.upgrade() else {
            return Poll::Ready(());
        };
        let receiver = Receiver { shared };
        receiver.pump_with(cx.waker());
        let ended = receiver
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .ended;
        if ended {
            return Poll::Ready(());
        }
        receiver.shared.session.register(cx.waker());
        // An exchange's stream gives no wake when bytes arrive (the owner's `ExchangeIo` is polled), so the driver
        // ticks while any binding lives, and rarely when none does.
        let clock = receiver.shared.session.clock();
        let busy = !receiver
            .shared
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .served
            .is_empty();
        let period = Duration::from_millis(if busy { 5 } else { 100 });
        loop {
            let tick = self
                .tick
                .get_or_insert_with(|| clock.sleep_until(clock.now().plus(period)));
            if tick.as_mut().poll(cx).is_pending() {
                break;
            }
            self.tick = None;
        }
        Poll::Pending
    }
}
impl fmt::Debug for Receiver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Receiver").finish_non_exhaustive()
    }
}

cfg_if::cfg_if! {
    if #[cfg(test)] {
        impl Receiver {
            pub(crate) fn discarded(&self) -> usize {
                self.shared.state.lock().unwrap().discarded
            }
            pub(crate) fn bindings(&self) -> usize {
                self.shared.state.lock().unwrap().served.len()
            }
        }
    }
}
