//! The core end's state: every remote binding of one session, and the pump that moves their messages between each
//! binding's `Mux` (the transport's own initiator state machine, one per binding) and the lane.
//!
//! One `Mutex<State>` covers the table, the queue of waiting admissions and the outbox; the pump runs under it and is
//! the only code that touches the lane. Every public call pumps first, so a caller sees what has arrived; the
//! handle's `driver` future pumps when the lane wakes it, for progress with no application call.
use super::{
    backend::RemoteConfig,
    lane::{CoreBinding, LaneEnded, LaneSend},
    table::{self, OpenContext},
};
use crate::{
    BindingId, CleanupStatus, ExchangeSpec, GitService, OpenFailure, OperationEnd, Registration,
    TerminalReason, cell::Cell,
};
use gwz_transport::{
    mux::{self, Attachment, Mux, Phase},
    protocol::{Disposition, Effect, ErrorCode, Limits, MessageKind},
    stream::{self, StreamMachine},
};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, OnceLock},
    task::Waker,
};

pub(super) enum Reg {
    Pending,
    Registered,
    Never(TerminalReason),
}

#[derive(PartialEq, Eq, Clone, Copy, Debug)]
pub(super) enum Stage {
    /// Waiting for the binding to be ready before the `Open` can be built.
    Queued,
    /// `Open` handed to the mux; `opened` or `open_failed` not yet here.
    Opening,
    Open,
    Closing,
    Ended,
}
/// How an exchange ended, as the core sees it.
#[derive(Clone, Debug)]
pub(super) struct ExchEnd {
    pub(super) disposition: Disposition,
    pub(super) failure: Option<(ErrorCode, Effect)>,
}
pub(super) struct Exch {
    pub(super) service: GitService,
    pub(super) stage: Stage,
    pub(super) spec: Option<ExchangeSpec>,
    pub(super) stream: Option<i64>,
    pub(super) machine: Option<StreamMachine>,
    /// The `Open` reached the outbox: a receive-pack exchange may have started at the remote from here on.
    pub(super) open_sent: bool,
    pub(super) reused: bool,
    /// Set once the opening is decided: the exchange is ready, or why not.
    pub(super) outcome: Option<Result<(), OpenFailure>>,
    pub(super) end: Option<ExchEnd>,
    pub(super) finish_requested: bool,
    /// The application handle is gone: the exchange is aborted and reclaimed once its stream ends.
    pub(super) dropped: bool,
    /// The operation was cancelled while the `Open` was unanswered: the opening already resolved `Cancelled`, and the
    /// answer, when it comes, is met with `cancel` (or ignored, if it is a failure). The handle is still held.
    pub(super) cancel_on_open: bool,
}
impl Exch {
    fn new(spec: ExchangeSpec) -> Self {
        Self {
            service: spec.service(),
            stage: Stage::Queued,
            spec: Some(spec),
            stream: None,
            machine: None,
            open_sent: false,
            reused: false,
            outcome: None,
            end: None,
            finish_requested: false,
            dropped: false,
            cancel_on_open: false,
        }
    }
    /// Decides the opening, unless the operation was cancelled meanwhile: that decision stands.
    fn decide(&mut self, outcome: Result<(), OpenFailure>) {
        if !self.cancel_on_open {
            self.outcome = Some(outcome);
        }
    }
    pub(super) fn effect_on_loss(&self) -> Effect {
        if self.service == GitService::ReceivePackExchange && self.open_sent {
            Effect::Possible
        } else {
            Effect::None
        }
    }
}

pub(super) struct Binding {
    pub(super) id: BindingId,
    pub(super) session: String,
    pub(super) request: String,
    pub(super) mux: Mux,
    pub(super) reg: Reg,
    pub(super) exchanges: BTreeMap<u64, Exch>,
    pub(super) by_stream: BTreeMap<i64, u64>,
    pub(super) next_token: u64,
    pub(super) sealed: bool,
    pub(super) cancelled: bool,
    pub(super) faulted: bool,
    pub(super) opened: u64,
    /// Application handles (openings, exchanges) that hold the binding open.
    pub(super) live: usize,
    pub(super) revoked: Option<TerminalReason>,
    pub(super) done: Cell<OperationEnd>,
    /// The registration as it stood when the binding left the table, set under the state lock before the binding goes:
    /// the operation reads it once its binding is gone, so its registration never steps back to `Pending`.
    pub(super) last: Arc<OnceLock<Registration>>,
    /// The binding's unsent messages were taken out of the outbox (it is over: faulted, cancelled, refused or timed out).
    pub(super) purged: bool,
}

pub(super) struct Waiting {
    pub(super) id: u64,
    pub(super) waker: Option<Waker>,
}
pub(super) struct State {
    pub(super) lost: bool,
    pub(super) serial: u64,
    pub(super) ids: u64,
    pub(super) queue: VecDeque<Waiting>,
    /// Slots held: granted permits and live bindings.
    pub(super) active: usize,
    pub(super) claims: BTreeSet<u64>,
    pub(super) requests: BTreeSet<String>,
    pub(super) bindings: BTreeMap<u64, Binding>,
    pub(super) by_session: BTreeMap<String, u64>,
    pub(super) outbox: VecDeque<Attachment>,
    pub(super) waiters: Vec<Waiter>,
    /// Messages dropped for a binding that was absent or retired: no state was made for any of them.
    pub(super) discarded: usize,
    /// Terminal receipts to publish once the lock is released.
    pub(super) finished: Vec<(Cell<OperationEnd>, OperationEnd)>,
}

/// What a pending future waits for. A waiter is woken when its condition holds, never by the poll that registered it.
/// Each value belongs to one future, which is its slot: the same future polled again replaces its waker, and no other
/// future's waker is ever replaced or evicted.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Wait {
    /// The binding's registration is decided. Any number of futures wait for it (`Operation::registered` may be called
    /// again), so each carries its own `slot`.
    Registered { serial: u64, slot: u64 },
    /// The exchange's opening is decided (or its record is gone).
    Opening(u64, u64),
    /// The exchange has ended (or its record is gone).
    Ending(u64, u64),
}
pub(super) struct Waiter {
    wait: Wait,
    waker: Waker,
}
/// The most messages the outbox holds beyond the binds that `register` puts in it unconditionally.
const OUTBOX_ROOM: usize = 64;

impl State {
    pub(super) fn new() -> Self {
        Self {
            lost: false,
            serial: 0,
            ids: 0,
            queue: VecDeque::new(),
            active: 0,
            claims: BTreeSet::new(),
            requests: BTreeSet::new(),
            bindings: BTreeMap::new(),
            by_session: BTreeMap::new(),
            outbox: VecDeque::new(),
            waiters: Vec::new(),
            discarded: 0,
            finished: Vec::new(),
        }
    }

    /// Registers `waker` to be woken when `wait` holds. The caller has just seen it not hold, under the same lock. The
    /// future owning `wait` keeps one waiter: a poll with another waker replaces its waker, so a task polled with a fresh
    /// waker each time cannot grow the list, and no other future's waker is touched.
    pub(super) fn wait_for(&mut self, wait: Wait, waker: &Waker) {
        match self.waiters.iter_mut().find(|waiter| waiter.wait == wait) {
            Some(waiter) => {
                if !waiter.waker.will_wake(waker) {
                    waiter.waker = waker.clone();
                }
            }
            None => self.waiters.push(Waiter {
                wait,
                waker: waker.clone(),
            }),
        }
    }

    /// The future owning `wait` is gone: its waiter goes with it.
    pub(super) fn forget(&mut self, wait: Wait) {
        self.waiters.retain(|waiter| waiter.wait != wait);
    }

    fn holds(&self, wait: Wait) -> bool {
        match wait {
            Wait::Registered { serial, .. } => self
                .bindings
                .get(&serial)
                .is_none_or(|binding| !matches!(binding.reg, Reg::Pending)),
            Wait::Opening(serial, token) => self
                .bindings
                .get(&serial)
                .and_then(|binding| binding.exchanges.get(&token))
                .is_none_or(|exch| exch.outcome.is_some()),
            Wait::Ending(serial, token) => self
                .bindings
                .get(&serial)
                .and_then(|binding| binding.exchanges.get(&token))
                .is_none_or(|exch| exch.end.is_some()),
        }
    }

    /// The wakers whose condition holds now, taken out: the waiting futures whose state moved, the admission at the head
    /// of the queue once a slot is free (`active_limit`), and every admission once the carrier is lost. A future that
    /// registered its waker in this very call and is still waiting is not among them.
    pub(super) fn ready_wakers(&mut self, active_limit: usize) -> Vec<Waker> {
        let mut ready = Vec::new();
        for waiter in std::mem::take(&mut self.waiters) {
            if self.holds(waiter.wait) {
                ready.push(waiter.waker);
            } else {
                self.waiters.push(waiter);
            }
        }
        let head = self.queue.front().map(|waiting| waiting.id);
        let free = self.active < active_limit;
        for waiting in &mut self.queue {
            if (self.lost || (free && Some(waiting.id) == head))
                && let Some(waker) = waiting.waker.take()
            {
                ready.push(waker);
            }
        }
        ready
    }

    /// Moves everything that can move: arrivals into their bindings, each binding's work, and the outbox to the lane.
    pub(super) fn pump(&mut self, binding: &dyn CoreBinding, config: &RemoteConfig) {
        if !self.lost {
            self.work(binding, config);
        }
        self.retire_finished(binding);
    }

    fn work(&mut self, binding: &dyn CoreBinding, config: &RemoteConfig) {
        let now = binding.clock().now().get();
        loop {
            match binding.recv() {
                Ok(Some(message)) => self.route_in(message),
                Ok(None) => break,
                Err(LaneEnded) => {
                    self.lose();
                    return;
                }
            }
        }
        // A binding the carrier faulted ends alone (contract section 6): the carrier dropped what it had queued, so the
        // lane is how the binding learns of it.
        for entry in self.bindings.values_mut() {
            if !entry.faulted && binding.faulted(&entry.session) {
                entry.abort_all(ErrorCode::Protocol, false);
            }
        }
        let serials: Vec<u64> = self.bindings.keys().copied().collect();
        for serial in serials {
            let room = (self.outbox.len() < OUTBOX_ROOM).then_some(OUTBOX_ROOM - self.outbox.len());
            let path_base = self
                .bindings
                .get(&serial)
                .and_then(|b| binding.endpoint_path_base(&b.request));
            if let Some(entry) = self.bindings.get_mut(&serial) {
                entry.drive(now, room, &mut self.outbox, config, path_base.as_deref());
            }
        }
        self.flush(binding);
    }

    fn route_in(&mut self, message: Attachment) {
        let Some(&serial) = self.by_session.get(&message.1.session_id) else {
            self.discarded += 1;
            return;
        };
        let Some(entry) = self.bindings.get_mut(&serial) else {
            self.discarded += 1;
            return;
        };
        entry.receive(message);
    }

    /// Takes the unsent messages of every binding that is over out of the outbox: a `bind` still queued behind a full lane
    /// when its operation ended must never leave, or the client would register and the carrier open a binding nothing
    /// holds.
    fn purge_ended(&mut self) {
        for entry in self.bindings.values_mut() {
            if entry.faulted && !entry.purged {
                entry.purged = true;
                let session = entry.session.as_str();
                self.outbox
                    .retain(|message| message.1.session_id != session);
            }
        }
    }

    fn flush(&mut self, binding: &dyn CoreBinding) {
        self.purge_ended();
        while let Some(message) = self.outbox.pop_front() {
            match binding.send(message) {
                Ok(()) => {}
                Err(LaneSend::Full(message)) => {
                    self.outbox.push_front(*message);
                    break;
                }
                Err(LaneSend::Closed(message)) => {
                    self.outbox.push_front(*message);
                    self.lose();
                    return;
                }
            }
        }
    }

    /// The carrier ended: pending registrations end `NeverRegistered(CarrierLost)` with zero effects, live bindings
    /// are revoked, open exchanges fail `carrier_lost` and a receive-pack exchange that had started reports
    /// `Effect::Possible`. Nothing is sent again.
    pub(super) fn lose(&mut self) {
        if self.lost {
            return;
        }
        self.lost = true;
        self.outbox.clear();
        for entry in self.bindings.values_mut() {
            entry.revoke();
        }
    }

    /// Removes a sealed binding with nothing live; its slot goes back, and the lane is told it is over.
    fn retire_finished(&mut self, lane: &dyn CoreBinding) {
        let finished: Vec<u64> = self
            .bindings
            .iter()
            .filter(|(_, b)| {
                b.sealed && b.live == 0 && b.exchanges.values().all(|e| e.stage == Stage::Ended)
            })
            .map(|(serial, _)| *serial)
            .collect();
        for serial in finished {
            if let Some(entry) = self.bindings.remove(&serial) {
                let _ = entry.last.set(entry.registration());
                self.by_session.remove(&entry.session);
                lane.retire(&entry.session);
                self.requests.remove(&entry.request);
                self.active = self.active.saturating_sub(1);
                self.finished.push((entry.done.clone(), entry.end()));
            }
        }
    }
}

/// The stream configuration both ends use for one exchange: the negotiated limits, no batching (a Git exchange is
/// latency-bound and its pkt-lines are small), profile 2.
pub(super) fn stream_config(
    session: &str,
    id: i64,
    side: stream::Side,
    limits: &Limits,
) -> stream::Config {
    let mut config = stream::Config::new(session, id, side);
    config.profile_version = 2;
    config.receive_limits = limits.clone();
    config.peer_limits = limits.clone();
    config.coalesce_delay_ms = 0;
    let window = usize::try_from(limits.receive_window).unwrap_or(usize::MAX);
    config.receive_window = config.receive_window.min(window);
    config.peer_receive_window = config.peer_receive_window.min(window);
    let payload = usize::try_from(limits.data_payload).unwrap_or(usize::MAX);
    config.max_payload = config.max_payload.min(payload);
    config
}

/// The most machine messages one pass moves into a binding's mux: below the mux's data room, so none is refused.
const PER_PASS: usize = 16;

impl Binding {
    pub(super) fn new(
        id: BindingId,
        session: String,
        request: String,
        mux: Mux,
        done: Cell<OperationEnd>,
    ) -> Self {
        Self {
            id,
            session,
            request,
            mux,
            reg: Reg::Pending,
            exchanges: BTreeMap::new(),
            by_stream: BTreeMap::new(),
            next_token: 0,
            sealed: false,
            cancelled: false,
            faulted: false,
            opened: 0,
            live: 0,
            revoked: None,
            done,
            last: Arc::new(OnceLock::new()),
            purged: false,
        }
    }

    pub(super) fn end(&self) -> OperationEnd {
        OperationEnd {
            registration: self.registration(),
            cleanup: CleanupStatus::Complete,
            exchanges_opened: self.opened,
            cancelled: self.cancelled,
            revoked: self.revoked,
        }
    }
    pub(super) fn registration(&self) -> Registration {
        match &self.reg {
            Reg::Pending => Registration::Pending,
            Reg::Registered => Registration::Registered(self.id),
            Reg::Never(reason) => Registration::NeverRegistered(*reason),
        }
    }

    /// One arrival for this binding. A name other than the call's, or anything the mux refuses, faults this binding
    /// only (contract section 6). The client's `bind_rejected` is the mux's own: it retains the failure and closes,
    /// and `drive` reads it (`bootstrap_failure`).
    pub(super) fn receive(&mut self, message: Attachment) {
        if self.faulted || self.mux.phase() == Phase::Closed {
            return;
        }
        if message.0 != self.request {
            self.abort_all(ErrorCode::Protocol, false);
            return;
        }
        if self.mux.receive(&message).is_err() {
            self.abort_all(ErrorCode::Protocol, false);
        }
    }

    /// The binding ends without a carrier: everything unfinished fails with `code`.
    pub(super) fn abort_all(&mut self, code: ErrorCode, revoked: bool) {
        self.faulted = true;
        if matches!(self.reg, Reg::Pending) {
            self.reg = Reg::Never(if revoked {
                TerminalReason::CarrierLost
            } else {
                TerminalReason::ClientRefused(code)
            });
        }
        for exch in self.exchanges.values_mut() {
            if exch.stage == Stage::Ended {
                continue;
            }
            let effect = exch.effect_on_loss();
            exch.spec = None;
            // An opening still unanswered takes the binding's end, even one that was cancelled meanwhile and not yet
            // polled: a carrier that ended is the more telling answer. A cancel is `Cancelled`, a fault of the binding is
            // the fault.
            if exch.outcome.is_none() || exch.cancel_on_open {
                exch.outcome = Some(Err(if revoked {
                    OpenFailure::CarrierLost { effect }
                } else if code == ErrorCode::Cancelled {
                    OpenFailure::Cancelled
                } else {
                    OpenFailure::Fault(code)
                }));
            }
            if let Some(machine) = exch.machine.as_mut() {
                machine.disconnect();
            }
            exch.end = Some(ExchEnd {
                disposition: Disposition::Discarded,
                failure: Some((code, effect)),
            });
            exch.stage = Stage::Ended;
        }
        self.by_stream.clear();
        self.mux.disconnect();
    }
    /// The carrier ended.
    pub(super) fn revoke(&mut self) {
        if matches!(self.reg, Reg::Registered) {
            self.revoked = Some(TerminalReason::CarrierLost);
        }
        self.abort_all(ErrorCode::CarrierLost, true);
    }

    /// Cancels every exchange of the binding; false if it was already cancelled. A queued exchange ends at once, one
    /// still opening is cancelled when its `opened` arrives, and an open one sends `cancel` on its stream.
    pub(super) fn cancel_all(&mut self) -> bool {
        if std::mem::replace(&mut self.cancelled, true) {
            return false;
        }
        if matches!(self.reg, Reg::Pending) {
            self.reg = Reg::Never(TerminalReason::Cancelled);
            self.abort_all(ErrorCode::Cancelled, false);
            return true;
        }
        for exch in self.exchanges.values_mut() {
            match exch.stage {
                Stage::Queued => {
                    exch.spec = None;
                    exch.outcome = Some(Err(OpenFailure::Cancelled));
                    exch.end = Some(ExchEnd {
                        disposition: Disposition::Discarded,
                        failure: Some((ErrorCode::Cancelled, Effect::None)),
                    });
                    exch.stage = Stage::Ended;
                }
                Stage::Opening => {
                    exch.cancel_on_open = true;
                    exch.outcome = Some(Err(OpenFailure::Cancelled));
                }
                Stage::Open | Stage::Closing => {
                    if let Some(machine) = exch.machine.as_mut() {
                        machine.cancel();
                    }
                }
                Stage::Ended => {}
            }
        }
        true
    }

    /// Queues an exchange on the binding (its spec already passed the table). Returns its token.
    pub(super) fn queue_exchange(&mut self, spec: ExchangeSpec) -> u64 {
        let token = self.next_token;
        self.next_token += 1;
        self.opened += 1;
        let mut exch = Exch::new(spec);
        if self.faulted {
            // The binding ended (a fault, or the carrier under it) after the operation was made: nothing opens on it.
            let code = if self.revoked.is_some() {
                ErrorCode::CarrierLost
            } else {
                ErrorCode::Protocol
            };
            exch.spec = None;
            exch.outcome = Some(Err(if code == ErrorCode::CarrierLost {
                OpenFailure::CarrierLost {
                    effect: Effect::None,
                }
            } else {
                OpenFailure::Fault(code)
            }));
            exch.end = Some(ExchEnd {
                disposition: Disposition::Discarded,
                failure: Some((code, Effect::None)),
            });
            exch.stage = Stage::Ended;
        }
        self.exchanges.insert(token, exch);
        token
    }

    pub(super) fn drive(
        &mut self,
        now: u64,
        room: Option<usize>,
        outbox: &mut VecDeque<Attachment>,
        config: &RemoteConfig,
        path_base: Option<&str>,
    ) {
        if self.faulted {
            self.collect(room, outbox);
            return;
        }
        self.mux.advance(now);
        if self.mux.phase() == Phase::Closed {
            // The mux ended itself: the bootstrap bound passed, or a deadline did.
            let timed_out =
                matches!(self.reg, Reg::Pending) && self.mux.bootstrap_failure().is_none();
            let code = self
                .mux
                .bootstrap_failure()
                .map_or(ErrorCode::Timeout, |failure| failure.code);
            if timed_out {
                self.reg = Reg::Never(TerminalReason::TimedOut);
            } else if matches!(self.reg, Reg::Pending) {
                self.reg = Reg::Never(TerminalReason::ClientRefused(code));
            }
            self.abort_all(code, false);
            return;
        }
        if matches!(self.reg, Reg::Pending) && self.mux.phase() == Phase::Ready {
            self.reg = Reg::Registered;
        }
        if matches!(self.reg, Reg::Registered) {
            self.issue_opens(config, path_base);
        }
        self.drain_actions();
        let mut quiet = BTreeSet::new();
        self.collect(room, outbox);
        if room.is_some() {
            self.run_machines(now, &mut quiet);
            self.collect(room, outbox);
        }
        self.settle(&quiet);
    }

    /// Moves the mux's outbound messages to the outbox, and notes which exchanges' `Open` has gone.
    fn collect(&mut self, room: Option<usize>, outbox: &mut VecDeque<Attachment>) {
        let mut room = room.unwrap_or(0);
        while room > 0 {
            let Some(message) = self.mux.next_message() else {
                break;
            };
            if message.1.kind == MessageKind::Open
                && let Some(token) = self.by_stream.get(&message.1.stream_id)
                && let Some(exch) = self.exchanges.get_mut(token)
            {
                exch.open_sent = true;
            }
            outbox.push_back(message);
            room -= 1;
        }
    }

    fn issue_opens(&mut self, config: &RemoteConfig, path_base: Option<&str>) {
        let Some(bound) = self.mux.binding() else {
            return;
        };
        let (endpoint_id, limits) = (bound.endpoint_id().to_owned(), bound.limits().clone());
        let tokens: Vec<u64> = self
            .exchanges
            .iter()
            .filter(|(_, e)| e.stage == Stage::Queued)
            .map(|(token, _)| *token)
            .collect();
        for token in tokens {
            let Some(spec) = self.exchanges.get_mut(&token).and_then(|e| e.spec.take()) else {
                continue;
            };
            let context = OpenContext {
                endpoint_id: &endpoint_id,
                operation_id: &self.request,
                limits: &limits,
                deadlines: &config.deadlines,
                path_base,
            };
            let failure = match table::open_for(&spec, &context) {
                Err(_) => Some(OpenFailure::Fault(ErrorCode::InvalidRequest)),
                Ok(open) => match self.mux.open(&self.request, open) {
                    Ok(stream) => {
                        self.by_stream.insert(stream, token);
                        if let Some(exch) = self.exchanges.get_mut(&token) {
                            exch.stage = Stage::Opening;
                            exch.stream = Some(stream);
                        }
                        None
                    }
                    // `RemoteOp::open` already holds the exchanges to the binding's streams, so a full mux is a route
                    // whose terminal is still in flight (an abort at the bound): the exchange waits for it, as it waits
                    // for room in the outbox.
                    Err(mux::Error::Capacity | mux::Error::WouldBlock) => {
                        if let Some(exch) = self.exchanges.get_mut(&token) {
                            exch.spec = Some(spec);
                        }
                        return;
                    }
                    Err(_) => Some(OpenFailure::Fault(ErrorCode::InvalidRequest)),
                },
            };
            if let Some(failure) = failure
                && let Some(exch) = self.exchanges.get_mut(&token)
            {
                exch.outcome = Some(Err(failure));
                exch.end = Some(ExchEnd {
                    disposition: Disposition::Discarded,
                    failure: Some((ErrorCode::InvalidRequest, Effect::None)),
                });
                exch.stage = Stage::Ended;
            }
        }
    }

    fn drain_actions(&mut self) {
        while let Some((_, message)) = self.mux.next_action() {
            let stream = message.stream_id;
            let Some(&token) = self.by_stream.get(&stream) else {
                continue;
            };
            let Some(exch) = self.exchanges.get_mut(&token) else {
                continue;
            };
            match message.kind {
                MessageKind::Opened => {
                    let (Some(opened), Some(limits)) = (
                        message.opened,
                        self.mux
                            .binding()
                            .map(|b| (b.session_id().to_owned(), b.limits().clone())),
                    ) else {
                        continue;
                    };
                    let config =
                        stream_config(&limits.0, stream, stream::Side::Initiator, &limits.1);
                    match StreamMachine::new(config) {
                        Ok(mut machine) => {
                            if exch.dropped || exch.cancel_on_open {
                                machine.cancel();
                            }
                            exch.machine = Some(machine);
                            exch.reused = opened.reused;
                            exch.stage = Stage::Open;
                            exch.decide(Ok(()));
                        }
                        Err(_) => {
                            exch.decide(Err(OpenFailure::Fault(ErrorCode::Protocol)));
                            exch.end = Some(ExchEnd {
                                disposition: Disposition::Discarded,
                                failure: Some((ErrorCode::Protocol, exch.effect_on_loss())),
                            });
                            exch.stage = Stage::Ended;
                        }
                    }
                }
                MessageKind::OpenFailed => {
                    let failure = message.open_failed.unwrap_or_default();
                    exch.end = Some(ExchEnd {
                        disposition: Disposition::Discarded,
                        failure: Some((failure.code, failure.effect)),
                    });
                    exch.decide(Err(OpenFailure::Refused(failure)));
                    exch.stage = Stage::Ended;
                    self.by_stream.remove(&stream);
                }
                _ => {
                    if let Some(machine) = exch.machine.as_mut() {
                        // A message the machine refuses ends only that exchange: it reports its own error.
                        let _ = machine.receive(message);
                    }
                }
            }
        }
    }

    /// Moves each open exchange's machine output into the mux. A machine's message is taken only while the mux has room
    /// (`PER_PASS` bounds it below the mux's data room); `quiet` names the machines that had nothing more.
    fn run_machines(&mut self, now: u64, quiet: &mut BTreeSet<u64>) {
        let mut moved = 0;
        let tokens: Vec<u64> = self
            .exchanges
            .iter()
            .filter(|(_, e)| matches!(e.stage, Stage::Open | Stage::Closing) && e.machine.is_some())
            .map(|(token, _)| *token)
            .collect();
        for token in tokens {
            let Some(exch) = self.exchanges.get_mut(&token) else {
                continue;
            };
            let Some(machine) = exch.machine.as_mut() else {
                continue;
            };
            machine.advance(now);
            loop {
                if moved == PER_PASS {
                    break;
                }
                let Some(message) = machine.next_message() else {
                    quiet.insert(token);
                    break;
                };
                moved += 1;
                if self.mux.send(&self.request, &message).is_err() {
                    machine.cancel();
                    exch.end = Some(ExchEnd {
                        disposition: Disposition::Discarded,
                        failure: Some((ErrorCode::Protocol, exch.effect_on_loss())),
                    });
                    exch.stage = Stage::Ended;
                    break;
                }
            }
        }
    }

    /// Ends the exchanges whose stream is over and whose last message has gone.
    fn settle(&mut self, quiet: &BTreeSet<u64>) {
        let mut ended = Vec::new();
        for (token, exch) in &mut self.exchanges {
            if exch.stage == Stage::Ended || !quiet.contains(token) {
                continue;
            }
            let Some(machine) = exch.machine.as_ref() else {
                continue;
            };
            let end = match machine.close_result() {
                Ok(result) => Some(ExchEnd {
                    disposition: result.disposition,
                    failure: None,
                }),
                Err(stream::Error::WouldBlock) => None,
                Err(stream::Error::PeerFailed { code, effect }) => Some(ExchEnd {
                    disposition: Disposition::Discarded,
                    failure: Some((code, effect)),
                }),
                Err(stream::Error::CarrierLost) => Some(ExchEnd {
                    disposition: Disposition::Discarded,
                    failure: Some((ErrorCode::CarrierLost, exch.effect_on_loss())),
                }),
                Err(stream::Error::Cancelled) => Some(ExchEnd {
                    disposition: Disposition::Discarded,
                    failure: Some((ErrorCode::Cancelled, exch.effect_on_loss())),
                }),
                Err(stream::Error::Timeout) => Some(ExchEnd {
                    disposition: Disposition::Discarded,
                    failure: Some((ErrorCode::Timeout, exch.effect_on_loss())),
                }),
                Err(_) => Some(ExchEnd {
                    disposition: Disposition::Discarded,
                    failure: Some((ErrorCode::Protocol, exch.effect_on_loss())),
                }),
            };
            if let Some(end) = end {
                exch.end = Some(end);
                exch.stage = Stage::Ended;
                if let Some(stream) = exch.stream {
                    ended.push(stream);
                }
            }
        }
        for stream in ended {
            self.by_stream.remove(&stream);
        }
        // An exchange whose handle is gone and whose stream has ended is reclaimed.
        self.exchanges
            .retain(|_, e| !(e.dropped && e.stage == Stage::Ended));
    }
}
