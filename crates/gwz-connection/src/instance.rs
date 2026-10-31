//! An endpoint instance: one adapter and the pool of its connections, owned by the host and leased to bindings.
//!
//! The pool is gwz-transport's state machine, which does no I/O: its actions go to the adapter and the adapter's
//! completions come back to it, here, in `pump`. The pump also carries the instance's in-flight work: openings whose
//! request waits for a lease (the lease is taken and the exchange started here, and handed to the opening's slot) and
//! finished exchanges whose adapter has not yet confirmed a clean end (settled here, then released). It runs when a
//! command arrives (a submission, a release, a cancellation) and on the host's driver (`pump.rs`) whenever the adapter
//! wakes the waker it was given or a pool deadline passes; never because an application polls.
//!
//! An instance is one profile's (step 1.5): every operation it serves has an equal profile, and none of them leaves its
//! authentication context here. A waiting opening lends its context to the connect the pool starts for it, by the tag
//! on its pool request, and takes it back when it stops waiting. What stays is each connection's setup scope, a
//! fixed-size value: on an HTTPS key a connection serves only an operation whose context has that scope (contract
//! sections 7 and 7.2; SSH connections are not partitioned by it, their reuse authorization being step 1.6's possession
//! proof). An HTTPS request goes to the pool with a scope-derived identity (`Identity::HttpsScoped` of an instance-local
//! token, `scope-N`, never the scope's bytes), so the pool's idle match offers an opening only its own scope's idle
//! connections, and reuse resumes under a changed context while the old context's connections are still idle. The scope
//! check at lease stays as the guard: a lease that does not fit (its setup scope unknown or other) is given back
//! untouched (`Reusable`, never retired), and the opening asks once more with a `fresh` request, which opens a
//! connection of its own under the opening's own tag and so its own scope. More than `misfit_bound` misfits (the larger of
//! the pool's and the binding's `per_user_host`, plus one) can only mean a broken scope record, and the opening then
//! fails by name (`OpenFailure::NoFittingConnection`). No resubmission, after a misfit or after a failed reuse proof,
//! extends the allocation deadline the opening was given at submit.
//!
//! Recorded limitation (lane owner's decision, step 1.5 remediation round 1): when one scope keeps the key's
//! `per_user_host` connections busy and its openings keep arriving, every connection that comes free goes to the next
//! waiter of that scope, so an opening of another scope waits for room: it is served when the load relents, or it ends
//! at its original deadline with `AllocationTimeout`, never `NoFittingConnection`.
//!
//! A fitting lease of an idle connection that another binding last used then waits for the adapter's reuse proof for
//! the leasing binding (`EndpointAdapter::authorize_reuse`; contract section 7.1; step 1.6) before its exchange starts:
//! scope fit first, then the cross-binding proof. The last lessee is metadata kept here with the connection's setup
//! scope, apart from the pool's owner, which an idle entry does not have (contract section 8). Each proof has a
//! host-minted `ProofId`: only the completion that names it ends it, and a proof whose opening went or whose host
//! sealed is cancelled (`cancel_reuse`). A lease no proof authorized goes back to idle unused (`release_unused`, which
//! keeps its idle time), and the opening asks again with it excluded, so a binding's own idle connection is not
//! shadowed by another binding's that it cannot prove. An opening that waits for its proof is governed by its binding's
//! cancellation (`cancel_authorizing`: the proof is cancelled, the lease goes back to idle and the opening ends
//! `Cancelled`, before the pool cancels the binding's owner and closes what it leases) and by its own allocation
//! deadline (`AllocationTimeout`, which the driver wakes for); the command tells the adapter how much of the deadline
//! is left. A binding that was cancelled asks the pool for nothing more.
use crate::{
    AuthContext, AuthScope, BindingId, CleanupStatus, CleanupTicket, Clock, Completion,
    ConnectCommand, EndpointAdapter, EndpointProfile, ExchangeEnd, ExchangeIo, ExchangeSpec,
    OpenFailure, ProofId, ReuseCommand, ReuseVerdict, TicketStatus,
    binding::Cleanups,
    cell::{Cell, Slot},
    disposal::Disposing,
    pump::Signal,
    tables::InstanceTables,
};
use gwz_transport::pool::{
    Action, Config, ConnectionId, Error, Identity, LeaseId, MAX_EXCLUDED, PoolMachine, Request,
    RequestId,
};
use gwz_transport::protocol::Disposition;
use gwz_transport::protocol::Scheme;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

/// What an opening's slot receives: the started exchange, its lease and whether its connection had served an exchange
/// before, or why there is none.
pub(crate) type Opened = Result<(Box<dyn ExchangeIo>, LeaseId, bool), OpenFailure>;
/// An opening whose request waits for a lease.
struct Pending {
    request: RequestId,
    /// The request as submitted (its tag included), to ask again after a lease of another scope, or, fresh, after a
    /// lease another binding last used could not serve it.
    template: Request,
    binding: BindingId,
    spec: ExchangeSpec,
    /// The operation's context, lent to the connect the pool starts for this request and to the exchange.
    auth: AuthContext,
    /// Leases of another scope this opening was offered and gave back.
    misfits: usize,
    /// Idle connections another binding last used that could not serve this opening (step 1.6): it asks again with
    /// them excluded, so its own binding's idle connection, or a new one, is offered next.
    excluded: Vec<ConnectionId>,
    /// The opening's absolute allocation deadline on the pool's clock, fixed at submit: a resubmission never extends it.
    deadline: u64,
    /// The binding's cancelled flag: an opening that asks the pool again for a cancelled binding ends Cancelled instead.
    cancelled: Arc<AtomicBool>,
    slot: Arc<Slot<Opened>>,
}
/// A connection the pool has started and not yet disposed of: the scope its setup used (none when its opener had gone)
/// and whether an exchange has run on it.
struct Setup {
    scope: Option<AuthScope>,
    /// An HTTPS connection, the scheme the scope partitions.
    https: bool,
    served: bool,
    /// The binding that last leased it, once one has.
    lessee: Option<BindingId>,
    /// The ticket of its disposal, once a receipt had to wait for it: confirmed when the adapter reports `Closed`.
    closing: Option<CleanupTicket>,
}
/// An opening holding a fitting lease whose reuse proof for its binding is running: the proof the host minted for it,
/// which only a completion naming it can end.
struct Authorizing {
    pending: Pending,
    lease: LeaseId,
    proof: ProofId,
}
/// A finished exchange waiting for its adapter to confirm how it ended.
struct Settling {
    io: Box<dyn ExchangeIo>,
    lease: LeaseId,
    end: Cell<ExchangeEnd>,
    /// The exchange's operation's record of disposals left unconfirmed.
    cleanups: Arc<Cleanups>,
}
/// A finished exchange whose connection is closing: its receipt is set when the adapter confirms the disposal
/// (`Complete`) or, if it has not by the cleanup bound, as `Pending` with the disposal's ticket.
struct Ending {
    ticket: CleanupTicket,
    deadline: u64,
    disposition: Disposition,
    end: Cell<ExchangeEnd>,
}
pub(crate) struct InstanceState {
    pub(crate) adapter: Option<Box<dyn EndpointAdapter>>,
    pub(crate) pool: PoolMachine,
    clock: Arc<dyn Clock>,
    /// The clock's epoch at the last pass: a change is a suspend or a wall-clock jump (contract section 10).
    epoch: u64,
    /// The pool's cleanup bound, which a receipt waits for a disposal.
    cleanup_ms: u64,
    ending: Vec<Ending>,
    openings: Vec<Pending>,
    authorizing: Vec<Authorizing>,
    settling: Vec<Settling>,
    connections: BTreeMap<ConnectionId, Setup>,
    /// Leases of another scope the instance has offered and refused, over its life (an observation, never a limit).
    misfits: u64,
    /// The pool identity token of each scope a live connection or opening uses (`scope-N`, `N` never reused), so the
    /// pool can match HTTPS idle connections by scope without holding the scope's bytes.
    scope_tokens: Vec<(AuthScope, u64)>,
    next_token: u64,
    /// The serial of the next reuse proof (`ProofId`), never reused within the instance's life.
    next_proof: u64,
}
impl InstanceState {
    /// The adapter, for disposal elsewhere, which confirms the unconfirmed tickets of this instance's connections when
    /// it is done: from then on no pass of this instance reads the adapter's completions.
    fn take_adapter(&mut self) -> Option<Box<dyn EndpointAdapter>> {
        let tickets = self
            .connections
            .values()
            .filter_map(|setup| setup.closing.clone())
            .filter(|ticket| ticket.status() == TicketStatus::Pending)
            .collect();
        self.adapter
            .take()
            .map(|adapter| Disposing::wrap(adapter, tickets))
    }
    /// The ticket for the disposal of `connection` if it is still closing, which the adapter confirms by `Closed`; none
    /// when the pool no longer holds it or the adapter has already disposed of it.
    fn closing_ticket(&mut self, connection: ConnectionId) -> Option<CleanupTicket> {
        if !self.pool.is_closing(connection) {
            return None;
        }
        let setup = self.connections.get_mut(&connection)?;
        let clock = &self.clock;
        Some(
            setup
                .closing
                .get_or_insert_with(|| CleanupTicket::new(connection.sequence(), clock.clone()))
                .clone(),
        )
    }
    /// Sets the receipt of a finished exchange whose lease was just released: at once if its connection stays, else
    /// when its disposal is confirmed or the cleanup bound passes. `cleanups` is its operation's record.
    fn report(
        &mut self,
        connection: ConnectionId,
        disposition: Disposition,
        end: Cell<ExchangeEnd>,
        cleanups: &Cleanups,
    ) {
        match self.closing_ticket(connection) {
            None => end.set(ExchangeEnd {
                disposition,
                cleanup: CleanupStatus::Complete,
                failure: None,
            }),
            Some(ticket) => {
                cleanups.note(ticket.clone());
                let deadline = self.clock.now().get().saturating_add(self.cleanup_ms);
                self.ending.push(Ending {
                    ticket,
                    deadline,
                    disposition,
                    end,
                });
            }
        }
    }
    /// Sets the receipts whose disposal is confirmed, and those past the cleanup bound (`all`: every one, as the
    /// instance stops) as pending on their ticket.
    fn resolve_ending(&mut self, all: bool) {
        let now = self.clock.now().get();
        self.ending.retain(|item| {
            let confirmed = item.ticket.status() == TicketStatus::Confirmed;
            if !(confirmed || all || now >= item.deadline) {
                return true;
            }
            item.end.set(ExchangeEnd {
                disposition: item.disposition,
                cleanup: if confirmed {
                    CleanupStatus::Complete
                } else {
                    CleanupStatus::Pending(item.ticket.clone())
                },
                failure: None,
            });
            false
        });
    }
    /// The pool identity of a request for `key` and `identity` from an operation whose context has `scope`: HTTPS
    /// requests are scoped, everything else is unchanged.
    fn pool_identity(
        &mut self,
        key: &gwz_transport::pool::Key,
        identity: Identity,
        scope: AuthScope,
    ) -> Identity {
        if key.scheme != Scheme::Https || identity != Identity::Https {
            return identity;
        }
        let token = match self.scope_tokens.iter().find(|(known, _)| *known == scope) {
            Some((_, token)) => *token,
            None => {
                let token = self.next_token;
                self.next_token += 1;
                self.scope_tokens.push((scope, token));
                token
            }
        };
        Identity::HttpsScoped(format!("scope-{token}"))
    }
    /// Forgets the tokens of scopes no live connection or opening uses. A forgotten scope gets a new token if it comes
    /// back, and a token is never given twice, so an orphaned connection's token matches nothing.
    fn prune_scope_tokens(&mut self) {
        let InstanceState {
            scope_tokens,
            connections,
            openings,
            authorizing,
            ..
        } = self;
        scope_tokens.retain(|(scope, _)| {
            connections
                .values()
                .any(|setup| setup.scope == Some(*scope))
                || openings
                    .iter()
                    .any(|pending| pending.auth.scope() == *scope)
                || authorizing
                    .iter()
                    .any(|item| item.pending.auth.scope() == *scope)
        });
    }
}
/// Wakes every opening that waits on this instance, and the host's driver, when its adapter has something to report.
struct Wakes {
    waiters: Mutex<Vec<Waker>>,
    driver: Arc<Signal>,
}
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.driver.notify();
        let waiters = std::mem::take(&mut *self.waiters.lock().unwrap_or_else(|e| e.into_inner()));
        for waiter in waiters {
            waiter.wake();
        }
    }
}
/// An instance pool's physical ceilings: 256 in total and per host and for a user and host, the reuse design's
/// provisional values (section 5). A request is held to its binding's limits and a new connection needs fewer than
/// max(ceiling, the binding's limit) entries, so a binding keeps limits above them and overlapping bindings share them.
fn ceilings() -> Config {
    Config {
        per_user_host: 256,
        per_host: 256,
        total: 256,
        ..Config::default()
    }
}
/// An attachment to an endpoint instance: while one exists the host does not retire the instance. A binding holds one
/// from the moment its admission attached to the instance until the binding is gone.
pub(crate) struct InstanceHold(Arc<Instance>);
impl InstanceHold {
    pub(crate) fn instance(&self) -> &Arc<Instance> {
        &self.0
    }
}
impl Drop for InstanceHold {
    fn drop(&mut self) {
        self.0.holds.fetch_sub(1, Ordering::AcqRel);
    }
}
pub(crate) struct Instance {
    pub(crate) profile: EndpointProfile,
    /// Attachments that hold the instance: one per [`InstanceHold`]. The host raises it under its own lock, when it
    /// hands the instance out, and it is the one thing that says "a binding is on the way" before the binding exists.
    holds: AtomicUsize,
    /// The host's use serial at the last attachment: the order the host retires unbound instances in.
    last_used: AtomicU64,
    clock: Arc<dyn Clock>,
    /// The pool's allocation bound, from which an opening's deadline is fixed at submit.
    allocation_ms: u64,
    wakes: Arc<Wakes>,
    state: Mutex<InstanceState>,
}
impl Instance {
    pub(crate) fn new(
        profile: EndpointProfile,
        adapter: Box<dyn EndpointAdapter>,
        clock: Arc<dyn Clock>,
        driver: Arc<Signal>,
    ) -> Result<Arc<Self>, Error> {
        let config = ceilings();
        let epoch = clock.epoch();
        Ok(Arc::new(Self {
            profile,
            holds: AtomicUsize::new(0),
            last_used: AtomicU64::new(0),
            clock: clock.clone(),
            allocation_ms: config.allocation_timeout_ms,
            wakes: Arc::new(Wakes {
                waiters: Mutex::new(Vec::new()),
                driver,
            }),
            state: Mutex::new(InstanceState {
                adapter: Some(adapter),
                epoch,
                cleanup_ms: config.cleanup_timeout_ms,
                clock,
                ending: Vec::new(),
                pool: PoolMachine::new(config)?,
                openings: Vec::new(),
                authorizing: Vec::new(),
                settling: Vec::new(),
                connections: BTreeMap::new(),
                misfits: 0,
                scope_tokens: Vec::new(),
                next_token: 0,
                next_proof: 0,
            }),
        }))
    }
    #[allow(dead_code, reason = "the sustained-use rows read it through the host")]
    pub(crate) fn tables(&self) -> InstanceTables {
        let state = self.lock();
        InstanceTables {
            openings: state.openings.len(),
            authorizing: state.authorizing.len(),
            settling: state.settling.len(),
            connections: state.connections.len(),
            scope_tokens: state.scope_tokens.len(),
            requests: state.pool.outstanding_requests(),
        }
    }
    /// Attaches to the instance for a binding that is about to be made, stamping its use. Only the host calls it, under
    /// its registry lock, which is what makes selection and retirement one transition (contract section 6): an
    /// instance whose holds are raised there cannot be chosen for retirement by a later transition.
    pub(crate) fn hold(self: &Arc<Self>, used: u64) -> InstanceHold {
        self.holds.fetch_add(1, Ordering::AcqRel);
        self.last_used.store(used, Ordering::Release);
        InstanceHold(self.clone())
    }
    /// The host's use serial when the instance was last attached.
    pub(crate) fn last_used(&self) -> u64 {
        self.last_used.load(Ordering::Acquire)
    }
    /// Whether the host may retire the instance now: nothing holds it (no binding, and none on the way), no opening
    /// waits on it and no connection is leased (a reuse proof and an exchange waiting to be settled each hold a lease, so
    /// retiring cannot strand a receipt). Idle, connecting and closing connections do not keep it: retiring stops the
    /// pool, which closes or cancels them. The host calls it under its registry lock and attaches only under that lock,
    /// so a hold cannot appear between this check and the removal. An opening belongs to a binding, so none waits
    /// once nothing holds the instance: that condition is the contract's own wording and holds by construction.
    pub(crate) fn unbound(&self) -> bool {
        if self.holds.load(Ordering::Acquire) != 0 {
            return false;
        }
        let state = self.lock();
        state.adapter.is_some() && state.openings.is_empty() && state.pool.counts().leased == 0
    }
    /// Retires an unbound instance the host has taken out of its registry: the pool stops (its idle connections are told
    /// to close) and the adapter is handed back for disposal. Nothing can attach to the instance any more, because only
    /// the registry hands it out.
    pub(crate) fn retire(&self) -> Option<Box<dyn EndpointAdapter>> {
        let mut state = self.lock();
        state.pool.shutdown();
        self.pump(&mut state);
        // The adapter goes for disposal elsewhere, so no pass of this instance will confirm what waits on it.
        state.resolve_ending(true);
        state.take_adapter()
    }
    /// Takes the adapter for disposal, which confirms the tickets of the receipts stated while it was still the
    /// instance's (`take_adapter` on the state).
    pub(crate) fn take_adapter(&self) -> Option<Box<dyn EndpointAdapter>> {
        self.lock().take_adapter()
    }
    pub(crate) fn lock(&self) -> MutexGuard<'_, InstanceState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Has `waker` woken when the adapter next reports, once.
    pub(crate) fn wait(&self, waker: &Waker) {
        let mut waiters = self.wakes.waiters.lock().unwrap_or_else(|e| e.into_inner());
        if !waiters.iter().any(|w| w.will_wake(waker)) {
            waiters.push(waker.clone());
        }
    }
    pub(crate) fn wake_waiters(&self) {
        let waiters =
            std::mem::take(&mut *self.wakes.waiters.lock().unwrap_or_else(|e| e.into_inner()));
        for waiter in waiters {
            waiter.wake();
        }
    }
    /// Ends a caller-driven pump (`submit`, `settle`, `release`, `abandon`, `exchange::cancel_binding`): wakes the openings
    /// that wait on the instance and re-arms the host's driver. The pump may have created a deadline (an opening's
    /// allocation bound, an idle entry's expiry, a cleanup bound) or moved one earlier than the driver's last pass
    /// computed, and the driver sleeps only on that computation (contract section 4.1: `Progress` runs the timers
    /// independently of application calls). The driver's own pass (`drive`) never calls this, or it would wake itself
    /// in a loop.
    pub(crate) fn pumped(&self) {
        self.wake_waiters();
        self.wakes.driver.notify();
    }
    /// The pool's next deadline on the host's clock, which the driver sleeps until.
    /// It includes the deadline of each opening that waits for its proof, which the pool no longer holds a request for.
    pub(crate) fn next_deadline(&self) -> Option<u64> {
        let state = self.lock();
        let proofs = state
            .authorizing
            .iter()
            .map(|item| item.pending.deadline)
            .min();
        let receipts = state.ending.iter().map(|item| item.deadline).min();
        [state.pool.next_deadline(), proofs, receipts]
            .into_iter()
            .flatten()
            .min()
    }
    /// One pass of the driver: everything the adapter reported, the pool's actions and the in-flight work.
    pub(crate) fn drive(&self) {
        {
            let mut state = self.lock();
            self.pump(&mut state);
        }
        self.wake_waiters();
    }
    /// A suspend or a wall-clock jump (the clock's epoch moved) makes every idle entry's age untrustworthy: they are
    /// retired before anything is leased (contract section 10). The clock is read outside the pool's transition.
    fn observe_epoch(&self, state: &mut InstanceState) {
        let epoch = self.clock.epoch();
        if epoch != state.epoch {
            state.epoch = epoch;
            state.pool.expire_idle();
        }
    }
    /// Carries the pool's actions to the adapter, the adapter's completions to the pool, leases to waiting openings
    /// and confirmed ends to the pool, until none of them has more.
    pub(crate) fn pump(&self, state: &mut InstanceState) {
        self.observe_epoch(state);
        state.pool.advance(self.clock.now().get());
        let waker = Waker::from(self.wakes.clone());
        let mut cx = Context::from_waker(&waker);
        loop {
            let mut progressed = actions(state);
            let mut verdicts = Vec::new();
            if let Some(adapter) = state.adapter.as_mut() {
                let mut ended = Vec::new();
                while let Poll::Ready(completion) = adapter.poll_completion(&mut cx) {
                    progressed = true;
                    // A stale or duplicate completion is refused by the pool and changes nothing.
                    let _ = match completion {
                        Completion::Connected { connection, result } => {
                            if result.is_err() {
                                ended.push(connection);
                            }
                            state.pool.connected(connection, result)
                        }
                        Completion::Closed { connection } => {
                            ended.push(connection);
                            state.pool.closed(connection)
                        }
                        Completion::IdleClosed { connection } => {
                            ended.push(connection);
                            state.pool.idle_closed(connection)
                        }
                        Completion::Reuse {
                            connection,
                            proof,
                            verdict,
                        } => {
                            verdicts.push((connection, proof, verdict));
                            Ok(())
                        }
                    };
                }
                for connection in ended {
                    // The disposal receipts wait for is confirmed here, whatever ended the connection.
                    if let Some(ticket) = state
                        .connections
                        .remove(&connection)
                        .and_then(|setup| setup.closing)
                    {
                        ticket.confirm();
                    }
                }
            }
            // An opening's deadline passes whatever its proof is doing, and a verdict after it serves nothing.
            progressed |= expire_authorizing(state, self.clock.now().get());
            for (connection, proof, verdict) in verdicts {
                reused(state, connection, proof, verdict);
            }
            progressed |= openings(state, self.clock.now().get());
            progressed |= settle(state, &mut cx);
            if !progressed {
                break;
            }
        }
        state.resolve_ending(false);
        state.prune_scope_tokens();
    }
    /// Queues an opening's request and pumps once, so that an exchange the pool can lease at once is started now.
    /// `request` carries the opening's tag, by which a connect started for it finds `auth`.
    pub(crate) fn submit(
        &self,
        request: Request,
        binding: BindingId,
        spec: ExchangeSpec,
        auth: AuthContext,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Arc<Slot<Opened>>, Error> {
        let slot = Slot::new();
        {
            let mut state = self.lock();
            // The pool's clock is brought to now first, so the deadline recorded here is the one the pool gives the
            // request, and an idle connection whose time is up is not offered to it.
            let now = self.clock.now().get();
            self.observe_epoch(&mut state);
            state.pool.advance(now);
            let deadline =
                now.saturating_add(request.allocation_timeout_ms.unwrap_or(self.allocation_ms));
            let mut request = request;
            request.identity =
                state.pool_identity(&request.key, request.identity.clone(), auth.scope());
            let template = request.clone();
            let request = state.pool.request(request)?;
            state.openings.push(Pending {
                request,
                template,
                binding,
                spec,
                auth,
                misfits: 0,
                excluded: Vec::new(),
                deadline,
                cancelled,
                slot: slot.clone(),
            });
            self.pump(&mut state);
        }
        self.pumped();
        Ok(slot)
    }
    /// Hands a finished exchange to the pump, which releases its lease once the adapter confirms how it ended.
    pub(crate) fn settle(
        &self,
        io: Box<dyn ExchangeIo>,
        lease: LeaseId,
        cleanups: Arc<Cleanups>,
    ) -> Cell<ExchangeEnd> {
        let end = Cell::new();
        {
            let mut state = self.lock();
            state.settling.push(Settling {
                io,
                lease,
                end: end.clone(),
                cleanups,
            });
            self.pump(&mut state);
        }
        self.pumped();
        end
    }
    /// Gives a lease back and lets the pool act on it. The ticket of the connection's disposal, if it is closing and the
    /// adapter has not confirmed it.
    pub(crate) fn release(
        &self,
        lease: LeaseId,
        disposition: Disposition,
    ) -> Option<CleanupTicket> {
        let ticket = {
            let mut state = self.lock();
            let connection = lease.connection();
            let _ = state.pool.release(lease, disposition);
            self.pump(&mut state);
            state.closing_ticket(connection)
        };
        self.pumped();
        ticket
    }
    /// Discards an exchange's lease, as `release` does, and states how its end went in the receipt.
    pub(crate) fn discard(&self, lease: LeaseId, cleanups: &Cleanups) -> Cell<ExchangeEnd> {
        let end = Cell::new();
        {
            let mut state = self.lock();
            let connection = lease.connection();
            let _ = state.pool.release(lease, Disposition::Discarded);
            state.report(connection, Disposition::Discarded, end.clone(), cleanups);
            self.pump(&mut state);
        }
        self.pumped();
        end
    }
    /// An opening went before its exchange: its request and its context leave the instance, and an exchange already
    /// handed to it is aborted and its lease discarded.
    /// The opening is found by its slot, so the request abandoned is its current one (a misfit resubmits it).
    pub(crate) fn abandon(&self, slot: &Arc<Slot<Opened>>) {
        let handed = slot.abandon();
        {
            let mut state = self.lock();
            // The opening is found by its slot: its request is the one it asked with last.
            if let Some(index) = state
                .openings
                .iter()
                .position(|pending| Arc::ptr_eq(&pending.slot, slot))
            {
                let pending = state.openings.remove(index);
                state.pool.abandon(pending.request);
            }
            // A lease whose reuse proof is running is still clean: it goes back to idle with the idle time it had, its
            // proof is cancelled, and the proof's late end, naming that proof, serves nothing.
            let mut index = 0;
            while index < state.authorizing.len() {
                if Arc::ptr_eq(&state.authorizing[index].pending.slot, slot) {
                    let item = state.authorizing.remove(index);
                    if let Some(adapter) = state.adapter.as_mut() {
                        adapter.cancel_reuse(item.proof);
                    }
                    let _ = state.pool.release_unused(item.lease);
                } else {
                    index += 1;
                }
            }
            if let Some(Ok((mut io, lease, _))) = handed {
                io.abort();
                let _ = state.pool.release(lease, Disposition::Discarded);
            }
            self.pump(&mut state);
        }
        self.pumped();
    }
    /// The host is sealed: what is in flight ends now. Openings get `Closing(reason)` from their own check; their
    /// slots are failed here so that nothing waits on a pump that has stopped. Settling exchanges are aborted and
    /// reported discarded.
    pub(crate) fn fail_in_flight(&self, reason: crate::operation::TerminalReason) {
        let (openings, settling) = {
            let mut state = self.lock();
            let mut openings = std::mem::take(&mut state.openings);
            for item in std::mem::take(&mut state.authorizing) {
                if let Some(adapter) = state.adapter.as_mut() {
                    adapter.cancel_reuse(item.proof);
                }
                let _ = state.pool.release(item.lease, Disposition::Discarded);
                openings.push(item.pending);
            }
            let settling = std::mem::take(&mut state.settling);
            for item in &settling {
                let _ = state.pool.release(item.lease, Disposition::Discarded);
            }
            // The adapter is told to close what was released, and what it has already confirmed is drained. The rest
            // goes for disposal elsewhere, whose confirmation is what confirms its tickets (`take_adapter`): receipts
            // that wait on this instance are stated now.
            self.pump(&mut state);
            state.resolve_ending(true);
            let settling: Vec<_> = settling
                .into_iter()
                .map(|item| {
                    let ticket = state.closing_ticket(item.lease.connection());
                    (item, ticket)
                })
                .collect();
            (openings, settling)
        };
        for pending in openings {
            let _ = pending.slot.fill(Err(OpenFailure::Closing(reason)));
        }
        for (mut item, ticket) in settling {
            item.io.abort();
            let cleanup = match ticket {
                Some(ticket) => {
                    item.cleanups.note(ticket.clone());
                    CleanupStatus::Pending(ticket)
                }
                None => CleanupStatus::Complete,
            };
            item.end.set(ExchangeEnd {
                disposition: Disposition::Discarded,
                cleanup,
                failure: None,
            });
        }
        self.wake_waiters();
    }
}

/// The pool's actions, to the adapter. A connect is given the context of the opening whose request it is for, and its
/// scope is recorded. Whether there were any.
fn actions(state: &mut InstanceState) -> bool {
    let mut progressed = false;
    while let Some(action) = state.pool.next_action() {
        progressed = true;
        let InstanceState {
            adapter,
            openings,
            connections,
            ..
        } = &mut *state;
        let Some(adapter) = adapter.as_mut() else {
            continue;
        };
        match action {
            Action::Connect {
                connection,
                key,
                identity,
                tag,
                ..
            } => {
                let auth = tag.and_then(|tag| {
                    openings
                        .iter()
                        .find(|pending| pending.template.tag.as_deref() == Some(tag.as_str()))
                        .map(|pending| pending.auth.clone())
                });
                connections.insert(
                    connection,
                    Setup {
                        scope: auth.as_ref().map(AuthContext::scope),
                        https: key.scheme == Scheme::Https,
                        served: false,
                        lessee: None,
                        closing: None,
                    },
                );
                adapter.connect(ConnectCommand {
                    connection,
                    key,
                    identity,
                    auth,
                })
            }
            Action::CancelConnect { connection, .. } | Action::AbortConnect { connection } => {
                adapter.cancel_connect(connection)
            }
            Action::Close { connection, .. } | Action::Abort { connection } => {
                adapter.close(connection)
            }
        }
    }
    progressed
}

/// Leases for the openings whose request the pool can now serve: the exchange is started and handed to its slot.
fn openings(state: &mut InstanceState, now: u64) -> bool {
    let mut progressed = false;
    let mut index = 0;
    while index < state.openings.len() {
        let request = state.openings[index].request;
        let outcome = match state.pool.take(request) {
            Err(Error::WouldBlock) => {
                index += 1;
                continue;
            }
            Err(error) => Err(OpenFailure::Pool(error)),
            Ok(lease) => {
                let scope = state.openings[index].auth.scope();
                let setup = state.connections.get(&lease.connection());
                if setup.is_none() {
                    // No setup record: the adapter has disposed of the connection (its IdleClosed lost the race to this
                    // lease, and the pool refused it). It is never handed back as reusable, which would revive it; the
                    // release ends it, and the opening asks again, not as a misfit, within its own deadline.
                    progressed = true;
                    let _ = state.pool.release(lease, Disposition::Discarded);
                    let pending = state.openings.remove(index);
                    lost_lease(state, pending);
                    continue;
                }
                // The scope partitions HTTPS keys only; SSH reuse is authorized by the proof below (step 1.6).
                let fits = setup.is_some_and(|setup| !setup.https || setup.scope == Some(scope));
                if !fits {
                    // Set up under another operation's scope (a known one): never handed over. It goes back untouched, for
                    // its own scope, and the opening asks once more for a connection of its own, by its original
                    // deadline.
                    progressed = true;
                    state.misfits += 1;
                    let _ = state.pool.release(lease, Disposition::Reusable);
                    let pending = &mut state.openings[index];
                    let bound = misfit_bound(&state.pool, &pending.template);
                    pending.misfits += 1;
                    let asked = if pending.misfits > bound {
                        Err(OpenFailure::NoFittingConnection)
                    } else {
                        let mut fresh = pending.template.clone();
                        fresh.fresh = true;
                        state
                            .pool
                            .request_until(fresh, Some(pending.deadline))
                            .map_err(OpenFailure::Pool)
                    };
                    match asked {
                        Ok(request) => {
                            state.openings[index].request = request;
                            index += 1;
                            continue;
                        }
                        Err(failure) => {
                            let pending = state.openings.remove(index);
                            let _ = pending.slot.fill(Err(failure));
                            continue;
                        }
                    }
                }
                // A fitting lease another binding last used serves this one only after this binding's proof.
                let last = setup.and_then(|setup| setup.lessee);
                progressed = true;
                let pending = state.openings.remove(index);
                if last.is_some_and(|last| last != pending.binding) {
                    state.next_proof += 1;
                    let proof = ProofId(state.next_proof);
                    let command = ReuseCommand {
                        connection: lease.connection(),
                        binding: pending.binding,
                        proof,
                        remaining_ms: pending.deadline.saturating_sub(now),
                    };
                    let started = state
                        .adapter
                        .as_mut()
                        .is_some_and(|adapter| adapter.authorize_reuse(command));
                    if started {
                        state.authorizing.push(Authorizing {
                            pending,
                            lease,
                            proof,
                        });
                    } else {
                        // Nothing this binding can prove: the connection goes back to idle, its idle time untouched,
                        // and this opening asks again without it.
                        let bounced = lease.connection();
                        let _ = state.pool.release_unused(lease);
                        ask_again(state, pending, Some(bounced));
                    }
                    continue;
                }
                serve(state, pending, lease);
                continue;
            }
        };
        progressed = true;
        let pending = state.openings.remove(index);
        let _ = pending.slot.fill(outcome);
    }
    progressed
}

/// Starts `pending`'s exchange on `lease`, with the operation's own context, and hands it to the opening's slot; the
/// lease's binding becomes its connection's last lessee.
fn serve(state: &mut InstanceState, pending: Pending, lease: LeaseId) {
    let connection = lease.connection();
    let reused = state
        .connections
        .get_mut(&connection)
        .is_some_and(|setup| std::mem::replace(&mut setup.served, true));
    let started = match state.adapter.as_mut() {
        Some(adapter) => adapter.start_exchange(connection, &pending.spec, &pending.auth),
        None => Err(std::io::Error::other("endpoint instance is closed")),
    };
    let outcome = match started {
        Ok(io) => {
            if let Some(setup) = state.connections.get_mut(&connection) {
                setup.lessee = Some(pending.binding);
            }
            Ok((io, lease, reused))
        }
        // The adapter retired the connection after the pool leased it (a failed proof of its key retired it): a lost
        // lease, as the pool's own `IdleClosed` race makes one, not an I/O failure of the opening.
        Err(error) if error.kind() == std::io::ErrorKind::NotConnected => {
            let _ = state.pool.release(lease, Disposition::Discarded);
            return lost_lease(state, pending);
        }
        Err(error) => {
            let _ = state.pool.release(lease, Disposition::Discarded);
            Err(OpenFailure::Io(error))
        }
    };
    if let Err(Ok((mut io, lease, _))) = pending.slot.fill(outcome) {
        // The opening went while its exchange was being started.
        io.abort();
        let _ = state.pool.release(lease, Disposition::Discarded);
    }
}

/// How many misfits an opening may meet: the larger of the pool's per-user-host ceiling and its binding's own limit, plus
/// one. Idle entries on a key can reach the larger of the two (overlapping bindings share the ceilings), and each one the
/// opening meets that cannot serve it, or that the adapter has lost, counts once; more than that can only mean a broken
/// adapter (a request without limits is bounded by the pool's own capacity).
fn misfit_bound(pool: &PoolMachine, request: &Request) -> usize {
    let ceiling = pool.capacity().per_user_host;
    request
        .limits
        .map_or(ceiling, |limits| ceiling.max(limits.per_user_host))
        + 1
}

/// `pending` held a lease of a connection that is gone (the adapter disposed of it, or retired it, before the exchange
/// began): the lease is discarded by the caller and the opening asks again, not as a misfit, within its own deadline.
/// More than `misfit_bound` of them can only mean a broken adapter, and the opening then fails by name.
fn lost_lease(state: &mut InstanceState, mut pending: Pending) {
    pending.misfits += 1;
    let bound = misfit_bound(&state.pool, &pending.template);
    if pending.misfits > bound {
        let _ = pending.slot.fill(Err(OpenFailure::NoFittingConnection));
        return;
    }
    ask_again(state, pending, None);
}

/// `pending`'s lease of `bounced` could not serve it: it asks the pool again, with every connection that could not serve
/// it excluded, so that its own binding's idle connection, or a new one, is offered next (step 1.6 remediation, S-P2-2),
/// within its own allocation. Past the pool's exclusion bound it asks fresh, for a new connection. A connection already
/// disposed of (`None`) is excluded by nothing: the pool no longer offers it.
fn ask_again(state: &mut InstanceState, mut pending: Pending, bounced: Option<ConnectionId>) {
    if refused_for_cancelled(&pending) {
        return;
    }
    if let Some(bounced) = bounced
        && !pending.excluded.contains(&bounced)
    {
        pending.excluded.push(bounced);
    }
    let mut asked = pending.template.clone();
    if pending.excluded.len() <= MAX_EXCLUDED {
        asked.exclude = pending.excluded.clone();
    } else {
        asked.fresh = true;
    }
    // Within the deadline the opening was given at submit, never a new one.
    match state.pool.request_until(asked, Some(pending.deadline)) {
        Ok(request) => {
            pending.request = request;
            state.openings.push(pending);
        }
        Err(error) => {
            let _ = pending.slot.fill(Err(OpenFailure::Pool(error)));
        }
    }
}

/// A binding that was cancelled asks the pool for nothing more (the pool keeps no record that its owner was cancelled, so
/// a new request would open a connection, and spend an agent signature, for an operation the user stopped): the opening
/// ends `Cancelled`. Whether it was.
fn refused_for_cancelled(pending: &Pending) -> bool {
    let cancelled = pending.cancelled.load(Ordering::Acquire);
    if cancelled {
        let _ = pending.slot.fill(Err(OpenFailure::Cancelled));
    }
    cancelled
}

/// `pending` asks the pool again for a new connection, with no exclusion, within the deadline it was given at submit.
fn ask_fresh(state: &mut InstanceState, mut pending: Pending) {
    if refused_for_cancelled(&pending) {
        return;
    }
    pending.excluded.clear();
    let mut asked = pending.template.clone();
    asked.fresh = true;
    asked.exclude.clear();
    match state.pool.request_until(asked, Some(pending.deadline)) {
        Ok(request) => {
            pending.request = request;
            state.openings.push(pending);
        }
        Err(error) => {
            let _ = pending.slot.fill(Err(OpenFailure::Pool(error)));
        }
    }
}

/// Ends `item`, an opening whose lease waits for its proof, without serving it: the proof is cancelled, the lease goes back
/// to idle unused (it is another binding's connection, still clean, and keeps the idle time it had) and the opening
/// fails with `failure`. A verdict that comes later names a proof the host no longer waits for and changes nothing.
fn end_authorizing(state: &mut InstanceState, item: Authorizing, failure: OpenFailure) {
    if let Some(adapter) = state.adapter.as_mut() {
        adapter.cancel_reuse(item.proof);
    }
    let _ = state.pool.release_unused(item.lease);
    let _ = item.pending.slot.fill(Err(failure));
}

/// The binding was cancelled: each of its openings that waits for a proof ends `Cancelled` now (contract section 7.1: a
/// cancelled proof returns a still-clean leased connection to idle before the binding is cancelled, and section 8: a
/// cancellation affects its binding only, not another binding's idle connections). Called before the pool cancels the
/// binding's owner, which closes what the owner leases.
pub(crate) fn cancel_authorizing(state: &mut InstanceState, binding: BindingId) {
    let mut index = 0;
    while index < state.authorizing.len() {
        if state.authorizing[index].pending.binding == binding {
            let item = state.authorizing.remove(index);
            end_authorizing(state, item, OpenFailure::Cancelled);
        } else {
            index += 1;
        }
    }
}

/// Each opening that waits for a proof past its allocation deadline ends `AllocationTimeout`: a proof never extends the
/// deadline (contract section 7.1). Whether any did.
fn expire_authorizing(state: &mut InstanceState, now: u64) -> bool {
    let mut progressed = false;
    let mut index = 0;
    while index < state.authorizing.len() {
        if state.authorizing[index].pending.deadline <= now {
            let item = state.authorizing.remove(index);
            end_authorizing(state, item, OpenFailure::Pool(Error::AllocationTimeout));
            progressed = true;
        } else {
            index += 1;
        }
    }
    progressed
}

/// The reuse proof `proof` for `connection` ended with `verdict` (contract section 7.1): an authorized lease serves its
/// opening; a revoked one is discarded and an unprovable one goes back to idle with its idle time untouched, and the
/// opening asks again without it. A completion whose proof the host no longer waits for (its opening went, or another
/// binding's proof now holds the connection) changes nothing (contract section 4.1: a late completion never acts on
/// another generation).
fn reused(
    state: &mut InstanceState,
    connection: ConnectionId,
    proof: ProofId,
    verdict: ReuseVerdict,
) {
    let Some(index) = state
        .authorizing
        .iter()
        .position(|item| item.proof == proof && item.lease.connection() == connection)
    else {
        return;
    };
    let Authorizing { pending, lease, .. } = state.authorizing.remove(index);
    match verdict {
        ReuseVerdict::Authorized => serve(state, pending, lease),
        ReuseVerdict::Revoked => {
            // A denied proof is asked once, not once per pooled connection (reuse design §4): the retry takes no idle
            // connection, whether or not the adapter has yet reported the siblings it retired as `IdleClosed`.
            let _ = state.pool.release(lease, Disposition::Discarded);
            ask_fresh(state, pending);
        }
        ReuseVerdict::Unprovable => {
            let _ = state.pool.release_unused(lease);
            ask_again(state, pending, Some(connection));
        }
    }
}

/// Finished exchanges whose adapter now says how they ended: their leases go back to the pool with that disposition.
fn settle(state: &mut InstanceState, cx: &mut Context<'_>) -> bool {
    let mut progressed = false;
    let mut index = 0;
    while index < state.settling.len() {
        let Poll::Ready(disposition) = state.settling[index].io.poll_finish(cx) else {
            index += 1;
            continue;
        };
        progressed = true;
        let item = state.settling.remove(index);
        let connection = item.lease.connection();
        let _ = state.pool.release(item.lease, disposition);
        state.report(connection, disposition, item.end, &item.cleanups);
    }
    progressed
}

cfg_if::cfg_if! {
    if #[cfg(test)] {
        impl Instance {
            /// Makes every connection the instance holds now one set up under `scope`, as if another operation's
            /// context had set it up: what a test needs to offer an opening only connections that do not fit.
            pub(crate) fn rescope_for_test(&self, scope: AuthScope) {
                for setup in self.lock().connections.values_mut() {
                    setup.scope = Some(scope);
                }
            }
            /// Leases of another scope this instance has refused.
            pub(crate) fn misfits_for_test(&self) -> u64 {
                self.lock().misfits
            }
            /// Pool requests that are still live.
            pub(crate) fn outstanding_for_test(&self) -> usize {
                self.lock().pool.outstanding_requests()
            }
        }
    }
}
