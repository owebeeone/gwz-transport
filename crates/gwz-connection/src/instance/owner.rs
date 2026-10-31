//! The instance itself: its attachment holds, its driver-facing and caller-facing commands (movement from `instance.rs`).
use super::{leasing::*, proofs::*, state::*};
use crate::{
    AuthContext, BindingId, CleanupStatus, CleanupTicket, Clock, Completion, EndpointAdapter,
    EndpointProfile, ExchangeEnd, ExchangeIo, ExchangeSpec, OpenFailure,
    binding::Cleanups,
    cell::{Cell, Slot},
    pump::Signal,
    tables::InstanceTables,
};
use gwz_transport::pool::{Config, Error, LeaseId, PoolMachine, Request};
use gwz_transport::protocol::Disposition;
use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

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
