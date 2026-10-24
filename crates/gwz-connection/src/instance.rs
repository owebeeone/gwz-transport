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
//! proof). The pool knows no scope, so it may offer a waiting opening an idle connection of another scope: the lease is
//! given back untouched (`Reusable`, never retired), and the opening asks once more with a `fresh` request, which opens
//! a connection of its own under the opening's own tag and so its own scope, within the allocation deadline it was given
//! at submit. A conforming opening therefore misfits at most once; more than `per_user_host + 1` misfits can only mean
//! a broken scope record, and the opening then fails by name (`OpenFailure::NoFittingConnection`).
use crate::{
    AuthContext, AuthScope, Clock, Completion, ConnectCommand, EndpointAdapter, EndpointProfile,
    ExchangeEnd, ExchangeIo, ExchangeSpec, OpenFailure,
    cell::{Cell, Slot},
    pump::Signal,
};
use gwz_transport::pool::{
    Action, Config, ConnectionId, Error, LeaseId, PoolMachine, Request, RequestId,
};
use gwz_transport::protocol::Disposition;
use gwz_transport::protocol::Scheme;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Wake, Waker},
};

/// What an opening's slot receives: the started exchange, its lease and whether its connection had served an exchange
/// before, or why there is none.
pub(crate) type Opened = Result<(Box<dyn ExchangeIo>, LeaseId, bool), OpenFailure>;
/// An opening whose request waits for a lease.
struct Pending {
    request: RequestId,
    /// The request as submitted (its tag included), to ask again after a lease of another scope.
    template: Request,
    spec: ExchangeSpec,
    /// The operation's context, lent to the connect the pool starts for this request and to the exchange.
    auth: AuthContext,
    /// Leases of another scope this opening was offered and gave back.
    misfits: usize,
    /// The opening's absolute allocation deadline on the pool's clock, fixed at submit: a resubmission never extends it.
    deadline: u64,
    slot: Arc<Slot<Opened>>,
}
/// A connection the pool has started and not yet disposed of: the scope its setup used (none when its opener had gone)
/// and whether an exchange has run on it.
struct Setup {
    scope: Option<AuthScope>,
    /// An HTTPS connection, the scheme the scope partitions.
    https: bool,
    served: bool,
}
/// A finished exchange waiting for its adapter to confirm how it ended.
struct Settling {
    io: Box<dyn ExchangeIo>,
    lease: LeaseId,
    end: Cell<ExchangeEnd>,
}
pub(crate) struct InstanceState {
    pub(crate) adapter: Option<Box<dyn EndpointAdapter>>,
    pub(crate) pool: PoolMachine,
    openings: Vec<Pending>,
    settling: Vec<Settling>,
    connections: BTreeMap<ConnectionId, Setup>,
    /// Leases of another scope the instance has offered and refused, over its life (an observation, never a limit).
    misfits: u64,
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
pub(crate) struct Instance {
    pub(crate) profile: EndpointProfile,
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
        let config = Config::default();
        Ok(Arc::new(Self {
            profile,
            clock,
            allocation_ms: config.allocation_timeout_ms,
            wakes: Arc::new(Wakes {
                waiters: Mutex::new(Vec::new()),
                driver,
            }),
            state: Mutex::new(InstanceState {
                adapter: Some(adapter),
                pool: PoolMachine::new(config)?,
                openings: Vec::new(),
                settling: Vec::new(),
                connections: BTreeMap::new(),
                misfits: 0,
            }),
        }))
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
    /// The pool's next deadline on the host's clock, which the driver sleeps until.
    pub(crate) fn next_deadline(&self) -> Option<u64> {
        self.lock().pool.next_deadline()
    }
    /// One pass of the driver: everything the adapter reported, the pool's actions and the in-flight work.
    pub(crate) fn drive(&self) {
        {
            let mut state = self.lock();
            self.pump(&mut state);
        }
        self.wake_waiters();
    }
    /// Carries the pool's actions to the adapter, the adapter's completions to the pool, leases to waiting openings
    /// and confirmed ends to the pool, until none of them has more.
    pub(crate) fn pump(&self, state: &mut InstanceState) {
        state.pool.advance(self.clock.now().get());
        let waker = Waker::from(self.wakes.clone());
        let mut cx = Context::from_waker(&waker);
        loop {
            let mut progressed = actions(state);
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
                    };
                }
                for connection in ended {
                    state.connections.remove(&connection);
                }
            }
            progressed |= openings(state);
            progressed |= settle(state, &mut cx);
            if !progressed {
                break;
            }
        }
    }
    /// Queues an opening's request and pumps once, so that an exchange the pool can lease at once is started now.
    /// `request` carries the opening's tag, by which a connect started for it finds `auth`.
    pub(crate) fn submit(
        &self,
        request: Request,
        spec: ExchangeSpec,
        auth: AuthContext,
    ) -> Result<Arc<Slot<Opened>>, Error> {
        let slot = Slot::new();
        {
            let mut state = self.lock();
            // The pool's clock is brought to now first, so the deadline recorded here is the one the pool gives the
            // request, and an idle connection whose time is up is not offered to it.
            let now = self.clock.now().get();
            state.pool.advance(now);
            let deadline =
                now.saturating_add(request.allocation_timeout_ms.unwrap_or(self.allocation_ms));
            let template = request.clone();
            let request = state.pool.request(request)?;
            state.openings.push(Pending {
                request,
                template,
                spec,
                auth,
                misfits: 0,
                deadline,
                slot: slot.clone(),
            });
            self.pump(&mut state);
        }
        self.wake_waiters();
        Ok(slot)
    }
    /// Hands a finished exchange to the pump, which releases its lease once the adapter confirms how it ended.
    pub(crate) fn settle(&self, io: Box<dyn ExchangeIo>, lease: LeaseId) -> Cell<ExchangeEnd> {
        let end = Cell::new();
        {
            let mut state = self.lock();
            state.settling.push(Settling {
                io,
                lease,
                end: end.clone(),
            });
            self.pump(&mut state);
        }
        self.wake_waiters();
        end
    }
    /// Gives a lease back and lets the pool act on it.
    pub(crate) fn release(&self, lease: LeaseId, disposition: Disposition) {
        {
            let mut state = self.lock();
            let _ = state.pool.release(lease, disposition);
            self.pump(&mut state);
        }
        self.wake_waiters();
    }
    /// An opening went before its exchange: its request and its context leave the instance, and an exchange already
    /// handed to it is aborted and its lease discarded.
    /// The opening is found by its slot, so the request abandoned is its current one (a misfit resubmits it).
    pub(crate) fn abandon(&self, slot: &Arc<Slot<Opened>>) {
        let handed = slot.abandon();
        {
            let mut state = self.lock();
            if let Some(index) = state
                .openings
                .iter()
                .position(|pending| Arc::ptr_eq(&pending.slot, slot))
            {
                let pending = state.openings.remove(index);
                state.pool.abandon(pending.request);
            }
            if let Some(Ok((mut io, lease, _))) = handed {
                io.abort();
                let _ = state.pool.release(lease, Disposition::Discarded);
            }
            self.pump(&mut state);
        }
        self.wake_waiters();
    }
    /// The host is sealed: what is in flight ends now. Openings get `Closing(reason)` from their own check; their
    /// slots are failed here so that nothing waits on a pump that has stopped. Settling exchanges are aborted and
    /// reported discarded.
    pub(crate) fn fail_in_flight(&self, reason: crate::operation::TerminalReason) {
        let (openings, settling) = {
            let mut state = self.lock();
            let openings = std::mem::take(&mut state.openings);
            let settling = std::mem::take(&mut state.settling);
            for item in &settling {
                let _ = state.pool.release(item.lease, Disposition::Discarded);
            }
            (openings, settling)
        };
        for pending in openings {
            let _ = pending.slot.fill(Err(OpenFailure::Closing(reason)));
        }
        for mut item in settling {
            item.io.abort();
            item.end.set(ExchangeEnd {
                disposition: Disposition::Discarded,
                cleanup: crate::CleanupStatus::Complete,
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
fn openings(state: &mut InstanceState) -> bool {
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
                let fits = match state.connections.get_mut(&lease.connection()) {
                    Some(setup) if !setup.https || setup.scope == Some(scope) => {
                        Some(std::mem::replace(&mut setup.served, true))
                    }
                    _ => None,
                };
                let Some(reused) = fits else {
                    // Set up under another operation's context: never handed over. It goes back untouched, for its own
                    // scope, and the opening asks once more for a connection of its own, by its original deadline.
                    progressed = true;
                    state.misfits += 1;
                    let _ = state.pool.release(lease, Disposition::Reusable);
                    let bound = state.pool.capacity().per_user_host + 1;
                    let pending = &mut state.openings[index];
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
                };
                let pending = &state.openings[index];
                let started = match state.adapter.as_mut() {
                    Some(adapter) => {
                        adapter.start_exchange(lease.connection(), &pending.spec, &pending.auth)
                    }
                    None => Err(std::io::Error::other("endpoint instance is closed")),
                };
                match started {
                    Ok(io) => Ok((io, lease, reused)),
                    Err(error) => {
                        let _ = state.pool.release(lease, Disposition::Discarded);
                        Err(OpenFailure::Io(error))
                    }
                }
            }
        };
        progressed = true;
        let pending = state.openings.remove(index);
        if let Err(Ok((mut io, lease, _))) = pending.slot.fill(outcome) {
            // The opening went while its exchange was being started.
            io.abort();
            let _ = state.pool.release(lease, Disposition::Discarded);
        }
    }
    progressed
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
        let _ = state.pool.release(item.lease, disposition);
        item.end.set(ExchangeEnd {
            disposition,
            cleanup: crate::CleanupStatus::Complete,
        });
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
