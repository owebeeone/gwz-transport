//! An endpoint instance: one adapter and the pool of its connections, owned by the host and leased to bindings.
//!
//! The pool is gwz-transport's state machine, which does no I/O: its actions go to the adapter and the adapter's
//! completions come back to it, here, in `pump`. The pump also carries the instance's in-flight work: openings whose
//! request waits for a lease (the lease is taken and the exchange started here, and handed to the opening's slot) and
//! finished exchanges whose adapter has not yet confirmed a clean end (settled here, then released). It runs when a
//! command arrives (a submission, a release, a cancellation) and on the host's driver (`pump.rs`) whenever the adapter
//! wakes the waker it was given or a pool deadline passes; never because an application polls.
use crate::{
    Clock, Completion, ConnectCommand, EndpointAdapter, EndpointProfile, ExchangeEnd, ExchangeIo,
    ExchangeSpec, OpenFailure,
    cell::{Cell, Slot},
    pump::Signal,
};
use gwz_transport::pool::{Action, Config, Error, LeaseId, PoolMachine, Request, RequestId};
use gwz_transport::protocol::Disposition;
use std::{
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Wake, Waker},
};

/// What an opening's slot receives: the started exchange and its lease, or why there is none.
pub(crate) type Opened = Result<(Box<dyn ExchangeIo>, LeaseId), OpenFailure>;
/// An opening whose request waits for a lease.
struct Pending {
    request: RequestId,
    spec: ExchangeSpec,
    slot: Arc<Slot<Opened>>,
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
        Ok(Arc::new(Self {
            profile,
            clock,
            wakes: Arc::new(Wakes {
                waiters: Mutex::new(Vec::new()),
                driver,
            }),
            state: Mutex::new(InstanceState {
                adapter: Some(adapter),
                pool: PoolMachine::new(Config::default())?,
                openings: Vec::new(),
                settling: Vec::new(),
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
                while let Poll::Ready(completion) = adapter.poll_completion(&mut cx) {
                    progressed = true;
                    // A stale or duplicate completion is refused by the pool and changes nothing.
                    let _ = match completion {
                        Completion::Connected { connection, result } => {
                            state.pool.connected(connection, result)
                        }
                        Completion::Closed { connection } => state.pool.closed(connection),
                        Completion::IdleClosed { connection } => state.pool.idle_closed(connection),
                    };
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
    pub(crate) fn submit(
        &self,
        request: Request,
        spec: ExchangeSpec,
    ) -> Result<(RequestId, Arc<Slot<Opened>>), Error> {
        let slot = Slot::new();
        let request = {
            let mut state = self.lock();
            let request = state.pool.request(request)?;
            state.openings.push(Pending {
                request,
                spec,
                slot: slot.clone(),
            });
            self.pump(&mut state);
            request
        };
        self.wake_waiters();
        Ok((request, slot))
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
    /// An opening went before its exchange: its request leaves the pool, and an exchange already handed to it is
    /// aborted and its lease discarded.
    pub(crate) fn abandon(&self, request: RequestId, slot: &Slot<Opened>) {
        let handed = slot.abandon();
        {
            let mut state = self.lock();
            state.openings.retain(|pending| pending.request != request);
            state.pool.abandon(request);
            if let Some(Ok((mut io, lease))) = handed {
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

/// The pool's actions, to the adapter. Whether there were any.
fn actions(state: &mut InstanceState) -> bool {
    let mut progressed = false;
    while let Some(action) = state.pool.next_action() {
        progressed = true;
        let Some(adapter) = state.adapter.as_mut() else {
            continue;
        };
        match action {
            Action::Connect {
                connection,
                key,
                identity,
                ..
            } => adapter.connect(ConnectCommand {
                connection,
                key,
                identity,
            }),
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
                let started = match state.adapter.as_mut() {
                    Some(adapter) => {
                        adapter.start_exchange(lease.connection(), &state.openings[index].spec)
                    }
                    None => Err(std::io::Error::other("endpoint instance is closed")),
                };
                match started {
                    Ok(io) => Ok((io, lease)),
                    Err(error) => {
                        let _ = state.pool.release(lease, Disposition::Discarded);
                        Err(OpenFailure::Io(error))
                    }
                }
            }
        };
        progressed = true;
        let pending = state.openings.remove(index);
        if let Err(Ok((mut io, lease))) = pending.slot.fill(outcome) {
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
