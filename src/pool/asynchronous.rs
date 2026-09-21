//! Futures and RAII ownership; the host supplies all physical I/O and clock ticks.
use super::*;
use crate::protocol::{Disposition, Failure};
use std::{
    collections::BTreeMap,
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

struct Runtime {
    machine: PoolMachine,
    waiters: BTreeMap<RequestId, Waker>,
    driver: Option<Waker>,
}
struct Shared {
    runtime: Mutex<Runtime>,
    owners: AtomicUsize,
}
impl Shared {
    fn change<T>(&self, action: impl FnOnce(&mut Runtime) -> T) -> T {
        let (result, wakes) = {
            let mut state = self.runtime.lock().expect("pool lock poisoned");
            let before = state.machine.revision;
            let result = action(&mut state);
            let wakes: Vec<_> = if before != state.machine.revision {
                state
                    .waiters
                    .values()
                    .chain(state.driver.iter())
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            };
            (result, wakes)
        };
        for waker in wakes {
            waker.wake();
        }
        result
    }
}

/// Clones share one endpoint's limits and connections. Dropping the final Pool
/// initiates shutdown, even if outstanding checkouts/leases still exist.
pub struct Pool {
    shared: Arc<Shared>,
}
/// Single host dispatcher. Drain actions, execute them outside the pool, and
/// acknowledge completion. Service next_deadline even without incoming work.
/// Before dropping this driver the host must dispose all physical resources and
/// cancel its connectors; driver loss invalidates clients, but cannot do I/O.
pub struct PoolDriver {
    shared: Arc<Shared>,
}
#[must_use = "dropping a checkout cancels the allocation"]
pub struct Checkout {
    shared: Arc<Shared>,
    request: Option<RequestId>,
}
/// Exclusive physical-connection allocation. Cloned streams may share this
/// lease in their owner; the lease itself cannot be cloned. Drop always discards.
#[must_use = "dropping a lease discards its connection"]
pub struct Lease {
    shared: Arc<Shared>,
    token: Option<LeaseId>,
}

impl Pool {
    pub fn new(config: Config) -> Result<(Self, PoolDriver), Error> {
        let shared = Arc::new(Shared {
            runtime: Mutex::new(Runtime {
                machine: PoolMachine::new(config)?,
                waiters: BTreeMap::new(),
                driver: None,
            }),
            owners: AtomicUsize::new(1),
        });
        Ok((
            Self {
                shared: shared.clone(),
            },
            PoolDriver { shared },
        ))
    }
    /// Begins allocation immediately; an unpolled future still owns a bounded
    /// request slot and cancels it when dropped.
    pub fn checkout(&self, request: Request) -> Result<Checkout, Error> {
        self.checkout_until(request, None)
    }
    pub fn checkout_until(
        &self,
        request: Request,
        absolute_deadline: Option<u64>,
    ) -> Result<Checkout, Error> {
        let request = self
            .shared
            .change(|state| state.machine.request_until(request, absolute_deadline))?;
        Ok(Checkout {
            shared: self.shared.clone(),
            request: Some(request),
        })
    }
    pub fn counts(&self) -> Counts {
        self.shared.change(|state| state.machine.counts())
    }
    pub fn outstanding_requests(&self) -> usize {
        self.shared
            .change(|state| state.machine.outstanding_requests())
    }
    pub fn shutdown(&self) {
        self.shared.change(|state| state.machine.shutdown());
    }
    pub fn cancel_session(&self, session: &str) {
        self.shared
            .change(|state| state.machine.cancel_session(session));
    }
    pub fn cancel_operation(&self, owner: &Owner) {
        self.shared
            .change(|state| state.machine.cancel_operation(owner));
    }
}
impl Clone for Pool {
    fn clone(&self) -> Self {
        self.shared.owners.fetch_add(1, Ordering::Relaxed);
        Self {
            shared: self.shared.clone(),
        }
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        if self.shared.owners.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.shutdown();
        }
    }
}
impl Future for Checkout {
    type Output = Result<Lease, Error>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let Some(id) = self.request else {
            return Poll::Ready(Err(Error::Stale));
        };
        let (result, retired) = self.shared.change(|state| {
            let result = state.machine.take(id);
            let retired = if result == Err(Error::WouldBlock) {
                state.waiters.insert(id, cx.waker().clone())
            } else {
                state.waiters.remove(&id)
            };
            (result, retired)
        });
        // Custom waker destructors, like wake callbacks, run outside the mutex.
        drop(retired);
        match result {
            Err(Error::WouldBlock) => Poll::Pending,
            result => {
                self.request = None;
                Poll::Ready(result.map(|token| Lease {
                    shared: self.shared.clone(),
                    token: Some(token),
                }))
            }
        }
    }
}
impl Drop for Checkout {
    fn drop(&mut self) {
        if let Some(id) = self.request.take() {
            let retired = self.shared.change(|state| {
                let retired = state.waiters.remove(&id);
                state.machine.abandon(id);
                retired
            });
            drop(retired);
        }
    }
}
impl Lease {
    pub fn connection(&self) -> Result<ConnectionId, Error> {
        let token = self.token.ok_or(Error::Stale)?;
        self.shared.change(|state| {
            if state.machine.is_live(token) {
                Ok(token.connection())
            } else {
                Err(Error::Stale)
            }
        })
    }
    /// Only use Reusable after successful exchange cleanup. This consumes the
    /// lease so no later Drop can return a newly allocated lease accidentally.
    pub fn release(mut self, disposition: Disposition) -> Result<(), Error> {
        let token = self.token.take().ok_or(Error::Stale)?;
        self.shared
            .change(|state| state.machine.release(token, disposition))
    }
}
impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(token) = self.token.take() {
            let _ = self
                .shared
                .change(|state| state.machine.release(token, Disposition::Discarded));
        }
    }
}

struct DriverWaiter {
    shared: Arc<Shared>,
}
impl Drop for DriverWaiter {
    fn drop(&mut self) {
        let retired = self.shared.change(|state| state.driver.take());
        drop(retired);
    }
}
impl PoolDriver {
    /// Exactly one command receiver, enforced by mutable borrowing. Cancelling
    /// this future unregisters its waker. The driver slot is reserved separately
    /// from request capacity, so a full queue cannot prevent pool progress.
    pub async fn next_action(&mut self) -> Option<Action> {
        let _waiter = DriverWaiter {
            shared: self.shared.clone(),
        };
        poll_fn(|cx| {
            let (result, retired) = self.shared.change(|state| {
                let result = if let Some(action) = state.machine.next_action() {
                    Poll::Ready(Some(action))
                } else if state.machine.shutdown_complete() {
                    Poll::Ready(None)
                } else {
                    Poll::Pending
                };
                let retired = if result.is_pending() {
                    state.driver.replace(cx.waker().clone())
                } else {
                    state.driver.take()
                };
                (result, retired)
            });
            drop(retired);
            result
        })
        .await
    }
    pub fn connected(
        &self,
        connection: ConnectionId,
        result: Result<Option<Identity>, Failure>,
    ) -> Result<(), Error> {
        self.shared
            .change(|state| state.machine.connected(connection, result))
    }
    /// Acknowledge spontaneous disposal; see PoolMachine::idle_closed for races.
    pub fn idle_closed(&self, connection: ConnectionId) -> Result<(), Error> {
        self.shared
            .change(|state| state.machine.idle_closed(connection))
    }
    pub fn closed(&self, connection: ConnectionId) -> Result<(), Error> {
        self.shared.change(|state| state.machine.closed(connection))
    }
    pub fn advance(&self, now_ms: u64) {
        self.shared.change(|state| state.machine.advance(now_ms));
    }
    pub fn next_deadline(&self) -> Option<u64> {
        self.shared.change(|state| state.machine.next_deadline())
    }
    pub fn begin_interaction(&self, connection: ConnectionId) -> Result<(), Error> {
        self.shared
            .change(|state| state.machine.begin_interaction(connection))
    }
    pub fn end_interaction(&self, connection: ConnectionId) -> Result<(), Error> {
        self.shared
            .change(|state| state.machine.end_interaction(connection))
    }
    pub fn shutdown_complete(&self) -> bool {
        self.shared
            .change(|state| state.machine.shutdown_complete())
    }
}
impl Drop for PoolDriver {
    fn drop(&mut self) {
        self.shared.change(|state| state.machine.driver_lost());
    }
}
