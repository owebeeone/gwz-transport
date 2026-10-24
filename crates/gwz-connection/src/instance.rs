//! An endpoint instance: one adapter and the pool of its connections, owned by the host and leased to bindings.
//!
//! The pool is gwz-transport's state machine, which does no I/O: its actions go to the adapter and the adapter's
//! completions come back to it, here, in `pump`. Pumping runs inside the callers' polls and releases in step 1.2; the
//! executor-driven pump arrives with the first adapter whose completions are asynchronous (step 1.3).
use crate::{Clock, Completion, ConnectCommand, EndpointAdapter, EndpointProfile};
use gwz_transport::pool::{Action, Config, LeaseId, PoolMachine, RequestId};
use gwz_transport::protocol::Disposition;
use std::{
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Wake, Waker},
};

pub(crate) struct InstanceState {
    pub(crate) adapter: Option<Box<dyn EndpointAdapter>>,
    pub(crate) pool: PoolMachine,
}
/// Wakes every opening that waits on this instance when its adapter has something to report.
struct Wakes {
    waiters: Mutex<Vec<Waker>>,
}
impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
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
    ) -> Result<Arc<Self>, gwz_transport::pool::Error> {
        Ok(Arc::new(Self {
            profile,
            clock,
            wakes: Arc::new(Wakes {
                waiters: Mutex::new(Vec::new()),
            }),
            state: Mutex::new(InstanceState {
                adapter: Some(adapter),
                pool: PoolMachine::new(Config::default())?,
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
        self.wakes.wake_by_ref();
    }
    /// Carries the pool's actions to the adapter and the adapter's completions to the pool until neither has more.
    pub(crate) fn pump(&self, state: &mut InstanceState) {
        state.pool.advance(self.clock.now().get());
        let waker = Waker::from(self.wakes.clone());
        let mut cx = Context::from_waker(&waker);
        loop {
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
                    Action::CancelConnect { connection, .. }
                    | Action::AbortConnect { connection } => adapter.cancel_connect(connection),
                    Action::Close { connection, .. } | Action::Abort { connection } => {
                        adapter.close(connection)
                    }
                }
            }
            if let Some(adapter) = state.adapter.as_mut() {
                while let Poll::Ready(completion) = adapter.poll_completion(&mut cx) {
                    progressed = true;
                    // A stale or duplicate completion is refused by the pool and changes nothing.
                    let _ = match completion {
                        Completion::Connected { connection, result } => {
                            state.pool.connected(connection, result)
                        }
                        Completion::Closed { connection } => state.pool.closed(connection),
                    };
                }
            }
            if !progressed {
                break;
            }
        }
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
    pub(crate) fn abandon(&self, request: RequestId) {
        {
            let mut state = self.lock();
            state.pool.abandon(request);
            self.pump(&mut state);
        }
        self.wake_waiters();
    }
}
