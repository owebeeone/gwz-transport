//! The host's driver: the executor-run pump that advances every endpoint instance's pending work independently of
//! application calls (contract sections 4.1 and 6: `Progress` "executes the host's bounded driver work and timers
//! independently of application calls"; plan step 1.3b).
//!
//! **What runs it.** One task per host, a `DriverWork` future spawned on the host's own [`Progress`] port (the
//! composition's executor; a stepped fake in this crate's tests) the first time an endpoint instance attaches, so a
//! host that never attaches spawns nothing. The task holds the host only weakly. Each pass drives every instance
//! (`Instance::drive`: the adapter's completions into the pool, the pool's actions to the adapter, leases to waiting
//! openings, confirmed ends of finished exchanges back to the pool) and then sleeps on the host's [`Clock`] until the
//! earliest pool deadline, or until an adapter wakes the waker it was given, which is this task's [`Signal`]. There
//! is no global, thread local or ambient executor: the signal is owned by the host and the stop authority by its
//! owner.
//!
//! **What stops it.** The [`HostOwner`](crate::HostOwner) holds a [`Driver`], the one authority that stops the task.
//! `HostOwner::close`, dropping the owner, and the cleanup domain's revocation all seal the host (`HostShared::seal`),
//! which stops the signal: the task's next pass ends it, and in-flight openings end with `OpenFailure::Closing`
//! naming the reason. The pump never holds the cleanup domain: a domain close reaches it only through that seal, and
//! the disposal of a sealed host's adapters is the close job's (or the domain retainer's), not the pump's.
//!
//! **If the executor drops it.** A driver task dropped before it was stopped (its executor went away) is a host
//! progress fault (contract section 8): the host is sealed with `HostFault`, its bindings fail, and its disposal goes
//! on under the close job and the domain; nothing is reported complete that was not.
use crate::{host::HostShared, ports::Sleep};
use std::{
    future::poll_fn,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
};

struct SignalState {
    waker: Option<Waker>,
    /// Something happened since the task last looked.
    dirty: bool,
    stopped: bool,
    spawned: bool,
}
/// How adapters and the host reach the driver task: a notification, and the stop.
pub(crate) struct Signal {
    state: Mutex<SignalState>,
}
impl Signal {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(SignalState {
                waker: None,
                dirty: false,
                stopped: false,
                spawned: false,
            }),
        })
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, SignalState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// There is work for the driver: wake it.
    pub(crate) fn notify(&self) {
        let waker = {
            let mut state = self.lock();
            state.dirty = true;
            state.waker.clone()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    /// The driver ends at its next pass. Idempotent.
    pub(crate) fn stop(&self) {
        let waker = {
            let mut state = self.lock();
            state.stopped = true;
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    pub(crate) fn stopped(&self) -> bool {
        self.lock().stopped
    }
    /// Whether the task should be spawned now: the first caller after build gets `true`, once.
    fn claim_spawn(&self) -> bool {
        let mut state = self.lock();
        if state.spawned || state.stopped {
            return false;
        }
        state.spawned = true;
        true
    }
    /// Starts a pass: keeps the task's waker and clears the notification. `false` once stopped.
    fn begin(&self, waker: &Waker) -> bool {
        let mut state = self.lock();
        if state.stopped {
            return false;
        }
        state.dirty = false;
        if !state.waker.as_ref().is_some_and(|w| w.will_wake(waker)) {
            state.waker = Some(waker.clone());
        }
        true
    }
    fn take_dirty(&self) -> bool {
        std::mem::take(&mut self.lock().dirty)
    }
}

/// The host owner's authority over its driver: stopping it. Built with the host; the task itself is spawned on the
/// first attach.
pub(crate) struct Driver {
    signal: Arc<Signal>,
}
impl Driver {
    pub(crate) fn new(signal: Arc<Signal>) -> Self {
        Self { signal }
    }
    pub(crate) fn stop(&self) {
        self.signal.stop();
    }
}

/// Spawns the host's driver task on its `Progress`, once. An executor that refuses is a host progress fault.
pub(crate) fn ensure_running(host: &Arc<HostShared>) -> Result<(), crate::ProgressUnavailable> {
    let signal = host.signal.clone();
    if !signal.claim_spawn() {
        return if signal.stopped() {
            Err(crate::ProgressUnavailable)
        } else {
            Ok(())
        };
    }
    let guard = Running {
        host: Arc::downgrade(host),
        signal,
    };
    host.progress
        .spawn(Box::pin(drive(guard)))
        .inspect_err(|_| host.fault())
}

/// The task's hold on its host. Dropped unstopped, it faults the host (see the module doc).
struct Running {
    host: Weak<HostShared>,
    signal: Arc<Signal>,
}
impl Drop for Running {
    fn drop(&mut self) {
        if !self.signal.stopped()
            && let Some(host) = self.host.upgrade()
        {
            host.fault();
        }
    }
}

async fn drive(running: Running) {
    let mut sleep: Option<(u64, Sleep)> = None;
    poll_fn(|cx: &mut Context<'_>| {
        loop {
            if !running.signal.begin(cx.waker()) {
                return Poll::Ready(());
            }
            let Some(host) = running.host.upgrade() else {
                return Poll::Ready(());
            };
            let instances = host.instances();
            let mut next: Option<u64> = None;
            for instance in &instances {
                instance.drive();
                if let Some(deadline) = instance.next_deadline() {
                    next = Some(next.map_or(deadline, |at| at.min(deadline)));
                }
            }
            match next {
                Some(deadline) => {
                    if sleep.as_ref().is_none_or(|(at, _)| *at != deadline) {
                        sleep =
                            Some((deadline, host.clock.sleep_until(crate::Tick::new(deadline))));
                    }
                    if let Some((_, wait)) = sleep.as_mut()
                        && wait.as_mut().poll(cx).is_ready()
                    {
                        sleep = None;
                        continue;
                    }
                }
                None => sleep = None,
            }
            drop(host);
            if running.signal.take_dirty() {
                continue;
            }
            return Poll::Pending;
        }
    })
    .await;
    // Ended by its stop, or by its host's end: not a fault.
    running.signal.stop();
}
