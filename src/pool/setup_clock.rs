//! One connection's setup arbitration. Notifications are delivered by owners
//! after releasing their outer locks; no callbacks run under this authority.
use super::ConnectionId;
use crate::protocol::{Effect, ErrorCode, SetupFailureCause};
use std::{
    future::poll_fn,
    sync::{Arc, Mutex},
    task::{Poll, Waker},
};

mod state;
mod transitions;
use state::{Slot, State};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhaseId(u64);
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LocalPhase {
    Admission,
    Interaction,
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SetupCause {
    NetworkAggregate,
    NetworkStall,
    LocalDeadline,
    PreparationDeadline,
    Cancelled,
    DriverLost,
    Completed,
    ResourceFailure {
        code: ErrorCode,
        effect: Effect,
        setup_cause: Option<SetupFailureCause>,
    },
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct SetupTerminal {
    pub connection: ConnectionId,
    pub phase: PhaseId,
    pub cause: SetupCause,
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Observation {
    Alive {
        phase: PhaseId,
        local: Option<LocalPhase>,
        deadline: Option<u64>,
        acknowledged: bool,
    },
    Terminal(SetupTerminal),
}
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum PublicationError {
    ActiveTerminal(SetupTerminal),
    PreparationExpired(SetupTerminal),
    InvalidToken,
    WrongState,
    Overflow,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SetupReceipt {
    connection: ConnectionId,
    phase: PhaseId,
}
impl SetupReceipt {
    pub fn phase(self) -> PhaseId {
        self.phase
    }
}

#[must_use = "deliver notifications only after releasing outer owner locks"]
pub struct ClockUpdate<T> {
    pub value: T,
    pub(super) wakes: Vec<Arc<Waker>>,
    retired: Vec<Arc<Waker>>,
}
impl<T> ClockUpdate<T> {
    pub(super) fn notifications(
        wakes: Vec<Arc<Waker>>,
        retired: Vec<Arc<Waker>>,
    ) -> ClockUpdate<()> {
        ClockUpdate {
            value: (),
            wakes,
            retired,
        }
    }
    pub fn deliver(self) -> T {
        drop(self.retired);
        for waker in self.wakes {
            waker.wake_by_ref();
        }
        self.value
    }
    pub(super) fn into_parts(self) -> (T, Vec<Arc<Waker>>, Vec<Arc<Waker>>) {
        (self.value, self.wakes, self.retired)
    }
}

struct Inner {
    source: Arc<dyn Fn() -> u64 + Send + Sync>,
    state: Mutex<State>,
}
struct ReceiptWaiter {
    clock: SetupClock,
    armed: bool,
}
impl Drop for ReceiptWaiter {
    fn drop(&mut self) {
        if self.armed {
            self.clock.terminate(SetupCause::Cancelled).deliver();
        }
    }
}
#[derive(Clone)]
pub struct SetupClock {
    inner: Arc<Inner>,
}
pub struct PreparedPhase {
    clock: SetupClock,
    connection: ConnectionId,
    phase: PhaseId,
    consumed: bool,
}
impl std::fmt::Debug for PreparedPhase {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("PreparedPhase")
            .field("connection", &self.connection)
            .field("phase", &self.phase)
            .finish()
    }
}
impl PreparedPhase {
    pub fn phase(&self) -> PhaseId {
        self.phase
    }
}
impl Drop for PreparedPhase {
    fn drop(&mut self) {
        if !self.consumed {
            self.clock
                .change(|state| {
                    if state
                        .slot
                        .as_ref()
                        .is_some_and(|slot| slot.prepared(self.phase))
                    {
                        state.slot = None;
                    }
                })
                .deliver();
        }
    }
}
impl SetupClock {
    pub(super) fn cached_deadline(&self) -> Option<u64> {
        let state = self.inner.state.lock().expect("setup clock lock poisoned");
        if state.terminal.is_some() {
            Some(state.now)
        } else {
            state.deadline()
        }
    }
    pub(super) fn new(
        connection: ConnectionId,
        source: Arc<dyn Fn() -> u64 + Send + Sync>,
        network_until: Option<u64>,
        stall_ms: u64,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                source,
                state: Mutex::new(State::new(connection, network_until, stall_ms)),
            }),
        }
    }
    fn change<T>(&self, action: impl FnOnce(&mut State) -> T) -> ClockUpdate<T> {
        let mut state = self.inner.state.lock().expect("setup clock lock poisoned");
        let now = (self.inner.source)().max(state.now);
        state.now = now;
        state.settle();
        let value = action(&mut state);
        ClockUpdate {
            value,
            wakes: std::mem::take(&mut state.wakes),
            retired: std::mem::take(&mut state.retired),
        }
    }
    pub fn connection(&self) -> ConnectionId {
        self.change(|s| s.connection).deliver()
    }
    pub fn deadline_after(&self, milliseconds: u64) -> ClockUpdate<Result<u64, PublicationError>> {
        self.change(|s| {
            s.alive()?;
            s.now
                .checked_add(milliseconds)
                .ok_or(PublicationError::Overflow)
        })
    }
    pub fn remaining(&self) -> ClockUpdate<Option<u64>> {
        self.change(|s| s.deadline().map(|deadline| deadline.saturating_sub(s.now)))
    }
    pub fn observe(&self) -> ClockUpdate<Observation> {
        self.change(|s| s.observation())
    }
    pub fn terminate(&self, cause: SetupCause) -> ClockUpdate<SetupTerminal> {
        self.change(|s| {
            s.finish(cause, s.phase);
            s.terminal.expect("terminal")
        })
    }
    /// Distinguish this caller's admission from an already committed terminal.
    pub fn terminate_if_alive(
        &self,
        cause: SetupCause,
    ) -> ClockUpdate<Result<SetupTerminal, SetupTerminal>> {
        self.change(|s| {
            if let Some(record) = s.terminal {
                return Err(record);
            }
            s.finish(cause, s.phase);
            Ok(s.terminal.expect("terminal"))
        })
    }
    pub fn register_driver(&self, waker: Arc<Waker>) -> ClockUpdate<()> {
        self.change(|s| {
            if s.terminal.is_some() {
                s.retired.push(waker);
                return;
            }
            if let Some(old) = s.driver.replace(waker) {
                s.retired.push(old);
            }
        })
    }
    pub fn pending_receipt(&self) -> ClockUpdate<Option<SetupReceipt>> {
        self.change(|s| match s.slot {
            Some(Slot::Pending(phase)) => Some(SetupReceipt {
                connection: s.connection,
                phase,
            }),
            _ => None,
        })
    }
    pub fn acknowledge(&self, receipt: SetupReceipt) -> ClockUpdate<Result<(), PublicationError>> {
        self.change(|s| {
            s.alive()?;
            if receipt.connection != s.connection
                || !matches!(s.slot, Some(Slot::Pending(phase)) if phase == receipt.phase)
            {
                return Err(PublicationError::InvalidToken);
            }
            s.slot = None;
            s.acknowledged = true;
            if let Some(waker) = s.waiter.take() {
                s.wakes.push(waker);
            }
            Ok(())
        })
    }
    pub async fn wait_acknowledged(&self, receipt: SetupReceipt) -> Result<(), PublicationError> {
        let mut waiter = ReceiptWaiter {
            clock: self.clone(),
            armed: false,
        };
        let result = poll_fn(|cx| {
            let registration = Arc::new(cx.waker().clone());
            self.change(|s| {
                if let Err(error) = s.alive() {
                    return Poll::Ready(Err(error));
                }
                if receipt.connection != s.connection || receipt.phase != s.phase {
                    return Poll::Ready(Err(PublicationError::InvalidToken));
                }
                if s.acknowledged {
                    return Poll::Ready(Ok(()));
                }
                waiter.armed = true;
                if let Some(old) = s.waiter.replace(registration) {
                    s.retired.push(old);
                }
                Poll::Pending
            })
            .deliver()
        })
        .await;
        waiter.armed = false;
        result
    }
    pub(super) fn admit_completed(&self) -> ClockUpdate<Result<SetupTerminal, PublicationError>> {
        self.change(|s| {
            s.alive()?;
            if s.local.is_some() || !s.acknowledged || s.slot.is_some() {
                return Err(PublicationError::WrongState);
            }
            s.finish(SetupCause::Completed, s.phase);
            Ok(s.terminal.expect("completed terminal"))
        })
    }
}
#[cfg(test)]
mod tests {
    include!("setup_clock/tests.rs");
}
