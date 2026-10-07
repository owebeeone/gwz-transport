use super::*;

#[derive(Clone, Copy)]
pub(super) enum Timer {
    Disabled,
    Inactive,
    Live(u64),
}
impl Timer {
    pub fn deadline(self) -> Option<u64> {
        if let Self::Live(value) = self {
            Some(value)
        } else {
            None
        }
    }
    pub fn pause(&mut self, now: u64) {
        if let Self::Live(until) = self {
            *until -= now;
        }
    }
    pub fn resume(&mut self, now: u64) -> Result<(), PublicationError> {
        if let Self::Live(remaining) = self {
            *remaining = now
                .checked_add(*remaining)
                .ok_or(PublicationError::Overflow)?;
        }
        Ok(())
    }
}
pub(super) enum Slot {
    Prepared {
        phase: PhaseId,
        kind: LocalPhase,
        until: u64,
        expired: bool,
    },
    Pending(PhaseId),
}
impl Slot {
    pub fn prepared(&self, id: PhaseId) -> bool {
        matches!(self, Self::Prepared { phase, .. } if *phase == id)
    }
}
pub(super) struct State {
    pub connection: ConnectionId,
    pub now: u64,
    pub serial: u64,
    pub phase: PhaseId,
    pub local: Option<LocalPhase>,
    pub local_until: Option<u64>,
    pub aggregate: Timer,
    pub stall: Timer,
    pub stall_ms: u64,
    pub slot: Option<Slot>,
    pub acknowledged: bool,
    pub terminal: Option<SetupTerminal>,
    pub driver: Option<Arc<Waker>>,
    pub waiter: Option<Arc<Waker>>,
    pub wakes: Vec<Arc<Waker>>,
    pub retired: Vec<Arc<Waker>>,
}
impl State {
    pub fn new(connection: ConnectionId, network_until: Option<u64>, stall_ms: u64) -> Self {
        Self {
            connection,
            now: 0,
            serial: 0,
            phase: PhaseId(0),
            local: None,
            local_until: None,
            aggregate: network_until.map_or(Timer::Disabled, Timer::Live),
            stall: if stall_ms == 0 {
                Timer::Disabled
            } else {
                Timer::Inactive
            },
            stall_ms,
            slot: None,
            acknowledged: true,
            terminal: None,
            driver: None,
            waiter: None,
            wakes: Vec::new(),
            retired: Vec::new(),
        }
    }
    pub fn issue(&mut self) -> Result<PhaseId, PublicationError> {
        self.serial = self
            .serial
            .checked_add(1)
            .ok_or(PublicationError::Overflow)?;
        Ok(PhaseId(self.serial))
    }
    pub fn alive(&self) -> Result<(), PublicationError> {
        self.terminal.map_or(Ok(()), |record| {
            Err(PublicationError::ActiveTerminal(record))
        })
    }
    pub fn deadline(&self) -> Option<u64> {
        if self.local.is_some() {
            self.local_until
        } else {
            self.aggregate
                .deadline()
                .into_iter()
                .chain(self.stall.deadline())
                .min()
        }
    }
    pub fn observation(&self) -> Observation {
        self.terminal.map_or(
            Observation::Alive {
                phase: self.phase,
                local: self.local,
                deadline: self.deadline(),
                acknowledged: self.acknowledged,
            },
            Observation::Terminal,
        )
    }
    pub fn settle(&mut self) {
        if self.terminal.is_some() {
            return;
        }
        let cause = if self.local.is_some() {
            let phase = self.local;
            self.local_until
                .filter(|until| *until <= self.now)
                .map(|_| {
                    if phase == Some(LocalPhase::Wait) {
                        SetupCause::LocalWaitExpired
                    } else {
                        SetupCause::LocalDeadline
                    }
                })
        } else {
            let aggregate = self.aggregate.deadline().filter(|until| *until <= self.now);
            let stall = self.stall.deadline().filter(|until| *until <= self.now);
            match (aggregate, stall) {
                (Some(a), Some(s)) if s < a => Some(SetupCause::NetworkStall),
                (Some(_), _) => Some(SetupCause::NetworkAggregate),
                (_, Some(_)) => Some(SetupCause::NetworkStall),
                _ => None,
            }
        };
        if let Some(cause) = cause {
            self.finish(cause, self.phase);
        }
        if let Some(Slot::Prepared { until, expired, .. }) = &mut self.slot {
            *expired |= *until <= self.now;
        }
    }
    pub fn finish(&mut self, cause: SetupCause, phase: PhaseId) {
        if self.terminal.is_none() {
            self.terminal = Some(SetupTerminal {
                connection: self.connection,
                phase,
                cause,
            });
            self.slot = None;
            if let Some(waker) = self.waiter.take() {
                self.wakes.push(waker);
            }
            if let Some(waker) = self.driver.take() {
                self.wakes.push(waker);
            }
        }
    }
    pub fn notify_driver(&mut self) {
        if let Some(waker) = &self.driver {
            self.wakes.push(waker.clone());
        }
    }
}
