use super::{state::Timer, *};

impl SetupClock {
    pub fn prepare_local(
        &self,
        kind: LocalPhase,
        until: u64,
    ) -> ClockUpdate<Result<PreparedPhase, PublicationError>> {
        self.change(|s| {
            s.alive()?;
            if s.slot.is_some() {
                return Err(PublicationError::WrongState);
            }
            let phase = s.issue()?;
            s.slot = Some(Slot::Prepared {
                phase,
                kind,
                until,
                expired: until <= s.now,
            });
            Ok(PreparedPhase {
                clock: self.clone(),
                connection: s.connection,
                phase,
                consumed: false,
            })
        })
    }
    pub fn publish_local(
        &self,
        mut prepared: PreparedPhase,
    ) -> ClockUpdate<Result<SetupReceipt, PublicationError>> {
        let matching_clock = Arc::ptr_eq(&self.inner, &prepared.clock.inner);
        let update = self.change(|s| {
            s.alive()?;
            if !matching_clock || prepared.connection != s.connection {
                return Err(PublicationError::InvalidToken);
            }
            let Some(Slot::Prepared {
                phase,
                kind,
                until,
                expired,
            }) = s.slot
            else {
                return Err(PublicationError::InvalidToken);
            };
            if phase != prepared.phase {
                return Err(PublicationError::InvalidToken);
            }
            prepared.consumed = true;
            if expired || until <= s.now {
                s.finish(SetupCause::PreparationDeadline, phase);
                return Err(PublicationError::PreparationExpired(
                    s.terminal.expect("prepared terminal"),
                ));
            }
            if s.local.is_none() {
                s.aggregate.pause(s.now);
                s.stall.pause(s.now);
            }
            s.local = Some(kind);
            s.local_until = Some(until);
            s.phase = phase;
            s.slot = Some(Slot::Pending(phase));
            s.acknowledged = false;
            s.notify_driver();
            Ok(SetupReceipt {
                connection: s.connection,
                phase,
            })
        });
        // Foreign-token cleanup must occur outside either authority lock.
        drop(prepared);
        update
    }
    pub fn publish_network(&self) -> ClockUpdate<Result<SetupReceipt, PublicationError>> {
        self.change(|s| {
            s.alive()?;
            if s.slot.is_some() || s.local.is_none() {
                return Err(PublicationError::WrongState);
            }
            let phase = s.issue()?;
            let mut aggregate = s.aggregate;
            let mut stall = s.stall;
            aggregate.resume(s.now)?;
            stall.resume(s.now)?;
            s.aggregate = aggregate;
            s.stall = stall;
            s.local = None;
            s.local_until = None;
            s.phase = phase;
            s.slot = Some(Slot::Pending(phase));
            s.acknowledged = false;
            s.notify_driver();
            Ok(SetupReceipt {
                connection: s.connection,
                phase,
            })
        })
    }
    pub fn begin_network_wait(&self) -> ClockUpdate<Result<(), PublicationError>> {
        self.change(|s| {
            s.alive()?;
            if s.local.is_some() {
                return Err(PublicationError::WrongState);
            }
            if matches!(s.stall, Timer::Inactive) {
                s.stall = Timer::Live(
                    s.now
                        .checked_add(s.stall_ms)
                        .ok_or(PublicationError::Overflow)?,
                );
            }
            Ok(())
        })
    }
    pub fn network_progress(&self) -> ClockUpdate<Result<(), PublicationError>> {
        self.change(|s| {
            s.alive()?;
            if s.local.is_some() {
                return Err(PublicationError::WrongState);
            }
            if s.stall_ms != 0 {
                s.stall = Timer::Live(
                    s.now
                        .checked_add(s.stall_ms)
                        .ok_or(PublicationError::Overflow)?,
                );
            }
            Ok(())
        })
    }
}
