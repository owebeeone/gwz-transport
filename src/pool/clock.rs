use super::{machine::*, *};

impl PoolMachine {
    /// Monotonic endpoint time in milliseconds. The host must service deadlines
    /// even when there are no incoming requests. Backward ticks are ignored.
    pub fn advance(&mut self, now_ms: u64) {
        if now_ms < self.now {
            return;
        }
        self.now = now_ms;
        let expired: Vec<_> = self
            .requests
            .iter()
            .filter_map(|(id, pending)| {
                let error = match pending.state {
                    RequestState::Waiting if self.now >= pending.deadline => {
                        Some(Error::AllocationTimeout)
                    }
                    RequestState::Opening(connection) => match &self.entries[&connection].state {
                        State::Opening { clock: None, .. } if self.now >= pending.deadline => {
                            Some(Error::AllocationTimeout)
                        }
                        State::Opening {
                            clock: Some(clock),
                            cancel: None,
                            ..
                        } if self.now >= clock.deadline() => Some(match clock {
                            ConnectClock::Network(_) => Error::ConnectTimeout,
                            ConnectClock::Interaction { .. } => Error::InteractionTimeout,
                        }),
                        _ => None,
                    },
                    _ => None,
                };
                error.map(|error| (*id, error))
            })
            .collect();
        for (id, error) in expired {
            let _ = self.fail_request(id, error, CloseReason::Cancelled);
        }
        let idle: Vec<_> = self.entries.iter().filter_map(|(id, entry)| {
            matches!(entry.state, State::Idle { since } if self.now >= since.saturating_add(self.config.idle_timeout_ms)).then_some(*id)
        }).collect();
        for id in idle {
            self.start_closing(id, CloseReason::IdleExpired);
        }
        self.schedule();
        // Time may make cleanup actions runnable even without a state transition.
        self.touch();
    }
    pub fn next_deadline(&self) -> Option<u64> {
        let queue = self
            .requests
            .values()
            .filter_map(|pending| match pending.state {
                RequestState::Waiting => Some(pending.deadline),
                RequestState::Opening(id)
                    if matches!(self.entries[&id].state, State::Opening { clock: None, .. }) =>
                {
                    Some(pending.deadline)
                }
                _ => None,
            });
        let resources = self
            .entries
            .values()
            .filter_map(|entry| match &entry.state {
                State::Idle { since } => Some(since.saturating_add(self.config.idle_timeout_ms)),
                State::Opening {
                    cancel: Some(cleanup),
                    ..
                }
                | State::Closing { cleanup, .. }
                    if !cleanup.aborted =>
                {
                    Some(cleanup.deadline)
                }
                State::Opening {
                    clock: Some(clock),
                    cancel: None,
                    ..
                } => Some(clock.deadline()),
                _ => None,
            });
        queue.chain(resources).min()
    }
    /// Pause only the network-connect budget for a bounded helper interaction.
    /// Repeated interactions share the original total interaction allowance.
    pub fn begin_interaction(&mut self, connection: ConnectionId) -> Result<(), Error> {
        let entry = self.entries.get_mut(&connection).ok_or(Error::Stale)?;
        let State::Opening {
            clock: Some(clock @ ConnectClock::Network(_)),
            cancel: None,
            interaction_ms,
            ..
        } = &mut entry.state
        else {
            return Err(Error::WrongState);
        };
        let remaining = clock.deadline().saturating_sub(self.now);
        if remaining == 0 {
            return Err(Error::ConnectTimeout);
        }
        if *interaction_ms == 0 {
            return Err(Error::InteractionTimeout);
        }
        *clock = ConnectClock::Interaction {
            until: self.now.saturating_add(*interaction_ms),
            remaining,
        };
        self.touch();
        Ok(())
    }
    pub fn end_interaction(&mut self, connection: ConnectionId) -> Result<(), Error> {
        let entry = self.entries.get_mut(&connection).ok_or(Error::Stale)?;
        let State::Opening {
            clock: Some(clock @ ConnectClock::Interaction { .. }),
            cancel: None,
            interaction_ms,
            ..
        } = &mut entry.state
        else {
            return Err(Error::WrongState);
        };
        let ConnectClock::Interaction { until, remaining } = *clock else {
            unreachable!()
        };
        if self.now >= until {
            return Err(Error::InteractionTimeout);
        }
        *interaction_ms = until - self.now;
        *clock = ConnectClock::Network(self.now.saturating_add(remaining));
        self.touch();
        Ok(())
    }
}
