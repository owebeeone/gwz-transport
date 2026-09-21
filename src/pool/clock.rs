use super::{machine::*, *};

impl PoolMachine {
    /// Monotonic endpoint time in milliseconds. The host must service deadlines
    /// even when there are no incoming requests. Backward ticks are ignored.
    pub fn advance(&mut self, now_ms: u64) {
        if now_ms < self.now {
            return;
        }
        let previous_now = self.now;
        let before_revision = self.revision;
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
                        } => clock
                            .deadline()
                            .filter(|deadline| self.now >= *deadline)
                            .map(|_| match clock {
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
        // A sent cleanup command becomes abortable at its deadline. Preserve
        // that wakeup without waking on ordinary clock bookkeeping.
        if self.revision == before_revision
            && previous_now < self.now
            && self.cleanup_deadline_crossed(previous_now, self.now)
        {
            self.touch();
        }
    }
    fn cleanup_deadline_crossed(&self, previous: u64, now: u64) -> bool {
        self.entries.values().any(|entry| {
            let cleanup = match &entry.state {
                State::Opening {
                    cancel: Some(cleanup),
                    ..
                }
                | State::Closing { cleanup, .. } => cleanup,
                _ => return false,
            };
            cleanup.sent
                && !cleanup.aborted
                && previous < cleanup.deadline
                && now >= cleanup.deadline
        })
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
                } => clock.deadline(),
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
        let remaining = clock
            .deadline()
            .map(|deadline| deadline.saturating_sub(self.now));
        if remaining == Some(0) {
            return Err(Error::ConnectTimeout);
        }
        if *interaction_ms == 0 {
            return Err(Error::InteractionTimeout);
        }
        *clock = ConnectClock::Interaction {
            until: self.now.saturating_add(*interaction_ms),
            remaining,
        };
        Ok(())
    }
    pub fn end_interaction(&mut self, connection: ConnectionId) -> Result<(), Error> {
        let absolute_deadline = self
            .entries
            .get(&connection)
            .and_then(|entry| match &entry.state {
                State::Opening { request, .. } => *request,
                _ => None,
            })
            .and_then(|id| self.requests.get(&id))
            .and_then(|pending| pending.absolute_deadline);
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
        let resumed = remaining.map(|remaining| self.now.saturating_add(remaining));
        let resumed = match (resumed, absolute_deadline) {
            (Some(network), Some(absolute)) => Some(network.min(absolute)),
            (None, Some(absolute)) => Some(absolute),
            (network, None) => network,
        };
        *clock = ConnectClock::Network(resumed);
        Ok(())
    }
}
