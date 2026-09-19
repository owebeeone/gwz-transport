use super::{machine::*, *};
use crate::protocol::Disposition;

impl PoolMachine {
    pub(super) fn start_closing(&mut self, connection: ConnectionId, reason: CloseReason) {
        let entry = self.entries.get_mut(&connection).expect("owned connection");
        if matches!(entry.state, State::Closing { .. }) {
            return;
        }
        let deadline = match &entry.state {
            State::Opening {
                cancel: Some(cleanup),
                ..
            } => cleanup.deadline,
            _ => self.now.saturating_add(self.config.cleanup_timeout_ms),
        };
        entry.owner = None;
        entry.state = State::Closing {
            reason,
            cleanup: Cleanup::new(deadline),
        };
        self.touch();
    }
    pub(super) fn fail_request(
        &mut self,
        id: RequestId,
        error: Error,
        reason: CloseReason,
    ) -> Result<(), Error> {
        let state = self.requests.get(&id).ok_or(Error::Stale)?.state;
        match state {
            RequestState::Failed(_) => {
                return Ok(());
            }
            RequestState::Waiting => {}
            RequestState::Ready(lease) => {
                self.start_closing(lease.connection, reason);
            }
            RequestState::Opening(connection) => {
                let entry = self
                    .entries
                    .get_mut(&connection)
                    .expect("opening reservation");
                if let State::Opening {
                    request,
                    clock,
                    cancel,
                    ..
                } = &mut entry.state
                {
                    if clock.is_none() {
                        self.entries.remove(&connection);
                    } else {
                        *request = None;
                        *cancel = Some(Cleanup::new(
                            self.now.saturating_add(self.config.cleanup_timeout_ms),
                        ));
                    }
                }
            }
        }
        self.requests.get_mut(&id).expect("owned request").state = RequestState::Failed(error);
        self.touch();
        Ok(())
    }
    pub fn cancel(&mut self, request: RequestId) -> Result<(), Error> {
        self.fail_request(request, Error::Cancelled, CloseReason::Cancelled)?;
        self.schedule();
        Ok(())
    }
    /// Cancel and forget an unclaimed result (including a connection that became
    /// ready just before its checkout future was dropped).
    pub fn abandon(&mut self, request: RequestId) {
        if self.cancel(request).is_ok() {
            self.requests.remove(&request);
            self.touch();
        }
    }
    /// Cancel exactly one operation within its fresh binding session.
    pub fn cancel_operation(&mut self, owner: &Owner) {
        self.cancel_matching(|candidate| candidate == owner);
    }
    /// After the host stops admission for this lost session, cancel all of its
    /// outstanding work. Idle resources have no session owner and survive.
    pub fn cancel_session(&mut self, session: &str) {
        self.cancel_matching(|candidate| candidate.session == session);
    }
    fn cancel_matching(&mut self, matches_owner: impl Fn(&Owner) -> bool) {
        let requests: Vec<_> = self
            .requests
            .iter()
            .filter_map(|(id, p)| matches_owner(&p.request.owner).then_some(*id))
            .collect();
        for id in requests {
            let _ = self.fail_request(id, Error::Cancelled, CloseReason::Cancelled);
        }
        let leased: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(id, entry)| {
                (entry.owner.as_ref().is_some_and(&matches_owner)
                    && matches!(entry.state, State::Leased { .. }))
                .then_some(*id)
            })
            .collect();
        for connection in leased {
            self.start_closing(connection, CloseReason::Cancelled);
        }
        self.schedule();
    }
    /// Reusable asserts that the host has completed protocol/backend cleanup.
    /// Uncertain health must use Discarded. Releasing twice is always stale.
    pub fn release(&mut self, lease: LeaseId, disposition: Disposition) -> Result<(), Error> {
        if !self.is_live(lease) {
            return Err(Error::Stale);
        }
        let entry = self.entries.get_mut(&lease.connection).expect("live lease");
        if matches!(
            entry.state,
            State::Leased {
                request: Some(_),
                ..
            }
        ) {
            return Err(Error::WrongState);
        }
        if disposition == Disposition::Reusable && entry.reusable && !self.stopped {
            entry.owner = None;
            entry.state = State::Idle { since: self.now };
        } else {
            let reason = if !entry.reusable {
                CloseReason::Unproven
            } else {
                CloseReason::Discarded
            };
            self.start_closing(lease.connection, reason);
        }
        self.schedule();
        self.touch();
        Ok(())
    }
    /// The host has actually disposed the physical resource. A close command or
    /// timeout by itself never frees a capacity slot.
    pub fn closed(&mut self, connection: ConnectionId) -> Result<(), Error> {
        let entry = self.entries.get(&connection).ok_or(Error::Stale)?;
        match &entry.state {
            State::Closing { cleanup, .. } if cleanup.sent || cleanup.aborted => {}
            _ => {
                return Err(Error::WrongState);
            }
        }
        self.entries.remove(&connection);
        self.schedule();
        self.touch();
        Ok(())
    }
    /// The host observed spontaneous idle loss and has disposed the resource.
    /// A concurrent eviction/expiry may already have marked it Closing; actual
    /// disposal still frees capacity without waiting for a redundant command.
    /// If checkout won first, WrongState leaves the exclusive lease untouched:
    /// route the I/O failure to that exchange and finish its lease cleanup.
    pub fn idle_closed(&mut self, connection: ConnectionId) -> Result<(), Error> {
        let entry = self.entries.get(&connection).ok_or(Error::Stale)?;
        if !matches!(entry.state, State::Idle { .. } | State::Closing { .. }) {
            return Err(Error::WrongState);
        }
        self.entries.remove(&connection);
        self.schedule();
        self.touch();
        Ok(())
    }
    pub fn shutdown(&mut self) {
        self.stop(Error::Shutdown, CloseReason::Shutdown);
    }
    pub fn driver_lost(&mut self) {
        self.driver_lost = true;
        self.stop(Error::DriverLost, CloseReason::DriverLost);
    }
    fn stop(&mut self, error: Error, reason: CloseReason) {
        self.stopped = true;
        let requests: Vec<_> = self.requests.keys().copied().collect();
        for id in requests {
            let _ = self.fail_request(id, error, reason);
        }
        let entries: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(id, e)| {
                matches!(e.state, State::Idle { .. } | State::Leased { .. }).then_some(*id)
            })
            .collect();
        for id in entries {
            self.start_closing(id, reason);
        }
        self.touch();
    }

    /// Take commands in cleanup-first order. The Connect timer starts here, not
    /// while waiting for capacity or for the host dispatcher to run.
    pub fn next_action(&mut self) -> Option<Action> {
        for (connection, entry) in &mut self.entries {
            let (cleanup, opening, reason) = match &mut entry.state {
                State::Opening {
                    cancel: Some(cleanup),
                    ..
                } => (cleanup, true, CloseReason::Cancelled),
                State::Closing { cleanup, reason } => (cleanup, false, *reason),
                _ => {
                    continue;
                }
            };
            let connection = *connection;
            let action = if !cleanup.aborted && self.now >= cleanup.deadline {
                cleanup.aborted = true;
                if opening {
                    Some(Action::AbortConnect { connection })
                } else {
                    Some(Action::Abort { connection })
                }
            } else if !cleanup.sent && !cleanup.aborted {
                cleanup.sent = true;
                if opening {
                    Some(Action::CancelConnect {
                        connection,
                        deadline: cleanup.deadline,
                    })
                } else {
                    Some(Action::Close {
                        connection,
                        reason,
                        deadline: cleanup.deadline,
                    })
                }
            } else {
                None
            };
            if action.is_some() {
                self.touch();
                return action;
            }
        }
        for (connection, entry) in &mut self.entries {
            if let State::Opening {
                clock,
                network_ms,
                cancel: None,
                ..
            } = &mut entry.state
                && clock.is_none()
            {
                let deadline = (*network_ms > 0).then(|| self.now.saturating_add(*network_ms));
                *clock = Some(ConnectClock::Network(deadline));
                let action = Action::Connect {
                    connection: *connection,
                    key: entry.key.clone(),
                    identity: entry.identity.clone(),
                    network_deadline: deadline,
                };
                self.touch();
                return Some(action);
            }
        }
        None
    }
}
