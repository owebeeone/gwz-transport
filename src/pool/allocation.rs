use super::{machine::*, *};

impl PoolMachine {
    pub(super) fn schedule(&mut self) {
        if self.stopped {
            return;
        }
        let waiting: Vec<_> = self
            .requests
            .iter()
            .filter_map(|(id, request)| {
                matches!(request.state, RequestState::Waiting).then_some(*id)
            })
            .collect();
        // Eligible FIFO reuse precedes creation/eviction. An incompatible head
        // cannot steal the compatible idle resource of a later waiter.
        for id in &waiting {
            let request = &self.requests[id].request;
            let idle = self.entries.iter().find_map(|(connection, entry)| {
                (matches!(entry.state, State::Idle { .. })
                    && entry.key == request.key
                    && entry.reusable
                    && entry.identity == request.identity)
                    .then_some(*connection)
            });
            if let Some(connection) = idle {
                match self.lease_id(connection) {
                    Ok(lease) => {
                        let pending = self.requests.get_mut(id).expect("waiting request");
                        let entry = self.entries.get_mut(&connection).expect("idle entry");
                        entry.owner = Some(pending.request.owner.clone());
                        entry.state = State::Leased {
                            lease,
                            request: Some(*id),
                        };
                        pending.state = RequestState::Ready(lease);
                        pending.eviction = None;
                    }
                    Err(error) => {
                        self.requests.get_mut(id).expect("waiting request").state =
                            RequestState::Failed(error);
                    }
                }
                self.touch();
            }
        }
        for id in &waiting {
            let pending = &self.requests[id];
            if !matches!(pending.state, RequestState::Waiting) {
                continue;
            }
            let key = &pending.request.key;
            if self.entries.len() >= self.config.total
                || self.counts_for_user_host(key).total() >= self.config.per_user_host
                || self.counts_for_host(&key.host).total() >= self.config.per_host
            {
                continue;
            }
            let Some(serial) = self.connection_serial.checked_add(1) else {
                self.requests.get_mut(id).expect("waiting request").state =
                    RequestState::Failed(Error::Capacity);
                continue;
            };
            self.connection_serial = serial;
            let connection = ConnectionId {
                pool: self.pool_id,
                serial,
            };
            let pending = self.requests.get_mut(id).expect("waiting request");
            let request = &pending.request;
            self.entries.insert(
                connection,
                Entry {
                    key: request.key.clone(),
                    identity: request.identity.clone(),
                    reusable: false,
                    owner: Some(request.owner.clone()),
                    state: State::Opening {
                        request: Some(*id),
                        clock: None,
                        network_ms: request
                            .connect_timeout_ms
                            .unwrap_or(self.config.connect_timeout_ms),
                        interaction_ms: request
                            .interaction_timeout_ms
                            .unwrap_or(self.config.interaction_timeout_ms),
                        cancel: None,
                    },
                },
            );
            pending.state = RequestState::Opening(connection);
            pending.eviction = None;
            self.touch();
        }
        for id in waiting {
            let pending = &self.requests[&id];
            if !matches!(pending.state, RequestState::Waiting)
                || pending
                    .eviction
                    .is_some_and(|victim| self.entries.contains_key(&victim))
            {
                continue;
            }
            let key = &pending.request.key;
            let user_host_full =
                self.counts_for_user_host(key).total() >= self.config.per_user_host;
            let host_full = self.counts_for_host(&key.host).total() >= self.config.per_host;
            let victim = self
                .entries
                .iter()
                .filter_map(|(connection, entry)| {
                    if let State::Idle { since } = entry.state
                        && (!user_host_full || entry.key.same_user_host(key))
                        && (!host_full || entry.key.host == key.host)
                    {
                        Some((since, *connection))
                    } else {
                        None
                    }
                })
                .min()
                .map(|(_, connection)| connection);
            if let Some(connection) = victim {
                self.start_closing(connection, CloseReason::Evicted);
                self.requests
                    .get_mut(&id)
                    .expect("waiting request")
                    .eviction = Some(connection);
            }
        }
    }

    /// Only the connector that received Connect may acknowledge this token.
    /// On Err the host must dispose any resource it still owns; no new resource
    /// is adopted by an invalid/stale completion.
    pub fn connected(
        &mut self,
        connection: ConnectionId,
        result: Result<Option<Identity>, crate::protocol::Failure>,
    ) -> Result<(), Error> {
        let entry = self.entries.get(&connection).ok_or(Error::Stale)?;
        let State::Opening {
            request,
            clock,
            cancel,
            ..
        } = &entry.state
        else {
            return Err(Error::WrongState);
        };
        if clock.is_none() {
            return Err(Error::WrongState);
        }
        let request = *request;
        let expired = request
            .and_then(|id| self.requests.get(&id))
            .and_then(|pending| pending.absolute_deadline)
            .is_some_and(|deadline| self.now >= deadline);
        if expired {
            let error = match clock.expect("connected opening clock") {
                ConnectClock::Network(_) => Error::ConnectTimeout,
                ConnectClock::Interaction { .. } => Error::InteractionTimeout,
            };
            if let Some(id) = request {
                self.fail_request(id, error, CloseReason::Cancelled)?;
            }
            self.schedule();
            self.touch();
            return Ok(());
        }
        let cancelled = cancel.is_some();
        match result {
            Err(failure) => {
                self.entries.remove(&connection);
                if let Some(id) = request {
                    self.requests.get_mut(&id).expect("opening request").state =
                        RequestState::Failed(Error::ConnectFailed {
                            code: failure.code,
                            effect: failure.effect,
                            setup_cause: failure.setup_cause,
                        });
                }
            }
            Ok(proof) => {
                let mismatch = proof.as_ref().is_some_and(|proof| *proof != entry.identity);
                if cancelled || mismatch {
                    if let Some(id) = request {
                        self.requests.get_mut(&id).expect("opening request").state =
                            RequestState::Failed(Error::IdentityMismatch);
                    }
                    self.start_closing(
                        connection,
                        if mismatch {
                            CloseReason::IdentityMismatch
                        } else {
                            CloseReason::Cancelled
                        },
                    );
                } else {
                    let id = request.expect("live opening owns a request");
                    match self.lease_id(connection) {
                        Ok(lease) => {
                            let entry = self.entries.get_mut(&connection).expect("opening entry");
                            entry.reusable = proof.is_some();
                            entry.state = State::Leased {
                                lease,
                                request: Some(id),
                            };
                            self.requests.get_mut(&id).expect("opening request").state =
                                RequestState::Ready(lease);
                        }
                        Err(error) => {
                            self.requests.get_mut(&id).expect("opening request").state =
                                RequestState::Failed(error);
                            self.start_closing(connection, CloseReason::Discarded);
                        }
                    }
                }
            }
        }
        self.schedule();
        self.touch();
        Ok(())
    }
}
