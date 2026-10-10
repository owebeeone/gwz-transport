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
            if request.fresh {
                continue;
            }
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
            self.open_for(*id);
        }
        for id in waiting {
            let pending = &self.requests[&id];
            if !matches!(pending.state, RequestState::Waiting)
                || pending.eviction.is_some_and(|victim| {
                    self.entries.contains_key(&victim) || self.holds.contains_key(&victim)
                })
            {
                continue;
            }
            let key = &pending.request.key;
            if self.no_evicts.iter().any(|site| key.on_site(site)) {
                continue;
            }
            let user_host_full = self.user_host_total(key) >= self.config.per_user_host;
            let host_full = self.host_total(&key.host) >= self.config.per_host;
            let site_full = self.site_full(key);
            let victim = self
                .entries
                .iter()
                .filter_map(|(connection, entry)| {
                    if let State::Idle { since } = entry.state
                        && (!user_host_full || entry.key.same_user_host(key))
                        && (!host_full || entry.key.host == key.host)
                        && (!site_full || entry.key.same_site(key))
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

    /// Opens a connection for waiting request `id` when the endpoint total,
    /// the host and user-host caps and the site limit leave room for one.
    /// Held slots count in all but the endpoint total.
    pub(super) fn open_for(&mut self, id: RequestId) {
        let Some(pending) = self.requests.get(&id) else {
            return;
        };
        if !matches!(pending.state, RequestState::Waiting) {
            return;
        }
        let key = &pending.request.key;
        if self.entries.len() >= self.config.total
            || self.user_host_total(key) >= self.config.per_user_host
            || self.host_total(&key.host) >= self.config.per_host
            || self.site_full(key)
        {
            return;
        }
        let Some(serial) = self.connection_serial.checked_add(1) else {
            self.requests.get_mut(&id).expect("waiting request").state =
                RequestState::Failed(Error::Capacity);
            return;
        };
        self.connection_serial = serial;
        let connection = ConnectionId {
            pool: self.pool_id,
            serial,
        };
        let pending = self.requests.get_mut(&id).expect("waiting request");
        let request = &pending.request;
        self.entries.insert(
            connection,
            Entry {
                key: request.key.clone(),
                identity: request.identity.clone(),
                reusable: false,
                owner: Some(request.owner.clone()),
                state: State::Opening {
                    request: Some(id),
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

    /// Only the connector that received Connect may acknowledge this token.
    /// On Err the host must dispose any resource it still owns; no new resource
    /// is adopted by an invalid/stale completion.
    pub fn connected(
        &mut self,
        connection: ConnectionId,
        result: Result<Option<Identity>, crate::protocol::Failure>,
    ) -> Result<(), Error> {
        let shared_clock = self.shared_setup_clock(connection);
        if let Some(clock) = &shared_clock {
            let identity_matches = result
                .as_ref()
                .ok()
                .and_then(|proof| proof.as_ref())
                .is_none_or(|proof| {
                    self.entries
                        .get(&connection)
                        .is_some_and(|entry| entry.identity == *proof)
                });
            let cause = match &result {
                Ok(_) if identity_matches => SetupCause::Completed,
                Ok(_) => SetupCause::ResourceFailure {
                    code: ErrorCode::Authentication,
                    effect: Effect::None,
                    setup_cause: None,
                },
                Err(failure) => SetupCause::ResourceFailure {
                    code: failure.code,
                    effect: failure.effect,
                    setup_cause: failure.setup_cause,
                },
            };
            let record = if cause == SetupCause::Completed {
                match self.absorb_clock_update(clock.admit_completed()) {
                    Ok(record) | Err(PublicationError::ActiveTerminal(record)) => record,
                    Err(_) => {
                        self.absorb_clock_update(clock.terminate(SetupCause::ResourceFailure {
                            code: ErrorCode::InvalidRequest,
                            effect: Effect::None,
                            setup_cause: None,
                        }))
                    }
                }
            } else {
                self.absorb_clock_update(clock.terminate(cause))
            };
            if !matches!(record.cause, SetupCause::Completed) || result.is_err() {
                let request = self
                    .entries
                    .get(&connection)
                    .and_then(|entry| match entry.state {
                        State::Opening { request, .. } => request,
                        _ => None,
                    });
                if let Some(id) = request {
                    self.requests.get_mut(&id).expect("opening request").state =
                        RequestState::Failed(Error::SetupEnded(record));
                }
                if result.is_err() {
                    // A server refusal leaves no hold; a connect the client
                    // cancelled does, for the server may still count it.
                    let cancelled = self.cancelled_connect(connection);
                    self.dispose(connection, cancelled);
                } else {
                    self.start_closing(connection, CloseReason::Cancelled);
                }
                self.schedule();
                self.touch();
                return Ok(());
            }
        }
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
        let expired = shared_clock.is_none()
            && request
                .and_then(|id| self.requests.get(&id))
                .and_then(|pending| pending.absolute_deadline)
                .is_some_and(|deadline| self.now >= deadline);
        if expired {
            let error = match clock.as_ref().expect("connected opening clock") {
                ConnectClock::Network(_) => Error::ConnectTimeout,
                ConnectClock::Interaction { .. } => Error::InteractionTimeout,
                ConnectClock::LocalWait { .. } => Error::LocalWaitExpired,
                ConnectClock::Shared(_) => unreachable!(),
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
                self.dispose(connection, cancelled);
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
