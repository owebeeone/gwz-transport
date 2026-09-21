use super::*;

impl Mux {
    pub fn receive(&mut self, item: &Attachment) -> Result<(), Error> {
        self.live()?;
        if item.1.session_id != self.session {
            return Ok(());
        }
        let result = self.receive_in(item);
        if matches!(
            result,
            Err(Error::Protocol | Error::InvalidRequest | Error::Capacity)
        ) {
            self.disconnect();
        }
        result
    }
    fn receive_in(&mut self, item: &Attachment) -> Result<(), Error> {
        let (request, message) = item;
        if !identifier(request) {
            return Err(Error::Protocol);
        }
        let limits = self
            .binding
            .as_ref()
            .map_or(&self.config.limits, |b| b.limits());
        codec::admit_limited(message, limits).map_err(|_| Error::Protocol)?;
        if message.stream_id == 0 {
            return self.bootstrap(item);
        }
        if self.phase != Phase::Ready || message.version != 2 {
            return Err(Error::Protocol);
        }
        if matches!(message.kind, MessageKind::Open | MessageKind::CheckIdentity) {
            if self.endpoint.is_none() {
                return Err(Error::Protocol);
            }
            if message.stream_id <= self.highest_id {
                return Ok(());
            }
            self.live_request(request).map_err(|_| Error::Protocol)?;
            if self.routes.len() == self.config.max_streams {
                return Err(Error::Capacity);
            }
            self.validate_open(message)?;
            let (operation, kind, timeout) = if let Some(open) = &message.open {
                (&open.operation_id, Kind::Opening, None)
            } else {
                let check = message.check_identity.as_ref().ok_or(Error::Protocol)?;
                if check.timeout_ms as u64 > self.config.max_check_ms {
                    return Err(Error::Protocol);
                }
                (
                    &check.operation_id,
                    Kind::Check,
                    Some(check.timeout_ms as u64),
                )
            };
            if !identifier(operation)
                || self.requests[request]
                    .operation
                    .as_ref()
                    .is_some_and(|op| op != operation)
            {
                return Err(Error::Protocol);
            }
            self.enqueue(item, false)?;
            self.requests
                .get_mut(request)
                .expect("registered request")
                .operation = Some(operation.clone());
            self.highest_id = message.stream_id;
            self.routes.insert(
                message.stream_id,
                Route {
                    request: request.clone(),
                    kind,
                    deadline: timeout.map(|t| self.now.saturating_add(t)),
                    cancel: None,
                },
            );
            return Ok(());
        }
        let Some(route) = self.routes.get(&message.stream_id) else {
            return if message.stream_id <= self.highest_id {
                Ok(())
            } else {
                Err(Error::Protocol)
            };
        };
        if route.request != *request {
            return Err(Error::Protocol);
        }
        self.validate_transition(route.kind, message.kind, false)?;
        self.validate_opened(message)?;
        self.enqueue(item, false)?;
        self.transition(message);
        Ok(())
    }
    fn bootstrap(&mut self, item: &Attachment) -> Result<(), Error> {
        let (request, message) = item;
        if self.endpoint.is_none()
            && self.phase == Phase::Ready
            && self.acknowledgement.as_ref() == Some(item)
        {
            return Ok(());
        }
        self.live_request(request).map_err(|_| Error::Protocol)?;
        if message.kind == MessageKind::Bind && self.endpoint.is_some() {
            if self.phase != Phase::Unbound {
                return Err(Error::Protocol);
            }
            if message
                .bind
                .as_ref()
                .is_none_or(|b| !b.versions.contains(&2))
            {
                return self.reject_binding(
                    request,
                    Failure {
                        code: ErrorCode::UnsupportedVersion,
                        effect: Effect::None,
                        facts: None,
                    },
                );
            }
            let (reply, bound) = match self
                .endpoint
                .as_ref()
                .expect("endpoint role")
                .accept(message)
            {
                Ok(accepted) => accepted,
                Err(failure) => {
                    return self.reject_binding(request, failure);
                }
            };
            if reply.bound.as_ref().is_none_or(|b| b.version != 2) {
                return Err(Error::Protocol);
            }
            self.enqueue(&(request.clone(), reply.clone()), true)?;
            self.acknowledgement = Some((request.clone(), reply));
            self.binding = Some(bound);
            self.phase = Phase::Ready;
            return Ok(());
        }
        if self.endpoint.is_some() {
            return Err(Error::Protocol);
        }
        if self.phase == Phase::Ready && self.acknowledgement.as_ref() == Some(item) {
            return Ok(());
        }
        if self.phase != Phase::Binding {
            return Err(Error::Protocol);
        }
        let offer = self.offer.as_ref().ok_or(Error::Protocol)?;
        if *request != offer.0 {
            return Err(Error::Protocol);
        }
        if message.kind == MessageKind::BindRejected {
            let failure = message.bind_rejected.as_ref().ok_or(Error::Protocol)?;
            codec::validate_bind_rejection(failure).map_err(|_| Error::Protocol)?;
            self.rejection = Some(failure.clone());
            self.disconnect();
            return Ok(());
        }
        let bound = binding::verify(&offer.1, message).map_err(|_| Error::Protocol)?;
        if message.bound.as_ref().is_none_or(|b| b.version != 2) {
            return Err(Error::Protocol);
        }
        self.binding = Some(bound);
        self.acknowledgement = Some(item.clone());
        self.bootstrap_deadline = None;
        self.phase = Phase::Ready;
        Ok(())
    }
    fn reject_binding(&mut self, request: &str, failure: Failure) -> Result<(), Error> {
        codec::validate_bind_rejection(&failure).map_err(|_| Error::Protocol)?;
        let reply = Envelope {
            version: 1,
            session_id: self.session.clone(),
            stream_id: 0,
            kind: MessageKind::BindRejected,
            bind_rejected: Some(failure.clone()),
            ..Default::default()
        };
        self.enqueue(&(request.into(), reply), true)?;
        self.rejection = Some(failure);
        self.phase = Phase::Rejecting;
        self.bootstrap_deadline = Some(self.now.saturating_add(self.config.bootstrap_timeout_ms));
        Ok(())
    }
    pub(super) fn validate_opened(&self, message: &Envelope) -> Result<(), Error> {
        if let Some(opened) = &message.opened {
            let binding = self.binding.as_ref().ok_or(Error::Protocol)?;
            if opened.endpoint_id != binding.endpoint_id()
                || opened.trust_owner != binding.trust_owner()
                || !binding::no_greater(&opened.receive_limits, binding.limits())
            {
                return Err(Error::Protocol);
            }
        }
        Ok(())
    }
    pub(super) fn validate_open(&self, message: &Envelope) -> Result<(), Error> {
        let bound = self.binding.as_ref().ok_or(Error::WrongState)?;
        if message.kind == MessageKind::Open {
            bound.check_open(message).map_err(|_| Error::Protocol)
        } else {
            bound.check_identity(message).map_err(|_| Error::Protocol)
        }
    }
    pub(super) fn validate_transition(
        &self,
        state: Kind,
        message: MessageKind,
        outgoing: bool,
    ) -> Result<(), Error> {
        let from_endpoint = self.endpoint.is_some() == outgoing;
        let valid = match state {
            Kind::Check => {
                if from_endpoint {
                    matches!(
                        message,
                        MessageKind::IdentityChecked | MessageKind::IdentityCheckFailed
                    )
                } else {
                    message == MessageKind::Cancel
                }
            }
            Kind::Opening => {
                if from_endpoint {
                    matches!(message, MessageKind::Opened | MessageKind::OpenFailed)
                } else {
                    message == MessageKind::Cancel
                }
            }
            Kind::Stream => match message {
                MessageKind::Data
                | MessageKind::Window
                | MessageKind::Flush
                | MessageKind::Flushed
                | MessageKind::EndWrite
                | MessageKind::Failed => true,
                MessageKind::Close | MessageKind::Cancel => !from_endpoint,
                MessageKind::Closed => from_endpoint,
                _ => false,
            },
        };
        if valid { Ok(()) } else { Err(Error::Protocol) }
    }
    pub(super) fn transition(&mut self, message: &Envelope) {
        if message.kind == MessageKind::Opened {
            self.routes
                .get_mut(&message.stream_id)
                .expect("validated route")
                .kind = Kind::Stream;
        } else if matches!(
            message.kind,
            MessageKind::Closed
                | MessageKind::Failed
                | MessageKind::OpenFailed
                | MessageKind::IdentityChecked
                | MessageKind::IdentityCheckFailed
        ) {
            self.routes.remove(&message.stream_id);
        }
    }
}
