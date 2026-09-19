use super::*;
use crate::protocol::*;

impl StreamMachine {
    /// Deliver one ordered, typed message. No encoding or physical I/O occurs.
    pub fn receive(&mut self, message: Envelope) -> Result<(), Error> {
        if self.error.is_some() || self.completed.is_some() {
            return Ok(());
        }
        self.expire_io_if_due()?;
        let result = self.accept(message);
        if result.is_err() {
            self.fail(Error::Protocol, true);
        }
        result
    }

    fn accept(&mut self, message: Envelope) -> Result<(), Error> {
        if message.session_id != self.config.session_id
            || message.stream_id != self.config.stream_id
        {
            return Err(Error::Protocol);
        }
        // Admission inspects the typed value without serialization or copying it.
        crate::codec::admit_limited(&message, &self.config.receive_limits)
            .map_err(|_| Error::Protocol)?;
        match message.kind {
            MessageKind::Data => {
                let data = message.data.ok_or(Error::Protocol)?;
                let end = data
                    .offset
                    .checked_add(data.payload.len() as i64)
                    .ok_or(Error::Protocol)?;
                if self.end_received
                    || data.offset != self.received
                    || data.payload.is_empty()
                    || data.payload.len() > self.config.max_payload
                    || end > self.advertised_limit
                {
                    return Err(Error::Protocol);
                }
                if self.discarding {
                    self.consumed = end;
                    self.discarded = true;
                } else {
                    if self.recv.len() + data.payload.len() > self.config.receive_window {
                        return Err(Error::Protocol);
                    }
                    self.recv.extend(data.payload);
                    self.peak_recv = self.peak_recv.max(self.recv.len());
                }
                self.received = end;
            }
            MessageKind::Window => {
                let limit = message.window.ok_or(Error::Protocol)?.max_offset;
                if limit <= self.peer_limit {
                    return Ok(());
                }
                if limit
                    > self
                        .sent
                        .saturating_add(self.config.peer_receive_window as i64)
                {
                    return Err(Error::Protocol);
                }
                self.peer_limit = limit;
            }
            MessageKind::Flush => {
                let barrier = message.flush.ok_or(Error::Protocol)?;
                if self.peer_flush.is_some()
                    || barrier.barrier_id <= self.peer_flush_serial
                    || barrier.offset != self.received
                    || self.end_received
                {
                    return Err(Error::Protocol);
                }
                self.peer_flush_serial = barrier.barrier_id;
                self.peer_flush = Some(barrier);
            }
            MessageKind::Flushed => {
                let barrier = message.flushed.ok_or(Error::Protocol)?;
                let pending = self.flush.as_ref().ok_or(Error::Protocol)?;
                if !pending.sent
                    || pending.id != barrier.barrier_id
                    || pending.offset != barrier.offset
                {
                    return Err(Error::Protocol);
                }
                self.flush_acked = pending.id;
                self.flush = None;
            }
            MessageKind::EndWrite => {
                if message.end_write.ok_or(Error::Protocol)?.final_offset != self.received {
                    return Err(Error::Protocol);
                }
                self.end_received = true;
            }
            MessageKind::Close => {
                if self.config.side != Side::Endpoint
                    || !self.end_received
                    || message.close.ok_or(Error::Protocol)?.final_offset != self.received
                {
                    return Err(Error::Protocol);
                }
                if !self.close_received {
                    self.close_received = true;
                    self.stop_io_clock();
                    self.close_deadline =
                        Some(self.now.saturating_add(self.config.close_timeout_ms));
                }
                // The endpoint must still consume pending request bytes into its sink.
                // Only the initiator discards unread response bytes on close.
            }
            MessageKind::Closed => {
                if self.config.side != Side::Initiator || !self.close_sent || !self.end_received {
                    return Err(Error::Protocol);
                }
                let closed = message.closed.ok_or(Error::Protocol)?;
                if let Some(failure) = closed.failure {
                    self.fail(
                        Error::PeerFailed {
                            code: failure.code,
                            effect: failure.effect,
                        },
                        false,
                    );
                } else {
                    self.completed = Some(CloseResult {
                        disposition: closed.disposition,
                        unread_response_discarded: self.discarded
                            || closed.unread_response_discarded,
                        facts: closed.facts,
                    });
                }
            }
            MessageKind::Cancel => {
                let reason = message.cancel.ok_or(Error::Protocol)?.reason;
                self.fail(
                    if reason == ErrorCode::Timeout {
                        Error::Timeout
                    } else {
                        Error::Cancelled
                    },
                    false,
                );
            }
            MessageKind::Failed => {
                let failure = message.failed.ok_or(Error::Protocol)?;
                self.fail(
                    Error::PeerFailed {
                        code: failure.code,
                        effect: failure.effect,
                    },
                    false,
                );
            }
            _ => {
                return Err(Error::Protocol);
            }
        }
        self.touch();
        Ok(())
    }
}
