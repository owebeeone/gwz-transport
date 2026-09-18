use super::*;
use crate::protocol::*;
use std::collections::VecDeque;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub send_buffer: usize,
    pub received_buffer: usize,
    pub peak_send_buffer: usize,
    pub peak_received_buffer: usize,
    pub sent: i64,
    pub received: i64,
    pub consumed: i64,
    pub peer_limit: i64,
    pub advertised_limit: i64,
    pub end_sent: bool,
    pub end_received: bool,
    pub terminal: bool,
}

pub(super) struct PendingFlush {
    pub id: i64,
    pub offset: i64,
    pub sent: bool,
}

pub struct StreamMachine {
    pub(super) config: Config,
    pub(super) send: VecDeque<u8>,
    pub(super) recv: VecDeque<u8>,
    pub(super) now: u64,
    pub(super) batch_deadline: Option<u64>,
    pub(super) force_send: bool,
    pub(super) sent: i64,
    pub(super) received: i64,
    pub(super) consumed: i64,
    pub(super) peer_limit: i64,
    pub(super) advertised_limit: i64,
    pub(super) end_requested: bool,
    pub(super) end_sent: bool,
    pub(super) end_received: bool,
    pub(super) flush: Option<PendingFlush>,
    pub(super) flush_serial: i64,
    pub(super) flush_acked: i64,
    pub(super) peer_flush: Option<Barrier>,
    pub(super) peer_flush_serial: i64,
    pub(super) close_deadline: Option<u64>,
    pub(super) close_sent: bool,
    pub(super) close_received: bool,
    pub(super) discarding: bool,
    pub(super) discarded: bool,
    pub(super) close_pending: Option<Closed>,
    pub(super) completed: Option<CloseResult>,
    pub(super) error: Option<Error>,
    pub(super) terminal_message: Option<Envelope>,
    pub(super) peak_send: usize,
    pub(super) peak_recv: usize,
    pub(super) revision: u64,
}

impl StreamMachine {
    /// Construct only after Open/Opened has established these limits and identity.
    /// This constructor has no network, pool or credential effects.
    pub fn new(config: Config) -> Result<Self, Error> {
        config.validate()?;
        Ok(Self {
            send: VecDeque::with_capacity(config.send_buffer),
            recv: VecDeque::with_capacity(config.receive_window),
            peer_limit: config.peer_receive_window as i64,
            advertised_limit: config.receive_window as i64,
            config,
            now: 0,
            batch_deadline: None,
            force_send: false,
            sent: 0,
            received: 0,
            consumed: 0,
            end_requested: false,
            end_sent: false,
            end_received: false,
            flush: None,
            flush_serial: 0,
            flush_acked: 0,
            peer_flush: None,
            peer_flush_serial: 0,
            close_deadline: None,
            close_sent: false,
            close_received: false,
            discarding: false,
            discarded: false,
            close_pending: None,
            completed: None,
            error: None,
            terminal_message: None,
            peak_send: 0,
            peak_recv: 0,
            revision: 0,
        })
    }

    pub fn write(&mut self, input: &[u8]) -> Result<usize, Error> {
        self.live()?;
        if self.end_requested {
            return Err(Error::WriteClosed);
        }
        if input.is_empty() {
            return Ok(0);
        }
        if self.flush.is_some() {
            return Err(Error::WouldBlock);
        }
        let count = input.len().min(self.config.send_buffer - self.send.len());
        if count == 0 {
            return Err(Error::WouldBlock);
        }
        if self
            .sent
            .checked_add(self.send.len() as i64)
            .and_then(|v| v.checked_add(count as i64))
            .is_none()
        {
            self.fail(Error::Protocol, true);
            return Err(Error::Protocol);
        }
        if self.send.is_empty() {
            self.batch_deadline = Some(self.now.saturating_add(self.config.coalesce_delay_ms));
        }
        self.send.extend(&input[..count]);
        self.peak_send = self.peak_send.max(self.send.len());
        self.touch();
        Ok(count)
    }

    pub fn read(&mut self, output: &mut [u8]) -> Result<usize, Error> {
        if output.is_empty() {
            return Ok(0);
        }
        if !self.send.is_empty() && !self.force_send {
            self.force_send = true;
            self.touch();
        }
        if !self.recv.is_empty() {
            let count = output.len().min(self.recv.len());
            for byte in &mut output[..count] {
                *byte = self.recv.pop_front().expect("bounded by queue length");
            }
            self.consumed += count as i64;
            self.touch();
            return Ok(count);
        }
        // Errors after an already received prefix remain errors, never clean EOF.
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.discarding || self.completed.is_some() {
            return Err(Error::Closed);
        }
        if self.end_received {
            return Ok(0);
        }
        Err(Error::WouldBlock)
    }

    pub fn start_flush(&mut self) -> Result<FlushTicket, Error> {
        self.live()?;
        if let Some(flush) = &self.flush {
            return Ok(FlushTicket(flush.id));
        }
        if self.end_sent {
            return Err(Error::WriteClosed);
        }
        self.flush_serial = self.flush_serial.checked_add(1).ok_or(Error::Protocol)?;
        let offset = self
            .sent
            .checked_add(self.send.len() as i64)
            .ok_or(Error::Protocol)?;
        self.flush = Some(PendingFlush {
            id: self.flush_serial,
            offset,
            sent: false,
        });
        self.force_send = true;
        self.touch();
        Ok(FlushTicket(self.flush_serial))
    }

    pub fn flush_complete(&self, ticket: FlushTicket) -> Result<bool, Error> {
        if let Some(error) = self.error {
            return Err(error);
        }
        Ok(ticket.0 <= self.flush_acked)
    }

    pub fn end_write(&mut self) -> Result<(), Error> {
        self.live()?;
        if !self.end_requested {
            self.end_requested = true;
            self.force_send = true;
            self.touch();
        }
        Ok(())
    }

    pub fn start_close(&mut self) -> Result<(), Error> {
        if self.completed.is_some() {
            return Ok(());
        }
        self.live()?;
        if self.config.side != Side::Initiator {
            return Err(Error::WrongSide);
        }
        if self.close_deadline.is_none() {
            self.close_deadline = Some(self.now.saturating_add(self.config.close_timeout_ms));
            self.discarding = true;
            self.discarded |= !self.recv.is_empty();
            self.consumed += self.recv.len() as i64;
            self.recv.clear();
            self.end_write()?;
            self.touch();
        }
        Ok(())
    }

    /// The endpoint calls this only after its sink/network cleanup is complete.
    /// The stream never infers reusable connection health or Git success itself.
    pub fn complete_close(&mut self, disposition: Disposition, facts: Facts) -> Result<(), Error> {
        self.live()?;
        if self.config.side != Side::Endpoint {
            return Err(Error::WrongSide);
        }
        if !self.close_received
            || !self.end_sent
            || !self.end_received
            || !self.recv.is_empty()
            || self.peer_flush.is_some()
            || self.flush.is_some()
        {
            return Err(Error::WouldBlock);
        }
        if self.close_pending.is_none() {
            let mut message = self.envelope(MessageKind::Closed);
            message.closed = Some(Closed {
                disposition,
                facts,
                unread_response_discarded: self.discarded,
                failure: None,
            });
            // Validate before retaining or cloning caller-owned metadata. A
            // rejected local construction leaves cleanup retryable.
            crate::codec::admit_limited(&message, &self.config.peer_limits)
                .map_err(|_| Error::Protocol)?;
            self.close_pending = message.closed;
            self.touch();
        }
        Ok(())
    }

    pub fn close_result(&self) -> Result<CloseResult, Error> {
        if let Some(error) = self.error {
            return Err(error);
        }
        self.completed.clone().ok_or(Error::WouldBlock)
    }

    pub fn cancel(&mut self) {
        self.fail(Error::Cancelled, true);
    }
    pub fn disconnect(&mut self) {
        self.fail(Error::CarrierLost, false);
        self.terminal_message = None;
        self.touch();
    }

    /// The host supplies monotonic time and must advance it even without writes.
    pub fn advance(&mut self, now_ms: u64) {
        if now_ms <= self.now {
            return;
        }
        let was_due = self.batch_deadline.is_some_and(|at| self.now >= at);
        self.now = now_ms;
        if self.close_deadline.is_some_and(|at| self.now >= at) {
            self.fail(Error::Timeout, true);
        } else if !was_due && self.batch_deadline.is_some_and(|at| self.now >= at) {
            self.touch();
        }
    }

    pub fn next_deadline(&self) -> Option<u64> {
        if self.error.is_some() || self.completed.is_some() {
            return None;
        }
        let batch = self
            .batch_deadline
            .filter(|at| *at > self.now && !self.force_send);
        match (batch, self.close_deadline) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    pub fn stats(&self) -> Snapshot {
        Snapshot {
            send_buffer: self.send.len(),
            received_buffer: self.recv.len(),
            peak_send_buffer: self.peak_send,
            peak_received_buffer: self.peak_recv,
            sent: self.sent,
            received: self.received,
            consumed: self.consumed,
            peer_limit: self.peer_limit,
            advertised_limit: self.advertised_limit,
            end_sent: self.end_sent,
            end_received: self.end_received,
            terminal: self.error.is_some() || self.completed.is_some(),
        }
    }

    pub(super) fn live(&self) -> Result<(), Error> {
        if let Some(error) = self.error {
            return Err(error);
        }
        if self.completed.is_some() {
            return Err(Error::Closed);
        }
        Ok(())
    }
    pub(super) fn touch(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }
    pub(super) fn envelope(&self, kind: MessageKind) -> Envelope {
        Envelope {
            version: 1,
            session_id: self.config.session_id.clone(),
            stream_id: self.config.stream_id,
            kind,
            ..Default::default()
        }
    }
    pub(super) fn fail(&mut self, error: Error, notify: bool) {
        if self.error.is_some() || self.completed.is_some() {
            return;
        }
        self.error = Some(error);
        self.send.clear();
        self.flush = None;
        self.peer_flush = None;
        self.close_pending = None;
        self.batch_deadline = None;
        if notify {
            if matches!(error, Error::Cancelled | Error::Timeout)
                && self.config.side == Side::Initiator
                || error == Error::Cancelled
            {
                let mut message = self.envelope(MessageKind::Cancel);
                message.cancel = Some(Cancel {
                    reason: if error == Error::Timeout {
                        ErrorCode::Timeout
                    } else {
                        ErrorCode::Cancelled
                    },
                });
                self.terminal_message = Some(message);
            } else {
                let mut message = self.envelope(MessageKind::Failed);
                message.failed = Some(Failure {
                    code: if error == Error::Timeout {
                        ErrorCode::Timeout
                    } else {
                        ErrorCode::Protocol
                    },
                    effect: Effect::Possible,
                });
                self.terminal_message = Some(message);
            }
        }
        self.touch();
    }
}
