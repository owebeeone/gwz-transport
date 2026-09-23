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
    pub(super) io: io_clock::IoClock,
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
    pub(super) failure_facts: Option<Facts>,
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
        let io_timeout_ms = config.io_timeout_ms;
        let interaction_budget_ms = config.interaction_budget_ms;
        Ok(Self {
            send: VecDeque::with_capacity(config.send_buffer),
            recv: VecDeque::with_capacity(config.receive_window),
            peer_limit: config.peer_receive_window as i64,
            advertised_limit: config.receive_window as i64,
            config,
            now: 0,
            io: io_clock::IoClock::new(io_timeout_ms, interaction_budget_ms),
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
            failure_facts: None,
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
            self.stop_io_clock();
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
        self.complete_close_with_failure(disposition, facts, None)
    }

    /// Queue a typed terminal failure. Facts are carried by Closed.facts, the
    /// sole authoritative evidence slot; nested Failure.facts is never used.
    pub fn complete_close_failure(
        &mut self,
        disposition: Disposition,
        facts: Facts,
        code: ErrorCode,
        effect: Effect,
    ) -> Result<(), Error> {
        self.complete_close_with_failure(
            disposition,
            facts,
            Some(Failure {
                setup_cause: None,
                code,
                effect,
                facts: None,
            }),
        )
    }

    /// Emit a typed terminal failure without waiting for the Close handshake.
    /// Endpoint failures such as repository refusal must reach the peer before
    /// it observes EOF; the first terminal outcome always wins.
    pub fn fail_terminal(&mut self, failure: Failure) -> Result<(), Error> {
        self.live()?;
        if self.config.side != Side::Endpoint {
            return Err(Error::WrongSide);
        }
        let mut message = self.envelope(MessageKind::Failed);
        message.failed = Some(failure.clone());
        crate::codec::admit_limited(&message, &self.config.peer_limits)
            .map_err(|_| Error::Protocol)?;
        self.failure_facts = failure.facts.clone();
        self.fail(
            Error::PeerFailed {
                code: failure.code,
                effect: failure.effect,
            },
            false,
        );
        self.terminal_message = Some(message);
        Ok(())
    }

    fn complete_close_with_failure(
        &mut self,
        disposition: Disposition,
        facts: Facts,
        failure: Option<Failure>,
    ) -> Result<(), Error> {
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
                failure,
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

    pub fn retained_failure_facts(&self) -> Option<&Facts> {
        self.failure_facts.as_ref()
    }

    pub fn cancel(&mut self) {
        self.fail(Error::Cancelled, true);
    }
    pub fn disconnect(&mut self) {
        self.fail(Error::CarrierLost, false);
        self.terminal_message = None;
        self.touch();
    }

    /// Set the host's aggregate backend activity classification. Only the
    /// endpoint host can make this report; message delivery never infers it.
    pub fn set_io_state(&mut self, state: IoState) -> Result<(), Error> {
        if self.config.side != Side::Endpoint {
            return Err(Error::WrongSide);
        }
        self.live()?;
        if self.close_deadline.is_some() || self.close_received {
            return Err(Error::WrongState);
        }
        self.expire_io_if_due()?;
        let before = self.io.status(self.now);
        self.io.set_state(self.now, state);
        if before.state == state {
            return Ok(());
        }
        if self.io.due(self.now) {
            self.fail(Error::Timeout, true);
            return Err(Error::Timeout);
        }
        Ok(())
    }

    /// Report bytes that were actually transferred with the backend peer.
    /// Local buffering, message delivery and EOF are deliberately excluded.
    pub fn record_io_progress(&mut self, bytes: usize) -> Result<(), Error> {
        if self.config.side != Side::Endpoint {
            return Err(Error::WrongSide);
        }
        self.live()?;
        if self.close_deadline.is_some() || self.close_received {
            return Err(Error::WrongState);
        }
        self.expire_io_if_due()?;
        self.io
            .progress(self.now, bytes, self.config.io_timeout_ms)?;
        Ok(())
    }

    pub fn io_status(&self) -> IoStatus {
        self.io.status(self.now)
    }

    /// The host supplies monotonic time and must advance it even without writes.
    pub fn advance(&mut self, now_ms: u64) {
        if now_ms < self.now {
            return;
        }
        let was_due = self.batch_deadline.is_some_and(|at| self.now >= at);
        self.now = now_ms;
        if self.error.is_some() || self.completed.is_some() {
            return;
        }
        if self.close_deadline.is_some_and(|at| self.now >= at) || self.io.due(self.now) {
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
        [self.io.deadline(), batch, self.close_deadline]
            .into_iter()
            .flatten()
            .min()
    }

    pub(super) fn expire_io_if_due(&mut self) -> Result<(), Error> {
        if self.close_deadline.is_some_and(|at| self.now >= at) || self.io.due(self.now) {
            self.fail(Error::Timeout, true);
            return Err(Error::Timeout);
        }
        Ok(())
    }

    pub(super) fn stop_io_clock(&mut self) {
        self.io.stop(self.now);
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
            version: self.config.profile_version,
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
        self.io.stop(self.now);
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
                    setup_cause: None,
                    code: if error == Error::Timeout {
                        ErrorCode::Timeout
                    } else {
                        ErrorCode::Protocol
                    },
                    effect: Effect::Possible,
                    facts: None,
                });
                self.terminal_message = Some(message);
            }
        }
        self.touch();
    }
}
