use super::*;
use crate::protocol::*;

impl StreamMachine {
    /// Take the next typed message. The host must preserve order and provide
    /// bounded delivery queues; taking a message transfers delivery ownership.
    pub fn next_message(&mut self) -> Option<Envelope> {
        if let Some(message) = self.terminal_message.take() {
            self.touch();
            return Some(message);
        }
        if self.error.is_some() || self.completed.is_some() {
            return None;
        }
        // Reverse data is already in the bounded read adapter; forward data must
        // first be consumed by the endpoint's sink adapter.
        let sink_offset = if self.config.side == Side::Initiator {
            self.received
        } else {
            self.consumed
        };
        if self
            .peer_flush
            .as_ref()
            .is_some_and(|barrier| barrier.offset <= sink_offset)
        {
            let mut message = self.envelope(MessageKind::Flushed);
            message.flushed = self.peer_flush.take();
            self.touch();
            return Some(message);
        }
        // Control slots are state, not an unbounded queue; credit never waits
        // behind this direction's buffered data or exhausted sending credit.
        let limit = self
            .consumed
            .saturating_add(self.config.receive_window as i64);
        if limit > self.advertised_limit {
            self.advertised_limit = limit;
            let mut message = self.envelope(MessageKind::Window);
            message.window = Some(Window { max_offset: limit });
            self.touch();
            return Some(message);
        }
        if let Some(closed) = self.close_pending.take() {
            self.completed = Some(CloseResult {
                disposition: closed.disposition,
                unread_response_discarded: closed.unread_response_discarded,
                facts: closed.facts.clone(),
            });
            let mut message = self.envelope(MessageKind::Closed);
            message.closed = Some(closed);
            self.touch();
            return Some(message);
        }
        if let Some(flush) = &mut self.flush
            && !flush.sent
            && self.sent == flush.offset
        {
            flush.sent = true;
            let barrier = Barrier {
                barrier_id: flush.id,
                offset: flush.offset,
            };
            let mut message = self.envelope(MessageKind::Flush);
            message.flush = Some(barrier);
            self.touch();
            return Some(message);
        }
        let eligible = self.force_send
            || self.send.len() >= self.config.max_payload
            || self.send.len() == self.config.send_buffer
            || self.batch_deadline.is_some_and(|at| self.now >= at);
        let credit = (self.peer_limit - self.sent) as usize;
        if !self.send.is_empty() && eligible && credit > 0 {
            let count = self.send.len().min(self.config.max_payload).min(credit);
            let data = Data {
                offset: self.sent,
                payload: self.send.drain(..count).collect(),
            };
            self.sent += count as i64;
            if self.send.is_empty() {
                self.batch_deadline = None;
                self.force_send = false;
            }
            let mut message = self.envelope(MessageKind::Data);
            message.data = Some(data);
            self.touch();
            return Some(message);
        }
        if self.end_requested && !self.end_sent && self.send.is_empty() {
            self.end_sent = true;
            let mut message = self.envelope(MessageKind::EndWrite);
            message.end_write = Some(EndWrite {
                final_offset: self.sent,
            });
            self.touch();
            return Some(message);
        }
        if self.config.side == Side::Initiator
            && self.close_deadline.is_some()
            && self.end_sent
            && !self.close_sent
        {
            self.close_sent = true;
            let mut message = self.envelope(MessageKind::Close);
            message.close = Some(Close {
                final_offset: self.sent,
            });
            self.touch();
            return Some(message);
        }
        None
    }
}
