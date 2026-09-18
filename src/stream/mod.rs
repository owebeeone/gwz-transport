//! Network-stream semantics over discrete messages, independent of their delivery.
//!
//! `StreamMachine` is a deterministic state machine for one already-open stream.
//! The host owns message delivery and monotonic timer notifications. No sockets,
//! framing, serialization, threads or executor are required by the machine.
use std::fmt;

mod asynchronous;
mod incoming;
mod machine;
mod outgoing;
pub use asynchronous::{MessageEndpoint, Stream};
pub use machine::{Snapshot, StreamMachine};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Initiator,
    Endpoint,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub session_id: String,
    pub stream_id: i64,
    pub side: Side,
    pub send_buffer: usize,
    pub receive_window: usize,
    pub peer_receive_window: usize,
    pub max_payload: usize,
    pub coalesce_delay_ms: u64,
    pub close_timeout_ms: u64,
    pub max_waiters: usize,
}
impl Config {
    pub fn new(session_id: impl Into<String>, stream_id: i64, side: Side) -> Self {
        Self {
            session_id: session_id.into(),
            stream_id,
            side,
            send_buffer: 65536,
            receive_window: 65536,
            peer_receive_window: 65536,
            max_payload: 16384,
            coalesce_delay_ms: 100,
            close_timeout_ms: 5000,
            max_waiters: 64,
        }
    }
    fn validate(&self) -> Result<(), Error> {
        if self.session_id.is_empty()
            || self.session_id.len() > 128
            || self.stream_id <= 0
            || self.send_buffer == 0
            || self.send_buffer > 4 * 1024 * 1024
            || self.receive_window == 0
            || self.receive_window > 4 * 1024 * 1024
            || self.peer_receive_window == 0
            || self.peer_receive_window > 4 * 1024 * 1024
            || self.max_payload == 0
            || self.max_payload > 65536
            || self.close_timeout_ms == 0
            || self.max_waiters == 0
            || self.max_waiters > 1024
        {
            return Err(Error::InvalidConfig);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    WouldBlock,
    InvalidConfig,
    Protocol,
    Cancelled,
    CarrierLost,
    Timeout,
    WriteClosed,
    Closed,
    WrongSide,
    WaiterCapacity,
    PeerFailed,
}
impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "message stream: {self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushTicket(pub(super) i64);

#[derive(Debug, Clone, PartialEq)]
pub struct CloseResult {
    pub disposition: crate::protocol::Disposition,
    pub unread_response_discarded: bool,
    pub facts: crate::protocol::Facts,
}
