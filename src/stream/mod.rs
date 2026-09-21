//! Network-stream semantics over discrete messages, independent of their delivery.
//!
//! `StreamMachine` is a deterministic state machine for one already-open stream.
//! The host owns message delivery and monotonic timer notifications. No sockets,
//! framing, serialization, threads or executor are required by the machine.
use std::fmt;

mod asynchronous;
mod incoming;
mod io_clock;
mod machine;
mod outgoing;
pub use asynchronous::{MessageEndpoint, Stream};
pub use machine::{Snapshot, StreamMachine};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Initiator,
    Endpoint,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoState {
    Idle,
    Network,
    Backpressure,
    Interaction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IoStatus {
    pub state: IoState,
    pub remaining_network_ms: u64,
    pub remaining_interaction_ms: u64,
    pub active_deadline: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub session_id: String,
    pub stream_id: i64,
    pub side: Side,
    /// Negotiated owner profile for non-bootstrap stream messages.
    pub profile_version: i64,
    /// This receiver's negotiated limits; the buffer/window may narrow them.
    pub receive_limits: crate::protocol::Limits,
    /// The peer receiver's negotiated limits, applied before emitting messages.
    pub peer_limits: crate::protocol::Limits,
    pub send_buffer: usize,
    pub receive_window: usize,
    pub peer_receive_window: usize,
    pub max_payload: usize,
    pub coalesce_delay_ms: u64,
    pub close_timeout_ms: u64,
    /// Aggregate backend peer-progress timeout while in [`IoState::Network`].
    pub io_timeout_ms: u64,
    /// Cumulative helper-wait allowance while in [`IoState::Interaction`].
    pub interaction_budget_ms: u64,
    /// Application registrations; one additional dispatcher slot is reserved.
    pub max_waiters: usize,
}
impl Config {
    pub fn new(session_id: impl Into<String>, stream_id: i64, side: Side) -> Self {
        Self {
            session_id: session_id.into(),
            stream_id,
            side,
            profile_version: 1,
            receive_limits: crate::binding::default_limits(),
            peer_limits: crate::binding::default_limits(),
            send_buffer: 65536,
            receive_window: 65536,
            peer_receive_window: 65536,
            max_payload: 16384,
            coalesce_delay_ms: 100,
            close_timeout_ms: 5000,
            io_timeout_ms: 3000,
            interaction_budget_ms: 120_000,
            max_waiters: 64,
        }
    }
    fn validate(&self) -> Result<(), Error> {
        for limits in [&self.receive_limits, &self.peer_limits] {
            crate::codec::validate_limits(limits).map_err(|_| Error::InvalidConfig)?;
            crate::binding::usable(limits).map_err(|_| Error::InvalidConfig)?;
        }
        if self.session_id.is_empty()
            || self.session_id.len() > 128
            || self.stream_id <= 0
            || !(1..=2).contains(&self.profile_version)
            || self.send_buffer == 0
            || self.send_buffer > 4 * 1024 * 1024
            || self.receive_window == 0
            || self.receive_window > 4 * 1024 * 1024
            || self.peer_receive_window == 0
            || self.peer_receive_window > 4 * 1024 * 1024
            || self.max_payload == 0
            || self.max_payload > 65536
            || self.close_timeout_ms == 0
            || self.io_timeout_ms > i32::MAX as u64
            || self.interaction_budget_ms > 86_400_000
            || self.max_waiters == 0
            || self.max_waiters > 1024
            || self.receive_window as i64 > self.receive_limits.receive_window
            || self.peer_receive_window as i64 > self.peer_limits.receive_window
            || self.max_payload as i64
                > self
                    .receive_limits
                    .data_payload
                    .min(self.peer_limits.data_payload)
            || self.send_buffer as i64
                > self.peer_limits.queued_bytes - self.peer_limits.control_reserve_bytes
        {
            return Err(Error::InvalidConfig);
        }
        // One bounded construction-time probe guarantees the largest emitted
        // Data fits every negotiated admission budget at maximum byte offset.
        let probe = crate::protocol::Envelope {
            version: 1,
            session_id: self.session_id.clone(),
            stream_id: self.stream_id,
            kind: crate::protocol::MessageKind::Data,
            data: Some(crate::protocol::Data {
                offset: i64::MAX - self.max_payload as i64,
                payload: vec![0; self.max_payload],
            }),
            ..Default::default()
        };
        for limits in [&self.receive_limits, &self.peer_limits] {
            crate::codec::admit_limited(&probe, limits).map_err(|_| Error::InvalidConfig)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Error {
    WouldBlock,
    InvalidConfig,
    Protocol,
    Cancelled,
    CarrierLost,
    Timeout,
    WrongState,
    WriteClosed,
    Closed,
    WrongSide,
    WaiterCapacity,
    PeerFailed {
        code: crate::protocol::ErrorCode,
        effect: crate::protocol::Effect,
    },
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
