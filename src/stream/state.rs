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
    pub(super) retained_failure: Option<Failure>,
    pub(super) terminal_message: Option<Envelope>,
    pub(super) peak_send: usize,
    pub(super) peak_recv: usize,
    pub(super) revision: u64,
}
