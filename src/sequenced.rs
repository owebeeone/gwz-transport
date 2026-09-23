//! Profile-3 typed-message ordering kernel. The host still owns request
//! authority, application admission, delivery tickets and physical delivery.
use crate::{binding, codec, protocol::*};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

const MAX_LIFETIME_STREAMS: usize = 4096;
const MAX_ACTIVE_STREAMS: usize = 64;
const MAX_REORDER_PER_STREAM: usize = 8;
const MAX_REORDER_PER_BINDING: usize = 32;
const GAP_TIMEOUT_MS: u64 = 30_000;
const RECORD_CHARGE: usize = std::mem::size_of::<StreamState>() + 256;
const FRAME_OVERHEAD: usize = std::mem::size_of::<Pending>() + 128;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Initiator,
    Endpoint,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Error {
    InvalidMessage,
    WrongSession,
    Capacity,
    WouldBlock,
    Protocol,
    GapTimeout,
    ApplyRejected,
    Closed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ApplyError {
    WouldBlock,
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    Buffered,
    Applied,
    Superseded,
    AlreadyResolved,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Resolution {
    pub stream_id: i64,
    pub message_seq: i64,
    pub status: Status,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Report {
    pub submitted: Status,
    pub resolved: Vec<Resolution>,
}

pub(crate) fn valid_limits(limits: &Limits) -> Result<(), Error> {
    codec::admit_limited(&binding::offer("probe", EndpointRole::Local), limits)
        .map_err(|_| Error::Capacity)?;
    let worst = limits
        .encoded_frame
        .checked_add(limits.decode_allocation)
        .and_then(|sum| sum.checked_add(FRAME_OVERHEAD as i64))
        .ok_or(Error::Capacity)?;
    let required = worst
        .checked_mul(2)
        .and_then(|sum| sum.checked_add(limits.control_reserve_bytes))
        .and_then(|sum| sum.checked_add(RECORD_CHARGE as i64))
        .ok_or(Error::Capacity)?;
    if limits.queued_bytes < required || limits.queued_frames < limits.control_reserve_frames + 2 {
        return Err(Error::Capacity);
    }
    Ok(())
}

fn first_kind(direction: Direction, kind: MessageKind) -> bool {
    match direction {
        Direction::Initiator => matches!(kind, MessageKind::Open | MessageKind::CheckIdentity),
        Direction::Endpoint => matches!(
            kind,
            MessageKind::Opened
                | MessageKind::OpenFailed
                | MessageKind::IdentityChecked
                | MessageKind::IdentityCheckFailed
                | MessageKind::Failed
        ),
    }
}

fn abortive(kind: MessageKind) -> bool {
    matches!(
        kind,
        MessageKind::OpenFailed
            | MessageKind::IdentityCheckFailed
            | MessageKind::Failed
            | MessageKind::Cancel
    )
}

fn sender_final(kind: MessageKind) -> bool {
    abortive(kind) || matches!(kind, MessageKind::Closed | MessageKind::IdentityChecked)
}

fn charge(message: &Envelope, limits: &Limits) -> Result<usize, Error> {
    codec::allocation_charge(message, limits).map_err(|_| Error::InvalidMessage)
}

/// Assigns one direction's sequence when a frame enters a bounded local queue.
/// Both mux and stream producers must use the same instance for that direction.
pub struct Outbound {
    session_id: String,
    direction: Direction,
    limits: Limits,
    max_queued: usize,
    queue: VecDeque<(Envelope, usize)>,
    queued_charge: usize,
    next: BTreeMap<i64, i64>,
    ended: BTreeMap<i64, bool>,
    close_sent: BTreeSet<i64>,
}

impl Outbound {
    pub fn new(
        session_id: &str,
        direction: Direction,
        limits: Limits,
        max_queued: usize,
    ) -> Result<Self, Error> {
        valid_limits(&limits)?;
        if session_id.is_empty()
            || max_queued == 0
            || max_queued > (limits.queued_frames - limits.control_reserve_frames) as usize
        {
            return Err(Error::Capacity);
        }
        Ok(Self {
            session_id: session_id.into(),
            direction,
            limits,
            max_queued,
            queue: VecDeque::new(),
            queued_charge: 0,
            next: BTreeMap::new(),
            ended: BTreeMap::new(),
            close_sent: BTreeSet::new(),
        })
    }

    pub fn enqueue(&mut self, mut message: Envelope) -> Result<i64, Error> {
        if message.session_id != self.session_id {
            return Err(Error::WrongSession);
        }
        if message.version != 3 || message.stream_id <= 0 || message.message_seq.is_some() {
            return Err(Error::InvalidMessage);
        }
        if self.ended.contains_key(&message.stream_id) {
            return Err(Error::Closed);
        }
        if self.close_sent.contains(&message.stream_id)
            && !matches!(
                message.kind,
                MessageKind::Window
                    | MessageKind::Flushed
                    | MessageKind::Cancel
                    | MessageKind::Failed
            )
        {
            return Err(Error::Protocol);
        }
        let new_stream = !self.next.contains_key(&message.stream_id);
        if new_stream && self.next.len() >= MAX_LIFETIME_STREAMS {
            return Err(Error::Capacity);
        }
        if new_stream && self.next.len().saturating_sub(self.ended.len()) >= MAX_ACTIVE_STREAMS {
            return Err(Error::Capacity);
        }
        if new_stream && !first_kind(self.direction, message.kind) {
            return Err(Error::Protocol);
        }
        let sequence = self.next.get(&message.stream_id).copied().unwrap_or(1);
        let next = sequence.checked_add(1).ok_or(Error::Capacity)?;
        message.message_seq = Some(sequence);
        let frame_charge = charge(&message, &self.limits)?.saturating_add(FRAME_OVERHEAD);
        let control = message.kind != MessageKind::Data;
        let frame_limit = self.max_queued
            + if control {
                self.limits.control_reserve_frames as usize
            } else {
                0
            };
        let byte_limit = (self.limits.queued_bytes
            - if control {
                0
            } else {
                self.limits.control_reserve_bytes
            }) as usize;
        if self.queue.len() >= frame_limit
            || self.queued_charge.saturating_add(frame_charge) > byte_limit
        {
            return Err(Error::WouldBlock);
        }
        self.queued_charge += frame_charge;
        self.queue.push_back((message.clone(), frame_charge));
        self.next.insert(message.stream_id, next);
        if message.kind == MessageKind::Close {
            self.close_sent.insert(message.stream_id);
        }
        if sender_final(message.kind) {
            self.ended.insert(message.stream_id, true);
        }
        Ok(sequence)
    }

    pub fn pop(&mut self) -> Option<Envelope> {
        self.queue.pop_front().map(|(message, charge)| {
            self.queued_charge -= charge;
            message
        })
    }
}

struct Pending {
    message: Envelope,
    charge: usize,
}

struct StreamState {
    expected: i64,
    pending: BTreeMap<i64, Pending>,
    gap_since: Option<u64>,
    opening: Option<Envelope>,
    terminal: Option<(i64, Envelope)>,
}

impl StreamState {
    fn new() -> Self {
        Self {
            expected: 1,
            pending: BTreeMap::new(),
            gap_since: None,
            opening: None,
            terminal: None,
        }
    }
}

/// Reassembles one sending direction. The host must validate and register the
/// request context before calling `register`, and the apply callback must make
/// the ordinary mux/stream transition atomically or return `WouldBlock` with
/// no effect. Its resolutions are the input to host-owned delivery tickets.
pub struct Receiver {
    session_id: String,
    direction: Direction,
    limits: Limits,
    streams: BTreeMap<i64, StreamState>,
    buffered_frames: usize,
    buffered_charge: usize,
    retained_charge: usize,
    failed: bool,
}

impl Receiver {
    pub fn new(session_id: &str, direction: Direction, limits: Limits) -> Result<Self, Error> {
        valid_limits(&limits)?;
        if session_id.is_empty() {
            return Err(Error::InvalidMessage);
        }
        Ok(Self {
            session_id: session_id.into(),
            direction,
            limits,
            streams: BTreeMap::new(),
            buffered_frames: 0,
            buffered_charge: 0,
            retained_charge: 0,
            failed: false,
        })
    }

    pub fn register(&mut self, stream_id: i64) -> Result<(), Error> {
        if self.failed {
            return Err(Error::Closed);
        }
        if stream_id <= 0 || self.streams.contains_key(&stream_id) {
            return Err(Error::Protocol);
        }
        let active = self
            .streams
            .values()
            .filter(|state| state.terminal.is_none())
            .count();
        if self.streams.len() >= MAX_LIFETIME_STREAMS || active >= MAX_ACTIVE_STREAMS {
            return Err(Error::Capacity);
        }
        let records = self
            .streams
            .len()
            .saturating_add(1)
            .saturating_mul(RECORD_CHARGE);
        if records
            .saturating_add(self.retained_charge)
            .saturating_add(self.buffered_charge)
            .saturating_add(self.predecessor_reserve())
            > self.available_bytes()
        {
            return Err(Error::Capacity);
        }
        self.streams.insert(stream_id, StreamState::new());
        Ok(())
    }

    pub fn buffered_frames(&self) -> usize {
        self.buffered_frames
    }

    pub fn advance(&mut self, now_ms: u64) -> Result<(), Error> {
        if self.failed {
            return Err(Error::Closed);
        }
        if self.streams.values().any(|state| {
            state
                .gap_since
                .is_some_and(|start| now_ms >= start.saturating_add(GAP_TIMEOUT_MS))
        }) {
            self.failed = true;
            return Err(Error::GapTimeout);
        }
        Ok(())
    }

    pub fn submit<F>(
        &mut self,
        message: Envelope,
        now_ms: u64,
        mut apply: F,
    ) -> Result<Report, Error>
    where
        F: FnMut(&Envelope) -> Result<(), ApplyError>,
    {
        self.advance(now_ms)?;
        if message.session_id != self.session_id {
            return Err(Error::WrongSession);
        }
        if message.version != 3 || message.stream_id <= 0 {
            return Err(Error::InvalidMessage);
        }
        let sequence = message.message_seq.ok_or(Error::InvalidMessage)?;
        let frame_charge = charge(&message, &self.limits)?.saturating_add(FRAME_OVERHEAD);
        let stream_id = message.stream_id;
        let mut state = self.streams.remove(&stream_id).ok_or(Error::Protocol)?;
        let result = self.submit_inner(
            &mut state,
            message,
            sequence,
            frame_charge,
            now_ms,
            &mut apply,
        );
        self.streams.insert(stream_id, state);
        if matches!(result, Err(Error::Protocol | Error::ApplyRejected)) {
            self.failed = true;
        }
        result
    }

    /// Retry a predecessor whose application previously returned WouldBlock.
    /// The host calls this after the application queue or read buffer advances.
    pub fn retry<F>(&mut self, stream_id: i64, now_ms: u64, mut apply: F) -> Result<Report, Error>
    where
        F: FnMut(&Envelope) -> Result<(), ApplyError>,
    {
        self.advance(now_ms)?;
        let mut state = self.streams.remove(&stream_id).ok_or(Error::Protocol)?;
        let mut resolved = Vec::new();
        let result = self
            .drain_pending(&mut state, stream_id, &mut apply, &mut resolved)
            .map(|()| Report {
                submitted: if resolved.is_empty() {
                    if state.pending.is_empty() {
                        Status::AlreadyResolved
                    } else {
                        Status::Buffered
                    }
                } else {
                    Status::Applied
                },
                resolved,
            });
        self.streams.insert(stream_id, state);
        if matches!(result, Err(Error::Protocol | Error::ApplyRejected)) {
            self.failed = true;
        }
        result
    }

    fn submit_inner<F>(
        &mut self,
        state: &mut StreamState,
        message: Envelope,
        sequence: i64,
        frame_charge: usize,
        now_ms: u64,
        apply: &mut F,
    ) -> Result<Report, Error>
    where
        F: FnMut(&Envelope) -> Result<(), ApplyError>,
    {
        let stream_id = message.stream_id;
        if let Some((terminal_sequence, terminal)) = &state.terminal {
            if sequence == 1
                && state
                    .opening
                    .as_ref()
                    .is_some_and(|opening| opening != &message)
            {
                return Err(Error::Protocol);
            }
            let submitted = if sequence < *terminal_sequence {
                if sequence < state.expected {
                    Status::AlreadyResolved
                } else {
                    Status::Superseded
                }
            } else if sequence == *terminal_sequence && &message == terminal {
                Status::AlreadyResolved
            } else {
                return Err(Error::Protocol);
            };
            return Ok(Report {
                submitted,
                resolved: Vec::new(),
            });
        }
        if sequence < state.expected {
            if sequence == 1 && state.opening.as_ref() != Some(&message) {
                return Err(Error::Protocol);
            }
            return Ok(Report {
                submitted: Status::AlreadyResolved,
                resolved: Vec::new(),
            });
        }
        if let Some(pending) = state.pending.get(&sequence) {
            if pending.message != message {
                return Err(Error::Protocol);
            }
            return Ok(Report {
                submitted: Status::Buffered,
                resolved: Vec::new(),
            });
        }
        if sequence == 1 && !first_kind(self.direction, message.kind) {
            return Err(Error::Protocol);
        }
        if sequence > state.expected
            && abortive(message.kind)
            && (state.opening.is_some()
                || (self.direction == Direction::Initiator && message.kind == MessageKind::Cancel))
        {
            if state
                .pending
                .range(..sequence)
                .any(|(_, pending)| sender_final(pending.message.kind))
            {
                return Err(Error::Protocol);
            }
            if state
                .pending
                .range((sequence.saturating_add(1))..)
                .next()
                .is_some()
            {
                return Err(Error::Protocol);
            }
            let superseded_charge: usize = state.pending.values().map(|p| p.charge).sum();
            self.check_retain(frame_charge, superseded_charge)?;
            apply(&message).map_err(|error| match error {
                ApplyError::WouldBlock => Error::WouldBlock,
                ApplyError::Rejected => Error::ApplyRejected,
            })?;
            let mut resolved = Vec::new();
            for (old_sequence, pending) in std::mem::take(&mut state.pending) {
                self.uncharge(pending.charge);
                resolved.push(Resolution {
                    stream_id,
                    message_seq: old_sequence,
                    status: Status::Superseded,
                });
            }
            state.terminal = Some((sequence, message));
            self.retained_charge += frame_charge;
            state.gap_since = None;
            resolved.push(Resolution {
                stream_id,
                message_seq: sequence,
                status: Status::Applied,
            });
            return Ok(Report {
                submitted: Status::Applied,
                resolved,
            });
        }
        if sequence > state.expected {
            self.buffer(state, message, frame_charge, now_ms, true)?;
            return Ok(Report {
                submitted: Status::Buffered,
                resolved: Vec::new(),
            });
        }
        let mut resolved = Vec::new();
        if sender_final(message.kind) && !state.pending.is_empty() {
            return Err(Error::Protocol);
        }
        if sequence == 1 || sender_final(message.kind) {
            self.check_retain(frame_charge, 0)?;
        }
        match apply(&message) {
            Ok(()) => {
                self.mark_applied(state, &message, frame_charge)?;
                resolved.push(Resolution {
                    stream_id,
                    message_seq: sequence,
                    status: Status::Applied,
                });
            }
            Err(ApplyError::WouldBlock) => {
                self.buffer(state, message, frame_charge, now_ms, false)?;
                return Ok(Report {
                    submitted: Status::Buffered,
                    resolved,
                });
            }
            Err(ApplyError::Rejected) => {
                return Err(Error::ApplyRejected);
            }
        }
        self.drain_pending(state, stream_id, apply, &mut resolved)?;
        Ok(Report {
            submitted: Status::Applied,
            resolved,
        })
    }

    fn drain_pending<F>(
        &mut self,
        state: &mut StreamState,
        stream_id: i64,
        apply: &mut F,
        resolved: &mut Vec<Resolution>,
    ) -> Result<(), Error>
    where
        F: FnMut(&Envelope) -> Result<(), ApplyError>,
    {
        while let Some(pending) = state.pending.remove(&state.expected) {
            if sender_final(pending.message.kind) && !state.pending.is_empty() {
                state.pending.insert(state.expected, pending);
                return Err(Error::Protocol);
            }
            match apply(&pending.message) {
                Ok(()) => {
                    self.uncharge(pending.charge);
                    self.mark_applied(state, &pending.message, pending.charge)?;
                    resolved.push(Resolution {
                        stream_id,
                        message_seq: pending.message.message_seq.unwrap(),
                        status: Status::Applied,
                    });
                }
                Err(ApplyError::WouldBlock) => {
                    state.pending.insert(state.expected, pending);
                    break;
                }
                Err(ApplyError::Rejected) => {
                    return Err(Error::ApplyRejected);
                }
            }
        }
        if state.pending.is_empty() {
            state.gap_since = None;
        }
        Ok(())
    }

    fn mark_applied(
        &mut self,
        state: &mut StreamState,
        message: &Envelope,
        charge: usize,
    ) -> Result<(), Error> {
        let sequence = message.message_seq.unwrap();
        if sequence == 1 && !sender_final(message.kind) {
            state.opening = Some(message.clone());
            self.retained_charge += charge;
        }
        state.expected = sequence.checked_add(1).ok_or(Error::Capacity)?;
        if sender_final(message.kind) {
            if !state.pending.is_empty() {
                return Err(Error::Protocol);
            }
            state.terminal = Some((sequence, message.clone()));
            if sequence != 1 {
                self.retained_charge += charge;
            }
        }
        Ok(())
    }

    fn buffer(
        &mut self,
        state: &mut StreamState,
        message: Envelope,
        charge: usize,
        now_ms: u64,
        out_of_order: bool,
    ) -> Result<(), Error> {
        let out_of_order_count = state
            .pending
            .keys()
            .filter(|sequence| **sequence > state.expected)
            .count();
        if out_of_order
            && (out_of_order_count >= MAX_REORDER_PER_STREAM
                || self.buffered_frames >= MAX_REORDER_PER_BINDING)
        {
            return Err(Error::WouldBlock);
        }
        let predecessor_reserve = if out_of_order {
            self.predecessor_reserve()
        } else {
            0
        };
        let available = (self.limits.queued_bytes - self.limits.control_reserve_bytes) as usize;
        let records = self
            .streams
            .len()
            .saturating_add(1)
            .saturating_mul(RECORD_CHARGE);
        if self
            .buffered_charge
            .saturating_add(self.retained_charge)
            .saturating_add(records)
            .saturating_add(charge)
            .saturating_add(predecessor_reserve)
            > available
            || self.buffered_frames + 1 + usize::from(out_of_order)
                > (self.limits.queued_frames - self.limits.control_reserve_frames) as usize
        {
            return Err(Error::WouldBlock);
        }
        self.buffered_frames += 1;
        self.buffered_charge += charge;
        state.gap_since.get_or_insert(now_ms);
        state
            .pending
            .insert(message.message_seq.unwrap(), Pending { message, charge });
        Ok(())
    }

    fn uncharge(&mut self, charge: usize) {
        self.buffered_frames -= 1;
        self.buffered_charge -= charge;
    }

    fn available_bytes(&self) -> usize {
        (self.limits.queued_bytes - self.limits.control_reserve_bytes) as usize
    }

    fn predecessor_reserve(&self) -> usize {
        (self.limits.encoded_frame + self.limits.decode_allocation) as usize + FRAME_OVERHEAD
    }

    fn check_retain(&self, charge: usize, releasing: usize) -> Result<(), Error> {
        let records = self
            .streams
            .len()
            .saturating_add(1)
            .saturating_mul(RECORD_CHARGE);
        let required = records
            .saturating_add(self.retained_charge)
            .saturating_add(self.buffered_charge.saturating_sub(releasing))
            .saturating_add(charge);
        if required > self.available_bytes() {
            return Err(Error::WouldBlock);
        }
        Ok(())
    }
}
