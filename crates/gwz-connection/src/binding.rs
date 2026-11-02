//! One operation's binding: a fresh session identifier and the two mux sessions that belong to it (contract D1, D6, section 6).
//!
//! The driver half and the endpoint half of the transport session are both made for the operation and die with it, so the
//! per-session registration limit (256 by default) is the binding's own and no host-wide count or rollover exists. A
//! binding identifier is never reused; a frame for an identifier that is absent or retired is discarded without a record
//! being made for it, and a binding is valid only on the carrier connection that bound it (today the host's in-process
//! link). After the Bind/Bound bootstrap the muxes carry registration and cancellation; stream data crosses the adapter's
//! `ExchangeIo` until the ordered-stream seam of contract section 6.1 exists in gwz-transport.
use crate::{
    AuthContext, CleanupStatus, CleanupTicket, EndpointRoute, ExchangeSpec, HostNamespace,
    OperationEnd, TicketStatus,
    cell::Cell,
    instance::{Instance, InstanceHold},
    operation::TerminalReason,
};
use gwz_transport::{
    binding as transport_binding,
    mux::{self, Attachment, Mux},
    pool::{ConnectionId, Key},
    protocol::Limits,
};
use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
    },
};

/// A binding's identifier: its host's namespace and a serial the host never issues twice.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BindingId {
    pub(crate) host: HostNamespace,
    pub(crate) serial: u64,
}
impl BindingId {
    /// The namespace of the host that issued it.
    pub fn host(self) -> HostNamespace {
        self.host
    }
    /// The serial that host never issues twice: with the namespace, a key by which an adapter keeps state per binding.
    pub fn serial(self) -> u64 {
        self.serial
    }
}

/// A carrier connection, as a host knows it. The in-process link is one per host.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CarrierId(pub(crate) HostNamespace);

/// Which half of a binding a frame is for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    ToEndpoint,
    ToDriver,
}
/// Why a frame was not delivered.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Discard {
    ForeignCarrier,
    ForeignHost,
    /// The binding is absent or retired.
    Retired,
    /// The frame belongs to another binding's session.
    WrongSession,
    /// The binding refused it and is cancelled.
    Rejected,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Routed {
    Delivered,
    Discarded(Discard),
}

/// The transport limits of every binding (the endpoint stream's receive window, as the placement uses).
pub(crate) fn limits() -> Limits {
    let mut limits = transport_binding::default_limits();
    limits.receive_window = 65536;
    limits
}

/// The configuration of both mux halves of a binding. Everything but the limits and the registration bound is the core
/// mux's default, including its bootstrap bound, which the host's default admission bound stays below (`host.rs`).
pub(crate) fn mux_config(registrations: usize) -> mux::Config {
    mux::Config {
        limits: limits(),
        max_requests: registrations,
        ..Default::default()
    }
}

/// The session identifier of a binding.
pub(crate) fn session_name(id: BindingId) -> String {
    format!("{:032x}.{}", id.host.get(), id.serial)
}

/// Both mux halves of a binding.
pub(crate) struct MuxPair {
    pub(crate) driver: Mux,
    pub(crate) endpoint: Mux,
}
/// A bootstrap or registration that the muxes refused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Fault {
    /// The binding's own registration limit.
    Limit,
    Protocol,
}
impl MuxPair {
    /// Makes the two halves, registers `request` on both and carries Bind/Bound between them in memory (no I/O).
    pub(crate) fn bootstrapped(
        id: BindingId,
        route: &EndpointRoute,
        registrations: usize,
        request: &str,
        operation: &str,
    ) -> Result<Self, Fault> {
        let session = session_name(id);
        let config = mux_config(registrations);
        let driver = Mux::initiator(&session, config.clone()).map_err(|_| Fault::Protocol)?;
        let endpoint = Mux::endpoint(&session, route.endpoint_config(&session), config)
            .map_err(|_| Fault::Protocol)?;
        let mut pair = Self { driver, endpoint };
        pair.register(request, operation)?;
        pair.driver.begin(request).map_err(|_| Fault::Protocol)?;
        for _ in 0..8 {
            let mut moved = false;
            while let Some(item) = pair.driver.next_message() {
                pair.endpoint.receive(&item).map_err(|_| Fault::Protocol)?;
                moved = true;
            }
            while let Some(item) = pair.endpoint.next_message() {
                pair.driver.receive(&item).map_err(|_| Fault::Protocol)?;
                moved = true;
            }
            if !moved {
                break;
            }
        }
        if pair.driver.phase() != mux::Phase::Ready || pair.endpoint.phase() != mux::Phase::Ready {
            return Err(Fault::Protocol);
        }
        Ok(pair)
    }
    /// One request on both halves: the unit the per-binding limit counts.
    pub(crate) fn register(&mut self, request: &str, operation: &str) -> Result<(), Fault> {
        self.driver
            .register(request, Some(operation.into()))
            .map_err(fault)?;
        self.endpoint.register(request, None).map_err(fault)
    }
    pub(crate) fn deliver(
        &mut self,
        direction: Direction,
        item: &Attachment,
    ) -> Result<(), mux::Error> {
        match direction {
            Direction::ToEndpoint => self.endpoint.receive(item),
            Direction::ToDriver => self.driver.receive(item),
        }
    }
}
fn fault(error: mux::Error) -> Fault {
    if error == mux::Error::Capacity {
        Fault::Limit
    } else {
        Fault::Protocol
    }
}

/// The tickets of the disposals that an operation's exchanges left unconfirmed when they ended (contract section 5: a
/// terminal receipt states its cleanup). Bounded: one ticket per connection still closing, and a confirmed one is
/// dropped at the next note.
#[derive(Default)]
pub(crate) struct Cleanups(Mutex<Vec<CleanupTicket>>);
impl Cleanups {
    pub(crate) fn note(&self, ticket: CleanupTicket) {
        let mut tickets = self.0.lock().unwrap_or_else(|e| e.into_inner());
        tickets.retain(|known| known.status() == TicketStatus::Pending);
        if ticket.status() == TicketStatus::Pending
            && !tickets
                .iter()
                .any(|known| known.generation() == ticket.generation())
        {
            tickets.push(ticket);
        }
    }
    /// `Pending` with the first disposal still unconfirmed, or `Complete`.
    pub(crate) fn status(&self) -> CleanupStatus {
        let tickets = self.0.lock().unwrap_or_else(|e| e.into_inner());
        tickets
            .iter()
            .find(|ticket| ticket.status() == TicketStatus::Pending)
            .map_or(CleanupStatus::Complete, |ticket| {
                CleanupStatus::Pending(ticket.clone())
            })
    }
}

/// The most entries a binding's affinity table holds; past it the oldest goes (HTTPS model 3.1).
pub(crate) const AFFINITY_ENTRIES: usize = 256;
/// The longest repository path the affinity table records, in bytes; a longer one is not recorded.
pub(crate) const AFFINITY_PATH_BYTES: usize = 1024;

/// What an exchange's connection is noted under in its binding's affinity table: its pool key and repository path.
pub(crate) struct AffinityKey {
    key: Key,
    path: String,
}
impl AffinityKey {
    /// `spec`'s key and path, or `None` for a path the table does not record (over [`AFFINITY_PATH_BYTES`]).
    pub(crate) fn of(spec: &ExchangeSpec) -> Option<Self> {
        (spec.path().len() <= AFFINITY_PATH_BYTES).then(|| Self {
            key: spec.key().clone(),
            path: spec.path().into(),
        })
    }
}

/// The connection each (pool key, repository path) of this binding last ran on while that connection was authenticated by
/// a mechanism that lives on it (`ExchangeIo::connection_bound`; HTTPS model 3.1, Consistency P3-4): the binding's next
/// exchange of the key and path prefers it (`Request::prefer`). Binding state, so a hint never crosses bindings, and a
/// hint never replaces `authorize_reuse`. Bounded to [`AFFINITY_ENTRIES`], the oldest evicted; the pool `Key` is
/// `PartialEq` only, so it is scanned linearly. A connection id is never reused, so a stale entry names nothing.
#[derive(Default)]
pub(crate) struct Affinity(Mutex<VecDeque<(AffinityKey, ConnectionId)>>);
impl Affinity {
    /// Records that `at` ran on `connection`, replacing (and renewing) an entry of the same key and path.
    pub(crate) fn note(&self, at: AffinityKey, connection: ConnectionId) {
        let mut entries = self.0.lock().unwrap_or_else(|e| e.into_inner());
        entries.retain(|(known, _)| known.key != at.key || known.path != at.path);
        if entries.len() >= AFFINITY_ENTRIES {
            entries.pop_front();
        }
        entries.push_back((at, connection));
    }
    /// The connection the binding prefers for an exchange of `spec`'s key and path, if any.
    pub(crate) fn preferred(&self, spec: &ExchangeSpec) -> Option<ConnectionId> {
        let entries = self.0.lock().unwrap_or_else(|e| e.into_inner());
        entries
            .iter()
            .find(|(known, _)| known.key == *spec.key() && known.path == spec.path())
            .map(|(_, connection)| *connection)
    }
    #[allow(dead_code, reason = "the affinity rows read the table's size")]
    pub(crate) fn len(&self) -> usize {
        self.0.lock().unwrap_or_else(|e| e.into_inner()).len()
    }
}

/// What the host table and the operation both hold for one registered binding.
pub(crate) struct BindingShared {
    pub(crate) id: BindingId,
    pub(crate) session: String,
    pub(crate) operation: String,
    /// The operation's limits: every pool request this binding makes carries them (reuse design section 5).
    pub(crate) limits: gwz_transport::pool::Capacity,
    pub(crate) carrier: CarrierId,
    pub(crate) pair: Mutex<MuxPair>,
    pub(crate) instance: Arc<Instance>,
    /// Held for the binding's life, from the admission's attach: the host does not retire an instance a binding is on.
    pub(crate) _hold: InstanceHold,
    /// The operation's authentication context: the binding's, for as long as it lives, and never the instance's.
    pub(crate) auth: AuthContext,
    /// Shared with the binding's pending openings, so that an opening that asks the pool again can see the cancellation.
    pub(crate) cancelled: Arc<AtomicBool>,
    /// Why the host cancelled it, when it did (0 none, 1 host closing, 2 domain closing).
    pub(crate) closing: AtomicUsize,
    pub(crate) sealed: AtomicBool,
    pub(crate) retired: AtomicBool,
    /// Openings and exchanges that still hold the binding open.
    pub(crate) live: AtomicUsize,
    pub(crate) opened: AtomicU64,
    pub(crate) next_exchange: AtomicU64,
    pub(crate) done: Cell<OperationEnd>,
    /// The disposals its exchanges left unconfirmed, which its end states.
    pub(crate) cleanups: Arc<Cleanups>,
    /// The connections its exchanges prefer, by key and path (HTTPS model 3.1).
    pub(crate) affinity: Affinity,
    /// Held for the binding's life: the retained-input charge goes back when it does. Its single-admission claim is
    /// freed if the binding never registers and consumed when it does.
    pub(crate) charge: crate::host::ChargeGuard,
}
pub(crate) fn closing_code(reason: TerminalReason) -> usize {
    match reason {
        TerminalReason::HostClosing => 1,
        TerminalReason::DomainClosing => 2,
        // The reasons of a carrier-backed registration never reach a host's binding.
        _ => 3,
    }
}
pub(crate) fn closing_reason(code: usize) -> Option<TerminalReason> {
    match code {
        1 => Some(TerminalReason::HostClosing),
        2 => Some(TerminalReason::DomainClosing),
        3 => Some(TerminalReason::HostFault),
        _ => None,
    }
}
