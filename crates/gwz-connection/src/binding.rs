//! One operation's binding: a fresh session identifier and the two mux sessions that belong to it (contract D1, D6, section 6).
//!
//! The driver half and the endpoint half of the transport session are both made for the operation and die with it, so the
//! per-session registration limit (256 by default) is the binding's own and no host-wide count or rollover exists. A
//! binding identifier is never reused; a frame for an identifier that is absent or retired is discarded without a record
//! being made for it, and a binding is valid only on the carrier connection that bound it (today the host's in-process
//! link). After the Bind/Bound bootstrap the muxes carry registration and cancellation; stream data crosses the adapter's
//! `ExchangeIo` until the ordered-stream seam of contract section 6.1 exists in gwz-transport.
use crate::{
    AuthContext, EndpointRoute, HostNamespace, OperationEnd, cell::Cell, instance::Instance,
    operation::TerminalReason,
};
use gwz_transport::{
    binding as transport_binding,
    mux::{self, Attachment, Mux},
    protocol::Limits,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, AtomicUsize},
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
    /// Held for the binding's life: the retained-input charge goes back when it does.
    pub(crate) _charge: crate::host::ChargeGuard,
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
