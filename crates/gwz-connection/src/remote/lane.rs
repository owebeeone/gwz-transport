//! The ports a carrier gives the remote module (GwzSplitCommandChannel section 3.3 and 3.4): the lane the transport
//! messages travel on, and what each end knows of its session.
//!
//! `gwz-connection` depends on `gwz-transport` only, so it cannot name core's `FrameSink` or `FrameRouter`; the
//! composition (core's adapter, or a test's fake carrier) implements these. Messages are gwz-transport's own
//! `Attachment`, `(request name, Envelope)`: the request name is the lane frame's `request_id`, the call this client
//! sent, and `Envelope.session_id` names the binding. Neither port decodes an envelope or waits.
use crate::{Clock, HostNamespace, Progress};
use gwz_transport::mux::Attachment;
use std::{sync::Arc, task::Waker};

/// Why a message was not queued. The message comes back: a lane never drops one.
#[derive(Debug)]
pub enum LaneSend {
    /// The lane's queue for this kind is full; nothing was queued and the lane stays open. The registered waker is
    /// woken when there is room.
    Full(Box<Attachment>),
    /// The carrier has ended.
    Closed(Box<Attachment>),
}

/// The carrier has ended. Its reason is the carrier's; the remote module needs only that it is over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LaneEnded;

/// One end of a carrier's transport lane, both directions.
pub trait Lane: Send + Sync + 'static {
    /// Queues `message` for the peer. Never blocks. The caller keeps each binding's messages in order.
    fn send(&self, message: Attachment) -> Result<(), LaneSend>;
    /// The next message from the peer, whichever binding it is for, or `None` when nothing has arrived. After the
    /// peer's close the messages sent before it still arrive, then the end.
    fn recv(&self) -> Result<Option<Attachment>, LaneEnded>;
    /// Wakes `waker` when a message arrives, a full queue gets room, or the carrier ends. One waker per lane: a later
    /// registration replaces the earlier.
    fn register(&self, waker: &Waker);
    /// The binding `session` is over on this end (retired, refused, faulted or revoked): the lane may drop what it
    /// keeps for it, and discards what still arrives for it. The default keeps nothing. A byte-stream carrier routes
    /// the frames of open bindings only, so the composition that opens a binding (`CarrierLane` opens the core's as the
    /// `bind` goes out) retires it here.
    fn retire(&self, session: &str) {
        let _ = session;
    }
    /// Whether the carrier faulted the binding `session` (it broke the binding's bounds or rules: contract section 6).
    /// The carrier drops what the binding had queued, so the binding learns of the fault here; it ends alone, and the
    /// carrier's other bindings go on. The default says no.
    fn faulted(&self, session: &str) -> bool {
        let _ = session;
        false
    }
}

/// What the core end knows of its session: the lane, the clock, the executor its driver runs on, and the live-call
/// registry of step 2.2b. It is the constructor argument of `remote::handle`, not a fifth `HostPort`.
pub trait CoreBinding: Lane {
    /// The nonsecret namespace this session's binding identifiers are minted under: never shared with another
    /// session of the same core.
    fn namespace(&self) -> HostNamespace;
    fn clock(&self) -> Arc<dyn Clock>;
    /// The executor the handle's driver runs on, so that its bindings progress with no application call.
    fn progress(&self) -> Arc<dyn Progress>;
    /// Whether `request_id` names a live call this client sent on this carrier.
    fn call_is_live(&self, request_id: &str) -> bool;
    /// The `endpoint_path_base` the client sent with that call's `TransportOptions`, which an explicit-key `Open`
    /// must echo (section 5.4).
    fn endpoint_path_base(&self, request_id: &str) -> Option<String>;
}

/// What the client end knows of its session.
pub trait ClientSession: Lane {
    fn clock(&self) -> Arc<dyn Clock>;
    /// The executor the receiver's driver runs on.
    fn progress(&self) -> Arc<dyn Progress>;
}
