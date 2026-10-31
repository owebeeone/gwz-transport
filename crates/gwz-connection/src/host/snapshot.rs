//! The host's phase and its fixed-size snapshot (contract section 11).
use crate::HostNamespace;
use gwz_transport::pool;

/// Where a host is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostPhase {
    Open,
    /// Sealed: admission refused, disposal under way.
    Closing,
    /// Nothing is left that the host owns: its disposal is confirmed, or belongs to the retainer.
    Closed,
}
/// Fixed-size counts. Step 1.2 adds the pool, binding and exchange counters of contract section 11. On a carrier-backed
/// handle the snapshot is a refusal ([`refused`](Self::refused)): every count is meaningless there, and only `phase` and
/// `namespace` are the handle's own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct HostSnapshot {
    pub phase: HostPhase,
    pub namespace: HostNamespace,
    pub endpoint_instances: usize,
    /// Records the host's domain retains now (all hosts on it).
    pub retained_cleanup: usize,
    /// Registered operations' bindings.
    pub bindings: usize,
    /// Slots in use: reserved by a permit or held by a registered binding.
    pub active_operations: usize,
    /// Admissions queued for a slot.
    pub waiting_admissions: usize,
    /// The instances' connections by state, summed.
    pub connections: pool::Counts,
    pub(super) refused: bool,
}
impl HostSnapshot {
    /// What a carrier-backed handle reports (GwzSplitCommandChannel 3.4): a refusal with no counts, every count zero. The
    /// client's pools, the hosts it is connected to, are not the core's to see. `phase` is the handle's own, truthfully:
    /// `Closed` once the carrier is lost.
    pub(crate) fn refusal(namespace: HostNamespace, phase: HostPhase) -> Self {
        Self {
            phase,
            namespace,
            endpoint_instances: 0,
            retained_cleanup: 0,
            bindings: 0,
            active_operations: 0,
            waiting_admissions: 0,
            connections: pool::Counts::default(),
            refused: true,
        }
    }
    /// Whether this is a carrier-backed handle's refusal. When it is, every count (`endpoint_instances`,
    /// `retained_cleanup`, `bindings`, `active_operations`, `waiting_admissions`, `connections`) is meaningless: its zeros
    /// say nothing of the client. Only `phase` and `namespace` are the handle's own.
    pub fn refused(&self) -> bool {
        self.refused
    }
}
