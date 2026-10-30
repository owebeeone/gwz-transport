//! The sizes of the tables a host keeps for its operations: what the sustained-use proof (contract section 13.2) reads to
//! show that each returns to its bound. Fixed-size counts read under the owners' own locks, never public API.

/// The size of every table a host keeps for its operations, summed over its instances.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "the sustained-use rows read it through HostHandle::tables"
)]
pub(crate) struct Tables {
    /// Registered bindings in the host's table.
    pub(crate) bindings: usize,
    /// Permits acquired and not yet registered or dropped.
    pub(crate) claims: usize,
    /// Admissions queued for a slot.
    pub(crate) queued: usize,
    /// Slots in use.
    pub(crate) active: usize,
    /// Retained-input charge, in bytes, that live admissions and bindings hold.
    pub(crate) charged: usize,
    /// Endpoint instances.
    pub(crate) instances: usize,
    /// What the instances keep (see [`InstanceTables`]), summed.
    pub(crate) instance: InstanceTables,
}

/// The sizes of one instance's per-operation tables (see [`Tables`]).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct InstanceTables {
    /// Openings waiting for a connection.
    pub(crate) openings: usize,
    /// Openings waiting for a reuse proof.
    pub(crate) authorizing: usize,
    /// Exchanges whose end the adapter has yet to confirm.
    pub(crate) settling: usize,
    /// Per-connection setup records.
    pub(crate) connections: usize,
    /// Scope tokens of live HTTPS connections and openings.
    pub(crate) scope_tokens: usize,
    /// Pool requests still live.
    pub(crate) requests: usize,
}
impl InstanceTables {
    pub(crate) fn plus(self, other: Self) -> Self {
        Self {
            openings: self.openings + other.openings,
            authorizing: self.authorizing + other.authorizing,
            settling: self.settling + other.settling,
            connections: self.connections + other.connections,
            scope_tokens: self.scope_tokens + other.scope_tokens,
            requests: self.requests + other.requests,
        }
    }
}
