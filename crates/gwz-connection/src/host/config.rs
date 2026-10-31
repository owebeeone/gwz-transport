//! The host's limits and config (contract section 10): plain values, validated at build.
use crate::{CleanupDomain, safety::SharedProcessSafety};

/// How long an admission waits for its slot and its endpoint by default, in milliseconds (contract section 10).
///
/// Release-kind decision A (2026-10-31): the client's default is strictly below the core mux's bootstrap bound
/// (`gwz_transport::mux::Config::bootstrap_timeout_ms`, also 5 s), so a client admission wait ends, by name, before the
/// bootstrap bound that a core waiting on the client would also be running. The row
/// `the_default_admission_bound_is_strictly_below_the_mux_bootstrap_bound` fails if the two ever meet.
pub(crate) const DEFAULT_ADMISSION_BOUND_MS: u64 = 4_000;

/// The finite limits of a host (contract section 10). Plain values; the defaults are the contract's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostLimits {
    /// Operations that run at once.
    pub active_slots: usize,
    /// Admissions that wait for a slot.
    pub waiting_admissions: usize,
    /// Endpoint instances.
    pub endpoint_instances: usize,
    /// The overall bound on a close, in milliseconds.
    pub close_bound_ms: u64,
    /// Requests one binding may register in each of its two sessions (the bootstrap's and every exchange's): the
    /// binding's own limit, never the host's.
    pub binding_registrations: usize,
    /// How long an admission may wait for its slot and its endpoint, in milliseconds. The default is
    /// [`DEFAULT_ADMISSION_BOUND_MS`], strictly below the core mux's bootstrap bound.
    pub admission_bound_ms: u64,
}
impl Default for HostLimits {
    fn default() -> Self {
        Self {
            active_slots: 8,
            waiting_admissions: 64,
            endpoint_instances: 16,
            close_bound_ms: 5_000,
            binding_registrations: 256,
            admission_bound_ms: DEFAULT_ADMISSION_BOUND_MS,
        }
    }
}
/// The longest close bound `build` accepts: a day, as the pool's clocks.
const MAX_CLOSE_BOUND_MS: u64 = 86_400_000;
impl HostLimits {
    pub(super) fn validate(&self) -> Result<(), ConfigReason> {
        if self.active_slots == 0 {
            return Err(ConfigReason::ActiveSlots);
        }
        if self.waiting_admissions == 0 {
            return Err(ConfigReason::WaitingAdmissions);
        }
        if self.endpoint_instances == 0 {
            return Err(ConfigReason::EndpointInstances);
        }
        if !(1..=MAX_CLOSE_BOUND_MS).contains(&self.close_bound_ms) {
            return Err(ConfigReason::CloseBound);
        }
        if !(1..=MAX_CLOSE_BOUND_MS).contains(&self.admission_bound_ms) {
            return Err(ConfigReason::AdmissionBound);
        }
        // The transport's own range for a session's requests.
        if !(1..=4096).contains(&self.binding_registrations) {
            return Err(ConfigReason::BindingRegistrations);
        }
        Ok(())
    }
}

/// What a host is made on besides its ports: the cleanup domain and the process's one gate, both owned by the
/// composition, and the limits. No `Default`.
pub struct HostConfig {
    pub domain: CleanupDomain,
    pub safety: SharedProcessSafety,
    pub limits: HostLimits,
}

/// Which limit `BuildError::InvalidConfig` names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConfigReason {
    ActiveSlots,
    WaitingAdmissions,
    EndpointInstances,
    CloseBound,
    AdmissionBound,
    BindingRegistrations,
    DomainLimits,
}
