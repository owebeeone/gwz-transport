//! The process-safety capability of contract section 7.3.
//!
//! One gate per process is supplied by the composition to every host it builds (`HostConfig::safety`); a host never
//! makes one, and two hosts built on one `Arc` share it. The type is a port: this crate holds no handle, mutex or
//! named object of its own.
use std::sync::Arc;

/// Whether the gate can demonstrate its scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SafetyState {
    Qualified,
    /// The gate cannot demonstrate its scope.
    Unavailable(SafetyUnavailable),
}
/// Why a gate is unavailable.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SafetyUnavailable {
    /// The Windows object that scopes the gate to this process could not be created.
    ObjectNotCreated,
    /// The object exists and another owner holds it.
    ForeignOwner,
    /// The platform has no qualified gate.
    NotSupported,
}

/// The gate. **Fail closed (contract 7.3, operator decision D-E).** When `state` is `Unavailable`, `hold` returns the
/// reason, and an adapter whose route needs a launch window (native authentication on Windows) refuses before it
/// reads a credential or has any effect; it does not take another identity and it does not proceed unguarded. Routes
/// that need no launch window are unaffected, so `build` does not refuse a host over an unavailable gate. The Windows
/// implementation (a per-process kernel object, with the third-party-spawn residual recorded in the contract) and the
/// refusal in the adapters are step 1.3's; this crate states the rule and carries the gate.
pub trait ProcessSafety: Send + Sync + 'static {
    fn state(&self) -> SafetyState;
    /// Waits for the gate and holds it until the returned value is dropped.
    fn hold(&self) -> Result<SafetyHold, SafetyUnavailable>;
}

/// A held gate. Dropping it releases the gate.
pub struct SafetyHold {
    _release: Box<dyn Send>,
}
impl SafetyHold {
    /// A hold that releases when `token` is dropped.
    pub fn new(token: impl Send + 'static) -> Self {
        Self {
            _release: Box::new(token),
        }
    }
}

/// The gate as a host holds it: shared, never owned by one host.
pub type SharedProcessSafety = Arc<dyn ProcessSafety>;
