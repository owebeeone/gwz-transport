//! The GWZ connection owner: ports, a cleanup domain, and a host with one owner and many handles.
//!
//! This crate coordinates injected ports and implements no socket, file, native authentication, helper spawn or
//! executor of its own (contract section 3). It depends on no other GWZ crate and never on gwz-core or git2, so the
//! first proof (contract section 13.1) is a build that cannot reach either.
//!
//! - [`ports`]: the four ports of contract section 4.1 (`Clock`, `OwnerSeed`, `Progress`, `EndpointFactory`).
//! - [`safety`]: the process-safety gate the composition supplies (section 7.3).
//! - [`domain`]: `cleanup_domain`, the retained-cleanup authority that outlives hosts (section 4.1).
//! - [`host`]: `build`, `HostOwner`, `HostHandle` (sections 4 and 5).
//!
//! Step 1.1 of dev-docs/GwzTransportReleaseConvergencePlan.md. Admission, operations and exchanges are step 1.2's.
mod cell;
mod domain;
mod host;
mod ports;
mod safety;

pub use domain::{
    CleanupDomain, CleanupDomainOwner, CleanupStatus, CleanupTicket, DomainClose, DomainConfig,
    RetainedWork, RetentionPort, RetentionRefused, TicketStatus, WaitUntil, cleanup_domain,
};
pub use host::{
    AttachError, BuildError, ConfigReason, HostClose, HostClosing, HostConfig, HostHandle,
    HostLimits, HostOwner, HostPhase, HostSnapshot, build,
};
pub use ports::{
    Clock, DriverWork, EndpointAdapter, EndpointFactory, EndpointSpec, FactoryEnv, FactoryError,
    FactoryReason, HostNamespace, HostPorts, OwnerSeed, Progress, ProgressUnavailable, Sleep, Tick,
};
pub use safety::{ProcessSafety, SafetyHold, SafetyState, SafetyUnavailable, SharedProcessSafety};

cfg_if::cfg_if! {
    if #[cfg(test)] {
        mod domain_tests;
        mod fakes;
        mod host_tests;
    }
}
