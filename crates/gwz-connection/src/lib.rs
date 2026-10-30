//! The GWZ connection owner: ports, a cleanup domain, and a host with one owner and many handles.
//!
//! This crate coordinates injected ports and implements no socket, file, native authentication, helper spawn or
//! executor of its own (contract section 3). It depends on gwz-transport (whose mux and pool state machines do no I/O)
//! and never on gwz-core or git2, so the first proof (contract section 13.1) is a build that cannot reach either.
//!
//! - [`ports`]: the four ports of contract section 4.1 (`Clock`, `OwnerSeed`, `Progress`, `EndpointFactory`).
//! - [`safety`]: the process-safety gate the composition supplies (section 7.3).
//! - [`domain`]: `cleanup_domain`, the retained-cleanup authority that outlives hosts (section 4.1).
//! - [`host`]: `build`, `HostOwner`, `HostHandle` (sections 4 and 5).
//! - `admission`, `exchange`: `admit`, `acquire`, `register`, and the `Operation`, `Opening` and `Exchange` an operation
//!   owns (sections 4, 5 and 8); `binding`: the two mux sessions each operation owns (sections 6 and D6); `instance`:
//!   an endpoint instance, its adapter and its pool; `pump`: the host's driver, the task on the composition's
//!   `Progress` that advances every instance's pending work without application calls.
//! - [`remote`]: what stands behind the same `HostHandle`, `Admission`, `Operation`, `Opening` and `Exchange` when the
//!   connection is carrier-backed (a served core's handle, `remote::handle`), and the client's receiver that serves the
//!   core's bindings against its own owner (GwzSplitCommandChannel section 3.3, 3.4 and 5). A caller cannot tell a local
//!   operation from a carrier-backed one by type: `admit` takes a `PreparedOperation` or an `OperationRef`,
//!   `Operation::registration` may be `Pending`, and `snapshot()` is refused.
//!
//! Steps 1.1, 1.2, 1.3b and 2.2c of dev-docs/GwzTransportReleaseConvergencePlan.md.
mod admission;
mod binding;
mod cell;
mod domain;
mod exchange;
mod host;
mod instance;
mod operation;
mod ports;
mod pump;
pub mod remote;
mod safety;
mod tables;

pub use admission::{Acquire, Admission, AdmissionPermit};
pub use binding::{BindingId, CarrierId, Direction, Discard, Routed};
pub use domain::{
    CleanupDomain, CleanupDomainOwner, CleanupStatus, CleanupTicket, DomainClose, DomainConfig,
    RetainedWork, RetentionPort, RetentionRefused, TicketStatus, WaitUntil, cleanup_domain,
};
pub use exchange::{
    Exchange, ExchangeFinish, MessageStream, OpenWait, Opening, Operation, OperationCancel,
    OperationFinish, Registered, StreamError,
};
pub use host::{
    AttachError, BuildError, ConfigReason, HostClose, HostClosing, HostConfig, HostHandle,
    HostLimits, HostOwner, HostPhase, HostSnapshot, build,
};
pub use operation::{
    AdmissionError, AdmitInput, AuthContext, AuthScope, CancelState, EndpointProfile,
    EndpointRoute, ExchangeEnd, ExchangeSpec, GitService, InvalidInput, MAX_PROFILE_BYTES,
    OpenError, OpenFailure, OperationEnd, OperationPolicy, OperationRef, PreparedOperation,
    Registration, TerminalReason,
};
pub use ports::{
    Clock, Completion, ConnectCommand, DriverWork, EndpointAdapter, EndpointFactory, EndpointSpec,
    ExchangeIo, FactoryEnv, FactoryError, FactoryReason, HostNamespace, HostPorts, OwnerSeed,
    Progress, ProgressUnavailable, ProofId, ReuseCommand, ReuseVerdict, Sleep, Tick,
};
pub use safety::{ProcessSafety, SafetyHold, SafetyState, SafetyUnavailable, SharedProcessSafety};

cfg_if::cfg_if! {
    if #[cfg(test)] {
        mod capacity_tests;
        mod domain_tests;
        mod fakes;
        mod host_tests;
        mod operation_tests;
        mod pump_tests;
        mod reuse_tests;
        mod sustained_tests;
    }
}
