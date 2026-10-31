//! The connection host: one owner of a registry of endpoint instances that outlives an operation, and the handles
//! that reach it. Steps 1.1 and 1.2 of dev-docs/GwzTransportReleaseConvergencePlan.md; contract sections 4, 4.1, 5, 6, 10 and 11.
//!
//! `build(config, ports)` returns two capabilities:
//!
//! - [`HostOwner`] is `Send` and not `Clone`, and it is the only authority that ends the host. `close` seals the host
//!   when it is called (admission is refused from then on, whether or not the returned future is ever polled) and
//!   hands the disposal of its instances to the composition's [`Progress`](crate::Progress); the future resolves to a
//!   [`CleanupStatus`](crate::CleanupStatus) within the close bound, `Pending(ticket)` once the bound passes, the unfinished disposal then belonging to the
//!   cleanup domain's retainer. Dropping the owner seals and disposes the same way.
//! - [`HostHandle`] is `Clone + Send + Sync`: it observes and (from step 1.2) submits, and it has no way to end the
//!   host. Dropping every handle leaves the host as it was for as long as its owner lives. A handle is a capability,
//!   not a pointer: it exposes no reference into the host, no identity beyond the host's namespace and no lifetime
//!   tied to this process, so a handle that reaches an owner through a carrier is the same type to its holder.
//!
//! From step 1.2 a handle admits operations (`admit`, in `admission.rs`), and each registered operation owns a binding of
//! its own (`binding.rs`, `exchange.rs`): the host keeps the endpoint instances and their pools (`instance.rs`) and
//! leases connections to bindings. From step 1.3b the owner also owns the host's driver (`pump.rs`), the task on the
//! composition's `Progress` that advances the instances' pending work without any application call.
//!
//! The registry holds at most `endpoint_instances` instances. At the limit it retires the least recently used instance that
//! no binding holds and nothing is in flight on, in the same transition that reserves the new instance's place
//! (`attach_profile`), and its adapter is disposed under the cleanup domain (contract sections 6 and 10).
//!
//! What it is not. It is not a process-wide singleton and there is no ambient lookup: the composition builds the owner,
//! keeps it, and hands handles out; nothing here is stored in a static or a thread local, and no identifier comes from
//! a global counter. It implements no endpoint: the SSH and HTTPS mechanisms stay behind the [`EndpointFactory`](crate::EndpointFactory) port.

mod config;
mod errors;
mod handle;
mod owner;
mod shared;
mod snapshot;

pub use config::{ConfigReason, HostConfig, HostLimits};
pub use errors::{AttachError, BuildError, HostClosing};
pub(crate) use handle::Backend;
pub use handle::HostHandle;
pub use owner::{HostClose, HostOwner};
pub use shared::build;
pub(crate) use shared::{ChargeGuard, HostShared, Slot};
pub use snapshot::{HostPhase, HostSnapshot};

cfg_if::cfg_if! {
    if #[cfg(test)] {
        pub(crate) use config::DEFAULT_ADMISSION_BOUND_MS;
    }
}
