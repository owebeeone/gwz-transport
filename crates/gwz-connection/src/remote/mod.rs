//! What stands behind the crate's own handle when the connection is carrier-backed, and the client's counterpart (step
//! 2.2c of dev-docs/GwzTransportReleaseConvergencePlan.md; GwzSplitCommandChannel section 3.3, 3.4 and 5).
//!
//! A served core holds [`handle`]: a [`HostHandle`](crate::HostHandle) whose `admit` takes an
//! [`OperationRef`](crate::OperationRef), whose `register` is a deferred commit answered over the carrier, whose `open`
//! maps the typed `ExchangeSpec` through the closed table (`table.rs`) to a typed `Open`, and whose `snapshot()` is
//! refused. It yields the same `Admission`, `Operation`, `Opening` and `Exchange` as a local host: callers cannot tell
//! one from the other by type. The client runs a [`Receiver`] that builds the `PreparedOperation` itself from the call
//! it sent and admits every `bind`, `open` and `check_identity` against its own allowed sets (`admission.rs`). Neither
//! end depends on core's frame types: they speak [`Lane`], the transport's own `(request, Envelope)` messages, and the
//! composition adapts a carrier to it.
//!
//! | Behaviour | Where |
//! | --- | --- |
//! | `admit(op_ref)` no carrier I/O; `acquire` core-local; `register` a deferred commit; `snapshot()` refused | `backend` |
//! | `bound` registers; `bind_rejected` and carrier loss end `NeverRegistered`; live bindings revoked on loss | `core`, `backend` |
//! | The core's openings, exchanges and byte streams over the lane | `stream` |
//! | The bounds a carrier holds one binding to (its receive window, its mux's streams) | `lane` |
//! | The closed `ExchangeSpec` to `Open.service` table | `table` |
//! | Tuple canonical form (5.2), service class and push allowance (5.3) | `admission` |
//! | The client's receiver: invocation capture, admission, the binding's endpoint half on the owner | `receiver`, `served` |
mod admission;
mod backend;
mod core;
mod lane;
mod receiver;
mod served;
mod stream;
mod table;

use crate::HostHandle;
use std::sync::Arc;

pub use admission::{
    Allowance, AllowedSet, CanonError, Canonical, Refusal, ServiceClass, admit_service,
};
pub use backend::RemoteConfig;
pub(crate) use backend::{
    RemoteAcquire, RemoteAdmission, RemoteCancel, RemoteOp, RemoteOpening, RemotePermit,
};
pub use lane::{BindBounds, ClientSession, CoreBinding, Lane, LaneEnded, LaneSend};
pub use receiver::{Invocation, Invocations, NamedKey, Receiver, RefusalNote, serve_bindings};
pub(crate) use stream::{Live as RemoteLive, RemoteFinish, RemoteOpenWait, RemoteStream};
pub use table::TableRefusal;

cfg_if::cfg_if! {
    if #[cfg(test)] {
        mod tests;
        pub(crate) use backend::{Shared, backend};
    }
}

/// The carrier-backed handle of a served session (section 3.4): `binding` is the session's lane and live-call
/// registry. Its driver is spawned on `binding`'s executor at the first `admit`.
pub fn handle(binding: Arc<dyn CoreBinding>) -> HostHandle {
    handle_with(binding, RemoteConfig::default())
}
/// [`handle`] with the core-local bounds `config`.
pub fn handle_with(binding: Arc<dyn CoreBinding>, config: RemoteConfig) -> HostHandle {
    backend::backend(binding, config).1
}
