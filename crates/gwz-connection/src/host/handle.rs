//! The handle and the crate-private backing it reaches the host through.
use super::{AttachError, HostClosing, HostSnapshot};
use crate::{
    Admission, AdmissionError, Direction, EndpointSpec,
    binding::{BindingId, CarrierId, Routed},
    operation::AdmitInput,
    tables::Tables,
};
use gwz_transport::mux::Attachment;
use std::{fmt, sync::Arc};

/// What a handle asks of the host it names. Crate-private so that a handle backed by a carrier is another
/// implementation of it, not another type; the admission and operation types below it are the local backing's.
#[allow(
    dead_code,
    reason = "attach_instance, check_open, carrier and route are used by the tests now and by the carrier in phase 2"
)]
pub(crate) trait Backend: Send + Sync {
    fn snapshot(&self) -> HostSnapshot;
    /// The sizes of the host's per-operation tables, when the backing has them (a carrier-backed handle has none).
    fn tables(&self) -> Option<Tables> {
        None
    }
    fn check_open(&self) -> Result<(), HostClosing>;
    fn attach_instance(self: Arc<Self>, spec: EndpointSpec) -> Result<(), AttachError>;
    fn admit(self: Arc<Self>, input: AdmitInput) -> Result<Admission, AdmissionError>;
    fn carrier(&self) -> CarrierId;
    fn route(
        &self,
        carrier: CarrierId,
        binding: BindingId,
        direction: Direction,
        item: &Attachment,
    ) -> Routed;
}

/// Observes the host and submits operations to it; cannot end it. `Clone + Send + Sync`.
#[derive(Clone)]
pub struct HostHandle {
    pub(super) backend: Arc<dyn Backend>,
}
impl HostHandle {
    pub fn snapshot(&self) -> HostSnapshot {
        self.backend.snapshot()
    }
    /// Validates `input` and reserves one of the host's waiting-admission records. It registers nothing, starts no
    /// thread, makes no endpoint and has no network effect; `Admission::acquire` waits for the slot (contract 5).
    /// A local host takes a [`PreparedOperation`](crate::PreparedOperation); a carrier-backed handle (`remote::handle`) takes an
    /// [`OperationRef`](crate::OperationRef) and does no carrier I/O. Each refuses the other with
    /// `AdmissionError::WrongInput`.
    pub fn admit(&self, input: impl Into<AdmitInput>) -> Result<Admission, AdmissionError> {
        self.backend.clone().admit(input.into())
    }
    /// A handle over `backend`: the local host's, or a carrier-backed one's.
    pub(crate) fn over(backend: Arc<dyn Backend>) -> Self {
        Self { backend }
    }
    /// The sizes of the host's per-operation tables; `None` on a carrier-backed handle, which has none.
    #[allow(dead_code, reason = "the sustained-use rows read it")]
    pub(crate) fn tables(&self) -> Option<Tables> {
        self.backend.tables()
    }
    #[allow(dead_code, reason = "the tests use it; the carrier adapter will")]
    pub(crate) fn check_open(&self) -> Result<(), HostClosing> {
        self.backend.check_open()
    }
    #[allow(dead_code, reason = "the tests attach instances directly")]
    pub(crate) fn attach_instance(&self, spec: EndpointSpec) -> Result<(), AttachError> {
        self.backend.clone().attach_instance(spec)
    }
    #[allow(dead_code, reason = "the carrier adapter routes frames through it")]
    pub(crate) fn carrier(&self) -> CarrierId {
        self.backend.carrier()
    }
    #[allow(dead_code, reason = "the carrier adapter routes frames through it")]
    pub(crate) fn route(
        &self,
        carrier: CarrierId,
        binding: BindingId,
        direction: Direction,
        item: &Attachment,
    ) -> Routed {
        self.backend.route(carrier, binding, direction, item)
    }
}
impl fmt::Debug for HostHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostHandle").finish_non_exhaustive()
    }
}
