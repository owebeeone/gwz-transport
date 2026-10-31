//! The local host as the backing of a handle.
use super::HostShared;
use crate::{
    Admission, AdmissionError, Direction, EndpointSpec,
    binding::{BindingId, CarrierId, Routed},
    host::{AttachError, Backend, HostClosing, HostPhase, HostSnapshot},
    operation::AdmitInput,
    tables::Tables,
};
use gwz_transport::{mux::Attachment, pool};
use std::sync::Arc;

impl Backend for HostShared {
    fn tables(&self) -> Option<Tables> {
        Some(HostShared::tables(self))
    }
    fn snapshot(&self) -> HostSnapshot {
        let (phase, instances, bindings, active, waiting) = {
            let state = self.lock();
            (
                state.phase,
                state.instances.clone(),
                state.bindings.len(),
                state.active,
                state.queue.len(),
            )
        };
        let mut connections = pool::Counts::default();
        for instance in &instances {
            let counts = instance.lock().pool.counts();
            connections.opening += counts.opening;
            connections.idle += counts.idle;
            connections.leased += counts.leased;
            connections.closing += counts.closing;
        }
        HostSnapshot {
            phase,
            namespace: self.namespace,
            endpoint_instances: instances.len(),
            retained_cleanup: self.domain.retained(),
            bindings,
            active_operations: active,
            waiting_admissions: waiting,
            connections,
            refused: false,
        }
    }
    fn check_open(&self) -> Result<(), HostClosing> {
        if self.lock().phase == HostPhase::Open {
            Ok(())
        } else {
            Err(HostClosing)
        }
    }
    fn attach_instance(self: Arc<Self>, spec: EndpointSpec) -> Result<(), AttachError> {
        self.attach_profile(&spec).map(drop)
    }
    fn admit(self: Arc<Self>, input: AdmitInput) -> Result<Admission, AdmissionError> {
        match input {
            AdmitInput::Prepared(input) => self.admit_input(input),
            // A local host builds nothing from a reference: its composition prepared the operation.
            AdmitInput::Reference(_) => Err(AdmissionError::WrongInput),
        }
    }
    fn carrier(&self) -> CarrierId {
        HostShared::carrier(self)
    }
    fn route(
        &self,
        carrier: CarrierId,
        binding: BindingId,
        direction: Direction,
        item: &Attachment,
    ) -> Routed {
        HostShared::route(self, carrier, binding, direction, item)
    }
}
