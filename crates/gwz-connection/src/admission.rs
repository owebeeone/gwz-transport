//! Admission, acquisition and registration (contract section 5).
//!
//! `Admission`, `Acquire` and `AdmissionPermit` are the one set of types for a local host and a carrier-backed handle
//! (GwzSplitCommandChannel 3.4): each holds the local record below or the carrier-backed one of `remote`, behind a
//! private representation. What follows describes the local one.
//!
//! `admit` reserves a waiting record; `acquire` waits for a slot in FIFO order and attaches the endpoint instance for
//! the operation's profile (metadata only), within the admission bound; the permit it returns owns the prepared binding
//! and one registration right. `register` is synchronous, does no I/O, calls no port and either publishes the binding
//! or, if the host or domain closed first, yields a terminal operation that never registered.
use crate::{
    AdmissionError, AttachError, BindingId, EndpointSpec, Operation, PreparedOperation,
    binding::{BindingShared, CarrierId, MuxPair, session_name},
    cell::Cell,
    host::{ChargeGuard, HostShared, Slot},
    operation::TerminalReason,
    ports::Sleep,
    remote::{RemoteAcquire, RemoteAdmission, RemotePermit},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize},
    },
    task::{Context, Poll},
};

/// A reserved admission record. Dropping it releases the record; nothing was registered.
pub struct Admission(AdmissionKind);
enum AdmissionKind {
    Local(Box<LocalAdmission>),
    Remote(RemoteAdmission),
}
impl Admission {
    pub(crate) fn local(
        host: Arc<HostShared>,
        id: u64,
        input: PreparedOperation,
        charge: ChargeGuard,
        sleep: Sleep,
    ) -> Self {
        Self(AdmissionKind::Local(Box::new(LocalAdmission::new(
            host, id, input, charge, sleep,
        ))))
    }
    pub(crate) fn remote(admission: RemoteAdmission) -> Self {
        Self(AdmissionKind::Remote(admission))
    }
    /// Waits for the operation's slot, fairly, and, for a local host, for its endpoint instance, within the admission
    /// bound. Dropping the future releases every reservation it made.
    pub fn acquire(self) -> Acquire {
        Acquire(match self.0 {
            AdmissionKind::Local(admission) => AcquireKind::Local(Box::new(admission.acquire())),
            AdmissionKind::Remote(admission) => AcquireKind::Remote(admission.acquire()),
        })
    }
}
impl std::fmt::Debug for Admission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Admission").finish_non_exhaustive()
    }
}

struct LocalAdmission {
    host: Arc<HostShared>,
    id: u64,
    input: Option<PreparedOperation>,
    charge: Option<ChargeGuard>,
    sleep: Sleep,
    queued: bool,
}
impl LocalAdmission {
    fn new(
        host: Arc<HostShared>,
        id: u64,
        input: PreparedOperation,
        charge: ChargeGuard,
        sleep: Sleep,
    ) -> Self {
        Self {
            host,
            id,
            input: Some(input),
            charge: Some(charge),
            sleep,
            queued: true,
        }
    }
    fn acquire(self) -> LocalAcquire {
        LocalAcquire(self)
    }
}
impl Drop for LocalAdmission {
    fn drop(&mut self) {
        if self.queued {
            self.host.dequeue(self.id);
        }
    }
}
/// See [`Admission::acquire`].
pub struct Acquire(AcquireKind);
enum AcquireKind {
    Local(Box<LocalAcquire>),
    Remote(RemoteAcquire),
}
impl Future for Acquire {
    type Output = Result<AdmissionPermit, AdmissionError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match &mut self.0 {
            AcquireKind::Local(acquire) => Pin::new(&mut **acquire)
                .poll(cx)
                .map_ok(|permit| AdmissionPermit(PermitKind::Local(permit))),
            AcquireKind::Remote(acquire) => Pin::new(acquire)
                .poll(cx)
                .map_ok(|permit| AdmissionPermit(PermitKind::Remote(permit))),
        }
    }
}

struct LocalAcquire(LocalAdmission);
impl Future for LocalAcquire {
    type Output = Result<LocalPermit, AdmissionError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let admission = &mut self.0;
        let host = admission.host.clone();
        let (claim, serial) = match host.take_slot(admission.id, cx.waker()) {
            Slot::Granted { claim, serial } => {
                admission.queued = false;
                (claim, serial)
            }
            Slot::Closing(reason) => {
                admission.queued = false;
                return Poll::Ready(Err(AdmissionError::Closing(reason)));
            }
            Slot::Exhausted => {
                admission.queued = false;
                return Poll::Ready(Err(AdmissionError::IdentifierExhausted));
            }
            Slot::Wait => {
                if admission.sleep.as_mut().poll(cx).is_ready() {
                    host.dequeue(admission.id);
                    admission.queued = false;
                    return Poll::Ready(Err(AdmissionError::AdmissionTimeout));
                }
                return Poll::Pending;
            }
        };
        // From here the slot is held in a claim: every refusal gives it back.
        let (Some(input), Some(charge)) = (admission.input.take(), admission.charge.take()) else {
            host.drop_claim(claim);
            return Poll::Ready(Err(AdmissionError::Closing(TerminalReason::HostFault)));
        };
        let instance = match host.attach_profile(&EndpointSpec::new(input.profile.clone())) {
            Ok(instance) => instance,
            Err(error) => {
                host.drop_claim(claim);
                return Poll::Ready(Err(match error {
                    AttachError::Closing(_) => AdmissionError::Closing(host.closing_reason()),
                    AttachError::InstanceLimit => AdmissionError::InstanceLimit,
                    AttachError::Factory(error) => AdmissionError::UnavailableAdapter(error),
                }));
            }
        };
        let id = BindingId {
            host: host.namespace,
            serial,
        };
        let request = format!("b{serial}");
        let pair = match MuxPair::bootstrapped(
            id,
            &input.route,
            host.limits.binding_registrations,
            &request,
            &input.operation,
        ) {
            Ok(pair) => pair,
            Err(_) => {
                host.drop_claim(claim);
                return Poll::Ready(Err(AdmissionError::Closing(TerminalReason::HostFault)));
            }
        };
        let binding = Arc::new(BindingShared {
            id,
            session: session_name(id),
            operation: input.operation,
            limits: input.policy.limits(),
            carrier: CarrierId(host.namespace),
            pair: Mutex::new(pair),
            instance,
            auth: input.auth,
            cancelled: Arc::new(AtomicBool::new(false)),
            closing: AtomicUsize::new(0),
            sealed: AtomicBool::new(false),
            retired: AtomicBool::new(false),
            live: AtomicUsize::new(0),
            opened: AtomicU64::new(0),
            next_exchange: AtomicU64::new(0),
            done: Cell::new(),
            _charge: charge,
        });
        Poll::Ready(Ok(LocalPermit {
            host,
            claim: Some(claim),
            binding: Some(binding),
        }))
    }
}

/// One registration right over a prepared binding (a local host) or a granted slot (a carrier-backed handle).
/// Dropping it unregistered releases the slot.
pub struct AdmissionPermit(PermitKind);
enum PermitKind {
    Local(LocalPermit),
    Remote(RemotePermit),
}
impl AdmissionPermit {
    /// Registers the operation: synchronous, no I/O, no port callback, infallible. If the host or the domain closed
    /// first (or, carrier-backed, the carrier ended), the result is a terminal operation that never registered, which
    /// opens nothing and did nothing. A carrier-backed registration is a deferred commit: the operation's registration
    /// is pending until the client answers.
    pub fn register(self) -> Operation {
        match self.0 {
            PermitKind::Local(permit) => permit.register(),
            PermitKind::Remote(permit) => permit.register(),
        }
    }
}

struct LocalPermit {
    host: Arc<HostShared>,
    claim: Option<u64>,
    binding: Option<Arc<BindingShared>>,
}
impl LocalPermit {
    fn register(mut self) -> Operation {
        let (claim, binding) = (self.claim.take(), self.binding.take());
        let (Some(claim), Some(binding)) = (claim, binding) else {
            return Operation::terminal(TerminalReason::HostFault);
        };
        match self.host.publish(claim, &binding) {
            Ok(()) => Operation::live(self.host.clone(), binding),
            Err(reason) => Operation::terminal(reason),
        }
    }
}
impl Drop for LocalPermit {
    fn drop(&mut self) {
        if let Some(claim) = self.claim.take() {
            self.host.drop_claim(claim);
        }
    }
}
