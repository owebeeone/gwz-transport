//! The carrier-backed backing of the crate's handle, admission and operation (GwzSplitCommandChannel section 3.4).
//!
//! A served core holds the crate's own `HostHandle`, `Admission`, `Operation`, `Opening` and `Exchange`; this module is
//! what stands behind them when the connection is carrier-backed. `admit` takes an operation reference only and does
//! no carrier I/O; `acquire` waits for core-side bounds only; `register` is a deferred commit that enqueues one `bind`
//! and returns an operation whose registration is pending, answered by `bound` or `bind_rejected` or ended by the
//! carrier; `snapshot()` is refused. Nothing here is public: callers cannot tell this backing from a local one by type.
use super::{
    core::{Binding, Reg, Stage, State, Waiting},
    lane::CoreBinding,
    stream::Live,
    table::{self, OpenContext},
};
use crate::{
    Admission, AdmissionError, AdmitInput, AttachError, BindingId, CancelState, CarrierId,
    Direction, Discard, EndpointSpec, ExchangeSpec, HostClosing, HostHandle, HostSnapshot,
    OpenError, OperationEnd, Registration, Routed, Sleep, TerminalReason, binding::session_name,
    cell::Cell, host::Backend,
};
use gwz_transport::{
    mux::{self, Attachment, Mux},
    protocol::{Deadlines, Effect},
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, Ordering::AcqRel},
    },
    task::{Context, Poll, Waker},
};

/// The core-local bounds of a carrier-backed handle and the defaults it offers the client; the client clamps `deadlines`
/// to its own ceilings (section 5.4), so these are asks, not authority.
#[derive(Clone, Debug)]
pub struct RemoteConfig {
    /// Operations holding a slot (granted permits and live bindings).
    pub active_operations: usize,
    /// Admission records waiting for a slot.
    pub waiting_admissions: usize,
    /// How long `acquire` may wait for a slot.
    pub admission_bound_ms: u64,
    /// Streams one binding's mux holds at once (at most 1024).
    pub streams_per_binding: usize,
    pub deadlines: Deadlines,
}
impl Default for RemoteConfig {
    fn default() -> Self {
        Self {
            active_operations: 32,
            waiting_admissions: 64,
            admission_bound_ms: 30_000,
            streams_per_binding: 64,
            deadlines: Deadlines {
                allocation_ms: 30_000,
                connect_ms: 30_000,
                io_ms: 60_000,
                interaction_ms: 120_000,
                cleanup_ms: 5_000,
            },
        }
    }
}

/// The state behind a carrier-backed `HostHandle`.
pub(crate) struct Shared {
    pub(super) binding: Arc<dyn CoreBinding>,
    pub(super) config: RemoteConfig,
    state: Mutex<State>,
    driver_spawned: AtomicBool,
}
impl Shared {
    pub(super) fn new(binding: Arc<dyn CoreBinding>, config: RemoteConfig) -> Arc<Self> {
        Arc::new(Self {
            binding,
            config,
            state: Mutex::new(State::new()),
            driver_spawned: AtomicBool::new(false),
        })
    }
    /// Pumps, runs `f` under the state lock, pumps again, then (lock released) publishes receipts and wakes waiters.
    pub(super) fn with<R>(&self, f: impl FnOnce(&mut State) -> R) -> R {
        let (result, wakers, finished) = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.pump(&*self.binding, &self.config);
            let result = f(&mut state);
            state.pump(&*self.binding, &self.config);
            let wakers = state.wake_all();
            let finished = std::mem::take(&mut state.finished);
            (result, wakers, finished)
        };
        for (cell, end) in finished {
            cell.set(end);
        }
        for waker in wakers {
            waker.wake();
        }
        result
    }
    /// Spawns the driver on the binding's executor, once: progress with no application call. A refusal leaves the
    /// handle progressing on its calls, as every call pumps first.
    fn spawn_driver(self: &Arc<Self>) {
        if self.driver_spawned.swap(true, AcqRel) {
            return;
        }
        let driver = Driver {
            shared: Arc::downgrade(self),
            tick: None,
        };
        let _ = self.binding.progress().spawn(Box::pin(driver));
    }
    fn namespace_id(&self, serial: u64) -> BindingId {
        BindingId {
            host: self.binding.namespace(),
            serial,
        }
    }
}

/// The handle's driver: resolves when the carrier has ended and no binding is left, or when every handle is gone.
struct Driver {
    shared: Weak<Shared>,
    tick: Option<Sleep>,
}
impl Future for Driver {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let Some(shared) = self.shared.upgrade() else {
            return Poll::Ready(());
        };
        if shared.with(|state| state.lost && state.bindings.is_empty()) {
            return Poll::Ready(());
        }
        shared.binding.register(cx.waker());
        // The mux's deadlines (the bootstrap bound) need a wake with no lane traffic.
        let clock = shared.binding.clock();
        loop {
            let tick = self.tick.get_or_insert_with(|| {
                clock.sleep_until(clock.now().plus(std::time::Duration::from_millis(100)))
            });
            if tick.as_mut().poll(cx).is_pending() {
                break;
            }
            self.tick = None;
        }
        Poll::Pending
    }
}

impl Backend for Shared {
    /// Refusal-only: the client's pools, the hosts it is connected to, are not the core's to see.
    fn snapshot(&self) -> HostSnapshot {
        HostSnapshot::refusal(self.binding.namespace())
    }
    fn check_open(&self) -> Result<(), HostClosing> {
        if self.with(|state| state.lost) {
            Err(HostClosing)
        } else {
            Ok(())
        }
    }
    /// A carrier-backed handle has no endpoint instances: the client's owner makes them.
    fn attach_instance(self: Arc<Self>, _spec: EndpointSpec) -> Result<(), AttachError> {
        Err(AttachError::Closing(HostClosing))
    }
    fn admit(self: Arc<Self>, input: AdmitInput) -> Result<Admission, AdmissionError> {
        let AdmitInput::Reference(call) = input else {
            // The core supplies no profile, authentication context, policy or capacity (section 3.4).
            return Err(AdmissionError::WrongInput);
        };
        let live = self.binding.call_is_live(call.request_id());
        let limit = self.config.waiting_admissions;
        let id = self.with(|state| {
            if state.lost {
                return Err(AdmissionError::CarrierLost);
            }
            if !live {
                return Err(AdmissionError::UnknownCall);
            }
            if state.requests.contains(call.request_id()) {
                return Err(AdmissionError::DuplicateOperation);
            }
            if state.queue.len() >= limit {
                return Err(AdmissionError::QueueFull);
            }
            state.ids += 1;
            let id = state.ids;
            state.requests.insert(call.request_id().to_owned());
            state.queue.push_back(Waiting { id, waker: None });
            Ok(id)
        })?;
        self.spawn_driver();
        Ok(Admission::remote(RemoteAdmission {
            shared: self,
            id,
            request: call.request_id().to_owned(),
            queued: true,
        }))
    }
    fn carrier(&self) -> CarrierId {
        CarrierId(self.binding.namespace())
    }
    /// Frames of a carrier-backed binding arrive on its lane and are routed by `pump`, not offered one by one.
    fn route(
        &self,
        _carrier: CarrierId,
        _binding: BindingId,
        _direction: Direction,
        _item: &Attachment,
    ) -> Routed {
        Routed::Discarded(Discard::ForeignCarrier)
    }
}

/// Builds the state behind a carrier-backed handle and the handle over it.
pub(crate) fn backend(
    binding: Arc<dyn CoreBinding>,
    config: RemoteConfig,
) -> (Arc<Shared>, HostHandle) {
    let shared = Shared::new(binding, config);
    let handle = HostHandle::over(shared.clone());
    (shared, handle)
}

/// A reserved admission record. Dropping it releases the record; nothing was registered.
pub(crate) struct RemoteAdmission {
    shared: Arc<Shared>,
    id: u64,
    request: String,
    queued: bool,
}
impl RemoteAdmission {
    /// Waits for a slot, fairly, within the admission bound. It waits for core-side bounds only; no carrier I/O.
    pub(crate) fn acquire(self) -> RemoteAcquire {
        let clock = self.shared.binding.clock();
        let bound = std::time::Duration::from_millis(self.shared.config.admission_bound_ms);
        let sleep = clock.sleep_until(clock.now().plus(bound));
        RemoteAcquire {
            admission: Some(self),
            sleep,
        }
    }
}
impl Drop for RemoteAdmission {
    fn drop(&mut self) {
        if self.queued {
            let (id, request) = (self.id, self.request.clone());
            self.shared.with(|state| {
                state.queue.retain(|waiting| waiting.id != id);
                state.requests.remove(&request);
            });
        }
    }
}
/// See [`RemoteAdmission::acquire`].
pub(crate) struct RemoteAcquire {
    admission: Option<RemoteAdmission>,
    sleep: Sleep,
}
impl Future for RemoteAcquire {
    type Output = Result<RemotePermit, AdmissionError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = &mut *self;
        let Some(admission) = this.admission.as_mut() else {
            return Poll::Ready(Err(AdmissionError::CarrierLost));
        };
        let (id, limit) = (admission.id, admission.shared.config.active_operations);
        let shared = admission.shared.clone();
        let waker = cx.waker().clone();
        let granted = shared.with(|state| {
            if state.lost {
                return Some(Err(AdmissionError::CarrierLost));
            }
            let first = state.queue.front().is_some_and(|waiting| waiting.id == id);
            if first && state.active < limit {
                state.queue.pop_front();
                state.active += 1;
                state.claims.insert(id);
                return Some(Ok(()));
            }
            if let Some(waiting) = state.queue.iter_mut().find(|waiting| waiting.id == id) {
                waiting.waker = Some(waker);
            }
            None
        });
        match granted {
            Some(Ok(())) => {
                let mut admission = this.admission.take().expect("checked above");
                admission.queued = false;
                Poll::Ready(Ok(RemotePermit {
                    shared: admission.shared.clone(),
                    id: admission.id,
                    request: std::mem::take(&mut admission.request),
                    held: true,
                }))
            }
            Some(Err(error)) => Poll::Ready(Err(error)),
            None => {
                if this.sleep.as_mut().poll(cx).is_ready() {
                    return Poll::Ready(Err(AdmissionError::AdmissionTimeout));
                }
                Poll::Pending
            }
        }
    }
}

/// One registration right. Dropping it unregistered gives the slot back.
pub(crate) struct RemotePermit {
    shared: Arc<Shared>,
    id: u64,
    request: String,
    held: bool,
}
impl RemotePermit {
    /// Registers the operation: synchronous and infallible. It is a deferred commit: it makes the binding, puts one
    /// `bind` (a fresh session identifier, stream 0, role driver) in that binding's own mux queue, which is the slot
    /// the bind keeps whatever the lane's room, and returns an operation whose registration is pending. If the carrier
    /// has ended, the operation is terminal `NeverRegistered(CarrierLost)`, and did nothing.
    pub(crate) fn register(mut self) -> crate::Operation {
        self.held = false;
        let (shared, id, request) = (self.shared.clone(), self.id, self.request.clone());
        let streams = shared.config.streams_per_binding.clamp(1, 1024);
        let outcome = shared.with(|state| {
            state.claims.remove(&id);
            if state.lost {
                state.release(&request);
                return Err(TerminalReason::CarrierLost);
            }
            state.serial += 1;
            let serial = state.serial;
            let binding_id = shared.namespace_id(serial);
            let session = session_name(binding_id);
            let config = mux::Config {
                limits: crate::binding::limits(),
                max_requests: 4,
                max_streams: streams,
                ..Default::default()
            };
            let Ok(mut mux) = Mux::initiator(&session, config) else {
                state.release(&request);
                return Err(TerminalReason::HostFault);
            };
            if mux.register(&request, Some(request.clone())).is_err()
                || mux.begin(&request).is_err()
            {
                state.release(&request);
                return Err(TerminalReason::HostFault);
            }
            let done = Cell::new();
            state.bindings.insert(
                serial,
                Binding::new(
                    binding_id,
                    session.clone(),
                    request.clone(),
                    mux,
                    done.clone(),
                ),
            );
            state.by_session.insert(session, serial);
            Ok((serial, done))
        });
        match outcome {
            Ok((serial, done)) => crate::Operation::remote(RemoteOp {
                shared,
                serial,
                done,
            }),
            Err(reason) => crate::Operation::terminal(reason),
        }
    }
}
impl Drop for RemotePermit {
    fn drop(&mut self) {
        if self.held {
            let (id, request) = (self.id, self.request.clone());
            self.shared.with(|state| {
                if state.claims.remove(&id) {
                    state.active = state.active.saturating_sub(1);
                }
                state.requests.remove(&request);
            });
        }
    }
}
impl State {
    /// Gives a granted slot and its call back.
    fn release(&mut self, request: &str) {
        self.active = self.active.saturating_sub(1);
        self.requests.remove(request);
    }
}

/// What a carrier-backed `Operation` holds: the shared state, its binding's serial and its receipt.
pub(crate) struct RemoteOp {
    pub(super) shared: Arc<Shared>,
    pub(super) serial: u64,
    pub(super) done: Cell<OperationEnd>,
}
impl RemoteOp {
    /// Where the deferred commit stands: `Pending` until `bound` or `bind_rejected` arrives or the carrier ends.
    pub(crate) fn registration(&self) -> Registration {
        let serial = self.serial;
        self.shared.with(|state| match state.bindings.get(&serial) {
            Some(entry) => entry.registration(),
            None => done_registration(&self.done),
        })
    }
    /// Resolves once the registration is no longer pending.
    pub(crate) fn poll_registered(&self, cx: &mut Context<'_>) -> Poll<Registration> {
        let registration = self.registration();
        if registration != Registration::Pending {
            return Poll::Ready(registration);
        }
        let waker = cx.waker().clone();
        self.shared.with(|state| state.waiters.push(waker));
        Poll::Pending
    }
    pub(crate) fn cancel_handle(&self) -> RemoteCancel {
        RemoteCancel {
            shared: Arc::downgrade(&self.shared),
            serial: self.serial,
        }
    }
    /// Maps `spec` through the closed table and queues the exchange. A spec with no row is refused here (`Effect::None`,
    /// nothing sent). An open on a pending operation waits for `bound` first. Nothing more is sent until the opening
    /// is polled.
    pub(crate) fn open(&self, spec: ExchangeSpec) -> Result<RemoteOpening, OpenError> {
        let (shared, serial) = (self.shared.clone(), self.serial);
        let streams = shared.config.streams_per_binding;
        let path_base = {
            let request =
                shared.with(|state| state.bindings.get(&serial).map(|b| b.request.clone()));
            request.and_then(|request| shared.binding.endpoint_path_base(&request))
        };
        // The table's shape rules, before anything is queued; the `Open` itself is built when the binding is ready.
        let limits = crate::binding::limits();
        let probe = OpenContext {
            endpoint_id: "probe",
            operation_id: "probe",
            limits: &limits,
            deadlines: &shared.config.deadlines,
            path_base: path_base.as_deref(),
        };
        table::open_for(&spec, &probe).map_err(OpenError::Refused)?;
        let token = shared.with(|state| {
            let entry = state.bindings.get_mut(&serial).ok_or(OpenError::Sealed)?;
            if let Reg::Never(reason) = &entry.reg {
                return Err(OpenError::Terminal {
                    reason: *reason,
                    effect: Effect::None,
                });
            }
            if entry.cancelled {
                return Err(OpenError::Cancelled);
            }
            if entry.sealed {
                return Err(OpenError::Sealed);
            }
            let active = entry
                .exchanges
                .values()
                .filter(|e| e.stage != Stage::Ended)
                .count();
            if active >= streams {
                return Err(OpenError::BindingLimit);
            }
            entry.live += 1;
            Ok(entry.queue_exchange(spec))
        })?;
        Ok(RemoteOpening {
            live: Live::new(shared, serial, token),
        })
    }
    /// Seals the operation now (no new opens). A binding still pending is cancelled: it ends
    /// `NeverRegistered(Cancelled)`. The receipt resolves when nothing of it is live.
    pub(crate) fn finish(&self) -> Cell<OperationEnd> {
        let serial = self.serial;
        self.shared.with(|state| {
            if let Some(entry) = state.bindings.get_mut(&serial) {
                entry.sealed = true;
                if matches!(entry.reg, Reg::Pending) {
                    entry.reg = Reg::Never(TerminalReason::Cancelled);
                    entry.abort_all(gwz_transport::protocol::ErrorCode::Cancelled, false);
                }
            }
        });
        self.done.clone()
    }
    /// An operation dropped without `finish` abandons its binding: it is cancelled and sealed.
    pub(crate) fn abandon(&self) {
        let serial = self.serial;
        self.shared.with(|state| {
            if let Some(entry) = state.bindings.get_mut(&serial) {
                entry.cancel_all();
                entry.sealed = true;
            }
        });
    }
}
fn done_registration(done: &Cell<OperationEnd>) -> Registration {
    let mut cx = Context::from_waker(Waker::noop());
    match done.poll(&mut cx) {
        Poll::Ready(end) => end.registration,
        Poll::Pending => Registration::Pending,
    }
}

/// Cancels one operation; it holds the state weakly, so it keeps nothing alive.
#[derive(Clone)]
pub(crate) struct RemoteCancel {
    shared: Weak<Shared>,
    serial: u64,
}
impl RemoteCancel {
    pub(crate) fn cancel(&self) -> CancelState {
        let Some(shared) = self.shared.upgrade() else {
            return CancelState::Gone;
        };
        let serial = self.serial;
        shared.with(|state| match state.bindings.get_mut(&serial) {
            Some(entry) => {
                if entry.cancel_all() {
                    CancelState::Cancelled
                } else {
                    CancelState::AlreadyCancelled
                }
            }
            None => CancelState::Gone,
        })
    }
}

/// A request for an exchange: queued, nothing sent yet.
pub(crate) struct RemoteOpening {
    pub(super) live: Live,
}

cfg_if::cfg_if! {
    if #[cfg(test)] {
        impl Shared {
            /// Moves whatever can move now (every call does it first; the driver does it when the lane wakes it).
            pub(crate) fn pump(&self) {
                self.with(|_| ());
            }
            /// Messages dropped for an absent or retired binding (each made no state).
            pub(crate) fn discarded(&self) -> usize {
                self.with(|state| state.discarded)
            }
            /// Bindings in the table.
            pub(crate) fn bindings(&self) -> usize {
                self.with(|state| state.bindings.len())
            }
            /// Streams the mux of the first binding holds.
            pub(crate) fn streams_held(&self) -> usize {
                self.with(|state| {
                    state
                        .bindings
                        .values()
                        .map(|binding| binding.mux.active_streams())
                        .max()
                        .unwrap_or(0)
                })
            }
        }
    }
}
