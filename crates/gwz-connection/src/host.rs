//! The connection host: one owner of a registry of endpoint instances that outlives an operation, and the handles
//! that reach it. Step 1.1 of dev-docs/GwzTransportReleaseConvergencePlan.md; contract sections 4, 4.1, 5, 10 and 11.
//!
//! `build(config, ports)` returns two capabilities:
//!
//! - [`HostOwner`] is `Send` and not `Clone`, and it is the only authority that ends the host. `close` seals the host
//!   when it is called (admission is refused from then on, whether or not the returned future is ever polled) and
//!   hands the disposal of its instances to the composition's [`Progress`]; the future resolves to a [`CleanupStatus`]
//!   within the close bound, `Pending(ticket)` once the bound passes, the unfinished disposal then belonging to the
//!   cleanup domain's retainer. Dropping the owner seals and disposes the same way.
//! - [`HostHandle`] is `Clone + Send + Sync`: it observes and (from step 1.2) submits, and it has no way to end the
//!   host. Dropping every handle leaves the host as it was for as long as its owner lives. A handle is a capability,
//!   not a pointer: it exposes no reference into the host, no identity beyond the host's namespace and no lifetime
//!   tied to this process, so a handle that reaches an owner through a carrier is the same type to its holder.
//!
//! What it is not. It is not a process-wide singleton and there is no ambient lookup: the composition builds the owner,
//! keeps it, and hands handles out; nothing here is stored in a static or a thread local, and no identifier comes from
//! a global counter. It does not admit operations yet (`admit`, `Operation` and `Exchange` are step 1.2's), and it
//! implements no endpoint: the SSH and HTTPS mechanisms stay behind the [`EndpointFactory`] port.
use crate::{
    CleanupDomain, CleanupStatus, EndpointAdapter, EndpointSpec, FactoryEnv, FactoryError,
    HostNamespace, HostPorts,
    cell::Cell,
    domain::{CleanupTicket, RetainedWork, Revocable},
    ports::{Clock, EndpointFactory, Progress},
    safety::SharedProcessSafety,
};
use std::{
    fmt,
    future::{Future, poll_fn},
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll},
    time::Duration,
};

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
}
impl Default for HostLimits {
    fn default() -> Self {
        Self {
            active_slots: 8,
            waiting_admissions: 64,
            endpoint_instances: 16,
            close_bound_ms: 5_000,
        }
    }
}
/// The longest close bound `build` accepts: a day, as the pool's clocks.
const MAX_CLOSE_BOUND_MS: u64 = 86_400_000;
impl HostLimits {
    fn validate(&self) -> Result<(), ConfigReason> {
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
    DomainLimits,
}
/// Why `build` or `cleanup_domain` refused. (`MissingPort` of contract section 11 cannot arise: `HostPorts::new` takes
/// all four.)
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum BuildError {
    InvalidConfig(ConfigReason),
    UnavailableProgress,
    DomainClosing,
}
impl fmt::Display for BuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(reason) => write!(f, "connection host limits invalid: {reason:?}"),
            Self::UnavailableProgress => f.write_str("the host's progress executor is unavailable"),
            Self::DomainClosing => f.write_str("the cleanup domain is closing"),
        }
    }
}
impl std::error::Error for BuildError {}

/// A handle reached a host that is closing or closed (`HostClosing` of contract section 11).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HostClosing;
impl fmt::Display for HostClosing {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("connection host is closing or closed")
    }
}
impl std::error::Error for HostClosing {}

/// Why an instance could not be attached.
#[derive(Debug)]
#[non_exhaustive]
pub enum AttachError {
    Closing(HostClosing),
    InstanceLimit,
    Factory(FactoryError),
}

/// Where a host is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HostPhase {
    Open,
    /// Sealed: admission refused, disposal under way.
    Closing,
    /// Nothing is left that the host owns: its disposal is confirmed, or belongs to the retainer.
    Closed,
}
/// Fixed-size counts. Step 1.2 adds the pool, binding and exchange counters of contract section 11.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct HostSnapshot {
    pub phase: HostPhase,
    pub namespace: HostNamespace,
    pub endpoint_instances: usize,
    /// Records the host's domain retains now (all hosts on it).
    pub retained_cleanup: usize,
}

struct HostState {
    phase: HostPhase,
    instances: Vec<Box<dyn EndpointAdapter>>,
    /// Instances a factory is making, which count against the limit before they exist.
    creating: usize,
    close: Option<Cell<CleanupStatus>>,
}
struct HostShared {
    namespace: HostNamespace,
    clock: Arc<dyn Clock>,
    progress: Arc<dyn Progress>,
    factory: Arc<dyn EndpointFactory>,
    domain: CleanupDomain,
    safety: SharedProcessSafety,
    limits: HostLimits,
    state: Mutex<HostState>,
}

/// Builds a host from its config and its four ports. Nothing is shared with any other host except what the config
/// names: the domain and the gate.
pub fn build(config: HostConfig, ports: HostPorts) -> Result<(HostOwner, HostHandle), BuildError> {
    config
        .limits
        .validate()
        .map_err(BuildError::InvalidConfig)?;
    ports
        .progress
        .check()
        .map_err(|_| BuildError::UnavailableProgress)?;
    let shared = Arc::new(HostShared {
        namespace: ports.seed.namespace(),
        clock: ports.clock,
        progress: ports.progress,
        factory: ports.factory,
        domain: config.domain,
        safety: config.safety,
        limits: config.limits,
        state: Mutex::new(HostState {
            phase: HostPhase::Open,
            instances: Vec::new(),
            creating: 0,
            close: None,
        }),
    });
    let weak: Weak<dyn Revocable> = Arc::<HostShared>::downgrade(&shared);
    shared.domain.register(weak)?;
    Ok((
        HostOwner {
            shared: Some(shared.clone()),
        },
        HostHandle { backend: shared },
    ))
}

impl HostShared {
    fn lock(&self) -> std::sync::MutexGuard<'_, HostState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Seals the host and starts the disposal of its instances on the composition's executor. Idempotent: every call
    /// returns the one close.
    fn seal(self: &Arc<Self>) -> Cell<CleanupStatus> {
        let (cell, adapters) = {
            let mut state = self.lock();
            if let Some(cell) = &state.close {
                return cell.clone();
            }
            state.phase = HostPhase::Closing;
            let cell = Cell::new();
            state.close = Some(cell.clone());
            (cell, std::mem::take(&mut state.instances))
        };
        if adapters.is_empty() {
            self.finish(&cell, CleanupStatus::Complete);
            return cell;
        }
        let slot: InstanceSlot = Arc::new(Mutex::new(Some(adapters)));
        let job = close_job(self.clone(), slot.clone(), cell.clone());
        if self.progress.spawn(Box::pin(job)).is_err() {
            // The executor cannot run it, so the retainer must: the work was never started.
            let taken = slot.lock().unwrap_or_else(|e| e.into_inner()).take();
            if let Some(adapters) = taken {
                let (ticket, _kept) = self.pend(adapters);
                self.finish(&cell, CleanupStatus::Pending(ticket));
            }
        }
        cell
    }
    fn finish(&self, cell: &Cell<CleanupStatus>, status: CleanupStatus) {
        self.lock().phase = HostPhase::Closed;
        cell.set(status);
    }
    /// Offers `adapters`, which did not finish disposing, to the domain's retainer. The work comes back with the ticket
    /// when the budget or the retainer refused it, and its holder then keeps driving it.
    fn pend(
        &self,
        adapters: Vec<Box<dyn EndpointAdapter>>,
    ) -> (CleanupTicket, Option<RetainedWork>) {
        let generation = self.domain.next_generation();
        let ticket = CleanupTicket::new(generation, self.clock.clone());
        let work = RetainedWork::new(generation, adapters, ticket.clone(), self.domain.clone());
        match self.domain.retain(work) {
            Ok(()) => (ticket, None),
            Err(refused) => (ticket, Some(refused.into_work())),
        }
    }
    fn attach(&self, spec: EndpointSpec) -> Result<(), AttachError> {
        {
            let mut state = self.lock();
            if state.phase != HostPhase::Open {
                return Err(AttachError::Closing(HostClosing));
            }
            if state.instances.len() + state.creating >= self.limits.endpoint_instances {
                return Err(AttachError::InstanceLimit);
            }
            state.creating += 1;
        }
        // The factory runs outside the host's lock.
        let made = self
            .factory
            .create(&spec, &FactoryEnv::new(&self.domain, &self.safety));
        let mut state = self.lock();
        state.creating -= 1;
        match made {
            Err(error) => Err(AttachError::Factory(error)),
            Ok(adapter) if state.phase == HostPhase::Open => {
                state.instances.push(adapter);
                Ok(())
            }
            Ok(adapter) => {
                // Sealed while the factory worked: the new instance belongs to the retainer, not to a closed host.
                drop(state);
                drop(self.pend(vec![adapter]));
                Err(AttachError::Closing(HostClosing))
            }
        }
    }
}
impl Revocable for HostShared {
    fn revoke(self: Arc<Self>) -> Cell<CleanupStatus> {
        self.seal()
    }
}

/// The instances a close job takes when it first runs (or the host takes back if the executor refused the job).
type InstanceSlot = Arc<Mutex<Option<Vec<Box<dyn EndpointAdapter>>>>>;

/// The disposal of a sealed host's instances, run by the composition's executor: concurrently, within one bound.
async fn close_job(host: Arc<HostShared>, slot: InstanceSlot, cell: Cell<CleanupStatus>) {
    let taken = slot.lock().unwrap_or_else(|e| e.into_inner()).take();
    let Some(mut adapters) = taken else {
        return;
    };
    let bound = Duration::from_millis(host.limits.close_bound_ms);
    let mut sleep = host.clock.sleep_until(host.clock.now().plus(bound));
    let disposed = poll_fn(|cx| {
        adapters.retain_mut(|adapter| adapter.poll_dispose(cx).is_pending());
        if adapters.is_empty() {
            return Poll::Ready(true);
        }
        if sleep.as_mut().poll(cx).is_ready() {
            return Poll::Ready(false);
        }
        Poll::Pending
    })
    .await;
    if disposed {
        host.finish(&cell, CleanupStatus::Complete);
        return;
    }
    let (ticket, kept) = host.pend(adapters);
    host.finish(&cell, CleanupStatus::Pending(ticket));
    if let Some(mut work) = kept {
        // The retainer would not take it, so this job is its owner until the disposal is confirmed.
        poll_fn(|cx| work.poll_work(cx)).await;
    }
}

/// The only authority that ends the host. `Send`, not `Clone`.
pub struct HostOwner {
    shared: Option<Arc<HostShared>>,
}
impl HostOwner {
    /// Seals the host now and returns the wait for its cleanup. The seal and the disposal do not depend on polling it.
    pub fn close(mut self) -> HostClose {
        let cell = match self.shared.take() {
            Some(shared) => shared.seal(),
            None => {
                let cell = Cell::new();
                cell.set(CleanupStatus::Complete);
                cell
            }
        };
        HostClose { cell }
    }
}
impl Drop for HostOwner {
    /// An owner that goes without `close` still seals the host: handles that outlive it must not keep open a host that
    /// nothing can end.
    fn drop(&mut self) {
        if let Some(shared) = self.shared.take() {
            drop(shared.seal());
        }
    }
}
impl fmt::Debug for HostOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostOwner").finish_non_exhaustive()
    }
}

/// The wait for a host's cleanup.
pub struct HostClose {
    cell: Cell<CleanupStatus>,
}
impl Future for HostClose {
    type Output = CleanupStatus;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<CleanupStatus> {
        self.cell.poll(cx)
    }
}

/// What a handle asks of the host it names. Crate-private so that a handle backed by a carrier is another
/// implementation of it, not another type.
#[allow(
    dead_code,
    reason = "attach_instance and check_open are used by the tests now and by step 1.2's admission"
)]
trait Backend: Send + Sync {
    fn snapshot(&self) -> HostSnapshot;
    fn check_open(&self) -> Result<(), HostClosing>;
    fn attach_instance(&self, spec: EndpointSpec) -> Result<(), AttachError>;
}
impl Backend for HostShared {
    fn snapshot(&self) -> HostSnapshot {
        let (phase, endpoint_instances) = {
            let state = self.lock();
            (state.phase, state.instances.len())
        };
        HostSnapshot {
            phase,
            namespace: self.namespace,
            endpoint_instances,
            retained_cleanup: self.domain.retained(),
        }
    }
    fn check_open(&self) -> Result<(), HostClosing> {
        if self.lock().phase == HostPhase::Open {
            Ok(())
        } else {
            Err(HostClosing)
        }
    }
    fn attach_instance(&self, spec: EndpointSpec) -> Result<(), AttachError> {
        self.attach(spec)
    }
}

/// Observes the host and (from step 1.2) submits to it; cannot end it. `Clone + Send + Sync`.
#[derive(Clone)]
pub struct HostHandle {
    backend: Arc<dyn Backend>,
}
impl HostHandle {
    pub fn snapshot(&self) -> HostSnapshot {
        self.backend.snapshot()
    }
    #[allow(dead_code, reason = "step 1.2's admit begins with it")]
    pub(crate) fn check_open(&self) -> Result<(), HostClosing> {
        self.backend.check_open()
    }
    #[allow(dead_code, reason = "step 1.2's acquire attaches instances through it")]
    pub(crate) fn attach_instance(&self, spec: EndpointSpec) -> Result<(), AttachError> {
        self.backend.attach_instance(spec)
    }
}
impl fmt::Debug for HostHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostHandle").finish_non_exhaustive()
    }
}
