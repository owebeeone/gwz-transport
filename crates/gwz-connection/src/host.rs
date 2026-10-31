//! The connection host: one owner of a registry of endpoint instances that outlives an operation, and the handles
//! that reach it. Steps 1.1 and 1.2 of dev-docs/GwzTransportReleaseConvergencePlan.md; contract sections 4, 4.1, 5, 6, 10 and 11.
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
//! a global counter. It implements no endpoint: the SSH and HTTPS mechanisms stay behind the [`EndpointFactory`] port.
use crate::{
    Admission, AdmissionError, CleanupDomain, CleanupStatus, Direction, EndpointAdapter,
    EndpointSpec, FactoryEnv, FactoryError, FactoryReason, HostNamespace, HostPorts,
    PreparedOperation,
    binding::{BindingId, BindingShared, CarrierId, Discard, Routed, closing_code},
    cell::Cell,
    domain::{CleanupTicket, RetainedWork, Revocable},
    instance::{Instance, InstanceHold},
    operation::AdmitInput,
    operation::{OperationEnd, Registration, TerminalReason},
    ports::{Clock, EndpointFactory, Progress},
    pump::{self, Driver, Signal},
    safety::SharedProcessSafety,
    tables::{InstanceTables, Tables},
};
use gwz_transport::{mux::Attachment, pool};
use std::{
    collections::{BTreeMap, VecDeque},
    fmt,
    future::{Future, poll_fn},
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

/// How long an admission waits for its slot and its endpoint by default, in milliseconds (contract section 10).
///
/// Release-kind decision A (2026-10-31): the client's default is strictly below the core mux's bootstrap bound
/// (`gwz_transport::mux::Config::bootstrap_timeout_ms`, also 5 s), so a client admission wait ends, by name, before the
/// bootstrap bound that a core waiting on the client would also be running. The row
/// `the_default_admission_bound_is_strictly_below_the_mux_bootstrap_bound` fails if the two ever meet.
pub(crate) const DEFAULT_ADMISSION_BOUND_MS: u64 = 4_000;

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
    /// Requests one binding may register in each of its two sessions (the bootstrap's and every exchange's): the
    /// binding's own limit, never the host's.
    pub binding_registrations: usize,
    /// How long an admission may wait for its slot and its endpoint, in milliseconds. The default is
    /// [`DEFAULT_ADMISSION_BOUND_MS`], strictly below the core mux's bootstrap bound.
    pub admission_bound_ms: u64,
}
impl Default for HostLimits {
    fn default() -> Self {
        Self {
            active_slots: 8,
            waiting_admissions: 64,
            endpoint_instances: 16,
            close_bound_ms: 5_000,
            binding_registrations: 256,
            admission_bound_ms: DEFAULT_ADMISSION_BOUND_MS,
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
        if !(1..=MAX_CLOSE_BOUND_MS).contains(&self.admission_bound_ms) {
            return Err(ConfigReason::AdmissionBound);
        }
        // The transport's own range for a session's requests.
        if !(1..=4096).contains(&self.binding_registrations) {
            return Err(ConfigReason::BindingRegistrations);
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
    AdmissionBound,
    BindingRegistrations,
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
/// The most one admission may charge, and the most a host's admissions and operations may charge together (contract 10).
const MAX_ADMISSION_CHARGE: usize = 16 * 1024 * 1024;
const MAX_HOST_CHARGE: usize = 64 * 1024 * 1024;

/// A retained-charge reservation; dropping it gives the charge back. Atomics only, so it is safe to drop anywhere.
pub(crate) struct ChargeGuard {
    counter: Arc<AtomicUsize>,
    amount: usize,
}
impl Drop for ChargeGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.amount, Ordering::AcqRel);
    }
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
/// Fixed-size counts. Step 1.2 adds the pool, binding and exchange counters of contract section 11. On a carrier-backed
/// handle the snapshot is a refusal ([`refused`](Self::refused)): every count is meaningless there, and only `phase` and
/// `namespace` are the handle's own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct HostSnapshot {
    pub phase: HostPhase,
    pub namespace: HostNamespace,
    pub endpoint_instances: usize,
    /// Records the host's domain retains now (all hosts on it).
    pub retained_cleanup: usize,
    /// Registered operations' bindings.
    pub bindings: usize,
    /// Slots in use: reserved by a permit or held by a registered binding.
    pub active_operations: usize,
    /// Admissions queued for a slot.
    pub waiting_admissions: usize,
    /// The instances' connections by state, summed.
    pub connections: pool::Counts,
    refused: bool,
}
impl HostSnapshot {
    /// What a carrier-backed handle reports (GwzSplitCommandChannel 3.4): a refusal with no counts, every count zero. The
    /// client's pools, the hosts it is connected to, are not the core's to see. `phase` is the handle's own, truthfully:
    /// `Closed` once the carrier is lost.
    pub(crate) fn refusal(namespace: HostNamespace, phase: HostPhase) -> Self {
        Self {
            phase,
            namespace,
            endpoint_instances: 0,
            retained_cleanup: 0,
            bindings: 0,
            active_operations: 0,
            waiting_admissions: 0,
            connections: pool::Counts::default(),
            refused: true,
        }
    }
    /// Whether this is a carrier-backed handle's refusal. When it is, every count (`endpoint_instances`,
    /// `retained_cleanup`, `bindings`, `active_operations`, `waiting_admissions`, `connections`) is meaningless: its zeros
    /// say nothing of the client. Only `phase` and `namespace` are the handle's own.
    pub fn refused(&self) -> bool {
        self.refused
    }
}

struct Waiter {
    id: u64,
    waker: Option<Waker>,
}
/// A slot an acquired permit holds before it registers.
struct Claim {
    revoked: Option<TerminalReason>,
    /// The slot was given back already (by a revocation), so no one gives it back twice.
    released: bool,
}
pub(crate) struct HostState {
    phase: HostPhase,
    closing: Option<TerminalReason>,
    instances: Vec<Arc<Instance>>,
    /// Instances a factory is making, which count against the limit before they exist.
    creating: usize,
    /// The serial of the last attachment, which orders the instances by use (the least recently used goes first when
    /// the limit is reached). Host-owned, never a global; 2^64 attachments are out of reach.
    next_use: u64,
    close: Option<Cell<CleanupStatus>>,
    queue: VecDeque<Waiter>,
    next_waiter: u64,
    active: usize,
    next_claim: u64,
    claims: BTreeMap<u64, Claim>,
    /// Checked and monotonic: never wraps or resets.
    next_binding: u64,
    bindings: BTreeMap<u64, Arc<BindingShared>>,
}
impl HostState {
    /// Attaches to `instance`: the hold that keeps it from being retired, stamped with the next use serial.
    fn hold(&mut self, instance: &Arc<Instance>) -> InstanceHold {
        self.next_use += 1;
        instance.hold(self.next_use)
    }
    /// Takes out of the registry the least recently used instance that nothing holds and nothing is in flight on.
    fn take_least_recently_used_unbound(&mut self) -> Option<Arc<Instance>> {
        let index = self
            .instances
            .iter()
            .enumerate()
            .filter(|(_, instance)| instance.unbound())
            .min_by_key(|(_, instance)| instance.last_used())
            .map(|(index, _)| index)?;
        Some(self.instances.remove(index))
    }
}
pub(crate) struct HostShared {
    pub(crate) namespace: HostNamespace,
    pub(crate) clock: Arc<dyn Clock>,
    pub(crate) progress: Arc<dyn Progress>,
    /// How the instances' adapters reach the host's driver, and how a seal stops it.
    pub(crate) signal: Arc<Signal>,
    factory: Arc<dyn EndpointFactory>,
    domain: CleanupDomain,
    safety: SharedProcessSafety,
    pub(crate) limits: HostLimits,
    charged: Arc<AtomicUsize>,
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
    let signal = Signal::new();
    let shared = Arc::new(HostShared {
        namespace: ports.seed.namespace(),
        clock: ports.clock,
        progress: ports.progress,
        signal: signal.clone(),
        factory: ports.factory,
        domain: config.domain,
        safety: config.safety,
        limits: config.limits,
        charged: Arc::new(AtomicUsize::new(0)),
        state: Mutex::new(HostState {
            phase: HostPhase::Open,
            closing: None,
            instances: Vec::new(),
            creating: 0,
            next_use: 0,
            close: None,
            queue: VecDeque::new(),
            next_waiter: 0,
            active: 0,
            next_claim: 0,
            claims: BTreeMap::new(),
            next_binding: 1,
            bindings: BTreeMap::new(),
        }),
    });
    let weak: Weak<dyn Revocable> = Arc::<HostShared>::downgrade(&shared);
    shared.domain.register(weak)?;
    Ok((
        HostOwner {
            shared: Some(shared.clone()),
            driver: Driver::new(signal),
        },
        HostHandle { backend: shared },
    ))
}

impl HostShared {
    pub(crate) fn lock(&self) -> std::sync::MutexGuard<'_, HostState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Seals the host and starts the disposal of its instances on the composition's executor. Idempotent: every call
    /// returns the one close, and the first `reason` stands. Under the one state lock it also revokes every reserved
    /// claim (releasing its slot, once) and cancels every registered binding, so a registration either happened before
    /// this (and is counted as cancelled) or finds itself revoked.
    fn seal(self: &Arc<Self>, reason: TerminalReason) -> Cell<CleanupStatus> {
        // The driver ends at its next pass, whatever sealed the host (its owner, its domain, a progress fault).
        self.signal.stop();
        let (cell, instances, wakers) = {
            let mut state = self.lock();
            if let Some(cell) = &state.close {
                return cell.clone();
            }
            state.phase = HostPhase::Closing;
            state.closing = Some(reason);
            let cell = Cell::new();
            state.close = Some(cell.clone());
            let mut released = 0;
            for claim in state.claims.values_mut() {
                claim.revoked.get_or_insert(reason);
                if !claim.released {
                    claim.released = true;
                    released += 1;
                }
            }
            state.active -= released;
            for binding in state.bindings.values() {
                binding
                    .closing
                    .store(closing_code(reason), Ordering::Release);
                binding.cancelled.store(true, Ordering::Release);
            }
            let wakers: Vec<Waker> = state
                .queue
                .drain(..)
                .filter_map(|waiter| waiter.waker)
                .collect();
            (cell, std::mem::take(&mut state.instances), wakers)
        };
        for waker in wakers {
            waker.wake();
        }
        // The instances' pools stop (their connections are told to close), what is in flight on them ends with the
        // reason, and their adapters are taken for disposal.
        let mut adapters = Vec::new();
        for instance in &instances {
            {
                let mut state = instance.lock();
                state.pool.shutdown();
                instance.pump(&mut state);
            }
            instance.fail_in_flight(reason);
            if let Some(adapter) = instance.lock().adapter.take() {
                adapters.push(adapter);
            }
            instance.wake_waiters();
        }
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
    /// The host's progress failed (its executor refused or dropped the driver): the host is sealed with `HostFault`.
    pub(crate) fn fault(self: &Arc<Self>) {
        drop(self.seal(TerminalReason::HostFault));
    }
    /// The sizes of the host's tables and of its instances' per-operation tables.
    pub(crate) fn tables(&self) -> Tables {
        let (bindings, claims, queued, active, instances) = {
            let state = self.lock();
            (
                state.bindings.len(),
                state.claims.len(),
                state.queue.len(),
                state.active,
                state.instances.clone(),
            )
        };
        let mut tables = Tables {
            bindings,
            claims,
            queued,
            active,
            charged: self.charged.load(Ordering::Acquire),
            instances: instances.len(),
            instance: InstanceTables::default(),
        };
        for instance in &instances {
            tables.instance = tables.instance.plus(instance.tables());
        }
        tables
    }
    /// The instances the driver drives.
    pub(crate) fn instances(&self) -> Vec<Arc<Instance>> {
        self.lock().instances.clone()
    }
    /// The instance for `spec`'s profile: an existing one (equal profiles coalesce) or a new one the factory makes. The
    /// first instance starts the host's driver. The instance comes with a hold, taken under the registry lock: until the
    /// hold is dropped the instance is not retired.
    ///
    /// At the instance limit the least recently used instance that nothing holds and nothing is in flight on is retired
    /// to make room, in the same registry transition that reserves the new instance's place (contract sections 6 and
    /// 10): no attach can reach it after that, and none that reached it before can lose it. Its adapter is disposed
    /// under the cleanup domain. When every instance is held or busy, the limit is refused.
    pub(crate) fn attach_profile(
        self: &Arc<Self>,
        spec: &EndpointSpec,
    ) -> Result<InstanceHold, AttachError> {
        let find = |state: &HostState| {
            state
                .instances
                .iter()
                .find(|instance| instance.profile.same_as(&spec.profile))
                .cloned()
        };
        let retired = {
            let mut state = self.lock();
            if state.phase != HostPhase::Open {
                return Err(AttachError::Closing(HostClosing));
            }
            if let Some(instance) = find(&state) {
                return Ok(state.hold(&instance));
            }
            let retired =
                if state.instances.len() + state.creating >= self.limits.endpoint_instances {
                    match state.take_least_recently_used_unbound() {
                        Some(instance) => Some(instance),
                        None => return Err(AttachError::InstanceLimit),
                    }
                } else {
                    None
                };
            state.creating += 1;
            retired
        };
        if let Some(instance) = retired
            && self.dispose_retired(&instance).is_err()
        {
            self.lock().creating -= 1;
            return Err(AttachError::Closing(HostClosing));
        }
        // The factory runs outside the host's lock.
        let made = self
            .factory
            .create(spec, &FactoryEnv::new(&self.domain, &self.safety))
            .and_then(|adapter| {
                Instance::new(
                    spec.profile.clone(),
                    adapter,
                    self.clock.clone(),
                    self.signal.clone(),
                )
                .map_err(|_| FactoryError::new(FactoryReason::Unavailable, CleanupStatus::Complete))
            });
        let mut state = self.lock();
        state.creating -= 1;
        match made {
            Err(error) => Err(AttachError::Factory(error)),
            Ok(instance) if state.phase == HostPhase::Open => {
                // Another attach of the same profile may have finished first: keep the one that is in the table.
                if let Some(existing) = find(&state) {
                    return Ok(state.hold(&existing));
                }
                state.instances.push(instance.clone());
                let hold = state.hold(&instance);
                drop(state);
                // The executor is asked outside the host's lock; a refusal has faulted the host.
                pump::ensure_running(self).map_err(|_| AttachError::Closing(HostClosing))?;
                Ok(hold)
            }
            Ok(instance) => {
                // Sealed while the factory worked: the new instance belongs to the retainer, not to a closed host.
                drop(state);
                let adapter = instance.lock().adapter.take();
                if let Some(adapter) = adapter {
                    drop(self.pend(vec![adapter]));
                }
                Err(AttachError::Closing(HostClosing))
            }
        }
    }
    /// Disposes of an instance the registry has retired: its pool stops and its adapter goes to the cleanup domain's
    /// retainer, which owns the disposal until it is confirmed. A retainer that refuses leaves the work with the host,
    /// which has the executor drive it; an executor that refuses that is a host progress fault.
    fn dispose_retired(
        self: &Arc<Self>,
        instance: &Instance,
    ) -> Result<(), crate::ProgressUnavailable> {
        let Some(adapter) = instance.retire() else {
            return Ok(());
        };
        let (_ticket, kept) = self.pend(vec![adapter]);
        let Some(mut work) = kept else {
            return Ok(());
        };
        self.progress
            .spawn(Box::pin(poll_fn(move |cx| work.poll_work(cx))))
            .inspect_err(|_| self.fault())
    }
    /// Reserves `charge` of the retained-input budget, or refuses.
    fn charge(&self, charge: usize) -> Result<ChargeGuard, AdmissionError> {
        if charge > MAX_ADMISSION_CHARGE {
            return Err(AdmissionError::InputBudget);
        }
        let before = self.charged.fetch_add(charge, Ordering::AcqRel);
        let guard = ChargeGuard {
            counter: self.charged.clone(),
            amount: charge,
        };
        if before + charge > MAX_HOST_CHARGE {
            // `guard` gives the charge back as it drops.
            return Err(AdmissionError::InputBudget);
        }
        Ok(guard)
    }
    /// Validates and queues one admission record. Synchronous, with no endpoint, thread or effect.
    fn admit_input(self: Arc<Self>, input: PreparedOperation) -> Result<Admission, AdmissionError> {
        let charge = self.charge(input.auth.charge())?;
        let id = {
            let mut state = self.lock();
            if state.phase != HostPhase::Open {
                return Err(AdmissionError::Closing(
                    state.closing.unwrap_or(TerminalReason::HostClosing),
                ));
            }
            if state.queue.len() >= self.limits.waiting_admissions {
                return Err(AdmissionError::QueueFull);
            }
            let id = state.next_waiter;
            state.next_waiter = id.wrapping_add(1);
            state.queue.push_back(Waiter { id, waker: None });
            id
        };
        let deadline = self
            .clock
            .now()
            .plus(Duration::from_millis(self.limits.admission_bound_ms));
        let sleep = self.clock.sleep_until(deadline);
        Ok(Admission::local(self, id, input, charge, sleep))
    }
    /// What an admission found when it asked for its slot.
    pub(crate) fn take_slot(&self, id: u64, waker: &Waker) -> Slot {
        let mut state = self.lock();
        if state.phase != HostPhase::Open {
            state.queue.retain(|waiter| waiter.id != id);
            return Slot::Closing(state.closing.unwrap_or(TerminalReason::HostClosing));
        }
        if state.queue.front().is_some_and(|waiter| waiter.id == id)
            && state.active < self.limits.active_slots
        {
            let Some(serial) = state.next_binding.checked_add(1) else {
                state.queue.pop_front();
                return Slot::Exhausted;
            };
            state.queue.pop_front();
            let serial = std::mem::replace(&mut state.next_binding, serial);
            state.active += 1;
            let claim = state.next_claim;
            state.next_claim += 1;
            state.claims.insert(
                claim,
                Claim {
                    revoked: None,
                    released: false,
                },
            );
            return Slot::Granted { claim, serial };
        }
        if let Some(waiter) = state.queue.iter_mut().find(|waiter| waiter.id == id) {
            waiter.waker = Some(waker.clone());
        }
        Slot::Wait
    }
    /// Takes `id` out of the queue; the next waiter may now be at the front.
    pub(crate) fn dequeue(&self, id: u64) {
        let waker = {
            let mut state = self.lock();
            state.queue.retain(|waiter| waiter.id != id);
            state
                .queue
                .front_mut()
                .and_then(|waiter| waiter.waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    /// Gives a claim's slot back unless a revocation already did, and wakes the next waiter.
    pub(crate) fn drop_claim(&self, claim: u64) {
        let waker = {
            let mut state = self.lock();
            if let Some(claim) = state.claims.remove(&claim)
                && !claim.released
            {
                state.active -= 1;
            }
            state
                .queue
                .front_mut()
                .and_then(|waiter| waiter.waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
    /// The one authority transition of registration: under the state lock the claim is either still good (and the
    /// binding is published) or it was revoked (and the caller gets the reason). It does no I/O and calls no port.
    pub(crate) fn publish(
        &self,
        claim: u64,
        binding: &Arc<BindingShared>,
    ) -> Result<(), TerminalReason> {
        let (outcome, waker) = {
            let mut state = self.lock();
            let claim = state.claims.remove(&claim);
            let revoked = claim
                .as_ref()
                .and_then(|claim| claim.revoked)
                .or(match state.phase {
                    HostPhase::Open => None,
                    _ => Some(state.closing.unwrap_or(TerminalReason::HostClosing)),
                });
            match (claim, revoked) {
                (Some(_), None) => {
                    state.bindings.insert(binding.id.serial, binding.clone());
                    (Ok(()), None)
                }
                (claim, reason) => {
                    if claim.is_some_and(|claim| !claim.released) {
                        state.active -= 1;
                    }
                    let waker = state
                        .queue
                        .front_mut()
                        .and_then(|waiter| waiter.waker.take());
                    (Err(reason.unwrap_or(TerminalReason::HostFault)), waker)
                }
            }
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        outcome
    }
    /// When a sealed binding has nothing live left, it leaves the table, gives its slot back and reports its end.
    pub(crate) fn retire_if_done(&self, binding: &Arc<BindingShared>) {
        use std::sync::atomic::Ordering::{AcqRel, Acquire};
        if !binding.sealed.load(Acquire)
            || binding.live.load(Acquire) != 0
            || binding.retired.swap(true, AcqRel)
        {
            return;
        }
        let waker = {
            let mut state = self.lock();
            if state.bindings.remove(&binding.id.serial).is_some() {
                state.active -= 1;
            }
            state
                .queue
                .front_mut()
                .and_then(|waiter| waiter.waker.take())
        };
        if let Some(waker) = waker {
            waker.wake();
        }
        binding.done.set(OperationEnd {
            registration: Registration::Registered(binding.id),
            cleanup: CleanupStatus::Complete,
            exchanges_opened: binding.opened.load(Acquire),
            cancelled: binding.cancelled.load(Acquire),
            revoked: None,
        });
    }
    pub(crate) fn closing_reason(&self) -> TerminalReason {
        self.lock().closing.unwrap_or(TerminalReason::HostClosing)
    }
    pub(crate) fn carrier(&self) -> CarrierId {
        CarrierId(self.namespace)
    }
    /// A frame for a binding, from a carrier connection. Only the binding that this carrier bound takes it; anything
    /// else is discarded and no record is made.
    pub(crate) fn route(
        &self,
        carrier: CarrierId,
        binding: BindingId,
        direction: Direction,
        item: &Attachment,
    ) -> Routed {
        if carrier != self.carrier() {
            return Routed::Discarded(Discard::ForeignCarrier);
        }
        if binding.host != self.namespace {
            return Routed::Discarded(Discard::ForeignHost);
        }
        let entry = self.lock().bindings.get(&binding.serial).cloned();
        let Some(entry) = entry else {
            return Routed::Discarded(Discard::Retired);
        };
        if entry.carrier != carrier {
            return Routed::Discarded(Discard::ForeignCarrier);
        }
        if item.1.session_id != entry.session {
            return Routed::Discarded(Discard::WrongSession);
        }
        let delivered = entry
            .pair
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .deliver(direction, item);
        match delivered {
            Ok(()) => Routed::Delivered,
            Err(_) => {
                // The binding's own frame was invalid: it, and only it, is cancelled, as `OperationCancel::cancel` cancels
                // it (its openings that wait for a proof end, then its pool owner is cancelled). No host or pool lock
                // is held here.
                crate::exchange::cancel_binding(&entry);
                Routed::Discarded(Discard::Rejected)
            }
        }
    }
}
/// What an admission's request for a slot returned.
pub(crate) enum Slot {
    Wait,
    Granted { claim: u64, serial: u64 },
    Closing(TerminalReason),
    Exhausted,
}
impl Revocable for HostShared {
    fn revoke(self: Arc<Self>) -> Cell<CleanupStatus> {
        self.seal(TerminalReason::DomainClosing)
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
    /// The stop authority over the host's driver task.
    driver: Driver,
}
impl HostOwner {
    /// Seals the host now and returns the wait for its cleanup. The seal and the disposal do not depend on polling it.
    /// The host's driver stops: in-flight openings end with `OpenFailure::Closing(HostClosing)`.
    pub fn close(mut self) -> HostClose {
        self.driver.stop();
        let cell = match self.shared.take() {
            Some(shared) => shared.seal(TerminalReason::HostClosing),
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
        self.driver.stop();
        if let Some(shared) = self.shared.take() {
            drop(shared.seal(TerminalReason::HostClosing));
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

/// Observes the host and submits operations to it; cannot end it. `Clone + Send + Sync`.
#[derive(Clone)]
pub struct HostHandle {
    backend: Arc<dyn Backend>,
}
impl HostHandle {
    pub fn snapshot(&self) -> HostSnapshot {
        self.backend.snapshot()
    }
    /// Validates `input` and reserves one of the host's waiting-admission records. It registers nothing, starts no
    /// thread, makes no endpoint and has no network effect; `Admission::acquire` waits for the slot (contract 5).
    /// A local host takes a [`PreparedOperation`]; a carrier-backed handle (`remote::handle`) takes an
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
