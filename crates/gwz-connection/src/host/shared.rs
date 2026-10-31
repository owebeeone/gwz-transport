//! The state a host shares between its owner, its handles and its bindings: the registry and the admission tables
//! behind one lock, the seal and the instance registry. `admit` has the admission and routing half of its methods,
//! `close` the disposal job of a seal, `backend` the local backing of a handle.
use super::{
    AttachError, BuildError, HostClosing, HostConfig, HostHandle, HostLimits, HostOwner, HostPhase,
};
use crate::{
    CleanupDomain, CleanupStatus, EndpointAdapter, EndpointSpec, FactoryEnv, FactoryError,
    FactoryReason, HostNamespace, HostPorts,
    binding::{BindingShared, closing_code},
    cell::Cell,
    domain::{CleanupTicket, RetainedWork, Revocable},
    instance::{Instance, InstanceHold},
    operation::TerminalReason,
    ports::{Clock, EndpointFactory, Progress},
    pump::{self, Driver, Signal},
    safety::SharedProcessSafety,
    tables::{InstanceTables, Tables},
};
use close::{InstanceSlot, close_job};
use std::{
    collections::{BTreeMap, VecDeque},
    future::poll_fn,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::Waker,
};
mod admit;
mod backend;
mod close;

pub(crate) use admit::{ChargeGuard, Slot};

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
    pub(super) fn seal(self: &Arc<Self>, reason: TerminalReason) -> Cell<CleanupStatus> {
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
            if let Some(adapter) = instance.take_adapter() {
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
}
impl Revocable for HostShared {
    fn revoke(self: Arc<Self>) -> Cell<CleanupStatus> {
        self.seal(TerminalReason::DomainClosing)
    }
}
