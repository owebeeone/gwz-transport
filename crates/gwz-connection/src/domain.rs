//! The cleanup domain of contract section 4.1: the externally owned authority that outlives the hosts built on it and
//! keeps a failed host's cleanup until it is confirmed.
//!
//! The composition makes one with `cleanup_domain(config, retention)` and passes its handle to every host in
//! `HostConfig`. The `RetentionPort` is the composition's supervised cleanup service (a stepped fake in tests, core's
//! reaper in step 1.3): once it has accepted a `RetainedWork` it owns it until the work's disposal is confirmed,
//! whatever becomes of the host, the tickets and the domain handles.
use crate::{BuildError, Clock, ConfigReason, EndpointAdapter, Tick, cell::Cell};
use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    task::{Context, Poll, Waker},
};

/// The finite budgets a domain holds for its composition (contract section 10). Plain limits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DomainConfig {
    pub setup_jobs: usize,
    pub retained_cleanup: usize,
    pub helper_slots: usize,
}
impl Default for DomainConfig {
    fn default() -> Self {
        Self {
            setup_jobs: 64,
            retained_cleanup: 64,
            helper_slots: 8,
        }
    }
}

/// Whether a retained generation's disposal is confirmed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TicketStatus {
    Pending,
    Confirmed,
}
struct TicketState {
    confirmed: bool,
    wakers: Vec<Waker>,
}
struct TicketShared {
    generation: u64,
    clock: Arc<dyn Clock>,
    state: Mutex<TicketState>,
}
/// A constant-size, cloneable view of one retained generation. It observes; it has no way to release anything.
#[derive(Clone)]
pub struct CleanupTicket(Arc<TicketShared>);
impl CleanupTicket {
    pub(crate) fn new(generation: u64, clock: Arc<dyn Clock>) -> Self {
        Self(Arc::new(TicketShared {
            generation,
            clock,
            state: Mutex::new(TicketState {
                confirmed: false,
                wakers: Vec::new(),
            }),
        }))
    }
    /// Which retained generation this names; a host's ticket and the domain's, for the same cleanup, are equal.
    pub fn generation(&self) -> u64 {
        self.0.generation
    }
    pub fn status(&self) -> TicketStatus {
        if self
            .0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .confirmed
        {
            TicketStatus::Confirmed
        } else {
            TicketStatus::Pending
        }
    }
    /// Resolves with the status at confirmation or at `deadline`, whichever comes first.
    pub fn wait_until(&self, deadline: Tick) -> WaitUntil {
        WaitUntil {
            ticket: self.clone(),
            sleep: self.0.clock.sleep_until(deadline),
        }
    }
    fn confirm(&self) {
        let wakers = {
            let mut state = self.0.state.lock().unwrap_or_else(|e| e.into_inner());
            state.confirmed = true;
            std::mem::take(&mut state.wakers)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}
impl fmt::Debug for CleanupTicket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CleanupTicket")
            .field("generation", &self.0.generation)
            .field("status", &self.status())
            .finish()
    }
}
/// See [`CleanupTicket::wait_until`].
pub struct WaitUntil {
    ticket: CleanupTicket,
    sleep: crate::Sleep,
}
impl Future for WaitUntil {
    type Output = TicketStatus;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<TicketStatus> {
        {
            let mut state = self
                .ticket
                .0
                .state
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if state.confirmed {
                return Poll::Ready(TicketStatus::Confirmed);
            }
            if !state.wakers.iter().any(|waker| waker.will_wake(cx.waker())) {
                state.wakers.push(cx.waker().clone());
            }
        }
        if self.sleep.as_mut().poll(cx).is_ready() {
            return Poll::Ready(self.ticket.status());
        }
        Poll::Pending
    }
}

/// How a close or a factory error ended its cleanup. `Pending` is not a disposal receipt: the retainer still owns
/// the work, and dropping the ticket releases nothing.
#[derive(Clone, Debug)]
pub enum CleanupStatus {
    Complete,
    Pending(CleanupTicket),
}

/// Work the retainer owns until its disposal is confirmed: the adapters a host could not dispose within its bound.
pub struct RetainedWork {
    generation: u64,
    adapters: Vec<Box<dyn EndpointAdapter>>,
    ticket: CleanupTicket,
    domain: CleanupDomain,
    /// Whether the domain's budget counts this work (set by the domain on acceptance, cleared at release).
    counted: bool,
}
impl RetainedWork {
    pub(crate) fn new(
        generation: u64,
        adapters: Vec<Box<dyn EndpointAdapter>>,
        ticket: CleanupTicket,
        domain: CleanupDomain,
    ) -> Self {
        Self {
            generation,
            adapters,
            ticket,
            domain,
            counted: false,
        }
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// One pass over the work with a waker that does nothing, for a retainer that polls on its own schedule.
    /// `true` once every adapter's disposal is confirmed: the ticket is then confirmed and the domain's budget
    /// released, once.
    pub fn step(&mut self) -> bool {
        self.poll_work(&mut Context::from_waker(Waker::noop()))
            .is_ready()
    }
    pub(crate) fn poll_work(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.adapters
            .retain_mut(|adapter| adapter.poll_dispose(cx).is_pending());
        if !self.adapters.is_empty() {
            return Poll::Pending;
        }
        self.ticket.confirm();
        if self.counted {
            self.counted = false;
            self.domain.release();
        }
        Poll::Ready(())
    }
}
/// The retainer would not take the work; it comes back, and its owner keeps it.
pub struct RetentionRefused(RetainedWork);
impl RetentionRefused {
    pub fn new(work: RetainedWork) -> Self {
        Self(work)
    }
    pub fn into_work(self) -> RetainedWork {
        self.0
    }
}
/// The composition's supervised cleanup service.
pub trait RetentionPort: Send + Sync + 'static {
    /// Takes ownership of `work` until its disposal is confirmed, or returns it.
    fn retain(&self, work: RetainedWork) -> Result<(), RetentionRefused>;
}

/// What a domain can end on behalf of a host: seals it and starts its close (or returns the close it has).
pub(crate) trait Revocable: Send + Sync {
    fn revoke(self: Arc<Self>) -> Cell<CleanupStatus>;
}
struct DomainState {
    sealed: bool,
    in_use: usize,
    next_generation: u64,
    hosts: Vec<Weak<dyn Revocable>>,
    tickets: Vec<CleanupTicket>,
}
struct DomainShared {
    config: DomainConfig,
    retention: Arc<dyn RetentionPort>,
    state: Mutex<DomainState>,
}
/// The submission capability: `Clone + Send + Sync`, with no close and no reset.
#[derive(Clone)]
pub struct CleanupDomain(Arc<DomainShared>);
impl CleanupDomain {
    pub fn config(&self) -> DomainConfig {
        self.0.config
    }
    /// Retained records the domain's budget counts now.
    pub fn retained(&self) -> usize {
        self.0
            .state
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .in_use
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, DomainState> {
        self.0.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    /// Registers a host so the domain can revoke it; refused once the domain is sealed.
    pub(crate) fn register(&self, host: Weak<dyn Revocable>) -> Result<(), BuildError> {
        let mut state = self.lock();
        if state.sealed {
            return Err(BuildError::DomainClosing);
        }
        state.hosts.retain(|host| host.strong_count() > 0);
        state.hosts.push(host);
        Ok(())
    }
    pub(crate) fn next_generation(&self) -> u64 {
        let mut state = self.lock();
        state.next_generation += 1;
        state.next_generation
    }
    /// Offers `work` to the retainer if the budget has room. A refusal, by the budget or by the retainer, hands the
    /// work back uncounted.
    pub(crate) fn retain(&self, mut work: RetainedWork) -> Result<(), RetentionRefused> {
        {
            let mut state = self.lock();
            if state.in_use >= self.0.config.retained_cleanup {
                return Err(RetentionRefused(work));
            }
            state.in_use += 1;
            state
                .tickets
                .retain(|ticket| ticket.status() == TicketStatus::Pending);
            state.tickets.push(work.ticket.clone());
        }
        work.counted = true;
        match self.0.retention.retain(work) {
            Ok(()) => Ok(()),
            Err(refused) => {
                let mut work = refused.into_work();
                work.counted = false;
                self.release();
                Err(RetentionRefused(work))
            }
        }
    }
    fn release(&self) {
        let mut state = self.lock();
        state.in_use = state.in_use.saturating_sub(1);
    }
    /// Seals the domain and returns the hosts to revoke. Idempotent.
    fn seal(&self) -> Vec<Arc<dyn Revocable>> {
        let mut state = self.lock();
        state.sealed = true;
        std::mem::take(&mut state.hosts)
            .into_iter()
            .filter_map(|host| host.upgrade())
            .collect()
    }
    fn first_pending_ticket(&self) -> Option<CleanupTicket> {
        self.lock()
            .tickets
            .iter()
            .find(|ticket| ticket.status() == TicketStatus::Pending)
            .cloned()
    }
}

/// Builds a domain. The retainer must already exist: a domain without one cannot be made.
pub fn cleanup_domain(
    config: DomainConfig,
    retention: Arc<dyn RetentionPort>,
) -> Result<(CleanupDomainOwner, CleanupDomain), BuildError> {
    if config.setup_jobs == 0 || config.retained_cleanup == 0 || config.helper_slots == 0 {
        return Err(BuildError::InvalidConfig(ConfigReason::DomainLimits));
    }
    let domain = CleanupDomain(Arc::new(DomainShared {
        config,
        retention,
        state: Mutex::new(DomainState {
            sealed: false,
            in_use: 0,
            next_generation: 0,
            hosts: Vec::new(),
            tickets: Vec::new(),
        }),
    }));
    Ok((
        CleanupDomainOwner {
            domain: Some(domain.clone()),
        },
        domain,
    ))
}

/// The composition's final shutdown authority over the domain. `Send`, not `Clone`.
pub struct CleanupDomainOwner {
    domain: Option<CleanupDomain>,
}
impl CleanupDomainOwner {
    /// Seals the domain and revokes every host on it now; the result resolves when their closes have (each is
    /// bounded by its own host's close bound) to `Complete`, or to a ticket for the first retained generation.
    pub fn close(mut self) -> DomainClose {
        match self.domain.take() {
            Some(domain) => DomainClose::start(domain),
            None => DomainClose {
                domain: None,
                cells: Vec::new(),
            },
        }
    }
}
impl Drop for CleanupDomainOwner {
    fn drop(&mut self) {
        if let Some(domain) = self.domain.take() {
            drop(DomainClose::start(domain));
        }
    }
}
impl fmt::Debug for CleanupDomainOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CleanupDomainOwner").finish_non_exhaustive()
    }
}

/// The wait for a domain's close.
pub struct DomainClose {
    domain: Option<CleanupDomain>,
    cells: Vec<Cell<CleanupStatus>>,
}
impl DomainClose {
    fn start(domain: CleanupDomain) -> Self {
        let cells = domain
            .seal()
            .into_iter()
            .map(|host| host.revoke())
            .collect();
        Self {
            domain: Some(domain),
            cells,
        }
    }
}
impl Future for DomainClose {
    type Output = CleanupStatus;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<CleanupStatus> {
        let mut statuses = Vec::with_capacity(self.cells.len());
        let mut waiting = false;
        for cell in &self.cells {
            match cell.poll(cx) {
                Poll::Ready(status) => statuses.push(status),
                Poll::Pending => waiting = true,
            }
        }
        if waiting {
            return Poll::Pending;
        }
        let pending = statuses
            .into_iter()
            .find_map(|status| match status {
                CleanupStatus::Pending(ticket) if ticket.status() == TicketStatus::Pending => {
                    Some(ticket)
                }
                _ => None,
            })
            .or_else(|| {
                self.domain
                    .as_ref()
                    .and_then(CleanupDomain::first_pending_ticket)
            });
        Poll::Ready(match pending {
            Some(ticket) => CleanupStatus::Pending(ticket),
            None => CleanupStatus::Complete,
        })
    }
}
