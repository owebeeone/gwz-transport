//! Fakes for the ports of contract section 4.1: a manual clock, a stepped executor, a controllable endpoint factory,
//! a stepped retainer and a counting process-safety gate. Nothing here touches a file, a socket or a thread.
use crate::{
    CleanupDomain, CleanupDomainOwner, Clock, DomainConfig, DriverWork, EndpointAdapter,
    EndpointFactory, EndpointSpec, FactoryEnv, FactoryError, FactoryReason, HostConfig, HostHandle,
    HostLimits, HostNamespace, HostOwner, HostPorts, OwnerSeed, ProcessSafety, Progress,
    ProgressUnavailable, RetainedWork, RetentionPort, RetentionRefused, SafetyHold, SafetyState,
    SafetyUnavailable, Sleep, Tick, cleanup_domain,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

struct ClockState {
    now: u64,
    sleepers: Vec<(u64, Waker)>,
}

/// A clock only a test moves.
pub(crate) struct ManualClock {
    state: Arc<Mutex<ClockState>>,
}
impl ManualClock {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Arc::new(Mutex::new(ClockState {
                now: 0,
                sleepers: Vec::new(),
            })),
        })
    }
    pub(crate) fn advance(&self, ms: u64) {
        let due = {
            let mut state = self.state.lock().unwrap();
            state.now += ms;
            let now = state.now;
            let (due, later): (Vec<_>, Vec<_>) = std::mem::take(&mut state.sleepers)
                .into_iter()
                .partition(|(at, _)| *at <= now);
            state.sleepers = later;
            due
        };
        for (_, waker) in due {
            waker.wake();
        }
    }
}
struct SleepUntil {
    state: Arc<Mutex<ClockState>>,
    deadline: u64,
}
impl Future for SleepUntil {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let mut state = self.state.lock().unwrap();
        if state.now >= self.deadline {
            return Poll::Ready(());
        }
        state.sleepers.push((self.deadline, cx.waker().clone()));
        Poll::Pending
    }
}
impl Clock for ManualClock {
    fn now(&self) -> Tick {
        Tick::new(self.state.lock().unwrap().now)
    }
    fn sleep_until(&self, deadline: Tick) -> Sleep {
        Box::pin(SleepUntil {
            state: self.state.clone(),
            deadline: deadline.get(),
        })
    }
    fn epoch(&self) -> u64 {
        0
    }
}

pub(crate) struct FixedSeed(pub(crate) u128);
impl OwnerSeed for FixedSeed {
    fn namespace(&self) -> HostNamespace {
        HostNamespace::new(self.0)
    }
}

struct TaskWake {
    woken: AtomicBool,
}
impl Wake for TaskWake {
    fn wake(self: Arc<Self>) {
        self.woken.store(true, Ordering::SeqCst);
    }
}
struct Task {
    work: DriverWork,
    wake: Arc<TaskWake>,
}

/// An executor the test steps: spawned work runs only inside `run`, so a test sees what is true before it.
pub(crate) struct ManualProgress {
    tasks: Mutex<Vec<Task>>,
    available: AtomicBool,
    spawned: AtomicUsize,
}
impl ManualProgress {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            tasks: Mutex::new(Vec::new()),
            available: AtomicBool::new(true),
            spawned: AtomicUsize::new(0),
        })
    }
    pub(crate) fn set_available(&self, available: bool) {
        self.available.store(available, Ordering::SeqCst);
    }
    pub(crate) fn spawned(&self) -> usize {
        self.spawned.load(Ordering::SeqCst)
    }
    /// Polls every woken task until none is.
    pub(crate) fn run(&self) {
        loop {
            let batch = std::mem::take(&mut *self.tasks.lock().unwrap());
            let mut polled = false;
            let mut keep = Vec::new();
            for mut task in batch {
                if task.wake.woken.swap(false, Ordering::SeqCst) {
                    polled = true;
                    let waker = Waker::from(task.wake.clone());
                    let mut cx = Context::from_waker(&waker);
                    if task.work.as_mut().poll(&mut cx).is_pending() {
                        keep.push(task);
                    }
                } else {
                    keep.push(task);
                }
            }
            self.tasks.lock().unwrap().extend(keep);
            if !polled {
                break;
            }
        }
    }
}
impl Progress for ManualProgress {
    fn check(&self) -> Result<(), ProgressUnavailable> {
        if self.available.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err(ProgressUnavailable)
        }
    }
    fn spawn(&self, work: DriverWork) -> Result<(), ProgressUnavailable> {
        self.check()?;
        self.spawned.fetch_add(1, Ordering::SeqCst);
        self.tasks.lock().unwrap().push(Task {
            work,
            wake: Arc::new(TaskWake {
                woken: AtomicBool::new(true),
            }),
        });
        Ok(())
    }
}

/// Polls `future` once with a waker that does nothing.
pub(crate) fn poll_once<F: Future + Unpin>(future: &mut F) -> Poll<F::Output> {
    let mut cx = Context::from_waker(Waker::noop());
    Pin::new(future).poll(&mut cx)
}

/// What `release` opens: the disposal of every adapter that was made stuck.
pub(crate) struct Gate {
    open: Mutex<(bool, Vec<Waker>)>,
}
impl Gate {
    pub(crate) fn release(&self) {
        let wakers = {
            let mut open = self.open.lock().unwrap();
            open.0 = true;
            std::mem::take(&mut open.1)
        };
        for waker in wakers {
            waker.wake();
        }
    }
}

/// What the fake factory has seen and how its next adapters behave.
pub(crate) struct FakeWorld {
    pub(crate) created: AtomicUsize,
    pub(crate) disposed: AtomicUsize,
    pub(crate) dropped: AtomicUsize,
    pub(crate) stuck: AtomicBool,
    pub(crate) fail_next: AtomicBool,
    pub(crate) gate: Arc<Gate>,
    pub(crate) safeties: Mutex<Vec<(Arc<dyn ProcessSafety>, SafetyState)>>,
    pub(crate) domains: Mutex<Vec<CleanupDomain>>,
}
impl FakeWorld {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            created: AtomicUsize::new(0),
            disposed: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            stuck: AtomicBool::new(false),
            fail_next: AtomicBool::new(false),
            gate: Arc::new(Gate {
                open: Mutex::new((false, Vec::new())),
            }),
            safeties: Mutex::new(Vec::new()),
            domains: Mutex::new(Vec::new()),
        })
    }
    pub(crate) fn disposed(&self) -> usize {
        self.disposed.load(Ordering::SeqCst)
    }
    pub(crate) fn dropped(&self) -> usize {
        self.dropped.load(Ordering::SeqCst)
    }
}
pub(crate) struct FakeFactory(pub(crate) Arc<FakeWorld>);
impl EndpointFactory for FakeFactory {
    fn create(
        &self,
        _spec: &EndpointSpec,
        env: &FactoryEnv<'_>,
    ) -> Result<Box<dyn EndpointAdapter>, FactoryError> {
        let world = &self.0;
        world
            .safeties
            .lock()
            .unwrap()
            .push((env.safety().clone(), env.safety().state()));
        world.domains.lock().unwrap().push(env.domain().clone());
        if world.fail_next.swap(false, Ordering::SeqCst) {
            return Err(FactoryError::new(
                FactoryReason::Unavailable,
                crate::CleanupStatus::Complete,
            ));
        }
        world.created.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(FakeAdapter {
            world: world.clone(),
            stuck: world.stuck.load(Ordering::SeqCst),
            done: false,
        }))
    }
}
struct FakeAdapter {
    world: Arc<FakeWorld>,
    stuck: bool,
    done: bool,
}
impl EndpointAdapter for FakeAdapter {
    fn poll_dispose(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.done {
            return Poll::Ready(());
        }
        if self.stuck {
            let mut open = self.world.gate.open.lock().unwrap();
            if !open.0 {
                open.1.push(cx.waker().clone());
                return Poll::Pending;
            }
        }
        self.done = true;
        self.world.disposed.fetch_add(1, Ordering::SeqCst);
        Poll::Ready(())
    }
}
impl Drop for FakeAdapter {
    fn drop(&mut self) {
        self.world.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

/// A retainer the test steps. `refuse` makes it hand the work back.
pub(crate) struct SteppedRetention {
    works: Mutex<Vec<RetainedWork>>,
    refuse: AtomicBool,
}
impl SteppedRetention {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            works: Mutex::new(Vec::new()),
            refuse: AtomicBool::new(false),
        })
    }
    pub(crate) fn set_refuse(&self, refuse: bool) {
        self.refuse.store(refuse, Ordering::SeqCst);
    }
    pub(crate) fn held(&self) -> usize {
        self.works.lock().unwrap().len()
    }
    /// One pass over the held work; confirmed work leaves.
    pub(crate) fn step_all(&self) {
        self.works.lock().unwrap().retain_mut(|work| !work.step());
    }
}
impl RetentionPort for SteppedRetention {
    fn retain(&self, work: RetainedWork) -> Result<(), RetentionRefused> {
        if self.refuse.load(Ordering::SeqCst) {
            return Err(RetentionRefused::new(work));
        }
        self.works.lock().unwrap().push(work);
        Ok(())
    }
}

/// A gate that counts its holds; `unavailable` is the Windows object that could not be created.
pub(crate) struct FakeSafety {
    state: SafetyState,
    pub(crate) holds: AtomicUsize,
}
impl FakeSafety {
    pub(crate) fn qualified() -> Arc<Self> {
        Arc::new(Self {
            state: SafetyState::Qualified,
            holds: AtomicUsize::new(0),
        })
    }
    pub(crate) fn unavailable() -> Arc<Self> {
        Arc::new(Self {
            state: SafetyState::Unavailable(SafetyUnavailable::ForeignOwner),
            holds: AtomicUsize::new(0),
        })
    }
}
impl ProcessSafety for FakeSafety {
    fn state(&self) -> SafetyState {
        self.state
    }
    fn hold(&self) -> Result<SafetyHold, SafetyUnavailable> {
        match self.state {
            SafetyState::Qualified => {
                self.holds.fetch_add(1, Ordering::SeqCst);
                Ok(SafetyHold::new(()))
            }
            SafetyState::Unavailable(reason) => Err(reason),
        }
    }
}

/// Everything one composition supplies: one clock, one executor, one retainer, one gate, one domain.
pub(crate) struct Rig {
    pub(crate) clock: Arc<ManualClock>,
    pub(crate) progress: Arc<ManualProgress>,
    pub(crate) world: Arc<FakeWorld>,
    pub(crate) retention: Arc<SteppedRetention>,
    pub(crate) safety: Arc<FakeSafety>,
    pub(crate) domain: CleanupDomain,
    pub(crate) domain_owner: Option<CleanupDomainOwner>,
}
impl Rig {
    pub(crate) fn new() -> Self {
        Self::with_safety(FakeSafety::qualified())
    }
    pub(crate) fn with_safety(safety: Arc<FakeSafety>) -> Self {
        let retention = SteppedRetention::new();
        let (domain_owner, domain) =
            cleanup_domain(DomainConfig::default(), retention.clone()).unwrap();
        Self {
            clock: ManualClock::new(),
            progress: ManualProgress::new(),
            world: FakeWorld::new(),
            retention,
            safety,
            domain,
            domain_owner: Some(domain_owner),
        }
    }
    pub(crate) fn ports(&self, seed: u128) -> HostPorts {
        HostPorts::new(
            self.clock.clone(),
            Arc::new(FixedSeed(seed)),
            self.progress.clone(),
            Arc::new(FakeFactory(self.world.clone())),
        )
    }
    pub(crate) fn config(&self, limits: HostLimits) -> HostConfig {
        HostConfig {
            domain: self.domain.clone(),
            safety: self.safety.clone(),
            limits,
        }
    }
    pub(crate) fn host(&self, seed: u128) -> (HostOwner, HostHandle) {
        crate::build(self.config(HostLimits::default()), self.ports(seed)).unwrap()
    }
    /// One more instance on `handle`, made through the factory.
    pub(crate) fn attach(&self, handle: &HostHandle) {
        handle.attach_instance(EndpointSpec::new([7; 32])).unwrap();
    }
}
