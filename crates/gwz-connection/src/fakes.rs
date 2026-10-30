//! Fakes for the ports of contract section 4.1: a manual clock, a stepped executor, a controllable endpoint factory,
//! a stepped retainer and a counting process-safety gate. Nothing here touches a file, a socket or a thread. The fake
//! adapter completes at once by default; `asynchronous` makes its connects (and `hold_finish` its exchanges' ends)
//! complete only when the test says, waking the waker the host gave it, as a real adapter's worker does.
use crate::{
    AuthContext, AuthScope, CleanupDomain, CleanupDomainOwner, Clock, Completion, ConnectCommand,
    DomainConfig, DriverWork, EndpointAdapter, EndpointFactory, EndpointProfile, EndpointRoute,
    EndpointSpec, ExchangeIo, ExchangeSpec, FactoryEnv, FactoryError, FactoryReason, GitService,
    HostConfig, HostHandle, HostLimits, HostNamespace, HostOwner, HostPorts, OperationPolicy,
    OwnerSeed, PreparedOperation, ProcessSafety, Progress, ProgressUnavailable, ProofId,
    RetainedWork, RetentionPort, RetentionRefused, ReuseCommand, ReuseVerdict, SafetyHold,
    SafetyState, SafetyUnavailable, Sleep, Tick, cleanup_domain,
};
use gwz_transport::{
    pool::{ConnectionId, Identity, Key},
    protocol::Disposition,
};
use std::{
    collections::VecDeque,
    future::Future,
    io,
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
    /// Tasks spawned and not yet finished.
    pub(crate) fn pending(&self) -> usize {
        self.tasks.lock().unwrap().len()
    }
    /// The executor goes away: every unfinished task is dropped unrun.
    pub(crate) fn drop_all(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        drop(tasks);
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
    pub(crate) connects: AtomicUsize,
    pub(crate) closes: AtomicUsize,
    pub(crate) exchanges: AtomicUsize,
    /// The connection each exchange started on, in order.
    pub(crate) exchange_connections: Mutex<Vec<ConnectionId>>,
    /// The next exchange to finish reports that its connection cannot be reused.
    pub(crate) discard_next: AtomicBool,
    /// Connects complete only on `complete_connects`, as a real worker's do.
    pub(crate) asynchronous: AtomicBool,
    /// Exchanges' ends are confirmed only on `confirm_finishes`.
    pub(crate) hold_finish: AtomicBool,
    pub(crate) confirmed: AtomicBool,
    pub(crate) cancelled_connects: AtomicUsize,
    /// The service and path of the last exchange started.
    pub(crate) last_spec: Mutex<Option<(GitService, String)>>,
    /// Every reuse proof the host asked for, and how the next ones end: `None` starts none (the port's default),
    /// and `hold_reuse` keeps them running until `end_reuse`.
    pub(crate) reuses: Mutex<Vec<ReuseCommand>>,
    pub(crate) reuse_verdict: Mutex<Option<ReuseVerdict>>,
    pub(crate) hold_reuse: AtomicBool,
    /// Every proof the host cancelled, in order.
    pub(crate) cancelled_reuses: Mutex<Vec<ProofId>>,
    /// The next exchange start finds its connection retired (the adapter's `NotConnected`).
    pub(crate) start_lost: AtomicBool,
    /// The next this many exchange starts find their connection retired.
    pub(crate) start_lost_n: AtomicUsize,
    /// Every adapter's queue and the waker the host last gave it.
    queues: Mutex<Vec<Arc<Mutex<Queue>>>>,
    /// Which operation's context each connect and exchange was given, by the provider's tag, in order.
    pub(crate) contexts: Mutex<Vec<(&'static str, Option<String>)>>,
    /// The validated inputs each adapter was made from, by their tag.
    pub(crate) inputs: Mutex<Vec<Option<String>>>,
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
            connects: AtomicUsize::new(0),
            closes: AtomicUsize::new(0),
            exchanges: AtomicUsize::new(0),
            exchange_connections: Mutex::new(Vec::new()),
            discard_next: AtomicBool::new(false),
            asynchronous: AtomicBool::new(false),
            hold_finish: AtomicBool::new(false),
            confirmed: AtomicBool::new(false),
            cancelled_connects: AtomicUsize::new(0),
            last_spec: Mutex::new(None),
            reuses: Mutex::new(Vec::new()),
            reuse_verdict: Mutex::new(Some(ReuseVerdict::Authorized)),
            hold_reuse: AtomicBool::new(false),
            cancelled_reuses: Mutex::new(Vec::new()),
            start_lost: AtomicBool::new(false),
            start_lost_n: AtomicUsize::new(0),
            queues: Mutex::new(Vec::new()),
            contexts: Mutex::new(Vec::new()),
            inputs: Mutex::new(Vec::new()),
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
    pub(crate) fn connects(&self) -> usize {
        self.connects.load(Ordering::SeqCst)
    }
    pub(crate) fn closes(&self) -> usize {
        self.closes.load(Ordering::SeqCst)
    }
    /// Every adapter's pending connects succeed now; the hosts' wakers are woken (no caller is polled).
    pub(crate) fn complete_connects(&self) {
        self.each_queue(|queue| {
            for (connection, identity) in std::mem::take(&mut queue.connecting) {
                queue.connected.push(connection);
                queue.completions.push_back(Completion::Connected {
                    connection,
                    result: Ok(Some(identity)),
                });
            }
        });
    }
    /// Every held reuse proof ends with `verdict`; the hosts' wakers are woken.
    pub(crate) fn end_reuse(&self, verdict: ReuseVerdict) {
        let held: Vec<_> = self.reuses.lock().unwrap().clone();
        for command in held {
            self.end_reuse_of(command, verdict);
        }
    }
    /// The held proof `command` alone ends with `verdict`, as an adapter's worker reports one proof.
    pub(crate) fn end_reuse_of(&self, command: ReuseCommand, verdict: ReuseVerdict) {
        self.each_queue(|queue| {
            if queue.connected.contains(&command.connection) {
                queue.completions.push_back(Completion::Reuse {
                    connection: command.connection,
                    proof: command.proof,
                    verdict,
                });
            }
        });
    }
    pub(crate) fn cancelled_reuses(&self) -> Vec<ProofId> {
        self.cancelled_reuses.lock().unwrap().clone()
    }
    pub(crate) fn reuses(&self) -> Vec<ReuseCommand> {
        self.reuses.lock().unwrap().clone()
    }
    /// Every connection every adapter holds is lost while idle (the peer closed it), and disposed.
    pub(crate) fn lose_connections(&self) {
        self.each_queue(|queue| {
            for connection in std::mem::take(&mut queue.connected) {
                queue
                    .completions
                    .push_back(Completion::IdleClosed { connection });
            }
        });
    }
    /// The connections every adapter holds now, in the order they connected.
    pub(crate) fn connected_ids(&self) -> Vec<ConnectionId> {
        let queues = self.queues.lock().unwrap().clone();
        queues
            .iter()
            .flat_map(|queue| queue.lock().unwrap().connected.clone())
            .collect()
    }
    /// `connection` alone is lost while idle (the peer closed it), and its adapter disposes of it and reports it.
    pub(crate) fn idle_closed_of(&self, connection: ConnectionId) {
        self.each_queue(|queue| {
            if queue.connected.contains(&connection) {
                queue.connected.retain(|id| *id != connection);
                queue
                    .completions
                    .push_back(Completion::IdleClosed { connection });
            }
        });
    }
    /// The held exchanges' ends are confirmed; the hosts' wakers are woken.
    pub(crate) fn confirm_finishes(&self) {
        self.confirmed.store(true, Ordering::SeqCst);
        self.each_queue(|_| {});
    }
    fn each_queue(&self, change: impl Fn(&mut Queue)) {
        let queues = self.queues.lock().unwrap().clone();
        for queue in queues {
            let waker = {
                let mut queue = queue.lock().unwrap();
                change(&mut queue);
                queue.waker.clone()
            };
            if let Some(waker) = waker {
                waker.wake();
            }
        }
    }
}
/// One fake adapter's state that the test reaches through the world.
#[derive(Default)]
struct Queue {
    completions: VecDeque<Completion>,
    connecting: Vec<(ConnectionId, Identity)>,
    connected: Vec<ConnectionId>,
    waker: Option<Waker>,
}
pub(crate) struct FakeFactory(pub(crate) Arc<FakeWorld>);
impl EndpointFactory for FakeFactory {
    fn create(
        &self,
        spec: &EndpointSpec,
        env: &FactoryEnv<'_>,
    ) -> Result<Box<dyn EndpointAdapter>, FactoryError> {
        let world = &self.0;
        world.inputs.lock().unwrap().push(
            spec.profile
                .inputs()
                .and_then(|inputs| inputs.downcast_ref::<String>())
                .cloned(),
        );
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
        let queue = Arc::new(Mutex::new(Queue::default()));
        world.queues.lock().unwrap().push(queue.clone());
        Ok(Box::new(FakeAdapter {
            world: world.clone(),
            stuck: world.stuck.load(Ordering::SeqCst),
            done: false,
            queue,
        }))
    }
}
struct FakeAdapter {
    world: Arc<FakeWorld>,
    stuck: bool,
    done: bool,
    queue: Arc<Mutex<Queue>>,
}
/// A loopback exchange: what is written is what is read.
struct FakeIo {
    world: Arc<FakeWorld>,
    queue: Arc<Mutex<Queue>>,
    buffer: VecDeque<u8>,
}
impl ExchangeIo for FakeIo {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        self.buffer.extend(data);
        Ok(data.len())
    }
    fn read(&mut self, out: &mut [u8]) -> io::Result<usize> {
        let mut n = 0;
        while n < out.len() {
            match self.buffer.pop_front() {
                Some(byte) => {
                    out[n] = byte;
                    n += 1;
                }
                None => break,
            }
        }
        Ok(n)
    }
    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Disposition> {
        if self.world.hold_finish.load(Ordering::SeqCst)
            && !self.world.confirmed.load(Ordering::SeqCst)
        {
            // The adapter's worker wakes the host's waker when it confirms; the fake keeps it in its queue.
            self.queue.lock().unwrap().waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(if self.world.discard_next.swap(false, Ordering::SeqCst) {
            Disposition::Discarded
        } else {
            Disposition::Reusable
        })
    }
    fn abort(&mut self) {}
}
impl EndpointAdapter for FakeAdapter {
    fn connect(&mut self, command: ConnectCommand) {
        self.world.connects.fetch_add(1, Ordering::SeqCst);
        self.world
            .contexts
            .lock()
            .unwrap()
            .push(("connect", command.auth.as_ref().and_then(tag_of)));
        let mut queue = self.queue.lock().unwrap();
        if self.world.asynchronous.load(Ordering::SeqCst) {
            queue
                .connecting
                .push((command.connection, command.identity));
            return;
        }
        queue.connected.push(command.connection);
        queue.completions.push_back(Completion::Connected {
            connection: command.connection,
            // The proof of the identity the connection was made for is what lets the pool reuse it.
            result: Ok(Some(command.identity)),
        });
    }
    fn cancel_connect(&mut self, connection: ConnectionId) {
        self.world.cancelled_connects.fetch_add(1, Ordering::SeqCst);
        let mut queue = self.queue.lock().unwrap();
        // The cancelled connect still reports, after its disposal.
        if let Some(index) = queue
            .connecting
            .iter()
            .position(|(id, _)| *id == connection)
        {
            queue.connecting.remove(index);
            queue.completions.push_back(Completion::Connected {
                connection,
                result: Err(gwz_transport::protocol::Failure {
                    code: gwz_transport::protocol::ErrorCode::Cancelled,
                    ..Default::default()
                }),
            });
        }
    }
    fn close(&mut self, connection: ConnectionId) {
        self.world.closes.fetch_add(1, Ordering::SeqCst);
        let mut queue = self.queue.lock().unwrap();
        queue.connected.retain(|id| *id != connection);
        queue
            .completions
            .push_back(Completion::Closed { connection });
    }
    fn start_exchange(
        &mut self,
        connection: ConnectionId,
        spec: &ExchangeSpec,
        auth: &AuthContext,
    ) -> io::Result<Box<dyn ExchangeIo>> {
        if self.world.start_lost.swap(false, Ordering::SeqCst)
            || self
                .world
                .start_lost_n
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            return Err(io::ErrorKind::NotConnected.into());
        }
        self.world.exchanges.fetch_add(1, Ordering::SeqCst);
        self.world
            .exchange_connections
            .lock()
            .unwrap()
            .push(connection);
        *self.world.last_spec.lock().unwrap() = Some((spec.service(), spec.path().into()));
        self.world
            .contexts
            .lock()
            .unwrap()
            .push(("exchange", tag_of(auth)));
        Ok(Box::new(FakeIo {
            world: self.world.clone(),
            queue: self.queue.clone(),
            buffer: VecDeque::new(),
        }))
    }
    fn authorize_reuse(&mut self, command: ReuseCommand) -> bool {
        let Some(verdict) = *self.world.reuse_verdict.lock().unwrap() else {
            return false;
        };
        self.world.reuses.lock().unwrap().push(command);
        if !self.world.hold_reuse.load(Ordering::SeqCst) {
            self.queue
                .lock()
                .unwrap()
                .completions
                .push_back(Completion::Reuse {
                    connection: command.connection,
                    proof: command.proof,
                    verdict,
                });
        }
        true
    }
    fn cancel_reuse(&mut self, proof: ProofId) {
        self.world.cancelled_reuses.lock().unwrap().push(proof);
    }
    fn poll_completion(&mut self, cx: &mut Context<'_>) -> Poll<Completion> {
        let mut queue = self.queue.lock().unwrap();
        match queue.completions.pop_front() {
            Some(completion) => Poll::Ready(completion),
            None => {
                queue.waker = Some(cx.waker().clone());
                Poll::Pending
            }
        }
    }
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

/// The tag of a context `auth` made: what the adapter can see of whose operation it serves.
fn tag_of(auth: &AuthContext) -> Option<String> {
    auth.provider().downcast_ref::<String>().cloned()
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
    profiles: AtomicUsize,
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
            profiles: AtomicUsize::new(0),
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
    /// One more instance on `handle`, made through the factory (each call a profile of its own).
    pub(crate) fn attach(&self, handle: &HostHandle) {
        let n = self.profiles.fetch_add(1, Ordering::SeqCst) as u8 + 100;
        handle.attach_instance(spec(n)).unwrap();
    }
}

/// The profile of kind `n`: equal for equal `n`, different otherwise.
pub(crate) fn profile(n: u8) -> EndpointProfile {
    EndpointProfile::new(vec![n; 8], [n; 32]).unwrap()
}
pub(crate) fn spec(n: u8) -> EndpointSpec {
    EndpointSpec::new(profile(n))
}
/// A valid frozen operation named `name` over the profile of kind `n`.
pub(crate) fn prepared(name: &str, n: u8) -> PreparedOperation {
    prepared_with(name, n, auth(0, "none"))
}
/// An authentication context of scope `scope` whose provider is the text `tag`.
pub(crate) fn auth(scope: u8, tag: &str) -> AuthContext {
    AuthContext::new(64, AuthScope::new([scope; 32]), Arc::new(tag.to_string()))
}
/// A valid frozen operation named `name` over the profile of kind `n`, with `auth` as its context.
pub(crate) fn prepared_with(name: &str, n: u8, auth: AuthContext) -> PreparedOperation {
    PreparedOperation::new(
        name,
        EndpointRoute::ssh_and_https(),
        profile(n),
        auth,
        OperationPolicy::new(4, 32, 3).unwrap(),
    )
    .unwrap()
}
pub(crate) fn ssh_key() -> Key {
    Key::ssh("git", "example.invalid", 22)
}
pub(crate) fn exchange_spec() -> ExchangeSpec {
    ExchangeSpec::git(
        ssh_key(),
        Identity::Ambient,
        GitService::UploadPackExchange,
        "/srv/repo.git",
    )
    .unwrap()
}
/// An HTTPS exchange: the scheme whose connections the authentication scope partitions.
pub(crate) fn https_spec() -> ExchangeSpec {
    ExchangeSpec::git(
        Key::https("example.invalid", 443),
        Identity::Https,
        GitService::UploadPackExchange,
        "/srv/repo.git",
    )
    .unwrap()
}
/// Polls a future that must already be ready.
pub(crate) fn now<F: Future>(future: F) -> F::Output {
    let mut future = std::pin::pin!(future);
    match future
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    {
        Poll::Ready(value) => value,
        Poll::Pending => panic!("the future was not ready"),
    }
}
