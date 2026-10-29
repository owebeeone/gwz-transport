//! The fixture: a client composition (a `Rig`'s owner), a core-side `HostHandle`, a `Receiver`, and a fake carrier
//! between them whose every message is logged.
use super::super::{
    AllowedSet, ClientSession, CoreBinding, Invocation, Invocations, Lane, LaneEnded, LaneSend,
    NamedKey, Receiver, RemoteConfig, ServiceClass, Shared, backend, serve_bindings,
};
use crate::{
    Clock, EndpointProfile, EndpointRoute, HostHandle, HostNamespace, HostOwner, OperationPolicy,
    PreparedOperation, Progress,
    fakes::{ManualClock, ManualProgress, Rig, auth, prepared_with},
};
use gwz_transport::{mux::Attachment, protocol::MessageKind};
use std::{
    collections::{BTreeMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

/// A waker that counts its wakes: a row's stand-in for one executor task, so a row can tell a future that is woken by a
/// state change from one that wakes itself.
pub(crate) struct Count(AtomicUsize);
impl Count {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self(AtomicUsize::new(0)))
    }
    pub(crate) fn wakes(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }
    /// Polls `future` once with this task's waker.
    pub(crate) fn poll<F: Future + Unpin>(self: &Arc<Self>, future: &mut F) -> Poll<F::Output> {
        let waker = Waker::from(self.clone());
        Pin::new(future).poll(&mut Context::from_waker(&waker))
    }
}
impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

/// Which way a logged message went.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Way {
    ToClient,
    ToCore,
}

struct Direction {
    queue: VecDeque<Attachment>,
    waker: Option<Waker>,
}
struct Pipe {
    to_client: Direction,
    to_core: Direction,
    /// How many messages each direction holds before `send` answers `Full`.
    capacity: usize,
    ended: bool,
    log: Vec<(Way, Attachment)>,
    /// The sessions each end told the lane were over, as `(way of the end's sends, session)`.
    retired: Vec<(Way, String)>,
    /// The sessions the carrier faulted at each end, as `(way of the end's sends, session)`.
    faults: Vec<(Way, String)>,
}
/// The two ends of an in-memory carrier.
pub(crate) struct Carrier {
    pipe: Arc<Mutex<Pipe>>,
}
impl Carrier {
    pub(crate) fn new() -> Self {
        let empty = || Direction {
            queue: VecDeque::new(),
            waker: None,
        };
        Self {
            pipe: Arc::new(Mutex::new(Pipe {
                to_client: empty(),
                to_core: empty(),
                capacity: 4096,
                ended: false,
                log: Vec::new(),
                retired: Vec::new(),
                faults: Vec::new(),
            })),
        }
    }
    pub(crate) fn set_capacity(&self, capacity: usize) {
        self.pipe.lock().unwrap().capacity = capacity;
    }
    /// The carrier ends under both sides.
    pub(crate) fn end(&self) {
        let wakers: Vec<Waker> = {
            let mut pipe = self.pipe.lock().unwrap();
            pipe.ended = true;
            [pipe.to_client.waker.take(), pipe.to_core.waker.take()]
                .into_iter()
                .flatten()
                .collect()
        };
        for waker in wakers {
            waker.wake();
        }
    }
    /// The carrier faults the binding `session` at the core's end (a peer frame broke its rules or bounds).
    pub(crate) fn fault_at_core(&self, session: &str) {
        self.pipe
            .lock()
            .unwrap()
            .faults
            .push((Way::ToClient, session.to_owned()));
    }
    /// The same at the client's end.
    pub(crate) fn fault_at_client(&self, session: &str) {
        self.pipe
            .lock()
            .unwrap()
            .faults
            .push((Way::ToCore, session.to_owned()));
    }
    /// The sessions the end that sends toward `way` told its lane were over, in order.
    pub(crate) fn retired(&self, way: Way) -> Vec<String> {
        self.pipe
            .lock()
            .unwrap()
            .retired
            .iter()
            .filter(|(w, _)| *w == way)
            .map(|(_, session)| session.clone())
            .collect()
    }
    pub(crate) fn log(&self) -> Vec<(Way, Attachment)> {
        self.pipe.lock().unwrap().log.clone()
    }
    /// Messages sent in `way` of `kind`, in order.
    pub(crate) fn sent(&self, way: Way, kind: MessageKind) -> Vec<Attachment> {
        self.log()
            .into_iter()
            .filter(|(w, a)| *w == way && a.1.kind == kind)
            .map(|(_, a)| a)
            .collect()
    }
    /// Puts a message on the wire toward `way` as the peer would (a hostile or broken peer's frame).
    pub(crate) fn inject(&self, way: Way, message: Attachment) {
        let mut pipe = self.pipe.lock().unwrap();
        match way {
            Way::ToClient => pipe.to_client.queue.push_back(message),
            Way::ToCore => pipe.to_core.queue.push_back(message),
        }
    }
    /// Takes everything queued toward `way` without a reader: what a raw peer would read.
    pub(crate) fn take(&self, way: Way) -> Vec<Attachment> {
        let mut pipe = self.pipe.lock().unwrap();
        let direction = match way {
            Way::ToClient => &mut pipe.to_client,
            Way::ToCore => &mut pipe.to_core,
        };
        direction.queue.drain(..).collect()
    }
    pub(crate) fn queued(&self, way: Way) -> usize {
        let pipe = self.pipe.lock().unwrap();
        match way {
            Way::ToClient => pipe.to_client.queue.len(),
            Way::ToCore => pipe.to_core.queue.len(),
        }
    }
}

/// One end: `send_way` is where its sends go, and it receives from the other direction.
struct End {
    pipe: Arc<Mutex<Pipe>>,
    send_way: Way,
}
impl End {
    fn send(&self, message: Attachment) -> Result<(), LaneSend> {
        // The fake moves envelopes without encoding them, so it checks what a real carrier would: every message
        // either end builds is one gwz-transport's codec admits (the `bind_rejected` codes of 2.0 section 3.4 among them).
        assert!(
            gwz_transport::codec::admit(&message.1).is_ok(),
            "a message the transport's codec refuses: {message:?}"
        );
        let mut pipe = self.pipe.lock().unwrap();
        if pipe.ended {
            return Err(LaneSend::Closed(Box::new(message)));
        }
        let capacity = pipe.capacity;
        let way = self.send_way;
        let direction = match way {
            Way::ToClient => &mut pipe.to_client,
            Way::ToCore => &mut pipe.to_core,
        };
        if direction.queue.len() >= capacity {
            return Err(LaneSend::Full(Box::new(message)));
        }
        direction.queue.push_back(message.clone());
        let waker = direction.waker.take();
        pipe.log.push((way, message));
        drop(pipe);
        if let Some(waker) = waker {
            waker.wake();
        }
        Ok(())
    }
    fn recv(&self) -> Result<Option<Attachment>, LaneEnded> {
        let mut pipe = self.pipe.lock().unwrap();
        let direction = match self.send_way {
            Way::ToClient => &mut pipe.to_core,
            Way::ToCore => &mut pipe.to_client,
        };
        if let Some(message) = direction.queue.pop_front() {
            return Ok(Some(message));
        }
        if pipe.ended { Err(LaneEnded) } else { Ok(None) }
    }
    fn retire(&self, session: &str) {
        let mut pipe = self.pipe.lock().unwrap();
        pipe.retired.push((self.send_way, session.to_owned()));
    }
    fn faulted(&self, session: &str) -> bool {
        let pipe = self.pipe.lock().unwrap();
        pipe.faults
            .iter()
            .any(|(way, found)| *way == self.send_way && found == session)
    }
    fn register(&self, waker: &Waker) {
        let mut pipe = self.pipe.lock().unwrap();
        let direction = match self.send_way {
            Way::ToClient => &mut pipe.to_core,
            Way::ToCore => &mut pipe.to_client,
        };
        direction.waker = Some(waker.clone());
    }
}

pub(crate) struct CoreEnd {
    end: End,
    clock: Arc<ManualClock>,
    progress: Arc<ManualProgress>,
    /// The live calls and each one's `endpoint_path_base`.
    calls: Mutex<BTreeMap<String, Option<String>>>,
}
impl CoreEnd {
    pub(crate) fn call(&self, request: &str, path_base: Option<&str>) {
        self.calls
            .lock()
            .unwrap()
            .insert(request.to_owned(), path_base.map(str::to_owned));
    }
}
impl Lane for CoreEnd {
    fn retire(&self, session: &str) {
        self.end.retire(session);
    }
    fn faulted(&self, session: &str) -> bool {
        self.end.faulted(session)
    }
    fn send(&self, message: Attachment) -> Result<(), LaneSend> {
        self.end.send(message)
    }
    fn recv(&self) -> Result<Option<Attachment>, LaneEnded> {
        self.end.recv()
    }
    fn register(&self, waker: &Waker) {
        self.end.register(waker);
    }
}
impl CoreBinding for CoreEnd {
    fn namespace(&self) -> HostNamespace {
        HostNamespace::new(0xc0)
    }
    fn clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }
    fn progress(&self) -> Arc<dyn Progress> {
        self.progress.clone()
    }
    fn call_is_live(&self, request_id: &str) -> bool {
        self.calls.lock().unwrap().contains_key(request_id)
    }
    fn endpoint_path_base(&self, request_id: &str) -> Option<String> {
        self.calls
            .lock()
            .unwrap()
            .get(request_id)
            .cloned()
            .flatten()
    }
}
pub(crate) struct ClientEnd {
    end: End,
    clock: Arc<ManualClock>,
    progress: Arc<ManualProgress>,
}
impl Lane for ClientEnd {
    fn retire(&self, session: &str) {
        self.end.retire(session);
    }
    fn faulted(&self, session: &str) -> bool {
        self.end.faulted(session)
    }
    fn send(&self, message: Attachment) -> Result<(), LaneSend> {
        self.end.send(message)
    }
    fn recv(&self) -> Result<Option<Attachment>, LaneEnded> {
        self.end.recv()
    }
    fn register(&self, waker: &Waker) {
        self.end.register(waker);
    }
}
impl ClientSession for ClientEnd {
    fn clock(&self) -> Arc<dyn Clock> {
        self.clock.clone()
    }
    fn progress(&self) -> Arc<dyn Progress> {
        self.progress.clone()
    }
}

/// The calls the client sent: each taken once.
#[derive(Default)]
pub(crate) struct Calls {
    map: Mutex<BTreeMap<String, Invocation>>,
    /// A source that yields a call's invocation every time it is asked: the receiver's own record of bound calls is
    /// then all that keeps a second `bind` out.
    pub(crate) persistent: std::sync::atomic::AtomicBool,
}
impl Invocations for Calls {
    fn take(&self, request_id: &str) -> Option<Invocation> {
        let mut map = self.map.lock().unwrap();
        if self.persistent.load(std::sync::atomic::Ordering::SeqCst) {
            map.get(request_id).cloned()
        } else {
            map.remove(request_id)
        }
    }
}

pub(crate) struct World {
    pub(crate) rig: Rig,
    pub(crate) owner: std::cell::RefCell<Option<HostOwner>>,
    pub(crate) host: HostHandle,
    pub(crate) carrier: Carrier,
    pub(crate) core: Arc<CoreEnd>,
    pub(crate) handle: HostHandle,
    /// The core end's state, for the rows that watch it (`pump`, the discard count, the binding table).
    pub(crate) remote: Arc<Shared>,
    pub(crate) receiver: Receiver,
    /// The executors of the two carrier ends: not the owner's, so a row steps each by name.
    pub(crate) core_progress: Arc<ManualProgress>,
    pub(crate) client_progress: Arc<ManualProgress>,
    pub(crate) calls: Arc<Calls>,
}
impl World {
    pub(crate) fn new() -> Self {
        Self::with(RemoteConfig::default())
    }
    pub(crate) fn with(config: RemoteConfig) -> Self {
        Self::with_host(config, crate::HostLimits::default())
    }
    pub(crate) fn with_host(config: RemoteConfig, limits: crate::HostLimits) -> Self {
        let rig = Rig::new();
        let (owner, host) = crate::build(rig.config(limits), rig.ports(7)).unwrap();
        let carrier = Carrier::new();
        let (core_progress, client_progress) = (ManualProgress::new(), ManualProgress::new());
        let core = Arc::new(CoreEnd {
            end: End {
                pipe: carrier.pipe.clone(),
                send_way: Way::ToClient,
            },
            clock: rig.clock.clone(),
            progress: core_progress.clone(),
            calls: Mutex::new(BTreeMap::new()),
        });
        let client = Arc::new(ClientEnd {
            end: End {
                pipe: carrier.pipe.clone(),
                send_way: Way::ToCore,
            },
            clock: rig.clock.clone(),
            progress: client_progress.clone(),
        });
        let calls = Arc::new(Calls::default());
        let (remote, handle) = backend(core.clone(), config);
        let receiver = serve_bindings(&host, client, calls.clone());
        Self {
            rig,
            owner: std::cell::RefCell::new(Some(owner)),
            host,
            carrier,
            core,
            handle,
            remote,
            receiver,
            core_progress,
            client_progress,
            calls,
        }
    }

    /// The client's host closes: its owner is gone and every later admission is refused.
    pub(crate) fn close_client_host(&self) {
        if let Some(owner) = self.owner.borrow_mut().take() {
            let mut close = Box::pin(owner.close());
            self.rig.progress.run();
            let _ = crate::fakes::poll_once(&mut close);
            self.rig.progress.run();
        }
    }

    /// A live call the client sent as `request`: its invocation (fetch class, `allow` as the fetch allowance) and the
    /// core's registry entry.
    pub(crate) fn call(&self, request: &str, class: ServiceClass, fetch: &[&str], push: &[&str]) {
        let mut invocation = Invocation::new(prepared(request, 1), class);
        invocation.fetch = AllowedSet::parse(fetch).unwrap();
        invocation.push = AllowedSet::parse(push).unwrap();
        self.call_with(request, invocation, None);
    }
    pub(crate) fn call_with(&self, request: &str, invocation: Invocation, path_base: Option<&str>) {
        self.core.call(request, path_base);
        self.calls
            .map
            .lock()
            .unwrap()
            .insert(request.to_owned(), invocation);
    }
    /// Pumps only the client end and the owner's executor: for rows where a raw peer plays the core.
    pub(crate) fn settle_client(&self) {
        for _ in 0..10 {
            self.receiver.pump();
            self.rig.progress.run();
        }
    }
    /// Pumps both ends and the owner's executor until nothing more moves.
    pub(crate) fn settle(&self) {
        for _ in 0..40 {
            let before = self.carrier.log().len();
            self.remote.pump();
            self.rig.progress.run();
            self.receiver.pump();
            self.rig.progress.run();
            self.remote.pump();
            if self.carrier.log().len() == before
                && self.carrier.queued(Way::ToClient) == 0
                && self.carrier.queued(Way::ToCore) == 0
            {
                break;
            }
        }
    }
}

pub(crate) fn prepared(request: &str, n: u8) -> PreparedOperation {
    prepared_with(request, n, auth(0, "client"))
}
#[allow(dead_code, reason = "named for rows that restrict the route")]
pub(crate) fn ssh_only() -> PreparedOperation {
    use gwz_transport::protocol::{AuthPolicy, Scheme};
    PreparedOperation::new(
        "ssh-only",
        EndpointRoute::new(
            vec![Scheme::Ssh],
            vec![AuthPolicy::SshAmbient, AuthPolicy::SshExplicit],
        )
        .unwrap(),
        EndpointProfile::new(vec![1; 8], [1; 32]).unwrap(),
        auth(0, "client"),
        OperationPolicy::new(4, 32, 3).unwrap(),
    )
    .unwrap()
}
#[allow(dead_code, reason = "named for rows that name a key")]
pub(crate) fn key(path: &str) -> NamedKey {
    NamedKey {
        key_path: path.to_owned(),
        proof: format!("proof-{path}"),
    }
}
