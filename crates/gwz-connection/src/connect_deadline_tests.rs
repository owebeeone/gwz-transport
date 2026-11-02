//! The pool's network deadline reaches the adapter (Phase 1 State P3-2(e), step R2b part A; contract sections 4.1 and
//! 8), against fakes only. A `ConnectCommand` carries what is left of the connect's network deadline when it is issued
//! (`network_remaining_ms`), the earlier of the pool's connect bound and the opening's allocation deadline; an adapter
//! that bounds its setup by it reports the connect's end by the pool's deadline, whether or not it acts on the host's
//! `cancel_connect`.
use crate::fakes::{
    FakeFactory, FixedSeed, ManualClock, Rig, auth, exchange_spec, now, poll_once, profile,
};
use crate::{
    AuthContext, Clock, Completion, ConnectCommand, EndpointAdapter, EndpointFactory,
    EndpointRoute, EndpointSpec, ExchangeIo, ExchangeSpec, FactoryEnv, FactoryError, HostHandle,
    HostLimits, HostOwner, HostPorts, OpenFailure, Operation, OperationPolicy, PreparedOperation,
    ProofId, ReuseCommand, Tick,
};
use gwz_transport::{
    pool::{ConnectionId, Error},
    protocol::{ErrorCode, Failure},
};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

/// What the adapters of one host were told and reported.
#[derive(Default)]
struct Seen {
    /// Each connect's `network_remaining_ms`, in order.
    spans: Mutex<Vec<Option<u64>>>,
    /// When (the fake clock) each stalled connect ended by its own deadline.
    ended_at: Mutex<Vec<u64>>,
    /// `cancel_connect`s the host sent to a stalled connect, withheld from it.
    withheld: AtomicUsize,
}
/// The fake adapter, with every connect's span recorded. With `stall`, a connect never completes of itself: it ends
/// `Timeout` when its span runs out, and the host's `cancel_connect` is withheld from it.
struct Spanning {
    inner: Box<dyn EndpointAdapter>,
    clock: Arc<ManualClock>,
    stall: bool,
    seen: Arc<Seen>,
    /// The stalled connects and their deadlines (none without a span).
    stalled: Vec<(ConnectionId, Option<u64>)>,
}
impl EndpointAdapter for Spanning {
    fn connect(&mut self, command: ConnectCommand) {
        self.seen
            .spans
            .lock()
            .unwrap()
            .push(command.network_remaining_ms);
        if !self.stall {
            return self.inner.connect(command);
        }
        let now = self.clock.now().get();
        let deadline = command
            .network_remaining_ms
            .map(|span| now.saturating_add(span));
        self.stalled.push((command.connection, deadline));
    }
    fn cancel_connect(&mut self, connection: ConnectionId) {
        if self.stall {
            self.seen.withheld.fetch_add(1, Ordering::SeqCst);
        } else {
            self.inner.cancel_connect(connection);
        }
    }
    fn close(&mut self, connection: ConnectionId) {
        self.inner.close(connection);
    }
    fn start_exchange(
        &mut self,
        connection: ConnectionId,
        spec: &ExchangeSpec,
        auth: &AuthContext,
    ) -> io::Result<Box<dyn ExchangeIo>> {
        self.inner.start_exchange(connection, spec, auth)
    }
    fn authorize_reuse(&mut self, command: ReuseCommand) -> bool {
        self.inner.authorize_reuse(command)
    }
    fn cancel_reuse(&mut self, proof: ProofId) {
        self.inner.cancel_reuse(proof);
    }
    fn poll_completion(&mut self, cx: &mut Context<'_>) -> Poll<Completion> {
        let now = self.clock.now().get();
        let due = self
            .stalled
            .iter()
            .position(|(_, deadline)| deadline.is_some_and(|at| at <= now));
        if let Some(index) = due {
            let (connection, _) = self.stalled.remove(index);
            self.seen.ended_at.lock().unwrap().push(now);
            return Poll::Ready(Completion::Connected {
                connection,
                result: Err(Failure {
                    code: ErrorCode::Timeout,
                    ..Default::default()
                }),
            });
        }
        // The earliest deadline wakes the host's driver, as a real adapter's own timer does.
        if let Some(at) = self.stalled.iter().filter_map(|(_, at)| *at).min() {
            let _ = self.clock.sleep_until(Tick::new(at)).as_mut().poll(cx);
        }
        self.inner.poll_completion(cx)
    }
    fn poll_dispose(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.inner.poll_dispose(cx)
    }
}
struct SpanningFactory {
    inner: FakeFactory,
    clock: Arc<ManualClock>,
    stall: bool,
    seen: Arc<Seen>,
}
impl EndpointFactory for SpanningFactory {
    fn create(
        &self,
        spec: &EndpointSpec,
        env: &FactoryEnv<'_>,
    ) -> Result<Box<dyn EndpointAdapter>, FactoryError> {
        Ok(Box::new(Spanning {
            inner: self.inner.create(spec, env)?,
            clock: self.clock.clone(),
            stall: self.stall,
            seen: self.seen.clone(),
            stalled: Vec::new(),
        }))
    }
}

fn host(rig: &Rig, stall: bool) -> (Arc<Seen>, HostOwner, HostHandle) {
    let seen = Arc::new(Seen::default());
    let factory = SpanningFactory {
        inner: FakeFactory(rig.world.clone()),
        clock: rig.clock.clone(),
        stall,
        seen: seen.clone(),
    };
    let ports = HostPorts::new(
        rig.clock.clone(),
        Arc::new(FixedSeed(1)),
        rig.progress.clone(),
        Arc::new(factory),
    );
    let (owner, handle) = crate::build(rig.config(HostLimits::default()), ports).unwrap();
    (seen, owner, handle)
}
fn registered(handle: &HostHandle, per_host: usize) -> Operation {
    let prepared = PreparedOperation::new(
        "op-a",
        EndpointRoute::ssh_and_https(),
        profile(1),
        auth(0, "none"),
        OperationPolicy::new(4, per_host, 3).unwrap(),
    )
    .unwrap();
    now(handle.admit(prepared).unwrap().acquire())
        .unwrap()
        .register()
}

#[test]
fn p3_2e_a_fresh_openings_connect_carries_the_pools_connect_bound() {
    let rig = Rig::new();
    let (seen, _owner, handle) = host(&rig, false);
    let op = registered(&handle, 32);
    let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    // The pool's connect bound (30 s) and the opening's allocation bound (30 s) are equal: either is the span.
    assert_eq!(*seen.spans.lock().unwrap(), [Some(30_000)]);
    now(exchange.finish());
    now(op.finish());
}

#[test]
fn p3_2e_an_opening_that_asks_again_carries_what_its_allocation_has_left() {
    // An opening that asks the pool again (its lease was lost) asks within the deadline it was given at submit, which the
    // pool takes as the request's absolute deadline: its new connect, issued 20 s into a 30 s allocation, is given the
    // 10 s that are left, not the pool's whole 30 s connect bound. (A first request that waits for capacity is not
    // capped so: the pool's allocation bound ends when its connect begins, `gwz_transport::pool` clock.rs.)
    let rig = Rig::new();
    rig.world.asynchronous.store(true, Ordering::SeqCst);
    let (seen, _owner, handle) = host(&rig, false);
    let op = registered(&handle, 32);
    let mut wait = op.open(exchange_spec()).unwrap().wait();
    rig.progress.run();
    rig.clock.advance(20_000);
    rig.world.start_lost.store(true, Ordering::SeqCst);
    rig.world.complete_connects();
    rig.progress.run();
    assert!(
        !wait.is_ready(),
        "the lease was lost and the opening asked again"
    );
    assert_eq!(*seen.spans.lock().unwrap(), [Some(30_000), Some(10_000)]);
    rig.world.complete_connects();
    rig.progress.run();
    let Poll::Ready(Ok(exchange)) = poll_once(&mut wait) else {
        panic!("the opening was not served by its second connect");
    };
    rig.world.asynchronous.store(false, Ordering::SeqCst);
    now(exchange.finish());
    now(op.finish());
}

#[test]
fn p3_2e_an_adapter_honouring_the_span_ends_a_stalled_connect_by_the_pools_deadline() {
    let rig = Rig::new();
    let (seen, _owner, handle) = host(&rig, true);
    let op = registered(&handle, 32);
    let mut wait = op.open(exchange_spec()).unwrap().wait();
    rig.progress.run();
    assert_eq!(
        rig.world.connects(),
        0,
        "the connect is held by the stalling adapter"
    );
    rig.clock.advance(29_999);
    rig.progress.run();
    assert!(!wait.is_ready(), "still within the deadline");
    assert!(seen.ended_at.lock().unwrap().is_empty());
    rig.clock.advance(1);
    rig.progress.run();
    // The pool fails the opening at its deadline, and the adapter's own end of the connect has arrived by then: the
    // host's cancel was withheld, so nothing but the span ended it.
    assert!(matches!(
        poll_once(&mut wait),
        Poll::Ready(Err(OpenFailure::Pool(Error::ConnectTimeout)))
    ));
    assert_eq!(*seen.ended_at.lock().unwrap(), [30_000]);
    assert_eq!(seen.withheld.load(Ordering::SeqCst), 1);
    drop(wait);
    now(op.finish());
}
