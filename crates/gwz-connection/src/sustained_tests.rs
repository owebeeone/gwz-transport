//! The sustained-use proof of contract section 13.2 (step 1.8 of dev-docs/GwzTransportReleaseConvergencePlan.md), against
//! the fakes and in this crate alone: ten thousand successive operations on one host, with repeated external request
//! identifiers, stale events after finish, abandoned receipts, abandoned operations and cancellations mixed in, and the
//! size of every per-operation table returning to its bound. The clock is the logical one; nothing waits.
//!
//! Phase 1 remediation R1 (Code P3-1) made the workload as diverse as its claims: every operation has a scope of its own
//! (so a scope token that is not pruned grows with the run), the operations use more profiles than the host has
//! instances (so the registry must retire unbound instances to keep admitting), each operation's end is asserted by its
//! own disposition (so a cancellation that reaches another operation under a repeated name shows), and once the work is
//! done the idle connections must EXPIRE on the logical clock, not merely stay within a bound.
use crate::fakes::{Rig, auth, exchange_spec, https_spec, now, poll_once, prepared_with};
use crate::tables::{InstanceTables, Tables};
use crate::{
    AuthContext, AuthScope, BindingId, CancelState, Direction, Discard, Exchange, HostHandle,
    Operation, Registration, Routed,
};
use gwz_transport::protocol::{Disposition, Envelope, MessageKind};
use std::{
    collections::VecDeque,
    sync::{Arc, Weak, atomic::Ordering},
};

/// Operations of the workload that are in flight together: the ring holds this many beside the one being driven, so
/// no more than `RING + 1` exchanges are ever leased at once and the pool never needs more connections than that.
const RING: usize = 3;
/// Operations alive at once at most: the ring, the one being driven, and the one that is dropped mid-connect.
const CONCURRENT: usize = RING + 2;
/// How far the logical clock moves after each operation.
const STEP_MS: u64 = 1_000;
/// Physical connections the workload may hold at most, whatever the number of operations: one per concurrent lease, and
/// the idle connections the operations of the last idle timeout left behind. A fresh scope per operation means an HTTPS
/// connection serves nobody else, so each one waits for its expiry (or the pool's eviction), and each operation leaves
/// at most one.
const PHYSICAL: usize = CONCURRENT + (IDLE_MS / STEP_MS) as usize + 1;
/// External request identifiers the workload repeats: two names for ten thousand operations, so operations alive at once
/// share a name and every name is reused after its holder ended.
const EXTERNAL_IDS: usize = 2;
/// The most retired bindings the workload remembers, to send them stale events.
const REMEMBERED: usize = 64;
/// Profiles every eighth operation draws on, one after another: with the three hot ones, more than the host's sixteen
/// instances, so each new cold profile needs an unbound instance retired.
const COLD_PROFILES: usize = 40;
/// The host's default instance limit, which the workload's profiles exceed.
const INSTANCES: usize = 16;
/// The pool's idle timeout, which the host's driver enforces on the logical clock.
const IDLE_MS: u64 = 60_000;

/// How one operation of the workload ends.
#[derive(Clone, Copy, Debug)]
enum Ending {
    /// The exchange finishes, then the operation.
    Clean,
    /// The operation is cancelled with its exchange open, then both finish.
    Cancelled,
    /// Neither is finished: both are dropped, the exchange first (an abandoned operation).
    Abandoned,
    /// The exchange finishes; the operation's finish future is built and dropped unpolled (an abandoned receipt).
    ReceiptDropped,
    /// The exchange is aborted, then the operation finishes.
    Aborted,
    /// The operation is cancelled and dropped, its exchange dropped after it.
    CancelledAndDropped,
}
const ENDINGS: [Ending; 7] = [
    Ending::Clean,
    Ending::Cancelled,
    Ending::Abandoned,
    Ending::ReceiptDropped,
    Ending::Aborted,
    Ending::CancelledAndDropped,
    Ending::Clean,
];

/// An operation in flight in the ring, with how it will end.
struct InFlight {
    operation: Operation,
    exchange: Exchange,
    ending: Ending,
}

/// The largest each table has been, and what the workload did.
#[derive(Default, Debug)]
struct Report {
    peak: Tables,
    peak_physical: usize,
    stale_sent: usize,
    ended: [usize; 6],
    wall: std::time::Duration,
}

struct Workload {
    rig: Rig,
    handle: HostHandle,
    ring: VecDeque<InFlight>,
    retired: VecDeque<(BindingId, String)>,
    bindings: Vec<Weak<crate::binding::BindingShared>>,
    report: Report,
    _owner: crate::HostOwner,
}

/// A frame as the binding of `session` would carry it: only its session names the binding.
fn frame(session: &str) -> (String, Envelope) {
    (
        "x".into(),
        Envelope {
            version: 2,
            session_id: session.into(),
            stream_id: 5,
            kind: MessageKind::Data,
            ..Default::default()
        },
    )
}

fn higher(peak: Tables, now: Tables) -> Tables {
    let (p, n) = (peak.instance, now.instance);
    Tables {
        bindings: peak.bindings.max(now.bindings),
        claims: peak.claims.max(now.claims),
        queued: peak.queued.max(now.queued),
        active: peak.active.max(now.active),
        charged: peak.charged.max(now.charged),
        instances: peak.instances.max(now.instances),
        instance: InstanceTables {
            openings: p.openings.max(n.openings),
            authorizing: p.authorizing.max(n.authorizing),
            settling: p.settling.max(n.settling),
            connections: p.connections.max(n.connections),
            scope_tokens: p.scope_tokens.max(n.scope_tokens),
            requests: p.requests.max(n.requests),
        },
    }
}

/// Operation `i`'s authentication context: a scope no other operation has, so the instance meets as many scopes as there
/// are operations and must forget each one's identity token once nothing of it is left.
fn fresh_auth(i: usize) -> AuthContext {
    let mut scope = [0xA5; 32];
    scope[..8].copy_from_slice(&(i as u64).to_le_bytes());
    AuthContext::new(64, AuthScope::new(scope), Arc::new("ctx".to_string()))
}
/// Operation `i`'s profile: usually one of three hot ones, which are reused, and every eighth a cold one out of
/// `COLD_PROFILES`, which needs an instance of its own and so a retirement once the host is at its limit.
fn profile_of(i: usize) -> u8 {
    if i.is_multiple_of(8) {
        10 + ((i / 8) % COLD_PROFILES) as u8
    } else {
        1 + (i % 3) as u8
    }
}

impl Workload {
    fn new() -> Self {
        let rig = Rig::new();
        let (owner, handle) = rig.host(1);
        Self {
            rig,
            handle,
            ring: VecDeque::new(),
            retired: VecDeque::new(),
            bindings: Vec::new(),
            report: Report::default(),
            _owner: owner,
        }
    }

    fn tables(&self) -> Tables {
        self.handle.tables().expect("a local host has tables")
    }

    /// Samples every table and the physical connections against their bounds.
    fn sample(&mut self) {
        let tables = self.tables();
        self.report.peak = higher(self.report.peak, tables);
        let counts = self.handle.snapshot().connections;
        self.report.peak_physical = self.report.peak_physical.max(counts.total());
        assert!(
            counts.total() <= PHYSICAL,
            "physical connections exceed the bound: {counts:?}"
        );
        assert!(
            self.rig.world.connected_ids().len() <= counts.total(),
            "the fake holds a connection the pool does not know: {counts:?}"
        );
    }

    /// Delayed events of bindings that already ended, and a foreign session's event under a live binding's id: each is
    /// discarded and none of them creates a record in any table.
    fn stale_events(&mut self, i: usize, live: Option<(BindingId, &str)>) {
        let before = self.tables();
        let carrier = self.handle.carrier();
        if !self.retired.is_empty() {
            let (id, session) = self.retired[(i * 7) % self.retired.len()].clone();
            let late = frame(&session);
            for direction in [Direction::ToEndpoint, Direction::ToDriver] {
                assert_eq!(
                    self.handle.route(carrier, id, direction, &late),
                    Routed::Discarded(Discard::Retired),
                    "a retired binding takes nothing (operation {i})"
                );
                self.report.stale_sent += 1;
            }
        }
        if let (Some((id, _)), Some((_, other))) = (live, self.retired.back()) {
            let late = frame(other);
            assert_eq!(
                self.handle.route(carrier, id, Direction::ToEndpoint, &late),
                Routed::Discarded(Discard::WrongSession),
                "another binding's event is not the live binding's (operation {i})"
            );
            self.report.stale_sent += 1;
        }
        assert_eq!(self.tables(), before, "a stale event made a record");
    }

    fn remember(&mut self, id: BindingId, session: String) {
        self.retired.push_back((id, session));
        if self.retired.len() > REMEMBERED {
            self.retired.pop_front();
        }
    }

    /// Ends one in-flight operation the way it was marked to end.
    fn end(&mut self, flight: InFlight) {
        let InFlight {
            operation,
            exchange,
            ending,
        } = flight;
        let id = match operation.registration() {
            Registration::Registered(id) => id,
            other => panic!("not registered: {other:?}"),
        };
        let session = operation.binding_session();
        self.report.ended[ending as usize] += 1;
        // Every operation ends by its own disposition: an operation's cancellation or abort reaches no other operation,
        // whatever name they share, so a clean exchange is `Reusable` and a cancelled or aborted one `Discarded`.
        let finished = |exchange: Exchange, expected: Disposition| {
            assert_eq!(now(exchange.finish()).disposition, expected, "{ending:?}");
        };
        let ended = |operation: Operation, cancelled: bool| {
            let end = now(operation.finish());
            assert_eq!(end.cancelled, cancelled, "{ending:?}");
            assert_eq!(end.exchanges_opened, 1, "{ending:?}");
        };
        match ending {
            Ending::Clean => {
                finished(exchange, Disposition::Reusable);
                ended(operation, false);
            }
            Ending::Cancelled => {
                assert_eq!(operation.cancel_handle().cancel(), CancelState::Cancelled);
                finished(exchange, Disposition::Discarded);
                ended(operation, true);
            }
            Ending::Abandoned => {
                drop(exchange);
                drop(operation);
            }
            Ending::ReceiptDropped => {
                finished(exchange, Disposition::Reusable);
                drop(operation.finish());
            }
            Ending::Aborted => {
                exchange.abort();
                ended(operation, false);
            }
            Ending::CancelledAndDropped => {
                assert_eq!(operation.cancel_handle().cancel(), CancelState::Cancelled);
                drop(operation);
                drop(exchange);
            }
        }
        self.remember(id, session);
    }

    /// Operation `i`: registered under a repeated external identifier, one exchange open (HTTPS under one of three
    /// scopes, or SSH), then held in the ring or ended at once.
    fn run_one(&mut self, i: usize) {
        let https = i % 4 == 1;
        let name = format!("req-{}", i % EXTERNAL_IDS);
        let permit = now(self
            .handle
            .admit(prepared_with(&name, profile_of(i), fresh_auth(i)))
            .unwrap()
            .acquire())
        .unwrap();
        let operation = permit.register();
        self.bindings.push(operation.binding_weak_for_test());
        let id = match operation.registration() {
            Registration::Registered(id) => id,
            other => panic!("not registered: {other:?}"),
        };
        let session = operation.binding_session();
        self.stale_events(i, Some((id, &session)));
        if i % 10 == 5 {
            self.opening_dropped_mid_connect(i);
        }
        let spec = if https { https_spec() } else { exchange_spec() };
        let mut exchange = now(operation.open(spec).unwrap().wait()).unwrap();
        assert_eq!(exchange.stream().write(b"ping").unwrap(), 4);
        let mut echo = [0u8; 8];
        assert_eq!(exchange.stream().read(&mut echo).unwrap(), 4);
        assert_eq!(&echo[..4], b"ping");
        self.sample();
        let ending = ENDINGS[i % ENDINGS.len()];
        let flight = InFlight {
            operation,
            exchange,
            ending,
        };
        if i.is_multiple_of(3) {
            self.ring.push_back(flight);
            if self.ring.len() > RING {
                let oldest = self.ring.pop_front().unwrap();
                self.end(oldest);
            }
        } else {
            self.end(flight);
        }
        // The logical clock moves a little each time, and the host's driver runs. The cleanup domain's retainer takes
        // the retired instances' disposals.
        self.rig.clock.advance(STEP_MS);
        self.rig.progress.run();
        self.rig.retention.step_all();
        self.sample();
    }

    /// An opening dropped while its connect is still under way, on an HTTPS scope that never has an idle connection to
    /// reuse: its connect is cancelled, its operation finishes, and nothing of either stays.
    fn opening_dropped_mid_connect(&mut self, i: usize) {
        let permit = now(self
            .handle
            .admit(prepared_with("req-mid", 1, auth(200, "mid")))
            .unwrap()
            .acquire())
        .unwrap();
        let operation = permit.register();
        self.bindings.push(operation.binding_weak_for_test());
        self.rig.world.asynchronous.store(true, Ordering::SeqCst);
        let mut wait = operation.open(https_spec()).unwrap().wait();
        assert!(poll_once(&mut wait).is_pending(), "operation {i}");
        self.sample();
        drop(wait);
        self.rig.world.asynchronous.store(false, Ordering::SeqCst);
        self.rig.progress.run();
        now(operation.finish());
    }

    /// Ends everything in flight, as a clean operation does, and lets the idle connections expire (by the clock).
    fn quiesce(&mut self) -> Tables {
        while let Some(mut flight) = self.ring.pop_front() {
            flight.ending = Ending::Clean;
            self.end(flight);
        }
        self.rig.progress.run();
        self.sample();
        // Nothing is left to wait for and no call wakes the host any more: the logical clock passes the idle timeout and
        // the host's driver alone must expire every idle connection.
        self.rig.clock.advance(IDLE_MS + 1);
        self.rig.progress.run();
        self.rig.retention.step_all();
        self.sample();
        self.tables()
    }

    fn run(&mut self, operations: usize) {
        let started = std::time::Instant::now();
        for i in 0..operations {
            self.run_one(i);
        }
        self.report.wall = started.elapsed();
    }
}

/// What a table may hold with nothing in flight and the idle timeout passed: everything is empty. The connections, their
/// setup records and the scope tokens expired with the clock, and every instance the registry retired was disposed of
/// by the cleanup domain.
fn assert_at_bound(tables: Tables, workload: &Workload) {
    let counts = workload.handle.snapshot().connections;
    assert_eq!(counts.total(), 0, "a connection did not expire: {counts:?}");
    assert_eq!(tables.bindings, 0, "bindings: {tables:?}");
    assert_eq!(tables.claims, 0, "claims: {tables:?}");
    assert_eq!(tables.queued, 0, "queued admissions: {tables:?}");
    assert_eq!(tables.active, 0, "active slots: {tables:?}");
    assert_eq!(tables.charged, 0, "retained charge: {tables:?}");
    assert_eq!(
        tables.instance,
        InstanceTables::default(),
        "an instance table did not empty: {tables:?}"
    );
    assert!(tables.instances <= INSTANCES, "{tables:?}");
    let world = &workload.rig.world;
    assert!(
        world.connected_ids().is_empty(),
        "the adapters still hold connections"
    );
    assert_eq!(
        world.closes() + world.cancelled_connects.load(Ordering::SeqCst),
        world.connects(),
        "every connection was closed or its connect cancelled"
    );
    assert_eq!(
        world.disposed(),
        world.created.load(Ordering::SeqCst) - tables.instances,
        "every retired instance's adapter was disposed of"
    );
    assert_eq!(workload.handle.snapshot().retained_cleanup, 0);
    assert!(
        workload
            .bindings
            .iter()
            .all(|binding| binding.upgrade().is_none()),
        "a retired binding is still alive somewhere"
    );
}

#[test]
fn ten_thousand_operations_keep_every_table_and_the_connections_at_their_bounds() {
    const N: usize = 10_000;
    let mut workload = Workload::new();
    workload.run(N);
    let tables = workload.quiesce();
    assert_at_bound(tables, &workload);

    let report = &workload.report;
    // Bounds held throughout, not only at the end.
    assert!(report.peak.bindings <= CONCURRENT, "{:?}", report.peak);
    assert!(report.peak.active <= CONCURRENT, "{:?}", report.peak);
    assert_eq!(
        report.peak.claims, 0,
        "an acquired permit always registered at once"
    );
    assert_eq!(report.peak.queued, 0);
    assert!(report.peak.instance.openings <= 1, "{:?}", report.peak);
    assert!(
        report.peak.instance.requests <= CONCURRENT,
        "{:?}",
        report.peak
    );
    assert!(
        report.peak.instance.connections <= PHYSICAL,
        "{:?}",
        report.peak
    );
    assert!(
        report.peak_physical <= PHYSICAL,
        "{} physical",
        report.peak_physical
    );
    assert_eq!(
        tables.instances, INSTANCES,
        "forty-three profiles through the host's sixteen instances"
    );
    assert!(report.peak.instances <= INSTANCES, "{:?}", report.peak);
    let world = &workload.rig.world;
    assert!(
        world.created.load(Ordering::SeqCst) > INSTANCES + N / 10 / 2,
        "{} instances made: the registry retired unbound ones to admit new profiles",
        world.created.load(Ordering::SeqCst)
    );
    // The workload really did what it says: every ending and the stale events happened, binding ids are not reused
    // (the 256 per-session bound is crossed forty times over with no host-wide registration bound in sight), and the
    // host's one driver task served all of it.
    assert!(
        report.ended.iter().all(|count| *count > 0),
        "{:?}",
        report.ended
    );
    assert!(report.stale_sent > N, "{} stale events", report.stale_sent);
    assert_eq!(workload.rig.progress.spawned(), 1);
    assert_eq!(workload.rig.progress.pending(), 1);
    assert!(workload.rig.world.cancelled_connects.load(Ordering::SeqCst) >= N / 10);
    assert_eq!(workload.bindings.len(), N + N / 10);
    // Connections were reused, not made per operation.
    let connects = workload.rig.world.connects();
    let cancelled = workload.rig.world.cancelled_connects.load(Ordering::SeqCst);
    let exchanges = workload.rig.world.exchanges.load(Ordering::SeqCst);
    assert!(exchanges >= N, "{exchanges} exchanges");
    assert!(
        exchanges - (connects - cancelled) > N / 4,
        "{connects} connects ({cancelled} cancelled) for {exchanges} exchanges"
    );

    eprintln!(
        "sustained: {N} operations, {connects} connects, {exchanges} exchanges, {} closes, {} stale events, peak {:?}, \
         peak physical {}, wall {:?}",
        workload.rig.world.closes(),
        report.stale_sent,
        report.peak,
        report.peak_physical,
        report.wall,
    );
}

/// Every table but the instance count (a short run has met fewer profiles than the host has instances): the ones that
/// must not depend on how long the host has lived.
fn per_operation(tables: Tables) -> Tables {
    Tables {
        instances: 0,
        ..tables
    }
}

#[test]
fn no_host_wide_table_grows_with_the_number_of_operations() {
    // The same workload at N = 100 and N = 10,000 on two hosts: once each is quiet, every table is equal but the idle
    // connections' own records, which stay within the same bound however long the host has lived. A table that grew
    // by one entry per operation (a tombstone set, a retired-binding list, a receipt store) would differ by 9,900.
    let mut small = Workload::new();
    small.run(100);
    let at_100 = small.quiesce();
    let mut large = Workload::new();
    large.run(10_000);
    let at_10_000 = large.quiesce();
    assert_at_bound(at_100, &small);
    assert_at_bound(at_10_000, &large);
    assert_eq!(per_operation(at_10_000), per_operation(at_100));
    assert_eq!(per_operation(at_100), Tables::default());
    assert!(at_100.instances <= at_10_000.instances && at_10_000.instances <= INSTANCES);
    assert!(large.report.peak_physical <= PHYSICAL && small.report.peak_physical <= PHYSICAL);
    assert!(large.report.peak.bindings <= CONCURRENT && small.report.peak.bindings <= CONCURRENT);
    assert_eq!(
        large.report.peak.instance.requests, small.report.peak.instance.requests,
        "the most pool requests at once does not grow with the run"
    );
}
