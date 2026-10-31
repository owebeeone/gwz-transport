//! The host's driver and the Git exchange spec, against fakes only (step 1.3b of
//! dev-docs/GwzTransportReleaseConvergencePlan.md; contract sections 4.1, 6, 8, 9 and 11). The fake adapter here
//! completes its connects (and confirms its exchanges' ends) only when the test says, and then wakes the waker the
//! host gave it, as a real adapter's worker does; the stepped executor runs the driver only inside `progress.run()`,
//! so a row sees exactly what the driver did and that no application poll did it.
use crate::fakes::{Rig, auth, exchange_spec, now, poll_once, prepared, profile, ssh_key};
use crate::{
    AdmissionError, AdmissionPermit, CancelState, EndpointRoute, ExchangeSpec, GitService,
    HostPhase, InvalidInput, OpenFailure, Operation, OperationPolicy, PreparedOperation,
    TerminalReason,
};
use gwz_transport::{pool::Identity, protocol::Disposition};
use std::{
    pin::pin,
    sync::atomic::Ordering,
    task::{Context, Poll, Waker},
};

fn registered(handle: &crate::HostHandle, name: &str) -> Operation {
    let permit: AdmissionPermit = now(handle.admit(prepared(name, 1)).unwrap().acquire()).unwrap();
    permit.register()
}
fn asynchronous_rig() -> Rig {
    let rig = Rig::new();
    rig.world.asynchronous.store(true, Ordering::SeqCst);
    rig
}

#[test]
fn an_opening_completes_with_no_caller_poll() {
    let rig = asynchronous_rig();
    let (_owner, handle) = rig.host(1);
    let op = registered(&handle, "op-a");
    let wait = op.open(exchange_spec()).unwrap().wait();
    // Submitted: the connect is under way and nothing polled the opening.
    assert_eq!(rig.world.connects(), 1);
    assert!(!wait.is_ready());
    // The adapter's worker finishes the connect and wakes the host; the driver alone leases and starts the exchange.
    rig.world.complete_connects();
    rig.progress.run();
    assert!(
        wait.is_ready(),
        "the driver started the exchange without the opening being polled"
    );
    assert_eq!(handle.snapshot().connections.leased, 1);
    assert_eq!(rig.world.exchanges.load(Ordering::SeqCst), 1);
    // Its first poll takes it.
    let exchange = now(wait).unwrap();
    rig.world.asynchronous.store(false, Ordering::SeqCst);
    now(exchange.finish());
    now(op.finish());
}

#[test]
fn an_exchanges_end_is_settled_by_the_driver_not_by_its_caller() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let op = registered(&handle, "op-a");
    let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    rig.world.hold_finish.store(true, Ordering::SeqCst);
    let mut finish = exchange.finish();
    // The adapter has not confirmed the protocol's end: the connection is still leased, not idle.
    assert!(matches!(
        pin!(&mut finish).poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    assert_eq!(handle.snapshot().connections.leased, 1);
    rig.world.confirm_finishes();
    rig.progress.run();
    assert_eq!(handle.snapshot().connections.idle, 1);
    assert_eq!(now(finish).disposition, Disposition::Reusable);
    now(op.finish());
}

#[test]
fn a_dead_idle_connection_is_detected_and_discarded() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let a = registered(&handle, "op-a");
    now(now(a.open(exchange_spec()).unwrap().wait())
        .unwrap()
        .finish());
    now(a.finish());
    assert_eq!(handle.snapshot().connections.idle, 1);
    // The peer closes the idle connection: the adapter reports it, and the driver alone takes it out of the pool.
    rig.world.lose_connections();
    rig.progress.run();
    assert_eq!(handle.snapshot().connections.total(), 0);
    assert_eq!(
        rig.world.closes(),
        0,
        "a lost connection is not closed again"
    );
    let b = registered(&handle, "op-b");
    now(now(b.open(exchange_spec()).unwrap().wait())
        .unwrap()
        .finish());
    assert_eq!(
        rig.world.connects(),
        2,
        "the next operation got a fresh connection"
    );
    now(b.finish());
}

#[test]
fn cancel_during_connect_ends_the_opening_and_frees_the_connect() {
    let rig = asynchronous_rig();
    let (_owner, handle) = rig.host(1);
    let op = registered(&handle, "op-a");
    let wait = op.open(exchange_spec()).unwrap().wait();
    assert_eq!(handle.snapshot().connections.opening, 1);
    op.cancel_handle().cancel();
    rig.progress.run();
    assert_eq!(rig.world.cancelled_connects.load(Ordering::SeqCst), 1);
    assert_eq!(handle.snapshot().connections.total(), 0);
    assert!(matches!(now(wait), Err(OpenFailure::Cancelled)));
    // A late success of the cancelled connect changes nothing.
    rig.world.complete_connects();
    rig.progress.run();
    assert_eq!(handle.snapshot().connections.total(), 0);
    assert!(now(op.finish()).cancelled);
}

#[test]
fn owner_close_stops_the_driver_and_in_flight_opens_end_with_a_named_error() {
    let rig = asynchronous_rig();
    let (owner, handle) = rig.host(1);
    let op = registered(&handle, "op-a");
    let wait = op.open(exchange_spec()).unwrap().wait();
    rig.progress.run();
    assert_eq!(rig.progress.pending(), 1, "the driver waits for work");
    let close = owner.close();
    // Ended by the close, before anything polled the opening.
    assert!(wait.is_ready());
    rig.progress.run();
    assert_eq!(
        rig.progress.pending(),
        0,
        "the driver and the disposal have ended"
    );
    assert!(matches!(
        now(wait),
        Err(OpenFailure::Closing(TerminalReason::HostClosing))
    ));
    assert!(matches!(now(close), crate::CleanupStatus::Complete));
    drop(op);
}

#[test]
fn the_driver_is_spawned_once_and_only_when_an_instance_attaches() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    assert_eq!(rig.progress.spawned(), 0);
    let a = registered(&handle, "op-a");
    let b = registered(&handle, "op-b");
    rig.attach(&handle);
    assert_eq!(rig.progress.spawned(), 1);
    drop((a, b));
}

#[test]
fn a_driver_its_executor_drops_faults_the_host() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let op = registered(&handle, "op-a");
    rig.progress.drop_all();
    assert_ne!(handle.snapshot().phase, HostPhase::Open);
    // Its bindings are cancelled, and new admissions name the fault.
    assert!(matches!(
        op.open(exchange_spec()),
        Err(crate::OpenError::Cancelled)
    ));
    assert!(matches!(
        handle.admit(prepared("op-b", 1)),
        Err(AdmissionError::Closing(TerminalReason::HostFault))
    ));
}

#[test]
fn an_executor_that_refuses_the_driver_faults_the_host_at_attach() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    rig.progress.set_available(false);
    let acquired = now(handle.admit(prepared("op-a", 1)).unwrap().acquire());
    assert!(matches!(
        acquired,
        Err(AdmissionError::Closing(TerminalReason::HostFault))
    ));
    assert_ne!(handle.snapshot().phase, HostPhase::Open);
}

#[test]
fn the_exchange_spec_carries_a_typed_git_service_and_path_to_the_adapter() {
    let long = "r".repeat(16 * 1024 + 1);
    for path in ["", "-oProxyCommand=x", "/a\0b", long.as_str()] {
        assert!(matches!(
            ExchangeSpec::git(
                ssh_key(),
                Identity::Ambient,
                GitService::UploadPackExchange,
                path
            ),
            Err(InvalidInput::Spec)
        ));
    }
    assert!(GitService::ReceivePackExchange.is_receive_pack());
    assert!(!GitService::UploadPackAdvertisement.is_receive_pack());
    assert!(GitService::ReceivePackAdvertisement.is_advertisement());
    assert!(!GitService::UploadPackExchange.is_advertisement());
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let op = registered(&handle, "op-a");
    let spec = ExchangeSpec::git(
        ssh_key(),
        Identity::Ambient,
        GitService::ReceivePackExchange,
        "/srv/team/repo.git",
    )
    .unwrap();
    assert_eq!(spec.service(), GitService::ReceivePackExchange);
    assert_eq!(spec.path(), "/srv/team/repo.git");
    now(now(op.open(spec).unwrap().wait()).unwrap().finish());
    assert_eq!(
        *rig.world.last_spec.lock().unwrap(),
        Some((GitService::ReceivePackExchange, "/srv/team/repo.git".into()))
    );
    now(op.finish());
}

// Phase 1 remediation R1 (dev-docs/GwzPhase1-RemPlan.md, Code P2-1 = State P2-1): a caller-driven pump (a submission, a
// settlement, a release, an abandonment, a cancellation) can create or move a pool deadline, so it re-arms the driver;
// otherwise a host that is quiet beyond the application's own calls never enforces the bound. Each row parks the driver
// first (`progress.run()` while the deadline does not yet exist) and then moves the logical clock to the bound exactly.

/// The pool's allocation bound, the one an opening waiting for capacity or for a proof is given.
fn allocation_ms() -> u64 {
    gwz_transport::pool::Config::default().allocation_timeout_ms
}
/// The pool's idle timeout.
const IDLE_MS: u64 = 60_000;

/// An operation limited to one connection per host, so a second opening waits for the first one's lease.
fn registered_one_per_host(handle: &crate::HostHandle, name: &str) -> Operation {
    let prepared = PreparedOperation::new(
        name,
        EndpointRoute::ssh_and_https(),
        profile(1),
        auth(0, "none"),
        OperationPolicy::new(4, 1, 3).unwrap(),
    )
    .unwrap();
    let permit: AdmissionPermit = now(handle.admit(prepared).unwrap().acquire()).unwrap();
    permit.register()
}

#[test]
fn a_waiting_opening_ends_at_its_allocation_bound_on_a_driver_that_parked_first() {
    // PROBE-ALLOC. E1 holds the one connection the operation may have; the driver ran and parked with no deadline; E2's
    // submission then creates its allocation deadline, which only the submission's own pump knows.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let op = registered_one_per_host(&handle, "op-a");
    let first = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    rig.progress.run();
    let mut second = op.open(exchange_spec()).unwrap().wait();
    assert!(!second.is_ready(), "no capacity for a second connection");
    rig.clock.advance(allocation_ms() - 1);
    rig.progress.run();
    assert!(!second.is_ready(), "still within the bound");
    rig.clock.advance(1);
    rig.progress.run();
    assert!(second.is_ready(), "the driver decided it at the bound");
    assert!(matches!(
        poll_once(&mut second),
        Poll::Ready(Err(OpenFailure::Pool(
            gwz_transport::pool::Error::AllocationTimeout
        )))
    ));
    drop(second);
    now(first.finish());
    now(op.finish());
}

#[test]
fn an_opening_waiting_for_a_proof_ends_at_its_allocation_bound_on_a_driver_that_parked_first() {
    // PROBE-PROOF. A's connection is idle (the driver sleeps until its 60 s expiry); B leases it and waits for a proof
    // that never ends. B's own deadline (30 s) is earlier than the driver's wake, and only B's submission knows it.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let a = registered(&handle, "op-a");
    now(now(a.open(exchange_spec()).unwrap().wait())
        .unwrap()
        .finish());
    now(a.finish());
    assert_eq!(handle.snapshot().connections.idle, 1);
    rig.progress.run();
    rig.world.hold_reuse.store(true, Ordering::SeqCst);
    let b = registered(&handle, "op-b");
    let mut wait = b.open(exchange_spec()).unwrap().wait();
    assert_eq!(rig.world.reuses().len(), 1, "B waits for its proof");
    rig.clock.advance(allocation_ms() - 1);
    rig.progress.run();
    assert!(!wait.is_ready(), "still within the bound");
    rig.clock.advance(1);
    rig.progress.run();
    assert!(wait.is_ready(), "the driver decided it at the bound");
    assert!(matches!(
        poll_once(&mut wait),
        Poll::Ready(Err(OpenFailure::Pool(
            gwz_transport::pool::Error::AllocationTimeout
        )))
    ));
    drop(wait);
    now(b.finish());
}

#[test]
fn idle_connections_expire_on_a_quiet_host_however_the_driver_ran_before() {
    // The p1c-crate finding (2026-10-31-p1c-crate/raw/finding-idle-expiry-repro.rs): an exchange released to idle by a
    // caller-driven pump after the driver has run does not re-arm it. The driver ran never, once, or twice first.
    for runs in 0..3 {
        let rig = Rig::new();
        let (_owner, handle) = rig.host(1);
        let a = registered(&handle, "op-a");
        let exchange = now(a.open(exchange_spec()).unwrap().wait()).unwrap();
        if runs >= 1 {
            rig.progress.run();
        }
        if runs >= 2 {
            rig.clock.advance(7);
            rig.progress.run();
        }
        now(exchange.finish());
        now(a.finish());
        assert_eq!(handle.snapshot().connections.idle, 1, "runs {runs}");
        rig.clock.advance(IDLE_MS - 1);
        rig.progress.run();
        assert_eq!(
            handle.snapshot().connections.idle,
            1,
            "runs {runs}: not yet"
        );
        rig.clock.advance(1);
        rig.progress.run();
        assert_eq!(handle.snapshot().connections.idle, 0, "runs {runs}");
        assert_eq!(rig.world.closes(), 1, "runs {runs}");
    }
}

#[test]
fn two_overlapping_exchanges_idle_connections_both_expire_on_a_quiet_host() {
    // The same finding with the exchanges overlapping: both connections end up idle, and both must expire.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let (a, b) = (registered(&handle, "op-a"), registered(&handle, "op-b"));
    let first = now(a.open(exchange_spec()).unwrap().wait()).unwrap();
    let second = now(b.open(exchange_spec()).unwrap().wait()).unwrap();
    rig.progress.run();
    now(first.finish());
    now(second.finish());
    now(a.finish());
    now(b.finish());
    assert_eq!(handle.snapshot().connections.idle, 2);
    rig.clock.advance(IDLE_MS);
    rig.progress.run();
    assert_eq!(handle.snapshot().connections.idle, 0);
    assert_eq!(rig.world.closes(), 2);
}

/// The pool's bound on a connection's disposal, after which it asks the adapter to close again.
const CLEANUP_MS: u64 = 5_000;

/// A leased exchange on a host whose driver has run and parked, and whose adapter will not confirm a close: whatever
/// discards the lease starts the pool's cleanup bound, which only a caller-driven pump knows about.
fn parked_with_a_stalled_close() -> (Rig, crate::HostOwner, crate::HostHandle, Operation) {
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    let op = registered(&handle, "op-a");
    rig.progress.run();
    rig.world.hold_close.store(true, Ordering::SeqCst);
    (rig, owner, handle, op)
}
/// The close the pool asks for at the cleanup bound: the adapter is asked to close the connection a second time.
fn assert_closed_again_at_the_cleanup_bound(rig: &Rig) {
    assert_eq!(rig.world.closes(), 1, "the first close is asked at once");
    rig.clock.advance(CLEANUP_MS - 1);
    rig.progress.run();
    assert_eq!(rig.world.closes(), 1, "still within the bound");
    rig.clock.advance(1);
    rig.progress.run();
    assert_eq!(rig.world.closes(), 2, "the driver ran the cleanup bound");
}

#[test]
fn an_aborted_exchanges_cleanup_bound_is_enforced_on_a_driver_that_parked_first() {
    let (rig, _owner, _handle, op) = parked_with_a_stalled_close();
    let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    rig.progress.run();
    exchange.abort();
    assert_closed_again_at_the_cleanup_bound(&rig);
    now(op.finish());
}

#[test]
fn an_abandoned_openings_cleanup_bound_is_enforced_on_a_driver_that_parked_first() {
    // The opening was leased a connection (its slot filled) and went before taking it: the lease is discarded.
    let (rig, _owner, _handle, op) = parked_with_a_stalled_close();
    let wait = op.open(exchange_spec()).unwrap().wait();
    assert!(wait.is_ready());
    rig.progress.run();
    drop(wait);
    assert_closed_again_at_the_cleanup_bound(&rig);
    now(op.finish());
}

#[test]
fn a_cancelled_operations_cleanup_bound_is_enforced_on_a_driver_that_parked_first() {
    let (rig, _owner, _handle, op) = parked_with_a_stalled_close();
    let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    rig.progress.run();
    assert_eq!(op.cancel_handle().cancel(), CancelState::Cancelled);
    assert_closed_again_at_the_cleanup_bound(&rig);
    drop(exchange);
    now(op.finish());
}
