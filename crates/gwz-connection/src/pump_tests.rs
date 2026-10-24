//! The host's driver and the Git exchange spec, against fakes only (step 1.3b of
//! dev-docs/GwzTransportReleaseConvergencePlan.md; contract sections 4.1, 6, 8, 9 and 11). The fake adapter here
//! completes its connects (and confirms its exchanges' ends) only when the test says, and then wakes the waker the
//! host gave it, as a real adapter's worker does; the stepped executor runs the driver only inside `progress.run()`,
//! so a row sees exactly what the driver did and that no application poll did it.
use crate::fakes::{Rig, exchange_spec, now, prepared, ssh_key};
use crate::{
    AdmissionError, AdmissionPermit, ExchangeSpec, GitService, HostPhase, InvalidInput,
    OpenFailure, Operation, TerminalReason,
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
