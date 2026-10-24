//! The host's owner and handle against fakes only (step 1.1 of dev-docs/GwzTransportReleaseConvergencePlan.md;
//! contract sections 4, 4.1, 5, 10, 11 and 13.1): no file, socket, thread or helper is involved.
use crate::fakes::{FakeSafety, Rig, poll_once, spec};
use crate::{
    AttachError, BuildError, CleanupDomain, CleanupDomainOwner, CleanupStatus, CleanupTicket,
    ConfigReason, DomainClose, HostClose, HostClosing, HostHandle, HostLimits, HostOwner,
    HostPhase, TicketStatus,
};
use std::{
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

// The capability shapes of contract section 4, as compile-time facts.
trait AmbiguousIfClone<A> {
    fn some_item() {}
}
impl<T: ?Sized> AmbiguousIfClone<()> for T {}
impl<T: Clone> AmbiguousIfClone<u8> for T {}
fn assert_clone_send_sync<T: Clone + Send + Sync>() {}
fn assert_send<T: Send>() {}

#[test]
fn the_capability_types_have_the_shapes_the_contract_names() {
    assert_clone_send_sync::<HostHandle>();
    assert_clone_send_sync::<CleanupDomain>();
    assert_clone_send_sync::<CleanupTicket>();
    assert_send::<HostOwner>();
    assert_send::<CleanupDomainOwner>();
    assert_send::<HostClose>();
    assert_send::<DomainClose>();
    // Each fails to compile if the type implements `Clone` (the call is then ambiguous).
    let _ = <HostOwner as AmbiguousIfClone<_>>::some_item;
    let _ = <CleanupDomainOwner as AmbiguousIfClone<_>>::some_item;
}

#[test]
fn a_built_host_is_open_and_empty_and_names_its_namespace() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(41);
    let snapshot = handle.snapshot();
    assert_eq!(snapshot.phase, HostPhase::Open);
    assert_eq!(snapshot.namespace.get(), 41);
    assert_eq!(snapshot.endpoint_instances, 0);
    assert_eq!(snapshot.retained_cleanup, 0);
    let (_other, second) = rig.host(42);
    assert_ne!(handle.snapshot().namespace, second.snapshot().namespace);
}

#[test]
fn an_invalid_limit_is_refused_at_build() {
    let rig = Rig::new();
    type Case = (fn(&mut HostLimits), ConfigReason);
    let cases: [Case; 5] = [
        (|l| l.active_slots = 0, ConfigReason::ActiveSlots),
        (
            |l| l.waiting_admissions = 0,
            ConfigReason::WaitingAdmissions,
        ),
        (
            |l| l.endpoint_instances = 0,
            ConfigReason::EndpointInstances,
        ),
        (|l| l.close_bound_ms = 0, ConfigReason::CloseBound),
        (|l| l.close_bound_ms = 86_400_001, ConfigReason::CloseBound),
    ];
    for (change, reason) in cases {
        let mut limits = HostLimits::default();
        change(&mut limits);
        match crate::build(rig.config(limits), rig.ports(1)) {
            Err(BuildError::InvalidConfig(got)) => assert_eq!(got, reason),
            other => panic!("expected InvalidConfig({reason:?}), got {other:?}"),
        }
    }
    assert_eq!(rig.progress.spawned(), 0);
}

#[test]
fn a_progress_that_cannot_run_work_is_refused_at_build() {
    let rig = Rig::new();
    rig.progress.set_available(false);
    assert!(matches!(
        crate::build(rig.config(HostLimits::default()), rig.ports(1)),
        Err(BuildError::UnavailableProgress)
    ));
}

#[test]
fn an_unqualified_process_safety_does_not_stop_a_host_that_has_no_native_route() {
    // The fail-closed rule is the native adapter's (contract 7.3, implemented in step 1.3): the host builds, and the
    // factory is told the state with every adapter it makes.
    let rig = Rig::with_safety(FakeSafety::unavailable());
    let (_owner, handle) = rig.host(1);
    rig.attach(&handle);
    let seen = rig.world.safeties.lock().unwrap();
    assert!(matches!(seen[0].1, crate::SafetyState::Unavailable(_)));
}

#[test]
fn close_seals_when_it_is_built_not_when_it_is_polled() {
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    let close = owner.close();
    assert_eq!(handle.snapshot().phase, HostPhase::Closed);
    assert_eq!(handle.check_open(), Err(HostClosing));
    // Nothing to dispose, so nothing needed the executor.
    assert_eq!(rig.progress.spawned(), 0);
    let mut close = close;
    assert!(matches!(
        poll_once(&mut close),
        Poll::Ready(CleanupStatus::Complete)
    ));
}

#[test]
fn close_with_instances_seals_at_once_and_completes_within_the_bound() {
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    rig.attach(&handle);
    rig.attach(&handle);
    assert_eq!(handle.snapshot().endpoint_instances, 2);
    let mut close = owner.close();
    // Sealed before anything is polled; the disposal waits for the executor, not for the caller.
    assert_eq!(handle.snapshot().phase, HostPhase::Closing);
    assert_eq!(handle.snapshot().endpoint_instances, 0);
    assert_eq!(rig.world.disposed(), 0);
    assert!(matches!(poll_once(&mut close), Poll::Pending));
    rig.progress.run();
    assert!(matches!(
        poll_once(&mut close),
        Poll::Ready(CleanupStatus::Complete)
    ));
    assert_eq!(rig.world.disposed(), 2);
    assert_eq!(handle.snapshot().phase, HostPhase::Closed);
}

#[test]
fn an_unpolled_close_still_disposes_because_the_executor_owns_the_work() {
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    rig.attach(&handle);
    drop(owner.close());
    rig.progress.run();
    assert_eq!(rig.world.disposed(), 1);
    assert_eq!(handle.snapshot().phase, HostPhase::Closed);
}

#[test]
fn dropping_the_owner_seals_and_disposes_like_close() {
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    rig.attach(&handle);
    drop(owner);
    assert_eq!(handle.snapshot().phase, HostPhase::Closing);
    rig.progress.run();
    assert_eq!(rig.world.disposed(), 1);
    assert_eq!(handle.snapshot().phase, HostPhase::Closed);
}

#[test]
fn dropping_every_handle_does_not_close_the_host_while_the_owner_lives() {
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    rig.attach(&handle);
    let clone = handle.clone();
    drop(handle);
    drop(clone);
    rig.progress.run();
    assert_eq!(rig.world.disposed(), 0);
    assert_eq!(rig.world.dropped(), 0);
    let mut close = owner.close();
    rig.progress.run();
    assert!(matches!(
        poll_once(&mut close),
        Poll::Ready(CleanupStatus::Complete)
    ));
    assert_eq!(rig.world.disposed(), 1);
}

#[test]
fn a_handle_that_outlives_the_owner_fails_with_a_named_error_and_does_not_wait() {
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    let survivor = handle.clone();
    assert_eq!(survivor.check_open(), Ok(()));
    let mut close = owner.close();
    rig.progress.run();
    assert!(matches!(poll_once(&mut close), Poll::Ready(_)));
    assert_eq!(survivor.check_open(), Err(HostClosing));
    assert!(matches!(
        survivor.attach_instance(spec(1)),
        Err(AttachError::Closing(HostClosing))
    ));
    assert!(HostClosing.to_string().contains("closing or closed"));
    assert_eq!(rig.world.created.load(Ordering::SeqCst), 0);
}

#[test]
fn an_adapter_that_does_not_dispose_in_time_returns_pending_at_the_bound_and_the_domain_retains_it()
{
    let rig = Rig::new();
    rig.world.stuck.store(true, Ordering::SeqCst);
    let (owner, handle) = rig.host(1);
    rig.attach(&handle);
    let mut close = owner.close();
    rig.progress.run();
    rig.clock.advance(4_999);
    rig.progress.run();
    assert!(
        matches!(poll_once(&mut close), Poll::Pending),
        "before the five-second bound"
    );
    rig.clock.advance(1);
    rig.progress.run();
    let Poll::Ready(CleanupStatus::Pending(ticket)) = poll_once(&mut close) else {
        panic!("at the bound the close returns Pending(ticket)");
    };
    assert_eq!(ticket.status(), TicketStatus::Pending);
    assert_eq!(rig.retention.held(), 1);
    assert_eq!(rig.domain.retained(), 1);
    assert_eq!(handle.snapshot().retained_cleanup, 1);
    // Dropping the ticket releases nothing.
    drop(ticket.clone());
    rig.retention.step_all();
    assert_eq!(rig.retention.held(), 1);
    assert_eq!(
        rig.world.dropped(),
        0,
        "the retainer still owns the adapter"
    );
    // The late disposal confirms the ticket and releases the generation once.
    rig.world.gate.release();
    rig.retention.step_all();
    assert_eq!(ticket.status(), TicketStatus::Confirmed);
    assert_eq!(rig.retention.held(), 0);
    assert_eq!(rig.domain.retained(), 0);
    assert_eq!(rig.world.disposed(), 1);
}

#[test]
fn a_ticket_wait_ends_at_its_deadline_or_at_confirmation() {
    let rig = Rig::new();
    rig.world.stuck.store(true, Ordering::SeqCst);
    let (owner, handle) = rig.host(1);
    rig.attach(&handle);
    let mut close = owner.close();
    rig.progress.run();
    rig.clock.advance(5_000);
    rig.progress.run();
    let Poll::Ready(CleanupStatus::Pending(ticket)) = poll_once(&mut close) else {
        panic!("pending at the bound");
    };
    let mut wait = ticket.wait_until(crate::Tick::new(5_100));
    assert!(matches!(poll_once(&mut wait), Poll::Pending));
    rig.clock.advance(100);
    assert!(matches!(
        poll_once(&mut wait),
        Poll::Ready(TicketStatus::Pending)
    ));
    let mut wait = ticket.wait_until(crate::Tick::new(9_000));
    assert!(matches!(poll_once(&mut wait), Poll::Pending));
    rig.world.gate.release();
    rig.retention.step_all();
    assert!(matches!(
        poll_once(&mut wait),
        Poll::Ready(TicketStatus::Confirmed)
    ));
}

#[test]
fn a_refused_retention_leaves_the_close_owning_the_work_until_it_is_confirmed() {
    let rig = Rig::new();
    rig.world.stuck.store(true, Ordering::SeqCst);
    rig.retention.set_refuse(true);
    let (owner, handle) = rig.host(1);
    rig.attach(&handle);
    let mut close = owner.close();
    rig.progress.run();
    rig.clock.advance(5_000);
    rig.progress.run();
    let Poll::Ready(CleanupStatus::Pending(ticket)) = poll_once(&mut close) else {
        panic!("pending at the bound");
    };
    assert_eq!(rig.retention.held(), 0);
    assert_eq!(ticket.status(), TicketStatus::Pending);
    assert_eq!(rig.world.dropped(), 0);
    rig.world.gate.release();
    rig.progress.run();
    assert_eq!(ticket.status(), TicketStatus::Confirmed);
    assert_eq!(rig.domain.retained(), 0);
}

#[test]
fn a_progress_that_cannot_run_work_at_close_hands_the_disposal_to_the_retainer() {
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    rig.attach(&handle);
    rig.progress.set_available(false);
    let mut close = owner.close();
    let Poll::Ready(CleanupStatus::Pending(ticket)) = poll_once(&mut close) else {
        panic!("the retainer owns it");
    };
    assert_eq!(rig.retention.held(), 1);
    rig.retention.step_all();
    assert_eq!(ticket.status(), TicketStatus::Confirmed);
    assert_eq!(rig.world.disposed(), 1);
}

#[test]
fn the_instance_bound_and_a_factory_failure_are_reported_with_their_cleanup() {
    let rig = Rig::new();
    let limits = HostLimits {
        endpoint_instances: 2,
        ..HostLimits::default()
    };
    let (_owner, handle) = crate::build(rig.config(limits), rig.ports(1)).unwrap();
    rig.attach(&handle);
    rig.attach(&handle);
    assert!(matches!(
        handle.attach_instance(spec(2)),
        Err(AttachError::InstanceLimit)
    ));
    assert_eq!(rig.world.created.load(Ordering::SeqCst), 2);
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    rig.world.fail_next.store(true, Ordering::SeqCst);
    match handle.attach_instance(spec(3)) {
        Err(AttachError::Factory(error)) => {
            assert!(matches!(error.cleanup(), CleanupStatus::Complete))
        }
        other => panic!("expected a factory error with its cleanup status, got {other:?}"),
    }
    assert_eq!(handle.snapshot().endpoint_instances, 0);
}

#[test]
fn two_hosts_built_on_one_process_safety_hand_the_same_gate_to_their_factories() {
    let rig = Rig::new();
    let (_a, first) = rig.host(1);
    let (_b, second) = rig.host(2);
    rig.attach(&first);
    rig.attach(&second);
    let seen = rig.world.safeties.lock().unwrap();
    assert_eq!(seen.len(), 2);
    assert!(Arc::ptr_eq(&seen[0].0, &seen[1].0));
    let shared: Arc<dyn crate::ProcessSafety> = rig.safety.clone();
    assert!(Arc::ptr_eq(&seen[0].0, &shared));
    // The gate is the composition's: a hold taken through it is counted once, whichever host's adapter took it.
    let _held = seen[0].0.hold().unwrap();
    let _held_too = seen[1].0.hold().unwrap();
    assert_eq!(rig.safety.holds.load(Ordering::SeqCst), 2);
}

#[test]
fn a_domain_is_not_manufactured_by_a_host() {
    // The same domain reaches every adapter of every host built on it.
    let rig = Rig::new();
    let (_a, first) = rig.host(1);
    let (_b, second) = rig.host(2);
    rig.attach(&first);
    rig.attach(&second);
    assert_eq!(rig.world.domains.lock().unwrap().len(), 2);
}
