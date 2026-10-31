//! Seal and disposal: close, drop, a handle that outlives its owner, pending cleanup and retention.
use crate::{
    AttachError, CleanupStatus, HostClosing, HostPhase, TicketStatus,
    fakes::{Rig, poll_once, spec},
};
use std::{sync::atomic::Ordering, task::Poll};

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
