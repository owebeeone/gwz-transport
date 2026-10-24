//! The cleanup domain against a stepped retainer (contract sections 4 and 4.1; plan step 1.1).
use crate::fakes::{Rig, poll_once};
use crate::{
    BuildError, CleanupStatus, ConfigReason, DomainConfig, HostClosing, HostLimits, HostPhase,
    TicketStatus, cleanup_domain,
};
use std::{sync::atomic::Ordering, task::Poll};

#[test]
fn an_invalid_domain_budget_is_refused() {
    let rig = Rig::new();
    let cases: [fn(&mut DomainConfig); 3] = [
        |c| c.setup_jobs = 0,
        |c| c.retained_cleanup = 0,
        |c| c.helper_slots = 0,
    ];
    for change in cases {
        let mut config = DomainConfig::default();
        change(&mut config);
        assert!(matches!(
            cleanup_domain(config, rig.retention.clone()),
            Err(BuildError::InvalidConfig(ConfigReason::DomainLimits))
        ));
    }
    let defaults = DomainConfig::default();
    assert_eq!(
        (
            defaults.setup_jobs,
            defaults.retained_cleanup,
            defaults.helper_slots
        ),
        (64, 64, 8)
    );
}

#[test]
fn closing_the_domain_revokes_its_hosts_and_refuses_new_ones() {
    let mut rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    rig.attach(&handle);
    let mut close = rig.domain_owner.take().unwrap().close();
    // Sealed and revoked before anything is polled.
    assert_eq!(handle.snapshot().phase, HostPhase::Closing);
    assert_eq!(handle.check_open(), Err(HostClosing));
    assert!(matches!(
        crate::build(rig.config(HostLimits::default()), rig.ports(2)),
        Err(BuildError::DomainClosing)
    ));
    assert!(matches!(poll_once(&mut close), Poll::Pending));
    rig.progress.run();
    assert!(matches!(
        poll_once(&mut close),
        Poll::Ready(CleanupStatus::Complete)
    ));
    assert_eq!(rig.world.disposed(), 1);
    assert_eq!(handle.snapshot().phase, HostPhase::Closed);
}

#[test]
fn dropping_the_domain_owner_revokes_like_close() {
    let mut rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    drop(rig.domain_owner.take());
    assert_eq!(handle.check_open(), Err(HostClosing));
    assert!(matches!(
        crate::build(rig.config(HostLimits::default()), rig.ports(2)),
        Err(BuildError::DomainClosing)
    ));
}

#[test]
fn closing_one_host_does_not_release_another_hosts_pending_record() {
    let mut rig = Rig::new();
    rig.world.stuck.store(true, Ordering::SeqCst);
    let (owner_a, host_a) = rig.host(1);
    rig.attach(&host_a);
    let mut close_a = owner_a.close();
    rig.progress.run();
    rig.clock.advance(5_000);
    rig.progress.run();
    let Poll::Ready(CleanupStatus::Pending(ticket_a)) = poll_once(&mut close_a) else {
        panic!("A is pending at the bound");
    };
    // B closes cleanly on the same domain.
    rig.world.stuck.store(false, Ordering::SeqCst);
    let (owner_b, host_b) = rig.host(2);
    rig.attach(&host_b);
    let mut close_b = owner_b.close();
    rig.progress.run();
    assert!(matches!(
        poll_once(&mut close_b),
        Poll::Ready(CleanupStatus::Complete)
    ));
    assert_eq!(ticket_a.status(), TicketStatus::Pending);
    assert_eq!(rig.domain.retained(), 1);
    // The domain's own close reports the same retained generation.
    let mut domain_close = rig.domain_owner.take().unwrap().close();
    rig.progress.run();
    let Poll::Ready(CleanupStatus::Pending(domain_ticket)) = poll_once(&mut domain_close) else {
        panic!("the domain close names A's pending generation");
    };
    assert_eq!(domain_ticket.generation(), ticket_a.generation());
    // Dropping every ticket and the domain handle releases nothing; only the confirmation does.
    drop(domain_ticket);
    drop(ticket_a.clone());
    assert_eq!(rig.retention.held(), 1);
    rig.world.gate.release();
    rig.retention.step_all();
    assert_eq!(ticket_a.status(), TicketStatus::Confirmed);
    assert_eq!(rig.domain.retained(), 0);
}

#[test]
fn the_retained_cleanup_budget_is_finite() {
    // A domain with room for one retained record: the second pending close is not retained (the close keeps owning it).
    let rig = Rig::new();
    let (_domain_owner, domain) = cleanup_domain(
        DomainConfig {
            retained_cleanup: 1,
            ..DomainConfig::default()
        },
        rig.retention.clone(),
    )
    .unwrap();
    rig.world.stuck.store(true, Ordering::SeqCst);
    let mut tickets = Vec::new();
    for seed in [1u128, 2] {
        let config = crate::HostConfig {
            domain: domain.clone(),
            safety: rig.safety.clone(),
            limits: HostLimits::default(),
        };
        let (owner, handle) = crate::build(config, rig.ports(seed)).unwrap();
        rig.attach(&handle);
        let mut close = owner.close();
        rig.progress.run();
        rig.clock.advance(5_000);
        rig.progress.run();
        let Poll::Ready(CleanupStatus::Pending(ticket)) = poll_once(&mut close) else {
            panic!("pending at the bound");
        };
        tickets.push(ticket);
    }
    assert_eq!(domain.retained(), 1);
    assert_eq!(
        rig.retention.held(),
        1,
        "the second was kept by its close, not by the retainer"
    );
}
