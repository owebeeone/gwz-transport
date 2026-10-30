//! The host's owner and handle against fakes only (step 1.1 of dev-docs/GwzTransportReleaseConvergencePlan.md;
//! contract sections 4, 4.1, 5, 10, 11 and 13.1): no file, socket, thread or helper is involved.
use crate::fakes::{
    FakeSafety, Rig, auth, exchange_spec, https_spec, now, poll_once, prepared_with, profile, spec,
};
use crate::{
    AttachError, AuthContext, AuthScope, BindingId, BuildError, CleanupDomain, CleanupDomainOwner,
    CleanupStatus, CleanupTicket, ConfigReason, Direction, Discard, DomainClose, EndpointProfile,
    EndpointRoute, EndpointSpec, HostClose, HostClosing, HostHandle, HostLimits, HostOwner,
    HostPhase, OpenFailure, Operation, OperationPolicy, PreparedOperation, Registration, Routed,
    TicketStatus,
};
use gwz_transport::protocol::{Disposition, Envelope, MessageKind};
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
fn the_default_admission_bound_is_strictly_below_the_mux_bootstrap_bound() {
    // Release-kind decision A (2026-10-31): the client's admission wait must end before the core mux's bootstrap bound.
    // Both are read live, so a change to either one that makes them meet or cross fails here.
    let admission = HostLimits::default().admission_bound_ms;
    let bootstrap = gwz_transport::mux::Config::default().bootstrap_timeout_ms;
    assert_eq!(admission, crate::host::DEFAULT_ADMISSION_BOUND_MS);
    assert!(
        admission < bootstrap,
        "admission bound {admission} ms is not below the mux bootstrap bound {bootstrap} ms"
    );
    // The bindings this crate makes use that same default bootstrap bound (binding.rs `mux::Config`).
    assert_eq!(
        crate::binding::mux_config(1).bootstrap_timeout_ms,
        bootstrap
    );
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

// Step 1.5, profile and partition (contract sections 6 and 7; plan row R4), against fakes. Core captures HOME, the
// agent, the CA file and the proxy into the profile, and the helper environment (`GIT_CONFIG_GLOBAL`, `GH_TOKEN`) into
// the operation's authentication context; these rows hold what the host does with each.

/// An admitted and registered operation over the profile of kind `profile`, with `auth` as its context.
fn registered_with(handle: &HostHandle, name: &str, profile: u8, auth: AuthContext) -> Operation {
    now(handle
        .admit(prepared_with(name, profile, auth))
        .unwrap()
        .acquire())
    .unwrap()
    .register()
}

/// One operation with one `spec` exchange, finished cleanly: whether its exchange reused a connection.
fn one_exchange_on(
    handle: &HostHandle,
    name: &str,
    profile: u8,
    auth: AuthContext,
    spec: crate::ExchangeSpec,
) -> bool {
    let operation = registered_with(handle, name, profile, auth);
    let exchange = now(operation.open(spec).unwrap().wait()).unwrap();
    let reused = exchange.reused();
    assert_eq!(now(exchange.finish()).disposition, Disposition::Reusable);
    now(operation.finish());
    reused
}
/// One operation with one SSH exchange.
fn one_exchange(handle: &HostHandle, name: &str, profile: u8, auth: AuthContext) -> bool {
    one_exchange_on(handle, name, profile, auth, exchange_spec())
}
/// One operation with one HTTPS exchange: the scheme the authentication scope partitions.
fn one_https(handle: &HostHandle, name: &str, profile: u8, auth: AuthContext) -> bool {
    one_exchange_on(handle, name, profile, auth, https_spec())
}

#[test]
fn a_changed_profile_is_served_by_another_instance_and_reuse_resumes_when_it_changes_back() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    assert!(!one_exchange(&handle, "op-1", 1, auth(1, "a")));
    assert!(
        one_exchange(&handle, "op-2", 1, auth(1, "a")),
        "an equal profile reuses"
    );
    assert!(
        !one_exchange(&handle, "op-3", 2, auth(1, "a")),
        "a changed profile reuses nothing"
    );
    assert_eq!(handle.snapshot().endpoint_instances, 2);
    assert!(
        one_exchange(&handle, "op-4", 1, auth(1, "a")),
        "the first profile's instance and idle connection are still there"
    );
    assert_eq!(rig.world.connects(), 2);
    assert_eq!(rig.world.closes(), 0);
}

#[test]
fn a_changed_auth_context_on_an_equal_profile_reuses_nothing_and_reuse_resumes_under_the_new_one() {
    // HTTPS (the scope partitions HTTPS only, the lane owner's ruling on C-P3-2): the pool's idle match compares a
    // scope-derived identity, so an operation is offered its own scope's idle connection, never another's.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    assert!(!one_https(&handle, "op-1", 1, auth(1, "a")));
    rig.clock.advance(30_000);
    assert!(
        !one_https(&handle, "op-2", 1, auth(2, "b")),
        "a connection set up under another context is never leased"
    );
    // Equal profiles share the instance: the context is not a profile field (contract section 7).
    assert_eq!(handle.snapshot().endpoint_instances, 1);
    assert_eq!(rig.world.connects(), 2);
    assert_eq!(
        rig.world.closes(),
        0,
        "the other context's idle connection is kept, not retired"
    );
    // B's second operation reuses B's connection while A's older idle connection still exists.
    assert_eq!(handle.snapshot().connections.idle, 2);
    assert!(one_https(&handle, "op-3", 1, auth(2, "b")));
    assert_eq!(rig.world.connects(), 2);
    assert_eq!(handle.snapshot().connections.idle, 2);
    // And A still reuses its own.
    assert!(one_https(&handle, "op-4", 1, auth(1, "a")));
    assert_eq!(rig.world.connects(), 2);
    assert_eq!(
        operation_misfits(&handle, &rig),
        0,
        "the pool offered no other scope's connection"
    );
}

/// Leases of another scope the instance of profile 1 has refused, read through a fresh operation on it.
fn operation_misfits(handle: &HostHandle, rig: &Rig) -> u64 {
    let _ = rig;
    let probe = registered_with(handle, "probe", 1, auth(1, "a"));
    let misfits = probe.instance_for_test().misfits_for_test();
    now(probe.finish());
    misfits
}

#[test]
fn each_operations_connects_and_exchanges_carry_its_own_auth_context() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let a = registered_with(&handle, "op-a", 1, auth(1, "a"));
    let b = registered_with(&handle, "op-b", 1, auth(2, "b"));
    let exchange_a = now(a.open(exchange_spec()).unwrap().wait()).unwrap();
    let exchange_b = now(b.open(exchange_spec()).unwrap().wait()).unwrap();
    assert_eq!(handle.snapshot().endpoint_instances, 1);
    let seen = |kind: &'static str, tag: &str| (kind, Some(tag.to_string()));
    assert_eq!(
        *rig.world.contexts.lock().unwrap(),
        vec![
            seen("connect", "a"),
            seen("exchange", "a"),
            seen("connect", "b"),
            seen("exchange", "b"),
        ],
        "equal-profile operations never run on one another's context"
    );
    now(exchange_a.finish());
    now(exchange_b.finish());
    now(a.finish());
    now(b.finish());
}

#[test]
fn no_operations_auth_context_outlives_it_in_the_host() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let provider = Arc::new(String::from("a"));
    let context = || AuthContext::new(64, AuthScope::new([1; 32]), provider.clone());
    // An opening dropped while its connect is under way leaves nothing behind it.
    rig.world.asynchronous.store(true, Ordering::SeqCst);
    let waiting = registered_with(&handle, "op-1", 1, context());
    let mut opening = waiting.open(exchange_spec()).unwrap().wait();
    assert!(poll_once(&mut opening).is_pending());
    drop(opening);
    // The live operation's binding holds its context, and nothing else does: the instance gave it back.
    assert_eq!(Arc::strong_count(&provider), 2, "the abandoned opening");
    drop(waiting);
    assert_eq!(Arc::strong_count(&provider), 1, "the dropped operation");
    // A finished operation leaves its connection idle, and none of its context.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    assert!(!one_exchange(&handle, "op-2", 1, context()));
    assert_eq!(handle.snapshot().connections.idle, 1);
    assert_eq!(Arc::strong_count(&provider), 1, "the finished operation");
}

/// One operation with one exchange (HTTPS or SSH) over profile `profile` and context `context`, finished cleanly, whether
/// its exchange reused a connection. `previous` is the binding and session of the operation before it: that operation's
/// late frames are sent to this one's binding, and must be discarded with no record made anywhere.
fn r4_operation(
    handle: &HostHandle,
    profile: u8,
    context: AuthContext,
    https: bool,
    previous: &mut Option<(BindingId, String)>,
) -> bool {
    let operation = registered_with(handle, "op", profile, context);
    let Registration::Registered(id) = operation.registration() else {
        panic!("not registered");
    };
    if let Some((old_id, old_session)) = previous.take() {
        let late = (
            "x".to_string(),
            Envelope {
                version: 2,
                session_id: old_session,
                stream_id: 5,
                kind: MessageKind::Data,
                ..Default::default()
            },
        );
        let before = handle.snapshot();
        for direction in [Direction::ToEndpoint, Direction::ToDriver] {
            assert_eq!(
                handle.route(handle.carrier(), id, direction, &late),
                Routed::Discarded(Discard::WrongSession),
                "the old operation's event does not reach the new one"
            );
            assert_eq!(
                handle.route(handle.carrier(), old_id, direction, &late),
                Routed::Discarded(Discard::Retired)
            );
        }
        assert_eq!(handle.snapshot(), before);
        assert_eq!(operation.binding_streams(), (0, 0));
    }
    let session = operation.binding_session();
    let spec = if https { https_spec() } else { exchange_spec() };
    let exchange = now(operation.open(spec).unwrap().wait()).unwrap();
    let reused = exchange.reused();
    assert_eq!(now(exchange.finish()).disposition, Disposition::Reusable);
    now(operation.finish());
    *previous = Some((id, session));
    reused
}

#[test]
fn r4_a_changed_identity_configuration_or_helper_environment_reuses_nothing_and_no_stale_event_crosses()
 {
    // Plan section 3, R4. The core folds HOME, the agent, the CA file and the proxy into the endpoint profile (one profile
    // kind each here) and the helper environment, GIT_CONFIG_GLOBAL and GH_TOKEN, into the operation's authentication
    // context (one HTTPS scope each). After each change the first operation reuses nothing, reuse then resumes under the
    // new value, the old value's idle connection is untouched, and no event of the old operation reaches the new one.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let mut previous = None;
    let ssh = |profile, previous: &mut Option<(BindingId, String)>| {
        r4_operation(&handle, profile, auth(1, "a"), false, previous)
    };
    assert!(!ssh(1, &mut previous), "the first operation connects");
    assert!(ssh(1, &mut previous), "an unchanged configuration reuses");
    for (changed, profile) in [("HOME", 2), ("agent", 3), ("CA file", 4), ("proxy", 5)] {
        assert!(
            !ssh(profile, &mut previous),
            "{changed} changed: the first operation after it reuses nothing"
        );
        assert!(
            ssh(profile, &mut previous),
            "{changed}: reuse resumes under the new value"
        );
        assert!(
            ssh(1, &mut previous),
            "{changed}: the old value's idle connection is untouched"
        );
    }
    assert_eq!(
        handle.snapshot().endpoint_instances,
        5,
        "one instance per profile"
    );
    assert_eq!(rig.world.connects(), 5);
    let https = |scope: u8, previous: &mut Option<(BindingId, String)>| {
        r4_operation(&handle, 1, auth(scope, "env"), true, previous)
    };
    assert!(!https(1, &mut previous));
    for (changed, scope) in [("GIT_CONFIG_GLOBAL", 2), ("GH_TOKEN", 3)] {
        assert!(
            !https(scope, &mut previous),
            "{changed} changed: the first operation after it reuses nothing"
        );
        assert!(
            https(scope, &mut previous),
            "{changed}: reuse resumes under the new value"
        );
        assert!(
            https(1, &mut previous),
            "{changed}: the old value's idle connection is untouched"
        );
    }
    assert_eq!(
        rig.world.connects(),
        8,
        "five SSH profiles and three HTTPS scopes, each connected once"
    );
    assert_eq!(
        rig.world.closes(),
        0,
        "no change closed another value's connection"
    );
    assert_eq!(handle.snapshot().bindings, 0);
}

#[test]
fn the_factory_is_made_from_the_profiles_validated_inputs() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let profile =
        EndpointProfile::with_inputs(vec![9; 8], [9; 32], Arc::new(String::from("inputs-9")))
            .unwrap();
    let input = PreparedOperation::new(
        "op",
        EndpointRoute::ssh_and_https(),
        profile,
        auth(1, "a"),
        OperationPolicy::new(4, 32, 3).unwrap(),
    )
    .unwrap();
    let operation = now(handle.admit(input).unwrap().acquire())
        .unwrap()
        .register();
    assert_eq!(
        *rig.world.inputs.lock().unwrap(),
        vec![Some("inputs-9".to_string())]
    );
    now(operation.finish());
    // The inputs are not compared: equal canonical bytes are one profile whatever inputs they carry.
    let same = EndpointProfile::with_inputs(vec![9; 8], [9; 32], Arc::new(0u8)).unwrap();
    assert!(same.same_as(&EndpointProfile::new(vec![9; 8], [9; 32]).unwrap()));
}

#[test]
fn a_digest_collision_is_not_equality_and_gets_another_instance() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let colliding = |n: u8| EndpointSpec::new(EndpointProfile::new(vec![n; 8], [7; 32]).unwrap());
    handle.attach_instance(colliding(1)).unwrap();
    handle.attach_instance(colliding(2)).unwrap();
    assert_eq!(handle.snapshot().endpoint_instances, 2);
    handle.attach_instance(colliding(1)).unwrap();
    assert_eq!(handle.snapshot().endpoint_instances, 2);
}

#[test]
fn an_opening_that_no_connection_ever_fits_fails_by_name_within_its_bound() {
    // Every connection the pool offers was set up under another context (the helper below rescopes each one before
    // its connect completes), so each lease is retired and the opening asks again; it must end, typed, after at most
    // `misfit_bound` misfits (the instance pool's per-user-host ceiling, 256, plus one: idle entries on a key can reach it),
    // never spin.
    let rig = Rig::new();
    rig.world.asynchronous.store(true, Ordering::SeqCst);
    let (_owner, handle) = rig.host(1);
    let operation = registered_with(&handle, "op", 1, auth(1, "a"));
    let mut wait = operation.open(https_spec()).unwrap().wait();
    let foreign = AuthScope::new([9; 32]);
    let bound = 256 + 1;
    let mut passes = 0;
    while !wait.is_ready() {
        assert!(
            passes <= bound,
            "the opening was still asking after {passes} misfits"
        );
        operation.instance_for_test().rescope_for_test(foreign);
        rig.world.complete_connects();
        rig.progress.run();
        passes += 1;
    }
    assert!(matches!(
        poll_once(&mut wait),
        Poll::Ready(Err(OpenFailure::NoFittingConnection))
    ));
    assert_eq!(passes, bound + 1, "one misfit per completed connect");
    assert_eq!(
        rig.world.exchanges.load(Ordering::SeqCst),
        0,
        "nothing was started"
    );
    drop(wait);
    now(operation.finish());
}

// Step 1.5 remediation round 1 (dev-docs/GwzConnectionApiContract-Step1.5-RemPlan.md).

/// The scope-a exchanges that fill the instance's ceiling on the HTTPS key, all leased: 256 of them, held by two
/// operations at 128 per host each (an operation is held to its own limit, so one at 32 no longer fills the instance).
fn saturate(handle: &HostHandle) -> (Operation, Operation, Vec<crate::Exchange>) {
    let at_128 = |name: &str| {
        let prepared = PreparedOperation::new(
            name,
            EndpointRoute::ssh_and_https(),
            profile(1),
            auth(1, "a"),
            OperationPolicy::new(4, 128, 3).unwrap(),
        )
        .unwrap();
        now(handle.admit(prepared).unwrap().acquire())
            .unwrap()
            .register()
    };
    let (first, second) = (at_128("op-a"), at_128("op-a2"));
    let exchanges = [&first, &second]
        .into_iter()
        .flat_map(|operation| std::iter::repeat_n(operation, 128))
        .map(|operation| now(operation.open(https_spec()).unwrap().wait()).unwrap())
        .collect();
    (first, second, exchanges)
}

#[test]
fn a_misfit_keeps_the_other_scopes_connection_and_opens_its_own() {
    // C-P2-1 = S-P2-1, closure row 1: A, B (other scope), A.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    assert!(!one_https(&handle, "op-a1", 1, auth(1, "a")));
    assert!(!one_https(&handle, "op-b", 1, auth(2, "b")));
    assert_eq!(rig.world.connects(), 2, "B opened its own connection");
    assert_eq!(
        rig.world.closes(),
        0,
        "A's idle connection was released untouched, never retired"
    );
    assert_eq!(handle.snapshot().connections.idle, 2);
    assert!(
        one_https(&handle, "op-a2", 1, auth(1, "a")),
        "A's own connection still serves it"
    );
}

#[test]
fn a_misfit_opening_times_out_at_its_original_allocation_deadline() {
    // C-P2-1 = S-P2-1, closure row 2: a misfit at 20 s (the connection B's request opened is found set up under another
    // scope) resubmits fresh, and the resubmission keeps the deadline B was given at submit.
    let rig = Rig::new();
    rig.world.asynchronous.store(true, Ordering::SeqCst);
    let (_owner, handle) = rig.host(1);
    let operation_b = registered_with(&handle, "op-b", 1, auth(2, "b"));
    let mut wait = operation_b.open(https_spec()).unwrap().wait();
    assert!(!wait.is_ready());
    rig.clock.advance(20_000);
    let instance = operation_b.instance_for_test();
    instance.rescope_for_test(AuthScope::new([9; 32]));
    rig.world.complete_connects();
    rig.progress.run();
    assert_eq!(instance.misfits_for_test(), 1);
    assert!(!wait.is_ready());
    // The misfit lease went back untouched, for its own scope: idle, not retired.
    assert_eq!(rig.world.closes(), 0);
    assert_eq!(handle.snapshot().connections.idle, 1);
    let allocation = gwz_transport::pool::Config::default().allocation_timeout_ms;
    rig.clock.advance(allocation - 20_000 - 1);
    rig.progress.run();
    assert!(!wait.is_ready(), "still within the original deadline");
    rig.clock.advance(1);
    rig.progress.run();
    assert!(wait.is_ready(), "the misfit did not extend the deadline");
    assert!(matches!(
        poll_once(&mut wait),
        Poll::Ready(Err(OpenFailure::Pool(_)))
    ));
    drop(wait);
    rig.world.asynchronous.store(false, Ordering::SeqCst);
    now(operation_b.finish());
}

#[test]
fn a_failed_reuse_proof_resubmits_under_the_original_deadline() {
    // Step 1.6's ask-again after an unprovable proof keeps the opening's original allocation deadline too.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    assert!(!one_exchange(&handle, "op-a", 1, auth(1, "a")));
    rig.world.hold_reuse.store(true, Ordering::SeqCst);
    let operation_b = registered_with(&handle, "op-b", 1, auth(1, "a"));
    let mut wait = operation_b.open(exchange_spec()).unwrap().wait();
    assert_eq!(rig.world.reuses().len(), 1, "B waits for its proof");
    rig.clock.advance(20_000);
    rig.world.asynchronous.store(true, Ordering::SeqCst);
    rig.world.end_reuse(crate::ReuseVerdict::Unprovable);
    rig.progress.run();
    assert!(
        !wait.is_ready(),
        "B asked again for a connection of its own"
    );
    let allocation = gwz_transport::pool::Config::default().allocation_timeout_ms;
    rig.clock.advance(allocation - 20_000 - 1);
    rig.progress.run();
    assert!(!wait.is_ready(), "still within the original deadline");
    rig.clock.advance(1);
    rig.progress.run();
    assert!(
        wait.is_ready(),
        "the resubmission did not extend the deadline"
    );
    assert!(matches!(
        poll_once(&mut wait),
        Poll::Ready(Err(OpenFailure::Pool(_)))
    ));
    drop(wait);
    rig.world.asynchronous.store(false, Ordering::SeqCst);
    now(operation_b.finish());
}

#[test]
fn under_saturated_load_of_one_scope_another_scope_is_served_after_at_most_one_misfit() {
    // C-P2-1 = S-P2-1, closure rows 3 and 4: Code's sequence (a scope-a opening queues behind B, then a scope-a exchange
    // ends) for more than `per_user_host + 1` cycles never fails B with NoFittingConnection; when the load ends B is
    // served on a connection of its own, after at most one misfit.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let (operation_a, _operation_a2, mut exchanges) = saturate(&handle);
    let operation_b = registered_with(&handle, "op-b", 1, auth(2, "b"));
    let mut wait = operation_b.open(https_spec()).unwrap().wait();
    let cycles = gwz_transport::pool::Config::default().per_user_host + 2;
    let mut queued = Vec::new();
    for _ in 0..cycles {
        queued.push(operation_a.open(https_spec()).unwrap().wait());
        now(exchanges.remove(0).finish());
        rig.progress.run();
        assert!(
            !matches!(
                poll_once(&mut wait),
                Poll::Ready(Err(OpenFailure::NoFittingConnection))
            ),
            "B failed NoFittingConnection under legal load"
        );
        // The scope-a waiters are served from the connections their scope set up.
        for opening in &mut queued {
            if opening.is_ready()
                && let Poll::Ready(Ok(exchange)) = poll_once(opening)
            {
                exchanges.push(exchange);
            }
        }
        queued.retain(|opening| !opening.is_ready());
    }
    // The load ends: the next connection that comes free makes room for B's own.
    now(exchanges.remove(0).finish());
    rig.progress.run();
    let Poll::Ready(Ok(exchange)) = poll_once(&mut wait) else {
        panic!("B was not served once the load ended");
    };
    assert!(
        !exchange.reused(),
        "B's exchange runs on a connection B set up"
    );
    assert!(operation_b.instance_for_test().misfits_for_test() <= 1);
    now(exchange.finish());
    drop(queued);
    drop(exchanges);
    now(operation_b.finish());
    now(operation_a.finish());
}

#[test]
fn dropping_an_opening_after_a_misfit_leaves_no_request_context_or_exchange() {
    // C-P2-2: the dropped opening is found by its slot, and its current request is the one abandoned.
    let rig = Rig::new();
    rig.world.asynchronous.store(true, Ordering::SeqCst);
    let (_owner, handle) = rig.host(1);
    let provider = Arc::new(String::from("a"));
    let operation = registered_with(
        &handle,
        "op",
        1,
        AuthContext::new(64, AuthScope::new([1; 32]), provider.clone()),
    );
    let mut wait = operation.open(https_spec()).unwrap().wait();
    assert!(poll_once(&mut wait).is_pending());
    let instance = operation.instance_for_test();
    instance.rescope_for_test(AuthScope::new([9; 32]));
    rig.world.complete_connects();
    rig.progress.run();
    assert_eq!(instance.misfits_for_test(), 1);
    drop(wait);
    assert_eq!(
        Arc::strong_count(&provider),
        2,
        "only the binding holds the context"
    );
    assert_eq!(instance.outstanding_for_test(), 0, "no live pool request");
    rig.world.complete_connects();
    rig.progress.run();
    assert_eq!(
        rig.world.exchanges.load(Ordering::SeqCst),
        0,
        "nothing started after the drop"
    );
    let end = now(operation.finish());
    assert_eq!(end.exchanges_opened, 1);
    assert_eq!(Arc::strong_count(&provider), 1);
}

#[test]
fn ssh_connections_are_not_partitioned_by_the_auth_context() {
    // C-P3-2, the lane owner's ruling: the scope applies to HTTPS keys only; SSH reuse authorization is step 1.6's.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    assert!(!one_exchange(&handle, "op-a", 1, auth(1, "a")));
    assert!(
        one_exchange(&handle, "op-b", 1, auth(2, "b")),
        "a changed GH_TOKEN on SSH reuses"
    );
    assert_eq!(rig.world.connects(), 1);
}

#[test]
fn an_adapter_holds_the_connect_context_only_until_the_connect_completes() {
    // S-P3-2: the fake adapter's conformance to `ConnectCommand.auth`'s retention rule.
    let rig = Rig::new();
    rig.world.asynchronous.store(true, Ordering::SeqCst);
    let (_owner, handle) = rig.host(1);
    let provider = Arc::new(String::from("a"));
    let operation = registered_with(
        &handle,
        "op",
        1,
        AuthContext::new(64, AuthScope::new([1; 32]), provider.clone()),
    );
    let wait = operation.open(https_spec()).unwrap().wait();
    rig.world.complete_connects();
    rig.progress.run();
    assert!(wait.is_ready());
    // The test's and the binding's: neither the adapter nor the instance kept one.
    assert_eq!(Arc::strong_count(&provider), 2);
    rig.world.asynchronous.store(false, Ordering::SeqCst);
    let exchange = now(wait).unwrap();
    now(exchange.finish());
    now(operation.finish());
}
