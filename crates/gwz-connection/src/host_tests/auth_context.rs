//! Profile and authentication-context partitioning of reuse (step 1.5, plan row R4).
use super::support::{one_exchange, one_https, registered_with};
use crate::{
    AuthContext, AuthScope, BindingId, Direction, Discard, HostHandle, Registration, Routed,
    fakes::{Rig, auth, exchange_spec, https_spec, now, poll_once},
};
use gwz_transport::protocol::{Disposition, Envelope, MessageKind};
use std::sync::{Arc, atomic::Ordering};

// Step 1.5, profile and partition (contract sections 6 and 7; plan row R4), against fakes. Core captures HOME, the
// agent, the CA file and the proxy into the profile, and the helper environment (`GIT_CONFIG_GLOBAL`, `GH_TOKEN`) into
// the operation's authentication context; these rows hold what the host does with each.

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
