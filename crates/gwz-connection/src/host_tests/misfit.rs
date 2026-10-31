//! Misfits: an opening that no connection fits, its deadline, and the proof that resubmits.
use super::support::{one_exchange, one_https, registered_with};
use crate::{
    AuthContext, AuthScope, EndpointRoute, HostHandle, OpenFailure, Operation, OperationPolicy,
    PreparedOperation,
    fakes::{Rig, auth, exchange_spec, https_spec, now, poll_once, profile},
};
use std::{
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

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
