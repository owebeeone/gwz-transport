//! Capacity per binding (step 1.4 of dev-docs/GwzTransportReleaseConvergencePlan.md; reuse design section 5; contract
//! section 8), against fakes only: an operation's limits come from its own policy, travel with its binding to every pool
//! request it makes, and hold it alone. Two bindings with different `--max-per-host` overlap on one instance, each
//! held to its own limit; the instance's ceilings are the pool's (max rule: a limit above them is neither clamped nor
//! refused); one binding finishing changes nothing for the other.
use crate::fakes::{Rig, auth, exchange_spec, now, profile};
use crate::{
    AdmissionPermit, EndpointRoute, Exchange, HostHandle, OpenWait, Operation, OperationPolicy,
    PreparedOperation,
};

fn registered(handle: &HostHandle, name: &str, jobs: usize, per_host: usize) -> Operation {
    let policy = OperationPolicy::new(jobs, per_host, 3).unwrap();
    let prepared = PreparedOperation::new(
        name,
        EndpointRoute::ssh_and_https(),
        profile(1),
        auth(0, "none"),
        policy,
    )
    .unwrap();
    let permit: AdmissionPermit = now(handle.admit(prepared).unwrap().acquire()).unwrap();
    permit.register()
}
/// `count` openings of `operation`, each waiting for a connection.
fn open_many(rig: &Rig, operation: &Operation, count: usize) -> Vec<OpenWait> {
    let waits = (0..count)
        .map(|_| operation.open(exchange_spec()).unwrap().wait())
        .collect();
    rig.progress.run();
    waits
}
/// Takes the exchanges of the waits that are ready; the rest keep waiting.
fn ready(waits: &mut Vec<OpenWait>) -> Vec<Exchange> {
    let mut started = Vec::new();
    let mut waiting = Vec::new();
    for wait in waits.drain(..) {
        if wait.is_ready() {
            started.push(now(wait).unwrap());
        } else {
            waiting.push(wait);
        }
    }
    *waits = waiting;
    started
}

#[test]
fn p1_4_two_bindings_with_different_max_per_host_overlap_each_held_to_its_own() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let small = registered(&handle, "four", 4, 4);
    let large = registered(&handle, "thirty-two", 4, 32);
    let mut small_waits = open_many(&rig, &small, 8);
    let mut large_waits = open_many(&rig, &large, 40);
    let small_started = ready(&mut small_waits);
    let large_started = ready(&mut large_waits);
    assert_eq!(small_started.len(), 4, "the binding at 4 holds 4");
    assert_eq!(large_started.len(), 32, "the binding at 32 holds 32");
    assert_eq!(handle.snapshot().connections.leased, 36);
    assert_eq!(rig.world.connects(), 36);
    for exchange in small_started.into_iter().chain(large_started) {
        now(exchange.finish());
    }
}

#[test]
fn p1_4_one_binding_finishing_changes_neither_the_other_s_limit_nor_its_connections() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let small = registered(&handle, "four", 4, 4);
    let large = registered(&handle, "thirty-two", 4, 32);
    let mut small_waits = open_many(&rig, &small, 4);
    let mut large_waits = open_many(&rig, &large, 33);
    let small_started = ready(&mut small_waits);
    let mut large_started = ready(&mut large_waits);
    assert_eq!((small_started.len(), large_started.len()), (4, 32));
    for exchange in small_started {
        now(exchange.finish());
    }
    now(small.finish());
    rig.progress.run();
    // The 4 connections are idle and the 33rd opening of the binding at 32 is still held to 32.
    assert_eq!(handle.snapshot().connections.leased, 32);
    assert!(!large_waits[0].is_ready());
    assert!(ready(&mut large_waits).is_empty());
    for exchange in large_started.drain(..) {
        now(exchange.finish());
    }
}

#[test]
fn p1_4_a_limit_above_the_old_pool_default_is_neither_clamped_nor_refused() {
    // A lone binding at 64 per host gets 64 (the pool's default per host is 32; its ceilings are the reuse design's 256):
    // the no-clamp rule. The max rule with ceilings below the limit is proved on the pool itself (tests/pool_owner_limits.rs).
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let big = registered(&handle, "big", 64, 64);
    let mut waits = open_many(&rig, &big, 70);
    let started = ready(&mut waits);
    assert_eq!(started.len(), 64);
    for exchange in started {
        now(exchange.finish());
    }
}
