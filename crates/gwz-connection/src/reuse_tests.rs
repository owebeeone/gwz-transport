//! Cross-binding reuse authorization at the host (step 1.6 of dev-docs/GwzTransportReleaseConvergencePlan.md;
//! contract sections 4.1 and 7.1), against fakes only: an idle connection another binding last used is leased to a
//! binding only after the adapter's `authorize_reuse` proof for that binding says `Authorized`; a revoked proof
//! discards it, an unprovable one leaves it idle for its binding, and the opening asks for a new connection either way.
use crate::fakes::{Rig, exchange_spec, now, prepared};
use crate::{AdmissionPermit, Operation, Registration, ReuseVerdict};
use std::sync::atomic::Ordering;

fn registered(handle: &crate::HostHandle, name: &str) -> Operation {
    let permit: AdmissionPermit = now(handle.admit(prepared(name, 1)).unwrap().acquire()).unwrap();
    permit.register()
}
fn binding(operation: &Operation) -> crate::BindingId {
    match operation.registration() {
        Registration::Registered(id) => id,
        other => panic!("not registered: {other:?}"),
    }
}
/// A ran one exchange to completion and finished; its connection is idle.
fn idle_after_a(rig: &Rig, handle: &crate::HostHandle) {
    let a = registered(handle, "op-a");
    now(now(a.open(exchange_spec()).unwrap().wait())
        .unwrap()
        .finish());
    now(a.finish());
    assert_eq!(handle.snapshot().connections.idle, 1);
    assert_eq!(rig.world.connects(), 1);
}

#[test]
fn p1_6_the_same_binding_reuses_without_a_proof() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let a = registered(&handle, "op-a");
    now(now(a.open(exchange_spec()).unwrap().wait())
        .unwrap()
        .finish());
    now(now(a.open(exchange_spec()).unwrap().wait())
        .unwrap()
        .finish());
    assert_eq!(rig.world.connects(), 1);
    assert!(
        rig.world.reuses().is_empty(),
        "within one binding nothing is proved"
    );
    now(a.finish());
}

#[test]
fn p1_6_another_binding_is_leased_the_idle_connection_only_after_its_proof() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    idle_after_a(&rig, &handle);
    rig.world.hold_reuse.store(true, Ordering::SeqCst);
    let b = registered(&handle, "op-b");
    let wait = b.open(exchange_spec()).unwrap().wait();
    let asked = rig.world.reuses();
    assert_eq!(asked.len(), 1, "one proof, for B");
    assert_eq!(asked[0].binding, binding(&b));
    assert!(!wait.is_ready(), "no exchange starts before the proof ends");
    assert_eq!(rig.world.exchanges.load(Ordering::SeqCst), 1);
    rig.world.end_reuse(ReuseVerdict::Authorized);
    rig.progress.run();
    assert!(wait.is_ready());
    let exchange = now(wait).unwrap();
    assert_eq!(rig.world.connects(), 1, "the proven connection served B");
    now(exchange.finish());
    now(b.finish());
}

#[test]
fn p1_6_a_revoked_proof_discards_the_connection_and_opens_a_new_one() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    idle_after_a(&rig, &handle);
    *rig.world.reuse_verdict.lock().unwrap() = Some(ReuseVerdict::Revoked);
    let b = registered(&handle, "op-b");
    let exchange = now(b.open(exchange_spec()).unwrap().wait()).unwrap();
    assert_eq!(rig.world.reuses().len(), 1);
    assert_eq!(
        rig.world.connects(),
        2,
        "B is served by a connection of its own"
    );
    assert_eq!(rig.world.closes(), 1, "the revoked connection is closed");
    now(exchange.finish());
    now(b.finish());
}

#[test]
fn p1_6_an_unprovable_connection_stays_idle_and_the_binding_gets_its_own() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    idle_after_a(&rig, &handle);
    // The port's default: the adapter starts no proof.
    *rig.world.reuse_verdict.lock().unwrap() = None;
    let b = registered(&handle, "op-b");
    let exchange = now(b.open(exchange_spec()).unwrap().wait()).unwrap();
    assert_eq!(rig.world.connects(), 2, "B never gets A's connection");
    assert_eq!(rig.world.closes(), 0, "A's connection is not closed");
    assert_eq!(handle.snapshot().connections.idle, 1);
    now(exchange.finish());
    now(b.finish());
}

#[test]
fn p1_6_an_opening_dropped_during_its_proof_gives_the_connection_back_to_idle() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    idle_after_a(&rig, &handle);
    rig.world.hold_reuse.store(true, Ordering::SeqCst);
    let b = registered(&handle, "op-b");
    let wait = b.open(exchange_spec()).unwrap().wait();
    assert_eq!(rig.world.reuses().len(), 1);
    drop(wait);
    assert_eq!(
        handle.snapshot().connections.idle,
        1,
        "the clean connection is idle again"
    );
    assert_eq!(handle.snapshot().connections.leased, 0);
    // The proof's late end changes nothing.
    rig.world.end_reuse(ReuseVerdict::Authorized);
    rig.progress.run();
    assert_eq!(rig.world.exchanges.load(Ordering::SeqCst), 1);
    now(b.finish());
}
