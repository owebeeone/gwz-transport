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

// Step 1.6 remediation round 1 (dev-docs/GwzConnectionApiContract-Step1.6-RemPlan.md).

#[test]
fn p1_6_rem_a_late_verdict_of_a_dropped_openings_proof_serves_no_other_binding() {
    // C-P2-1 = S-P2-1: B's proof is held and B's opening is dropped; C then leases the same connection and waits for
    // its own proof. B's late verdict serves nothing; C's own verdict decides C.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    idle_after_a(&rig, &handle);
    rig.world.hold_reuse.store(true, Ordering::SeqCst);
    let b = registered(&handle, "op-b");
    let wait_b = b.open(exchange_spec()).unwrap().wait();
    let proof_b = rig.world.reuses()[0];
    drop(wait_b);
    assert_eq!(
        rig.world.cancelled_reuses(),
        vec![proof_b.proof],
        "the dropped opening's proof is cancelled"
    );
    let c = registered(&handle, "op-c");
    let mut wait_c = c.open(exchange_spec()).unwrap().wait();
    let proof_c = rig.world.reuses()[1];
    assert_eq!(proof_c.binding, binding(&c));
    assert_eq!(proof_c.connection, proof_b.connection);
    assert_ne!(proof_c.proof, proof_b.proof);
    rig.world.end_reuse_of(proof_b, ReuseVerdict::Authorized);
    rig.progress.run();
    assert!(!wait_c.is_ready(), "B's verdict served C");
    assert_eq!(rig.world.exchanges.load(Ordering::SeqCst), 1);
    rig.world.hold_reuse.store(false, Ordering::SeqCst);
    rig.world.end_reuse_of(proof_c, ReuseVerdict::Revoked);
    rig.progress.run();
    let exchange = now(&mut wait_c).unwrap();
    assert!(
        !exchange.reused(),
        "C's own Revoked gave C a new connection"
    );
    assert_eq!(rig.world.connects(), 2);
    now(exchange.finish());
    now(b.finish());
    now(c.finish());
}

#[test]
fn p1_6_rem_a_binding_reuses_its_own_idle_connection_beside_one_it_cannot_prove() {
    // S-P2-2: A's connection is one B cannot prove (the port's default starts no proof). B's first open is bounced off
    // it to a connection of its own; B's second open is offered A's first again, and then B's own.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    idle_after_a(&rig, &handle);
    *rig.world.reuse_verdict.lock().unwrap() = None;
    let b = registered(&handle, "op-b");
    let first = now(b.open(exchange_spec()).unwrap().wait()).unwrap();
    assert!(!first.reused());
    now(first.finish());
    let second = now(b.open(exchange_spec()).unwrap().wait()).unwrap();
    assert!(second.reused(), "B's second open reuses B's own connection");
    assert_eq!(
        rig.world.connects(),
        2,
        "one connect for B, not one per open"
    );
    assert_eq!(rig.world.closes(), 0, "A's connection is kept for A");
    now(second.finish());
    now(b.finish());
}

#[test]
fn p1_6_rem_a_bounced_connection_expires_from_its_last_real_release() {
    // S-P2-2: bouncing A's connection off B's opens never restarts its idle clock.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    idle_after_a(&rig, &handle);
    *rig.world.reuse_verdict.lock().unwrap() = None;
    let idle_ms = gwz_transport::pool::Config::default().idle_timeout_ms;
    rig.clock.advance(idle_ms / 2);
    let b = registered(&handle, "op-b");
    now(now(b.open(exchange_spec()).unwrap().wait())
        .unwrap()
        .finish());
    assert_eq!(handle.snapshot().connections.idle, 2);
    rig.clock.advance(idle_ms / 2);
    rig.progress.run();
    assert_eq!(
        rig.world.closes(),
        1,
        "A's connection expired 60 s after A released it"
    );
    assert_eq!(
        handle.snapshot().connections.idle,
        1,
        "B's own connection is still idle"
    );
    now(b.finish());
}

#[test]
fn p1_6_rem_a_misfit_lease_another_binding_last_used_is_given_back_with_no_proof() {
    // C-P3-4 (scope fit first): an HTTPS lease that does not fit the opening's scope is handled by 1.5's rule before
    // any cross-binding proof is asked for.
    use crate::fakes::{auth, https_spec, prepared_with};
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let register = |name: &str| {
        now(handle
            .admit(prepared_with(name, 1, auth(1, "a")))
            .unwrap()
            .acquire())
        .unwrap()
        .register()
    };
    let a = register("op-a");
    now(now(a.open(https_spec()).unwrap().wait()).unwrap().finish());
    now(a.finish());
    let b = register("op-b");
    b.instance_for_test()
        .rescope_for_test(crate::AuthScope::new([9; 32]));
    let exchange = now(b.open(https_spec()).unwrap().wait()).unwrap();
    assert!(!exchange.reused());
    assert!(
        rig.world.reuses().is_empty(),
        "no proof is asked for a lease that does not fit"
    );
    now(exchange.finish());
    now(b.finish());
}

#[test]
fn p1_6_rem_sealing_the_host_during_a_proof_closes_the_opening_and_ignores_the_late_verdict() {
    // C-P3-4: the host is sealed while B's proof is held.
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    idle_after_a(&rig, &handle);
    rig.world.hold_reuse.store(true, Ordering::SeqCst);
    let b = registered(&handle, "op-b");
    let mut wait = b.open(exchange_spec()).unwrap().wait();
    let proof = rig.world.reuses()[0];
    let close = owner.close();
    assert!(matches!(
        crate::fakes::poll_once(&mut wait),
        std::task::Poll::Ready(Err(crate::OpenFailure::Closing(_)))
    ));
    assert_eq!(rig.world.cancelled_reuses(), vec![proof.proof]);
    rig.world.end_reuse_of(proof, ReuseVerdict::Authorized);
    rig.progress.run();
    assert_eq!(
        rig.world.exchanges.load(Ordering::SeqCst),
        1,
        "the late verdict started nothing"
    );
    drop(close);
    drop(wait);
}

// Merge fix-up of the two accepted-review findings (dev-docs/GwzConnectionApiContract-Step1.5-ReviewState-2.md and
// -Step1.6-ReviewState-2.md, N-P3-1 each).

#[test]
fn p1_5_n_p3_1_a_lease_with_no_setup_record_is_discarded_and_never_revived() {
    // The adapter reports IdleClosed for a connection the pool has just leased to B's opening: the pool refuses it (the
    // lease keeps the connection) but the host forgets its setup record. The lease is discarded, not handed back
    // reusable, and the opening asks again without counting a misfit.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    idle_after_a(&rig, &handle);
    rig.world.lose_connections();
    let b = registered(&handle, "op-b");
    let exchange = now(b.open(exchange_spec()).unwrap().wait()).unwrap();
    assert!(!exchange.reused(), "B is served by a connection of its own");
    assert_eq!(rig.world.connects(), 2);
    assert_eq!(rig.world.closes(), 1, "the lost connection is closed once");
    assert_eq!(b.instance_for_test().misfits_for_test(), 0);
    now(exchange.finish());
    now(b.finish());
    // The next opening of the same scope is offered B's connection, not a revived dead one, and no misfit comes of it.
    let c = registered(&handle, "op-c");
    let exchange = now(c.open(exchange_spec()).unwrap().wait()).unwrap();
    assert!(exchange.reused());
    assert_eq!(rig.world.connects(), 2);
    assert_eq!(c.instance_for_test().misfits_for_test(), 0);
    now(exchange.finish());
    now(c.finish());
}

#[test]
fn p1_6_n_p3_1_a_revoked_proof_asks_fresh_before_the_siblings_idle_closed_arrives() {
    // Two idle connections of one key, both last used by A. B's proof for the first is revoked and the adapter reports
    // the second's IdleClosed only after that: B must not be offered the second (a second proof, and for the real
    // adapter another confirmation prompt for a key just denied), whatever the completion order.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let a = registered(&handle, "op-a");
    let first = now(a.open(exchange_spec()).unwrap().wait()).unwrap();
    let second = now(a.open(exchange_spec()).unwrap().wait()).unwrap();
    now(first.finish());
    now(second.finish());
    now(a.finish());
    assert_eq!(handle.snapshot().connections.idle, 2);
    let held = rig.world.connected_ids();
    rig.world.hold_reuse.store(true, Ordering::SeqCst);
    let b = registered(&handle, "op-b");
    let mut wait = b.open(exchange_spec()).unwrap().wait();
    let proof = rig.world.reuses()[0];
    rig.world.end_reuse_of(proof, ReuseVerdict::Revoked);
    rig.progress.run();
    assert_eq!(
        rig.world.reuses().len(),
        1,
        "the retry took no idle connection of the denied key"
    );
    let sibling = held.into_iter().find(|id| *id != proof.connection).unwrap();
    rig.world.idle_closed_of(sibling);
    rig.progress.run();
    let exchange = now(&mut wait).unwrap();
    assert!(!exchange.reused());
    assert_eq!(rig.world.connects(), 3, "B has a connection of its own");
    assert_eq!(rig.world.reuses().len(), 1);
    assert_eq!(rig.world.closes(), 1, "the revoked connection is closed");
    now(exchange.finish());
    now(b.finish());
}
