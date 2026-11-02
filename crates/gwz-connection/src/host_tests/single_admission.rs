//! A credential-retaining provider is single-admission (contract section 10; GwzHttpsAuthModel 3(b), rows H1e 1-4): the
//! claim its `AuthContext` and every clone share is taken by `admit`, freed when that admission never registers and
//! consumed for good when it does.
use super::support::registered_with;
use crate::{
    AdmissionError, AuthContext, AuthScope, HostLimits, Registration,
    fakes::{Rig, auth, https_spec, now, poll_once, prepared_with},
};
use std::{
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

fn single(provider: &Arc<String>) -> AuthContext {
    AuthContext::single_admission(64, AuthScope::new([1; 32]), provider.clone())
}
fn refused(result: Result<crate::Admission, AdmissionError>) -> bool {
    matches!(result, Err(AdmissionError::InvalidInput))
}

#[test]
fn h1e_1_a_second_admission_of_a_cloned_context_is_refused_while_the_first_waits_and_after_it_registered()
 {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let provider = Arc::new(String::from("a"));
    let context = single(&provider);
    let first = handle
        .admit(prepared_with("op-1", 1, context.clone()))
        .unwrap();
    let before = handle.snapshot();
    assert_eq!(before.waiting_admissions, 1);
    assert!(
        refused(handle.admit(prepared_with("op-2", 1, context.clone()))),
        "the first admission is still waiting"
    );
    assert_eq!(
        handle.snapshot(),
        before,
        "the refusal reserved no record and attached no instance"
    );
    let operation = now(first.acquire()).unwrap().register();
    assert!(matches!(
        operation.registration(),
        Registration::Registered(_)
    ));
    let before = handle.snapshot();
    assert!(
        refused(handle.admit(prepared_with("op-3", 1, context.clone()))),
        "the first admission registered"
    );
    assert_eq!(handle.snapshot(), before);
    now(operation.finish());
    assert!(
        refused(handle.admit(prepared_with("op-4", 1, context))),
        "a registered admission consumed the claim for good"
    );
    assert_eq!(handle.snapshot().bindings, 0);
    assert_eq!(handle.snapshot().waiting_admissions, 0);
    assert_eq!(
        (
            rig.world.created.load(Ordering::SeqCst),
            rig.world.connects()
        ),
        (1, 0),
        "only the first admission's instance, and no effect"
    );
}

#[test]
fn h1e_2_an_admission_that_never_registers_frees_the_claim_and_a_plain_context_is_still_shared() {
    let rig = Rig::new();
    let limits = HostLimits {
        active_slots: 1,
        waiting_admissions: 1,
        ..HostLimits::default()
    };
    let (_owner, handle) = crate::build(rig.config(limits), rig.ports(1)).unwrap();
    let provider = Arc::new(String::from("a"));
    let context = single(&provider);
    let admit = |name: &str| handle.admit(prepared_with(name, 1, context.clone()));
    let holder = registered_with(&handle, "holder", 1, auth(2, "b"));
    // Refused: the queue is full.
    let filler = handle
        .admit(prepared_with("filler", 1, auth(2, "b")))
        .unwrap();
    assert!(matches!(admit("op-1"), Err(AdmissionError::QueueFull)));
    drop(filler);
    // Dropped while it waits.
    drop(admit("op-2").unwrap());
    // Timed out: freed when the bound passes, whether or not its future is dropped.
    let mut timed_out = admit("op-3").unwrap().acquire();
    assert!(poll_once(&mut timed_out).is_pending());
    rig.clock.advance(limits.admission_bound_ms);
    assert!(matches!(
        poll_once(&mut timed_out),
        Poll::Ready(Err(AdmissionError::AdmissionTimeout))
    ));
    // Its permit dropped unregistered.
    let mut waiting = admit("op-4").unwrap().acquire();
    assert!(poll_once(&mut waiting).is_pending());
    now(holder.finish());
    let Poll::Ready(Ok(permit)) = poll_once(&mut waiting) else {
        panic!("the slot is free");
    };
    drop(permit);
    drop(waiting);
    // After each of those, the retry registers.
    let operation = now(admit("op-5").unwrap().acquire()).unwrap().register();
    assert!(matches!(
        operation.registration(),
        Registration::Registered(_)
    ));
    now(operation.finish());
    drop(timed_out);
    // A plain provider is shared as documented (contract section 10): its clones are admitted side by side.
    let plain = auth(3, "c");
    let a = handle.admit(prepared_with("plain-a", 1, plain.clone()));
    let b = handle.admit(prepared_with("plain-b", 1, plain.clone()));
    assert!(a.is_ok());
    assert!(
        matches!(b, Err(AdmissionError::QueueFull)),
        "only the queue limits it"
    );
    drop(a);
    assert!(handle.admit(prepared_with("plain-c", 1, plain)).is_ok());
}

#[test]
fn h1e_3_with_a_connect_in_flight_the_provider_is_released_within_the_connect_bound_after_finish() {
    let rig = Rig::new();
    rig.world.asynchronous.store(true, Ordering::SeqCst);
    rig.world.hold_connect_auth.store(true, Ordering::SeqCst);
    let (_owner, handle) = rig.host(1);
    let provider = Arc::new(String::from("a"));
    let operation = registered_with(&handle, "op", 1, single(&provider));
    let mut opening = operation.open(https_spec()).unwrap().wait();
    assert!(poll_once(&mut opening).is_pending());
    rig.progress.run();
    assert_eq!(rig.world.connects(), 1);
    assert_eq!(
        Arc::strong_count(&provider),
        4,
        "the test's, the binding's, the pending opening's and the in-flight connect's"
    );
    drop(opening);
    now(operation.finish());
    rig.progress.run();
    let bound = gwz_transport::pool::Config::default().connect_timeout_ms;
    rig.clock.advance(bound);
    rig.progress.run();
    assert_eq!(
        Arc::strong_count(&provider),
        1,
        "only the test's own handle is left once the connect bound has passed"
    );
}

#[test]
fn h1e_4_two_concurrent_admissions_of_clones_the_second_is_refused_at_once_and_register_stays_infallible()
 {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let provider = Arc::new(String::from("a"));
    let context = single(&provider);
    let first = handle
        .admit(prepared_with("op-1", 1, context.clone()))
        .unwrap();
    let second = handle.admit(prepared_with("op-2", 1, context.clone()));
    assert!(refused(second), "refused at admit, before any wait");
    let permit = now(first.acquire()).unwrap();
    // Nothing between the permit and `register` can take the claim from it.
    assert!(refused(handle.admit(prepared_with("op-3", 1, context))));
    let operation = permit.register();
    assert!(matches!(
        operation.registration(),
        Registration::Registered(_)
    ));
    now(operation.finish());
    assert_eq!(Arc::strong_count(&provider), 1);
}
