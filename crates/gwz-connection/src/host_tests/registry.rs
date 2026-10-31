//! The endpoint-instance registry: the instance bound, factory inputs, digest collisions and retirement.
use super::support::registered_with;
use crate::{
    AttachError, CleanupStatus, EndpointProfile, EndpointRoute, EndpointSpec, HostHandle,
    HostLimits, OperationPolicy, PreparedOperation,
    fakes::{Rig, auth, exchange_spec, now, prepared_with, spec},
};
use std::{
    sync::{Arc, atomic::Ordering},
    task::Poll,
};

#[test]
fn the_instance_bound_and_a_factory_failure_are_reported_with_their_cleanup() {
    let rig = Rig::new();
    let limits = HostLimits {
        endpoint_instances: 2,
        ..HostLimits::default()
    };
    let (_owner, handle) = crate::build(rig.config(limits), rig.ports(1)).unwrap();
    // Both instances are bound (an operation each), so none can be retired to make room (Code P2-3).
    let bound = [
        registered_with(&handle, "op-0", 0, auth(0, "a")),
        registered_with(&handle, "op-1", 1, auth(0, "a")),
    ];
    assert!(matches!(
        handle.attach_instance(spec(2)),
        Err(AttachError::InstanceLimit)
    ));
    assert_eq!(rig.world.created.load(Ordering::SeqCst), 2);
    drop(bound);
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

// Phase 1 remediation R1 (dev-docs/GwzPhase1-RemPlan.md, Code P2-3; contract sections 6 and 10): at the endpoint-instance
// limit an instance with no binding, opening or lease is retired, least recently used first, as one registry transition,
// and its adapter is disposed under the cleanup domain.

/// `count` instances (profiles `0..count`), each made by one operation that ran an exchange and ended: unbound, with an
/// idle connection. The instances are returned in the order they were made.
fn unbound_instances(handle: &HostHandle, count: u8) -> Vec<Arc<crate::instance::Instance>> {
    (0..count)
        .map(|n| {
            let operation = registered_with(handle, "op", n, auth(0, "a"));
            let instance = operation.instance_for_test();
            let exchange = now(operation.open(exchange_spec()).unwrap().wait()).unwrap();
            now(exchange.finish());
            now(operation.finish());
            instance
        })
        .collect()
}
fn adapter_kept(instance: &crate::instance::Instance) -> bool {
    instance.lock().adapter.is_some()
}

#[test]
fn the_seventeenth_profile_is_admitted_by_retiring_the_oldest_unbound_instance() {
    // The Phase 1 Code review's probe: sixteen unbound instances, then a seventeenth profile.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let made = unbound_instances(&handle, 16);
    assert_eq!(handle.snapshot().endpoint_instances, 16);
    assert_eq!(rig.world.closes(), 0);
    handle.attach_instance(spec(16)).unwrap();
    assert_eq!(handle.snapshot().endpoint_instances, 16, "still the limit");
    assert_eq!(rig.world.created.load(Ordering::SeqCst), 17);
    assert!(!adapter_kept(&made[0]), "the oldest left the registry");
    assert!(made[1..].iter().all(|instance| adapter_kept(instance)));
    assert_eq!(rig.world.closes(), 1, "its idle connection was closed");
    // Its adapter is disposed under the cleanup domain: the retainer owns it until the disposal is confirmed.
    assert_eq!(handle.snapshot().retained_cleanup, 1);
    assert_eq!(rig.world.disposed(), 0);
    rig.retention.step_all();
    assert_eq!(rig.world.disposed(), 1, "the retired adapter was disposed");
    assert_eq!(handle.snapshot().retained_cleanup, 0);
    // A new profile each time keeps working however long the host lives.
    for n in 17..60 {
        handle.attach_instance(spec(n)).unwrap();
        rig.retention.step_all();
    }
    assert_eq!(handle.snapshot().endpoint_instances, 16);
    assert_eq!(rig.world.disposed(), 44);
}

#[test]
fn the_least_recently_attached_unbound_instance_goes_first_and_a_bound_one_never_goes() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let made = unbound_instances(&handle, 16);
    // Profile 0 is used again, so profile 1 is now the oldest; profile 1 is then bound, so profile 2 is the victim.
    let again = registered_with(&handle, "op", 0, auth(0, "a"));
    assert!(Arc::ptr_eq(&again.instance_for_test(), &made[0]));
    let bound = registered_with(&handle, "op", 1, auth(0, "a"));
    drop(again);
    handle.attach_instance(spec(16)).unwrap();
    assert!(adapter_kept(&made[0]) && adapter_kept(&made[1]));
    assert!(!adapter_kept(&made[2]), "the oldest unbound instance went");
    assert!(made[3..].iter().all(|instance| adapter_kept(instance)));
    drop(bound);
}

#[test]
fn an_instance_whose_exchange_is_still_settling_is_not_retired_until_it_settles() {
    // The operation and its exchange are gone, but the adapter has not confirmed the exchange's end: the lease is still
    // out, so the instance is not unbound.
    let rig = Rig::new();
    let limits = HostLimits {
        endpoint_instances: 1,
        ..HostLimits::default()
    };
    let (_owner, handle) = crate::build(rig.config(limits), rig.ports(1)).unwrap();
    rig.world.hold_finish.store(true, Ordering::SeqCst);
    let operation = registered_with(&handle, "op", 0, auth(0, "a"));
    let instance = operation.instance_for_test();
    let exchange = now(operation.open(exchange_spec()).unwrap().wait()).unwrap();
    let settling = exchange.finish();
    now(operation.finish());
    assert_eq!(handle.snapshot().connections.leased, 1);
    assert!(matches!(
        handle.attach_instance(spec(1)),
        Err(AttachError::InstanceLimit)
    ));
    rig.world.confirm_finishes();
    rig.progress.run();
    assert_eq!(handle.snapshot().connections.leased, 0);
    drop(settling);
    handle.attach_instance(spec(1)).unwrap();
    assert!(!adapter_kept(&instance));
}

#[test]
fn an_attach_that_holds_an_instance_keeps_it_when_another_profile_needs_the_room() {
    // Attach-versus-retire (contract section 6): profile 0 is the oldest unbound instance, and an admission has attached
    // it but not yet registered. A seventeenth profile must pick another victim, and the admission gets its instance.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let made = unbound_instances(&handle, 16);
    let permit = now(handle
        .admit(prepared_with("late", 0, auth(0, "a")))
        .unwrap()
        .acquire())
    .unwrap();
    // The other fifteen are used after it, so the held one is now the least recently used of all.
    for n in 1..16 {
        handle.attach_instance(spec(n)).unwrap();
    }
    handle.attach_instance(spec(16)).unwrap();
    assert!(adapter_kept(&made[0]), "the held instance was not retired");
    assert!(!adapter_kept(&made[1]), "the next oldest was");
    let operation = permit.register();
    assert!(Arc::ptr_eq(&operation.instance_for_test(), &made[0]));
    let exchange = now(operation.open(exchange_spec()).unwrap().wait()).unwrap();
    assert!(exchange.reused(), "it still has the connection it kept");
    now(exchange.finish());
    now(operation.finish());
}

#[test]
fn an_attach_after_the_retirement_gets_a_fresh_instance_never_the_disposed_one() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let made = unbound_instances(&handle, 16);
    handle.attach_instance(spec(16)).unwrap();
    assert!(!adapter_kept(&made[0]));
    let operation = registered_with(&handle, "op", 0, auth(0, "a"));
    let fresh = operation.instance_for_test();
    assert!(!Arc::ptr_eq(&fresh, &made[0]), "not the retired instance");
    assert!(adapter_kept(&fresh));
    let exchange = now(operation.open(exchange_spec()).unwrap().wait()).unwrap();
    assert!(
        !exchange.reused(),
        "none of the retired instance's connections"
    );
    now(exchange.finish());
    now(operation.finish());
    assert_eq!(handle.snapshot().endpoint_instances, 16);
}

#[test]
fn concurrent_attaches_and_retirements_never_hand_out_a_retired_instance() {
    // Four threads admit and register on a few profiles while four others keep presenting new profiles at the limit:
    // every instance an admission holds still has its adapter for as long as the admission holds it.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    unbound_instances(&handle, 16);
    std::thread::scope(|scope| {
        for worker in 0..4u8 {
            let handle = handle.clone();
            scope.spawn(move || {
                for round in 0..150u8 {
                    let mut acquire = std::pin::pin!(
                        handle
                            .admit(prepared_with("racer", worker * 4 + round % 4, auth(0, "a")))
                            .unwrap()
                            .acquire()
                    );
                    // The queue is first come first served, so an admission may have to wait for the one before it.
                    let permit = loop {
                        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
                        if let Poll::Ready(permit) = acquire.as_mut().poll(&mut cx) {
                            break permit;
                        }
                        std::thread::yield_now();
                    };
                    let Ok(permit) = permit else {
                        continue;
                    };
                    let operation = permit.register();
                    assert!(adapter_kept(&operation.instance_for_test()));
                    std::thread::yield_now();
                    assert!(adapter_kept(&operation.instance_for_test()));
                    now(operation.finish());
                }
            });
        }
        for worker in 0..4u8 {
            let handle = handle.clone();
            scope.spawn(move || {
                for round in 0..150u8 {
                    // Refused only when every instance is bound at that moment.
                    let _ = handle.attach_instance(spec(32 + worker * 8 + round % 8));
                }
            });
        }
    });
    assert!(handle.snapshot().endpoint_instances <= 16);
}
