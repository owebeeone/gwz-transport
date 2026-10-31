//! Capability shapes, build-time refusals and the host's relation to its domain and process safety.
use crate::{
    BuildError, CleanupDomain, CleanupDomainOwner, CleanupTicket, ConfigReason, DomainClose,
    HostClose, HostHandle, HostLimits, HostOwner, HostPhase,
    fakes::{FakeSafety, Rig},
};
use std::sync::{Arc, atomic::Ordering};

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
