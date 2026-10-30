//! Operation bindings against fakes only (step 1.2 of dev-docs/GwzTransportReleaseConvergencePlan.md; contract
//! sections 4, 5, 6, 8 and 13.1): admission, registration, one binding (two muxes) per operation, leases from the
//! host's pools, cancellation, finish and the race with host and domain close.
use crate::fakes::{Rig, exchange_spec, now, poll_once, prepared};
use crate::{
    AdmissionError, AdmissionPermit, BuildError, CancelState, CleanupStatus, Direction, HostLimits,
    OpenError, OpenFailure, Operation, OperationCancel, OperationEnd, PreparedOperation,
    Registration, Routed, TerminalReason,
};
use gwz_transport::protocol::{Disposition, Envelope, MessageKind};
use std::{sync::atomic::Ordering, task::Poll};

fn registered(rig: &Rig, handle: &crate::HostHandle, name: &str, kind: u8) -> Operation {
    let _ = rig;
    let permit: AdmissionPermit =
        now(handle.admit(prepared(name, kind)).unwrap().acquire()).unwrap();
    permit.register()
}

fn binding_id(operation: &Operation) -> crate::BindingId {
    match operation.registration() {
        Registration::Registered(id) => id,
        other => panic!("not registered: {other:?}"),
    }
}

/// A frame as binding `session` would carry it: only its session names the binding.
fn late_frame(session: &str) -> (String, Envelope) {
    (
        "x".into(),
        Envelope {
            version: 2,
            session_id: session.into(),
            stream_id: 5,
            kind: MessageKind::Data,
            ..Default::default()
        },
    )
}

#[test]
fn the_operation_types_have_the_shapes_the_contract_names() {
    fn send<T: Send>() {}
    fn clone_send_sync<T: Clone + Send + Sync>() {}
    send::<crate::Admission>();
    send::<AdmissionPermit>();
    send::<Operation>();
    send::<crate::Opening>();
    send::<crate::Exchange>();
    send::<crate::OperationFinish>();
    clone_send_sync::<OperationCancel>();
    clone_send_sync::<PreparedOperation>();
}

#[test]
fn finish_a_then_b_reuses_the_idle_connection() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let a = registered(&rig, &handle, "op-a", 1);
    let mut exchange = now(a.open(exchange_spec()).unwrap().wait()).unwrap();
    assert_eq!(exchange.stream().write(b"ping").unwrap(), 4);
    let mut buffer = [0u8; 8];
    assert_eq!(exchange.stream().read(&mut buffer).unwrap(), 4);
    assert_eq!(&buffer[..4], b"ping");
    assert_eq!(handle.snapshot().connections.leased, 1);
    let ended = now(exchange.finish());
    assert_eq!(ended.disposition, Disposition::Reusable);
    assert_eq!(handle.snapshot().connections.idle, 1);
    let end_a: OperationEnd = now(a.finish());
    assert!(matches!(end_a.registration, Registration::Registered(_)));
    assert!(matches!(end_a.cleanup, CleanupStatus::Complete));
    assert_eq!(handle.snapshot().bindings, 0);
    // Finishing A closed neither the host nor the connection: B takes the idle one.
    let b = registered(&rig, &handle, "op-b", 1);
    let exchange = now(b.open(exchange_spec()).unwrap().wait()).unwrap();
    assert_eq!(
        rig.world.connects(),
        1,
        "one physical connection served both operations"
    );
    assert_eq!(rig.world.exchanges.load(Ordering::SeqCst), 2);
    let used = rig.world.exchange_connections.lock().unwrap().clone();
    assert_eq!(used.len(), 2);
    assert_eq!(
        used[0], used[1],
        "both exchanges ran on the one fake connection"
    );
    assert_eq!(handle.snapshot().connections.leased, 1);
    assert_eq!(handle.snapshot().connections.idle, 0);
    now(exchange.finish());
    now(b.finish());
    assert_eq!(handle.snapshot().connections.idle, 1);
}

#[test]
fn r3_a_and_b_overlap_a_is_cancelled_b_completes_and_c_reuses_the_idle_connection() {
    // Plan section 3, R3: creations <= 2; A Cancelled, B Ok; C reuses (`reused: true`) the connection B left idle.
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let a = registered(&rig, &handle, "op-a", 1);
    let b = registered(&rig, &handle, "op-b", 1);
    let mut exchange_a = now(a.open(exchange_spec()).unwrap().wait()).unwrap();
    let mut exchange_b = now(b.open(exchange_spec()).unwrap().wait()).unwrap();
    assert_eq!(
        rig.world.connects(),
        2,
        "A and B overlapped on two connections"
    );
    let (conn_a, conn_b) = {
        let used = rig.world.exchange_connections.lock().unwrap();
        (used[0], used[1])
    };
    assert_ne!(conn_a, conn_b);
    assert_eq!(a.cancel_handle().cancel(), CancelState::Cancelled);
    assert!(exchange_a.stream().write(b"late").is_err());
    // B completes untouched by A's cancellation.
    assert_eq!(exchange_b.stream().write(b"b-data").unwrap(), 6);
    let mut buffer = [0u8; 8];
    assert_eq!(exchange_b.stream().read(&mut buffer).unwrap(), 6);
    assert_eq!(&buffer[..6], b"b-data");
    assert_eq!(now(exchange_a.finish()).disposition, Disposition::Discarded);
    assert_eq!(now(exchange_b.finish()).disposition, Disposition::Reusable);
    assert!(now(a.finish()).cancelled);
    assert!(!now(b.finish()).cancelled);
    assert_eq!(rig.world.closes(), 1, "only A's connection was closed");
    assert_eq!(handle.snapshot().connections.idle, 1);
    assert_eq!(rig.world.connected_ids(), vec![conn_b]);
    // C is admitted afterwards and takes B's idle connection: no third connect, and its exchange runs on conn_b.
    let c = registered(&rig, &handle, "op-c", 1);
    let mut exchange_c = now(c.open(exchange_spec()).unwrap().wait()).unwrap();
    assert!(exchange_c.reused(), "C reused the idle connection");
    assert_eq!(rig.world.connects(), 2, "no connection was made for C");
    assert_eq!(rig.world.exchange_connections.lock().unwrap()[2], conn_b);
    assert_eq!(exchange_c.stream().write(b"c-data").unwrap(), 6);
    assert_eq!(exchange_c.stream().read(&mut buffer).unwrap(), 6);
    assert_eq!(&buffer[..6], b"c-data");
    assert_eq!(now(exchange_c.finish()).disposition, Disposition::Reusable);
    assert!(!now(c.finish()).cancelled);
    assert_eq!(handle.snapshot().connections.idle, 1);
    assert_eq!(rig.world.closes(), 1);
}

#[test]
fn a_different_profile_never_reuses_another_profiles_connection() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    for (name, kind) in [("op-a", 1), ("op-b", 2)] {
        let op = registered(&rig, &handle, name, kind);
        let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
        now(exchange.finish());
        now(op.finish());
    }
    assert_eq!(rig.world.connects(), 2);
    assert_eq!(handle.snapshot().endpoint_instances, 2);
    // The same profile again coalesces onto its instance.
    let op = registered(&rig, &handle, "op-c", 1);
    let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    now(exchange.finish());
    now(op.finish());
    assert_eq!(rig.world.connects(), 2);
    assert_eq!(handle.snapshot().endpoint_instances, 2);
}

#[test]
fn cancelling_a_leaves_bs_exchange_intact() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let a = registered(&rig, &handle, "op-a", 1);
    let b = registered(&rig, &handle, "op-b", 1);
    let mut exchange_a = now(a.open(exchange_spec()).unwrap().wait()).unwrap();
    let mut exchange_b = now(b.open(exchange_spec()).unwrap().wait()).unwrap();
    assert_eq!(rig.world.connects(), 2);
    let cancel = a.cancel_handle();
    assert_eq!(cancel.cancel(), CancelState::Cancelled);
    assert_eq!(cancel.cancel(), CancelState::AlreadyCancelled);
    // A's stream fails with a named error; its connection is closed; B's is untouched.
    assert!(exchange_a.stream().write(b"late").is_err());
    assert_eq!(rig.world.closes(), 1);
    assert_eq!(exchange_b.stream().write(b"still").unwrap(), 5);
    let mut buffer = [0u8; 8];
    assert_eq!(exchange_b.stream().read(&mut buffer).unwrap(), 5);
    assert_eq!(&buffer[..5], b"still");
    assert!(matches!(a.open(exchange_spec()), Err(OpenError::Cancelled)));
    let ended = now(exchange_a.finish());
    assert_eq!(ended.disposition, Disposition::Discarded);
    assert_eq!(now(exchange_b.finish()).disposition, Disposition::Reusable);
    let end_a = now(a.finish());
    assert!(end_a.cancelled);
    assert!(!now(b.finish()).cancelled);
    assert_eq!(handle.snapshot().connections.idle, 1);
}

#[test]
fn a_cancel_handle_is_weak_and_idempotent() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let op = registered(&rig, &handle, "op-a", 1);
    let cancel = op.cancel_handle();
    now(op.finish());
    assert_eq!(cancel.cancel(), CancelState::Gone);
}

#[test]
fn three_hundred_bindings_on_one_host_never_meet_a_256_bound_on_either_side() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let mut sessions = std::collections::BTreeSet::new();
    for i in 0..300u32 {
        let op = registered(&rig, &handle, &format!("op-{i}"), 1);
        let id = binding_id(&op);
        assert!(sessions.insert(id), "binding ids are never reused");
        let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
        now(exchange.finish());
        now(op.finish());
    }
    assert_eq!(handle.snapshot().bindings, 0);
    assert_eq!(
        rig.world.connects(),
        1,
        "every one of them reused the one connection"
    );
}

#[test]
fn the_registration_limit_is_the_bindings_own_and_typed() {
    let rig = Rig::new();
    let limits = HostLimits {
        binding_registrations: 4,
        ..HostLimits::default()
    };
    let (_owner, handle) = crate::build(rig.config(limits), rig.ports(1)).unwrap();
    let op = registered(&rig, &handle, "op-a", 1);
    // One registration went to the bootstrap; three exchanges fit, the fourth is the binding's limit.
    for _ in 0..3 {
        let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
        now(exchange.finish());
    }
    assert!(matches!(
        op.open(exchange_spec()),
        Err(OpenError::BindingLimit)
    ));
    // The next operation is a fresh binding with its own count.
    let next = registered(&rig, &handle, "op-b", 1);
    let exchange = now(next.open(exchange_spec()).unwrap().wait()).unwrap();
    now(exchange.finish());
}

#[test]
fn a_late_event_of_binding_g_is_rejected_by_g_plus_one_and_by_g_itself_once_retired() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let g = registered(&rig, &handle, "op-g", 1);
    let id_g = binding_id(&g);
    let session_g = g.binding_session();
    now(g.finish());
    let h = registered(&rig, &handle, "op-h", 1);
    let id_h = binding_id(&h);
    let carrier = handle.carrier();
    let before = handle.snapshot();
    let late = late_frame(&session_g);
    // g+1 names a different session: the frame is not its own.
    assert_eq!(
        handle.route(carrier, id_h, Direction::ToEndpoint, &late),
        Routed::Discarded(crate::Discard::WrongSession)
    );
    assert_eq!(
        handle.route(carrier, id_h, Direction::ToDriver, &late),
        Routed::Discarded(crate::Discard::WrongSession)
    );
    // g is retired: nothing is created for it.
    assert_eq!(
        handle.route(carrier, id_g, Direction::ToEndpoint, &late),
        Routed::Discarded(crate::Discard::Retired)
    );
    assert_eq!(handle.snapshot(), before);
    assert_eq!(h.binding_streams(), (0, 0));
}

#[test]
fn frames_under_binding_bs_id_arriving_on_carrier_a_are_discarded_with_no_state_created() {
    let rig_a = Rig::new();
    let rig_b = Rig::new();
    let (_owner_a, host_a) = rig_a.host(1);
    let (_owner_b, host_b) = rig_b.host(2);
    let op_b = registered(&rig_b, &host_b, "op-b", 1);
    let id_b = binding_id(&op_b);
    let frame = late_frame(&op_b.binding_session());
    let before = host_a.snapshot();
    // B's binding id, on A's carrier.
    assert_eq!(
        host_a.route(host_a.carrier(), id_b, Direction::ToEndpoint, &frame),
        Routed::Discarded(crate::Discard::ForeignHost)
    );
    // A binding of A, but on B's carrier.
    let op_a = registered(&rig_a, &host_a, "op-a", 1);
    let id_a = binding_id(&op_a);
    let frame_a = late_frame(&op_a.binding_session());
    assert_eq!(
        host_a.route(host_b.carrier(), id_a, Direction::ToEndpoint, &frame_a),
        Routed::Discarded(crate::Discard::ForeignCarrier)
    );
    assert_eq!(host_a.snapshot().bindings, 1);
    assert_eq!(before.bindings, 0);
    assert_eq!(op_a.binding_streams(), (0, 0));
    assert_eq!(
        host_b.snapshot().bindings,
        1,
        "B's own binding is untouched too"
    );
}

#[test]
fn register_after_host_close_is_a_terminal_operation_with_no_effect() {
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    let permit = now(handle.admit(prepared("op-a", 1)).unwrap().acquire()).unwrap();
    assert_eq!(handle.snapshot().active_operations, 1);
    let mut close = owner.close();
    // Close revoked the reserved claim and released its reservation, exactly once, before register.
    assert_eq!(handle.snapshot().active_operations, 0);
    let op = permit.register();
    assert_eq!(
        op.registration(),
        Registration::NeverRegistered(TerminalReason::HostClosing)
    );
    assert!(matches!(
        op.open(exchange_spec()),
        Err(OpenError::Terminal {
            reason: TerminalReason::HostClosing,
            effect: gwz_transport::protocol::Effect::None
        })
    ));
    let end = now(op.finish());
    assert_eq!(
        end.registration,
        Registration::NeverRegistered(TerminalReason::HostClosing)
    );
    assert!(matches!(end.cleanup, CleanupStatus::Complete));
    assert_eq!(handle.snapshot().bindings, 0);
    assert_eq!(handle.snapshot().active_operations, 0);
    assert_eq!(rig.world.connects(), 0);
    assert_eq!(rig.world.exchanges.load(Ordering::SeqCst), 0);
    rig.progress.run();
    assert!(matches!(poll_once(&mut close), Poll::Ready(_)));
}

#[test]
fn register_after_domain_close_names_the_domain() {
    let mut rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let permit = now(handle.admit(prepared("op-a", 1)).unwrap().acquire()).unwrap();
    drop(rig.domain_owner.take());
    let op = permit.register();
    assert_eq!(
        op.registration(),
        Registration::NeverRegistered(TerminalReason::DomainClosing)
    );
}

#[test]
fn registration_that_wins_the_race_is_cancelled_by_the_close_and_counted() {
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    let op = registered(&rig, &handle, "op-a", 1);
    let id = binding_id(&op);
    let opening = op.open(exchange_spec()).unwrap();
    let _close = owner.close();
    // The binding was registered first: it is cancelled, not erased, and its end says so.
    assert!(matches!(
        now(opening.wait()),
        Err(OpenFailure::Closing(TerminalReason::HostClosing))
    ));
    let end = now(op.finish());
    assert_eq!(end.registration, Registration::Registered(id));
    assert!(end.cancelled);
    assert_eq!(handle.snapshot().bindings, 0);
}

#[test]
fn operation_finish_built_but_not_polled_still_seals() {
    let rig = Rig::new();
    let limits = HostLimits {
        active_slots: 1,
        ..HostLimits::default()
    };
    let (_owner, handle) = crate::build(rig.config(limits), rig.ports(1)).unwrap();
    let op = registered(&rig, &handle, "op-a", 1);
    let opening = op.open(exchange_spec()).unwrap();
    drop(op.finish());
    // Sealed without a poll: the pending opening is refused, the binding left the table and its slot is free.
    assert!(matches!(now(opening.wait()), Err(OpenFailure::Sealed)));
    assert_eq!(handle.snapshot().bindings, 0);
    assert_eq!(handle.snapshot().active_operations, 0);
    let next = registered(&rig, &handle, "op-b", 1);
    assert_eq!(handle.snapshot().active_operations, 1);
    drop(next);
    assert_eq!(
        handle.snapshot().active_operations,
        0,
        "a dropped operation abandons its binding"
    );
}

#[test]
fn a_dropped_or_aborted_exchange_discards_its_connection_and_an_unclean_one_is_not_reused() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let op = registered(&rig, &handle, "op-a", 1);
    drop(now(op.open(exchange_spec()).unwrap().wait()).unwrap());
    assert_eq!(handle.snapshot().connections.idle, 0);
    assert_eq!(rig.world.closes(), 1);
    now(op.open(exchange_spec()).unwrap().wait())
        .unwrap()
        .abort();
    assert_eq!(rig.world.closes(), 2);
    rig.world.discard_next.store(true, Ordering::SeqCst);
    let exchange = now(op.open(exchange_spec()).unwrap().wait()).unwrap();
    assert_eq!(now(exchange.finish()).disposition, Disposition::Discarded);
    assert_eq!(rig.world.closes(), 3);
    assert_eq!(handle.snapshot().connections.idle, 0);
    now(op.finish());
}

#[test]
fn admission_is_bounded_fair_and_released_by_drop() {
    let rig = Rig::new();
    let limits = HostLimits {
        active_slots: 1,
        waiting_admissions: 2,
        ..HostLimits::default()
    };
    let (_owner, handle) = crate::build(rig.config(limits), rig.ports(1)).unwrap();
    let holder = registered(&rig, &handle, "holder", 1);
    let first = handle.admit(prepared("first", 1)).unwrap();
    let second = handle.admit(prepared("second", 1)).unwrap();
    assert_eq!(handle.snapshot().waiting_admissions, 2);
    // The queue is full: refused at once, no waiting thread or record is made.
    assert!(matches!(
        handle.admit(prepared("third", 1)),
        Err(AdmissionError::QueueFull)
    ));
    let mut first = first.acquire();
    let mut second = second.acquire();
    assert!(matches!(poll_once(&mut first), Poll::Pending));
    assert!(matches!(poll_once(&mut second), Poll::Pending));
    now(holder.finish());
    // FIFO: the second cannot pass the first.
    assert!(matches!(poll_once(&mut second), Poll::Pending));
    let Poll::Ready(Ok(permit)) = poll_once(&mut first) else {
        panic!("the first waiter gets the slot");
    };
    assert_eq!(handle.snapshot().waiting_admissions, 1);
    // A permit dropped before register releases the slot to the next waiter.
    drop(permit);
    assert!(matches!(poll_once(&mut second), Poll::Ready(Ok(_))));
    // Dropping an admission releases its record.
    let held = handle.admit(prepared("fourth", 1)).unwrap();
    assert_eq!(handle.snapshot().waiting_admissions, 1);
    drop(held);
    assert_eq!(handle.snapshot().waiting_admissions, 0);
}

#[test]
fn an_admission_that_waits_past_its_bound_times_out_and_one_at_close_fails_by_name() {
    let rig = Rig::new();
    let limits = HostLimits {
        active_slots: 1,
        ..HostLimits::default()
    };
    let (owner, handle) = crate::build(rig.config(limits), rig.ports(1)).unwrap();
    let _holder = registered(&rig, &handle, "holder", 1);
    let mut waiting = handle.admit(prepared("waiting", 1)).unwrap().acquire();
    assert!(matches!(poll_once(&mut waiting), Poll::Pending));
    let bound = HostLimits::default().admission_bound_ms;
    rig.clock.advance(bound - 1);
    assert!(matches!(poll_once(&mut waiting), Poll::Pending));
    rig.clock.advance(1);
    assert!(matches!(
        poll_once(&mut waiting),
        Poll::Ready(Err(AdmissionError::AdmissionTimeout))
    ));
    assert_eq!(handle.snapshot().waiting_admissions, 0);
    let mut doomed = handle.admit(prepared("doomed", 1)).unwrap().acquire();
    assert!(matches!(poll_once(&mut doomed), Poll::Pending));
    drop(owner.close());
    assert!(matches!(
        poll_once(&mut doomed),
        Poll::Ready(Err(AdmissionError::Closing(TerminalReason::HostClosing)))
    ));
    assert!(matches!(
        handle.admit(prepared("late", 1)),
        Err(AdmissionError::Closing(TerminalReason::HostClosing))
    ));
}

#[test]
fn admission_refuses_invalid_and_over_budget_input_before_any_effect() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let too_big = PreparedOperation::new(
        "op-a",
        crate::EndpointRoute::ssh_and_https(),
        crate::fakes::profile(1),
        crate::AuthContext::new(
            16 * 1024 * 1024 + 1,
            crate::AuthScope::new([0; 32]),
            std::sync::Arc::new(()),
        ),
        crate::OperationPolicy::new(4, 32, 3).unwrap(),
    )
    .unwrap();
    assert!(matches!(
        handle.admit(too_big),
        Err(AdmissionError::InputBudget)
    ));
    // The host total (64 MiB) counts what admissions hold, and gives it back when they go.
    let big = |name: &str| {
        PreparedOperation::new(
            name,
            crate::EndpointRoute::ssh_and_https(),
            crate::fakes::profile(1),
            crate::AuthContext::new(
                16 * 1024 * 1024,
                crate::AuthScope::new([0; 32]),
                std::sync::Arc::new(()),
            ),
            crate::OperationPolicy::new(4, 32, 3).unwrap(),
        )
        .unwrap()
    };
    let held: Vec<_> = (0..4)
        .map(|i| handle.admit(big(&format!("op-{i}"))).unwrap())
        .collect();
    assert!(matches!(
        handle.admit(big("op-5")),
        Err(AdmissionError::InputBudget)
    ));
    drop(held);
    assert!(handle.admit(big("op-6")).is_ok());
    assert_eq!(
        rig.world.created.load(Ordering::SeqCst),
        0,
        "admit makes no endpoint"
    );
    // Input is validated when it is built.
    assert!(
        PreparedOperation::new(
            "",
            crate::EndpointRoute::ssh_and_https(),
            crate::fakes::profile(1),
            crate::AuthContext::new(1, crate::AuthScope::new([0; 32]), std::sync::Arc::new(())),
            crate::OperationPolicy::new(4, 32, 3).unwrap()
        )
        .is_err()
    );
    assert!(crate::EndpointProfile::new(Vec::new(), [0; 32]).is_err());
    assert!(crate::OperationPolicy::new(0, 32, 3).is_err());
    assert!(crate::EndpointRoute::new(Vec::new(), Vec::new()).is_err());
}

#[test]
fn an_unavailable_adapter_at_acquire_is_named_and_gives_the_slot_back() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    rig.world.fail_next.store(true, Ordering::SeqCst);
    let admission = handle.admit(prepared("op-a", 1)).unwrap();
    assert!(matches!(
        now(admission.acquire()),
        Err(AdmissionError::UnavailableAdapter(_))
    ));
    assert_eq!(handle.snapshot().active_operations, 0);
    assert_eq!(handle.snapshot().waiting_admissions, 0);
}

#[test]
fn building_with_a_bad_binding_limit_is_refused() {
    let rig = Rig::new();
    for registrations in [0, 4097] {
        let limits = HostLimits {
            binding_registrations: registrations,
            ..HostLimits::default()
        };
        assert!(matches!(
            crate::build(rig.config(limits), rig.ports(1)),
            Err(BuildError::InvalidConfig(
                crate::ConfigReason::BindingRegistrations
            ))
        ));
    }
}
