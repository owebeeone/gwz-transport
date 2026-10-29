//! State-axis rows of remediation round 1 (step 2.2c): a pending future is woken by a state change and never by its own
//! poll; the registration never reads `Pending` after `Registered`; an effect is never under-reported when a call ends;
//! a binding that is over sends nothing more; a core that floods binds costs the client a bounded outbox; and a core
//! and a client driven only by their drivers and real (counting) wakers make progress with no explicit `settle`.
use super::{
    super::{
        AllowedSet, BindBounds, Invocation, RemoteConfig, ServiceClass, receiver::OUTBOX_ROOM,
    },
    core_rows::{FETCH, bound, finish, open, read_exact, register, write_all},
    world::{Count, Way, World},
};
use crate::{
    AdmissionError, CancelState, ExchangeSpec, GitService, HostPhase, OperationRef, Registration,
    TerminalReason,
    fakes::{exchange_spec, now, poll_once},
};
use gwz_transport::{
    pool::{Identity, Key},
    protocol::{Effect, ErrorCode, MessageKind},
};
use std::task::Poll;

fn push_spec() -> ExchangeSpec {
    ExchangeSpec::git(
        Key::ssh("git", "example.invalid", 22),
        Identity::Ambient,
        GitService::ReceivePackExchange,
        "/srv/repo.git",
    )
    .unwrap()
}
/// A registered operation on a push-class call.
fn push_op(w: &World, request: &str) -> crate::Operation {
    w.call(
        request,
        ServiceClass::Push,
        FETCH,
        &["ssh://example.invalid/"],
    );
    let op = register(w, request);
    w.settle();
    assert!(matches!(op.registration(), Registration::Registered(_)));
    op
}

// --- a pending future is woken by a state change, never by its own poll (State P2-1) ---

#[test]
fn a_pending_registered_wait_is_not_woken_by_its_own_poll_and_is_woken_by_the_answer() {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let op = register(&w, "call-1");
    let task = Count::new();
    let mut registered = Box::pin(op.registered());
    for _ in 0..5 {
        assert!(task.poll(&mut registered).is_pending());
    }
    assert_eq!(
        task.wakes(),
        0,
        "five polls with no lane event woke the task"
    );
    w.settle();
    assert!(task.wakes() >= 1, "the answer woke the task");
    assert!(matches!(
        task.poll(&mut registered),
        Poll::Ready(Registration::Registered(_))
    ));
}

#[test]
fn a_pending_open_wait_is_not_woken_by_its_own_poll_and_is_woken_by_the_answer() {
    let w = World::new();
    let op = bound(&w, "call-1");
    let task = Count::new();
    let mut wait = Box::pin(op.open(exchange_spec()).unwrap().wait());
    for _ in 0..5 {
        assert!(task.poll(&mut wait).is_pending());
    }
    assert_eq!(
        task.wakes(),
        0,
        "five polls with no lane event woke the task"
    );
    w.settle();
    assert!(task.wakes() >= 1, "the opened woke the task");
    assert!(matches!(task.poll(&mut wait), Poll::Ready(Ok(_))));
}

#[test]
fn a_pending_exchange_finish_is_not_woken_by_its_own_poll_and_is_woken_by_the_close() {
    let w = World::new();
    let op = bound(&w, "call-1");
    let exchange = open(&w, &op, exchange_spec());
    let task = Count::new();
    let mut finish = Box::pin(exchange.finish());
    for _ in 0..5 {
        assert!(task.poll(&mut finish).is_pending());
    }
    assert_eq!(
        task.wakes(),
        0,
        "five polls with no lane event woke the task"
    );
    w.settle();
    assert!(task.wakes() >= 1, "the client's closed woke the task");
    assert!(matches!(task.poll(&mut finish), Poll::Ready(_)));
}

#[test]
fn a_pending_acquire_is_not_woken_by_its_own_poll_and_is_woken_when_a_slot_goes_back() {
    let w = World::with(RemoteConfig {
        active_operations: 1,
        ..Default::default()
    });
    let first = bound(&w, "call-1");
    w.call("call-2", ServiceClass::Fetch, FETCH, &[]);
    let admission = w
        .handle
        .admit(OperationRef::new("call-2").unwrap())
        .unwrap();
    let task = Count::new();
    let mut acquire = Box::pin(admission.acquire());
    for _ in 0..5 {
        assert!(task.poll(&mut acquire).is_pending());
    }
    assert_eq!(
        task.wakes(),
        0,
        "five polls with the slot held woke the task"
    );
    drop(now(first.finish()));
    w.settle();
    assert!(task.wakes() >= 1, "the slot going back woke the task");
    assert!(matches!(task.poll(&mut acquire), Poll::Ready(Ok(_))));
}

#[test]
fn a_task_polled_with_a_fresh_waker_every_time_does_not_grow_the_waiters() {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let op = register(&w, "call-1");
    let mut registered = Box::pin(op.registered());
    for _ in 0..200 {
        let task = Count::new();
        assert!(task.poll(&mut registered).is_pending());
    }
    let held = w.remote.with(|state| state.waiters.len());
    assert!(held <= 16, "{held} wakers held for one wait");
}

// --- the registration is monotonic (State P3-3) ---

#[test]
fn the_registration_never_reads_pending_after_registered_even_between_a_retirement_and_its_receipt()
{
    // Two operations retire in one pump. Their receipts are published once the lock is released, in order; the first
    // receipt's waker looks at the second operation before its receipt is set: the binding is gone and the cell unset.
    let w = World::new();
    let first = bound(&w, "call-1");
    let second = bound(&w, "call-2");
    let first_receipt = w
        .remote
        .with(|state| state.bindings.values().next().unwrap().done.clone());
    let seen = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
    struct Look {
        op: crate::Operation,
        seen: std::sync::Arc<std::sync::Mutex<Vec<Registration>>>,
    }
    impl std::task::Wake for Look {
        fn wake(self: std::sync::Arc<Self>) {
            self.seen.lock().unwrap().push(self.op.registration());
        }
    }
    let look = std::sync::Arc::new(Look {
        op: second,
        seen: seen.clone(),
    });
    let waker = std::task::Waker::from(look);
    assert!(
        first_receipt
            .poll(&mut std::task::Context::from_waker(&waker))
            .is_pending()
    );
    w.remote.with(|state| {
        for binding in state.bindings.values_mut() {
            binding.sealed = true;
        }
    });
    let seen = seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 1, "the first receipt's waker ran");
    assert!(
        matches!(seen[0], Registration::Registered(_)),
        "the second operation read {:?} after it was registered",
        seen[0]
    );
    drop(first);
}

// --- an effect is never under-reported when the call ends or the binding faults (State P2-2) ---

#[test]
fn ending_the_call_under_a_forwarding_push_reports_the_effect_possible() {
    let w = World::new();
    let op = push_op(&w, "call-1");
    let mut exchange = open(&w, &op, push_spec());
    write_all(&w, &mut exchange, b"pack-bytes");
    assert_eq!(read_exact(&w, &mut exchange, 10), b"pack-bytes");
    w.receiver.end_call("call-1");
    w.settle();
    let end = finish(&w, exchange);
    assert_eq!(
        end.failure,
        Some((ErrorCode::Cancelled, Effect::Possible)),
        "the push's bytes had crossed the client's exchange"
    );
}

#[test]
fn a_fault_of_the_binding_at_the_client_under_a_forwarding_push_reports_the_effect_possible() {
    let w = World::new();
    let op = push_op(&w, "call-1");
    let session = w.carrier.sent(Way::ToClient, MessageKind::Bind)[0]
        .1
        .session_id
        .clone();
    let mut exchange = open(&w, &op, push_spec());
    write_all(&w, &mut exchange, b"pack-bytes");
    assert_eq!(read_exact(&w, &mut exchange, 10), b"pack-bytes");
    w.carrier.fault_at_client(&session);
    w.settle();
    let end = finish(&w, exchange);
    assert_eq!(
        end.failure,
        Some((ErrorCode::Protocol, Effect::Possible)),
        "the push's bytes had crossed the client's exchange"
    );
}

#[test]
fn ending_the_call_under_a_fetch_or_an_unforwarded_push_keeps_the_effect_none() {
    let w = World::new();
    let op = bound(&w, "call-1");
    let mut fetch = open(&w, &op, exchange_spec());
    write_all(&w, &mut fetch, b"want");
    assert_eq!(read_exact(&w, &mut fetch, 4), b"want");
    w.receiver.end_call("call-1");
    w.settle();
    assert_eq!(
        finish(&w, fetch).failure,
        Some((ErrorCode::Cancelled, Effect::None))
    );
    let w = World::new();
    let op = push_op(&w, "call-1");
    let push = open(&w, &op, push_spec());
    w.receiver.end_call("call-1");
    w.settle();
    assert_eq!(
        finish(&w, push).failure,
        Some((ErrorCode::Cancelled, Effect::None)),
        "no byte of the push had left"
    );
}

// --- a binding that is over sends nothing more (State P2-3) ---

/// The `bind` of `call-1` waits in the core's outbox behind a full lane while the binding ends by `end`; the lane then
/// has room.
fn a_stale_bind_never_leaves(end: impl FnOnce(&World, crate::Operation)) {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    w.carrier.set_capacity(0);
    let op = register(&w, "call-1");
    w.remote.pump();
    assert!(
        w.carrier.sent(Way::ToClient, MessageKind::Bind).is_empty(),
        "the lane is full: the bind is still queued"
    );
    end(&w, op);
    assert_eq!(
        w.carrier.retired(Way::ToClient).len(),
        1,
        "the binding retired"
    );
    w.carrier.set_capacity(4096);
    w.settle();
    assert!(
        w.carrier.sent(Way::ToClient, MessageKind::Bind).is_empty(),
        "a bind went out for a binding the core had already retired"
    );
    assert_eq!(w.receiver.bindings(), 0, "the client holds no binding");
    assert_eq!(w.host.snapshot().active_operations, 0, "and no operation");
}

#[test]
fn a_bind_queued_behind_a_full_lane_never_leaves_after_the_operation_is_finished() {
    a_stale_bind_never_leaves(|_, op| {
        let end = now(op.finish());
        assert_eq!(
            end.registration,
            Registration::NeverRegistered(TerminalReason::Cancelled)
        );
    });
}

#[test]
fn a_bind_queued_behind_a_full_lane_never_leaves_after_the_operation_is_cancelled() {
    a_stale_bind_never_leaves(|_, op| {
        assert_eq!(op.cancel_handle().cancel(), CancelState::Cancelled);
        drop(op);
    });
}

#[test]
fn a_bind_queued_behind_a_full_lane_never_leaves_after_the_bootstrap_bound_passed() {
    a_stale_bind_never_leaves(|w, op| {
        w.rig.clock.advance(6_000);
        w.remote.pump();
        assert_eq!(
            op.registration(),
            Registration::NeverRegistered(TerminalReason::TimedOut)
        );
        let _ = now(op.finish());
    });
}

// --- a core that sends binds and does not read costs the client a bounded outbox (State P3-1) ---

#[test]
fn a_bind_flood_against_a_full_lane_holds_at_most_the_outbox_room() {
    let w = World::new();
    w.carrier.set_capacity(0);
    for i in 0..20_000 {
        let session = format!("hostile.{i}");
        w.carrier.inject(
            Way::ToClient,
            (
                format!("req-{i}"),
                gwz_transport::binding::offer(
                    &session,
                    gwz_transport::protocol::EndpointRole::Driver,
                ),
            ),
        );
    }
    w.receiver.pump();
    assert!(
        w.carrier.queued(Way::ToClient) >= 20_000 - 2 * OUTBOX_ROOM,
        "the receiver stopped taking from the lane"
    );
    w.carrier.set_capacity(usize::MAX);
    w.receiver.pump();
    let rejected = w.carrier.sent(Way::ToCore, MessageKind::BindRejected).len();
    assert!(
        rejected <= OUTBOX_ROOM,
        "the receiver held {rejected} rejections beyond its outbox room of {OUTBOX_ROOM}"
    );
}

// --- the snapshot's phase is truthful (State P3-2 = Code P3-6) ---

#[test]
fn a_refused_snapshot_says_closed_once_the_carrier_is_lost_and_open_before() {
    let w = World::new();
    let _op = bound(&w, "call-1");
    let before = w.handle.snapshot();
    assert!(before.refused() && before.phase == HostPhase::Open);
    w.carrier.end();
    w.remote.pump();
    let after = w.handle.snapshot();
    assert!(after.refused(), "still a refusal after the loss");
    assert_eq!(after.phase, HostPhase::Closed, "the carrier is lost");
}

// --- a request name the lane can carry is admissible (Code P3-2) ---

#[test]
fn a_128_byte_request_id_is_admitted_and_registers_over_the_remote_handle() {
    let request = "r".repeat(128);
    assert!(OperationRef::new(&request).is_ok());
    assert!(OperationRef::new(&"r".repeat(129)).is_err());
    let w = World::new();
    let mut invocation = Invocation::new(
        crate::fakes::prepared_with("long-call", 1, crate::fakes::auth(0, "client")),
        ServiceClass::Fetch,
    );
    invocation.fetch = AllowedSet::parse(FETCH).unwrap();
    w.call_with(&request, invocation, None);
    let op = register(&w, &request);
    w.settle();
    assert!(
        matches!(op.registration(), Registration::Registered(_)),
        "{:?}",
        op.registration()
    );
}

// --- a core and a client driven only by their drivers and real wakers (the State reviewer's risk) ---

/// Steps the three executors the way a runtime would, advancing the clock in the receiver's tick: no `settle`, no
/// direct `pump` of either end.
fn drive(w: &World) {
    for _ in 0..40 {
        w.rig.clock.advance(5);
        w.core_progress.run();
        w.client_progress.run();
        w.rig.progress.run();
    }
}

#[test]
fn a_whole_operation_runs_through_the_drivers_and_counting_wakers_with_no_settle() {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let admission = w
        .handle
        .admit(OperationRef::new("call-1").unwrap())
        .unwrap();
    let op = now(admission.acquire()).unwrap().register();

    let registered_task = Count::new();
    let mut registered = Box::pin(op.registered());
    assert!(registered_task.poll(&mut registered).is_pending());
    drive(&w);
    assert!(matches!(
        registered_task.poll(&mut registered),
        Poll::Ready(Registration::Registered(_))
    ));
    assert!(
        (1..=4).contains(&registered_task.wakes()),
        "{} wakes",
        registered_task.wakes()
    );

    let open_task = Count::new();
    let mut wait = Box::pin(op.open(exchange_spec()).unwrap().wait());
    assert!(open_task.poll(&mut wait).is_pending());
    drive(&w);
    let Poll::Ready(Ok(mut exchange)) = open_task.poll(&mut wait) else {
        panic!("the opening did not resolve through the drivers");
    };
    assert!(
        (1..=4).contains(&open_task.wakes()),
        "{} wakes",
        open_task.wakes()
    );

    let mut written = 0;
    for _ in 0..100 {
        match exchange.stream().write(b"hello") {
            Ok(count) => {
                written = count;
                break;
            }
            Err(_) => drive(&w),
        }
    }
    assert_eq!(written, 5);
    let mut echoed = Vec::new();
    for _ in 0..100 {
        let mut buffer = [0u8; 16];
        match exchange.stream().read(&mut buffer) {
            Ok(count) => echoed.extend_from_slice(&buffer[..count]),
            Err(_) => drive(&w),
        }
        if echoed.len() >= 5 {
            break;
        }
    }
    assert_eq!(echoed, b"hello");

    let finish_task = Count::new();
    let mut finish = Box::pin(exchange.finish());
    assert!(finish_task.poll(&mut finish).is_pending());
    drive(&w);
    assert!(matches!(finish_task.poll(&mut finish), Poll::Ready(_)));
    assert!(
        (1..=4).contains(&finish_task.wakes()),
        "{} wakes",
        finish_task.wakes()
    );
    drop(finish);

    let done_task = Count::new();
    let mut done = Box::pin(op.finish());
    let _ = done_task.poll(&mut done);
    drive(&w);
    assert!(matches!(done_task.poll(&mut done), Poll::Ready(_)));
    assert_eq!(
        w.carrier.retired(Way::ToClient).len(),
        1,
        "the core's binding retired"
    );
}

#[test]
fn a_cancel_while_an_open_is_unanswered_resolves_through_the_drivers_and_counting_wakers_with_no_settle()
 {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let admission = w
        .handle
        .admit(OperationRef::new("call-1").unwrap())
        .unwrap();
    let op = now(admission.acquire()).unwrap().register();
    drive(&w);
    assert!(matches!(op.registration(), Registration::Registered(_)));

    let task = Count::new();
    let mut wait = Box::pin(op.open(exchange_spec()).unwrap().wait());
    assert!(task.poll(&mut wait).is_pending());
    assert_eq!(
        w.carrier.sent(Way::ToClient, MessageKind::Open).len(),
        1,
        "the Open is on the lane and no driver has run since"
    );
    assert_eq!(op.cancel_handle().cancel(), CancelState::Cancelled);
    drive(&w);
    assert!(
        matches!(
            task.poll(&mut wait),
            Poll::Ready(Err(crate::OpenFailure::Cancelled))
        ),
        "the cancelled opening never resolved through the drivers"
    );
    drop(wait);
    let mut done = Box::pin(op.finish());
    let _ = Count::new().poll(&mut done);
    drive(&w);
    assert!(matches!(poll_once(&mut done), Poll::Ready(_)));
    assert_eq!(w.carrier.retired(Way::ToClient).len(), 1);
}

// --- rows added for mutations that survived the first pass ---

#[test]
fn a_task_polled_again_with_the_same_waker_keeps_one_waker() {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let op = register(&w, "call-1");
    let task = Count::new();
    let mut registered = Box::pin(op.registered());
    for _ in 0..200 {
        assert!(task.poll(&mut registered).is_pending());
    }
    assert_eq!(w.remote.with(|state| state.waiters.len()), 1);
}

#[test]
fn a_pending_acquire_is_woken_and_refused_when_the_carrier_ends() {
    let w = World::with(RemoteConfig {
        active_operations: 1,
        ..Default::default()
    });
    let _first = bound(&w, "call-1");
    w.call("call-2", ServiceClass::Fetch, FETCH, &[]);
    let admission = w
        .handle
        .admit(OperationRef::new("call-2").unwrap())
        .unwrap();
    let task = Count::new();
    let mut acquire = Box::pin(admission.acquire());
    assert!(task.poll(&mut acquire).is_pending());
    w.carrier.end();
    w.remote.pump();
    assert!(task.wakes() >= 1, "the carrier's end woke the admission");
    assert!(matches!(
        task.poll(&mut acquire),
        Poll::Ready(Err(AdmissionError::CarrierLost))
    ));
}

#[test]
fn the_bind_bounds_are_the_bindings_window_and_the_muxs_streams() {
    let window = 65_536;
    let core = |streams_per_binding| {
        BindBounds::core(&RemoteConfig {
            streams_per_binding,
            ..Default::default()
        })
    };
    assert_eq!(
        core(7),
        BindBounds {
            receive_window: window,
            streams: 7
        }
    );
    assert_eq!(core(0).streams, 1);
    assert_eq!(core(5000).streams, 1024);
    assert_eq!(
        BindBounds::client(),
        BindBounds {
            receive_window: window,
            streams: 64
        }
    );
}

#[test]
fn cancelling_a_forwarding_push_tells_the_core_the_effect_possible_on_the_lane() {
    let w = World::new();
    let op = push_op(&w, "call-1");
    let mut exchange = open(&w, &op, push_spec());
    write_all(&w, &mut exchange, b"pack-bytes");
    assert_eq!(read_exact(&w, &mut exchange, 10), b"pack-bytes");
    assert_eq!(op.cancel_handle().cancel(), CancelState::Cancelled);
    w.settle();
    let failed = w.carrier.sent(Way::ToCore, MessageKind::Failed);
    assert_eq!(failed.len(), 1);
    let failure = failed[0].1.failed.as_ref().unwrap();
    assert_eq!(
        (failure.code, failure.effect),
        (ErrorCode::Cancelled, Effect::Possible)
    );
}

#[test]
fn closing_the_clients_host_under_a_forwarding_push_reports_the_effect_possible() {
    let w = World::new();
    let op = push_op(&w, "call-1");
    let mut exchange = open(&w, &op, push_spec());
    write_all(&w, &mut exchange, b"pack-bytes");
    assert_eq!(read_exact(&w, &mut exchange, 10), b"pack-bytes");
    w.close_client_host();
    w.settle();
    let end = finish(&w, exchange);
    let (_, effect) = end.failure.expect("the push failed");
    assert_eq!(effect, Effect::Possible);
}
