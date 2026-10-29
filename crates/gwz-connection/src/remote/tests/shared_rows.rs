//! One type family for local and carrier-backed operations (GwzSplitCommandChannel 3.4; plan step 2.2c rework): the
//! rows that a served core holds the same `HostHandle`, `Admission`, `Operation`, `Opening` and `Exchange` as a local
//! one, and the two fixes the rework owes (an initiator `Cancel` retires its stream slot; what releases a client binding).
use super::{
    super::{RemoteConfig, ServiceClass},
    core_rows::{FETCH, bound, open, register, round_trip, write_all},
    world::{Way, World},
};
use crate::{
    AdmissionError, HostHandle, OpenError, Opening, Operation, OperationRef, Registration,
    fakes::{exchange_spec, now, prepared},
};
use gwz_transport::protocol::MessageKind;

/// Code that cannot tell a local operation from a carrier-backed one: it names only the shared types.
fn kind_blind(handle: &HostHandle, op: &Operation) -> (bool, Result<Opening, OpenError>) {
    let _ = handle.snapshot();
    (
        matches!(op.registration(), Registration::Pending),
        op.open(exchange_spec()),
    )
}

#[test]
fn a_local_and_a_carrier_backed_operation_are_the_same_types_and_each_handle_takes_its_own_input() {
    let w = World::new();
    // The same function takes both handles and both operations.
    let local = now(w.host.admit(prepared("local", 1)).unwrap().acquire())
        .unwrap()
        .register();
    let remote = bound(&w, "call-1");
    let (pending, opening) = kind_blind(&w.host, &local);
    assert!(
        !pending && opening.is_ok(),
        "a local operation is never pending"
    );
    let (_, opening) = kind_blind(&w.handle, &remote);
    assert!(opening.is_ok());
    // The core supplies a reference, never an operation: a prepared one is refused on the carrier-backed handle,
    // and a reference on a local host (which builds nothing from one).
    w.call("call-2", ServiceClass::Fetch, FETCH, &[]);
    assert!(matches!(
        w.handle.admit(prepared("call-2", 1)),
        Err(AdmissionError::WrongInput)
    ));
    assert!(matches!(
        w.host.admit(OperationRef::new("call-2").unwrap()),
        Err(AdmissionError::WrongInput)
    ));
    w.settle();
    assert!(
        w.carrier.sent(Way::ToClient, MessageKind::Bind).len() == 1,
        "the refused input sent nothing: only call-1's bind crossed"
    );
}

#[test]
fn registered_resolves_at_once_for_a_local_operation_and_after_the_answer_for_a_carrier_backed_one()
{
    let w = World::new();
    let local = now(w.host.admit(prepared("local", 1)).unwrap().acquire())
        .unwrap()
        .register();
    assert!(matches!(
        now(local.registered()),
        Registration::Registered(_)
    ));
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let remote = register(&w, "call-1");
    let mut waiting = Box::pin(remote.registered());
    assert!(
        crate::fakes::poll_once(&mut waiting).is_pending(),
        "the deferred commit is in flight"
    );
    w.settle();
    assert!(matches!(
        crate::fakes::poll_once(&mut waiting),
        std::task::Poll::Ready(Registration::Registered(_))
    ));
}

#[test]
fn an_initiator_cancel_retires_its_stream_slot_so_aborted_exchanges_do_not_exhaust_a_binding() {
    // Old-lane report item 10: the stream slot stayed until the binding ended, so `streams_per_binding` aborted
    // exchanges ended the binding's ability to open.
    let w = World::with(RemoteConfig {
        streams_per_binding: 4,
        ..Default::default()
    });
    let op = bound(&w, "call-1");
    for round in 0..12 {
        let mut exchange = open(&w, &op, exchange_spec());
        write_all(&w, &mut exchange, b"partial");
        exchange.abort();
        w.settle();
        assert_eq!(
            op.open(exchange_spec()).map(drop),
            Ok(()),
            "round {round}: the slot of every aborted exchange came back"
        );
        w.settle();
    }
    // And the binding still carries a whole exchange.
    let (_, response, _) = round_trip(&w, &op, exchange_spec(), b"after");
    assert_eq!(response, b"after");
    let (core_streams, _) = (w.remote.streams_held(), ());
    assert!(
        core_streams <= 4,
        "the core's mux holds {core_streams} streams for a binding of four"
    );
}

#[test]
fn finishing_an_operation_sends_nothing_to_release_the_client_binding_the_calls_end_does() {
    // Old-lane report item 11: 2.0 3.3 has no kind that ends a binding, so the core's `finish()` cannot release the
    // client's binding; the call's end at the client (`Receiver::end_call`) does. Pinned so a change is deliberate.
    let w = World::new();
    let op = bound(&w, "call-1");
    let (_, response, _) = round_trip(&w, &op, exchange_spec(), b"x");
    assert_eq!(response, b"x");
    let before = w.carrier.log().len();
    let end = now(op.finish());
    w.settle();
    assert!(matches!(end.registration, Registration::Registered(_)));
    assert_eq!(
        w.carrier.log().len(),
        before,
        "finish() put nothing more on the lane"
    );
    assert_eq!(
        w.receiver.bindings(),
        1,
        "the client's binding is still held"
    );
    assert_eq!(w.host.snapshot().active_operations, 1);
    w.receiver.end_call("call-1");
    w.settle();
    assert_eq!(w.receiver.bindings(), 0);
    assert_eq!(w.host.snapshot().active_operations, 0);
}

#[test]
fn a_binding_that_ends_is_retired_on_the_lane_of_each_end() {
    // A byte-stream carrier routes the frames of open bindings only (GwzSplitCommandChannel 3.3), so each end tells its
    // lane when a binding is over, or the carrier's binding limit fills with the dead.
    let w = World::new();
    let op = bound(&w, "call-1");
    let session = w.carrier.sent(Way::ToClient, MessageKind::Bind)[0]
        .1
        .session_id
        .clone();
    assert!(
        w.carrier.retired(Way::ToClient).is_empty() && w.carrier.retired(Way::ToCore).is_empty()
    );
    drop(crate::fakes::now(op.finish()));
    w.receiver.end_call("call-1");
    w.settle();
    assert_eq!(
        w.carrier.retired(Way::ToClient),
        vec![session.clone()],
        "the core's end"
    );
    assert_eq!(
        w.carrier.retired(Way::ToCore),
        vec![session],
        "the client's end"
    );

    // A bind the client refuses is retired at the client at once (the carrier admitted it to ask), and at the core when
    // the refused operation goes.
    w.core.call("ghost", None);
    let ghost = register(&w, "ghost");
    w.settle();
    let session = w.carrier.sent(Way::ToClient, MessageKind::Bind)[1]
        .1
        .session_id
        .clone();
    assert!(w.carrier.retired(Way::ToCore).contains(&session));
    drop(crate::fakes::now(ghost.finish()));
    w.settle();
    assert!(w.carrier.retired(Way::ToClient).contains(&session));
}

#[test]
fn an_open_on_a_binding_the_carrier_ended_fails_at_once_and_does_not_wait_for_ever() {
    let w = World::new();
    let op = bound(&w, "call-1");
    w.carrier.end();
    w.remote.pump();
    let mut wait = Box::pin(op.open(exchange_spec()).unwrap().wait());
    w.remote.pump();
    match crate::fakes::now(&mut wait) {
        Err(crate::OpenFailure::CarrierLost { effect }) => {
            assert_eq!(effect, gwz_transport::protocol::Effect::None);
        }
        other => panic!("expected CarrierLost, got {other:?}"),
    }
}

#[test]
fn a_binding_the_carrier_faulted_ends_alone_at_the_core_and_at_the_client() {
    use crate::{OpenFailure, StreamError};
    use gwz_transport::protocol::ErrorCode;
    let session = |w: &World, n: usize| {
        w.carrier.sent(Way::ToClient, MessageKind::Bind)[n]
            .1
            .session_id
            .clone()
    };
    // The core's end: the binding's later opens fail with the fault, the other binding carries on.
    let w = World::new();
    let first = bound(&w, "call-1");
    let second = bound(&w, "call-2");
    let mut live = open(&w, &first, exchange_spec());
    w.carrier.fault_at_core(&session(&w, 0));
    w.settle();
    assert!(matches!(
        live.stream().write(b"x"),
        Err(StreamError::Failed {
            code: ErrorCode::Protocol,
            ..
        })
    ));
    let mut wait = Box::pin(first.open(exchange_spec()).unwrap().wait());
    w.settle();
    assert!(matches!(
        crate::fakes::now(&mut wait),
        Err(OpenFailure::Fault(ErrorCode::Protocol))
    ));
    let (_, response, _) = round_trip(&w, &second, exchange_spec(), b"unaffected");
    assert_eq!(response, b"unaffected");

    // The client's end: its half of the binding ends and is retired; the other binding carries on.
    let w = World::new();
    let first = bound(&w, "call-1");
    let second = bound(&w, "call-2");
    let mut live = open(&w, &first, exchange_spec());
    write_all(&w, &mut live, b"partial");
    let faulted = session(&w, 0);
    w.carrier.fault_at_client(&faulted);
    w.settle();
    assert_eq!(
        w.receiver.bindings(),
        1,
        "the faulted binding left the client"
    );
    assert!(w.carrier.retired(Way::ToCore).contains(&faulted));
    assert_eq!(
        w.host.snapshot().connections.leased,
        0,
        "its lease went with it"
    );
    let (_, response, _) = round_trip(&w, &second, exchange_spec(), b"unaffected");
    assert_eq!(response, b"unaffected");
}
