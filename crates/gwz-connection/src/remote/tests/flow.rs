//! Bytes, ends and losses: what crosses the lane once a binding is up, and what happens when the lane or a peer goes.
use super::{
    super::ServiceClass,
    core_rows::{FETCH, bound, finish, open, round_trip, would_block, write_all},
    world::{Way, World},
};
use crate::{
    CancelState, Exchange, ExchangeSpec, GitService, OpenFailure, Operation, OperationRef,
    Registration, StreamError, TerminalReason,
    fakes::{exchange_spec, now},
};
use gwz_transport::{
    pool::{Identity, Key},
    protocol::{Disposition, Effect, ErrorCode, MessageKind},
};
use std::sync::atomic::Ordering;

fn push_spec() -> ExchangeSpec {
    ExchangeSpec::git(
        Key::ssh("git", "example.invalid", 22),
        Identity::Ambient,
        GitService::ReceivePackExchange,
        "/srv/repo.git",
    )
    .unwrap()
}
fn push_op(w: &World, request: &str) -> Operation {
    w.call(
        request,
        ServiceClass::Push,
        FETCH,
        &["ssh://example.invalid/"],
    );
    let admission = w.handle.admit(OperationRef::new(request).unwrap()).unwrap();
    let op = now(admission.acquire()).unwrap().register();
    w.settle();
    assert!(matches!(op.registration(), Registration::Registered(_)));
    op
}

#[test]
fn a_transfer_larger_than_the_window_crosses_in_order_under_flow_control() {
    let w = World::new();
    let op = bound(&w, "call-1");
    let mut exchange = open(&w, &op, exchange_spec());
    let payload: Vec<u8> = (0..300_000u32).map(|n| (n % 251) as u8).collect();
    let (mut sent, mut received) = (0usize, Vec::new());
    let mut buffer = [0u8; 8192];
    for _ in 0..4000 {
        if sent < payload.len() {
            match exchange.stream().write(&payload[sent..]) {
                Ok(count) => sent += count,
                Err(error) if would_block(&error) => {}
                Err(error) => panic!("write: {error:?}"),
            }
        }
        match exchange.stream().read(&mut buffer) {
            Ok(count) => received.extend_from_slice(&buffer[..count]),
            Err(error) if would_block(&error) => {}
            Err(error) => panic!("read: {error:?}"),
        }
        w.settle();
        if received.len() >= payload.len() {
            break;
        }
    }
    assert_eq!(received.len(), payload.len());
    assert!(received == payload, "the echo is byte-exact");
    let end = finish(&w, exchange);
    assert_eq!(end.disposition, Disposition::Reusable);
}

#[test]
fn a_full_lane_holds_messages_in_order_and_loses_none() {
    let w = World::new();
    let op = bound(&w, "call-1");
    w.carrier.set_capacity(2);
    let request: Vec<u8> = (0..120_000u32).map(|n| (n % 241) as u8).collect();
    let mut exchange = open(&w, &op, exchange_spec());
    let (mut sent, mut received) = (0usize, Vec::new());
    let mut buffer = [0u8; 4096];
    for _ in 0..20_000 {
        if sent < request.len()
            && let Ok(count) = exchange.stream().write(&request[sent..])
        {
            sent += count;
        }
        if let Ok(count) = exchange.stream().read(&mut buffer) {
            received.extend_from_slice(&buffer[..count]);
        }
        w.settle();
        if received.len() >= request.len() {
            break;
        }
    }
    assert!(
        received == request,
        "{} of {} bytes, in order",
        received.len(),
        request.len()
    );
    assert!(finish(&w, exchange).failure.is_none());
}

#[test]
fn an_open_on_a_pending_operation_waits_for_bound_first() {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let op = now(w
        .handle
        .admit(OperationRef::new("call-1").unwrap())
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    let mut wait = Box::pin(op.open(exchange_spec()).unwrap().wait());
    w.remote.pump();
    assert_eq!(w.carrier.sent(Way::ToClient, MessageKind::Bind).len(), 1);
    assert!(
        w.carrier.sent(Way::ToClient, MessageKind::Open).is_empty(),
        "no open before the bind is answered"
    );
    assert!(crate::fakes::poll_once(&mut wait).is_pending());
    w.settle();
    let log = w.carrier.log();
    let position = |way: Way, kind: MessageKind| {
        log.iter()
            .position(|(w, a)| *w == way && a.1.kind == kind)
            .unwrap()
    };
    assert!(position(Way::ToCore, MessageKind::Bound) < position(Way::ToClient, MessageKind::Open));
    assert!(now(&mut wait).is_ok());
}

#[test]
fn a_bind_the_client_rejects_ends_never_registered_client_refused_with_no_effect() {
    let w = World::new();
    // The core's registry has the call; the client never sent it.
    w.core.call("ghost", None);
    let op = now(w
        .handle
        .admit(OperationRef::new("ghost").unwrap())
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    w.settle();
    assert_eq!(
        op.registration(),
        Registration::NeverRegistered(TerminalReason::ClientRefused(ErrorCode::InvalidRequest))
    );
    let rejected = w.carrier.sent(Way::ToCore, MessageKind::BindRejected);
    assert_eq!(rejected.len(), 1);
    assert_eq!(
        rejected[0].1.bind_rejected.as_ref().unwrap().effect,
        Effect::None
    );
    assert!(op.open(exchange_spec()).is_err());
    assert_eq!(
        w.rig.world.connects() + w.rig.world.exchanges.load(Ordering::SeqCst),
        0
    );
    let end = now(op.finish());
    assert_eq!(
        end.registration,
        Registration::NeverRegistered(TerminalReason::ClientRefused(ErrorCode::InvalidRequest))
    );
}

#[test]
fn a_client_whose_host_is_closing_answers_the_bind_cancelled() {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    w.close_client_host();
    let op = now(w
        .handle
        .admit(OperationRef::new("call-1").unwrap())
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    w.settle();
    assert_eq!(
        op.registration(),
        Registration::NeverRegistered(TerminalReason::ClientRefused(ErrorCode::Cancelled))
    );
}

#[test]
fn carrier_loss_revokes_a_live_binding_and_a_started_receive_pack_exchange_reports_a_possible_effect()
 {
    let w = World::new();
    let op = push_op(&w, "call-1");
    let mut exchange = open(&w, &op, push_spec());
    write_all(&w, &mut exchange, b"pack");
    w.carrier.end();
    w.remote.pump();
    assert!(matches!(
        exchange.stream().write(b"x"),
        Err(StreamError::CarrierLost {
            effect: Effect::Possible
        })
    ));
    assert!(matches!(
        exchange.stream().read(&mut [0u8; 8]),
        Err(StreamError::CarrierLost {
            effect: Effect::Possible
        })
    ));
    let end = finish(&w, exchange);
    assert_eq!(
        end.failure,
        Some((ErrorCode::CarrierLost, Effect::Possible))
    );
    assert!(end.disposition == Disposition::Discarded);
    let op_end = now(op.finish());
    assert_eq!(
        op_end.revoked,
        Some(TerminalReason::CarrierLost),
        "a live binding is revoked, not never-registered"
    );
    assert!(matches!(op_end.registration, Registration::Registered(_)));
    // The client side: its receiver sees the same end, and its connection is discarded, never pooled.
    w.receiver.pump();
    w.rig.progress.run();
    assert_eq!(w.receiver.bindings(), 0);
    assert_eq!(
        w.host.snapshot().connections.idle,
        0,
        "the connection was not returned to the pool"
    );
}

#[test]
fn carrier_loss_under_a_fetch_exchange_or_an_unsent_push_open_reports_no_effect() {
    let w = World::new();
    let op = bound(&w, "call-1");
    let mut exchange = open(&w, &op, exchange_spec());
    w.carrier.end();
    w.remote.pump();
    assert!(matches!(
        exchange.stream().write(b"x"),
        Err(StreamError::CarrierLost {
            effect: Effect::None
        })
    ));
    drop(exchange);

    // A receive-pack open that never left the core (the binding was still pending) is `Effect::None` too.
    let w = World::new();
    w.call(
        "call-2",
        ServiceClass::Push,
        FETCH,
        &["ssh://example.invalid/"],
    );
    let op = now(w
        .handle
        .admit(OperationRef::new("call-2").unwrap())
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    let mut wait = Box::pin(op.open(push_spec()).unwrap().wait());
    w.carrier.end();
    w.remote.pump();
    match now(&mut wait) {
        Err(OpenFailure::CarrierLost { effect }) => assert_eq!(effect, Effect::None),
        other => panic!("expected CarrierLost, got {other:?}"),
    }
    assert!(w.carrier.sent(Way::ToClient, MessageKind::Open).is_empty());
}

#[test]
fn a_receive_pack_open_sent_but_not_yet_answered_is_possible_on_loss() {
    let w = World::new();
    let op = push_op(&w, "call-1");
    let mut wait = Box::pin(op.open(push_spec()).unwrap().wait());
    w.remote.pump();
    assert_eq!(
        w.carrier.sent(Way::ToClient, MessageKind::Open).len(),
        1,
        "the open reached the lane"
    );
    w.carrier.end();
    w.remote.pump();
    match now(&mut wait) {
        Err(OpenFailure::CarrierLost { effect }) => assert_eq!(effect, Effect::Possible),
        other => panic!("expected CarrierLost, got {other:?}"),
    }
}

#[test]
fn a_frame_for_a_retired_binding_is_discarded_with_no_state_and_other_bindings_continue() {
    let w = World::new();
    let first = bound(&w, "call-1");
    let second = bound(&w, "call-2");
    let session = w.carrier.sent(Way::ToCore, MessageKind::Bound)[0]
        .1
        .session_id
        .clone();
    let end = now(first.finish());
    w.settle();
    assert!(matches!(end.registration, Registration::Registered(_)));
    assert_eq!(w.remote.bindings(), 1, "the retired binding left the table");
    let stray = gwz_transport::protocol::Envelope {
        version: 2,
        session_id: session,
        stream_id: 1,
        kind: MessageKind::Data,
        data: Some(gwz_transport::protocol::Data {
            offset: 0,
            payload: vec![1, 2, 3],
        }),
        ..Default::default()
    };
    let before = w.remote.discarded();
    w.carrier.inject(Way::ToCore, ("call-1".into(), stray));
    w.settle();
    assert_eq!(w.remote.discarded(), before + 1);
    assert_eq!(w.remote.bindings(), 1, "no state was made for it");
    let (_, response, end) = round_trip(&w, &second, exchange_spec(), b"still works");
    assert_eq!(response, b"still works");
    assert!(end.failure.is_none());
}

#[test]
fn a_frame_that_breaks_one_bindings_rules_faults_that_binding_only() {
    let w = World::new();
    let first = bound(&w, "call-1");
    let second = bound(&w, "call-2");
    let mut exchange = open(&w, &first, exchange_spec());
    let session = w.carrier.sent(Way::ToCore, MessageKind::Bound)[0]
        .1
        .session_id
        .clone();
    // A `data` frame under a request name that is not this binding's call.
    let hostile = gwz_transport::protocol::Envelope {
        version: 2,
        session_id: session,
        stream_id: 1,
        kind: MessageKind::Window,
        window: Some(gwz_transport::protocol::Window { max_offset: 1 }),
        ..Default::default()
    };
    w.carrier.inject(Way::ToCore, ("call-2".into(), hostile));
    w.settle();
    assert!(matches!(
        exchange.stream().write(b"x"),
        Err(StreamError::Failed {
            code: ErrorCode::Protocol,
            ..
        })
    ));
    let (_, response, _) = round_trip(&w, &second, exchange_spec(), b"unaffected");
    assert_eq!(response, b"unaffected");
}

#[test]
fn a_late_frame_for_a_retired_stream_under_another_calls_name_faults_the_binding() {
    // The transport lets a late frame for a retired stream pass silently; the name it travels under is still the
    // lane's rule (GwzSplitCommandChannel 3.3: a request id names the call that bound), so it faults that binding.
    let w = World::new();
    let first = bound(&w, "call-1");
    let second = bound(&w, "call-2");
    let session = w.carrier.sent(Way::ToCore, MessageKind::Bound)[0]
        .1
        .session_id
        .clone();
    let (_, response, _) = round_trip(&w, &first, exchange_spec(), b"done");
    assert_eq!(response, b"done");
    let mut live = open(&w, &first, exchange_spec());
    let late = gwz_transport::protocol::Envelope {
        version: 2,
        session_id: session,
        stream_id: 1,
        kind: MessageKind::Window,
        window: Some(gwz_transport::protocol::Window { max_offset: 1 }),
        ..Default::default()
    };
    w.carrier.inject(Way::ToCore, ("call-2".into(), late));
    w.settle();
    assert!(matches!(
        live.stream().write(b"x"),
        Err(StreamError::Failed {
            code: ErrorCode::Protocol,
            ..
        })
    ));
    let (_, response, _) = round_trip(&w, &second, exchange_spec(), b"other binding");
    assert_eq!(response, b"other binding");
}

#[test]
fn cancelling_an_open_exchange_reaches_the_client_and_discards_its_connection() {
    let w = World::new();
    let op = bound(&w, "call-1");
    let mut exchange = open(&w, &op, exchange_spec());
    write_all(&w, &mut exchange, b"partial");
    assert_eq!(op.cancel_handle().cancel(), CancelState::Cancelled);
    w.settle();
    assert_eq!(
        w.host.snapshot().connections.leased,
        0,
        "the client released the connection"
    );
    assert_eq!(
        w.host.snapshot().connections.idle,
        0,
        "and discarded it, not pooled it"
    );
    drop(exchange);
}

#[test]
fn the_client_ending_the_call_fails_the_cores_open_exchanges_cancelled() {
    let w = World::new();
    let op = bound(&w, "call-1");
    let mut exchange = open(&w, &op, exchange_spec());
    write_all(&w, &mut exchange, b"partial");
    w.receiver.end_call("call-1");
    w.settle();
    assert!(matches!(
        exchange.stream().write(b"x"),
        Err(StreamError::Cancelled)
    ));
    assert_eq!(w.receiver.bindings(), 0);
    assert_eq!(
        w.host.snapshot().connections.leased,
        0,
        "the client's lease went with its binding"
    );
}

fn try_open(w: &World, op: &Operation, spec: ExchangeSpec) -> Result<Exchange, OpenFailure> {
    let mut wait = Box::pin(op.open(spec).unwrap().wait());
    w.settle();
    now(&mut wait)
}
fn assert_refused_end_to_end(result: Result<Exchange, OpenFailure>) {
    match result {
        Err(OpenFailure::Refused(failure)) => {
            assert_eq!(
                (failure.code, failure.effect),
                (ErrorCode::InvalidRequest, Effect::None)
            );
            assert!(
                failure.facts.is_none(),
                "the core learns only that it was refused"
            );
        }
        other => panic!(
            "expected the client's refusal, got {:?}",
            other.map(|_| "an exchange")
        ),
    }
}

#[test]
fn a_core_chosen_destination_outside_the_allowed_set_is_refused_end_to_end_and_the_fixture_is_untouched()
 {
    let w = World::new();
    let op = bound(&w, "call-1");
    let elsewhere = ExchangeSpec::git(
        Key::ssh("git", "evil.invalid", 22),
        Identity::Ambient,
        GitService::UploadPackExchange,
        "/srv/repo.git",
    )
    .unwrap();
    assert_refused_end_to_end(try_open(&w, &op, elsewhere));
    // A receive-pack open under a fetch-class request, to a host the fetch allowance does cover.
    assert_refused_end_to_end(try_open(&w, &op, push_spec()));
    assert_eq!(
        w.rig.world.connects() + w.rig.world.exchanges.load(Ordering::SeqCst),
        0
    );
    // The binding is still good: the refusal was per open.
    let (_, response, _) = round_trip(&w, &op, exchange_spec(), b"ok");
    assert_eq!(response, b"ok");
}

#[test]
fn a_dry_run_push_refuses_a_receive_pack_open_even_with_a_push_allowance() {
    let w = World::new();
    w.call(
        "call-1",
        ServiceClass::Plan,
        FETCH,
        &["ssh://example.invalid/"],
    );
    let op = now(w
        .handle
        .admit(OperationRef::new("call-1").unwrap())
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    w.settle();
    assert!(
        matches!(op.registration(), Registration::Registered(_)),
        "a plan-class request still binds"
    );
    assert_refused_end_to_end(try_open(&w, &op, push_spec()));
    assert_refused_end_to_end(try_open(&w, &op, exchange_spec()));
    assert_eq!(
        w.rig.world.exchanges.load(Ordering::SeqCst),
        0,
        "the fixture remote is unchanged"
    );
}

#[test]
fn an_explicit_key_crosses_only_when_the_invocation_named_it_and_its_path_base() {
    use super::super::Invocation;
    use super::world::{key, prepared};
    let w = World::new();
    let mut named = Invocation::new(prepared("call-1", 1), ServiceClass::Fetch);
    named.fetch = super::super::AllowedSet::parse(FETCH).unwrap();
    named.named_keys = vec![key("/home/me/.ssh/id_work")];
    named.path_base = Some("/home/me".into());
    w.call_with("call-1", named, Some("/home/me"));
    let op = now(w
        .handle
        .admit(OperationRef::new("call-1").unwrap())
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    w.settle();
    let explicit = |path: &str| {
        ExchangeSpec::git(
            Key::ssh("git", "example.invalid", 22),
            Identity::Explicit(path.into()),
            GitService::UploadPackExchange,
            "/srv/repo.git",
        )
        .unwrap()
    };
    assert_refused_end_to_end(try_open(&w, &op, explicit("/home/me/.ssh/id_core_chose")));
    assert_eq!(w.rig.world.exchanges.load(Ordering::SeqCst), 0);
    let (_, response, _) = round_trip(&w, &op, explicit("/home/me/.ssh/id_work"), b"signed");
    assert_eq!(response, b"signed");
    // Ambient is not admitted when the invocation named a key.
    assert_refused_end_to_end(try_open(&w, &op, exchange_spec()));
}

#[test]
fn an_https_exchange_maps_through_the_table_and_crosses() {
    let w = World::new();
    let op = bound(&w, "call-1");
    let (_, response, end) = round_trip(&w, &op, crate::fakes::https_spec(), b"GET /info/refs");
    assert_eq!(response, b"GET /info/refs");
    assert_eq!(end.disposition, Disposition::Reusable);
    let opens = w.carrier.sent(Way::ToClient, MessageKind::Open);
    let open = opens[0].1.open.as_ref().unwrap();
    assert_eq!(open.policy, gwz_transport::protocol::AuthPolicy::Anonymous);
    assert_eq!(
        open.identity.mode,
        gwz_transport::protocol::IdentityMode::CredentialsDisabled
    );
    assert_eq!(
        open.operation_id, "call-1",
        "the open names the call, nothing the core built about the client"
    );
}

#[test]
fn a_third_call_beyond_the_clients_waiting_bound_is_bind_rejected_capacity_and_the_second_waits() {
    use crate::HostLimits;
    let w = World::with_host(
        super::super::RemoteConfig::default(),
        HostLimits {
            active_slots: 1,
            waiting_admissions: 1,
            ..HostLimits::default()
        },
    );
    let first = bound(&w, "call-1");
    w.call("call-2", ServiceClass::Fetch, FETCH, &[]);
    w.call("call-3", ServiceClass::Fetch, FETCH, &[]);
    let second = now(w
        .handle
        .admit(OperationRef::new("call-2").unwrap())
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    let third = now(w
        .handle
        .admit(OperationRef::new("call-3").unwrap())
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    w.settle();
    assert_eq!(
        second.registration(),
        Registration::Pending,
        "waiting for the client's slot"
    );
    assert_eq!(
        third.registration(),
        Registration::NeverRegistered(TerminalReason::ClientRefused(ErrorCode::Capacity))
    );
    let _first_end = now(first.finish());
    // The client's binding lives until its call ends (the reply reached it): then its slot goes to the waiter.
    w.settle();
    assert_eq!(second.registration(), Registration::Pending);
    w.receiver.end_call("call-1");
    w.settle();
    assert!(
        matches!(second.registration(), Registration::Registered(_)),
        "the slot went to the waiter"
    );
}

#[test]
fn the_drivers_progress_both_ends_with_no_application_call() {
    // Each end spawned its own driver on its own executor (`serve_bindings` at once, the core's at its first admit);
    // the rows below step the two executors and nothing else.
    let w = World::new();
    assert_eq!(w.client_progress.spawned(), 1);
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let admission = w
        .handle
        .admit(OperationRef::new("call-1").unwrap())
        .unwrap();
    let op = now(admission.acquire()).unwrap().register();
    assert_eq!(w.core_progress.spawned(), 1);
    // One driver serves the handle however many operations it admits.
    w.call("call-2", ServiceClass::Fetch, FETCH, &[]);
    let second = now(w
        .handle
        .admit(OperationRef::new("call-2").unwrap())
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    assert_eq!(w.core_progress.spawned(), 1);
    // No `pump`, `settle` or `registration()`: only the two executors run, and the owner's.
    for _ in 0..10 {
        w.core_progress.run();
        w.client_progress.run();
        w.rig.progress.run();
    }
    assert_eq!(
        w.carrier.sent(Way::ToCore, MessageKind::Bound).len(),
        2,
        "the receiver answered both calls on its own"
    );
    // A carrier that ends ends both drivers (the core's once its last binding is gone).
    w.carrier.end();
    drop((op, second));
    for _ in 0..10 {
        w.core_progress.run();
        w.client_progress.run();
        w.rig.progress.run();
    }
    assert_eq!(
        (w.core_progress.pending(), w.client_progress.pending()),
        (0, 0)
    );
}

#[test]
fn a_bind_rejected_under_another_calls_name_is_a_fault_of_the_binding_not_a_refusal() {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let op = now(w
        .handle
        .admit(OperationRef::new("call-1").unwrap())
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    let session = w.carrier.sent(Way::ToClient, MessageKind::Bind)[0]
        .1
        .session_id
        .clone();
    let mut forged =
        super::super::receiver::rejection("someone-else", &session, ErrorCode::InvalidRequest);
    forged.0 = "someone-else".into();
    w.carrier.inject(Way::ToCore, forged);
    w.remote.pump();
    assert_eq!(
        op.registration(),
        Registration::NeverRegistered(TerminalReason::ClientRefused(ErrorCode::Protocol)),
        "a rejection for another call is not this binding's refusal"
    );
}
