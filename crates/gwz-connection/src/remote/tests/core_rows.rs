//! The core end (`remote::handle`): the plan's red rows for the carrier-backed handle, over the fake carrier.
use super::{
    super::{ServiceClass, TableRefusal},
    world::{Way, World},
};
use crate::{
    AdmissionError, CancelState, Exchange, ExchangeEnd, ExchangeSpec, GitService, OpenError,
    OpenFailure, Operation, OperationRef, Registration, StreamError, TerminalReason,
    fakes::{exchange_spec, https_spec, now},
};
use gwz_transport::{
    pool::{Identity, Key},
    protocol::{Disposition, Effect, ErrorCode, MessageKind},
};
use std::sync::atomic::Ordering;

pub(super) const FETCH: &[&str] = &["ssh://example.invalid/", "https://example.invalid/"];

pub(super) fn register(w: &World, request: &str) -> Operation {
    let admission = w.handle.admit(OperationRef::new(request).unwrap()).unwrap();
    now(admission.acquire()).unwrap().register()
}
/// An operation on a fetch-class call whose bind is answered.
pub(super) fn bound(w: &World, request: &str) -> Operation {
    w.call(request, ServiceClass::Fetch, FETCH, &[]);
    let op = register(w, request);
    w.settle();
    assert!(
        matches!(op.registration(), Registration::Registered(_)),
        "{:?}",
        op.registration()
    );
    op
}
pub(super) fn open(w: &World, op: &Operation, spec: ExchangeSpec) -> Exchange {
    let mut wait = Box::pin(op.open(spec).unwrap().wait());
    w.settle();
    match now(&mut wait) {
        Ok(exchange) => exchange,
        Err(failure) => panic!("the opening failed: {failure:?}"),
    }
}
/// Whether a stream call said "not yet" (the local `ExchangeIo` rule: it returns `WouldBlock`, never blocks).
pub(super) fn would_block(error: &StreamError) -> bool {
    matches!(error, StreamError::Io(e) if e.kind() == std::io::ErrorKind::WouldBlock)
}
/// Reads until `wanted` bytes have arrived. The shared stream has no end-of-request call: an exchange ends when it is
/// finished, as a local one does.
pub(super) fn read_exact(w: &World, exchange: &mut Exchange, wanted: usize) -> Vec<u8> {
    let mut out = Vec::new();
    let mut buffer = [0u8; 4096];
    for _ in 0..4000 {
        if out.len() >= wanted {
            return out;
        }
        match exchange.stream().try_read(&mut buffer) {
            Ok(count) => out.extend_from_slice(&buffer[..count]),
            Err(error) if would_block(&error) => w.settle(),
            Err(error) => panic!("read: {error:?}"),
        }
    }
    panic!("the stream stopped at {} of {wanted} bytes", out.len());
}
pub(super) fn write_all(w: &World, exchange: &mut Exchange, data: &[u8]) {
    let mut rest = data;
    for _ in 0..400 {
        if rest.is_empty() {
            return;
        }
        match exchange.stream().try_write(rest) {
            Ok(count) => rest = &rest[count..],
            Err(error) if would_block(&error) => w.settle(),
            Err(error) => panic!("write: {error:?}"),
        }
    }
    panic!("the write never completed");
}
pub(super) fn finish(w: &World, exchange: Exchange) -> ExchangeEnd {
    let mut finish = Box::pin(exchange.finish());
    for _ in 0..50 {
        w.settle();
        if let std::task::Poll::Ready(end) = crate::fakes::poll_once(&mut finish) {
            return end;
        }
    }
    panic!("the exchange never ended");
}
/// A whole exchange: the request, its end, the echoed response, an orderly close.
pub(super) fn round_trip(
    w: &World,
    op: &Operation,
    spec: ExchangeSpec,
    request: &[u8],
) -> (bool, Vec<u8>, ExchangeEnd) {
    let mut exchange = open(w, op, spec);
    let reused = exchange.reused();
    write_all(w, &mut exchange, request);
    let response = read_exact(w, &mut exchange, request.len());
    (reused, response, finish(w, exchange))
}

#[test]
fn two_operations_over_the_remote_handle_reuse_one_fake_connection() {
    let w = World::new();
    let first = bound(&w, "call-1");
    let (reused, response, end) = round_trip(&w, &first, exchange_spec(), b"want 1");
    assert!(!reused);
    assert_eq!(
        response, b"want 1",
        "the bytes crossed the lane and came back"
    );
    assert_eq!(
        (end.disposition, end.failure),
        (Disposition::Reusable, None)
    );
    drop(first.finish());
    w.settle();

    let second = bound(&w, "call-2");
    let (reused, response, end) = round_trip(&w, &second, exchange_spec(), b"want 2");
    assert!(
        reused,
        "the owner leased the idle connection the first operation left"
    );
    assert_eq!(response, b"want 2");
    assert_eq!(end.disposition, Disposition::Reusable);
    assert_eq!(
        w.rig.world.connects(),
        1,
        "one connection served both operations"
    );
    assert_eq!(w.rig.world.exchanges.load(Ordering::SeqCst), 2);
    // The core never named the endpoint's profile: the owner built it from the client's invocation.
    assert_eq!(w.rig.world.created.load(Ordering::SeqCst), 1);
}

#[test]
fn the_client_builds_the_operation_so_the_core_supplies_no_profile_and_a_foreign_policy_is_refused_core_side()
 {
    // `admit` takes the call's name and nothing else: there is no field to refuse because there is none to fill.
    let w = World::new();
    let op = bound(&w, "call-1");
    assert_eq!(
        w.rig.world.inputs.lock().unwrap().len(),
        1,
        "the factory saw the client's profile only"
    );
    // An SSH-only client advertises no HTTPS in `bound`; an HTTPS spec is refused core-side, nothing sent.
    let w = World::new();
    w.call_with(
        "call-1",
        super::super::Invocation::new(super::world::ssh_only(), ServiceClass::Fetch),
        None,
    );
    let op2 = register(&w, "call-1");
    w.settle();
    assert!(matches!(op2.registration(), Registration::Registered(_)));
    let opens_before = w.carrier.sent(Way::ToClient, MessageKind::Open).len();
    let mut wait = Box::pin(op2.open(https_spec()).unwrap().wait());
    w.settle();
    match now(&mut wait) {
        Err(OpenFailure::Fault(ErrorCode::InvalidRequest)) => {}
        other => panic!("expected a core-side refusal, got {other:?}"),
    }
    assert_eq!(
        w.carrier.sent(Way::ToClient, MessageKind::Open).len(),
        opens_before,
        "nothing was sent"
    );
    assert_eq!(w.rig.world.exchanges.load(Ordering::SeqCst), 0);
    drop(op);
}

#[test]
fn an_exchange_spec_with_no_row_in_the_closed_table_is_refused_core_side_and_nothing_is_sent() {
    let w = World::new();
    let op = bound(&w, "call-1");
    let sent = w.carrier.log().len();
    let key = |user: Option<&str>, https: bool| {
        let mut key = if https {
            Key::https("example.invalid", 443)
        } else {
            Key::ssh("git", "example.invalid", 22)
        };
        key.username = user.map(str::to_owned);
        key
    };
    let cases = [
        // SSH has exchanges only; an advertisement is smart HTTP.
        (
            ExchangeSpec::git(
                key(Some("git"), false),
                Identity::Ambient,
                GitService::UploadPackAdvertisement,
                "/r",
            )
            .unwrap(),
            TableRefusal::SshAdvertisement,
        ),
        (
            ExchangeSpec::git(
                key(Some("git"), false),
                Identity::Ambient,
                GitService::ReceivePackAdvertisement,
                "/r",
            )
            .unwrap(),
            TableRefusal::SshAdvertisement,
        ),
        // An explicit key needs the path base the client sent; this call sent none.
        (
            ExchangeSpec::git(
                key(Some("git"), false),
                Identity::Explicit("k".into()),
                GitService::UploadPackExchange,
                "/r",
            )
            .unwrap(),
            TableRefusal::PathBase,
        ),
        // A scoped HTTPS identity is minted by the client's endpoint; the core cannot name one.
        (
            ExchangeSpec::git(
                key(None, true),
                Identity::HttpsScoped("s".into()),
                GitService::UploadPackExchange,
                "/r",
            )
            .unwrap(),
            TableRefusal::Identity,
        ),
        // An identity of the other scheme.
        (
            ExchangeSpec::git(
                key(Some("git"), false),
                Identity::Https,
                GitService::UploadPackExchange,
                "/r",
            )
            .unwrap(),
            TableRefusal::Identity,
        ),
        (
            ExchangeSpec::git(
                key(None, true),
                Identity::Ambient,
                GitService::UploadPackExchange,
                "/r",
            )
            .unwrap(),
            TableRefusal::Identity,
        ),
    ];
    for (spec, refusal) in cases {
        assert_eq!(op.open(spec).err(), Some(OpenError::Refused(refusal)));
    }
    w.settle();
    assert_eq!(
        w.carrier.log().len(),
        sent,
        "refused core-side: nothing reached the lane"
    );
    // The typed constructor already refuses what could not be a Git path (a dash-led path, NUL, empty).
    assert!(
        ExchangeSpec::git(
            key(Some("git"), false),
            Identity::Ambient,
            GitService::UploadPackExchange,
            "-oProxyCommand=x"
        )
        .is_err()
    );
}

#[test]
fn a_pending_register_whose_carrier_drops_ends_never_registered_with_zero_effects() {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let op = register(&w, "call-1");
    assert_eq!(
        op.registration(),
        Registration::Pending,
        "register is a deferred commit"
    );
    assert_eq!(
        w.carrier.sent(Way::ToClient, MessageKind::Bind).len(),
        1,
        "one bind, role driver, stream 0"
    );
    let bind = &w.carrier.sent(Way::ToClient, MessageKind::Bind)[0];
    assert_eq!(bind.1.stream_id, 0);
    assert_eq!(bind.0, "call-1");
    // The carrier ends before the client answers.
    w.carrier.end();
    w.remote.pump();
    assert_eq!(
        op.registration(),
        Registration::NeverRegistered(TerminalReason::CarrierLost)
    );
    assert_eq!(
        op.open(exchange_spec()).err(),
        Some(OpenError::Terminal {
            reason: TerminalReason::CarrierLost,
            effect: Effect::None
        })
    );
    let end = now(op.finish());
    assert_eq!(
        end.registration,
        Registration::NeverRegistered(TerminalReason::CarrierLost)
    );
    assert_eq!((end.exchanges_opened, end.revoked), (0, None));
    assert_eq!(
        w.rig.world.connects(),
        0,
        "zero effects on the client's side"
    );
    assert_eq!(w.rig.world.exchanges.load(Ordering::SeqCst), 0);
    // A permit acquired before the loss registers as the same terminal operation.
    let late = w.handle.admit(OperationRef::new("call-2").unwrap());
    assert!(matches!(late, Err(AdmissionError::CarrierLost)));
}

#[test]
fn snapshot_is_refused_and_carries_no_counts() {
    let w = World::new();
    let _op = bound(&w, "call-1");
    let snapshot = w.handle.snapshot();
    assert!(
        snapshot.refused(),
        "the carrier-backed handle refuses to count"
    );
    assert_eq!(
        (
            snapshot.bindings,
            snapshot.active_operations,
            snapshot.waiting_admissions,
            snapshot.connections.idle + snapshot.connections.leased,
        ),
        (0, 0, 0, 0),
        "and carries no counts, though a binding is live"
    );
    assert_eq!(snapshot.phase, crate::HostPhase::Open);
    // Across the carrier's loss it is still a refusal, and its phase says what the handle is: closed.
    w.carrier.end();
    w.remote.pump();
    let lost = w.handle.snapshot();
    assert!(lost.refused(), "still a refusal after the loss");
    assert_eq!(
        (
            lost.bindings,
            lost.active_operations,
            lost.waiting_admissions
        ),
        (0, 0, 0)
    );
    assert_eq!(
        lost.phase,
        crate::HostPhase::Closed,
        "a lost carrier is not an open handle"
    );
}

#[test]
fn a_permit_granted_before_the_carrier_ended_registers_a_terminal_operation() {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let admission = w
        .handle
        .admit(OperationRef::new("call-1").unwrap())
        .unwrap();
    let permit = now(admission.acquire()).unwrap();
    w.carrier.end();
    w.remote.pump();
    let op = permit.register();
    assert_eq!(
        op.registration(),
        Registration::NeverRegistered(TerminalReason::CarrierLost)
    );
    assert!(
        w.carrier.sent(Way::ToClient, MessageKind::Bind).is_empty(),
        "nothing was queued on an ended carrier"
    );
}

#[test]
fn admit_is_local_only_a_live_call_with_one_operation() {
    let w = World::new();
    let before = w.carrier.log().len();
    assert!(matches!(
        w.handle.admit(OperationRef::new("ghost").unwrap()),
        Err(AdmissionError::UnknownCall)
    ));
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let admission = w
        .handle
        .admit(OperationRef::new("call-1").unwrap())
        .unwrap();
    assert!(matches!(
        w.handle.admit(OperationRef::new("call-1").unwrap()),
        Err(AdmissionError::DuplicateOperation)
    ));
    let _permit = now(admission.acquire()).unwrap();
    assert_eq!(
        w.carrier.log().len(),
        before,
        "admit and acquire do no carrier I/O"
    );
    assert!(OperationRef::new("").is_err() && OperationRef::new("a\nb").is_err());
}

#[test]
fn acquire_waits_for_a_core_side_slot_in_order_and_times_out_on_the_admission_bound() {
    use super::super::RemoteConfig;
    let w = World::with(RemoteConfig {
        active_operations: 1,
        waiting_admissions: 2,
        admission_bound_ms: 1_000,
        ..Default::default()
    });
    let first = bound(&w, "a");
    for name in ["b", "c", "d"] {
        w.call(name, ServiceClass::Fetch, FETCH, &[]);
    }
    let mut second = Box::pin(
        w.handle
            .admit(OperationRef::new("b").unwrap())
            .unwrap()
            .acquire(),
    );
    let mut third = Box::pin(
        w.handle
            .admit(OperationRef::new("c").unwrap())
            .unwrap()
            .acquire(),
    );
    assert!(
        crate::fakes::poll_once(&mut second).is_pending(),
        "the one slot is held"
    );
    assert!(crate::fakes::poll_once(&mut third).is_pending());
    assert!(
        matches!(
            w.handle.admit(OperationRef::new("d").unwrap()),
            Err(AdmissionError::QueueFull)
        ),
        "the waiting records are bounded"
    );
    // The first operation ends and its slot goes to the first waiter, not the second.
    drop(first.finish());
    w.settle();
    let granted = crate::fakes::poll_once(&mut second);
    assert!(
        matches!(granted, std::task::Poll::Ready(Ok(_))),
        "FIFO: the first waiter gets the slot"
    );
    assert!(crate::fakes::poll_once(&mut third).is_pending());
    // The slot is held again; the second waiter's bound passes.
    w.rig.clock.advance(2_000);
    assert!(matches!(
        crate::fakes::poll_once(&mut third),
        std::task::Poll::Ready(Err(AdmissionError::AdmissionTimeout))
    ));
    drop(granted);
}

#[test]
fn cancelling_an_operation_cancels_its_exchanges_and_a_later_cancel_finds_it_gone() {
    let w = World::new();
    let op = bound(&w, "call-1");
    let mut exchange = open(&w, &op, exchange_spec());
    write_all(&w, &mut exchange, b"x");
    let cancel = op.cancel_handle();
    assert_eq!(cancel.cancel(), CancelState::Cancelled);
    assert_eq!(cancel.cancel(), CancelState::AlreadyCancelled);
    w.settle();
    assert!(matches!(
        exchange.stream().try_write(b"y"),
        Err(StreamError::Cancelled)
    ));
    drop(exchange);
    let end = now(op.finish());
    assert!(end.cancelled);
    w.settle();
    assert_eq!(cancel.cancel(), CancelState::Gone);
}
