//! Cancellation in every stage (step 2.2c remediation round 1: State P1-1 = Code P2-1, Code P2-2): whatever the stage of
//! an operation's openings and exchanges, and whatever the client answers or the carrier does, every wait resolves, its
//! `is_ready` agrees with its poll, and the binding retires.
//!
//! The matrix cancel x {queued, opening, open, closing} x {pending, registered} has these live cells: pending x queued
//! (the two pending rows), registered x opening (the three answer rows and the late-opened row), registered x open and
//! registered x closing (the last two rows). Pending x opening/open/closing do not exist: an `Open` is built only once
//! `bound` has registered the binding. Registered x queued exists only while the binding's mux has no room for the
//! `Open`, which one operation on a binding of 64 streams never reaches.
use super::{
    super::ServiceClass,
    core_rows::{FETCH, bound, open, register, write_all},
    world::{Count, Way, World},
};
use crate::{
    CancelState, ExchangeSpec, GitService, OpenFailure, Operation, Registration, TerminalReason,
    fakes::{exchange_spec, poll_once},
};
use gwz_transport::{
    pool::{Identity, Key},
    protocol::{ErrorCode, MessageKind},
};
use std::task::Poll;

/// A receive-pack exchange: a fetch-class call's client refuses it (`open_failed`).
fn push_spec() -> ExchangeSpec {
    ExchangeSpec::git(
        Key::ssh("git", "example.invalid", 22),
        Identity::Ambient,
        GitService::ReceivePackExchange,
        "/srv/repo.git",
    )
    .unwrap()
}

/// Settles, finishes `op` and asserts its binding went: the cancel left nothing behind.
fn assert_retires(w: &World, op: Operation) {
    w.settle();
    let mut finish = Box::pin(op.finish());
    w.settle();
    assert!(
        matches!(poll_once(&mut finish), Poll::Ready(_)),
        "the operation's receipt resolves"
    );
    assert_eq!(w.remote.bindings(), 0, "the binding retired");
}

#[derive(Clone, Copy, Debug)]
enum Answer {
    Opened,
    OpenFailed,
    CarrierEnds,
}

/// Bind, open, let the `Open` leave, cancel while it is unanswered, then the client answers `answer`.
fn cancel_while_the_open_is_unanswered(answer: Answer) {
    let w = World::new();
    let op = bound(&w, "call-1");
    let spec = match answer {
        Answer::OpenFailed => push_spec(),
        _ => exchange_spec(),
    };
    let task = Count::new();
    let mut wait = Box::pin(op.open(spec).unwrap().wait());
    assert!(task.poll(&mut wait).is_pending());
    w.remote.pump();
    assert_eq!(w.carrier.sent(Way::ToClient, MessageKind::Open).len(), 1);
    let woken = task.wakes();
    assert_eq!(op.cancel_handle().cancel(), CancelState::Cancelled);
    assert!(
        task.wakes() > woken,
        "{answer:?}: cancel wakes the waiter of the opening it cancelled"
    );
    match answer {
        Answer::CarrierEnds => {
            w.carrier.end();
            w.remote.pump();
        }
        _ => w.settle(),
    }
    let ready = wait.is_ready();
    let polled = task.poll(&mut wait);
    assert_eq!(ready, polled.is_ready(), "{answer:?}: is_ready agrees");
    match (answer, polled) {
        (Answer::Opened | Answer::OpenFailed, Poll::Ready(Err(OpenFailure::Cancelled))) => {}
        (Answer::CarrierEnds, Poll::Ready(Err(OpenFailure::CarrierLost { .. }))) => {}
        (_, other) => panic!("{answer:?}: the cancelled opening ended {other:?}"),
    }
    drop(wait);
    assert_retires(&w, op);
}

#[test]
fn a_cancel_while_an_open_is_unanswered_resolves_cancelled_when_the_client_answers_opened() {
    cancel_while_the_open_is_unanswered(Answer::Opened);
}

#[test]
fn a_cancel_while_an_open_is_unanswered_resolves_cancelled_when_the_client_answers_open_failed() {
    cancel_while_the_open_is_unanswered(Answer::OpenFailed);
}

#[test]
fn a_cancel_while_an_open_is_unanswered_resolves_carrier_lost_when_the_carrier_ends() {
    cancel_while_the_open_is_unanswered(Answer::CarrierEnds);
}

#[test]
fn a_late_opened_after_a_cancel_is_answered_with_a_cancel_and_the_stream_slot_goes() {
    // The distinct cancel-when-opened flag: the exchange is not reclaimed while its handle lives, and the client that
    // opened it is told to stop.
    let w = World::new();
    let op = bound(&w, "call-1");
    let mut wait = Box::pin(op.open(exchange_spec()).unwrap().wait());
    w.remote.pump();
    assert_eq!(op.cancel_handle().cancel(), CancelState::Cancelled);
    w.settle();
    assert_eq!(
        w.carrier.sent(Way::ToClient, MessageKind::Cancel).len(),
        1,
        "the late opened is answered with cancel"
    );
    assert!(matches!(
        poll_once(&mut wait),
        Poll::Ready(Err(OpenFailure::Cancelled))
    ));
    drop(wait);
    assert_retires(&w, op);
}

#[test]
fn a_cancel_of_a_pending_operation_ends_its_queued_opening_cancelled() {
    // Cancel: the registration is decided by the cancel, the queued opening is cancelled (never a fault of the binding).
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let op = register(&w, "call-1");
    let task = Count::new();
    let mut wait = Box::pin(op.open(exchange_spec()).unwrap().wait());
    assert!(task.poll(&mut wait).is_pending());
    assert_eq!(op.cancel_handle().cancel(), CancelState::Cancelled);
    assert_eq!(
        op.registration(),
        Registration::NeverRegistered(TerminalReason::Cancelled)
    );
    assert!(task.wakes() >= 1);
    let ready = wait.is_ready();
    let polled = task.poll(&mut wait);
    assert_eq!(ready, polled.is_ready());
    assert!(
        matches!(polled, Poll::Ready(Err(OpenFailure::Cancelled))),
        "the queued opening of a cancelled pending operation ended {polled:?}"
    );
    drop(wait);
    assert_retires(&w, op);
}

#[test]
fn a_finish_of_a_pending_operation_ends_its_queued_opening_cancelled() {
    let w = World::new();
    w.call("call-1", ServiceClass::Fetch, FETCH, &[]);
    let op = register(&w, "call-1");
    let mut wait = Box::pin(op.open(exchange_spec()).unwrap().wait());
    w.remote.pump();
    let mut finish = Box::pin(op.finish());
    let polled = poll_once(&mut wait);
    assert!(
        matches!(polled, Poll::Ready(Err(OpenFailure::Cancelled))),
        "the queued opening of a finished pending operation ended {polled:?}"
    );
    drop(wait);
    w.settle();
    let Poll::Ready(end) = poll_once(&mut finish) else {
        panic!("the receipt of the finished operation never resolved");
    };
    assert_eq!(
        end.registration,
        Registration::NeverRegistered(TerminalReason::Cancelled)
    );
    assert_eq!(w.remote.bindings(), 0);
}

#[test]
fn a_cancel_of_an_open_exchange_ends_its_finish_cancelled_and_the_binding_retires() {
    let w = World::new();
    let op = bound(&w, "call-1");
    let mut exchange = open(&w, &op, exchange_spec());
    write_all(&w, &mut exchange, b"partial");
    assert_eq!(op.cancel_handle().cancel(), CancelState::Cancelled);
    w.settle();
    let mut finish = Box::pin(exchange.finish());
    w.settle();
    let Poll::Ready(end) = poll_once(&mut finish) else {
        panic!("the finish of a cancelled exchange never resolved");
    };
    assert_eq!(end.failure.map(|f| f.0), Some(ErrorCode::Cancelled));
    drop(finish);
    assert_retires(&w, op);
}

#[test]
fn a_cancel_of_a_closing_exchange_resolves_its_finish_and_the_binding_retires() {
    let w = World::new();
    let op = bound(&w, "call-1");
    let mut exchange = open(&w, &op, exchange_spec());
    write_all(&w, &mut exchange, b"partial");
    // The orderly end has started: `close` is out, the client's `closed` not yet in.
    let mut finish = Box::pin(exchange.finish());
    w.remote.pump();
    assert_eq!(op.cancel_handle().cancel(), CancelState::Cancelled);
    w.settle();
    let Poll::Ready(end) = poll_once(&mut finish) else {
        panic!("the finish of a cancelled closing exchange never resolved");
    };
    assert!(
        end.failure.is_none() || end.failure.map(|f| f.0) == Some(ErrorCode::Cancelled),
        "{end:?}"
    );
    drop(finish);
    assert_retires(&w, op);
}

#[test]
fn a_missing_exchange_record_is_a_terminal_answer_for_the_wait_and_for_the_finish_never_a_wait() {
    // Defence in depth: a record that is gone answers at once (the opening `Cancelled`, the finish discarded and, for a
    // push that may have started, `Possible`), and `is_ready` says so. Nothing wakes a wait on a record that is not there.
    let w = World::new();
    let op = bound(&w, "call-1");
    let mut wait = Box::pin(op.open(exchange_spec()).unwrap().wait());
    w.remote.with(|state| {
        for binding in state.bindings.values_mut() {
            binding.exchanges.clear();
        }
    });
    assert!(wait.is_ready());
    assert!(matches!(
        poll_once(&mut wait),
        Poll::Ready(Err(OpenFailure::Cancelled))
    ));
    drop(wait);

    let mut exchange = open(&w, &op, exchange_spec());
    write_all(&w, &mut exchange, b"x");
    let mut finish = Box::pin(exchange.finish());
    w.remote.with(|state| {
        for binding in state.bindings.values_mut() {
            binding.exchanges.clear();
        }
    });
    let Poll::Ready(end) = poll_once(&mut finish) else {
        panic!("the finish of an exchange with no record waits");
    };
    assert_eq!(
        (end.disposition, end.failure),
        (
            gwz_transport::protocol::Disposition::Discarded,
            Some((
                ErrorCode::Cancelled,
                gwz_transport::protocol::Effect::Possible
            ))
        )
    );
}
