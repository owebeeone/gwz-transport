//! `Exchange::response_head` and the effect of an exchange's request (Phase 1 remediation R2a-2; contract sections 7.2
//! and 9, HTTPS model A7): the head resolves once, an SSH exchange has none, a dropped wait keeps it, a start failure and
//! an exchange's end carry the adapter's `Effect`, and the defaults claim none.
use crate::fakes::{Rig, auth, exchange_spec, https_spec, now, prepared_with};
use crate::{
    AuthContext, Completion, ConnectCommand, EndpointAdapter, Exchange, ExchangeIo, ExchangeSpec,
    HostOwner, HttpResponseHead, InvalidInput, OpenFailure, Operation, StartError, StreamError,
    TerminalReason,
};
use gwz_transport::{
    pool::ConnectionId,
    protocol::{Disposition, Effect, ErrorCode, Failure},
};
use std::{
    io,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

struct Count(AtomicUsize);
impl Wake for Count {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

fn head(status: u16) -> HttpResponseHead {
    HttpResponseHead::new(
        status,
        vec![(
            "Content-Type".into(),
            "application/x-git-upload-pack-result".into(),
        )],
    )
    .unwrap()
}
/// A host and one open exchange for `spec`; bind as `(rig, owner, operation, exchange)` so the exchange drops first.
fn opened(spec: ExchangeSpec) -> (Rig, HostOwner, Operation, Exchange) {
    let rig = Rig::new();
    let (owner, handle) = rig.host(1);
    let operation = now(handle
        .admit(prepared_with("op-head", 1, auth(0, "none")))
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    let exchange = now(operation.open(spec).unwrap().wait()).unwrap();
    (rig, owner, operation, exchange)
}

#[test]
fn response_head_resolves_once_and_then_gives_wrong_state() {
    let (rig, _owner, _operation, mut exchange) = opened(https_spec());
    rig.world.peer(0).send_head(head(200));
    assert_eq!(now(exchange.response_head()).unwrap(), head(200));
    for _ in 0..2 {
        assert!(matches!(
            now(exchange.response_head()),
            Err(StreamError::WrongState)
        ));
    }
}

#[test]
fn response_head_on_ssh_is_wrong_scheme_whatever_the_peer_sent() {
    let (rig, _owner, _operation, mut exchange) = opened(exchange_spec());
    rig.world.peer(0).send_head(head(200));
    for _ in 0..2 {
        assert!(matches!(
            now(exchange.response_head()),
            Err(StreamError::WrongScheme)
        ));
    }
}

#[test]
fn a_dropped_response_head_wait_keeps_the_head_for_a_later_call() {
    let (rig, _owner, _operation, mut exchange) = opened(https_spec());
    let count = Arc::new(Count(AtomicUsize::new(0)));
    let waker = Waker::from(count.clone());
    let mut cx = Context::from_waker(&waker);
    {
        let mut wait = pin!(exchange.response_head());
        assert!(wait.as_mut().poll(&mut cx).is_pending());
        rig.world.peer(0).send_head(head(401));
        assert_eq!(count.0.load(Ordering::SeqCst), 1, "the head woke the wait");
        // The woken wait is dropped before it is polled again.
    }
    assert_eq!(now(exchange.response_head()).unwrap(), head(401));
    assert!(matches!(
        now(exchange.response_head()),
        Err(StreamError::WrongState)
    ));
}

#[test]
fn a_response_head_is_a_bounded_record() {
    let ok =
        |name: &str, value: &str| HttpResponseHead::new(200, vec![(name.into(), value.into())]);
    assert!(ok("Location", "https://example.invalid/a b\tc").is_ok());
    assert_eq!(
        HttpResponseHead::new(99, vec![]),
        Err(InvalidInput::ResponseHead)
    );
    assert_eq!(
        HttpResponseHead::new(600, vec![]),
        Err(InvalidInput::ResponseHead)
    );
    assert_eq!(ok("", "v"), Err(InvalidInput::ResponseHead));
    assert_eq!(ok("Bad Name", "v"), Err(InvalidInput::ResponseHead));
    assert_eq!(ok("Name", "line\r\nbreak"), Err(InvalidInput::ResponseHead));
    let many = (0..257).map(|n| (format!("h{n}"), String::new())).collect();
    assert_eq!(
        HttpResponseHead::new(200, many),
        Err(InvalidInput::ResponseHead)
    );
    let long = vec![("h".to_string(), "x".repeat(64 * 1024))];
    assert_eq!(
        HttpResponseHead::new(200, long),
        Err(InvalidInput::ResponseHead)
    );
}

#[test]
fn a_local_open_failure_carries_the_effect_the_adapter_reported() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let operation = now(handle
        .admit(prepared_with("op-start", 1, auth(0, "none")))
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    for (kind, effect) in [
        (io::ErrorKind::ConnectionReset, Effect::Possible),
        (io::ErrorKind::ConnectionReset, Effect::None),
    ] {
        *rig.world.start_error.lock().unwrap() = Some((kind, effect));
        let failure = now(operation.open(exchange_spec()).unwrap().wait()).unwrap_err();
        assert_eq!(failure.effect(), effect);
        match failure {
            OpenFailure::Io(start) => {
                assert_eq!(start.error().kind(), kind);
                assert_eq!(start.effect(), effect);
            }
            other => panic!("expected a start failure: {other:?}"),
        }
    }
}

#[test]
fn a_lost_connection_is_asked_again_only_when_the_request_was_not_handed_over() {
    let rig = Rig::new();
    let (_owner, handle) = rig.host(1);
    let operation = now(handle
        .admit(prepared_with("op-lost", 1, auth(0, "none")))
        .unwrap()
        .acquire())
    .unwrap()
    .register();
    // Not handed over: the host asks again on a new connection and the opening succeeds.
    *rig.world.start_error.lock().unwrap() = Some((io::ErrorKind::NotConnected, Effect::None));
    let exchange = now(operation.open(exchange_spec()).unwrap().wait()).unwrap();
    now(exchange.finish());
    // Possibly sent: asking again would replay the request, so the opening fails with the effect.
    *rig.world.start_error.lock().unwrap() = Some((io::ErrorKind::NotConnected, Effect::Possible));
    let failure = now(operation.open(exchange_spec()).unwrap().wait()).unwrap_err();
    assert!(matches!(failure, OpenFailure::Io(_)), "{failure:?}");
    assert_eq!(failure.effect(), Effect::Possible);
}

#[test]
fn every_open_failure_has_an_effect_and_nothing_started_is_none() {
    let possible = Failure {
        code: ErrorCode::Io,
        effect: Effect::Possible,
        ..Default::default()
    };
    let rows = [
        (OpenFailure::Cancelled, Effect::None),
        (OpenFailure::Sealed, Effect::None),
        (
            OpenFailure::Closing(TerminalReason::HostClosing),
            Effect::None,
        ),
        (OpenFailure::NoFittingConnection, Effect::None),
        (
            OpenFailure::Pool(gwz_transport::pool::Error::AllocationTimeout),
            Effect::None,
        ),
        (
            OpenFailure::Io(StartError::none(io::ErrorKind::Other.into())),
            Effect::None,
        ),
        (OpenFailure::Refused(possible), Effect::Possible),
        (
            OpenFailure::CarrierLost {
                effect: Effect::Possible,
            },
            Effect::Possible,
        ),
        (
            OpenFailure::CarrierLost {
                effect: Effect::None,
            },
            Effect::None,
        ),
        (OpenFailure::Fault(ErrorCode::Protocol), Effect::Possible),
    ];
    for (failure, effect) in rows {
        assert_eq!(failure.effect(), effect, "{failure:?}");
    }
}

#[test]
fn an_exchange_end_carries_the_effect_its_adapter_reports() {
    let (rig, _owner, _operation, exchange) = opened(exchange_spec());
    rig.world.peer(0).set_effect(Effect::Possible);
    let end = now(exchange.finish());
    assert_eq!(end.disposition, Disposition::Reusable);
    assert_eq!(end.effect, Effect::Possible);
}

#[test]
fn a_cancelled_exchange_end_still_carries_the_effect() {
    let (rig, _owner, operation, exchange) = opened(exchange_spec());
    rig.world.peer(0).set_effect(Effect::Possible);
    operation.cancel_handle().cancel();
    let end = now(exchange.finish());
    assert_eq!(end.disposition, Disposition::Discarded);
    assert_eq!(end.effect, Effect::Possible);
}

#[test]
fn an_exchange_that_reports_nothing_ends_with_no_effect() {
    let (_rig, _owner, _operation, exchange) = opened(exchange_spec());
    assert_eq!(now(exchange.finish()).effect, Effect::None);
}

/// The earlier port: an adapter and an exchange that implement only what existed before.
struct Earlier;
impl ExchangeIo for Earlier {
    fn poll_finish(&mut self, _cx: &mut Context<'_>) -> Poll<Disposition> {
        Poll::Ready(Disposition::Discarded)
    }
    fn abort(&mut self) {}
}
impl EndpointAdapter for Earlier {
    fn connect(&mut self, _command: ConnectCommand) {}
    fn cancel_connect(&mut self, _connection: ConnectionId) {}
    fn close(&mut self, _connection: ConnectionId) {}
    fn start_exchange(
        &mut self,
        _connection: ConnectionId,
        _spec: &ExchangeSpec,
        _auth: &AuthContext,
    ) -> io::Result<Box<dyn ExchangeIo>> {
        Err(io::ErrorKind::ConnectionReset.into())
    }
    fn poll_completion(&mut self, _cx: &mut Context<'_>) -> Poll<Completion> {
        Poll::Pending
    }
    fn poll_dispose(&mut self, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Ready(())
    }
}

#[test]
fn the_defaults_claim_no_effect_and_no_head() {
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    let mut io = Earlier;
    assert_eq!(ExchangeIo::effect(&io), Effect::None);
    assert!(matches!(
        io.poll_response_head(&mut cx),
        Poll::Ready(Err(ref e)) if e.kind() == io::ErrorKind::Unsupported
    ));
    // The default `start_exchange_checked` delegates and says the request was not handed over.
    let (rig, _owner, _operation, _exchange) = opened(exchange_spec());
    let connection = rig.world.connected_ids()[0];
    let spec = exchange_spec();
    let failure = Earlier
        .start_exchange_checked(connection, &spec, &auth(0, "none"))
        .err()
        .expect("the earlier adapter fails");
    assert_eq!(failure.error().kind(), io::ErrorKind::ConnectionReset);
    assert_eq!(failure.effect(), Effect::None);
}

#[test]
fn an_end_whose_disposal_is_still_pending_carries_the_effect_when_it_is_stated() {
    use crate::{CleanupStatus, fakes::poll_once};
    let (rig, _owner, _operation, exchange) = opened(exchange_spec());
    rig.progress.run();
    rig.world.peer(0).set_effect(Effect::Possible);
    rig.world.discard_next.store(true, Ordering::SeqCst);
    rig.world.hold_close.store(true, Ordering::SeqCst);
    let mut finish = exchange.finish();
    assert!(poll_once(&mut finish).is_pending(), "the disposal is held");
    // At the cleanup bound the receipt states the cleanup as pending, and still says what the request did.
    rig.clock.advance(5_000);
    rig.progress.run();
    let end = now(finish);
    assert!(matches!(end.cleanup, CleanupStatus::Pending(_)));
    assert_eq!(end.effect, Effect::Possible);
}
