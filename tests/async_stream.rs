use gwz_transport::{
    protocol::*,
    stream::{Config, Error, MessageEndpoint, Side, Stream, StreamWaiter},
};
use std::{
    future::Future,
    pin::pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

#[derive(Default)]
struct Signal(AtomicUsize);
impl Wake for Signal {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}
fn endpoint() -> (Stream, MessageEndpoint) {
    let mut config = Config::new("async", 1, Side::Initiator);
    config.send_buffer = 2;
    config.max_payload = 2;
    config.max_waiters = 2;
    Stream::new(config).unwrap()
}

#[test]
fn asynchronous_waiters_wake_without_spinning_and_release_on_cancellation() {
    let (stream, port) = endpoint();
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    let mut context = Context::from_waker(&waker);
    let mut bytes = [0; 1];
    {
        let mut read = pin!(stream.read(&mut bytes));
        assert!(read.as_mut().poll(&mut context).is_pending());
        assert_eq!(port.waiter_count(), 1);
        assert!(read.as_mut().poll(&mut context).is_pending());
        assert_eq!(signal.0.load(Ordering::Relaxed), 0);
    }
    assert_eq!(port.waiter_count(), 0);
    let mut read = pin!(stream.read(&mut bytes));
    assert!(read.as_mut().poll(&mut context).is_pending());
    port.deliver(Envelope {
        version: 1,
        session_id: "async".into(),
        stream_id: 1,
        kind: MessageKind::Data,
        data: Some(Data {
            offset: 0,
            payload: vec![42],
        }),
        ..Default::default()
    })
    .unwrap();
    assert!(signal.0.load(Ordering::Relaxed) > 0);
    assert_eq!(read.as_mut().poll(&mut context), Poll::Ready(Ok(1)));
    assert_eq!(port.waiter_count(), 0);
}

#[test]
fn bounded_write_waits_for_message_take_and_last_owner_drop_cancels() {
    let (stream, port) = endpoint();
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert_eq!(pin!(stream.write(b"abc")).poll(&mut cx), Poll::Ready(Ok(2)));
    {
        let mut write = pin!(stream.write(b"c"));
        assert!(write.as_mut().poll(&mut cx).is_pending());
        assert!(matches!(
            pin!(port.next_message()).poll(&mut cx),
            Poll::Ready(Ok(Some(_)))
        ));
        assert_eq!(write.as_mut().poll(&mut cx), Poll::Ready(Ok(1)));
    }
    let clone = stream.clone();
    drop(stream);
    assert!(!port.stats().terminal);
    drop(clone);
    let Poll::Ready(Ok(Some(cancel))) = pin!(port.next_message()).poll(&mut cx) else {
        panic!("missing cancellation");
    };
    assert_eq!(cancel.kind, MessageKind::Cancel);
    assert_eq!(
        pin!(port.next_message()).poll(&mut cx),
        Poll::Ready(Ok(None))
    );
}

#[test]
fn message_port_drop_wakes_pending_reader_with_error() {
    let (stream, port) = endpoint();
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let mut bytes = [0; 1];
    let mut read = pin!(stream.read(&mut bytes));
    assert!(read.as_mut().poll(&mut cx).is_pending());
    drop(port);
    assert!(signal.0.load(Ordering::Relaxed) > 0);
    assert_eq!(
        read.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::CarrierLost))
    );
}

#[test]
fn waiter_limit_is_bounded_and_cancelled_future_releases_capacity() {
    let (stream, port) = endpoint();
    let mut cx = Context::from_waker(Waker::noop());
    let mut one = [0];
    let mut two = [0];
    let mut three = [0];
    {
        let mut first = pin!(stream.read(&mut one));
        let mut second = pin!(stream.read(&mut two));
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        assert_eq!(
            pin!(stream.read(&mut three)).poll(&mut cx),
            Poll::Ready(Err(Error::WaiterCapacity))
        );
        assert_eq!(port.waiter_count(), 2);
    }
    assert_eq!(port.waiter_count(), 0);
    assert!(pin!(stream.read(&mut three)).poll(&mut cx).is_pending());
    assert_eq!(port.waiter_count(), 0, "temporary future dropped");
}

#[test]
fn supplied_clock_wakes_message_receiver_for_partial_batch() {
    let (stream, port) = endpoint();
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    assert_eq!(pin!(stream.write(b"x")).poll(&mut cx), Poll::Ready(Ok(1)));
    let mut receive = pin!(port.next_message());
    assert!(receive.as_mut().poll(&mut cx).is_pending());
    assert_eq!(port.next_deadline(), Some(100));
    port.advance(99);
    assert_eq!(signal.0.load(Ordering::Relaxed), 0);
    port.advance(100);
    assert!(signal.0.load(Ordering::Relaxed) > 0);
    let Poll::Ready(Ok(Some(message))) = receive.as_mut().poll(&mut cx) else {
        panic!("timer failed to wake sender");
    };
    assert_eq!(message.data.unwrap().payload, b"x");
}

#[test]
fn dispatcher_has_reserved_capacity_in_both_registration_orders() {
    for dispatcher_first in [true, false] {
        let mut config = Config::new("reserved", 1, Side::Initiator);
        config.max_waiters = 1;
        config.max_payload = 1;
        let (stream, port) = Stream::new(config).unwrap();
        let signal = Arc::new(Signal::default());
        let waker = Waker::from(signal.clone());
        let mut cx = Context::from_waker(&waker);
        let mut bytes = [0];
        let mut extra = [0];
        let mut read = pin!(stream.read(&mut bytes));
        {
            let mut pump = pin!(port.next_message());
            if dispatcher_first {
                assert!(pump.as_mut().poll(&mut cx).is_pending());
            }
            assert!(read.as_mut().poll(&mut cx).is_pending());
            if !dispatcher_first {
                assert!(pump.as_mut().poll(&mut cx).is_pending());
            }
            assert_eq!(port.waiter_count(), 2);
            assert_eq!(
                pin!(stream.read(&mut extra)).poll(&mut cx),
                Poll::Ready(Err(Error::WaiterCapacity))
            );
            assert_eq!(pin!(stream.write(b"x")).poll(&mut cx), Poll::Ready(Ok(1)));
            assert!(signal.0.load(Ordering::Relaxed) > 0);
            assert!(matches!(
                pump.as_mut().poll(&mut cx),
                Poll::Ready(Ok(Some(_)))
            ));
        }
        let mut pump = pin!(port.next_message());
        assert!(pump.as_mut().poll(&mut cx).is_pending());
        let previous = signal.0.load(Ordering::Relaxed);
        stream.cancel();
        assert!(signal.0.load(Ordering::Relaxed) > previous);
        let Poll::Ready(Ok(Some(message))) = pump.as_mut().poll(&mut cx) else {
            panic!("control path stalled");
        };
        assert_eq!(message.kind, MessageKind::Cancel);
        assert_eq!(
            read.as_mut().poll(&mut cx),
            Poll::Ready(Err(Error::Cancelled))
        );
        assert_eq!(port.waiter_count(), 0);
    }
}

#[test]
fn typed_peer_failure_wakes_async_reader_with_exact_cause() {
    let (stream, port) = endpoint();
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let mut bytes = [0];
    let mut read = pin!(stream.read(&mut bytes));
    assert!(read.as_mut().poll(&mut cx).is_pending());
    port.deliver(Envelope {
        version: 1,
        session_id: "async".into(),
        stream_id: 1,
        kind: MessageKind::Failed,
        failed: Some(Failure {
            detail: None,
            setup_cause: None,
            code: ErrorCode::Io,
            effect: Effect::Possible,
            facts: None,
        }),
        ..Default::default()
    })
    .unwrap();
    assert!(signal.0.load(Ordering::Relaxed) > 0);
    let expected = Error::PeerFailed {
        code: ErrorCode::Io,
        effect: Effect::Possible,
    };
    assert_eq!(read.as_mut().poll(&mut cx), Poll::Ready(Err(expected)));
    assert_eq!(
        pin!(stream.write(b"x")).poll(&mut cx),
        Poll::Ready(Err(expected))
    );
}

#[test]
fn endpoint_failure_is_emitted_before_eof_and_wakes_reader() {
    let mut initiator_config = Config::new("async-failure", 1, Side::Initiator);
    initiator_config.profile_version = 2;
    let mut endpoint_config = initiator_config.clone();
    endpoint_config.side = Side::Endpoint;
    let (initiator, initiator_port) = Stream::new(initiator_config).unwrap();
    let (_endpoint, endpoint_port) = Stream::new(endpoint_config).unwrap();
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let mut bytes = [0];
    let mut read = pin!(initiator.read(&mut bytes));
    assert!(read.as_mut().poll(&mut cx).is_pending());
    endpoint_port
        .fail_terminal(Failure {
            detail: None,
            setup_cause: None,
            code: ErrorCode::RepositoryRefused,
            effect: Effect::None,
            facts: Some(Facts::default()),
        })
        .unwrap();
    assert_eq!(
        endpoint_port.fail_terminal(Failure {
            detail: None,
            setup_cause: None,
            code: ErrorCode::Timeout,
            effect: Effect::Possible,
            facts: None,
        }),
        Err(Error::PeerFailed {
            code: ErrorCode::RepositoryRefused,
            effect: Effect::None,
        })
    );
    let message = match pin!(endpoint_port.next_message()).poll(&mut cx) {
        Poll::Ready(Ok(Some(message))) => message,
        other => panic!("expected terminal failure, got {other:?}"),
    };
    initiator_port.deliver(message).unwrap();
    assert!(signal.0.load(Ordering::Relaxed) > 0);
    assert_eq!(
        read.as_mut().poll(&mut cx),
        Poll::Ready(Err(Error::PeerFailed {
            code: ErrorCode::RepositoryRefused,
            effect: Effect::None,
        }))
    );
    assert_eq!(initiator.retained_failure_facts(), Some(Facts::default()));
    assert!(endpoint_port.stats().terminal);
}

fn data(offset: i64, payload: &[u8]) -> Envelope {
    Envelope {
        version: 1,
        session_id: "async".into(),
        stream_id: 1,
        kind: MessageKind::Data,
        data: Some(Data {
            offset,
            payload: payload.to_vec(),
        }),
        ..Default::default()
    }
}
fn wakes(signal: &Signal) -> usize {
    signal.0.load(Ordering::Relaxed)
}

// B1: the slot stays registered between polls, so the data's delivery wakes the caller; nothing wakes it before.
#[test]
fn a_pending_poll_read_keeps_its_waiter_and_is_woken_by_delivered_data() {
    let (stream, port) = endpoint();
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let mut slot = StreamWaiter::new();
    let mut bytes = [0; 1];
    assert!(
        stream
            .poll_read(&mut cx, &mut slot, &mut bytes)
            .is_pending()
    );
    assert!(
        stream
            .poll_read(&mut cx, &mut slot, &mut bytes)
            .is_pending()
    );
    assert_eq!(
        port.waiter_count(),
        1,
        "one registration held between polls"
    );
    assert_eq!(wakes(&signal), 0, "no wake before data");
    port.deliver(data(0, &[42])).unwrap();
    assert_eq!(wakes(&signal), 1, "the delivery wakes the pending read");
    assert_eq!(
        stream.poll_read(&mut cx, &mut slot, &mut bytes),
        Poll::Ready(Ok(1))
    );
    assert_eq!(bytes, [42]);
    assert_eq!(port.waiter_count(), 0, "a ready poll releases the slot");
}

// B2: a write pending on a full buffer and no peer credit is woken by the returned credit and by the dispatcher's
// take that the credit allows, and then accepts its bytes.
#[test]
fn a_pending_poll_write_is_woken_by_returned_credit() {
    let mut config = Config::new("async", 1, Side::Initiator);
    config.send_buffer = 2;
    config.max_payload = 2;
    config.max_waiters = 2;
    config.peer_receive_window = 2;
    let (stream, port) = Stream::new(config).unwrap();
    let signal = Arc::new(Signal::default());
    let waker = Waker::from(signal.clone());
    let mut cx = Context::from_waker(&waker);
    let noop = Waker::noop();
    let mut quiet = Context::from_waker(noop);
    let mut slot = StreamWaiter::new();
    assert_eq!(
        stream.poll_write(&mut cx, &mut slot, b"ab"),
        Poll::Ready(Ok(2))
    );
    assert!(matches!(
        pin!(port.next_message()).poll(&mut quiet),
        Poll::Ready(Ok(Some(_)))
    ));
    assert_eq!(
        stream.poll_write(&mut cx, &mut slot, b"cd"),
        Poll::Ready(Ok(2))
    );
    assert!(stream.poll_write(&mut cx, &mut slot, b"e").is_pending());
    assert!(stream.poll_write(&mut cx, &mut slot, b"e").is_pending());
    assert!(pin!(port.next_message()).poll(&mut quiet).is_pending());
    assert_eq!(port.waiter_count(), 1);
    assert_eq!(wakes(&signal), 0, "no wake before credit");
    let mut credit = data(0, &[]);
    credit.kind = MessageKind::Window;
    credit.data = None;
    credit.window = Some(Window { max_offset: 4 });
    port.deliver(credit).unwrap();
    let after_credit = wakes(&signal);
    assert!(after_credit > 0, "the credit wakes the pending write");
    assert!(matches!(
        pin!(port.next_message()).poll(&mut quiet),
        Poll::Ready(Ok(Some(_)))
    ));
    assert!(
        wakes(&signal) > after_credit,
        "the take that frees room wakes it"
    );
    assert_eq!(
        stream.poll_write(&mut cx, &mut slot, b"e"),
        Poll::Ready(Ok(1))
    );
    assert_eq!(port.waiter_count(), 0);
}

// B3: dropping a pending slot unregisters it; the stream is not cancelled by that.
#[test]
fn dropping_a_pending_slot_unregisters_its_waiter() {
    let (stream, port) = endpoint();
    let mut cx = Context::from_waker(Waker::noop());
    let mut bytes = [0; 1];
    let mut slot = StreamWaiter::new();
    assert!(
        stream
            .poll_read(&mut cx, &mut slot, &mut bytes)
            .is_pending()
    );
    assert_eq!(port.waiter_count(), 1);
    drop(slot);
    assert_eq!(port.waiter_count(), 0);
    assert!(!port.stats().terminal);
    // A slot polled on another stream leaves the first one's waiters.
    let (other, other_port) = endpoint();
    let mut slot = StreamWaiter::new();
    assert!(
        stream
            .poll_read(&mut cx, &mut slot, &mut bytes)
            .is_pending()
    );
    assert!(other.poll_read(&mut cx, &mut slot, &mut bytes).is_pending());
    assert_eq!((port.waiter_count(), other_port.waiter_count()), (0, 1));
}

// B4: repeated polls of one slot hold one place against max_waiters (2 here); a third slot is refused.
#[test]
fn a_slot_counts_once_against_max_waiters() {
    let (stream, port) = endpoint();
    let mut cx = Context::from_waker(Waker::noop());
    let mut bytes = [0; 1];
    let mut first = StreamWaiter::new();
    let mut second = StreamWaiter::new();
    let mut third = StreamWaiter::new();
    for _ in 0..3 {
        assert!(
            stream
                .poll_read(&mut cx, &mut first, &mut bytes)
                .is_pending()
        );
    }
    assert_eq!(port.waiter_count(), 1);
    assert!(stream.poll_write(&mut cx, &mut second, b"ab").is_ready());
    assert!(stream.poll_write(&mut cx, &mut second, b"c").is_pending());
    assert!(stream.poll_write(&mut cx, &mut second, b"c").is_pending());
    assert_eq!(port.waiter_count(), 2);
    assert_eq!(
        stream.poll_read(&mut cx, &mut third, &mut bytes),
        Poll::Ready(Err(Error::WaiterCapacity))
    );
    drop(first);
    assert!(
        stream
            .poll_read(&mut cx, &mut third, &mut bytes)
            .is_pending()
    );
    assert_eq!(port.waiter_count(), 2);
}
