use gwz_transport::{
    protocol::*,
    stream::{Config, Error, MessageEndpoint, Side, Stream},
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
            code: ErrorCode::Io,
            effect: Effect::Possible,
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
