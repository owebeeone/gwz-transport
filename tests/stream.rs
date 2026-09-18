use gwz_transport::{
    protocol::*,
    stream::{Config, Error, Side, StreamMachine},
};

fn pair(window: usize, buffer: usize, payload: usize) -> (StreamMachine, StreamMachine) {
    let mut config = Config::new("memory-session", 1, Side::Initiator);
    config.receive_window = window;
    config.peer_receive_window = window;
    config.send_buffer = buffer;
    config.max_payload = payload;
    let mut peer = config.clone();
    peer.side = Side::Endpoint;
    (
        StreamMachine::new(config).unwrap(),
        StreamMachine::new(peer).unwrap(),
    )
}

fn transfer(from: &mut StreamMachine, to: &mut StreamMachine) -> bool {
    if let Some(message) = from.next_message() {
        to.receive(message).unwrap();
        true
    } else {
        false
    }
}

#[test]
fn tiny_window_applies_backpressure_until_application_reads() {
    let (mut sender, mut receiver) = pair(3, 4, 2);
    assert_eq!(sender.write(b"abcdefg").unwrap(), 4);
    sender.advance(100);
    while transfer(&mut sender, &mut receiver) {}
    assert_eq!(receiver.stats().received_buffer, 3);
    assert_eq!(sender.stats().sent, 3);
    assert_eq!(sender.write(b"efg").unwrap(), 3);
    assert_eq!(sender.write(b"h"), Err(Error::WouldBlock));
    assert!(sender.next_message().is_none());
    assert!(
        receiver.next_message().is_none(),
        "decoding is not consumption"
    );
    let mut output = [0; 2];
    assert_eq!(receiver.read(&mut output).unwrap(), 2);
    assert_eq!(&output, b"ab");
    let window = receiver.next_message().unwrap();
    assert_eq!(window.window.unwrap().max_offset, 5);
}

#[test]
fn batching_timer_is_from_first_byte_and_flush_bypasses_it() {
    let (mut sender, mut receiver) = pair(32, 16, 8);
    assert_eq!(sender.write(b"a").unwrap(), 1);
    assert_eq!(sender.next_deadline(), Some(100));
    sender.advance(99);
    sender.write(b"b").unwrap();
    assert_eq!(sender.next_deadline(), Some(100));
    assert!(sender.next_message().is_none());
    sender.advance(100);
    assert!(transfer(&mut sender, &mut receiver));
    sender.write(b"c").unwrap();
    let ticket = sender.start_flush().unwrap();
    while transfer(&mut sender, &mut receiver) {}
    assert!(!sender.flush_complete(ticket).unwrap());
    assert!(
        receiver.next_message().is_none(),
        "flush waits for sink consumption"
    );
    let mut bytes = [0; 3];
    assert_eq!(receiver.read(&mut bytes).unwrap(), 3);
    assert_eq!(&bytes, b"abc");
    while transfer(&mut receiver, &mut sender) {}
    assert!(sender.flush_complete(ticket).unwrap());
}

#[test]
fn half_close_preserves_reverse_direction_and_empty_read_is_not_eof() {
    let (mut a, mut b) = pair(8, 8, 4);
    let mut output = [0; 8];
    assert_eq!(b.read(&mut output), Err(Error::WouldBlock));
    a.write(b"abc").unwrap();
    a.end_write().unwrap();
    while transfer(&mut a, &mut b) {}
    assert_eq!(b.read(&mut output).unwrap(), 3);
    assert_eq!(&output[..3], b"abc");
    assert_eq!(b.read(&mut output).unwrap(), 0);
    b.write(b"reply").unwrap();
    b.end_write().unwrap();
    while transfer(&mut b, &mut a) {}
    assert_eq!(a.read(&mut output).unwrap(), 5);
    assert_eq!(&output[..5], b"reply");
}

#[test]
fn cancel_abandons_unsent_bytes_and_error_follows_received_prefix() {
    let (mut a, mut b) = pair(8, 8, 4);
    a.write(b"first").unwrap();
    assert!(transfer(&mut a, &mut b));
    a.cancel();
    let cancel = a.next_message().unwrap();
    assert_eq!(cancel.kind, MessageKind::Cancel);
    b.receive(cancel).unwrap();
    assert!(a.next_message().is_none());
    let mut bytes = [0; 8];
    assert_eq!(b.read(&mut bytes).unwrap(), 4);
    assert_eq!(&bytes[..4], b"firs");
    assert_eq!(b.read(&mut bytes), Err(Error::Cancelled));
}

#[test]
fn invalid_offsets_and_over_credit_fail_instead_of_corrupting_stream() {
    for (offset, len) in [(1, 1), (0, 9)] {
        let (_, mut b) = pair(8, 8, 8);
        let message = Envelope {
            version: 1,
            session_id: "memory-session".into(),
            stream_id: 1,
            kind: MessageKind::Data,
            data: Some(Data {
                offset,
                payload: vec![0; len],
            }),
            ..Default::default()
        };
        assert_eq!(b.receive(message), Err(Error::Protocol));
        assert_eq!(b.next_message().unwrap().kind, MessageKind::Failed);
    }
}

#[test]
fn close_deadline_includes_time_waiting_for_credit() {
    let (mut a, mut b) = pair(1, 8, 1);
    a.write(b"abc").unwrap();
    assert!(transfer(&mut a, &mut b));
    a.start_close().unwrap();
    a.advance(5000);
    assert_eq!(a.close_result(), Err(Error::Timeout));
    assert_eq!(a.next_message().unwrap().kind, MessageKind::Cancel);
}

#[test]
fn reverse_flush_acknowledges_bounded_read_adapter_but_does_not_grant_credit() {
    let (mut a, mut b) = pair(4, 4, 4);
    b.write(b"abcd").unwrap();
    let ticket = b.start_flush().unwrap();
    while transfer(&mut b, &mut a) {}
    let ack = a.next_message().unwrap();
    assert_eq!(ack.kind, MessageKind::Flushed);
    b.receive(ack).unwrap();
    assert!(b.flush_complete(ticket).unwrap());
    assert_eq!(b.stats().peer_limit, 4);
    assert!(a.next_message().is_none());
}

#[test]
fn graceful_close_drains_unread_response_and_waits_for_request_sink() {
    let (mut a, mut b) = pair(4, 8, 4);
    a.write(b"req").unwrap();
    a.start_close().unwrap();
    while transfer(&mut a, &mut b) {}
    b.write(b"response").unwrap();
    b.end_write().unwrap();
    while transfer(&mut b, &mut a) {}
    while transfer(&mut a, &mut b) {}
    while transfer(&mut b, &mut a) {}
    assert_eq!(
        b.complete_close(Disposition::Reusable, Facts::default()),
        Err(Error::WouldBlock)
    );
    let mut bytes = [0; 4];
    assert_eq!(b.read(&mut bytes).unwrap(), 3);
    assert_eq!(&bytes[..3], b"req");
    b.complete_close(Disposition::Reusable, Facts::default())
        .unwrap();
    while transfer(&mut b, &mut a) {}
    let closed = a.close_result().unwrap();
    assert!(closed.unread_response_discarded);
    assert_eq!(closed.disposition, Disposition::Reusable);
    assert_eq!(a.read(&mut bytes), Err(Error::Closed));
    a.start_close().unwrap();
    a.cancel();
    a.advance(10_000);
    assert_eq!(a.close_result().unwrap(), closed);
    assert!(a.next_message().is_none());
}

#[test]
fn read_makes_small_pending_request_immediately_eligible() {
    let (mut a, mut b) = pair(8, 8, 8);
    a.write(b"request").unwrap();
    assert!(a.next_message().is_none());
    assert_eq!(a.read(&mut [0]), Err(Error::WouldBlock));
    assert!(transfer(&mut a, &mut b));
}

#[test]
fn premature_close_and_data_after_eof_fail_closed() {
    for kind in [MessageKind::Close, MessageKind::Data] {
        let (mut a, mut b) = pair(8, 8, 8);
        if kind == MessageKind::Data {
            a.end_write().unwrap();
            assert!(transfer(&mut a, &mut b));
        }
        let mut message = Envelope {
            version: 1,
            session_id: "memory-session".into(),
            stream_id: 1,
            kind,
            ..Default::default()
        };
        if kind == MessageKind::Close {
            message.close = Some(Close { final_offset: 0 });
        } else {
            message.data = Some(Data {
                offset: 0,
                payload: vec![1],
            });
        }
        assert_eq!(b.receive(message.clone()), Err(Error::Protocol));
        assert!(
            b.receive(message).is_ok(),
            "late traffic cannot reopen terminal stream"
        );
        assert_eq!(b.read(&mut [0]), Err(Error::Protocol));
    }
}
