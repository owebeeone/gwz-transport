use gwz_transport::{
    binding, codec,
    protocol::*,
    stream::{Config, Error, Side, StreamMachine},
};

fn closing_pair() -> (StreamMachine, StreamMachine) {
    let mut a = Config::new("limits", 1, Side::Initiator);
    a.receive_limits.metadata_bytes = 256;
    a.max_payload = 4;
    let mut b = Config::new("limits", 1, Side::Endpoint);
    b.peer_limits = a.receive_limits.clone();
    b.max_payload = 4;
    let mut a = StreamMachine::new(a).unwrap();
    let mut b = StreamMachine::new(b).unwrap();
    a.start_close().unwrap();
    while let Some(message) = a.next_message() {
        b.receive(message).unwrap();
    }
    b.end_write().unwrap();
    while let Some(message) = b.next_message() {
        a.receive(message).unwrap();
    }
    (a, b)
}

#[test]
fn negotiated_metadata_limit_applies_before_local_close_is_retained() {
    let (mut a, mut b) = closing_pair();
    for length in [257, 16385] {
        let facts = Facts {
            key_fingerprint: Some("f".repeat(length)),
            ..Default::default()
        };
        assert_eq!(
            b.complete_close(Disposition::Reusable, facts),
            Err(Error::Protocol)
        );
        assert!(!b.stats().terminal);
        assert!(
            b.next_message().is_none(),
            "rejected facts must not be queued"
        );
    }
    let facts = Facts {
        key_fingerprint: Some("f".repeat(256)),
        ..Default::default()
    };
    b.complete_close(Disposition::Reusable, facts.clone())
        .unwrap();
    a.receive(b.next_message().unwrap()).unwrap();
    assert_eq!(a.close_result().unwrap().facts, facts);
}

#[test]
fn typed_and_codec_paths_accept_the_same_negotiated_metadata_boundary() {
    for length in [256, 257] {
        let (mut a, _) = closing_pair();
        let message = Envelope {
            version: 1,
            session_id: "limits".into(),
            stream_id: 1,
            kind: MessageKind::Closed,
            closed: Some(Closed {
                disposition: Disposition::Reusable,
                unread_response_discarded: false,
                facts: Facts {
                    key_fingerprint: Some("f".repeat(length)),
                    ..Default::default()
                },
                failure: None,
            }),
            ..Default::default()
        };
        let mut limits = binding::default_limits();
        limits.metadata_bytes = 256;
        let bytes = codec::encode(&message).unwrap();
        assert_eq!(a.receive(message.clone()).is_ok(), length == 256);
        assert_eq!(
            codec::admit_limited(&message, &limits).is_ok(),
            length == 256
        );
        assert_eq!(
            codec::encode_limited(&message, &limits).is_ok(),
            length == 256
        );
        assert_eq!(
            codec::decode_limited(&bytes, &limits).is_ok(),
            length == 256
        );
        if length > 256 {
            assert_eq!(a.close_result(), Err(Error::Protocol));
        }
    }
}

#[test]
fn construction_cannot_expand_negotiated_windows_or_payload_budgets() {
    for axis in 0..4 {
        let mut config = Config::new("limits", 1, Side::Initiator);
        match axis {
            0 => {
                config.receive_limits.receive_window = 1;
            }
            1 => {
                config.peer_limits.receive_window = 1;
            }
            2 => {
                config.peer_limits.data_payload = 1;
            }
            _ => {
                config.peer_limits.encoded_frame = 4096;
            }
        }
        assert!(matches!(
            StreamMachine::new(config),
            Err(Error::InvalidConfig)
        ));
    }
}
