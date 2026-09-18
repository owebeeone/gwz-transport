use gwz_transport::{codec, protocol::*};

fn data(payload: Vec<u8>) -> Envelope {
    Envelope {
        version: 1,
        session_id: "test-session".into(),
        stream_id: 1,
        kind: MessageKind::Data,
        data: Some(Data { offset: 0, payload }),
        ..Default::default()
    }
}

#[test]
fn binary_data_roundtrip_and_generated_shared_type() {
    let message = data(vec![0, 255, 128, 1, 0]);
    let bytes = codec::encode(&message).unwrap();
    assert_eq!(codec::decode(&bytes).unwrap(), message);
}

#[test]
fn rejects_ambiguous_body_and_wrong_stream_domain() {
    let mut message = data(vec![1]);
    message.cancel = Some(Cancel {
        reason: ErrorCode::Cancelled,
    });
    assert!(codec::encode(&message).is_err());
    message.cancel = None;
    message.stream_id = 0;
    assert!(codec::encode(&message).is_err());
}

#[test]
fn maximum_data_is_admitted_and_excess_is_refused() {
    let message = data(vec![7; 64 * 1024]);
    let bytes = codec::encode(&message).unwrap();
    assert_eq!(codec::decode(&bytes).unwrap(), message);
    assert!(codec::encode(&data(vec![0; 64 * 1024 + 1])).is_err());
}

#[test]
fn unknown_deep_fields_are_bounded_before_decode() {
    use gwz_transport::cbor::{self, Cbor};
    let mut value = data(vec![1]).to_cbor();
    let mut nested = Cbor::Null;
    for _ in 0..20 {
        nested = Cbor::Array(vec![nested]);
    }
    if let Cbor::Map(fields) = &mut value {
        fields.push((100, nested));
    }
    assert!(codec::decode(&cbor::encode(&value)).is_err());
}

#[test]
fn unknown_small_fields_are_ignored() {
    use gwz_transport::cbor::{self, Cbor};
    let message = data(vec![1]);
    let mut value = message.to_cbor();
    if let Cbor::Map(fields) = &mut value {
        fields.push((100, Cbor::Bool(true)));
    }
    assert_eq!(codec::decode(&cbor::encode(&value)).unwrap(), message);
}

#[test]
fn huge_truncated_collection_declaration_is_refused() {
    assert!(codec::decode(&[0x9a, 0xff, 0xff, 0xff, 0xff]).is_err());
}

#[test]
fn negotiated_payload_limit_applies_before_typed_decode() {
    let bytes = codec::encode(&data(vec![0; 8192])).unwrap();
    let mut limits = gwz_transport::binding::default_limits();
    limits.data_payload = 4096;
    assert!(codec::decode_limited(&bytes, &limits).is_err());
}

#[test]
fn bootstrap_unknown_fields_cannot_use_stream_allocation_budget() {
    use gwz_transport::{
        binding,
        cbor::{self, Cbor},
    };
    let message = binding::offer("bootstrap", EndpointRole::Driver);
    let mut tree = message.to_cbor();
    if let Cbor::Map(fields) = &mut tree {
        for key in 100..105 {
            fields.push((key, Cbor::Bytes(vec![0; 13000])));
        }
    }
    assert!(codec::decode(&cbor::encode(&tree)).is_err());
}
