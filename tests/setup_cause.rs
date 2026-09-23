use gwz_transport::{
    cbor::Cbor,
    protocol::{AuthMethod, Effect, ErrorCode, Facts, Failure, SetupFailureCause},
};

#[test]
fn setup_causes_have_fixed_wire_values_and_fail_closed() {
    let cases = [
        (SetupFailureCause::Stall, 1),
        (SetupFailureCause::Aggregate, 2),
        (SetupFailureCause::Interaction, 3),
        (SetupFailureCause::Allocation, 4),
        (SetupFailureCause::ConnectionRefused, 5),
        (SetupFailureCause::NotFound, 6),
        (SetupFailureCause::AddressNotAvailable, 7),
    ];
    for (cause, wire) in cases {
        let failure = Failure {
            code: ErrorCode::Timeout,
            effect: Effect::None,
            facts: None,
            setup_cause: Some(cause),
        };
        let encoded = failure.to_cbor();
        assert_eq!(encoded.try_get(4), Ok(&Cbor::Int(wire)));
        assert_eq!(Failure::from_cbor(&encoded), Ok(failure));
    }
    for unknown in [0, 8, -1, 255] {
        let mut encoded = Failure::default().to_cbor();
        if let Cbor::Map(fields) = &mut encoded {
            fields.retain(|(key, _)| *key != 4);
            fields.push((4, Cbor::Int(unknown)));
        }
        assert!(Failure::from_cbor(&encoded).is_err());
    }
}

#[test]
fn missing_cause_is_none_and_old_reader_retains_old_fields() {
    let failure = Failure {
        code: ErrorCode::Unavailable,
        effect: Effect::Possible,
        facts: Some(Facts {
            method: AuthMethod::SshAgent,
            key_fingerprint: Some("real-fingerprint".into()),
            ..Facts::default()
        }),
        setup_cause: Some(SetupFailureCause::ConnectionRefused),
    };
    let mut old_reader = failure.to_cbor();
    if let Cbor::Map(fields) = &mut old_reader {
        fields.retain(|(key, _)| *key != 4);
    }
    let decoded = Failure::from_cbor(&old_reader).unwrap();
    assert_eq!(decoded.setup_cause, None);
    assert_eq!(decoded.code, failure.code);
    assert_eq!(decoded.effect, failure.effect);
    assert_eq!(decoded.facts, failure.facts);
    assert_eq!(decoded.to_cbor().try_get(4), Ok(&Cbor::Null));
}
