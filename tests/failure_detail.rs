use gwz_transport::{
    cbor::{self, Cbor},
    codec,
    protocol::{
        Effect, Envelope, ErrorCode, Failure, FailureDetail, HelperFailureCause, MessageKind,
        RetryAttempt,
    },
};

fn message(detail: FailureDetail) -> Envelope {
    Envelope {
        version: 2,
        session_id: "detail-fixture".into(),
        stream_id: 1,
        kind: MessageKind::OpenFailed,
        open_failed: Some(Failure {
            code: ErrorCode::Authentication,
            effect: Effect::None,
            detail: Some(Box::new(detail)),
            ..Failure::default()
        }),
        ..Envelope::default()
    }
}

#[test]
fn absent_detail_and_fixed_causes_round_trip() {
    assert!(
        std::mem::size_of::<Failure>() < 128,
        "diagnostics must not inflate common Result errors"
    );
    let mut absent = Failure::default().to_cbor();
    if let Cbor::Map(fields) = &mut absent {
        fields.retain(|(tag, _)| *tag != 5);
    }
    assert_eq!(Failure::from_cbor(&absent).unwrap().detail, None);
    for cause in [
        HelperFailureCause::PipeFailure,
        HelperFailureCause::OutputLimit,
        HelperFailureCause::ControlCharacter,
        HelperFailureCause::UsernameColon,
        HelperFailureCause::NotUtf8,
        HelperFailureCause::MissingNewline,
        HelperFailureCause::MissingField,
    ] {
        let detail = FailureDetail {
            helper_cause: Some(cause),
            pipe_kind: (cause == HelperFailureCause::PipeFailure).then(|| "Other".into()),
            ..FailureDetail::default()
        };
        let value = message(detail);
        assert_eq!(codec::decode(&codec::encode(&value).unwrap()), Ok(value));
    }
}

#[test]
fn bounded_scheme_tokens_and_endpoint_retry_counts_round_trip() {
    let detail = FailureDetail {
        schemes: Some(vec!["Negotiate".into(), "Bearer".into(), "x".repeat(32)]),
        retry_attempt: Some(RetryAttempt {
            attempt: 2,
            attempts: 4,
        }),
        ..FailureDetail::default()
    };
    let value = message(detail);
    assert_eq!(codec::decode(&codec::encode(&value).unwrap()), Ok(value));
    let no_schemes = message(FailureDetail {
        schemes: Some(Vec::new()),
        ..FailureDetail::default()
    });
    assert_eq!(
        codec::decode(&codec::encode(&no_schemes).unwrap()),
        Ok(no_schemes)
    );
}

fn refused(detail: FailureDetail) {
    let value = message(detail);
    assert_eq!(codec::admit(&value), Err(codec::Error::InvalidMessage));
    let unchecked = cbor::encode(&value.to_cbor());
    assert_eq!(codec::decode(&unchecked), Err(codec::Error::InvalidMessage));
}

#[test]
fn oversized_or_instruction_shaped_scheme_values_are_refused() {
    for schemes in [
        vec!["a".into(); 5],
        vec!["x".repeat(33)],
        vec![String::new()],
        vec!["Basic realm=secret".into()],
        vec!["Bearer\npassword=secret".into()],
        vec!["\"secret\"".into()],
        vec!["ümlaut".into()],
    ] {
        refused(FailureDetail {
            schemes: Some(schemes),
            ..FailureDetail::default()
        });
    }
}

#[test]
fn invalid_counts_and_nonfixed_pipe_kinds_are_refused() {
    for (attempt, attempts) in [(0, 4), (-1, 4), (5, 4), (1, 0), (1, i64::MAX)] {
        refused(FailureDetail {
            retry_attempt: Some(RetryAttempt { attempt, attempts }),
            ..FailureDetail::default()
        });
    }
    for pipe_kind in [
        None,
        Some("sentinel-secret".into()),
        Some("https://host/".into()),
    ] {
        refused(FailureDetail {
            helper_cause: Some(HelperFailureCause::PipeFailure),
            pipe_kind,
            ..FailureDetail::default()
        });
    }
    refused(FailureDetail {
        pipe_kind: Some("Other".into()),
        ..FailureDetail::default()
    });
}

#[test]
fn diagnostic_cause_and_scheme_alternatives_cannot_be_combined() {
    refused(FailureDetail {
        helper_cause: Some(HelperFailureCause::MissingField),
        schemes: Some(vec!["Basic".into()]),
        ..FailureDetail::default()
    });
}

#[test]
fn an_unknown_helper_cause_is_not_a_new_diagnostic_string() {
    let mut detail = FailureDetail::default().to_cbor();
    if let Cbor::Map(fields) = &mut detail {
        fields.retain(|(tag, _)| *tag != 1);
        fields.push((1, Cbor::Int(8)));
    }
    assert!(FailureDetail::from_cbor(&detail).is_err());
}

#[test]
fn every_failure_carrier_checks_detail_bounds() {
    use gwz_transport::protocol::{Closed, Disposition, Facts};
    let mut value = message(FailureDetail {
        schemes: Some(vec!["secret\nvalue".into()]),
        ..FailureDetail::default()
    });
    let failure = value.open_failed.take().unwrap();
    for kind in [
        MessageKind::Failed,
        MessageKind::IdentityCheckFailed,
        MessageKind::Closed,
    ] {
        value.kind = kind;
        value.failed = None;
        value.identity_check_failed = None;
        value.closed = None;
        match kind {
            MessageKind::Failed => value.failed = Some(failure.clone()),
            MessageKind::IdentityCheckFailed => value.identity_check_failed = Some(failure.clone()),
            MessageKind::Closed => {
                value.closed = Some(Closed {
                    disposition: Disposition::Discarded,
                    unread_response_discarded: false,
                    facts: Facts::default(),
                    failure: Some(failure.clone()),
                })
            }
            _ => unreachable!(),
        }
        assert_eq!(codec::admit(&value), Err(codec::Error::InvalidMessage));
        assert_eq!(
            codec::decode(&cbor::encode(&value.to_cbor())),
            Err(codec::Error::InvalidMessage)
        );
    }
}
