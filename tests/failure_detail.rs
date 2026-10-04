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
fn terminal_failure_admits_its_owned_detail_without_cloning_it() {
    use gwz_transport::stream::{Config, Error, Side, StreamMachine};
    let mut endpoint = StreamMachine::new(Config::new("detail-owner", 1, Side::Endpoint)).unwrap();
    let mut oversized = message(FailureDetail {
        schemes: Some(vec!["x".repeat(33)]),
        ..FailureDetail::default()
    });
    assert_eq!(
        endpoint.fail_terminal(oversized.open_failed.take().unwrap()),
        Err(Error::Protocol)
    );
    assert!(endpoint.next_message().is_none());
    assert_eq!(endpoint.write(b"still-live"), Ok(10));

    let mut valid = message(FailureDetail {
        schemes: Some(vec!["Negotiate".into()]),
        ..FailureDetail::default()
    });
    let failure = valid.open_failed.take().unwrap();
    let owned_bytes = failure.detail.as_ref().unwrap().schemes.as_ref().unwrap()[0].as_ptr();
    endpoint.fail_terminal(failure).unwrap();
    let sent = endpoint.next_message().unwrap().failed.unwrap();
    assert_eq!(
        sent.detail.as_ref().unwrap().schemes.as_ref().unwrap()[0].as_ptr(),
        owned_bytes
    );
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
        HelperFailureCause::MalformedOutput,
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
fn helper_timing_round_trips_only_with_its_bounded_timeout_provenance() {
    use gwz_transport::protocol::{Closed, Disposition, Facts, SetupFailureCause};
    for (cause, ms) in [
        (SetupFailureCause::Interaction, 1),
        (SetupFailureCause::Interaction, 120_000),
        (SetupFailureCause::Allocation, 0),
        (SetupFailureCause::Allocation, 86_400_000),
    ] {
        let mut value = message(FailureDetail {
            helper_budget_ms: Some(ms),
            retry_attempt: Some(RetryAttempt {
                attempt: 2,
                attempts: 4,
            }),
            ..Default::default()
        });
        let failure = value.open_failed.as_mut().unwrap();
        failure.code = ErrorCode::Timeout;
        failure.setup_cause = Some(cause);
        assert_eq!(
            codec::decode(&codec::encode(&value).unwrap()),
            Ok(value.clone())
        );
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
                MessageKind::IdentityCheckFailed => {
                    value.identity_check_failed = Some(failure.clone())
                }
                MessageKind::Closed => {
                    value.closed = Some(Closed {
                        disposition: Disposition::Discarded,
                        unread_response_discarded: false,
                        facts: Facts::default(),
                        failure: Some(failure.clone()),
                    });
                }
                _ => unreachable!(),
            }
            assert_eq!(
                codec::decode(&codec::encode(&value).unwrap()),
                Ok(value.clone())
            );
            match kind {
                MessageKind::Failed => value.failed.as_mut().unwrap().effect = Effect::Possible,
                MessageKind::IdentityCheckFailed => {
                    value.identity_check_failed.as_mut().unwrap().effect = Effect::Possible
                }
                MessageKind::Closed => {
                    value
                        .closed
                        .as_mut()
                        .unwrap()
                        .failure
                        .as_mut()
                        .unwrap()
                        .effect = Effect::Possible
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
    for detail in [
        FailureDetail {
            helper_budget_ms: Some(1),
            helper_cause: Some(HelperFailureCause::MissingField),
            ..Default::default()
        },
        FailureDetail {
            helper_budget_ms: Some(1),
            schemes: Some(vec!["Basic".into()]),
            ..Default::default()
        },
        FailureDetail {
            helper_budget_ms: Some(1),
            ..Default::default()
        },
    ] {
        refused(detail);
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
        fields.push((1, Cbor::Int(9)));
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

#[test]
fn encoded_helper_timing_cannot_be_ignored_or_unbounded() {
    use gwz_transport::protocol::SetupFailureCause;
    for (cause, allowance) in [
        (SetupFailureCause::Interaction, 0),
        (SetupFailureCause::Interaction, 120_001),
        (SetupFailureCause::Allocation, -1),
        (SetupFailureCause::Allocation, 86_400_001),
        (SetupFailureCause::NotFound, 1),
    ] {
        let mut value = message(FailureDetail::default());
        let failure = value.open_failed.as_mut().unwrap();
        failure.code = ErrorCode::Timeout;
        failure.setup_cause = Some(cause);
        let mut encoded = value.to_cbor();
        if let Cbor::Map(fields) = &mut encoded {
            if let Some((_, Cbor::Map(failure))) = fields.iter_mut().find(|(tag, _)| *tag == 15) {
                if let Some((_, Cbor::Map(detail))) = failure.iter_mut().find(|(tag, _)| *tag == 5)
                {
                    detail.retain(|(tag, _)| *tag != 5);
                    detail.push((5, Cbor::Int(allowance)));
                }
            }
        }
        assert_eq!(
            codec::decode(&cbor::encode(&encoded)),
            Err(codec::Error::InvalidMessage)
        );
    }
}

#[test]
fn retained_incoming_failure_moves_the_first_admitted_answer_and_ignores_late_terminals() {
    use gwz_transport::stream::{Config, Error, Side, StreamMachine};
    let config = || {
        let mut config = Config::new("retained", 1, Side::Initiator);
        config.profile_version = 2;
        config
    };
    let make = |token: &str| Envelope {
        version: 2,
        session_id: "retained".into(),
        stream_id: 1,
        kind: MessageKind::Failed,
        failed: Some(Failure {
            code: ErrorCode::Authentication,
            detail: Some(Box::new(FailureDetail {
                schemes: Some(vec![token.into()]),
                ..Default::default()
            })),
            ..Default::default()
        }),
        ..Default::default()
    };
    let mut stream = StreamMachine::new(config()).unwrap();
    let first = make("Negotiate");
    let pointer = first
        .failed
        .as_ref()
        .unwrap()
        .detail
        .as_ref()
        .unwrap()
        .schemes
        .as_ref()
        .unwrap()[0]
        .as_ptr();
    stream.receive(first).unwrap();
    let held = stream.retained_failure().unwrap();
    assert_eq!(
        held.detail.as_ref().unwrap().schemes.as_ref().unwrap()[0].as_ptr(),
        pointer
    );
    stream.receive(make("Basic")).unwrap();
    assert_eq!(
        stream
            .retained_failure()
            .unwrap()
            .detail
            .as_ref()
            .unwrap()
            .schemes
            .as_ref()
            .unwrap()[0],
        "Negotiate"
    );
    let mut invalid = StreamMachine::new(config()).unwrap();
    assert_eq!(invalid.receive(make(&"x".repeat(33))), Err(Error::Protocol));
    assert!(invalid.retained_failure().is_none());
    invalid.receive(make("Basic")).unwrap();
    assert!(invalid.retained_failure().is_none());
    let mut cancelled = StreamMachine::new(config()).unwrap();
    cancelled.cancel();
    cancelled.receive(make("Basic")).unwrap();
    assert!(cancelled.retained_failure().is_none());
}
