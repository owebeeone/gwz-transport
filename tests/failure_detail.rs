use gwz_transport::{
    cbor::{self, Cbor},
    codec,
    protocol::{
        CapacityCause, CapacityDetail, CapacitySignal, Effect, Envelope, ErrorCode, Failure,
        FailureDetail, HelperFailureCause, MessageKind, RetryAttempt, Scheme,
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
        HelperFailureCause::NoHelper,
        HelperFailureCause::Declined,
        HelperFailureCause::ConfigurationRefused,
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
fn the_three_configured_helper_causes_keep_their_wire_numbers() {
    for (cause, number) in [
        (HelperFailureCause::NoHelper, 9),
        (HelperFailureCause::Declined, 10),
        (HelperFailureCause::ConfigurationRefused, 11),
    ] {
        assert_eq!(cause.wire(), number);
        assert_eq!(HelperFailureCause::from_wire(number), Ok(cause));
        // Like every cause, each stands alone: a pipe kind belongs to PipeFailure only.
        refused(FailureDetail {
            helper_cause: Some(cause),
            pipe_kind: Some("Other".into()),
            ..FailureDetail::default()
        });
        refused(FailureDetail {
            helper_cause: Some(cause),
            schemes: Some(vec!["Basic".into()]),
            ..FailureDetail::default()
        });
    }
}

#[test]
fn an_unknown_helper_cause_is_not_a_new_diagnostic_string() {
    let mut detail = FailureDetail::default().to_cbor();
    if let Cbor::Map(fields) = &mut detail {
        fields.retain(|(tag, _)| *tag != 1);
        fields.push((1, Cbor::Int(12)));
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

// Capacity detail (gwz-core's adaptive concurrency design, section 5.3). The code
// and the cause travel together; the detail holds counts, milliseconds and the
// host the member configured, never a URL, a response body or a credential.

fn capacity_message(code: ErrorCode, detail: FailureDetail) -> Envelope {
    let mut value = message(detail);
    value.open_failed.as_mut().unwrap().code = code;
    value
}

fn throttle() -> CapacityDetail {
    CapacityDetail {
        cause: CapacityCause::ServerThrottle,
        scheme: Some(Scheme::Https),
        host: Some("github.com".into()),
        port: Some(443),
        limit_applied: Some(8),
        max_per_host: Some(32),
        elapsed_ms: Some(21_000),
        last_signal: Some(CapacitySignal::Http429),
        retry_after_ms: Some(2_000),
    }
}

fn long_retry_after() -> CapacityDetail {
    CapacityDetail {
        cause: CapacityCause::ServerRetryAfter,
        limit_applied: None,
        last_signal: Some(CapacitySignal::Http503),
        retry_after_ms: Some(120_000),
        ..throttle()
    }
}

fn local() -> CapacityDetail {
    CapacityDetail {
        cause: CapacityCause::Local,
        ..CapacityDetail::default()
    }
}

fn with_capacity(capacity: CapacityDetail) -> FailureDetail {
    FailureDetail {
        capacity: Some(capacity),
        ..FailureDetail::default()
    }
}

fn capacity_refused(capacity: CapacityDetail) {
    refused_with(ErrorCode::Capacity, with_capacity(capacity));
}

fn refused_with(code: ErrorCode, detail: FailureDetail) {
    let value = capacity_message(code, detail);
    assert_eq!(codec::admit(&value), Err(codec::Error::InvalidMessage));
    assert_eq!(
        codec::decode(&cbor::encode(&value.to_cbor())),
        Err(codec::Error::InvalidMessage)
    );
}

#[test]
fn the_capacity_causes_and_signals_keep_their_wire_numbers() {
    for (cause, number) in [
        (CapacityCause::ServerThrottle, 1),
        (CapacityCause::ServerRetryAfter, 2),
        (CapacityCause::Local, 3),
    ] {
        assert_eq!(cause.wire(), number);
        assert_eq!(CapacityCause::from_wire(number), Ok(cause));
    }
    for (signal, number) in [(CapacitySignal::Http429, 1), (CapacitySignal::Http503, 2)] {
        assert_eq!(signal.wire(), number);
        assert_eq!(CapacitySignal::from_wire(number), Ok(signal));
    }
    assert!(CapacityCause::from_wire(4).is_err());
    assert!(CapacityCause::from_wire(0).is_err());
    assert!(CapacitySignal::from_wire(3).is_err());
}

#[test]
fn every_capacity_cause_round_trips_beside_the_retry_attempt() {
    for capacity in [throttle(), long_retry_after()] {
        let detail = FailureDetail {
            retry_attempt: Some(RetryAttempt {
                attempt: 4,
                attempts: 4,
            }),
            capacity: Some(capacity),
            ..FailureDetail::default()
        };
        let value = capacity_message(ErrorCode::Capacity, detail);
        assert_eq!(codec::decode(&codec::encode(&value).unwrap()), Ok(value));
    }
    let value = capacity_message(ErrorCode::Capacity, with_capacity(local()));
    assert_eq!(codec::decode(&codec::encode(&value).unwrap()), Ok(value));
}

#[test]
fn a_throttle_that_never_lowered_its_limit_or_hid_its_redirect_host_round_trips() {
    for capacity in [
        CapacityDetail {
            limit_applied: None,
            retry_after_ms: None,
            ..throttle()
        },
        CapacityDetail {
            scheme: None,
            host: None,
            port: None,
            ..throttle()
        },
        CapacityDetail {
            limit_applied: Some(32),
            port: Some(65_535),
            host: Some("::1".into()),
            ..throttle()
        },
    ] {
        let value = capacity_message(ErrorCode::Capacity, with_capacity(capacity));
        assert_eq!(codec::decode(&codec::encode(&value).unwrap()), Ok(value));
    }
}

#[test]
fn a_detail_without_the_capacity_field_still_decodes_as_an_older_peer_encodes_it() {
    // Field 6 absent: the keys an older producer wrote.
    let mut detail = FailureDetail {
        retry_attempt: Some(RetryAttempt {
            attempt: 1,
            attempts: 4,
        }),
        ..FailureDetail::default()
    }
    .to_cbor();
    if let Cbor::Map(fields) = &mut detail {
        fields.retain(|(tag, _)| *tag != 6);
    }
    let decoded = FailureDetail::from_cbor(&detail).unwrap();
    assert_eq!(decoded.capacity, None);
    assert!(decoded.retry_attempt.is_some());
}

#[test]
fn an_unknown_capacity_cause_or_signal_is_not_a_new_diagnostic() {
    let mut detail = throttle().to_cbor();
    if let Cbor::Map(fields) = &mut detail {
        fields.retain(|(tag, _)| *tag != 1);
        fields.push((1, Cbor::Int(4)));
    }
    assert!(CapacityDetail::from_cbor(&detail).is_err());
    let mut detail = throttle().to_cbor();
    if let Cbor::Map(fields) = &mut detail {
        fields.retain(|(tag, _)| *tag != 8);
        fields.push((8, Cbor::Int(3)));
    }
    assert!(CapacityDetail::from_cbor(&detail).is_err());
}

#[test]
fn a_capacity_detail_belongs_to_the_capacity_code_only() {
    for code in [
        ErrorCode::Io,
        ErrorCode::Timeout,
        ErrorCode::Unavailable,
        ErrorCode::Authentication,
        ErrorCode::RepositoryRefused,
    ] {
        refused_with(code, with_capacity(throttle()));
        refused_with(code, with_capacity(local()));
    }
}

#[test]
fn a_local_capacity_carries_no_host_no_signal_and_no_attempt() {
    for extra in [
        CapacityDetail {
            host: Some("github.com".into()),
            ..local()
        },
        CapacityDetail {
            scheme: Some(Scheme::Https),
            ..local()
        },
        CapacityDetail {
            port: Some(443),
            ..local()
        },
        CapacityDetail {
            last_signal: Some(CapacitySignal::Http429),
            ..local()
        },
        CapacityDetail {
            retry_after_ms: Some(1),
            ..local()
        },
        CapacityDetail {
            limit_applied: Some(8),
            ..local()
        },
        CapacityDetail {
            max_per_host: Some(32),
            ..local()
        },
        CapacityDetail {
            elapsed_ms: Some(30_000),
            ..local()
        },
    ] {
        capacity_refused(extra);
    }
    refused_with(
        ErrorCode::Capacity,
        FailureDetail {
            retry_attempt: Some(RetryAttempt {
                attempt: 1,
                attempts: 4,
            }),
            capacity: Some(local()),
            ..FailureDetail::default()
        },
    );
}

#[test]
fn a_server_capacity_names_its_ceiling_and_time_and_a_long_wait_names_the_wait() {
    for capacity in [
        CapacityDetail {
            max_per_host: None,
            ..throttle()
        },
        CapacityDetail {
            elapsed_ms: None,
            ..throttle()
        },
        CapacityDetail {
            retry_after_ms: None,
            ..long_retry_after()
        },
    ] {
        capacity_refused(capacity);
    }
    // A 403 that carried `Retry-After` is a throttle with no 429 or 503 to name.
    let no_signal = capacity_message(
        ErrorCode::Capacity,
        with_capacity(CapacityDetail {
            last_signal: None,
            ..throttle()
        }),
    );
    assert_eq!(
        codec::decode(&codec::encode(&no_signal).unwrap()),
        Ok(no_signal)
    );
}

#[test]
fn the_host_scheme_and_port_come_together_or_not_at_all() {
    for capacity in [
        CapacityDetail {
            scheme: None,
            ..throttle()
        },
        CapacityDetail {
            host: None,
            ..throttle()
        },
        CapacityDetail {
            port: None,
            ..throttle()
        },
    ] {
        capacity_refused(capacity);
    }
}

#[test]
fn ports_limits_and_times_are_range_checked() {
    for port in [0, -1, 65_536, i64::MAX] {
        capacity_refused(CapacityDetail {
            port: Some(port),
            ..throttle()
        });
    }
    for limit in [0, -1, 33, i64::MAX] {
        capacity_refused(CapacityDetail {
            limit_applied: Some(limit),
            ..throttle()
        });
    }
    for ceiling in [0, -1, i64::from(u32::MAX) + 1] {
        capacity_refused(CapacityDetail {
            max_per_host: Some(ceiling),
            limit_applied: None,
            ..throttle()
        });
    }
    capacity_refused(CapacityDetail {
        elapsed_ms: Some(-1),
        ..throttle()
    });
    capacity_refused(CapacityDetail {
        retry_after_ms: Some(-1),
        ..throttle()
    });
    // The largest values still pass.
    let top = capacity_message(
        ErrorCode::Capacity,
        with_capacity(CapacityDetail {
            max_per_host: Some(i64::from(u32::MAX)),
            limit_applied: Some(i64::from(u32::MAX)),
            elapsed_ms: Some(i64::MAX),
            retry_after_ms: Some(i64::MAX),
            ..long_retry_after()
        }),
    );
    assert_eq!(codec::decode(&codec::encode(&top).unwrap()), Ok(top));
}

#[test]
fn a_capacity_host_is_a_bare_host_name_never_a_url_or_instruction() {
    for host in [
        "",
        "https://github.com",
        "github.com/path",
        "user@github.com",
        "github.com:443",
        "git hub.com",
        "github.com\nx",
        "secret=token",
        "ümlaut.example",
    ] {
        capacity_refused(CapacityDetail {
            host: Some(host.into()),
            ..throttle()
        });
    }
    capacity_refused(CapacityDetail {
        host: Some("a".repeat(256)),
        ..throttle()
    });
    for host in [
        "github.com",
        "10.0.0.1",
        "::1",
        "2001:db8::1",
        "a-b.c_d.example",
    ] {
        let value = capacity_message(
            ErrorCode::Capacity,
            with_capacity(CapacityDetail {
                host: Some(host.into()),
                ..throttle()
            }),
        );
        assert_eq!(codec::decode(&codec::encode(&value).unwrap()), Ok(value));
    }
}

#[test]
fn a_capacity_detail_stands_alone_among_the_diagnostic_alternatives() {
    for detail in [
        FailureDetail {
            helper_cause: Some(HelperFailureCause::MissingField),
            capacity: Some(throttle()),
            ..FailureDetail::default()
        },
        FailureDetail {
            pipe_kind: Some("Other".into()),
            capacity: Some(throttle()),
            ..FailureDetail::default()
        },
        FailureDetail {
            schemes: Some(vec!["Basic".into()]),
            capacity: Some(throttle()),
            ..FailureDetail::default()
        },
        FailureDetail {
            helper_budget_ms: Some(1),
            capacity: Some(throttle()),
            ..FailureDetail::default()
        },
    ] {
        refused_with(ErrorCode::Capacity, detail);
    }
}

#[test]
fn every_failure_carrier_checks_the_capacity_detail() {
    use gwz_transport::protocol::{Closed, Disposition, Facts};
    let mut value = capacity_message(ErrorCode::Capacity, with_capacity(throttle()));
    let good = value.open_failed.take().unwrap();
    let wrong_code = Failure {
        code: ErrorCode::Io,
        ..good.clone()
    };
    for kind in [
        MessageKind::Failed,
        MessageKind::IdentityCheckFailed,
        MessageKind::Closed,
    ] {
        for (failure, valid) in [(&good, true), (&wrong_code, false)] {
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
                    })
                }
                _ => unreachable!(),
            }
            if valid {
                assert_eq!(
                    codec::decode(&codec::encode(&value).unwrap()),
                    Ok(value.clone())
                );
            } else {
                assert_eq!(codec::admit(&value), Err(codec::Error::InvalidMessage));
                assert_eq!(
                    codec::decode(&cbor::encode(&value.to_cbor())),
                    Err(codec::Error::InvalidMessage)
                );
            }
        }
    }
}
