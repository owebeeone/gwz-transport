use gwz_transport::{
    binding::{self, EndpointConfig},
    codec,
    protocol::*,
    stream::{Config, Error, Side, StreamMachine},
};

mod cbor {
    pub use gwz_transport::cbor::*;
}

#[path = "fixtures/retained_protocol_v1.rs"]
mod retained_protocol_v1;

fn endpoint() -> EndpointConfig {
    EndpointConfig {
        endpoint_id: "endpoint".into(),
        role: EndpointRole::Driver,
        schemes: vec![Scheme::Ssh],
        policies: vec![AuthPolicy::SshExplicit],
        limits: binding::default_limits(),
        trust_owner: "owner".into(),
    }
}

fn v2_offer() -> Envelope {
    let mut offer = binding::offer("session", EndpointRole::Driver);
    offer.bind.as_mut().unwrap().versions = vec![1, 2];
    offer
}

fn identity_check(version: i64) -> Envelope {
    Envelope {
        version,
        session_id: "session".into(),
        stream_id: 1,
        kind: MessageKind::CheckIdentity,
        check_identity: Some(CheckIdentity {
            endpoint_id: "endpoint".into(),
            operation_id: "operation".into(),
            identity: Identity {
                mode: IdentityMode::ExplicitKey,
                key_path: Some("key".into()),
                path_base: None,
            },
            timeout_ms: 100,
        }),
        ..Default::default()
    }
}

fn open_message(version: i64, identity: Identity) -> Envelope {
    Envelope {
        version,
        session_id: "session".into(),
        stream_id: 1,
        kind: MessageKind::Open,
        open: Some(Open {
            endpoint_id: "endpoint".into(),
            operation_id: "operation".into(),
            destination: Destination {
                scheme: Scheme::Ssh,
                host: "example.test".into(),
                port: 22,
                path: "/repo".into(),
                ssh_username: Some("git".into()),
            },
            service: GitService::UploadPackExchange,
            identity,
            policy: AuthPolicy::SshExplicit,
            deadlines: Deadlines {
                allocation_ms: 1,
                connect_ms: 1,
                io_ms: 1,
                interaction_ms: 1,
                cleanup_ms: 1,
            },
            receive_limits: binding::default_limits(),
        }),
        ..Default::default()
    }
}

#[test]
fn highest_common_profile_is_selected_after_v1_bootstrap() {
    let (reply, binding) = endpoint().accept(&v2_offer()).unwrap();
    assert_eq!(reply.version, 1);
    assert_eq!(reply.bound.as_ref().unwrap().version, 2);
    assert!(binding.check_identity(&identity_check(2)).is_ok());
    assert_eq!(
        binding.check_identity(&identity_check(1)).unwrap_err().code,
        ErrorCode::InvalidRequest
    );
}

#[test]
fn v2_identity_check_cannot_be_sent_under_v1_profile() {
    assert_eq!(
        codec::admit(&identity_check(1)),
        Err(codec::Error::UnsupportedVersion)
    );
}

#[test]
fn identity_paths_reject_empty_and_nul_before_endpoint_effects() {
    for (key_path, path_base) in [
        (Some("\0key".into()), None),
        (Some("key".into()), Some("".into())),
        (Some("key".into()), Some("base\0path".into())),
    ] {
        let identity = Identity {
            mode: IdentityMode::ExplicitKey,
            key_path,
            path_base,
        };
        assert_eq!(
            codec::admit(&open_message(2, identity.clone())),
            Err(codec::Error::InvalidMessage)
        );
        let mut check = identity_check(2);
        check.check_identity.as_mut().unwrap().identity = identity;
        assert_eq!(codec::admit(&check), Err(codec::Error::InvalidMessage));
    }
}

#[test]
fn identity_checked_requires_a_map_body() {
    use gwz_transport::cbor::{self, Cbor};
    let message = Envelope {
        version: 2,
        session_id: "session".into(),
        stream_id: 1,
        kind: MessageKind::IdentityChecked,
        identity_checked: Some(IdentityChecked {}),
        ..Default::default()
    };
    let mut tree = message.to_cbor();
    if let Cbor::Map(fields) = &mut tree {
        if let Some((_, value)) = fields.iter_mut().find(|(tag, _)| *tag == 26) {
            *value = Cbor::Int(1);
        }
    }
    assert!(codec::decode(&cbor::encode(&tree)).is_err());
}

#[test]
fn v1_rejects_v2_failure_fields_and_repository_refused_code() {
    use gwz_transport::cbor;

    let failures = [
        (MessageKind::BindRejected, 0),
        (MessageKind::OpenFailed, 1),
        (MessageKind::Failed, 1),
    ];
    for (kind, stream_id) in failures {
        for failure in [
            Failure {
                setup_cause: None,
                code: ErrorCode::Io,
                effect: Effect::Possible,
                facts: Some(Facts::default()),
            },
            Failure {
                setup_cause: None,
                code: ErrorCode::RepositoryRefused,
                effect: Effect::None,
                facts: None,
            },
        ] {
            let message = Envelope {
                version: 1,
                session_id: "session".into(),
                stream_id,
                kind,
                bind_rejected: (kind == MessageKind::BindRejected).then_some(failure.clone()),
                open_failed: (kind == MessageKind::OpenFailed).then_some(failure.clone()),
                failed: (kind == MessageKind::Failed).then_some(failure),
                ..Default::default()
            };
            assert_eq!(codec::admit(&message), Err(codec::Error::InvalidMessage));
            let bytes = cbor::encode(&message.to_cbor());
            assert_eq!(codec::decode(&bytes), Err(codec::Error::InvalidMessage));
        }
    }
    for failure in [
        Failure {
            setup_cause: None,
            code: ErrorCode::Io,
            effect: Effect::Possible,
            facts: Some(Facts::default()),
        },
        Failure {
            setup_cause: None,
            code: ErrorCode::RepositoryRefused,
            effect: Effect::None,
            facts: None,
        },
    ] {
        let message = Envelope {
            version: 1,
            session_id: "session".into(),
            stream_id: 1,
            kind: MessageKind::Closed,
            closed: Some(Closed {
                disposition: Disposition::Discarded,
                unread_response_discarded: false,
                facts: Facts::default(),
                failure: Some(failure),
            }),
            ..Default::default()
        };
        assert_eq!(codec::admit(&message), Err(codec::Error::InvalidMessage));
        let bytes = cbor::encode(&message.to_cbor());
        assert_eq!(codec::decode(&bytes), Err(codec::Error::InvalidMessage));
    }

    for (kind, body) in [
        (
            MessageKind::OpenFailed,
            Envelope {
                version: 2,
                session_id: "session".into(),
                stream_id: 1,
                kind: MessageKind::OpenFailed,
                open_failed: Some(Failure {
                    setup_cause: None,
                    code: ErrorCode::RepositoryRefused,
                    effect: Effect::None,
                    facts: Some(Facts::default()),
                }),
                ..Default::default()
            },
        ),
        (
            MessageKind::Failed,
            Envelope {
                version: 2,
                session_id: "session".into(),
                stream_id: 1,
                kind: MessageKind::Failed,
                failed: Some(Failure {
                    setup_cause: None,
                    code: ErrorCode::RepositoryRefused,
                    effect: Effect::None,
                    facts: Some(Facts::default()),
                }),
                ..Default::default()
            },
        ),
    ] {
        assert_eq!(body.kind, kind);
        assert!(codec::admit(&body).is_ok());
    }
    let closed_v2 = Envelope {
        version: 2,
        session_id: "session".into(),
        stream_id: 1,
        kind: MessageKind::Closed,
        closed: Some(Closed {
            disposition: Disposition::Discarded,
            unread_response_discarded: false,
            facts: Facts::default(),
            failure: Some(Failure {
                setup_cause: None,
                code: ErrorCode::RepositoryRefused,
                effect: Effect::None,
                facts: None,
            }),
        }),
        ..Default::default()
    };
    assert!(codec::admit(&closed_v2).is_ok());
}

#[test]
fn retained_v1_reader_accepts_new_v1_bytes_and_new_reader_accepts_old_fixture() {
    use gwz_transport::cbor::{self, Cbor};
    // Immutable generated v1 reader from transport commit a6562e6.
    let old = Envelope {
        version: 1,
        session_id: "old".into(),
        stream_id: 1,
        kind: MessageKind::Failed,
        failed: Some(Failure {
            setup_cause: None,
            code: ErrorCode::Io,
            effect: Effect::Possible,
            facts: None,
        }),
        ..Default::default()
    };
    let new_v1_bytes = codec::encode(&old).unwrap();
    let new_tree = cbor::try_decode(&new_v1_bytes).unwrap();
    let Cbor::Map(new_fields) = &new_tree else {
        panic!("envelope must be a map")
    };
    let retained = retained_protocol_v1::Envelope::from_cbor(&new_tree).unwrap();
    assert_eq!(retained.version, 1);
    assert_eq!(retained.kind, retained_protocol_v1::MessageKind::Failed);
    assert!(retained.failed.is_some());
    // The retained decoder recognizes tags 1..24 and ignores additive slots.
    assert!(new_fields.iter().any(|(tag, _)| *tag == 24));
    assert!(new_fields.iter().any(|(tag, _)| *tag == 25));
    let failure_value = new_fields
        .iter()
        .find(|(tag, _)| *tag == 24)
        .map(|(_, value)| value)
        .expect("failed body");
    let Cbor::Map(failure_fields) = failure_value else {
        panic!("failed body must be a map")
    };
    assert!(failure_fields.iter().any(|(tag, _)| *tag == 1));
    assert!(failure_fields.iter().any(|(tag, _)| *tag == 2));
    assert!(failure_fields.iter().any(|(tag, _)| *tag == 3));
    let mut tree = old.to_cbor();
    if let Cbor::Map(fields) = &mut tree {
        fields.retain(|(tag, _)| *tag < 25);
        if let Some((_, Cbor::Map(failure))) = fields.iter_mut().find(|(tag, _)| *tag == 24) {
            failure.retain(|(tag, _)| *tag != 3);
        }
    }
    let bytes = cbor::encode(&tree);
    assert_eq!(codec::decode(&bytes).unwrap(), old);
    let retained_old = retained_protocol_v1::Envelope {
        version: 1,
        session_id: "old-fixture".into(),
        stream_id: 1,
        kind: retained_protocol_v1::MessageKind::Failed,
        failed: Some(retained_protocol_v1::Failure {
            code: retained_protocol_v1::ErrorCode::Io,
            effect: retained_protocol_v1::Effect::Possible,
        }),
        ..Default::default()
    };
    let retained_old_bytes = cbor::encode(&retained_old.to_cbor());
    let decoded = codec::decode(&retained_old_bytes).unwrap();
    assert_eq!(decoded.version, 1);
    assert_eq!(decoded.kind, MessageKind::Failed);
    assert_eq!(decoded.failed.unwrap().facts, None);
}

#[test]
fn typed_allocation_charge_is_bounded_and_nonzero() {
    let message = identity_check(2);
    let charge = codec::allocation_charge(&message, &binding::default_limits()).unwrap();
    assert!(charge > 0);
    let mut limits = binding::default_limits();
    limits.decode_allocation = 1;
    assert_eq!(
        codec::allocation_charge(&message, &limits),
        Err(codec::Error::Bounds)
    );
}

#[test]
fn closed_nested_failure_facts_are_rejected() {
    let message = Envelope {
        version: 2,
        session_id: "session".into(),
        stream_id: 1,
        kind: MessageKind::Closed,
        closed: Some(Closed {
            disposition: Disposition::Discarded,
            unread_response_discarded: false,
            facts: Facts::default(),
            failure: Some(Failure {
                setup_cause: None,
                code: ErrorCode::RepositoryRefused,
                effect: Effect::None,
                facts: Some(Facts::default()),
            }),
        }),
        ..Default::default()
    };
    assert_eq!(codec::admit(&message), Err(codec::Error::InvalidMessage));
}

#[test]
fn identity_check_failure_has_no_effect_or_facts() {
    let message = Envelope {
        version: 2,
        session_id: "session".into(),
        stream_id: 1,
        kind: MessageKind::IdentityCheckFailed,
        identity_check_failed: Some(Failure {
            setup_cause: None,
            code: ErrorCode::Authentication,
            effect: Effect::Possible,
            facts: Some(Facts::default()),
        }),
        ..Default::default()
    };
    assert_eq!(codec::admit(&message), Err(codec::Error::InvalidMessage));
}

#[test]
fn typed_close_failure_retains_authoritative_facts() {
    let mut config = Config::new("session", 1, Side::Initiator);
    config.profile_version = 2;
    let mut machine = StreamMachine::new(config).unwrap();
    let facts = Facts::default();
    machine
        .receive(Envelope {
            version: 2,
            session_id: "session".into(),
            stream_id: 1,
            kind: MessageKind::Failed,
            failed: Some(Failure {
                setup_cause: None,
                code: ErrorCode::RepositoryRefused,
                effect: Effect::None,
                facts: Some(facts.clone()),
            }),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(
        machine.read(&mut [0]),
        Err(Error::PeerFailed {
            code: ErrorCode::RepositoryRefused,
            effect: Effect::None,
        })
    );
    assert_eq!(machine.retained_failure_facts(), Some(&facts));
}
