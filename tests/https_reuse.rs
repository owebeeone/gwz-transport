use gwz_transport::{
    binding, codec,
    mux::{Config, Error, Mux, Phase},
    protocol::*,
};

fn native_facts() -> Facts {
    Facts {
        method: AuthMethod::Sspi,
        native: Some(NativeFacts {
            source: NativeSource::CurrentLogon,
            scheme: NativeScheme::Ntlm,
            observation: NativeObservation::Selected,
            mechanism: Some(NativeMechanism::Ntlm),
            authoritative: true,
        }),
        credential_offered: true,
        ..Default::default()
    }
}
fn native_opened(facts: Facts, version: i64) -> Envelope {
    Envelope {
        version,
        session_id: "native".into(),
        stream_id: 1,
        kind: MessageKind::Opened,
        opened: Some(Opened {
            connection_id: "physical".into(),
            endpoint_id: "endpoint".into(),
            trust_owner: "account".into(),
            reused: true,
            facts,
            receive_limits: binding::default_limits(),
        }),
        ..Default::default()
    }
}
#[test]
fn native_facts_are_profile_gated_and_preserve_authority_without_remote_success() {
    let message = native_opened(native_facts(), 2);
    let bytes = codec::encode(&message).unwrap();
    let decoded = codec::decode(&bytes).unwrap();
    let facts = decoded.opened.unwrap().facts;
    assert!(facts.native.unwrap().authoritative);
    assert_eq!(facts.authenticated, None);
    assert!(codec::encode(&native_opened(native_facts(), 1)).is_err());
    let mut invalid = native_facts();
    invalid.native = None;
    assert!(codec::encode(&native_opened(invalid, 2)).is_err());
    let mut invalid = native_facts();
    invalid.method = AuthMethod::Gh;
    assert!(codec::encode(&native_opened(invalid, 2)).is_err());
    for case in 0..5 {
        let mut invalid = native_facts();
        let native = invalid.native.as_mut().unwrap();
        match case {
            0 => native.mechanism = None,
            1 => native.mechanism = Some(NativeMechanism::Kerberos),
            2 => {
                native.observation = NativeObservation::Unresolved;
                native.mechanism = None;
                native.authoritative = false;
            }
            3 => {
                native.observation = NativeObservation::NotStarted;
                native.mechanism = None;
                native.authoritative = false;
            }
            _ => native.scheme = NativeScheme::Digest,
        }
        assert!(
            codec::encode(&native_opened(invalid, 2)).is_err(),
            "case={case}"
        );
    }
    let mut unknown = native_facts();
    unknown.credential_offered = false;
    let native = unknown.native.as_mut().unwrap();
    native.observation = NativeObservation::NotStarted;
    native.mechanism = None;
    native.authoritative = false;
    let mut message = native_opened(unknown, 2);
    message.opened.as_mut().unwrap().reused = false;
    assert!(codec::encode(&message).is_ok());
    message.opened.as_mut().unwrap().facts.authenticated = Some(true);
    assert!(codec::encode(&message).is_err());
}
#[test]
fn native_binding_requires_an_acknowledged_policy_and_profile_two() {
    let endpoint = binding::EndpointConfig {
        endpoint_id: "endpoint".into(),
        role: EndpointRole::Driver,
        schemes: vec![Scheme::Https],
        policies: vec![AuthPolicy::WindowsDefault],
        limits: binding::default_limits(),
        trust_owner: "account".into(),
    };
    let mut offer = binding::offer("native", EndpointRole::Driver);
    let bind = offer.bind.as_mut().unwrap();
    bind.versions = vec![2];
    bind.schemes = vec![Scheme::Https];
    bind.policies = vec![AuthPolicy::WindowsDefault];
    let (reply, _) = endpoint.accept(&offer).unwrap();
    assert_eq!(
        binding::verify(&offer, &reply).unwrap().profile_version(),
        2
    );
    offer.bind.as_mut().unwrap().versions = vec![1];
    assert_eq!(
        endpoint.accept(&offer).err().unwrap().code,
        ErrorCode::UnsupportedOperation
    );
}

fn pair_for(scheme: Scheme, policy: AuthPolicy) -> (Mux, Mux) {
    let config = Config::default();
    let endpoint = binding::EndpointConfig {
        endpoint_id: "endpoint".into(),
        role: EndpointRole::Driver,
        schemes: vec![scheme],
        policies: vec![policy],
        limits: binding::default_limits(),
        trust_owner: "account".into(),
    };
    (
        Mux::initiator("session", config.clone()).unwrap(),
        Mux::endpoint("session", endpoint, config).unwrap(),
    )
}

fn bind(core: &mut Mux, endpoint: &mut Mux) {
    core.register("a", Some("op-a".into())).unwrap();
    endpoint.register("a", None).unwrap();
    core.begin("a").unwrap();
    endpoint.receive(&core.next_message().unwrap()).unwrap();
    core.receive(&endpoint.next_message().unwrap()).unwrap();
    assert_eq!(core.phase(), Phase::Ready);
}

fn open_stream(core: &mut Mux, endpoint: &mut Mux, scheme: Scheme, policy: AuthPolicy) -> i64 {
    let id = core
        .open(
            "a",
            Open {
                endpoint_id: "endpoint".into(),
                operation_id: "op-a".into(),
                destination: Destination {
                    scheme,
                    host: "git.example".into(),
                    port: if scheme == Scheme::Https { 443 } else { 22 },
                    path: "repo".into(),
                    ssh_username: (scheme == Scheme::Ssh).then(|| "git".into()),
                    https_username: None,
                },
                service: GitService::UploadPackExchange,
                identity: Identity {
                    mode: if matches!(
                        policy,
                        AuthPolicy::Gh | AuthPolicy::WindowsConfigured | AuthPolicy::WindowsDefault
                    ) {
                        IdentityMode::Ambient
                    } else if scheme == Scheme::Https {
                        IdentityMode::CredentialsDisabled
                    } else {
                        IdentityMode::Ambient
                    },
                    ..Default::default()
                },
                policy,
                deadlines: Deadlines {
                    allocation_ms: 100,
                    connect_ms: 100,
                    io_ms: 100,
                    interaction_ms: 100,
                    cleanup_ms: 100,
                },
                receive_limits: binding::default_limits(),
            },
        )
        .unwrap();
    endpoint.receive(&core.next_message().unwrap()).unwrap();
    endpoint.next_action().unwrap().1;
    id
}

fn reused_credential_opened(endpoint: &Mux, id: i64) -> Envelope {
    let mut opened = endpoint.message(id, MessageKind::Opened).unwrap();
    opened.opened = Some(Opened {
        endpoint_id: "endpoint".into(),
        connection_id: "connection".into(),
        trust_owner: "account".into(),
        reused: true,
        facts: Facts {
            method: AuthMethod::Gh,
            credential_offered: true,
            ..Default::default()
        },
        receive_limits: binding::default_limits(),
    });
    opened
}

#[test]
fn https_gh_allows_credential_offer_on_reused_connection() {
    let (mut core, mut endpoint) = pair_for(Scheme::Https, AuthPolicy::Gh);
    bind(&mut core, &mut endpoint);
    let id = open_stream(&mut core, &mut endpoint, Scheme::Https, AuthPolicy::Gh);
    let opened = reused_credential_opened(&endpoint, id);
    assert_eq!(endpoint.send("a", &opened), Ok(()));
    core.receive(&endpoint.next_message().unwrap()).unwrap();
}

#[test]
fn https_anonymous_rejects_credential_offer_on_reused_connection() {
    let (mut core, mut endpoint) = pair_for(Scheme::Https, AuthPolicy::Anonymous);
    bind(&mut core, &mut endpoint);
    let id = open_stream(
        &mut core,
        &mut endpoint,
        Scheme::Https,
        AuthPolicy::Anonymous,
    );
    let opened = reused_credential_opened(&endpoint, id);
    assert_eq!(endpoint.send("a", &opened), Err(Error::Protocol));
}

#[test]
fn ssh_reuse_cannot_widen_to_a_gh_credential_offer() {
    let (mut core, mut endpoint) = pair_for(Scheme::Ssh, AuthPolicy::SshAmbient);
    bind(&mut core, &mut endpoint);
    let id = open_stream(
        &mut core,
        &mut endpoint,
        Scheme::Ssh,
        AuthPolicy::SshAmbient,
    );
    let opened = reused_credential_opened(&endpoint, id);
    assert_eq!(endpoint.send("a", &opened), Err(Error::Protocol));
}

#[test]
fn codec_rejects_reused_credential_facts_from_non_gh_auth() {
    let (mut core, mut endpoint) = pair_for(Scheme::Https, AuthPolicy::Gh);
    bind(&mut core, &mut endpoint);
    let id = open_stream(&mut core, &mut endpoint, Scheme::Https, AuthPolicy::Gh);
    let mut opened = reused_credential_opened(&endpoint, id);
    opened.opened.as_mut().unwrap().facts.method = AuthMethod::SshKey;
    assert_eq!(codec::admit(&opened), Err(codec::Error::InvalidMessage));
}

#[test]
fn https_discovery_failures_are_open_failed_before_opened() {
    let cases = [
        (ErrorCode::Authentication, 401),
        (ErrorCode::RepositoryRefused, 403),
        (ErrorCode::RepositoryRefused, 404),
        (ErrorCode::Io, 500),
    ];
    for (code, status) in cases {
        let (mut core, mut endpoint) = pair_for(Scheme::Https, AuthPolicy::Gh);
        bind(&mut core, &mut endpoint);
        let id = open_stream(&mut core, &mut endpoint, Scheme::Https, AuthPolicy::Gh);
        let failure = Failure {
            detail: None,
            setup_cause: None,
            code,
            effect: Effect::None,
            facts: Some(Facts {
                method: AuthMethod::Gh,
                credential_offered: true,
                authenticated: None,
                http_status: Some(status),
                ..Default::default()
            }),
        };
        let mut message = endpoint.message(id, MessageKind::OpenFailed).unwrap();
        message.open_failed = Some(failure.clone());
        assert_eq!(endpoint.send("a", &message), Ok(()));
        assert_eq!(endpoint.active_streams(), 0);
        let delivered = endpoint.next_message().unwrap();
        assert_eq!(delivered.1.kind, MessageKind::OpenFailed);
        assert_eq!(core.receive(&delivered), Ok(()));
        assert_eq!(core.active_streams(), 0);
        let terminal = core.next_action().unwrap().1;
        assert_eq!(terminal.kind, MessageKind::OpenFailed);
        assert_eq!(terminal.open_failed, Some(failure));
        assert!(terminal.opened.is_none());
        assert!(terminal.failed.is_none());
    }
}

#[test]
fn native_default_rejects_configured_claims_in_every_terminal_shape() {
    for method in [AuthMethod::Sspi, AuthMethod::Gh] {
        for kind in [MessageKind::Opened, MessageKind::OpenFailed] {
            let (mut core, mut endpoint) = pair_for(Scheme::Https, AuthPolicy::WindowsDefault);
            bind(&mut core, &mut endpoint);
            let id = open_stream(
                &mut core,
                &mut endpoint,
                Scheme::Https,
                AuthPolicy::WindowsDefault,
            );
            let mut facts = native_facts();
            if method == AuthMethod::Sspi {
                facts.native.as_mut().unwrap().source = NativeSource::Configured;
            } else {
                facts.method = method;
                facts.native = None;
            }
            let mut message = endpoint.message(id, kind).unwrap();
            if kind == MessageKind::Opened {
                message.opened = Some(Opened {
                    endpoint_id: "endpoint".into(),
                    connection_id: "generation".into(),
                    trust_owner: "account".into(),
                    reused: false,
                    facts,
                    receive_limits: binding::default_limits(),
                });
            } else {
                message.open_failed = Some(Failure {
                    code: ErrorCode::Authentication,
                    facts: Some(facts),
                    ..Default::default()
                });
            }
            assert_eq!(endpoint.send("a", &message), Err(Error::Protocol));
        }
    }
}
