use gwz_transport::{
    binding, codec,
    mux::{Config, Error, Mux, Phase},
    protocol::*,
};

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
                },
                service: GitService::UploadPackExchange,
                identity: Identity {
                    mode: if policy == AuthPolicy::Gh {
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
