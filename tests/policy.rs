use gwz_transport::{
    binding::{self, EndpointConfig},
    codec,
    protocol::*,
};

fn open(scheme: Scheme, policy: AuthPolicy, mode: IdentityMode) -> Envelope {
    Envelope {
        version: 1,
        session_id: "policy".into(),
        stream_id: 1,
        kind: MessageKind::Open,
        open: Some(Open {
            endpoint_id: "endpoint".into(),
            operation_id: "operation".into(),
            destination: Destination {
                scheme,
                host: "example.test".into(),
                port: 443,
                path: "/repo".into(),
                ssh_username: if scheme == Scheme::Ssh {
                    Some("git".into())
                } else {
                    None
                },
            },
            service: GitService::UploadPackExchange,
            identity: Identity {
                mode,
                key_path: if mode == IdentityMode::ExplicitKey {
                    Some("key".into())
                } else {
                    None
                },
                path_base: None,
            },
            policy,
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
fn complete_scheme_policy_identity_matrix_is_unambiguous() {
    let endpoint = EndpointConfig {
        endpoint_id: "endpoint".into(),
        role: EndpointRole::Local,
        schemes: vec![Scheme::Ssh, Scheme::Https],
        policies: vec![
            AuthPolicy::SshAmbient,
            AuthPolicy::SshExplicit,
            AuthPolicy::Anonymous,
            AuthPolicy::Gh,
        ],
        limits: binding::default_limits(),
        trust_owner: "account".into(),
    };
    let (_, binding) = endpoint
        .accept(&binding::offer("policy", EndpointRole::Local))
        .unwrap();
    for scheme in [Scheme::Ssh, Scheme::Https] {
        for policy in [
            AuthPolicy::SshAmbient,
            AuthPolicy::SshExplicit,
            AuthPolicy::Anonymous,
            AuthPolicy::Gh,
        ] {
            for mode in [
                IdentityMode::Ambient,
                IdentityMode::ExplicitKey,
                IdentityMode::CredentialsDisabled,
            ] {
                let expected = matches!(
                    (scheme, policy, mode),
                    (Scheme::Ssh, AuthPolicy::SshAmbient, IdentityMode::Ambient)
                        | (
                            Scheme::Ssh,
                            AuthPolicy::SshExplicit,
                            IdentityMode::ExplicitKey
                        )
                        | (
                            Scheme::Https,
                            AuthPolicy::Anonymous,
                            IdentityMode::CredentialsDisabled
                        )
                        | (Scheme::Https, AuthPolicy::Gh, IdentityMode::Ambient)
                );
                let message = open(scheme, policy, mode);
                assert_eq!(
                    codec::admit(&message).is_ok(),
                    expected,
                    "{scheme:?}/{policy:?}/{mode:?}"
                );
                assert_eq!(binding.check_open(&message).is_ok(), expected);
            }
        }
    }
}

#[test]
fn impossible_capabilities_never_install_a_binding() {
    let endpoint = EndpointConfig {
        endpoint_id: "endpoint".into(),
        role: EndpointRole::Local,
        schemes: vec![Scheme::Ssh],
        policies: vec![AuthPolicy::Gh],
        limits: binding::default_limits(),
        trust_owner: "account".into(),
    };
    let offer = binding::offer("policy", EndpointRole::Local);
    assert_eq!(endpoint.accept(&offer).unwrap_err().effect, Effect::None);
    let mut reply = offer.clone();
    reply.kind = MessageKind::Bound;
    reply.bind = None;
    reply.bound = Some(Bound {
        version: 1,
        endpoint_id: "endpoint".into(),
        role: EndpointRole::Local,
        schemes: vec![Scheme::Ssh],
        policies: vec![AuthPolicy::Gh],
        receive_limits: binding::default_limits(),
        trust_owner: "account".into(),
    });
    assert_eq!(
        binding::verify(&offer, &reply).unwrap_err().effect,
        Effect::None
    );
}
