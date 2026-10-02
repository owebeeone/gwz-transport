use gwz_transport::{binding, cbor, codec, protocol::*};

fn open(selector: Option<&str>) -> Envelope {
    Envelope {
        version: 2,
        session_id: "selector".into(),
        stream_id: 1,
        kind: MessageKind::Open,
        open: Some(Open {
            endpoint_id: "https".into(),
            operation_id: "operation".into(),
            destination: Destination {
                scheme: Scheme::Https,
                host: "example.test".into(),
                port: 443,
                path: "/repo".into(),
                ssh_username: None,
                https_username: selector.map(str::to_owned),
            },
            service: GitService::UploadPackAdvertisement,
            identity: Identity {
                mode: IdentityMode::CredentialsDisabled,
                ..Default::default()
            },
            policy: AuthPolicy::Anonymous,
            deadlines: Deadlines {
                allocation_ms: 100,
                connect_ms: 100,
                io_ms: 100,
                interaction_ms: 100,
                cleanup_ms: 100,
            },
            receive_limits: binding::default_limits(),
        }),
        ..Default::default()
    }
}

fn refused(value: &Envelope) {
    assert_eq!(codec::admit(value), Err(codec::Error::InvalidMessage));
    assert_eq!(codec::decode(&cbor::encode(&value.to_cbor())), Err(codec::Error::InvalidMessage));
}

#[test]
fn encoded_selector_is_private_and_round_trips_unchanged() {
    for selector in [None, Some("account%2Bselector%40team"), Some("name%oops"), Some("%FF")] {
        let value = open(selector);
        assert_eq!(codec::decode(&codec::encode(&value).unwrap()), Ok(value));
    }
    assert!(!format!("{:?}", open(Some("sentinel-account"))).contains("sentinel-account"));
    let a = open(Some("account-a"));
    let b = open(Some("account-b"));
    assert_ne!(codec::encode(&a).unwrap(), codec::encode(&b).unwrap());
}

#[test]
fn refused_selectors_and_decoded_controls_never_enter_open() {
    for selector in ["", "a:b", "a@b", "a/b", "a?b", "a#b", "a\\b", "a b", "ümlaut", "%00", "%0d", "%0A", "%7f", "%C2%85", "%FF%0a"] {
        refused(&open(Some(selector)));
    }
    for suffix in ["%00", "%0d", "%0A", "%7f", "%C2%85", "%FF%0a"] {
        let mut value = open(None);
        value.open.as_mut().unwrap().destination.path.push_str(suffix);
        refused(&value);
    }
    let mut ssh = open(Some("account"));
    ssh.open.as_mut().unwrap().destination.scheme = Scheme::Ssh;
    refused(&ssh);
}

#[test]
fn complete_serialized_url_limit_counts_selector_and_ipv6_brackets() {
    let account = "a".repeat(2_000);
    let mut value = open(Some(&account));
    let destination = &mut value.open.as_mut().unwrap().destination;
    let fixed = format!("https://{account}@example.test").len();
    destination.path = format!("/{}", "r".repeat(18_000 - fixed - 1));
    assert!(codec::admit(&value).is_ok());
    value.open.as_mut().unwrap().destination.path.push('r');
    refused(&value);

    let mut ipv6 = open(Some(&account));
    let destination = &mut ipv6.open.as_mut().unwrap().destination;
    destination.host = "::1".into();
    destination.port = 8443;
    let fixed = format!("https://{account}@[::1]:8443").len();
    destination.path = format!("/{}", "r".repeat(18_000 - fixed - 1));
    assert!(codec::admit(&ipv6).is_ok());
    ipv6.open.as_mut().unwrap().destination.path.push('r');
    refused(&ipv6);
}
