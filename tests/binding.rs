use gwz_transport::{
    binding::{self, EndpointConfig},
    protocol::*,
};

fn endpoint() -> EndpointConfig {
    EndpointConfig {
        endpoint_id: "cli-1".into(),
        role: EndpointRole::Driver,
        schemes: vec![Scheme::Ssh],
        policies: vec![AuthPolicy::SshAmbient],
        limits: binding::default_limits(),
        trust_owner: "local-account".into(),
    }
}

#[test]
fn binding_intersects_capabilities_and_narrows_limits() {
    let endpoint = endpoint();
    let mut offer = binding::offer("session-1", EndpointRole::Driver);
    offer.bind.as_mut().unwrap().receive_limits.data_payload = 8192;
    let (reply, binding) = endpoint.accept(&offer).unwrap();
    assert_eq!(reply.bound.as_ref().unwrap().schemes, vec![Scheme::Ssh]);
    assert_eq!(
        reply.bound.as_ref().unwrap().receive_limits.data_payload,
        8192
    );
    let verified = binding::verify(&offer, &reply).unwrap();
    assert_eq!(verified, binding);
}

#[test]
fn unsupported_version_has_no_installed_binding() {
    let mut offer = binding::offer("session-1", EndpointRole::Driver);
    offer.bind.as_mut().unwrap().versions = vec![4];
    let error = endpoint().accept(&offer).unwrap_err();
    assert_eq!(error.code, ErrorCode::UnsupportedVersion);
    assert_eq!(error.effect, Effect::None);
}

/// Profile 3, the sequenced profile, binds only in a build with the unreviewed
/// `unstable-sequenced` feature. Without it the endpoint answers as one older
/// than the profile: a Bind that offers only profile 3, as a sequenced host
/// offers it, is refused as `UnsupportedVersion` with no effect and no
/// binding, so no Open, lease, credential or Git work can follow; an offer
/// with other profiles binds the highest of those; and a host refuses a
/// Bound at profile 3.
#[test]
fn profile_three_binds_only_with_the_unstable_sequenced_feature() {
    let mut offer = binding::offer("session-1", EndpointRole::Driver);
    offer.bind.as_mut().unwrap().versions = vec![3];
    let accepted = endpoint().accept(&offer);
    let mut mixed = offer.clone();
    mixed.bind.as_mut().unwrap().versions = vec![1, 2, 3];
    let (mixed_reply, _) = endpoint().accept(&mixed).unwrap();
    let mut reply = mixed_reply.clone();
    reply.bound.as_mut().unwrap().version = 3;
    let verified = binding::verify(&offer, &reply);
    if cfg!(feature = "unstable-sequenced") {
        assert_eq!(accepted.unwrap().1.profile_version(), 3);
        assert_eq!(mixed_reply.bound.unwrap().version, 3);
        assert_eq!(verified.unwrap().profile_version(), 3);
    } else {
        let refused = accepted.unwrap_err();
        assert_eq!(refused.code, ErrorCode::UnsupportedVersion);
        assert_eq!(refused.effect, Effect::None);
        assert_eq!(mixed_reply.bound.unwrap().version, 2);
        assert_eq!(verified.unwrap_err().code, ErrorCode::InvalidRequest);
    }
}

#[test]
fn acknowledgment_cannot_raise_limits_or_change_session() {
    let offer = binding::offer("session-1", EndpointRole::Driver);
    let (mut reply, _) = endpoint().accept(&offer).unwrap();
    reply.session_id = "old-session".into();
    assert!(binding::verify(&offer, &reply).is_err());
    reply.session_id = offer.session_id.clone();
    reply.bound.as_mut().unwrap().receive_limits.data_payload = 65537;
    assert!(binding::verify(&offer, &reply).is_err());
}

#[test]
fn unusable_control_reserve_refuses_without_effects() {
    let mut offer = binding::offer("session-1", EndpointRole::Driver);
    offer
        .bind
        .as_mut()
        .unwrap()
        .receive_limits
        .control_reserve_bytes = 1;
    assert!(endpoint().accept(&offer).is_err());
}
