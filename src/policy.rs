//! Flat capability lists denote only pairs in this compatibility relation.
use crate::protocol::{AuthPolicy, IdentityMode, Scheme};

pub(crate) fn allows(scheme: Scheme, policy: AuthPolicy, identity: IdentityMode) -> bool {
    matches!(
        (scheme, policy, identity),
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
    )
}
pub(crate) fn supports(scheme: Scheme, policy: AuthPolicy) -> bool {
    [
        IdentityMode::Ambient,
        IdentityMode::ExplicitKey,
        IdentityMode::CredentialsDisabled,
    ]
    .into_iter()
    .any(|identity| allows(scheme, policy, identity))
}
pub(crate) fn capabilities(schemes: &[Scheme], policies: &[AuthPolicy]) -> bool {
    !schemes.is_empty()
        && !policies.is_empty()
        && schemes
            .iter()
            .all(|s| policies.iter().any(|p| supports(*s, *p)))
        && policies
            .iter()
            .all(|p| schemes.iter().any(|s| supports(*s, *p)))
}
