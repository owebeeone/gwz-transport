//! The closed `ExchangeSpec` to `Open.service` table (GwzSplitCommandChannel section 3.4), in the typed form 1.3b
//! froze: `ExchangeSpec::git(key, identity, service, path)` carries a `GitService` and a path, never an SSH command
//! string, an HTTP method, a header or a content type, so most rows of the section's table cannot be built and the
//! refusals that remain are about shape.
//!
//! | Typed spec | `Open` |
//! | --- | --- |
//! | SSH key, `UploadPackExchange` / `ReceivePackExchange` | `upload_pack_exchange` / `receive_pack_exchange` |
//! | HTTPS key, any of the four services | the service of the same name, identity `credentials_disabled`, policy `anonymous` |
//! | SSH key with an advertisement service | refused: SSH has no advertisement row |
//! | HTTPS key with a username, an SSH key without one, an identity of the other scheme, a scoped HTTPS identity | refused |
//!
//! The same shape check runs on the client for every `Open` it receives (`check_shape`).
use crate::{ExchangeSpec, GitService};
use gwz_transport::{
    pool::{Identity as PoolIdentity, Key},
    protocol::{
        AuthPolicy, Deadlines, Destination, GitService as WireService, Identity as WireIdentity,
        IdentityMode, Limits, Open, Scheme,
    },
};

/// Why the table has no row for a spec or an `Open`. The core refuses with `invalid_request` and `Effect::None` and
/// sends nothing; the client refuses a received `Open` the same way, as a fault of its binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableRefusal {
    /// SSH has exchanges only: an advertisement is the smart-HTTP GET.
    SshAdvertisement,
    /// The identity does not belong to the key's scheme, or is one only the client can mint (a scoped HTTPS identity).
    Identity,
    /// An SSH key needs a username; an HTTPS key and destination carry none in the key.
    Username,
    /// An explicit key without the path base the client sent.
    PathBase,
}

/// The wire service of a crate service.
pub(crate) fn wire_service(service: GitService) -> WireService {
    match service {
        GitService::UploadPackAdvertisement => WireService::UploadPackAdvertisement,
        GitService::UploadPackExchange => WireService::UploadPackExchange,
        GitService::ReceivePackAdvertisement => WireService::ReceivePackAdvertisement,
        GitService::ReceivePackExchange => WireService::ReceivePackExchange,
    }
}
pub(crate) fn crate_service(service: WireService) -> GitService {
    match service {
        WireService::UploadPackAdvertisement => GitService::UploadPackAdvertisement,
        WireService::UploadPackExchange => GitService::UploadPackExchange,
        WireService::ReceivePackAdvertisement => GitService::ReceivePackAdvertisement,
        WireService::ReceivePackExchange => GitService::ReceivePackExchange,
    }
}

/// The table's shape rule, shared by the core (before it sends) and the client (before it admits).
pub(crate) fn check_shape(scheme: Scheme, service: WireService) -> Result<(), TableRefusal> {
    if scheme == Scheme::Ssh
        && matches!(
            service,
            WireService::UploadPackAdvertisement | WireService::ReceivePackAdvertisement
        )
    {
        return Err(TableRefusal::SshAdvertisement);
    }
    Ok(())
}

/// What the core knows when it builds an `Open`: the binding's identity and limits, and the client's path base for the
/// call (from the live-call registry). The core chooses none of the client's policy: the one `AuthPolicy` it names is
/// the least that the scheme and identity need, and the client admits it only if its own set holds it.
pub(crate) struct OpenContext<'a> {
    pub(crate) endpoint_id: &'a str,
    pub(crate) operation_id: &'a str,
    pub(crate) limits: &'a Limits,
    pub(crate) deadlines: &'a Deadlines,
    pub(crate) path_base: Option<&'a str>,
}

/// Maps a spec to its `Open`, or refuses it. Nothing is sent by this function.
pub(crate) fn open_for(
    spec: &ExchangeSpec,
    context: &OpenContext<'_>,
) -> Result<Open, TableRefusal> {
    let key = spec.key();
    let service = wire_service(spec.service());
    check_shape(key.scheme, service)?;
    let (identity, policy) = match (key.scheme, spec.identity()) {
        (Scheme::Ssh, PoolIdentity::Ambient) => (
            WireIdentity {
                mode: IdentityMode::Ambient,
                key_path: None,
                path_base: None,
            },
            AuthPolicy::SshAmbient,
        ),
        (Scheme::Ssh, PoolIdentity::Explicit(key_path)) => (
            WireIdentity {
                mode: IdentityMode::ExplicitKey,
                key_path: Some(key_path.clone()),
                path_base: Some(context.path_base.ok_or(TableRefusal::PathBase)?.to_owned()),
            },
            AuthPolicy::SshExplicit,
        ),
        (Scheme::Https, PoolIdentity::Https) => (
            WireIdentity {
                mode: IdentityMode::CredentialsDisabled,
                key_path: None,
                path_base: None,
            },
            AuthPolicy::Anonymous,
        ),
        _ => return Err(TableRefusal::Identity),
    };
    Ok(Open {
        endpoint_id: context.endpoint_id.to_owned(),
        operation_id: context.operation_id.to_owned(),
        destination: destination_of(key, spec.path())?,
        service,
        identity,
        policy,
        deadlines: context.deadlines.clone(),
        receive_limits: context.limits.clone(),
    })
}
fn destination_of(key: &Key, path: &str) -> Result<Destination, TableRefusal> {
    let (ssh_username, https_username) = match (key.scheme, &key.username) {
        (Scheme::Ssh, Some(user)) => (Some(user.clone()), None),
        (Scheme::Https, None) => (None, None),
        _ => return Err(TableRefusal::Username),
    };
    Ok(Destination {
        scheme: key.scheme,
        host: key.host.clone(),
        port: i64::from(key.port),
        path: path.to_owned(),
        ssh_username,
        https_username,
    })
}
