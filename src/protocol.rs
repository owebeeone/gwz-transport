// GENERATED native Rust types + codec — do not edit.
#![allow(dead_code)]
use crate::cbor::{Cbor, DecodeError};

// The file's bounds, for a decode rooted at a type that is not a message:
// `cbor::try_decode_with(bytes, MAX_DEPTH, MAX_ENCODED_LEN)`.
pub const MAX_DEPTH: usize = 32;
pub const MAX_ENCODED_LEN: Option<usize> = None;

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum MessageKind {
    #[default]
    Bind,
    Bound,
    BindRejected,
    Open,
    Opened,
    OpenFailed,
    Data,
    Window,
    Flush,
    Flushed,
    EndWrite,
    Close,
    Closed,
    Cancel,
    Failed,
    CheckIdentity,
    IdentityChecked,
    IdentityCheckFailed,
}
impl MessageKind {
    pub fn wire(self) -> i64 {
        match self {
            Self::Bind => 1,
            Self::Bound => 2,
            Self::BindRejected => 3,
            Self::Open => 4,
            Self::Opened => 5,
            Self::OpenFailed => 6,
            Self::Data => 7,
            Self::Window => 8,
            Self::Flush => 9,
            Self::Flushed => 10,
            Self::EndWrite => 11,
            Self::Close => 12,
            Self::Closed => 13,
            Self::Cancel => 14,
            Self::Failed => 15,
            Self::CheckIdentity => 16,
            Self::IdentityChecked => 17,
            Self::IdentityCheckFailed => 18,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::Bind,
            2 => Self::Bound,
            3 => Self::BindRejected,
            4 => Self::Open,
            5 => Self::Opened,
            6 => Self::OpenFailed,
            7 => Self::Data,
            8 => Self::Window,
            9 => Self::Flush,
            10 => Self::Flushed,
            11 => Self::EndWrite,
            12 => Self::Close,
            13 => Self::Closed,
            14 => Self::Cancel,
            15 => Self::Failed,
            16 => Self::CheckIdentity,
            17 => Self::IdentityChecked,
            18 => Self::IdentityCheckFailed,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "MessageKind",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum EndpointRole {
    #[default]
    Local,
    Driver,
}
impl EndpointRole {
    pub fn wire(self) -> i64 {
        match self {
            Self::Local => 1,
            Self::Driver => 2,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::Local,
            2 => Self::Driver,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "EndpointRole",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum Scheme {
    #[default]
    Ssh,
    Https,
}
impl Scheme {
    pub fn wire(self) -> i64 {
        match self {
            Self::Ssh => 1,
            Self::Https => 2,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::Ssh,
            2 => Self::Https,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "Scheme",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum AuthPolicy {
    #[default]
    SshAmbient,
    SshExplicit,
    Anonymous,
    Gh,
    WindowsConfigured,
    WindowsDefault,
}
impl AuthPolicy {
    pub fn wire(self) -> i64 {
        match self {
            Self::SshAmbient => 1,
            Self::SshExplicit => 2,
            Self::Anonymous => 3,
            Self::Gh => 4,
            Self::WindowsConfigured => 5,
            Self::WindowsDefault => 6,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::SshAmbient,
            2 => Self::SshExplicit,
            3 => Self::Anonymous,
            4 => Self::Gh,
            5 => Self::WindowsConfigured,
            6 => Self::WindowsDefault,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "AuthPolicy",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum GitService {
    #[default]
    UploadPackAdvertisement,
    UploadPackExchange,
    ReceivePackAdvertisement,
    ReceivePackExchange,
}
impl GitService {
    pub fn wire(self) -> i64 {
        match self {
            Self::UploadPackAdvertisement => 1,
            Self::UploadPackExchange => 2,
            Self::ReceivePackAdvertisement => 3,
            Self::ReceivePackExchange => 4,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::UploadPackAdvertisement,
            2 => Self::UploadPackExchange,
            3 => Self::ReceivePackAdvertisement,
            4 => Self::ReceivePackExchange,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "GitService",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum IdentityMode {
    #[default]
    Ambient,
    ExplicitKey,
    CredentialsDisabled,
}
impl IdentityMode {
    pub fn wire(self) -> i64 {
        match self {
            Self::Ambient => 1,
            Self::ExplicitKey => 2,
            Self::CredentialsDisabled => 3,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::Ambient,
            2 => Self::ExplicitKey,
            3 => Self::CredentialsDisabled,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "IdentityMode",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum ErrorCode {
    #[default]
    UnsupportedVersion,
    UnsupportedOperation,
    Unavailable,
    InvalidRequest,
    Capacity,
    Timeout,
    Cancelled,
    Authentication,
    Trust,
    Io,
    Protocol,
    CarrierLost,
    RepositoryRefused,
}
impl ErrorCode {
    pub fn wire(self) -> i64 {
        match self {
            Self::UnsupportedVersion => 1,
            Self::UnsupportedOperation => 2,
            Self::Unavailable => 3,
            Self::InvalidRequest => 4,
            Self::Capacity => 5,
            Self::Timeout => 6,
            Self::Cancelled => 7,
            Self::Authentication => 8,
            Self::Trust => 9,
            Self::Io => 10,
            Self::Protocol => 11,
            Self::CarrierLost => 12,
            Self::RepositoryRefused => 13,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::UnsupportedVersion,
            2 => Self::UnsupportedOperation,
            3 => Self::Unavailable,
            4 => Self::InvalidRequest,
            5 => Self::Capacity,
            6 => Self::Timeout,
            7 => Self::Cancelled,
            8 => Self::Authentication,
            9 => Self::Trust,
            10 => Self::Io,
            11 => Self::Protocol,
            12 => Self::CarrierLost,
            13 => Self::RepositoryRefused,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "ErrorCode",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum SetupFailureCause {
    #[default]
    Stall,
    Aggregate,
    Interaction,
    Allocation,
    ConnectionRefused,
    NotFound,
    AddressNotAvailable,
}
impl SetupFailureCause {
    pub fn wire(self) -> i64 {
        match self {
            Self::Stall => 1,
            Self::Aggregate => 2,
            Self::Interaction => 3,
            Self::Allocation => 4,
            Self::ConnectionRefused => 5,
            Self::NotFound => 6,
            Self::AddressNotAvailable => 7,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::Stall,
            2 => Self::Aggregate,
            3 => Self::Interaction,
            4 => Self::Allocation,
            5 => Self::ConnectionRefused,
            6 => Self::NotFound,
            7 => Self::AddressNotAvailable,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "SetupFailureCause",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum HelperFailureCause {
    #[default]
    PipeFailure,
    OutputLimit,
    ControlCharacter,
    UsernameColon,
    NotUtf8,
    MissingNewline,
    MissingField,
    MalformedOutput,
}
impl HelperFailureCause {
    pub fn wire(self) -> i64 {
        match self {
            Self::PipeFailure => 1,
            Self::OutputLimit => 2,
            Self::ControlCharacter => 3,
            Self::UsernameColon => 4,
            Self::NotUtf8 => 5,
            Self::MissingNewline => 6,
            Self::MissingField => 7,
            Self::MalformedOutput => 8,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::PipeFailure,
            2 => Self::OutputLimit,
            3 => Self::ControlCharacter,
            4 => Self::UsernameColon,
            5 => Self::NotUtf8,
            6 => Self::MissingNewline,
            7 => Self::MissingField,
            8 => Self::MalformedOutput,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "HelperFailureCause",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum Effect {
    #[default]
    None,
    Possible,
}
impl Effect {
    pub fn wire(self) -> i64 {
        match self {
            Self::None => 1,
            Self::Possible => 2,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::None,
            2 => Self::Possible,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "Effect",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum Disposition {
    #[default]
    Reusable,
    Discarded,
}
impl Disposition {
    pub fn wire(self) -> i64 {
        match self {
            Self::Reusable => 1,
            Self::Discarded => 2,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::Reusable,
            2 => Self::Discarded,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "Disposition",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum AuthMethod {
    #[default]
    None,
    SshAgent,
    SshKey,
    Gh,
    Sspi,
}
impl AuthMethod {
    pub fn wire(self) -> i64 {
        match self {
            Self::None => 1,
            Self::SshAgent => 2,
            Self::SshKey => 3,
            Self::Gh => 4,
            Self::Sspi => 5,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::None,
            2 => Self::SshAgent,
            3 => Self::SshKey,
            4 => Self::Gh,
            5 => Self::Sspi,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "AuthMethod",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum NativeSource {
    #[default]
    Configured,
    CurrentLogon,
}
impl NativeSource {
    pub fn wire(self) -> i64 {
        match self {
            Self::Configured => 1,
            Self::CurrentLogon => 2,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::Configured,
            2 => Self::CurrentLogon,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "NativeSource",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum NativeScheme {
    #[default]
    Negotiate,
    Ntlm,
    Digest,
}
impl NativeScheme {
    pub fn wire(self) -> i64 {
        match self {
            Self::Negotiate => 1,
            Self::Ntlm => 2,
            Self::Digest => 3,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::Negotiate,
            2 => Self::Ntlm,
            3 => Self::Digest,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "NativeScheme",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum NativeObservation {
    #[default]
    NotStarted,
    Unresolved,
    Selected,
}
impl NativeObservation {
    pub fn wire(self) -> i64 {
        match self {
            Self::NotStarted => 1,
            Self::Unresolved => 2,
            Self::Selected => 3,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::NotStarted,
            2 => Self::Unresolved,
            3 => Self::Selected,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "NativeObservation",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Default)]
pub enum NativeMechanism {
    #[default]
    Kerberos,
    Ntlm,
}
impl NativeMechanism {
    pub fn wire(self) -> i64 {
        match self {
            Self::Kerberos => 1,
            Self::Ntlm => 2,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::Kerberos,
            2 => Self::Ntlm,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "NativeMechanism",
                    value: v,
                });
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct NativeFacts {
    pub source: NativeSource,
    pub scheme: NativeScheme,
    pub observation: NativeObservation,
    pub mechanism: Option<NativeMechanism>,
    pub authoritative: bool,
}
impl NativeFacts {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.source.wire())),
            (2, Cbor::Int(self.scheme.wire())),
            (3, Cbor::Int(self.observation.wire())),
            (
                4,
                match &self.mechanism {
                    Some(v) => Cbor::Int(v.wire()),
                    None => Cbor::Null,
                },
            ),
            (5, Cbor::Bool(self.authoritative)),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            source: NativeSource::from_wire(c.try_get(1)?.try_int()?)?,
            scheme: NativeScheme::from_wire(c.try_get(2)?.try_int()?)?,
            observation: NativeObservation::from_wire(c.try_get(3)?.try_int()?)?,
            mechanism: {
                let v = c.try_get(4)?;
                if v.is_null() {
                    None
                } else {
                    Some(NativeMechanism::from_wire(v.try_int()?)?)
                }
            },
            authoritative: c.try_get(5)?.try_bool()?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Limits {
    pub encoded_frame: i64,
    pub data_payload: i64,
    pub metadata_bytes: i64,
    pub nesting: i64,
    pub collection_entries: i64,
    pub decode_allocation: i64,
    pub queued_bytes: i64,
    pub queued_frames: i64,
    pub receive_window: i64,
    pub control_reserve_bytes: i64,
    pub control_reserve_frames: i64,
}
impl Limits {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.encoded_frame)),
            (2, Cbor::Int(self.data_payload)),
            (3, Cbor::Int(self.metadata_bytes)),
            (4, Cbor::Int(self.nesting)),
            (5, Cbor::Int(self.collection_entries)),
            (6, Cbor::Int(self.decode_allocation)),
            (7, Cbor::Int(self.queued_bytes)),
            (8, Cbor::Int(self.queued_frames)),
            (9, Cbor::Int(self.receive_window)),
            (10, Cbor::Int(self.control_reserve_bytes)),
            (11, Cbor::Int(self.control_reserve_frames)),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            encoded_frame: c.try_get(1)?.try_int()?,
            data_payload: c.try_get(2)?.try_int()?,
            metadata_bytes: c.try_get(3)?.try_int()?,
            nesting: c.try_get(4)?.try_int()?,
            collection_entries: c.try_get(5)?.try_int()?,
            decode_allocation: c.try_get(6)?.try_int()?,
            queued_bytes: c.try_get(7)?.try_int()?,
            queued_frames: c.try_get(8)?.try_int()?,
            receive_window: c.try_get(9)?.try_int()?,
            control_reserve_bytes: c.try_get(10)?.try_int()?,
            control_reserve_frames: c.try_get(11)?.try_int()?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Bind {
    pub versions: Vec<i64>,
    pub role: EndpointRole,
    pub schemes: Vec<Scheme>,
    pub policies: Vec<AuthPolicy>,
    pub receive_limits: Limits,
}
impl Bind {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (
                1,
                Cbor::Array(self.versions.iter().map(|x| Cbor::Int(*x)).collect()),
            ),
            (2, Cbor::Int(self.role.wire())),
            (
                3,
                Cbor::Array(self.schemes.iter().map(|x| Cbor::Int(x.wire())).collect()),
            ),
            (
                4,
                Cbor::Array(self.policies.iter().map(|x| Cbor::Int(x.wire())).collect()),
            ),
            (5, self.receive_limits.to_cbor()),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            versions: c
                .try_get(1)?
                .try_array()?
                .iter()
                .map(|x| Ok(x.try_int()?))
                .collect::<Result<Vec<_>, DecodeError>>()?,
            role: EndpointRole::from_wire(c.try_get(2)?.try_int()?)?,
            schemes: c
                .try_get(3)?
                .try_array()?
                .iter()
                .map(|x| Ok(Scheme::from_wire(x.try_int()?)?))
                .collect::<Result<Vec<_>, DecodeError>>()?,
            policies: c
                .try_get(4)?
                .try_array()?
                .iter()
                .map(|x| Ok(AuthPolicy::from_wire(x.try_int()?)?))
                .collect::<Result<Vec<_>, DecodeError>>()?,
            receive_limits: Limits::from_cbor(c.try_get(5)?)?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Bound {
    pub version: i64,
    pub endpoint_id: String,
    pub role: EndpointRole,
    pub schemes: Vec<Scheme>,
    pub policies: Vec<AuthPolicy>,
    pub receive_limits: Limits,
    pub trust_owner: String,
}
impl Bound {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.version)),
            (2, Cbor::Text(self.endpoint_id.clone())),
            (3, Cbor::Int(self.role.wire())),
            (
                4,
                Cbor::Array(self.schemes.iter().map(|x| Cbor::Int(x.wire())).collect()),
            ),
            (
                5,
                Cbor::Array(self.policies.iter().map(|x| Cbor::Int(x.wire())).collect()),
            ),
            (6, self.receive_limits.to_cbor()),
            (7, Cbor::Text(self.trust_owner.clone())),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            version: c.try_get(1)?.try_int()?,
            endpoint_id: c.try_get(2)?.try_text()?,
            role: EndpointRole::from_wire(c.try_get(3)?.try_int()?)?,
            schemes: c
                .try_get(4)?
                .try_array()?
                .iter()
                .map(|x| Ok(Scheme::from_wire(x.try_int()?)?))
                .collect::<Result<Vec<_>, DecodeError>>()?,
            policies: c
                .try_get(5)?
                .try_array()?
                .iter()
                .map(|x| Ok(AuthPolicy::from_wire(x.try_int()?)?))
                .collect::<Result<Vec<_>, DecodeError>>()?,
            receive_limits: Limits::from_cbor(c.try_get(6)?)?,
            trust_owner: c.try_get(7)?.try_text()?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct RetryAttempt {
    pub attempt: i64,
    pub attempts: i64,
}
impl RetryAttempt {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.attempt)),
            (2, Cbor::Int(self.attempts)),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            attempt: c.try_get(1)?.try_int()?,
            attempts: c.try_get(2)?.try_int()?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct FailureDetail {
    pub helper_cause: Option<HelperFailureCause>,
    pub pipe_kind: Option<String>,
    pub schemes: Option<Vec<String>>,
    pub retry_attempt: Option<RetryAttempt>,
    pub helper_budget_ms: Option<i64>,
}
impl FailureDetail {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (
                1,
                match &self.helper_cause {
                    Some(v) => Cbor::Int(v.wire()),
                    None => Cbor::Null,
                },
            ),
            (
                2,
                match &self.pipe_kind {
                    Some(v) => Cbor::Text(v.clone()),
                    None => Cbor::Null,
                },
            ),
            (
                3,
                match &self.schemes {
                    Some(v) => Cbor::Array(v.iter().map(|x| Cbor::Text(x.clone())).collect()),
                    None => Cbor::Null,
                },
            ),
            (
                4,
                match &self.retry_attempt {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                5,
                match &self.helper_budget_ms {
                    Some(v) => Cbor::Int(*v),
                    None => Cbor::Null,
                },
            ),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            helper_cause: {
                let v = c.try_get_opt(1)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(HelperFailureCause::from_wire(v.try_int()?)?)
                        }
                    }
                }
            },
            pipe_kind: {
                let v = c.try_get_opt(2)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(v.try_text()?)
                        }
                    }
                }
            },
            schemes: {
                let v = c.try_get_opt(3)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(
                                v.try_array()?
                                    .iter()
                                    .map(|x| Ok(x.try_text()?))
                                    .collect::<Result<Vec<_>, DecodeError>>()?,
                            )
                        }
                    }
                }
            },
            retry_attempt: {
                let v = c.try_get_opt(4)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(RetryAttempt::from_cbor(v)?)
                        }
                    }
                }
            },
            helper_budget_ms: {
                let v = c.try_get_opt(5)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(v.try_int()?)
                        }
                    }
                }
            },
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Failure {
    pub code: ErrorCode,
    pub effect: Effect,
    pub facts: Option<Facts>,
    pub setup_cause: Option<SetupFailureCause>,
    pub detail: Option<Box<FailureDetail>>,
}
impl Failure {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.code.wire())),
            (2, Cbor::Int(self.effect.wire())),
            (
                3,
                match &self.facts {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                4,
                match &self.setup_cause {
                    Some(v) => Cbor::Int(v.wire()),
                    None => Cbor::Null,
                },
            ),
            (
                5,
                match &self.detail {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            code: ErrorCode::from_wire(c.try_get(1)?.try_int()?)?,
            effect: Effect::from_wire(c.try_get(2)?.try_int()?)?,
            facts: {
                let v = c.try_get_opt(3)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(Facts::from_cbor(v)?)
                        }
                    }
                }
            },
            setup_cause: {
                let v = c.try_get_opt(4)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(SetupFailureCause::from_wire(v.try_int()?)?)
                        }
                    }
                }
            },
            detail: {
                let v = c.try_get_opt(5)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(Box::new(FailureDetail::from_cbor(v)?))
                        }
                    }
                }
            },
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, PartialEq, Default)]
pub struct Destination {
    pub scheme: Scheme,
    pub host: String,
    pub port: i64,
    pub path: String,
    pub ssh_username: Option<String>,
    pub https_username: Option<String>,
}
impl std::fmt::Debug for Destination {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Destination")
            .field("scheme", &self.scheme)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("path", &self.path)
            .field("ssh_username", &self.ssh_username)
            .field(
                "https_username",
                &self.https_username.as_ref().map(|_| "<redacted>"),
            )
            .finish()
    }
}

impl Destination {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.scheme.wire())),
            (2, Cbor::Text(self.host.clone())),
            (3, Cbor::Int(self.port)),
            (4, Cbor::Text(self.path.clone())),
            (
                5,
                match &self.ssh_username {
                    Some(v) => Cbor::Text(v.clone()),
                    None => Cbor::Null,
                },
            ),
            (
                6,
                match &self.https_username {
                    Some(v) => Cbor::Text(v.clone()),
                    None => Cbor::Null,
                },
            ),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            scheme: Scheme::from_wire(c.try_get(1)?.try_int()?)?,
            host: c.try_get(2)?.try_text()?,
            port: c.try_get(3)?.try_int()?,
            path: c.try_get(4)?.try_text()?,
            ssh_username: {
                let v = c.try_get(5)?;
                if v.is_null() {
                    None
                } else {
                    Some(v.try_text()?)
                }
            },
            https_username: {
                let v = c.try_get_opt(6)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(v.try_text()?)
                        }
                    }
                }
            },
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Identity {
    pub mode: IdentityMode,
    pub key_path: Option<String>,
    pub path_base: Option<String>,
}
impl Identity {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.mode.wire())),
            (
                2,
                match &self.key_path {
                    Some(v) => Cbor::Text(v.clone()),
                    None => Cbor::Null,
                },
            ),
            (
                3,
                match &self.path_base {
                    Some(v) => Cbor::Text(v.clone()),
                    None => Cbor::Null,
                },
            ),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            mode: IdentityMode::from_wire(c.try_get(1)?.try_int()?)?,
            key_path: {
                let v = c.try_get(2)?;
                if v.is_null() {
                    None
                } else {
                    Some(v.try_text()?)
                }
            },
            path_base: {
                let v = c.try_get(3)?;
                if v.is_null() {
                    None
                } else {
                    Some(v.try_text()?)
                }
            },
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Deadlines {
    pub allocation_ms: i64,
    pub connect_ms: i64,
    pub io_ms: i64,
    pub interaction_ms: i64,
    pub cleanup_ms: i64,
}
impl Deadlines {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.allocation_ms)),
            (2, Cbor::Int(self.connect_ms)),
            (3, Cbor::Int(self.io_ms)),
            (4, Cbor::Int(self.interaction_ms)),
            (5, Cbor::Int(self.cleanup_ms)),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            allocation_ms: c.try_get(1)?.try_int()?,
            connect_ms: c.try_get(2)?.try_int()?,
            io_ms: c.try_get(3)?.try_int()?,
            interaction_ms: c.try_get(4)?.try_int()?,
            cleanup_ms: c.try_get(5)?.try_int()?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Open {
    pub endpoint_id: String,
    pub operation_id: String,
    pub destination: Destination,
    pub service: GitService,
    pub identity: Identity,
    pub policy: AuthPolicy,
    pub deadlines: Deadlines,
    pub receive_limits: Limits,
}
impl Open {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Text(self.endpoint_id.clone())),
            (2, Cbor::Text(self.operation_id.clone())),
            (3, self.destination.to_cbor()),
            (4, Cbor::Int(self.service.wire())),
            (5, self.identity.to_cbor()),
            (6, Cbor::Int(self.policy.wire())),
            (7, self.deadlines.to_cbor()),
            (8, self.receive_limits.to_cbor()),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            endpoint_id: c.try_get(1)?.try_text()?,
            operation_id: c.try_get(2)?.try_text()?,
            destination: Destination::from_cbor(c.try_get(3)?)?,
            service: GitService::from_wire(c.try_get(4)?.try_int()?)?,
            identity: Identity::from_cbor(c.try_get(5)?)?,
            policy: AuthPolicy::from_wire(c.try_get(6)?.try_int()?)?,
            deadlines: Deadlines::from_cbor(c.try_get(7)?)?,
            receive_limits: Limits::from_cbor(c.try_get(8)?)?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct CheckIdentity {
    pub endpoint_id: String,
    pub operation_id: String,
    pub identity: Identity,
    pub timeout_ms: i64,
}
impl CheckIdentity {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Text(self.endpoint_id.clone())),
            (2, Cbor::Text(self.operation_id.clone())),
            (3, self.identity.to_cbor()),
            (4, Cbor::Int(self.timeout_ms)),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            endpoint_id: c.try_get(1)?.try_text()?,
            operation_id: c.try_get(2)?.try_text()?,
            identity: Identity::from_cbor(c.try_get(3)?)?,
            timeout_ms: c.try_get(4)?.try_int()?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct IdentityChecked {}
impl IdentityChecked {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        if !c.is_map() {
            return Err(DecodeError::WrongType { expected: "map" });
        }
        Ok(Self {})
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Facts {
    pub method: AuthMethod,
    pub credential_offered: bool,
    pub authenticated: Option<bool>,
    pub key_fingerprint: Option<String>,
    pub http_status: Option<i64>,
    pub ssh_exit_status: Option<i64>,
    pub native: Option<NativeFacts>,
}
impl Facts {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.method.wire())),
            (2, Cbor::Bool(self.credential_offered)),
            (
                3,
                match &self.authenticated {
                    Some(v) => Cbor::Bool(*v),
                    None => Cbor::Null,
                },
            ),
            (
                4,
                match &self.key_fingerprint {
                    Some(v) => Cbor::Text(v.clone()),
                    None => Cbor::Null,
                },
            ),
            (
                5,
                match &self.http_status {
                    Some(v) => Cbor::Int(*v),
                    None => Cbor::Null,
                },
            ),
            (
                6,
                match &self.ssh_exit_status {
                    Some(v) => Cbor::Int(*v),
                    None => Cbor::Null,
                },
            ),
            (
                7,
                match &self.native {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            method: AuthMethod::from_wire(c.try_get(1)?.try_int()?)?,
            credential_offered: c.try_get(2)?.try_bool()?,
            authenticated: {
                let v = c.try_get(3)?;
                if v.is_null() {
                    None
                } else {
                    Some(v.try_bool()?)
                }
            },
            key_fingerprint: {
                let v = c.try_get(4)?;
                if v.is_null() {
                    None
                } else {
                    Some(v.try_text()?)
                }
            },
            http_status: {
                let v = c.try_get(5)?;
                if v.is_null() {
                    None
                } else {
                    Some(v.try_int()?)
                }
            },
            ssh_exit_status: {
                let v = c.try_get(6)?;
                if v.is_null() {
                    None
                } else {
                    Some(v.try_int()?)
                }
            },
            native: {
                let v = c.try_get_opt(7)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(NativeFacts::from_cbor(v)?)
                        }
                    }
                }
            },
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Opened {
    pub connection_id: String,
    pub reused: bool,
    pub endpoint_id: String,
    pub trust_owner: String,
    pub facts: Facts,
    pub receive_limits: Limits,
}
impl Opened {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Text(self.connection_id.clone())),
            (2, Cbor::Bool(self.reused)),
            (3, Cbor::Text(self.endpoint_id.clone())),
            (4, Cbor::Text(self.trust_owner.clone())),
            (5, self.facts.to_cbor()),
            (6, self.receive_limits.to_cbor()),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            connection_id: c.try_get(1)?.try_text()?,
            reused: c.try_get(2)?.try_bool()?,
            endpoint_id: c.try_get(3)?.try_text()?,
            trust_owner: c.try_get(4)?.try_text()?,
            facts: Facts::from_cbor(c.try_get(5)?)?,
            receive_limits: Limits::from_cbor(c.try_get(6)?)?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Data {
    pub offset: i64,
    pub payload: Vec<u8>,
}
impl Data {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.offset)),
            (2, Cbor::Bytes(self.payload.clone())),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            offset: c.try_get(1)?.try_int()?,
            payload: c.try_get(2)?.try_bytes()?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Window {
    pub max_offset: i64,
}
impl Window {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![(1, Cbor::Int(self.max_offset))])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            max_offset: c.try_get(1)?.try_int()?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Barrier {
    pub barrier_id: i64,
    pub offset: i64,
}
impl Barrier {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.barrier_id)),
            (2, Cbor::Int(self.offset)),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            barrier_id: c.try_get(1)?.try_int()?,
            offset: c.try_get(2)?.try_int()?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct EndWrite {
    pub final_offset: i64,
}
impl EndWrite {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![(1, Cbor::Int(self.final_offset))])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            final_offset: c.try_get(1)?.try_int()?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Close {
    pub final_offset: i64,
}
impl Close {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![(1, Cbor::Int(self.final_offset))])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            final_offset: c.try_get(1)?.try_int()?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Closed {
    pub disposition: Disposition,
    pub unread_response_discarded: bool,
    pub facts: Facts,
    pub failure: Option<Failure>,
}
impl Closed {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.disposition.wire())),
            (2, Cbor::Bool(self.unread_response_discarded)),
            (3, self.facts.to_cbor()),
            (
                4,
                match &self.failure {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            disposition: Disposition::from_wire(c.try_get(1)?.try_int()?)?,
            unread_response_discarded: c.try_get(2)?.try_bool()?,
            facts: Facts::from_cbor(c.try_get(3)?)?,
            failure: {
                let v = c.try_get(4)?;
                if v.is_null() {
                    None
                } else {
                    Some(Failure::from_cbor(v)?)
                }
            },
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Cancel {
    pub reason: ErrorCode,
}
impl Cancel {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![(1, Cbor::Int(self.reason.wire()))])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            reason: ErrorCode::from_wire(c.try_get(1)?.try_int()?)?,
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Envelope {
    pub version: i64,
    pub session_id: String,
    pub stream_id: i64,
    pub kind: MessageKind,
    pub message_seq: Option<i64>,
    pub bind: Option<Bind>,
    pub bound: Option<Bound>,
    pub bind_rejected: Option<Failure>,
    pub open: Option<Open>,
    pub opened: Option<Opened>,
    pub open_failed: Option<Failure>,
    pub data: Option<Data>,
    pub window: Option<Window>,
    pub flush: Option<Barrier>,
    pub flushed: Option<Barrier>,
    pub end_write: Option<EndWrite>,
    pub close: Option<Close>,
    pub closed: Option<Closed>,
    pub cancel: Option<Cancel>,
    pub failed: Option<Failure>,
    pub check_identity: Option<CheckIdentity>,
    pub identity_checked: Option<IdentityChecked>,
    pub identity_check_failed: Option<Failure>,
}
impl Envelope {
    pub const MAX_DEPTH: usize = 32;
    pub const MAX_ENCODED_LEN: Option<usize> = None;
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.version)),
            (2, Cbor::Text(self.session_id.clone())),
            (3, Cbor::Int(self.stream_id)),
            (4, Cbor::Int(self.kind.wire())),
            (
                5,
                match &self.message_seq {
                    Some(v) => Cbor::Int(*v),
                    None => Cbor::Null,
                },
            ),
            (
                10,
                match &self.bind {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                11,
                match &self.bound {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                12,
                match &self.bind_rejected {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                13,
                match &self.open {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                14,
                match &self.opened {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                15,
                match &self.open_failed {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                16,
                match &self.data {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                17,
                match &self.window {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                18,
                match &self.flush {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                19,
                match &self.flushed {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                20,
                match &self.end_write {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                21,
                match &self.close {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                22,
                match &self.closed {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                23,
                match &self.cancel {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                24,
                match &self.failed {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                25,
                match &self.check_identity {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                26,
                match &self.identity_checked {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
            (
                27,
                match &self.identity_check_failed {
                    Some(v) => v.to_cbor(),
                    None => Cbor::Null,
                },
            ),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            version: c.try_get(1)?.try_int()?,
            session_id: c.try_get(2)?.try_text()?,
            stream_id: c.try_get(3)?.try_int()?,
            kind: MessageKind::from_wire(c.try_get(4)?.try_int()?)?,
            message_seq: {
                let v = c.try_get_opt(5)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(v.try_int()?)
                        }
                    }
                }
            },
            bind: {
                let v = c.try_get(10)?;
                if v.is_null() {
                    None
                } else {
                    Some(Bind::from_cbor(v)?)
                }
            },
            bound: {
                let v = c.try_get(11)?;
                if v.is_null() {
                    None
                } else {
                    Some(Bound::from_cbor(v)?)
                }
            },
            bind_rejected: {
                let v = c.try_get(12)?;
                if v.is_null() {
                    None
                } else {
                    Some(Failure::from_cbor(v)?)
                }
            },
            open: {
                let v = c.try_get(13)?;
                if v.is_null() {
                    None
                } else {
                    Some(Open::from_cbor(v)?)
                }
            },
            opened: {
                let v = c.try_get(14)?;
                if v.is_null() {
                    None
                } else {
                    Some(Opened::from_cbor(v)?)
                }
            },
            open_failed: {
                let v = c.try_get(15)?;
                if v.is_null() {
                    None
                } else {
                    Some(Failure::from_cbor(v)?)
                }
            },
            data: {
                let v = c.try_get(16)?;
                if v.is_null() {
                    None
                } else {
                    Some(Data::from_cbor(v)?)
                }
            },
            window: {
                let v = c.try_get(17)?;
                if v.is_null() {
                    None
                } else {
                    Some(Window::from_cbor(v)?)
                }
            },
            flush: {
                let v = c.try_get(18)?;
                if v.is_null() {
                    None
                } else {
                    Some(Barrier::from_cbor(v)?)
                }
            },
            flushed: {
                let v = c.try_get(19)?;
                if v.is_null() {
                    None
                } else {
                    Some(Barrier::from_cbor(v)?)
                }
            },
            end_write: {
                let v = c.try_get(20)?;
                if v.is_null() {
                    None
                } else {
                    Some(EndWrite::from_cbor(v)?)
                }
            },
            close: {
                let v = c.try_get(21)?;
                if v.is_null() {
                    None
                } else {
                    Some(Close::from_cbor(v)?)
                }
            },
            closed: {
                let v = c.try_get(22)?;
                if v.is_null() {
                    None
                } else {
                    Some(Closed::from_cbor(v)?)
                }
            },
            cancel: {
                let v = c.try_get(23)?;
                if v.is_null() {
                    None
                } else {
                    Some(Cancel::from_cbor(v)?)
                }
            },
            failed: {
                let v = c.try_get(24)?;
                if v.is_null() {
                    None
                } else {
                    Some(Failure::from_cbor(v)?)
                }
            },
            check_identity: {
                let v = c.try_get_opt(25)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(CheckIdentity::from_cbor(v)?)
                        }
                    }
                }
            },
            identity_checked: {
                let v = c.try_get_opt(26)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(IdentityChecked::from_cbor(v)?)
                        }
                    }
                }
            },
            identity_check_failed: {
                let v = c.try_get_opt(27)?;
                match v {
                    None => None,
                    Some(v) => {
                        if v.is_null() {
                            None
                        } else {
                            Some(Failure::from_cbor(v)?)
                        }
                    }
                }
            },
        })
    }
    pub fn decode(bytes: &[u8]) -> Result<Self, DecodeError> {
        Self::from_cbor(&crate::cbor::try_decode_with(
            bytes,
            Self::MAX_DEPTH,
            Self::MAX_ENCODED_LEN,
        )?)
    }
}
