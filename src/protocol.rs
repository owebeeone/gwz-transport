// GENERATED native Rust types + codec — do not edit.
#![allow(dead_code)]
use crate::cbor::{Cbor, DecodeError};

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
}
impl AuthPolicy {
    pub fn wire(self) -> i64 {
        match self {
            Self::SshAmbient => 1,
            Self::SshExplicit => 2,
            Self::Anonymous => 3,
            Self::Gh => 4,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::SshAmbient,
            2 => Self::SshExplicit,
            3 => Self::Anonymous,
            4 => Self::Gh,
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
}
impl AuthMethod {
    pub fn wire(self) -> i64 {
        match self {
            Self::None => 1,
            Self::SshAgent => 2,
            Self::SshKey => 3,
            Self::Gh => 4,
        }
    }
    pub fn from_wire(v: i64) -> Result<Self, DecodeError> {
        Ok(match v {
            1 => Self::None,
            2 => Self::SshAgent,
            3 => Self::SshKey,
            4 => Self::Gh,
            _ => {
                return Err(DecodeError::UnknownEnum {
                    enum_name: "AuthMethod",
                    value: v,
                });
            }
        })
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
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Failure {
    pub code: ErrorCode,
    pub effect: Effect,
}
impl Failure {
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.code.wire())),
            (2, Cbor::Int(self.effect.wire())),
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            code: ErrorCode::from_wire(c.try_get(1)?.try_int()?)?,
            effect: Effect::from_wire(c.try_get(2)?.try_int()?)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Destination {
    pub scheme: Scheme,
    pub host: String,
    pub port: i64,
    pub path: String,
    pub ssh_username: Option<String>,
}
impl Destination {
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
        })
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Identity {
    pub mode: IdentityMode,
    pub key_path: Option<String>,
    pub path_base: Option<String>,
}
impl Identity {
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
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Facts {
    pub method: AuthMethod,
    pub credential_offered: bool,
    pub authenticated: Option<bool>,
    pub key_fingerprint: Option<String>,
    pub http_status: Option<i64>,
    pub ssh_exit_status: Option<i64>,
}
impl Facts {
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
        })
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
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Data {
    pub offset: i64,
    pub payload: Vec<u8>,
}
impl Data {
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
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Window {
    pub max_offset: i64,
}
impl Window {
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![(1, Cbor::Int(self.max_offset))])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            max_offset: c.try_get(1)?.try_int()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Barrier {
    pub barrier_id: i64,
    pub offset: i64,
}
impl Barrier {
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
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct EndWrite {
    pub final_offset: i64,
}
impl EndWrite {
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![(1, Cbor::Int(self.final_offset))])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            final_offset: c.try_get(1)?.try_int()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Close {
    pub final_offset: i64,
}
impl Close {
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![(1, Cbor::Int(self.final_offset))])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            final_offset: c.try_get(1)?.try_int()?,
        })
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
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Cancel {
    pub reason: ErrorCode,
}
impl Cancel {
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![(1, Cbor::Int(self.reason.wire()))])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            reason: ErrorCode::from_wire(c.try_get(1)?.try_int()?)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Default)]
pub struct Envelope {
    pub version: i64,
    pub session_id: String,
    pub stream_id: i64,
    pub kind: MessageKind,
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
}
impl Envelope {
    pub fn to_cbor(&self) -> Cbor {
        Cbor::Map(vec![
            (1, Cbor::Int(self.version)),
            (2, Cbor::Text(self.session_id.clone())),
            (3, Cbor::Int(self.stream_id)),
            (4, Cbor::Int(self.kind.wire())),
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
        ])
    }
    pub fn from_cbor(c: &Cbor) -> Result<Self, DecodeError> {
        Ok(Self {
            version: c.try_get(1)?.try_int()?,
            session_id: c.try_get(2)?.try_text()?,
            stream_id: c.try_get(3)?.try_int()?,
            kind: MessageKind::from_wire(c.try_get(4)?.try_int()?)?,
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
        })
    }
}
