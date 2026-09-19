//! Effect-free endpoint capability binding. No credentials, sockets or pool access.
use crate::{codec, protocol::*};

pub fn default_limits() -> Limits {
    Limits {
        encoded_frame: 131072,
        data_payload: 65536,
        metadata_bytes: 16384,
        nesting: 16,
        collection_entries: 256,
        decode_allocation: 524288,
        queued_bytes: 4194304,
        queued_frames: 64,
        receive_window: 1048576,
        control_reserve_bytes: 65536,
        control_reserve_frames: 8,
    }
}

pub fn offer(session_id: &str, role: EndpointRole) -> Envelope {
    Envelope {
        version: 1,
        session_id: session_id.into(),
        stream_id: 0,
        kind: MessageKind::Bind,
        bind: Some(Bind {
            versions: vec![1],
            role,
            schemes: vec![Scheme::Ssh, Scheme::Https],
            policies: vec![
                AuthPolicy::SshAmbient,
                AuthPolicy::SshExplicit,
                AuthPolicy::Anonymous,
                AuthPolicy::Gh,
            ],
            receive_limits: default_limits(),
        }),
        ..Default::default()
    }
}

#[derive(Debug, Clone)]
pub struct EndpointConfig {
    pub endpoint_id: String,
    pub role: EndpointRole,
    pub schemes: Vec<Scheme>,
    pub policies: Vec<AuthPolicy>,
    pub limits: Limits,
    pub trust_owner: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Binding {
    session_id: String,
    bound: Bound,
}
impl Binding {
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
    pub fn endpoint_id(&self) -> &str {
        &self.bound.endpoint_id
    }
    pub fn limits(&self) -> &Limits {
        &self.bound.receive_limits
    }

    /// Must precede lease allocation or any endpoint authority/network effect.
    pub fn check_open(&self, message: &Envelope) -> Result<(), Failure> {
        codec::admit_limited(message, &self.bound.receive_limits)
            .map_err(|_| failure(ErrorCode::InvalidRequest))?;
        let open = message
            .open
            .as_ref()
            .ok_or_else(|| failure(ErrorCode::InvalidRequest))?;
        if message.session_id != self.session_id || open.endpoint_id != self.bound.endpoint_id {
            return Err(failure(ErrorCode::Unavailable));
        }
        if !self.bound.schemes.contains(&open.destination.scheme)
            || !self.bound.policies.contains(&open.policy)
            || !no_greater(&open.receive_limits, &self.bound.receive_limits)
        {
            return Err(failure(ErrorCode::UnsupportedOperation));
        }
        usable(&open.receive_limits)?;
        Ok(())
    }
}

fn failure(code: ErrorCode) -> Failure {
    Failure {
        code,
        effect: Effect::None,
    }
}

impl EndpointConfig {
    pub fn accept(&self, message: &Envelope) -> Result<(Envelope, Binding), Failure> {
        if !crate::policy::capabilities(&self.schemes, &self.policies) {
            return Err(failure(ErrorCode::UnsupportedOperation));
        }
        codec::admit(message).map_err(|_| failure(ErrorCode::InvalidRequest))?;
        let bind = message
            .bind
            .as_ref()
            .ok_or_else(|| failure(ErrorCode::InvalidRequest))?;
        if !bind.versions.contains(&1) {
            return Err(failure(ErrorCode::UnsupportedVersion));
        }
        if bind.role != self.role {
            return Err(failure(ErrorCode::UnsupportedOperation));
        }
        codec::validate_limits(&self.limits).map_err(|_| failure(ErrorCode::InvalidRequest))?;
        let mut schemes: Vec<_> = self
            .schemes
            .iter()
            .copied()
            .filter(|s| bind.schemes.contains(s))
            .collect();
        let mut policies: Vec<_> = self
            .policies
            .iter()
            .copied()
            .filter(|p| bind.policies.contains(p))
            .collect();
        schemes.retain(|s| policies.iter().any(|p| crate::policy::supports(*s, *p)));
        policies.retain(|p| schemes.iter().any(|s| crate::policy::supports(*s, *p)));
        if schemes.is_empty() || policies.is_empty() {
            return Err(failure(ErrorCode::UnsupportedOperation));
        }
        let receive_limits = minimum(&bind.receive_limits, &self.limits);
        usable(&receive_limits)?;
        let bound = Bound {
            version: 1,
            endpoint_id: self.endpoint_id.clone(),
            role: self.role,
            schemes,
            policies,
            receive_limits,
            trust_owner: self.trust_owner.clone(),
        };
        let reply = Envelope {
            version: 1,
            session_id: message.session_id.clone(),
            stream_id: 0,
            kind: MessageKind::Bound,
            bound: Some(bound.clone()),
            ..Default::default()
        };
        codec::admit(&reply).map_err(|_| failure(ErrorCode::InvalidRequest))?;
        Ok((
            reply,
            Binding {
                session_id: message.session_id.clone(),
                bound,
            },
        ))
    }
}

/// Only a verified acknowledgement may be installed in the mux.
pub fn verify(offer: &Envelope, reply: &Envelope) -> Result<Binding, Failure> {
    codec::admit(offer).map_err(|_| failure(ErrorCode::InvalidRequest))?;
    codec::admit(reply).map_err(|_| failure(ErrorCode::InvalidRequest))?;
    let requested = offer
        .bind
        .as_ref()
        .ok_or_else(|| failure(ErrorCode::InvalidRequest))?;
    let bound = reply
        .bound
        .as_ref()
        .ok_or_else(|| failure(ErrorCode::Unavailable))?;
    if reply.session_id != offer.session_id
        || bound.role != requested.role
        || !requested.versions.contains(&bound.version)
        || !bound.schemes.iter().all(|v| requested.schemes.contains(v))
        || !bound
            .policies
            .iter()
            .all(|v| requested.policies.contains(v))
        || !no_greater(&bound.receive_limits, &requested.receive_limits)
    {
        return Err(failure(ErrorCode::InvalidRequest));
    }
    usable(&bound.receive_limits)?;
    Ok(Binding {
        session_id: reply.session_id.clone(),
        bound: bound.clone(),
    })
}

pub(crate) fn usable(limits: &Limits) -> Result<(), Failure> {
    // Enough space for one worst-case frame plus its decode reservation and
    // independent control progress. Smaller offers are refused, never enlarged.
    if limits.encoded_frame < 4096
        || limits.metadata_bytes < 256
        || limits.nesting < 10
        || limits.collection_entries < 128
        || limits.decode_allocation < 65536
        || limits.control_reserve_bytes < 4096
        || limits.control_reserve_frames < 2
        || limits.queued_bytes - limits.control_reserve_bytes
            < limits.encoded_frame + limits.decode_allocation
        || limits.queued_frames - limits.control_reserve_frames < 2
        || limits.receive_window > limits.queued_bytes - limits.control_reserve_bytes
    {
        return Err(failure(ErrorCode::UnsupportedOperation));
    }
    Ok(())
}

pub(crate) fn no_greater(a: &Limits, b: &Limits) -> bool {
    let pairs = [
        (a.encoded_frame, b.encoded_frame),
        (a.data_payload, b.data_payload),
        (a.metadata_bytes, b.metadata_bytes),
        (a.nesting, b.nesting),
        (a.collection_entries, b.collection_entries),
        (a.decode_allocation, b.decode_allocation),
        (a.queued_bytes, b.queued_bytes),
        (a.queued_frames, b.queued_frames),
        (a.receive_window, b.receive_window),
        (a.control_reserve_bytes, b.control_reserve_bytes),
        (a.control_reserve_frames, b.control_reserve_frames),
    ];
    pairs.iter().all(|(left, right)| left <= right)
}

fn minimum(a: &Limits, b: &Limits) -> Limits {
    Limits {
        encoded_frame: a.encoded_frame.min(b.encoded_frame),
        data_payload: a.data_payload.min(b.data_payload),
        metadata_bytes: a.metadata_bytes.min(b.metadata_bytes),
        nesting: a.nesting.min(b.nesting),
        collection_entries: a.collection_entries.min(b.collection_entries),
        decode_allocation: a.decode_allocation.min(b.decode_allocation),
        queued_bytes: a.queued_bytes.min(b.queued_bytes),
        queued_frames: a.queued_frames.min(b.queued_frames),
        receive_window: a.receive_window.min(b.receive_window),
        control_reserve_bytes: a.control_reserve_bytes.min(b.control_reserve_bytes),
        control_reserve_frames: a.control_reserve_frames.min(b.control_reserve_frames),
    }
}
