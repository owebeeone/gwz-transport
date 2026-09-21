use super::*;
use crate::protocol::*;

pub(crate) fn limits(value: &Limits) -> Result<(), Error> {
    let fields = [
        value.encoded_frame,
        value.data_payload,
        value.metadata_bytes,
        value.nesting,
        value.collection_entries,
        value.decode_allocation,
        value.queued_bytes,
        value.queued_frames,
        value.receive_window,
        value.control_reserve_bytes,
        value.control_reserve_frames,
    ];
    if fields.iter().any(|v| *v <= 0)
        || value.encoded_frame > MAX_FRAME as i64
        || value.data_payload > MAX_DATA as i64
        || value.metadata_bytes > MAX_METADATA as i64
        || value.nesting > MAX_DEPTH as i64
        || value.collection_entries > MAX_ENTRIES as i64
        || value.decode_allocation > MAX_ALLOCATION as i64
        || value.queued_bytes > 4 * 1024 * 1024
        || value.queued_frames > 64
        || value.receive_window > value.queued_bytes
        || value.control_reserve_bytes >= value.queued_bytes
        || value.control_reserve_frames >= value.queued_frames
    {
        return Err(Error::Bounds);
    }
    Ok(())
}

pub(super) fn envelope(value: &Envelope) -> Result<(), Error> {
    if !(1..=2).contains(&value.version) {
        return Err(Error::UnsupportedVersion);
    }
    if value.session_id.is_empty() || value.session_id.len() > 128 {
        return Err(Error::InvalidMessage);
    }
    let bodies = [
        value.bind.is_some(),
        value.bound.is_some(),
        value.bind_rejected.is_some(),
        value.open.is_some(),
        value.opened.is_some(),
        value.open_failed.is_some(),
        value.data.is_some(),
        value.window.is_some(),
        value.flush.is_some(),
        value.flushed.is_some(),
        value.end_write.is_some(),
        value.close.is_some(),
        value.closed.is_some(),
        value.cancel.is_some(),
        value.failed.is_some(),
        value.check_identity.is_some(),
        value.identity_checked.is_some(),
        value.identity_check_failed.is_some(),
    ];
    let kind = value.kind.wire() as usize;
    if bodies.iter().filter(|present| **present).count() != 1 || !bodies[kind - 1] {
        return Err(Error::InvalidMessage);
    }
    if (kind <= 3 && (value.stream_id != 0 || value.version != 1))
        || (kind > 3 && value.stream_id <= 0)
    {
        return Err(Error::InvalidMessage);
    }
    if kind >= 16 && value.version != 2 {
        return Err(Error::UnsupportedVersion);
    }
    if let Some(bind) = &value.bind {
        if bind.versions.is_empty() || !crate::policy::capabilities(&bind.schemes, &bind.policies) {
            return Err(Error::InvalidMessage);
        }
        limits(&bind.receive_limits)?;
    }
    if let Some(bound) = &value.bound {
        if !(1..=2).contains(&bound.version)
            || bound.endpoint_id.is_empty()
            || bound.trust_owner.is_empty()
            || !crate::policy::capabilities(&bound.schemes, &bound.policies)
        {
            return Err(Error::InvalidMessage);
        }
        limits(&bound.receive_limits)?;
    }
    if let Some(failure) = &value.bind_rejected {
        if failure.facts.is_some() || failure.code == ErrorCode::RepositoryRefused {
            return Err(Error::InvalidMessage);
        }
    }
    if value.version == 1 {
        if value
            .open_failed
            .as_ref()
            .is_some_and(failure_has_v2_fields)
            || value.failed.as_ref().is_some_and(failure_has_v2_fields)
        {
            return Err(Error::InvalidMessage);
        }
    }
    if let Some(failure) = &value.identity_check_failed {
        if failure.effect != Effect::None || failure.facts.is_some() {
            return Err(Error::InvalidMessage);
        }
    }
    if let Some(closed) = &value.closed {
        if closed.failure.as_ref().is_some_and(|failure| {
            failure.facts.is_some()
                || (value.version == 1 && failure.code == ErrorCode::RepositoryRefused)
        }) {
            return Err(Error::InvalidMessage);
        }
    }
    if let Some(open) = &value.open {
        limits(&open.receive_limits)?;
        destination(&open.destination)?;
        if open.endpoint_id.is_empty() || open.operation_id.is_empty() {
            return Err(Error::InvalidMessage);
        }
        if !valid_identity(&open.identity) {
            return Err(Error::InvalidMessage);
        }
        if !crate::policy::allows(open.destination.scheme, open.policy, open.identity.mode) {
            return Err(Error::InvalidMessage);
        }
        let d = &open.deadlines;
        if [d.allocation_ms, d.interaction_ms, d.cleanup_ms]
            .iter()
            .any(|v| *v <= 0)
            || [d.connect_ms, d.io_ms]
                .iter()
                .any(|v| !(0..=i32::MAX as i64).contains(v))
        {
            return Err(Error::InvalidMessage);
        }
    }
    if let Some(check) = &value.check_identity {
        if check.endpoint_id.is_empty()
            || check.operation_id.is_empty()
            || check.timeout_ms <= 0
            || check.timeout_ms > i32::MAX as i64
            || check.identity.mode != IdentityMode::ExplicitKey
            || !valid_identity(&check.identity)
        {
            return Err(Error::InvalidMessage);
        }
    }
    if let Some(opened) = &value.opened {
        limits(&opened.receive_limits)?;
        if opened.connection_id.is_empty()
            || opened.endpoint_id.is_empty()
            || opened.trust_owner.is_empty()
            || (opened.reused && opened.facts.credential_offered)
        {
            return Err(Error::InvalidMessage);
        }
    }
    if value
        .data
        .as_ref()
        .is_some_and(|v| v.offset < 0 || v.offset.checked_add(v.payload.len() as i64).is_none())
        || value.window.as_ref().is_some_and(|v| v.max_offset < 0)
        || value
            .flush
            .as_ref()
            .is_some_and(|v| v.offset < 0 || v.barrier_id <= 0)
        || value
            .flushed
            .as_ref()
            .is_some_and(|v| v.offset < 0 || v.barrier_id <= 0)
        || value.end_write.as_ref().is_some_and(|v| v.final_offset < 0)
        || value.close.as_ref().is_some_and(|v| v.final_offset < 0)
    {
        return Err(Error::InvalidMessage);
    }
    if value
        .cancel
        .as_ref()
        .is_some_and(|cancel| !matches!(cancel.reason, ErrorCode::Cancelled | ErrorCode::Timeout))
    {
        return Err(Error::InvalidMessage);
    }
    Ok(())
}

fn failure_has_v2_fields(failure: &Failure) -> bool {
    failure.facts.is_some() || failure.code == ErrorCode::RepositoryRefused
}

fn valid_identity(value: &Identity) -> bool {
    if value.mode != IdentityMode::ExplicitKey {
        return value.key_path.is_none() && value.path_base.is_none();
    }
    value
        .key_path
        .as_ref()
        .is_some_and(|path| !path.is_empty() && !path.contains('\0'))
        && value
            .path_base
            .as_ref()
            .is_none_or(|base| !base.is_empty() && !base.contains('\0'))
}

fn destination(value: &Destination) -> Result<(), Error> {
    if value.host.is_empty()
        || value
            .host
            .chars()
            .any(|c| c.is_control() || c.is_whitespace() || "@/?#\\".contains(c))
        || !(1..=65535).contains(&value.port)
        || value.path.is_empty()
        || value.path.contains('\0')
    {
        return Err(Error::InvalidMessage);
    }
    if value.scheme == Scheme::Https
        && (value.ssh_username.is_some() || value.path.contains(['?', '#']))
    {
        return Err(Error::InvalidMessage);
    }
    if value.scheme == Scheme::Ssh
        && value
            .ssh_username
            .as_ref()
            .is_none_or(|s| s.is_empty() || s.chars().any(char::is_control))
    {
        return Err(Error::InvalidMessage);
    }
    Ok(())
}
