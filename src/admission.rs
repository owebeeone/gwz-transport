// GENERATED from transport.taut.py; do not edit.
use crate::budget::Budget;
use crate::{
    codec::{Error, MAX_DATA, MAX_METADATA},
    protocol::*,
};

pub(crate) fn allocation_charge(value: &Envelope, limits: &Limits) -> Result<usize, Error> {
    let mut budget = Budget::new(value.stream_id == 0, limits);
    visit_envelope(value, &mut budget, 0)?;
    budget.total_charge()
}

pub(crate) fn envelope(value: &Envelope, limits: &Limits) -> Result<(), Error> {
    allocation_charge(value, limits).map(|_| ())
}

fn visit_limits(value: &Limits, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(11, depth)?;
    b.key(1)?;
    b.integer(value.encoded_frame, depth + 1)?;
    b.key(2)?;
    b.integer(value.data_payload, depth + 1)?;
    b.key(3)?;
    b.integer(value.metadata_bytes, depth + 1)?;
    b.key(4)?;
    b.integer(value.nesting, depth + 1)?;
    b.key(5)?;
    b.integer(value.collection_entries, depth + 1)?;
    b.key(6)?;
    b.integer(value.decode_allocation, depth + 1)?;
    b.key(7)?;
    b.integer(value.queued_bytes, depth + 1)?;
    b.key(8)?;
    b.integer(value.queued_frames, depth + 1)?;
    b.key(9)?;
    b.integer(value.receive_window, depth + 1)?;
    b.key(10)?;
    b.integer(value.control_reserve_bytes, depth + 1)?;
    b.key(11)?;
    b.integer(value.control_reserve_frames, depth + 1)?;
    Ok(())
}

fn visit_bind(value: &Bind, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(5, depth)?;
    b.key(1)?;
    b.container(value.versions.len(), depth + 1)?;
    for item in &value.versions {
        b.integer(*item, depth + 1 + 1)?;
    }
    b.key(2)?;
    b.integer(value.role.wire(), depth + 1)?;
    b.key(3)?;
    b.container(value.schemes.len(), depth + 1)?;
    for item in &value.schemes {
        b.integer(item.wire(), depth + 1 + 1)?;
    }
    b.key(4)?;
    b.container(value.policies.len(), depth + 1)?;
    for item in &value.policies {
        b.integer(item.wire(), depth + 1 + 1)?;
    }
    b.key(5)?;
    visit_limits(&value.receive_limits, b, depth + 1)?;
    Ok(())
}

fn visit_bound(value: &Bound, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(7, depth)?;
    b.key(1)?;
    b.integer(value.version, depth + 1)?;
    b.key(2)?;
    b.bytes(value.endpoint_id.len(), MAX_METADATA, depth + 1)?;
    b.key(3)?;
    b.integer(value.role.wire(), depth + 1)?;
    b.key(4)?;
    b.container(value.schemes.len(), depth + 1)?;
    for item in &value.schemes {
        b.integer(item.wire(), depth + 1 + 1)?;
    }
    b.key(5)?;
    b.container(value.policies.len(), depth + 1)?;
    for item in &value.policies {
        b.integer(item.wire(), depth + 1 + 1)?;
    }
    b.key(6)?;
    visit_limits(&value.receive_limits, b, depth + 1)?;
    b.key(7)?;
    b.bytes(value.trust_owner.len(), MAX_METADATA, depth + 1)?;
    Ok(())
}

fn visit_failure(value: &Failure, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(3, depth)?;
    b.key(1)?;
    b.integer(value.code.wire(), depth + 1)?;
    b.key(2)?;
    b.integer(value.effect.wire(), depth + 1)?;
    b.key(3)?;
    if let Some(item) = &value.facts {
        visit_facts(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    Ok(())
}

fn visit_destination(value: &Destination, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(5, depth)?;
    b.key(1)?;
    b.integer(value.scheme.wire(), depth + 1)?;
    b.key(2)?;
    b.bytes(value.host.len(), MAX_METADATA, depth + 1)?;
    b.key(3)?;
    b.integer(value.port, depth + 1)?;
    b.key(4)?;
    b.bytes(value.path.len(), MAX_METADATA, depth + 1)?;
    b.key(5)?;
    if let Some(item) = &value.ssh_username {
        b.bytes(item.len(), MAX_METADATA, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    Ok(())
}

fn visit_identity(value: &Identity, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(3, depth)?;
    b.key(1)?;
    b.integer(value.mode.wire(), depth + 1)?;
    b.key(2)?;
    if let Some(item) = &value.key_path {
        b.bytes(item.len(), MAX_METADATA, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(3)?;
    if let Some(item) = &value.path_base {
        b.bytes(item.len(), MAX_METADATA, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    Ok(())
}

fn visit_deadlines(value: &Deadlines, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(5, depth)?;
    b.key(1)?;
    b.integer(value.allocation_ms, depth + 1)?;
    b.key(2)?;
    b.integer(value.connect_ms, depth + 1)?;
    b.key(3)?;
    b.integer(value.io_ms, depth + 1)?;
    b.key(4)?;
    b.integer(value.interaction_ms, depth + 1)?;
    b.key(5)?;
    b.integer(value.cleanup_ms, depth + 1)?;
    Ok(())
}

fn visit_open(value: &Open, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(8, depth)?;
    b.key(1)?;
    b.bytes(value.endpoint_id.len(), MAX_METADATA, depth + 1)?;
    b.key(2)?;
    b.bytes(value.operation_id.len(), MAX_METADATA, depth + 1)?;
    b.key(3)?;
    visit_destination(&value.destination, b, depth + 1)?;
    b.key(4)?;
    b.integer(value.service.wire(), depth + 1)?;
    b.key(5)?;
    visit_identity(&value.identity, b, depth + 1)?;
    b.key(6)?;
    b.integer(value.policy.wire(), depth + 1)?;
    b.key(7)?;
    visit_deadlines(&value.deadlines, b, depth + 1)?;
    b.key(8)?;
    visit_limits(&value.receive_limits, b, depth + 1)?;
    Ok(())
}

fn visit_checkidentity(value: &CheckIdentity, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(4, depth)?;
    b.key(1)?;
    b.bytes(value.endpoint_id.len(), MAX_METADATA, depth + 1)?;
    b.key(2)?;
    b.bytes(value.operation_id.len(), MAX_METADATA, depth + 1)?;
    b.key(3)?;
    visit_identity(&value.identity, b, depth + 1)?;
    b.key(4)?;
    b.integer(value.timeout_ms, depth + 1)?;
    Ok(())
}

fn visit_identitychecked(
    value: &IdentityChecked,
    b: &mut Budget,
    depth: usize,
) -> Result<(), Error> {
    b.container(0, depth)?;
    let _ = value;
    Ok(())
}

fn visit_facts(value: &Facts, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(6, depth)?;
    b.key(1)?;
    b.integer(value.method.wire(), depth + 1)?;
    b.key(2)?;
    let _ = &value.credential_offered;
    b.scalar(depth + 1)?;
    b.key(3)?;
    if let Some(item) = &value.authenticated {
        let _ = item;
        b.scalar(depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(4)?;
    if let Some(item) = &value.key_fingerprint {
        b.bytes(item.len(), MAX_METADATA, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(5)?;
    if let Some(item) = &value.http_status {
        b.integer(*item, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(6)?;
    if let Some(item) = &value.ssh_exit_status {
        b.integer(*item, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    Ok(())
}

fn visit_opened(value: &Opened, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(6, depth)?;
    b.key(1)?;
    b.bytes(value.connection_id.len(), MAX_METADATA, depth + 1)?;
    b.key(2)?;
    let _ = &value.reused;
    b.scalar(depth + 1)?;
    b.key(3)?;
    b.bytes(value.endpoint_id.len(), MAX_METADATA, depth + 1)?;
    b.key(4)?;
    b.bytes(value.trust_owner.len(), MAX_METADATA, depth + 1)?;
    b.key(5)?;
    visit_facts(&value.facts, b, depth + 1)?;
    b.key(6)?;
    visit_limits(&value.receive_limits, b, depth + 1)?;
    Ok(())
}

fn visit_data(value: &Data, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(2, depth)?;
    b.key(1)?;
    b.integer(value.offset, depth + 1)?;
    b.key(2)?;
    b.bytes(value.payload.len(), MAX_DATA, depth + 1)?;
    Ok(())
}

fn visit_window(value: &Window, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(1, depth)?;
    b.key(1)?;
    b.integer(value.max_offset, depth + 1)?;
    Ok(())
}

fn visit_barrier(value: &Barrier, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(2, depth)?;
    b.key(1)?;
    b.integer(value.barrier_id, depth + 1)?;
    b.key(2)?;
    b.integer(value.offset, depth + 1)?;
    Ok(())
}

fn visit_endwrite(value: &EndWrite, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(1, depth)?;
    b.key(1)?;
    b.integer(value.final_offset, depth + 1)?;
    Ok(())
}

fn visit_close(value: &Close, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(1, depth)?;
    b.key(1)?;
    b.integer(value.final_offset, depth + 1)?;
    Ok(())
}

fn visit_closed(value: &Closed, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(4, depth)?;
    b.key(1)?;
    b.integer(value.disposition.wire(), depth + 1)?;
    b.key(2)?;
    let _ = &value.unread_response_discarded;
    b.scalar(depth + 1)?;
    b.key(3)?;
    visit_facts(&value.facts, b, depth + 1)?;
    b.key(4)?;
    if let Some(item) = &value.failure {
        visit_failure(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    Ok(())
}

fn visit_cancel(value: &Cancel, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(1, depth)?;
    b.key(1)?;
    b.integer(value.reason.wire(), depth + 1)?;
    Ok(())
}

fn visit_envelope(value: &Envelope, b: &mut Budget, depth: usize) -> Result<(), Error> {
    b.container(22, depth)?;
    b.key(1)?;
    b.integer(value.version, depth + 1)?;
    b.key(2)?;
    b.bytes(value.session_id.len(), MAX_METADATA, depth + 1)?;
    b.key(3)?;
    b.integer(value.stream_id, depth + 1)?;
    b.key(4)?;
    b.integer(value.kind.wire(), depth + 1)?;
    b.key(10)?;
    if let Some(item) = &value.bind {
        visit_bind(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(11)?;
    if let Some(item) = &value.bound {
        visit_bound(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(12)?;
    if let Some(item) = &value.bind_rejected {
        visit_failure(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(13)?;
    if let Some(item) = &value.open {
        visit_open(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(14)?;
    if let Some(item) = &value.opened {
        visit_opened(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(15)?;
    if let Some(item) = &value.open_failed {
        visit_failure(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(16)?;
    if let Some(item) = &value.data {
        visit_data(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(17)?;
    if let Some(item) = &value.window {
        visit_window(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(18)?;
    if let Some(item) = &value.flush {
        visit_barrier(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(19)?;
    if let Some(item) = &value.flushed {
        visit_barrier(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(20)?;
    if let Some(item) = &value.end_write {
        visit_endwrite(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(21)?;
    if let Some(item) = &value.close {
        visit_close(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(22)?;
    if let Some(item) = &value.closed {
        visit_closed(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(23)?;
    if let Some(item) = &value.cancel {
        visit_cancel(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(24)?;
    if let Some(item) = &value.failed {
        visit_failure(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(25)?;
    if let Some(item) = &value.check_identity {
        visit_checkidentity(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(26)?;
    if let Some(item) = &value.identity_checked {
        visit_identitychecked(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    b.key(27)?;
    if let Some(item) = &value.identity_check_failed {
        visit_failure(item, b, depth + 1)?;
    } else {
        b.scalar(depth + 1)?;
    }
    Ok(())
}
