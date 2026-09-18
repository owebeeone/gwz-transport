// GENERATED from transport.taut.py; do not edit.
use crate::budget::Budget;
use crate::{
    codec::{Error, MAX_DATA, MAX_METADATA},
    protocol::*,
};

pub(crate) fn envelope(value: &Envelope, limits: &Limits) -> Result<(), Error> {
    let mut budget = Budget::new(value.stream_id == 0, limits);
    visit_envelope(value, &mut budget)
}

fn visit_limits(value: &Limits, b: &mut Budget) -> Result<(), Error> {
    b.container(11)?;
    let _ = &value.encoded_frame;
    b.scalar()?;
    let _ = &value.data_payload;
    b.scalar()?;
    let _ = &value.metadata_bytes;
    b.scalar()?;
    let _ = &value.nesting;
    b.scalar()?;
    let _ = &value.collection_entries;
    b.scalar()?;
    let _ = &value.decode_allocation;
    b.scalar()?;
    let _ = &value.queued_bytes;
    b.scalar()?;
    let _ = &value.queued_frames;
    b.scalar()?;
    let _ = &value.receive_window;
    b.scalar()?;
    let _ = &value.control_reserve_bytes;
    b.scalar()?;
    let _ = &value.control_reserve_frames;
    b.scalar()?;
    Ok(())
}

fn visit_bind(value: &Bind, b: &mut Budget) -> Result<(), Error> {
    b.container(5)?;
    b.container(value.versions.len())?;
    for item in &value.versions {
        let _ = item;
        b.scalar()?;
    }
    let _ = &value.role;
    b.scalar()?;
    b.container(value.schemes.len())?;
    for item in &value.schemes {
        let _ = item;
        b.scalar()?;
    }
    b.container(value.policies.len())?;
    for item in &value.policies {
        let _ = item;
        b.scalar()?;
    }
    visit_limits(&value.receive_limits, b)?;
    Ok(())
}

fn visit_bound(value: &Bound, b: &mut Budget) -> Result<(), Error> {
    b.container(7)?;
    let _ = &value.version;
    b.scalar()?;
    b.bytes(value.endpoint_id.len(), MAX_METADATA)?;
    let _ = &value.role;
    b.scalar()?;
    b.container(value.schemes.len())?;
    for item in &value.schemes {
        let _ = item;
        b.scalar()?;
    }
    b.container(value.policies.len())?;
    for item in &value.policies {
        let _ = item;
        b.scalar()?;
    }
    visit_limits(&value.receive_limits, b)?;
    b.bytes(value.trust_owner.len(), MAX_METADATA)?;
    Ok(())
}

fn visit_failure(value: &Failure, b: &mut Budget) -> Result<(), Error> {
    b.container(2)?;
    let _ = &value.code;
    b.scalar()?;
    let _ = &value.effect;
    b.scalar()?;
    Ok(())
}

fn visit_destination(value: &Destination, b: &mut Budget) -> Result<(), Error> {
    b.container(5)?;
    let _ = &value.scheme;
    b.scalar()?;
    b.bytes(value.host.len(), MAX_METADATA)?;
    let _ = &value.port;
    b.scalar()?;
    b.bytes(value.path.len(), MAX_METADATA)?;
    if let Some(item) = &value.ssh_username {
        b.bytes(item.len(), MAX_METADATA)?;
    } else {
        b.scalar()?;
    }
    Ok(())
}

fn visit_identity(value: &Identity, b: &mut Budget) -> Result<(), Error> {
    b.container(3)?;
    let _ = &value.mode;
    b.scalar()?;
    if let Some(item) = &value.key_path {
        b.bytes(item.len(), MAX_METADATA)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.path_base {
        b.bytes(item.len(), MAX_METADATA)?;
    } else {
        b.scalar()?;
    }
    Ok(())
}

fn visit_deadlines(value: &Deadlines, b: &mut Budget) -> Result<(), Error> {
    b.container(5)?;
    let _ = &value.allocation_ms;
    b.scalar()?;
    let _ = &value.connect_ms;
    b.scalar()?;
    let _ = &value.io_ms;
    b.scalar()?;
    let _ = &value.interaction_ms;
    b.scalar()?;
    let _ = &value.cleanup_ms;
    b.scalar()?;
    Ok(())
}

fn visit_open(value: &Open, b: &mut Budget) -> Result<(), Error> {
    b.container(8)?;
    b.bytes(value.endpoint_id.len(), MAX_METADATA)?;
    b.bytes(value.operation_id.len(), MAX_METADATA)?;
    visit_destination(&value.destination, b)?;
    let _ = &value.service;
    b.scalar()?;
    visit_identity(&value.identity, b)?;
    let _ = &value.policy;
    b.scalar()?;
    visit_deadlines(&value.deadlines, b)?;
    visit_limits(&value.receive_limits, b)?;
    Ok(())
}

fn visit_facts(value: &Facts, b: &mut Budget) -> Result<(), Error> {
    b.container(6)?;
    let _ = &value.method;
    b.scalar()?;
    let _ = &value.credential_offered;
    b.scalar()?;
    if let Some(item) = &value.authenticated {
        let _ = item;
        b.scalar()?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.key_fingerprint {
        b.bytes(item.len(), MAX_METADATA)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.http_status {
        let _ = item;
        b.scalar()?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.ssh_exit_status {
        let _ = item;
        b.scalar()?;
    } else {
        b.scalar()?;
    }
    Ok(())
}

fn visit_opened(value: &Opened, b: &mut Budget) -> Result<(), Error> {
    b.container(6)?;
    b.bytes(value.connection_id.len(), MAX_METADATA)?;
    let _ = &value.reused;
    b.scalar()?;
    b.bytes(value.endpoint_id.len(), MAX_METADATA)?;
    b.bytes(value.trust_owner.len(), MAX_METADATA)?;
    visit_facts(&value.facts, b)?;
    visit_limits(&value.receive_limits, b)?;
    Ok(())
}

fn visit_data(value: &Data, b: &mut Budget) -> Result<(), Error> {
    b.container(2)?;
    let _ = &value.offset;
    b.scalar()?;
    b.bytes(value.payload.len(), MAX_DATA)?;
    Ok(())
}

fn visit_window(value: &Window, b: &mut Budget) -> Result<(), Error> {
    b.container(1)?;
    let _ = &value.max_offset;
    b.scalar()?;
    Ok(())
}

fn visit_barrier(value: &Barrier, b: &mut Budget) -> Result<(), Error> {
    b.container(2)?;
    let _ = &value.barrier_id;
    b.scalar()?;
    let _ = &value.offset;
    b.scalar()?;
    Ok(())
}

fn visit_endwrite(value: &EndWrite, b: &mut Budget) -> Result<(), Error> {
    b.container(1)?;
    let _ = &value.final_offset;
    b.scalar()?;
    Ok(())
}

fn visit_close(value: &Close, b: &mut Budget) -> Result<(), Error> {
    b.container(1)?;
    let _ = &value.final_offset;
    b.scalar()?;
    Ok(())
}

fn visit_closed(value: &Closed, b: &mut Budget) -> Result<(), Error> {
    b.container(4)?;
    let _ = &value.disposition;
    b.scalar()?;
    let _ = &value.unread_response_discarded;
    b.scalar()?;
    visit_facts(&value.facts, b)?;
    if let Some(item) = &value.failure {
        visit_failure(item, b)?;
    } else {
        b.scalar()?;
    }
    Ok(())
}

fn visit_cancel(value: &Cancel, b: &mut Budget) -> Result<(), Error> {
    b.container(1)?;
    let _ = &value.reason;
    b.scalar()?;
    Ok(())
}

fn visit_envelope(value: &Envelope, b: &mut Budget) -> Result<(), Error> {
    b.container(19)?;
    let _ = &value.version;
    b.scalar()?;
    b.bytes(value.session_id.len(), MAX_METADATA)?;
    let _ = &value.stream_id;
    b.scalar()?;
    let _ = &value.kind;
    b.scalar()?;
    if let Some(item) = &value.bind {
        visit_bind(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.bound {
        visit_bound(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.bind_rejected {
        visit_failure(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.open {
        visit_open(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.opened {
        visit_opened(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.open_failed {
        visit_failure(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.data {
        visit_data(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.window {
        visit_window(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.flush {
        visit_barrier(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.flushed {
        visit_barrier(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.end_write {
        visit_endwrite(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.close {
        visit_close(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.closed {
        visit_closed(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.cancel {
        visit_cancel(item, b)?;
    } else {
        b.scalar()?;
    }
    if let Some(item) = &value.failed {
        visit_failure(item, b)?;
    } else {
        b.scalar()?;
    }
    Ok(())
}
