//! The closed diagnostic vocabulary authorized by TR1.6 OQ7(1).
use super::Error;
use crate::protocol::{
    CapacityCause, CapacityDetail, Effect, ErrorCode, Failure, FailureDetail, HelperFailureCause,
    SetupFailureCause,
};

pub(super) fn validate(failure: &Failure) -> Result<(), Error> {
    let Some(detail) = &failure.detail else {
        return Ok(());
    };
    if let Some(allowance) = detail.helper_budget_ms {
        let bounded = match failure.setup_cause {
            Some(SetupFailureCause::Interaction) => (1..=120_000).contains(&allowance),
            Some(SetupFailureCause::Allocation) => (0..=86_400_000).contains(&allowance),
            _ => false,
        };
        if failure.code != ErrorCode::Timeout
            || failure.effect != Effect::None
            || !bounded
            || detail.helper_cause.is_some()
            || detail.pipe_kind.is_some()
            || detail.schemes.is_some()
        {
            return Err(Error::InvalidMessage);
        }
    }
    if detail.helper_cause.is_some() && detail.schemes.is_some() {
        return Err(Error::InvalidMessage);
    }
    match (detail.helper_cause, detail.pipe_kind.as_deref()) {
        (Some(HelperFailureCause::PipeFailure), Some(kind)) if pipe_kind(kind) => {}
        (Some(HelperFailureCause::PipeFailure), _) | (_, Some(_)) => {
            return Err(Error::InvalidMessage);
        }
        _ => {}
    }
    if detail.schemes.as_ref().is_some_and(|schemes| {
        schemes.len() > 4 || schemes.iter().any(|scheme| !scheme_token(scheme))
    }) {
        return Err(Error::InvalidMessage);
    }
    if detail.retry_attempt.as_ref().is_some_and(|count| {
        count.attempt <= 0 || count.attempts < count.attempt || count.attempts > u32::MAX as i64
    }) {
        return Err(Error::InvalidMessage);
    }
    if let Some(capacity) = &detail.capacity {
        self::capacity(failure, detail, capacity)?;
    }
    Ok(())
}

/// A member's final `Capacity` failure and why (gwz-core's adaptive concurrency
/// design, section 5.3). The detail belongs to the `Capacity` code alone and
/// stands apart from the other diagnostics. A server cause names the ceiling
/// and the time spent (and the last signal when it was a 429 or a 503; a 403
/// that carried `Retry-After` has none); the host, scheme and port come together
/// or not at all (a redirect target is never shown); a `Retry-After` cause
/// names the wait. A local cause says nothing but its cause: no host, no
/// signal, no attempt count.
fn capacity(
    failure: &Failure,
    detail: &FailureDetail,
    capacity: &CapacityDetail,
) -> Result<(), Error> {
    if failure.code != ErrorCode::Capacity
        || detail.helper_cause.is_some()
        || detail.pipe_kind.is_some()
        || detail.schemes.is_some()
        || detail.helper_budget_ms.is_some()
    {
        return Err(Error::InvalidMessage);
    }
    let named = [
        capacity.scheme.is_some(),
        capacity.host.is_some(),
        capacity.port.is_some(),
    ];
    let any = named.iter().any(|present| *present);
    let all = named.iter().all(|present| *present);
    if capacity.cause == CapacityCause::Local {
        if any
            || detail.retry_attempt.is_some()
            || capacity.limit_applied.is_some()
            || capacity.max_per_host.is_some()
            || capacity.elapsed_ms.is_some()
            || capacity.last_signal.is_some()
            || capacity.retry_after_ms.is_some()
        {
            return Err(Error::InvalidMessage);
        }
        return Ok(());
    }
    let Some(ceiling) = capacity.max_per_host else {
        return Err(Error::InvalidMessage);
    };
    if (any && !all)
        || capacity.elapsed_ms.is_none_or(|ms| ms < 0)
        || capacity.retry_after_ms.is_some_and(|ms| ms < 0)
        || (capacity.cause == CapacityCause::ServerRetryAfter && capacity.retry_after_ms.is_none())
        || !(1..=i64::from(u32::MAX)).contains(&ceiling)
        || capacity
            .limit_applied
            .is_some_and(|limit| limit < 1 || limit > ceiling)
        || capacity
            .port
            .is_some_and(|port| !(1..=i64::from(u16::MAX)).contains(&port))
        || capacity
            .host
            .as_deref()
            .is_some_and(|host| !capacity_host(host))
    {
        return Err(Error::InvalidMessage);
    }
    Ok(())
}

/// A host name, an IPv4 address or an IPv6 address without brackets or a zone:
/// what a pool key holds, never a URL, a user, a path or free text.
fn capacity_host(value: &str) -> bool {
    if value.is_empty() || value.len() > 255 {
        return false;
    }
    if value.contains(':') {
        return value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || b":.".contains(&byte));
    }
    value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || b"-._".contains(&byte))
}

fn scheme_token(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&byte))
}

// Rust's stable ErrorKind names are a fixed vocabulary. Unknown/future kinds
// map to Other at the producer; arbitrary local error text is never admitted.
fn pipe_kind(value: &str) -> bool {
    matches!(
        value,
        "NotFound"
            | "PermissionDenied"
            | "ConnectionRefused"
            | "ConnectionReset"
            | "HostUnreachable"
            | "NetworkUnreachable"
            | "ConnectionAborted"
            | "NotConnected"
            | "AddrInUse"
            | "AddrNotAvailable"
            | "NetworkDown"
            | "BrokenPipe"
            | "AlreadyExists"
            | "WouldBlock"
            | "NotADirectory"
            | "IsADirectory"
            | "DirectoryNotEmpty"
            | "ReadOnlyFilesystem"
            | "FilesystemLoop"
            | "StaleNetworkFileHandle"
            | "InvalidInput"
            | "InvalidData"
            | "TimedOut"
            | "WriteZero"
            | "StorageFull"
            | "NotSeekable"
            | "QuotaExceeded"
            | "FileTooLarge"
            | "ResourceBusy"
            | "ExecutableFileBusy"
            | "Deadlock"
            | "CrossesDevices"
            | "TooManyLinks"
            | "InvalidFilename"
            | "ArgumentListTooLong"
            | "Interrupted"
            | "Unsupported"
            | "UnexpectedEof"
            | "OutOfMemory"
            | "Other"
    )
}
