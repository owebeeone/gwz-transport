//! The closed diagnostic vocabulary authorized by TR1.6 OQ7(1).
use super::Error;
use crate::protocol::{Effect, ErrorCode, Failure, HelperFailureCause, SetupFailureCause};

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
    Ok(())
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
