//! The owner, the only authority that ends the host, and the wait for its cleanup.
use super::HostShared;
use crate::{CleanupStatus, cell::Cell, operation::TerminalReason, pump::Driver};
use std::{
    fmt,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

/// The only authority that ends the host. `Send`, not `Clone`.
pub struct HostOwner {
    pub(super) shared: Option<Arc<HostShared>>,
    /// The stop authority over the host's driver task.
    pub(super) driver: Driver,
}
impl HostOwner {
    /// Seals the host now and returns the wait for its cleanup. The seal and the disposal do not depend on polling it.
    /// The host's driver stops: in-flight openings end with `OpenFailure::Closing(HostClosing)`.
    pub fn close(mut self) -> HostClose {
        self.driver.stop();
        let cell = match self.shared.take() {
            Some(shared) => shared.seal(TerminalReason::HostClosing),
            None => {
                let cell = Cell::new();
                cell.set(CleanupStatus::Complete);
                cell
            }
        };
        HostClose { cell }
    }
}
impl Drop for HostOwner {
    /// An owner that goes without `close` still seals the host: handles that outlive it must not keep open a host that
    /// nothing can end.
    fn drop(&mut self) {
        self.driver.stop();
        if let Some(shared) = self.shared.take() {
            drop(shared.seal(TerminalReason::HostClosing));
        }
    }
}
impl fmt::Debug for HostOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HostOwner").finish_non_exhaustive()
    }
}

/// The wait for a host's cleanup.
pub struct HostClose {
    cell: Cell<CleanupStatus>,
}
impl Future for HostClose {
    type Output = CleanupStatus;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<CleanupStatus> {
        self.cell.poll(cx)
    }
}
