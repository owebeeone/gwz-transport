//! The disposal job a sealed host hands to the composition's executor.
use super::HostShared;
use crate::{CleanupStatus, EndpointAdapter, cell::Cell};
use std::{
    future::poll_fn,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};

/// The instances a close job takes when it first runs (or the host takes back if the executor refused the job).
pub(super) type InstanceSlot = Arc<Mutex<Option<Vec<Box<dyn EndpointAdapter>>>>>;

/// The disposal of a sealed host's instances, run by the composition's executor: concurrently, within one bound.
pub(super) async fn close_job(
    host: Arc<HostShared>,
    slot: InstanceSlot,
    cell: Cell<CleanupStatus>,
) {
    let taken = slot.lock().unwrap_or_else(|e| e.into_inner()).take();
    let Some(mut adapters) = taken else {
        return;
    };
    let bound = Duration::from_millis(host.limits.close_bound_ms);
    let mut sleep = host.clock.sleep_until(host.clock.now().plus(bound));
    let disposed = poll_fn(|cx| {
        adapters.retain_mut(|adapter| adapter.poll_dispose(cx).is_pending());
        if adapters.is_empty() {
            return Poll::Ready(true);
        }
        if sleep.as_mut().poll(cx).is_ready() {
            return Poll::Ready(false);
        }
        Poll::Pending
    })
    .await;
    if disposed {
        host.finish(&cell, CleanupStatus::Complete);
        return;
    }
    let (ticket, kept) = host.pend(adapters);
    host.finish(&cell, CleanupStatus::Pending(ticket));
    if let Some(mut work) = kept {
        // The retainer would not take it, so this job is its owner until the disposal is confirmed.
        poll_fn(|cx| work.poll_work(cx)).await;
    }
}
