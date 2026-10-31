//! The reuse proofs of openings that lease another binding's idle connection (movement from `instance.rs`).
use super::{leasing::*, state::*};
use crate::{BindingId, OpenFailure, ProofId, ReuseVerdict};
use gwz_transport::pool::{ConnectionId, Error};
use gwz_transport::protocol::Disposition;

/// Ends `item`, an opening whose lease waits for its proof, without serving it: the proof is cancelled, the lease goes back
/// to idle unused (it is another binding's connection, still clean, and keeps the idle time it had) and the opening
/// fails with `failure`. A verdict that comes later names a proof the host no longer waits for and changes nothing.
pub(super) fn end_authorizing(state: &mut InstanceState, item: Authorizing, failure: OpenFailure) {
    if let Some(adapter) = state.adapter.as_mut() {
        adapter.cancel_reuse(item.proof);
    }
    let _ = state.pool.release_unused(item.lease);
    let _ = item.pending.slot.fill(Err(failure));
}

/// The binding was cancelled: each of its openings that waits for a proof ends `Cancelled` now (contract section 7.1: a
/// cancelled proof returns a still-clean leased connection to idle before the binding is cancelled, and section 8: a
/// cancellation affects its binding only, not another binding's idle connections). Called before the pool cancels the
/// binding's owner, which closes what the owner leases.
pub(crate) fn cancel_authorizing(state: &mut InstanceState, binding: BindingId) {
    let mut index = 0;
    while index < state.authorizing.len() {
        if state.authorizing[index].pending.binding == binding {
            let item = state.authorizing.remove(index);
            end_authorizing(state, item, OpenFailure::Cancelled);
        } else {
            index += 1;
        }
    }
}

/// Each opening that waits for a proof past its allocation deadline ends `AllocationTimeout`: a proof never extends the
/// deadline (contract section 7.1). Whether any did.
pub(super) fn expire_authorizing(state: &mut InstanceState, now: u64) -> bool {
    let mut progressed = false;
    let mut index = 0;
    while index < state.authorizing.len() {
        if state.authorizing[index].pending.deadline <= now {
            let item = state.authorizing.remove(index);
            end_authorizing(state, item, OpenFailure::Pool(Error::AllocationTimeout));
            progressed = true;
        } else {
            index += 1;
        }
    }
    progressed
}

/// The reuse proof `proof` for `connection` ended with `verdict` (contract section 7.1): an authorized lease serves its
/// opening; a revoked one is discarded and an unprovable one goes back to idle with its idle time untouched, and the
/// opening asks again without it. A completion whose proof the host no longer waits for (its opening went, or another
/// binding's proof now holds the connection) changes nothing (contract section 4.1: a late completion never acts on
/// another generation).
pub(super) fn reused(
    state: &mut InstanceState,
    connection: ConnectionId,
    proof: ProofId,
    verdict: ReuseVerdict,
) {
    let Some(index) = state
        .authorizing
        .iter()
        .position(|item| item.proof == proof && item.lease.connection() == connection)
    else {
        return;
    };
    let Authorizing { pending, lease, .. } = state.authorizing.remove(index);
    match verdict {
        ReuseVerdict::Authorized => serve(state, pending, lease),
        ReuseVerdict::Revoked => {
            // A denied proof is asked once, not once per pooled connection (reuse design §4): the retry takes no idle
            // connection, whether or not the adapter has yet reported the siblings it retired as `IdleClosed`.
            let _ = state.pool.release(lease, Disposition::Discarded);
            ask_fresh(state, pending);
        }
        ReuseVerdict::Unprovable => {
            let _ = state.pool.release_unused(lease);
            ask_again(state, pending, Some(connection));
        }
    }
}
