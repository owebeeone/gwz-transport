//! The pool's actions to the adapter and the leases it gives openings (movement from `instance.rs`).
use super::state::*;
use crate::{AuthContext, ConnectCommand, OpenFailure, ProofId, ReuseCommand};
use gwz_transport::pool::{
    Action, ConnectionId, Error, LeaseId, MAX_EXCLUDED, PoolMachine, Request,
};
use gwz_transport::protocol::Disposition;
use gwz_transport::protocol::Scheme;
use std::sync::atomic::Ordering;

/// The pool's actions, to the adapter. A connect is given the context of the opening whose request it is for, and its
/// scope is recorded. Whether there were any.
pub(super) fn actions(state: &mut InstanceState) -> bool {
    let mut progressed = false;
    while let Some(action) = state.pool.next_action() {
        progressed = true;
        let InstanceState {
            adapter,
            openings,
            connections,
            ..
        } = &mut *state;
        let Some(adapter) = adapter.as_mut() else {
            continue;
        };
        match action {
            Action::Connect {
                connection,
                key,
                identity,
                tag,
                ..
            } => {
                let auth = tag.and_then(|tag| {
                    openings
                        .iter()
                        .find(|pending| pending.template.tag.as_deref() == Some(tag.as_str()))
                        .map(|pending| pending.auth.clone())
                });
                connections.insert(
                    connection,
                    Setup {
                        scope: auth.as_ref().map(AuthContext::scope),
                        https: key.scheme == Scheme::Https,
                        served: false,
                        lessee: None,
                        closing: None,
                    },
                );
                adapter.connect(ConnectCommand {
                    connection,
                    key,
                    identity,
                    auth,
                })
            }
            Action::CancelConnect { connection, .. } | Action::AbortConnect { connection } => {
                adapter.cancel_connect(connection)
            }
            Action::Close { connection, .. } | Action::Abort { connection } => {
                adapter.close(connection)
            }
        }
    }
    progressed
}

/// Leases for the openings whose request the pool can now serve: the exchange is started and handed to its slot.
pub(super) fn openings(state: &mut InstanceState, now: u64) -> bool {
    let mut progressed = false;
    let mut index = 0;
    while index < state.openings.len() {
        let request = state.openings[index].request;
        let outcome = match state.pool.take(request) {
            Err(Error::WouldBlock) => {
                index += 1;
                continue;
            }
            Err(error) => Err(OpenFailure::Pool(error)),
            Ok(lease) => {
                let scope = state.openings[index].auth.scope();
                let setup = state.connections.get(&lease.connection());
                if setup.is_none() {
                    // No setup record: the adapter has disposed of the connection (its IdleClosed lost the race to this
                    // lease, and the pool refused it). It is never handed back as reusable, which would revive it; the
                    // release ends it, and the opening asks again, not as a misfit, within its own deadline.
                    progressed = true;
                    let _ = state.pool.release(lease, Disposition::Discarded);
                    let pending = state.openings.remove(index);
                    lost_lease(state, pending);
                    continue;
                }
                // The scope partitions HTTPS keys only; SSH reuse is authorized by the proof below (step 1.6).
                let fits = setup.is_some_and(|setup| !setup.https || setup.scope == Some(scope));
                if !fits {
                    // Set up under another operation's scope (a known one): never handed over. It goes back untouched, for
                    // its own scope, and the opening asks once more for a connection of its own, by its original
                    // deadline.
                    progressed = true;
                    state.misfits += 1;
                    let _ = state.pool.release(lease, Disposition::Reusable);
                    let pending = &mut state.openings[index];
                    let bound = misfit_bound(&state.pool, &pending.template);
                    pending.misfits += 1;
                    let asked = if pending.misfits > bound {
                        Err(OpenFailure::NoFittingConnection)
                    } else {
                        let mut fresh = pending.template.clone();
                        fresh.fresh = true;
                        state
                            .pool
                            .request_until(fresh, Some(pending.deadline))
                            .map_err(OpenFailure::Pool)
                    };
                    match asked {
                        Ok(request) => {
                            state.openings[index].request = request;
                            index += 1;
                            continue;
                        }
                        Err(failure) => {
                            let pending = state.openings.remove(index);
                            let _ = pending.slot.fill(Err(failure));
                            continue;
                        }
                    }
                }
                // A fitting lease another binding last used serves this one only after this binding's proof.
                let last = setup.and_then(|setup| setup.lessee);
                progressed = true;
                let pending = state.openings.remove(index);
                if last.is_some_and(|last| last != pending.binding) {
                    state.next_proof += 1;
                    let proof = ProofId(state.next_proof);
                    let command = ReuseCommand {
                        connection: lease.connection(),
                        binding: pending.binding,
                        proof,
                        remaining_ms: pending.deadline.saturating_sub(now),
                    };
                    let started = state
                        .adapter
                        .as_mut()
                        .is_some_and(|adapter| adapter.authorize_reuse(command));
                    if started {
                        state.authorizing.push(Authorizing {
                            pending,
                            lease,
                            proof,
                        });
                    } else {
                        // Nothing this binding can prove: the connection goes back to idle, its idle time untouched,
                        // and this opening asks again without it.
                        let bounced = lease.connection();
                        let _ = state.pool.release_unused(lease);
                        ask_again(state, pending, Some(bounced));
                    }
                    continue;
                }
                serve(state, pending, lease);
                continue;
            }
        };
        progressed = true;
        let pending = state.openings.remove(index);
        let _ = pending.slot.fill(outcome);
    }
    progressed
}

/// Starts `pending`'s exchange on `lease`, with the operation's own context, and hands it to the opening's slot; the
/// lease's binding becomes its connection's last lessee.
pub(super) fn serve(state: &mut InstanceState, pending: Pending, lease: LeaseId) {
    let connection = lease.connection();
    let reused = state
        .connections
        .get_mut(&connection)
        .is_some_and(|setup| std::mem::replace(&mut setup.served, true));
    let started = match state.adapter.as_mut() {
        Some(adapter) => adapter.start_exchange(connection, &pending.spec, &pending.auth),
        None => Err(std::io::Error::other("endpoint instance is closed")),
    };
    let outcome = match started {
        Ok(io) => {
            if let Some(setup) = state.connections.get_mut(&connection) {
                setup.lessee = Some(pending.binding);
            }
            Ok((io, lease, reused))
        }
        // The adapter retired the connection after the pool leased it (a failed proof of its key retired it): a lost
        // lease, as the pool's own `IdleClosed` race makes one, not an I/O failure of the opening.
        Err(error) if error.kind() == std::io::ErrorKind::NotConnected => {
            let _ = state.pool.release(lease, Disposition::Discarded);
            return lost_lease(state, pending);
        }
        Err(error) => {
            let _ = state.pool.release(lease, Disposition::Discarded);
            Err(OpenFailure::Io(error))
        }
    };
    if let Err(Ok((mut io, lease, _))) = pending.slot.fill(outcome) {
        // The opening went while its exchange was being started.
        io.abort();
        let _ = state.pool.release(lease, Disposition::Discarded);
    }
}

/// How many misfits an opening may meet: the larger of the pool's per-user-host ceiling and its binding's own limit, plus
/// one. Idle entries on a key can reach the larger of the two (overlapping bindings share the ceilings), and each one the
/// opening meets that cannot serve it, or that the adapter has lost, counts once; more than that can only mean a broken
/// adapter (a request without limits is bounded by the pool's own capacity).
pub(super) fn misfit_bound(pool: &PoolMachine, request: &Request) -> usize {
    let ceiling = pool.capacity().per_user_host;
    request
        .limits
        .map_or(ceiling, |limits| ceiling.max(limits.per_user_host))
        + 1
}

/// `pending` held a lease of a connection that is gone (the adapter disposed of it, or retired it, before the exchange
/// began): the lease is discarded by the caller and the opening asks again, not as a misfit, within its own deadline.
/// More than `misfit_bound` of them can only mean a broken adapter, and the opening then fails by name.
pub(super) fn lost_lease(state: &mut InstanceState, mut pending: Pending) {
    pending.misfits += 1;
    let bound = misfit_bound(&state.pool, &pending.template);
    if pending.misfits > bound {
        let _ = pending.slot.fill(Err(OpenFailure::NoFittingConnection));
        return;
    }
    ask_again(state, pending, None);
}

/// `pending`'s lease of `bounced` could not serve it: it asks the pool again, with every connection that could not serve
/// it excluded, so that its own binding's idle connection, or a new one, is offered next (step 1.6 remediation, S-P2-2),
/// within its own allocation. Past the pool's exclusion bound it asks fresh, for a new connection. A connection already
/// disposed of (`None`) is excluded by nothing: the pool no longer offers it.
pub(super) fn ask_again(
    state: &mut InstanceState,
    mut pending: Pending,
    bounced: Option<ConnectionId>,
) {
    if refused_for_cancelled(&pending) {
        return;
    }
    if let Some(bounced) = bounced
        && !pending.excluded.contains(&bounced)
    {
        pending.excluded.push(bounced);
    }
    let mut asked = pending.template.clone();
    if pending.excluded.len() <= MAX_EXCLUDED {
        asked.exclude = pending.excluded.clone();
    } else {
        asked.fresh = true;
    }
    // Within the deadline the opening was given at submit, never a new one.
    match state.pool.request_until(asked, Some(pending.deadline)) {
        Ok(request) => {
            pending.request = request;
            state.openings.push(pending);
        }
        Err(error) => {
            let _ = pending.slot.fill(Err(OpenFailure::Pool(error)));
        }
    }
}

/// A binding that was cancelled asks the pool for nothing more (the pool keeps no record that its owner was cancelled, so
/// a new request would open a connection, and spend an agent signature, for an operation the user stopped): the opening
/// ends `Cancelled`. Whether it was.
pub(super) fn refused_for_cancelled(pending: &Pending) -> bool {
    let cancelled = pending.cancelled.load(Ordering::Acquire);
    if cancelled {
        let _ = pending.slot.fill(Err(OpenFailure::Cancelled));
    }
    cancelled
}

/// `pending` asks the pool again for a new connection, with no exclusion, within the deadline it was given at submit.
pub(super) fn ask_fresh(state: &mut InstanceState, mut pending: Pending) {
    if refused_for_cancelled(&pending) {
        return;
    }
    pending.excluded.clear();
    let mut asked = pending.template.clone();
    asked.fresh = true;
    asked.exclude.clear();
    match state.pool.request_until(asked, Some(pending.deadline)) {
        Ok(request) => {
            pending.request = request;
            state.openings.push(pending);
        }
        Err(error) => {
            let _ = pending.slot.fill(Err(OpenFailure::Pool(error)));
        }
    }
}
