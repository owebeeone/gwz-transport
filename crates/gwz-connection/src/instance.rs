//! An endpoint instance: one adapter and the pool of its connections, owned by the host and leased to bindings.
//!
//! The pool is gwz-transport's state machine, which does no I/O: its actions go to the adapter and the adapter's
//! completions come back to it, here, in `pump`. The pump also carries the instance's in-flight work: openings whose
//! request waits for a lease (the lease is taken and the exchange started here, and handed to the opening's slot) and
//! finished exchanges whose adapter has not yet confirmed a clean end (settled here, then released). It runs when a
//! command arrives (a submission, a release, a cancellation) and on the host's driver (`pump.rs`) whenever the adapter
//! wakes the waker it was given or a pool deadline passes; never because an application polls.
//!
//! An instance is one profile's (step 1.5): every operation it serves has an equal profile, and none of them leaves its
//! authentication context here. A waiting opening lends its context to the connect the pool starts for it, by the tag
//! on its pool request, and takes it back when it stops waiting. What stays is each connection's setup scope, a
//! fixed-size value: on an HTTPS key a connection serves only an operation whose context has that scope (contract
//! sections 7 and 7.2; SSH connections are not partitioned by it, their reuse authorization being step 1.6's possession
//! proof). An HTTPS request goes to the pool with a scope-derived identity (`Identity::HttpsScoped` of an instance-local
//! token, `scope-N`, never the scope's bytes), so the pool's idle match offers an opening only its own scope's idle
//! connections, and reuse resumes under a changed context while the old context's connections are still idle. The scope
//! check at lease stays as the guard: a lease that does not fit (its setup scope unknown or other) is given back
//! untouched (`Reusable`, never retired), and the opening asks once more with a `fresh` request, which opens a
//! connection of its own under the opening's own tag and so its own scope. More than `misfit_bound` misfits (the larger of
//! the pool's and the binding's `per_user_host`, plus one) can only mean a broken scope record, and the opening then
//! fails by name (`OpenFailure::NoFittingConnection`). No resubmission, after a misfit or after a failed reuse proof,
//! extends the allocation deadline the opening was given at submit.
//!
//! Recorded limitation (lane owner's decision, step 1.5 remediation round 1): when one scope keeps the key's
//! `per_user_host` connections busy and its openings keep arriving, every connection that comes free goes to the next
//! waiter of that scope, so an opening of another scope waits for room: it is served when the load relents, or it ends
//! at its original deadline with `AllocationTimeout`, never `NoFittingConnection`.
//!
//! A fitting lease of an idle connection that another binding last used then waits for the adapter's reuse proof for
//! the leasing binding (`EndpointAdapter::authorize_reuse`; contract section 7.1; step 1.6) before its exchange starts:
//! scope fit first, then the cross-binding proof. The last lessee is metadata kept here with the connection's setup
//! scope, apart from the pool's owner, which an idle entry does not have (contract section 8). Each proof has a
//! host-minted `ProofId`: only the completion that names it ends it, and a proof whose opening went or whose host
//! sealed is cancelled (`cancel_reuse`). A lease no proof authorized goes back to idle unused (`release_unused`, which
//! keeps its idle time), and the opening asks again with it excluded, so a binding's own idle connection is not
//! shadowed by another binding's that it cannot prove. An opening that waits for its proof is governed by its binding's
//! cancellation (`cancel_authorizing`: the proof is cancelled, the lease goes back to idle and the opening ends
//! `Cancelled`, before the pool cancels the binding's owner and closes what it leases) and by its own allocation
//! deadline (`AllocationTimeout`, which the driver wakes for); the command tells the adapter how much of the deadline
//! is left. A binding that was cancelled asks the pool for nothing more.

mod leasing;
mod owner;
mod proofs;
mod state;
mod test_support;

pub(crate) use owner::{Instance, InstanceHold};
pub(crate) use proofs::cancel_authorizing;
pub(crate) use state::Opened;
