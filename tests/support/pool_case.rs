use super::support::Random;
use gwz_transport::{
    pool::*,
    protocol::{Disposition, Effect, ErrorCode, Failure},
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

#[derive(Clone)]
struct Resource {
    key: Key,
    identity: Identity,
    settled: bool,
    closing: bool,
}
#[derive(Default, Debug, PartialEq, Eq)]
pub struct Coverage(pub [usize; 12]);
impl Coverage {
    pub fn add(&mut self, other: &Self) {
        for (a, b) in self.0.iter_mut().zip(other.0) {
            *a += b;
        }
    }
}
// Coverage: connect, reuse, lease, cancel, late success, close, abort,
// queue block, interaction, timeout, idle loss, session cancellation.
pub struct Case {
    random: Random,
    sessions: [usize; 2],
    next_session: usize,
    pub config: Config,
    pool: PoolMachine,
    requests: Vec<(RequestId, Request)>,
    leases: Vec<LeaseId>,
    resources: BTreeMap<ConnectionId, Resource>,
    cancelled: BTreeSet<ConnectionId>,
    leased_before: BTreeSet<ConnectionId>,
    pub clock: u64,
    pub step: usize,
    pub trace: VecDeque<String>,
    pub digest: u64,
    pub coverage: Coverage,
}
impl Case {
    pub fn new(seed: u64) -> Self {
        let mut random = Random(seed);
        let config = Config {
            per_user_host: 1 + random.below(3),
            per_host: 1 + random.below(4),
            total: 1 + random.below(8),
            max_requests: 1 + random.below(16),
            idle_timeout_ms: 10 + random.below(100) as u64,
            allocation_timeout_ms: 100,
            connect_timeout_ms: 80,
            interaction_timeout_ms: 100,
            cleanup_timeout_ms: 20,
        };
        Self {
            pool: PoolMachine::new(config.clone()).unwrap(),
            config,
            random,
            sessions: [0, 1],
            next_session: 2,
            requests: Vec::new(),
            leases: Vec::new(),
            resources: BTreeMap::new(),
            cancelled: BTreeSet::new(),
            leased_before: BTreeSet::new(),
            clock: 0,
            step: 0,
            trace: VecDeque::new(),
            digest: 0xcbf29ce484222325,
            coverage: Coverage::default(),
        }
    }
    fn record(&mut self, event: String) {
        for byte in event.bytes() {
            self.digest = (self.digest ^ byte as u64).wrapping_mul(0x100000001b3);
        }
        if self.trace.len() == 40 {
            self.trace.pop_front();
        }
        self.trace.push_back(format!("{}: {event}", self.step));
    }
    fn request(&mut self) {
        let host = format!("host{}", self.random.below(3));
        let key = if self.random.below(4) == 0 {
            Key::https(host, 443)
        } else {
            Key::ssh(
                format!("user{}", self.random.below(2)),
                host,
                if self.random.below(3) == 0 { 2222 } else { 22 },
            )
        };
        let identity = if key.username.is_none() {
            Identity::Https
        } else if self.random.below(3) == 0 {
            Identity::Explicit(format!("proof{}", self.random.below(2)))
        } else {
            Identity::Ambient
        };
        let request = Request::new(
            key,
            identity,
            Owner::new(
                format!("session{}", self.sessions[self.random.below(2)]),
                format!("operation{}", self.random.below(2)),
            ),
        );
        let result = self.pool.request(request.clone());
        self.record(format!(
            "request {request:?}: {:?}",
            result.map(RequestId::sequence)
        ));
        match result {
            Ok(id) => {
                self.requests.push((id, request));
            }
            Err(Error::Capacity | Error::Shutdown) => {}
            Err(e) => {
                panic!("unexpected request error {e:?}");
            }
        }
    }
    fn command(&mut self) -> bool {
        let Some(action) = self.pool.next_action() else {
            return false;
        };
        match action {
            Action::Connect {
                connection,
                key,
                identity,
                network_deadline,
            } => {
                self.record(format!(
                    "connect {} {key:?} {identity:?} {network_deadline:?}",
                    connection.sequence()
                ));
                assert!(!self.resources.contains_key(&connection));
                self.resources.insert(
                    connection,
                    Resource {
                        key,
                        identity,
                        settled: false,
                        closing: false,
                    },
                );
                self.coverage.0[0] += 1;
            }
            Action::CancelConnect {
                connection,
                deadline,
            } => {
                self.record(format!(
                    "cancel-connect {} {deadline}",
                    connection.sequence()
                ));
                assert!(!self.resources[&connection].settled);
                self.cancelled.insert(connection);
                self.coverage.0[3] += 1;
            }
            Action::AbortConnect { connection } => {
                self.record(format!("abort-connect {}", connection.sequence()));
                assert!(!self.resources[&connection].settled);
                self.cancelled.insert(connection);
                self.coverage.0[6] += 1;
            }
            Action::Close {
                connection,
                reason,
                deadline,
            } => {
                self.record(format!(
                    "close {} {reason:?} {deadline}",
                    connection.sequence()
                ));
                let resource = self.resources.get_mut(&connection).unwrap();
                assert!(resource.settled && !resource.closing);
                resource.closing = true;
                self.coverage.0[5] += 1;
            }
            Action::Abort { connection } => {
                self.record(format!("abort {}", connection.sequence()));
                let resource = self.resources.get_mut(&connection).unwrap();
                assert!(resource.settled);
                resource.closing = true;
                self.coverage.0[6] += 1;
            }
        }
        true
    }
    fn complete(&mut self, force: bool) {
        let eligible: Vec<_> = self
            .resources
            .iter()
            .filter_map(|(id, r)| (!r.settled || r.closing).then_some(*id))
            .collect();
        if eligible.is_empty() {
            return;
        }
        let id = eligible[self.random.below(eligible.len())];
        let resource = &self.resources[&id];
        if resource.closing {
            self.record(format!("closed {}", id.sequence()));
            self.pool.closed(id).unwrap();
            self.resources.remove(&id);
        } else {
            let result = if force || self.random.below(5) == 0 {
                Err(Failure {
                    code: ErrorCode::Io,
                    effect: Effect::Possible,
                })
            } else {
                Ok(if self.random.below(4) == 0 {
                    None
                } else {
                    Some(resource.identity.clone())
                })
            };
            self.record(format!("connected {} {result:?}", id.sequence()));
            self.pool.connected(id, result.clone()).unwrap();
            if result.is_err() {
                self.resources.remove(&id);
            } else {
                self.resources.get_mut(&id).unwrap().settled = true;
                if self.cancelled.contains(&id) {
                    self.coverage.0[4] += 1;
                }
            }
        }
    }
    fn take(&mut self) {
        if self.requests.is_empty() {
            return;
        }
        let index = self.random.below(self.requests.len());
        let (id, request) = self.requests[index].clone();
        let result = self.pool.take(id);
        self.record(format!(
            "take {} {:?}",
            id.sequence(),
            result.map(|lease| lease.connection().sequence())
        ));
        match result {
            Ok(lease) => {
                let resource = &self.resources[&lease.connection()];
                assert!(resource.settled && !resource.closing);
                assert_eq!(resource.key, request.key);
                assert_eq!(resource.identity, request.identity);
                assert!(
                    !self.leases.iter().any(|other| self.pool.is_live(*other)
                        && other.connection() == lease.connection()),
                    "double allocation"
                );
                if !self.leased_before.insert(lease.connection()) {
                    self.coverage.0[1] += 1;
                }
                self.coverage.0[2] += 1;
                self.leases.push(lease);
                self.requests.remove(index);
            }
            Err(Error::WouldBlock) => {
                self.coverage.0[7] += 1;
            }
            Err(error) => {
                if matches!(
                    error,
                    Error::AllocationTimeout | Error::ConnectTimeout | Error::InteractionTimeout
                ) {
                    self.coverage.0[9] += 1;
                }
                self.requests.remove(index);
            }
        }
    }
    fn release(&mut self) {
        if self.leases.is_empty() {
            return;
        }
        let lease = self.leases.remove(self.random.below(self.leases.len()));
        let disposition = if self.random.below(4) == 0 {
            Disposition::Discarded
        } else {
            Disposition::Reusable
        };
        let live = self.pool.is_live(lease);
        let result = self.pool.release(lease, disposition);
        self.record(format!(
            "release {} {disposition:?} {result:?}",
            lease.connection().sequence()
        ));
        assert_eq!(result, if live { Ok(()) } else { Err(Error::Stale) });
        assert_eq!(self.pool.release(lease, disposition), Err(Error::Stale));
    }
    fn cancel(&mut self) {
        if self.requests.is_empty() {
            return;
        }
        let index = self.random.below(self.requests.len());
        let id = self.requests[index].0;
        let abandon = self.random.below(2) == 0;
        self.record(format!("cancel {} abandon={abandon}", id.sequence()));
        if abandon {
            self.pool.abandon(id);
            self.requests.remove(index);
        } else {
            self.pool.cancel(id).unwrap();
        }
    }
    fn interaction(&mut self) {
        let eligible: Vec<_> = self
            .resources
            .iter()
            .filter_map(|(id, r)| (!r.settled).then_some(*id))
            .collect();
        if eligible.is_empty() {
            return;
        }
        let id = eligible[self.random.below(eligible.len())];
        let begin = self.random.below(2) == 0;
        let result = if begin {
            self.pool.begin_interaction(id)
        } else {
            self.pool.end_interaction(id)
        };
        self.record(format!(
            "interaction {} begin={begin} {result:?}",
            id.sequence()
        ));
        if result.is_ok() {
            self.coverage.0[8] += 1;
        }
    }
    fn idle_loss(&mut self) {
        let candidates: Vec<_> = self
            .resources
            .iter()
            .filter_map(|(id, r)| r.settled.then_some(*id))
            .collect();
        if candidates.is_empty() {
            return;
        }
        let id = candidates[self.random.below(candidates.len())];
        let result = self.pool.idle_closed(id);
        self.record(format!("idle-loss {} {result:?}", id.sequence()));
        match result {
            Ok(()) => {
                self.resources.remove(&id);
                self.coverage.0[10] += 1;
            }
            // Delayed idle observations must not steal a current lease.
            Err(Error::WrongState) => {}
            error => {
                panic!("unexpected idle-loss result {error:?}");
            }
        }
    }
    fn check(&self) {
        let counts = self.pool.counts();
        assert!(counts.total() <= self.config.total);
        assert!(self.resources.len() <= counts.total());
        assert_eq!(self.pool.outstanding_requests(), self.requests.len());
        assert!(self.requests.len() <= self.config.max_requests);
        // Independently count host resources, including cancelled connectors and
        // cleanup that has not yet acknowledged disposal.
        for resource in self.resources.values() {
            let host = self
                .resources
                .values()
                .filter(|r| r.key.host == resource.key.host)
                .count();
            let key = self
                .resources
                .values()
                .filter(|r| {
                    r.key.host == resource.key.host && r.key.username == resource.key.username
                })
                .count();
            assert!(host <= self.config.per_host && key <= self.config.per_user_host);
            assert!(self.pool.counts_for_host(&resource.key.host).total() <= self.config.per_host);
            assert!(
                self.pool.counts_for_user_host(&resource.key).total() <= self.config.per_user_host
            );
        }
        let mut live = BTreeSet::new();
        for lease in &self.leases {
            if self.pool.is_live(*lease) {
                assert!(live.insert(lease.connection()));
                assert!(self.resources[&lease.connection()].settled);
                assert!(!self.resources[&lease.connection()].closing);
            }
        }
    }
    pub fn run(&mut self) {
        for step in 0..600 {
            self.step = step;
            match self.random.below(22) {
                0..=4 => {
                    self.request();
                }
                5..=7 => {
                    self.command();
                }
                8..=9 => {
                    self.complete(false);
                }
                10..=12 => {
                    self.take();
                }
                13..=14 => {
                    self.release();
                }
                15 => {
                    self.cancel();
                }
                16 => {
                    self.interaction();
                }
                17 => {
                    let session = self.random.below(self.next_session);
                    let owner = Owner::new(
                        format!("session{session}"),
                        format!("operation{}", self.random.below(2)),
                    );
                    if self.random.below(2) == 0 {
                        self.record(format!("cancel-session {}", owner.session));
                        self.pool.cancel_session(&owner.session);
                        self.coverage.0[11] += 1;
                        // Future requests use a fresh binding ID; delayed old
                        // cancellations remain possible without ID reuse.
                        for current in &mut self.sessions {
                            if *current == session {
                                *current = self.next_session;
                                self.next_session += 1;
                            }
                        }
                    } else {
                        self.record(format!("cancel-operation {owner:?}"));
                        self.pool.cancel_operation(&owner);
                    }
                }
                18 => {
                    self.idle_loss();
                }
                _ => {
                    self.clock += self.random.below(41) as u64;
                    self.record(format!("clock {}", self.clock));
                    self.pool.advance(self.clock);
                }
            }
            self.check();
        }
        self.record("shutdown".into());
        self.pool.shutdown();
        for _ in 0..1000 {
            self.step += 1;
            self.command();
            self.complete(true);
            self.take();
            self.check();
            if self.pool.shutdown_complete() && self.requests.is_empty() {
                assert!(self.resources.is_empty());
                assert!(self.leases.iter().all(|lease| !self.pool.is_live(*lease)));
                return;
            }
        }
        panic!("shutdown failed to drain with a cooperative fake host");
    }
}
