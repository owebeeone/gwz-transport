use super::*;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_POOL: AtomicU64 = AtomicU64::new(1);

pub(super) struct Pending {
    pub request: Request,
    pub deadline: u64,
    pub absolute_deadline: Option<u64>,
    pub state: RequestState,
    pub eviction: Option<ConnectionId>,
}
#[derive(Clone, Copy)]
pub(super) enum RequestState {
    Waiting,
    Opening(ConnectionId),
    Ready(LeaseId),
    Failed(Error),
}
pub(super) struct Entry {
    pub key: Key,
    pub identity: Identity,
    pub reusable: bool,
    pub owner: Option<Owner>,
    pub state: State,
    /// When the connection last became idle on a real release, kept while it is
    /// leased from idle so that a lease given back unused
    /// (`release_unused`) restores it instead of restarting the idle clock.
    pub idle_since: Option<u64>,
    /// When the connection was established (the pool's clock), which `Config::max_age_ms` counts from.
    pub born: u64,
}
/// Whether a connection established at `born` has outlived `max_age_ms` at `now`.
pub(super) fn aged(now: u64, born: u64, max_age_ms: u64) -> bool {
    now >= born.saturating_add(max_age_ms)
}
pub(super) enum State {
    Opening {
        request: Option<RequestId>,
        clock: Option<ConnectClock>,
        network_ms: u64,
        interaction_ms: u64,
        cancel: Option<Cleanup>,
    },
    Idle {
        since: u64,
    },
    Leased {
        lease: LeaseId,
        request: Option<RequestId>,
    },
    Closing {
        reason: CloseReason,
        cleanup: Cleanup,
    },
}
#[derive(Clone)]
pub(super) enum ConnectClock {
    Network(Option<u64>),
    Interaction {
        until: u64,
        remaining: Option<u64>,
    },
    /// The network budget is paused while the resource waits on a local
    /// budget. Only the request's allocation deadline (`until`) bounds the
    /// wait; `remaining` is what the network budget had left.
    LocalWait {
        until: u64,
        remaining: Option<u64>,
    },
    Shared(SetupClock),
}
impl ConnectClock {
    pub fn deadline(&self) -> Option<u64> {
        match self {
            Self::Network(until) => *until,
            Self::Interaction { until, .. } | Self::LocalWait { until, .. } => Some(*until),
            Self::Shared(clock) => clock.cached_deadline(),
        }
    }
}
/// A slot the host has disposed of, held until the server has had time to
/// stop counting its connection. It counts in the host, user-host and site
/// totals, holds no resource, and carries the request that evicted its
/// connection, which is served first when the hold lapses.
pub(super) struct Hold {
    pub key: Key,
    pub expires: u64,
    pub evictor: Option<RequestId>,
}
pub(super) struct Cleanup {
    pub deadline: u64,
    pub sent: bool,
    pub aborted: bool,
}
impl Cleanup {
    pub fn new(deadline: u64) -> Self {
        Self {
            deadline,
            sent: false,
            aborted: false,
        }
    }
}

pub struct PoolMachine {
    pub(super) config: Config,
    pub(super) pool_id: u64,
    pub(super) now: u64,
    pub(super) request_serial: u64,
    pub(super) connection_serial: u64,
    pub(super) lease_serial: u64,
    pub(super) requests: BTreeMap<RequestId, Pending>,
    pub(super) entries: BTreeMap<ConnectionId, Entry>,
    /// Per-site admission targets and settle times, set by the host for the
    /// operation in force. Scheme is not `Eq`, so these are small lists.
    pub(super) limits: Vec<(Site, usize)>,
    pub(super) settles: Vec<(Site, u64)>,
    /// Sites where the host has stopped idle eviction.
    pub(super) no_evicts: Vec<Site>,
    pub(super) holds: BTreeMap<ConnectionId, Hold>,
    pub(super) stopped: bool,
    pub(super) driver_lost: bool,
    pub(super) revision: u64,
    pub(super) clock_wakes: Vec<std::sync::Arc<std::task::Waker>>,
    pub(super) clock_retired: Vec<std::sync::Arc<std::task::Waker>>,
}
impl PoolMachine {
    pub fn new(config: Config) -> Result<Self, Error> {
        config.validate()?;
        let pool_id = NEXT_POOL
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| Error::Capacity)?;
        Ok(Self {
            config,
            pool_id,
            now: 0,
            request_serial: 0,
            connection_serial: 0,
            lease_serial: 0,
            requests: BTreeMap::new(),
            entries: BTreeMap::new(),
            limits: Vec::new(),
            settles: Vec::new(),
            no_evicts: Vec::new(),
            holds: BTreeMap::new(),
            stopped: false,
            driver_lost: false,
            revision: 0,
            clock_wakes: Vec::new(),
            clock_retired: Vec::new(),
        })
    }
    pub fn capacity(&self) -> Capacity {
        Capacity::from(&self.config)
    }
    /// The limits `request` is held to: its operation's, or the pool's own capacity when it carries none.
    pub(super) fn limits_of(&self, request: &Request) -> Capacity {
        request.limits.unwrap_or_else(|| self.capacity())
    }
    /// How many entries `session` holds that satisfy `matches`: opening or leased. Idle entries have no owner (they
    /// belong to no operation) and closing ones are already released.
    fn owned_where(&self, session: &str, matches: impl Fn(&Entry) -> bool) -> usize {
        self.entries
            .values()
            .filter(|entry| {
                matches!(entry.state, State::Opening { .. } | State::Leased { .. })
                    && entry
                        .owner
                        .as_ref()
                        .is_some_and(|owner| owner.session == session)
                    && matches(entry)
            })
            .count()
    }
    /// Whether the owner of `request` holds as many entries as its limits allow for the request's key: in total, on its
    /// host or for its user and host. Leasing an idle entry or opening one charges the owner, so a full owner gets
    /// neither, and evicting an idle entry (which has no owner) cannot make room for it.
    pub(super) fn owner_full(&self, request: &Request) -> bool {
        let limits = self.limits_of(request);
        let session = &request.owner.session;
        self.owned_where(session, |_| true) >= limits.total
            || self.owned_where(session, |entry| entry.key.host == request.key.host)
                >= limits.per_host
            || self.owned_where(session, |entry| entry.key.same_user_host(&request.key))
                >= limits.per_user_host
    }
    /// The instance ceilings applied to `request`: each is max(ceiling, the request's limit), so a lone operation keeps
    /// its own limits and overlapping operations share the ceilings (reuse design section 5).
    pub(super) fn ceilings_for(&self, request: &Request) -> Capacity {
        let limits = self.limits_of(request);
        Capacity {
            per_user_host: self.config.per_user_host.max(limits.per_user_host),
            per_host: self.config.per_host.max(limits.per_host),
            total: self.config.total.max(limits.total),
            max_requests: self.config.max_requests.max(limits.max_requests),
        }
    }
    pub fn request(&mut self, request: Request) -> Result<RequestId, Error> {
        self.request_until(request, None)
    }
    pub fn request_until(
        &mut self,
        request: Request,
        absolute_deadline: Option<u64>,
    ) -> Result<RequestId, Error> {
        if self.driver_lost {
            return Err(Error::DriverLost);
        }
        if self.stopped {
            return Err(Error::Shutdown);
        }
        if !request.valid(&self.config) {
            return Err(Error::InvalidRequest);
        }
        if absolute_deadline.is_some_and(|deadline| deadline <= self.now) {
            return Err(Error::AllocationTimeout);
        }
        // An owner's outstanding requests stay below its own `max_requests`, and all of them below the ceiling.
        if self.requests.len() >= self.ceilings_for(&request).max_requests
            || self
                .requests
                .values()
                .filter(|pending| pending.request.owner.session == request.owner.session)
                .count()
                >= self.limits_of(&request).max_requests
        {
            return Err(Error::Capacity);
        }
        self.request_serial = self.request_serial.checked_add(1).ok_or(Error::Capacity)?;
        let id = RequestId {
            pool: self.pool_id,
            serial: self.request_serial,
        };
        let allocation_deadline = self.now.saturating_add(
            request
                .allocation_timeout_ms
                .unwrap_or(self.config.allocation_timeout_ms),
        );
        let deadline = absolute_deadline.map_or(allocation_deadline, |absolute| {
            allocation_deadline.min(absolute)
        });
        self.requests.insert(
            id,
            Pending {
                request,
                deadline,
                absolute_deadline,
                state: RequestState::Waiting,
                eviction: None,
            },
        );
        self.schedule();
        self.touch();
        Ok(id)
    }
    pub fn take(&mut self, request: RequestId) -> Result<LeaseId, Error> {
        let state = self.requests.get(&request).ok_or(Error::Stale)?.state;
        match state {
            RequestState::Ready(lease) => {
                if let Some(Entry {
                    state: State::Leased { request, .. },
                    ..
                }) = self.entries.get_mut(&lease.connection)
                {
                    *request = None;
                }
                self.requests.remove(&request);
                self.touch();
                Ok(lease)
            }
            RequestState::Failed(error) => {
                self.requests.remove(&request);
                self.touch();
                Err(error)
            }
            _ => Err(Error::WouldBlock),
        }
    }
    pub fn is_live(&self, lease: LeaseId) -> bool {
        self.entries.get(&lease.connection).is_some_and(
            |entry| matches!(entry.state, State::Leased { lease: live, .. } if live == lease),
        )
    }
    pub fn counts(&self) -> Counts {
        self.count_where(|_| true)
    }
    pub fn counts_for_host(&self, host: &str) -> Counts {
        self.count_where(|entry| entry.key.host == host)
    }
    pub fn counts_for_key(&self, key: &Key) -> Counts {
        self.count_where(|entry| entry.key == *key)
    }
    /// Physical capacity grouping, intentionally independent of reuse port.
    pub fn counts_for_user_host(&self, key: &Key) -> Counts {
        self.count_where(|entry| entry.key.same_user_host(key))
    }
    pub fn outstanding_requests(&self) -> usize {
        self.requests.len()
    }
    pub fn shutdown_complete(&self) -> bool {
        self.stopped && self.entries.is_empty()
    }
    pub(super) fn count_where(&self, predicate: impl Fn(&Entry) -> bool) -> Counts {
        let mut counts = Counts::default();
        for entry in self.entries.values().filter(|entry| predicate(entry)) {
            match entry.state {
                State::Opening { .. } => {
                    counts.opening += 1;
                }
                State::Idle { .. } => {
                    counts.idle += 1;
                }
                State::Leased { .. } => {
                    counts.leased += 1;
                }
                State::Closing { .. } => {
                    counts.closing += 1;
                }
            }
        }
        counts
    }
    pub(super) fn touch(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }
    pub(super) fn lease_id(&mut self, connection: ConnectionId) -> Result<LeaseId, Error> {
        self.lease_serial = self.lease_serial.checked_add(1).ok_or(Error::Capacity)?;
        Ok(LeaseId {
            connection,
            generation: self.lease_serial,
        })
    }
}
