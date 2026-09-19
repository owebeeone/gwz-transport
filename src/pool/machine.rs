use super::*;
use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT_POOL: AtomicU64 = AtomicU64::new(1);

pub(super) struct Pending {
    pub request: Request,
    pub deadline: u64,
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
    pub owner: Option<String>,
    pub state: State,
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
#[derive(Clone, Copy)]
pub(super) enum ConnectClock {
    Network(u64),
    Interaction { until: u64, remaining: u64 },
}
impl ConnectClock {
    pub fn deadline(self) -> u64 {
        match self {
            Self::Network(until) | Self::Interaction { until, .. } => until,
        }
    }
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
    pub(super) stopped: bool,
    pub(super) driver_lost: bool,
    pub(super) revision: u64,
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
            stopped: false,
            driver_lost: false,
            revision: 0,
        })
    }
    pub fn request(&mut self, request: Request) -> Result<RequestId, Error> {
        if self.driver_lost {
            return Err(Error::DriverLost);
        }
        if self.stopped {
            return Err(Error::Shutdown);
        }
        if !request.valid(&self.config) {
            return Err(Error::InvalidRequest);
        }
        if self.requests.len() >= self.config.max_requests {
            return Err(Error::Capacity);
        }
        self.request_serial = self.request_serial.checked_add(1).ok_or(Error::Capacity)?;
        let id = RequestId {
            pool: self.pool_id,
            serial: self.request_serial,
        };
        let deadline = self.now.saturating_add(
            request
                .allocation_timeout_ms
                .unwrap_or(self.config.allocation_timeout_ms),
        );
        self.requests.insert(
            id,
            Pending {
                request,
                deadline,
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
    pub fn outstanding_requests(&self) -> usize {
        self.requests.len()
    }
    pub fn shutdown_complete(&self) -> bool {
        self.stopped && self.entries.is_empty()
    }
    fn count_where(&self, predicate: impl Fn(&Entry) -> bool) -> Counts {
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
