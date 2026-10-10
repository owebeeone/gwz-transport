//! A numeric admission target and a settle time per site, and one bulk
//! discard of a site's idle connections. The host sets the numbers; the pool
//! compares them with its counts and learns nothing about why they are what
//! they are (gwz-core's adaptive concurrency design, section 4.9).
use super::{machine::*, *};

impl PoolMachine {
    /// Sets the number of connections the pool may hold on `site` at once:
    /// opening, idle, leased and closing, whatever their username, plus the
    /// slots still held for a settle time. It applies next to `per_host` and
    /// `per_user_host`, in creation and in idle eviction. Lowering it closes
    /// nothing by itself: requests wait, and idle connections of the site are
    /// evicted for them as for any cap. A limit lives until the next
    /// `install_capacity`.
    pub fn set_limit(&mut self, site: &Site, limit: usize) -> Result<(), Error> {
        self.accepts_numbers()?;
        if !site.valid() || limit == 0 {
            return Err(Error::InvalidRequest);
        }
        match self.limits.iter_mut().find(|(known, _)| known == site) {
            Some((_, current)) => {
                *current = limit;
            }
            None => {
                self.limits.push((site.clone(), limit));
            }
        }
        self.schedule();
        self.touch();
        Ok(())
    }
    /// Removes `site`'s limit: only the caps bound it again.
    pub fn clear_limit(&mut self, site: &Site) {
        self.limits.retain(|(known, _)| known != site);
        self.schedule();
        self.touch();
    }
    /// The limit in force on `site`, if the host set one.
    pub fn limit(&self, site: &Site) -> Option<usize> {
        self.limits
            .iter()
            .find_map(|(known, limit)| (known == site).then_some(*limit))
    }
    /// Sets how long a slot the host has disposed of stays held on `site`, in
    /// milliseconds; zero holds none. A hold counts in the host, user-host and
    /// site totals until it lapses, so a replacement cannot start while the
    /// server may still count the old connection. Holds already in force lapse
    /// on their own. A connection the server dropped during setup, or lost
    /// while idle, leaves no hold: it is no longer counted.
    pub fn set_settle(&mut self, site: &Site, milliseconds: u64) -> Result<(), Error> {
        self.accepts_numbers()?;
        if !site.valid() {
            return Err(Error::InvalidRequest);
        }
        match self.settles.iter_mut().find(|(known, _)| known == site) {
            Some((_, current)) => {
                *current = milliseconds;
            }
            None => {
                self.settles.push((site.clone(), milliseconds));
            }
        }
        Ok(())
    }
    /// The settle time on `site`, zero when none is set.
    pub fn settle(&self, site: &Site) -> u64 {
        self.settles
            .iter()
            .find_map(|(known, settle)| (known == site).then_some(*settle))
            .unwrap_or(0)
    }
    /// How many slots of `site` are held for a settle time.
    pub fn settling(&self, site: &Site) -> usize {
        self.holds
            .values()
            .filter(|hold| hold.key.on_site(site))
            .count()
    }
    /// Closes every idle connection of `site` at once, whatever its username
    /// or identity, as `Discarded`; each is then disposed by the host and
    /// acknowledged by `closed`. No request is involved and nothing is
    /// evicted. Returns how many it closed.
    pub fn discard_idle(&mut self, site: &Site) -> usize {
        let idle: Vec<_> = self
            .entries
            .iter()
            .filter_map(|(id, entry)| {
                (matches!(entry.state, State::Idle { .. }) && entry.key.on_site(site))
                    .then_some(*id)
            })
            .collect();
        for id in &idle {
            self.start_closing(*id, CloseReason::Discarded);
        }
        idle.len()
    }

    /// A stopped pool takes no new numbers.
    fn accepts_numbers(&self) -> Result<(), Error> {
        if self.driver_lost {
            Err(Error::DriverLost)
        } else if self.stopped {
            Err(Error::Shutdown)
        } else {
            Ok(())
        }
    }
    /// Whether `key`'s site is at its limit, holds and entries counted.
    pub(super) fn site_full(&self, key: &Key) -> bool {
        self.limits
            .iter()
            .find(|(site, _)| key.on_site(site))
            .is_some_and(|(site, limit)| self.site_total(site) >= *limit)
    }
    fn site_total(&self, site: &Site) -> usize {
        self.count_where(|entry| entry.key.on_site(site)).total() + self.settling(site)
    }
    /// Connections and held slots of `host`, across ports and schemes.
    pub(super) fn host_total(&self, host: &str) -> usize {
        self.counts_for_host(host).total()
            + self
                .holds
                .values()
                .filter(|hold| hold.key.host == host)
                .count()
    }
    /// Connections and held slots of `key`'s user on its host.
    pub(super) fn user_host_total(&self, key: &Key) -> usize {
        self.counts_for_user_host(key).total()
            + self
                .holds
                .values()
                .filter(|hold| hold.key.same_user_host(key))
                .count()
    }
    /// Whether the host's disposal of `connection` ended a connect the client
    /// had cancelled: the server may still count it.
    pub(super) fn cancelled_connect(&self, connection: ConnectionId) -> bool {
        self.entries.get(&connection).is_some_and(|entry| {
            matches!(
                entry.state,
                State::Opening {
                    cancel: Some(_),
                    ..
                }
            )
        })
    }
    /// Removes a disposed connection's entry. When `settle` is true and its
    /// site has a settle time, the slot stays held for that long, carrying
    /// the request that evicted the connection.
    pub(super) fn dispose(&mut self, connection: ConnectionId, settle: bool) {
        let Some(entry) = self.entries.remove(&connection) else {
            return;
        };
        let milliseconds = if settle && !self.stopped {
            self.settles
                .iter()
                .find_map(|(site, ms)| entry.key.on_site(site).then_some(*ms))
                .unwrap_or(0)
        } else {
            0
        };
        if milliseconds == 0 {
            return;
        }
        let evictor = self.requests.iter().find_map(|(id, pending)| {
            (matches!(pending.state, RequestState::Waiting) && pending.eviction == Some(connection))
                .then_some(*id)
        });
        self.holds.insert(
            connection,
            Hold {
                key: entry.key,
                expires: self.now.saturating_add(milliseconds),
                evictor,
            },
        );
    }
    /// Ends the holds whose time has come, and serves each one's evictor
    /// before any other waiting request can take the slot.
    pub(super) fn lapse_holds(&mut self) {
        let mut lapsed: Vec<_> = self
            .holds
            .iter()
            .filter(|(_, hold)| hold.expires <= self.now)
            .map(|(id, hold)| (hold.expires, *id))
            .collect();
        lapsed.sort();
        if lapsed.is_empty() {
            return;
        }
        let mut evictors = Vec::new();
        for (_, id) in lapsed {
            if let Some(hold) = self.holds.remove(&id) {
                evictors.extend(hold.evictor);
            }
        }
        for evictor in evictors {
            if self
                .requests
                .get(&evictor)
                .is_some_and(|pending| matches!(pending.state, RequestState::Waiting))
            {
                self.open_for(evictor);
            }
        }
        self.touch();
    }
    pub(super) fn next_hold_expiry(&self) -> Option<u64> {
        self.holds.values().map(|hold| hold.expires).min()
    }
}
