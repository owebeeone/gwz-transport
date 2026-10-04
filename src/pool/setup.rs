use super::{machine::*, *};
use std::sync::Arc;

impl PoolMachine {
    /// A raw-machine host takes this after each step and delivers it after
    /// releasing its owner lock. PoolDriver performs that handoff automatically.
    pub fn take_setup_notifications(&mut self) -> ClockUpdate<()> {
        ClockUpdate::<()>::notifications(
            std::mem::take(&mut self.clock_wakes),
            std::mem::take(&mut self.clock_retired),
        )
    }
    pub(super) fn absorb_clock_update<T>(&mut self, update: ClockUpdate<T>) -> T {
        let (value, wakes, retired) = update.into_parts();
        self.clock_wakes.extend(wakes);
        self.clock_retired.extend(retired);
        value
    }
    pub fn install_setup_clock(
        &mut self,
        connection: ConnectionId,
        source: Arc<dyn Fn() -> u64 + Send + Sync>,
        stall_ms: u64,
    ) -> Result<SetupClock, Error> {
        let entry = self.entries.get(&connection).ok_or(Error::Stale)?;
        let State::Opening {
            clock: Some(ConnectClock::Network(until)),
            cancel: None,
            ..
        } = &entry.state
        else {
            return Err(Error::WrongState);
        };
        let clock = SetupClock::new(connection, source, *until, stall_ms);
        let observation = self.absorb_clock_update(clock.observe());
        if let Observation::Terminal(record) = observation {
            return Err(Error::SetupEnded(record));
        }
        let State::Opening { clock: target, .. } = &mut self
            .entries
            .get_mut(&connection)
            .expect("opening entry")
            .state
        else {
            unreachable!()
        };
        *target = Some(ConnectClock::Shared(clock.clone()));
        self.touch();
        Ok(clock)
    }
    pub(super) fn shared_setup_clock(&self, connection: ConnectionId) -> Option<SetupClock> {
        match &self.entries.get(&connection)?.state {
            State::Opening {
                clock: Some(ConnectClock::Shared(clock)),
                ..
            } => Some(clock.clone()),
            _ => None,
        }
    }
    pub(super) fn arbitrate_setup_error(
        &mut self,
        connection: ConnectionId,
        error: Error,
    ) -> Error {
        let Some(clock) = self.shared_setup_clock(connection) else {
            return error;
        };
        let cause = match error {
            Error::DriverLost => SetupCause::DriverLost,
            Error::SetupEnded(record) => record.cause,
            Error::ConnectFailed {
                code,
                effect,
                setup_cause,
            } => SetupCause::ResourceFailure {
                code,
                effect,
                setup_cause,
            },
            _ => SetupCause::Cancelled,
        };
        Error::SetupEnded(self.absorb_clock_update(clock.terminate(cause)))
    }
}
