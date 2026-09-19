use super::{Error, IoState, IoStatus};

/// Host-reported active-I/O accounting. This clock deliberately knows nothing
/// about messages, carriers, or executors.
#[derive(Debug, Clone, Copy)]
pub(super) struct IoClock {
    state: IoState,
    network_remaining_ms: u64,
    interaction_remaining_ms: u64,
    deadline: Option<u64>,
}

impl IoClock {
    pub(super) fn new(network_ms: u64, interaction_ms: u64) -> Self {
        Self {
            state: IoState::Idle,
            network_remaining_ms: network_ms,
            interaction_remaining_ms: interaction_ms,
            deadline: None,
        }
    }

    pub(super) fn set_state(&mut self, now: u64, state: IoState) {
        if self.state == state {
            return;
        }
        self.account(now);
        self.state = state;
        self.deadline = match state {
            IoState::Network if self.network_remaining_ms > 0 => {
                Some(now.saturating_add(self.network_remaining_ms))
            }
            IoState::Network => None,
            IoState::Interaction => Some(now.saturating_add(self.interaction_remaining_ms)),
            IoState::Idle | IoState::Backpressure => None,
        };
    }

    pub(super) fn progress(
        &mut self,
        now: u64,
        bytes: usize,
        timeout_ms: u64,
    ) -> Result<(), Error> {
        if bytes == 0 {
            return Ok(());
        }
        if self.state != IoState::Network {
            return Err(Error::WrongState);
        }
        self.network_remaining_ms = timeout_ms;
        self.deadline = (timeout_ms > 0).then(|| now.saturating_add(timeout_ms));
        Ok(())
    }

    pub(super) fn account(&mut self, now: u64) {
        let Some(deadline) = self.deadline else {
            return;
        };
        let remaining = deadline.saturating_sub(now);
        match self.state {
            IoState::Network => self.network_remaining_ms = remaining,
            IoState::Interaction => self.interaction_remaining_ms = remaining,
            IoState::Idle | IoState::Backpressure => {}
        }
        self.deadline = None;
    }

    pub(super) fn stop(&mut self, now: u64) {
        self.account(now);
        self.state = IoState::Idle;
    }

    pub(super) fn due(&self, now: u64) -> bool {
        self.deadline.is_some_and(|deadline| now >= deadline)
    }

    pub(super) fn deadline(&self) -> Option<u64> {
        self.deadline
    }

    pub(super) fn status(&self, now: u64) -> IoStatus {
        let (network, interaction) = match self.state {
            IoState::Network => (
                self.deadline
                    .map_or(self.network_remaining_ms, |at| at.saturating_sub(now)),
                self.interaction_remaining_ms,
            ),
            IoState::Interaction => (
                self.network_remaining_ms,
                self.deadline
                    .map_or(self.interaction_remaining_ms, |at| at.saturating_sub(now)),
            ),
            IoState::Idle | IoState::Backpressure => {
                (self.network_remaining_ms, self.interaction_remaining_ms)
            }
        };
        IoStatus {
            state: self.state,
            remaining_network_ms: network,
            remaining_interaction_ms: interaction,
            active_deadline: self.deadline,
        }
    }
}
