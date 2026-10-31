//! P5, Step C: the node's mute switch, shared by the node and its shared-state gate.
//!
//! This was a plain field of `NodeState` (P1, PR #10). It moved into its own type, behind an
//! `Arc`, so the store wrapper in `gated_store.rs` can read the same switch: a muted node
//! must lose its access to the shared state as well as to the network. Behaviour is
//! unchanged; `NodeState::mute_for`, `is_muted` and `wait_while_muted` call through to it.

use std::sync::Mutex;
use std::time::Duration;

// tokio's Instant follows tokio's fake clock in tests, so a 10 s mute test runs instantly.
use tokio::time::{Instant, sleep_until};

/// When the current mute ends. An end time instead of an on/off flag: nothing has to switch
/// it back off, and overlapping mutes just keep the later end.
#[derive(Default)]
pub struct MuteSwitch {
    /// None = never muted.
    until: Mutex<Option<Instant>>,
}

impl MuteSwitch {
    /// Go silent for `d` from now. An existing longer mute is kept, never shortened.
    pub fn mute_for(&self, d: Duration) {
        let until = Instant::now() + d;
        let mut current = self.until.lock().unwrap();
        if current.is_none_or(|c| until > c) {
            *current = Some(until);
        }
    }

    /// True while a mute is running.
    pub fn is_muted(&self) -> bool {
        matches!(*self.until.lock().unwrap(), Some(u) if Instant::now() < u)
    }

    /// Returns at once if not muted; otherwise sleeps until the mute ends.
    pub async fn wait_while_muted(&self) {
        loop {
            // Copy the end time out so the lock is released before sleeping.
            let until = *self.until.lock().unwrap();
            match until {
                Some(u) if Instant::now() < u => sleep_until(u).await,
                _ => return,
            }
            // Loop: if the mute was extended while we slept, sleep again.
        }
    }
}
