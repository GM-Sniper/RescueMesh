//! Failure detector: decides which peers are alive and which are suspected down.
//!
//! How it works, in one sentence: every time we hear from a peer we write down
//! the time; if a peer stays silent for longer than `timeout`, we suspect it.
//!
//! This file is pure logic. It never reads the clock and never touches the
//! network. The caller passes the current time (`now`) into every function.
//! That is what lets the tests use fake time instead of really waiting.
//!
//! Who calls what (wiring happens on Monday, not in this file):
//!   - Heartbeat code calls `record(peer, now)` whenever a message from `peer`
//!     arrives, or whenever `peer` answers one of our heartbeats.
//!   - A small background task calls `tick(now)` about every 100 ms and passes
//!     the returned events on to whoever needs them (election, failover).
//!
//! What "Alive" means: the peer is sending heartbeats. It does NOT mean the
//! peer has caught up after a fault. Readiness is a separate question (Step C).

use std::collections::HashMap;
use std::time::{Duration, Instant};

use common::NodeId;

/// What we currently believe about one peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerStatus {
    /// Heard from it recently.
    Alive,
    /// Silent for at least `timeout`. We think it is down.
    Suspected,
}

/// A change in what we believe. Only produced when the status actually flips.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerEvent {
    /// The peer was alive and has now been silent too long.
    PeerDown(NodeId),
    /// The peer was suspected and we have heard from it again.
    PeerUp(NodeId),
}

pub struct FailureDetector {
    /// Last time we heard from each peer.
    last_heard: HashMap<NodeId, Instant>,
    /// What we decided about each peer at the last `tick`.
    /// This is the single source of truth: `is_alive` reads it, `tick` writes it.
    status: HashMap<NodeId, PeerStatus>,
    /// How long a peer may stay silent before we suspect it (planned: 1.5 s).
    timeout: Duration,
}

impl FailureDetector {
    /// Creates a detector for the given peers (do NOT include this node itself).
    ///
    /// Starting rule: every peer starts as Alive, as if we heard from it at `start`.
    /// So a peer gets the full `timeout` to send its first heartbeat. If it never
    /// does, it is reported down after `timeout`, just like any other silent peer.
    /// (The alternative, starting everyone as Suspected, would make every node
    /// look down for the first moment after startup.)
    pub fn new(peers: impl IntoIterator<Item = NodeId>, timeout: Duration, start: Instant) -> Self {
        let mut last_heard = HashMap::new();
        let mut status = HashMap::new();
        for peer in peers {
            last_heard.insert(peer, start);
            status.insert(peer, PeerStatus::Alive);
        }
        Self {
            last_heard,
            status,
            timeout,
        }
    }

    /// Writes down that we heard from `peer` at time `now`.
    ///
    /// Returns false if `peer` is not one of our known peers (it is then ignored).
    /// Does not change the status by itself: the next `tick` does that, so a
    /// peer coming back is reported within one tick period (about 100 ms).
    pub fn record(&mut self, peer: NodeId, now: Instant) -> bool {
        match self.last_heard.get_mut(&peer) {
            Some(last) => {
                // Messages can be processed out of order. Never move time backwards.
                if now > *last {
                    *last = now;
                }
                true
            }
            None => false,
        }
    }

    /// Re-checks every peer at time `now` and returns the status changes.
    ///
    /// A peer silent for `timeout` or longer becomes Suspected (PeerDown).
    /// A suspected peer heard from within `timeout` becomes Alive (PeerUp).
    /// A peer whose status did not change produces no event, so calling
    /// `tick` many times in a row never repeats the same event.
    pub fn tick(&mut self, now: Instant) -> Vec<PeerEvent> {
        let mut events = Vec::new();
        for (&peer, &last) in &self.last_heard {
            // saturating: if `last` is somehow after `now`, treat silence as zero.
            let silent_for = now.saturating_duration_since(last);
            let new_status = if silent_for >= self.timeout {
                PeerStatus::Suspected
            } else {
                PeerStatus::Alive
            };

            let old_status = self.status.insert(peer, new_status);
            match (old_status, new_status) {
                (Some(PeerStatus::Alive), PeerStatus::Suspected) => {
                    events.push(PeerEvent::PeerDown(peer))
                }
                (Some(PeerStatus::Suspected), PeerStatus::Alive) => {
                    events.push(PeerEvent::PeerUp(peer))
                }
                _ => {} // no change, no event
            }
        }
        // HashMap order is random; sort so logs and tests are predictable.
        events.sort_by_key(|e| match e {
            PeerEvent::PeerDown(p) | PeerEvent::PeerUp(p) => *p,
        });
        events
    }

    /// True if the last `tick` decided `peer` is alive. Unknown peers are not alive.
    pub fn is_alive(&self, peer: NodeId) -> bool {
        self.status.get(&peer) == Some(&PeerStatus::Alive)
    }

    /// All peers the last `tick` decided are alive, smallest ID first.
    pub fn alive_peers(&self) -> Vec<NodeId> {
        let mut alive: Vec<NodeId> = self
            .status
            .iter()
            .filter(|(_, s)| **s == PeerStatus::Alive)
            .map(|(p, _)| *p)
            .collect();
        alive.sort();
        alive
    }

    /// The configured timeout (handy for logging).
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

// ---------------------------------------------------------------------------
// Tests. "Fake time" = we pick a starting Instant `t0` and add durations to it
// (t0 + ms(1500)) instead of actually waiting. Run with: cargo test
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    const TIMEOUT: Duration = Duration::from_millis(1500);

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// Node 1's view of the world: peers 2 and 3, starting at t0.
    fn setup() -> (FailureDetector, Instant) {
        let t0 = Instant::now();
        (FailureDetector::new([2, 3], TIMEOUT, t0), t0)
    }

    #[test]
    fn everyone_starts_alive() {
        let (fd, _) = setup();
        assert!(fd.is_alive(2));
        assert!(fd.is_alive(3));
        assert_eq!(fd.alive_peers(), vec![2, 3]);
    }

    #[test]
    fn silent_for_timeout_gives_exactly_one_peer_down() {
        let (mut fd, t0) = setup();
        fd.record(3, t0 + ms(1400)); // peer 3 keeps talking, peer 2 goes quiet

        let events = fd.tick(t0 + ms(1500));
        assert_eq!(events, vec![PeerEvent::PeerDown(2)]);
        assert!(!fd.is_alive(2));

        // Ticking again must not repeat the event.
        assert!(fd.tick(t0 + ms(1600)).is_empty());
        assert!(
            fd.tick(t0 + ms(5000))
                .iter()
                .all(|e| *e != PeerEvent::PeerDown(2))
        );
    }

    #[test]
    fn heard_just_before_timeout_stays_alive() {
        let (mut fd, t0) = setup();
        fd.record(2, t0 + ms(1000));
        fd.record(3, t0 + ms(1000));

        // 1.49 s after the last heartbeat: still fine.
        assert!(fd.tick(t0 + ms(1000 + 1490)).is_empty());
        assert!(fd.is_alive(2));
    }

    #[test]
    fn heard_again_gives_exactly_one_peer_up() {
        let (mut fd, t0) = setup();
        fd.record(3, t0 + ms(1400));
        fd.tick(t0 + ms(1500)); // peer 2 is now suspected

        fd.record(2, t0 + ms(12_000)); // peer 2 wakes up after its mute
        fd.record(3, t0 + ms(12_000));
        assert_eq!(fd.tick(t0 + ms(12_050)), vec![PeerEvent::PeerUp(2)]);
        assert!(fd.is_alive(2));

        // No repeat.
        fd.record(2, t0 + ms(12_100));
        fd.record(3, t0 + ms(12_100));
        assert!(fd.tick(t0 + ms(12_150)).is_empty());
    }

    #[test]
    fn one_silent_peer_does_not_affect_the_other() {
        let (mut fd, t0) = setup();
        // Peer 3 heartbeats every 500 ms; peer 2 says nothing.
        for i in 1..=10 {
            fd.record(3, t0 + ms(500 * i));
        }
        let events = fd.tick(t0 + ms(5000));
        assert_eq!(events, vec![PeerEvent::PeerDown(2)]);
        assert!(fd.is_alive(3));
        assert_eq!(fd.alive_peers(), vec![3]);
    }

    #[test]
    fn both_peers_down_and_up_together() {
        let (mut fd, t0) = setup();
        assert_eq!(
            fd.tick(t0 + ms(2000)),
            vec![PeerEvent::PeerDown(2), PeerEvent::PeerDown(3)]
        );
        assert!(fd.alive_peers().is_empty());

        fd.record(2, t0 + ms(3000));
        fd.record(3, t0 + ms(3000));
        assert_eq!(
            fd.tick(t0 + ms(3010)),
            vec![PeerEvent::PeerUp(2), PeerEvent::PeerUp(3)]
        );
    }

    #[test]
    fn late_arriving_old_message_does_not_move_time_backwards() {
        let (mut fd, t0) = setup();
        fd.record(2, t0 + ms(1000));
        fd.record(2, t0 + ms(200)); // an old message processed late
        // If time had moved back to 200 ms, peer 2 would be suspected at 1.7 s.
        fd.record(3, t0 + ms(1600));
        assert!(fd.tick(t0 + ms(1700)).is_empty());
        assert!(fd.is_alive(2));
    }

    #[test]
    fn unknown_peer_is_ignored() {
        let (mut fd, t0) = setup();
        assert!(!fd.record(99, t0 + ms(100)));
        assert!(!fd.is_alive(99));
        assert_eq!(fd.alive_peers(), vec![2, 3]);
    }

    #[test]
    fn status_only_changes_on_tick() {
        let (mut fd, t0) = setup();
        fd.tick(t0 + ms(2000)); // both suspected
        fd.record(2, t0 + ms(2100));
        // Heard again, but no tick yet: still suspected until the next tick.
        assert!(!fd.is_alive(2));
        fd.tick(t0 + ms(2150));
        assert!(fd.is_alive(2));
    }

    #[test]
    fn mute_for_10s_scenario() {
        // The plan's "done when": mute node 2 for 10 s, others suspect it
        // within ~1.5 s, then see it alive after it returns.
        let (mut fd, t0) = setup();
        let tick_every = ms(100);
        let mut down_at = None;
        let mut up_at = None;

        let mut t = t0;
        while t < t0 + ms(15_000) {
            let elapsed = t - t0;
            // Peer 3 always heartbeats; peer 2 is muted from 2 s to 12 s.
            if elapsed.as_millis() % 500 == 0 {
                fd.record(3, t);
                let muted = elapsed >= ms(2000) && elapsed < ms(12_000);
                if !muted {
                    fd.record(2, t);
                }
            }
            for e in fd.tick(t) {
                match e {
                    PeerEvent::PeerDown(2) => down_at = Some(elapsed),
                    PeerEvent::PeerUp(2) => up_at = Some(elapsed),
                    other => panic!("unexpected event {other:?}"),
                }
            }
            t += tick_every;
        }

        // Last heartbeat from 2 was at 1.5 s, so suspicion at 3.0 s:
        // about 1.0 s after the mute started (mute began between heartbeats).
        let down = down_at.expect("peer 2 was never suspected");
        assert!(down >= ms(2000) && down <= ms(2000) + TIMEOUT + tick_every);
        // First heartbeat after the mute is at 12 s; PeerUp at that tick.
        let up = up_at.expect("peer 2 never came back");
        assert!(up >= ms(12_000) && up <= ms(12_000) + tick_every);
    }
}
