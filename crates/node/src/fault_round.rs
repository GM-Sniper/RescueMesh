//! Automatic fault injector (P5, Step B). Turned on with `[faults] enabled = true`.
//!
//! Spec: "the three cloud servers will periodically elect one machine to ignore
//! communication ... for a random interval of up to 20 seconds and then rejoin."
//!
//! This is the plan's fallback version (handoff 6: "P5 uses a simple Bully rule on
//! alive_peers()"), because P3's bid exchange is not in `main` yet:
//!
//!   1. **Coordinator** = the lowest node ID among this node and the peers its failure
//!      detector reports alive (Bully rule, lowest ID wins like every other tie in the
//!      project). Only the coordinator schedules faults.
//!   2. **Safety rule:** the coordinator only acts while it is not muted, every peer is
//!      alive, and the previous fault's duration has passed. So a second node is never
//!      silenced while one is down. If the coordinator is the one silenced, nobody
//!      injects until it is back.
//!   3. **Wait a random gap**, counted from when all nodes are alive again. If anyone
//!      goes down meanwhile, the wait starts over.
//!   4. **Pick the victim and duration** with a seeded generator: any of the 3 nodes
//!      (the coordinator itself included), silent for a random time in
//!      [min_duration, max_duration]. Same seed = same sequence, so runs repeat.
//!   5. **Silence it** through the same code path as the manual `inject` CLI: a
//!      `FaultCommand` to the victim, or a direct call if the victim is the coordinator.
//!      The victim writes MUTE_START / MUTE_END to its fault CSV as before.
//!
//! Knowledge rule (design doc, Section 7): who was picked stays inside this module.
//! Failover code must learn about the failure only from the failure detector.
//!
//! Upgrade path: when P3's election lands, step 1 becomes a fault round with seeded
//! bids from all three nodes (design doc, Sections 2 to 4). Steps 2 to 5 stay.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::NodeId;
use common::config::Faults;
use common::pb::FaultCommand;
use tokio::time::{Instant, MissedTickBehavior, interval, timeout};

use crate::state::NodeState;

/// How often the coordinator re-checks whether it may inject. Small next to the gaps.
const POLL: Duration = Duration::from_millis(100);

/// One planned fault: who goes silent, for how long, and which round it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannedFault {
    pub round: u64,
    pub victim: NodeId,
    pub duration: Duration,
}

/// The scheduling logic, without network or clock: the caller passes `now`, whether
/// this node is muted, and who its detector reports alive. Tested on its own below.
pub struct Scheduler {
    cfg: Faults,
    me: NodeId,
    /// Every node in the config, sorted (the possible victims).
    all: Vec<NodeId>,
    /// Next round number (starts at 1).
    round: u64,
    /// When the current calm period started (this node coordinator, all alive). None = not calm.
    calm_since: Option<Instant>,
    /// No new fault before this time: the previous victim is still meant to be silent.
    busy_until: Option<Instant>,
}

impl Scheduler {
    pub fn new(cfg: Faults, me: NodeId, mut all: Vec<NodeId>) -> Self {
        all.sort_unstable();
        Scheduler {
            cfg,
            me,
            all,
            round: 1,
            calm_since: None,
            busy_until: None,
        }
    }

    /// Called every POLL. Returns a fault to inject now, or None.
    pub fn poll(
        &mut self,
        now: Instant,
        muted: bool,
        alive_peers: &[NodeId],
    ) -> Option<PlannedFault> {
        // Safety rule: act only when this node is the coordinator, is not muted,
        // and sees every other node alive.
        let everyone_alive = alive_peers.len() + 1 == self.all.len();
        if muted || !everyone_alive || coordinator(self.me, alive_peers) != self.me {
            self.calm_since = None; // the gap starts over once things are calm again
            return None;
        }
        // The previous victim may not have been detected as down yet: wait out its duration.
        if self.busy_until.is_some_and(|t| now < t) {
            return None;
        }
        let calm_since = *self.calm_since.get_or_insert(now);
        if now.duration_since(calm_since) < self.gap(self.round) {
            return None;
        }

        let fault = PlannedFault {
            round: self.round,
            victim: self.victim(self.round),
            duration: self.duration(self.round),
        };
        self.round += 1;
        self.calm_since = None;
        self.busy_until = Some(now + fault.duration);
        Some(fault)
    }

    /// Random wait before round `r`, in [min_gap, max_gap].
    fn gap(&self, r: u64) -> Duration {
        let ms = uniform(
            rng(self.cfg.seed, r, PURPOSE_GAP),
            self.cfg.min_gap_ms,
            self.cfg.max_gap_ms,
        );
        Duration::from_millis(ms)
    }

    /// Random silence for round `r`, in [min_duration, max_duration].
    fn duration(&self, r: u64) -> Duration {
        let ms = uniform(
            rng(self.cfg.seed, r, PURPOSE_DURATION),
            self.cfg.min_duration_ms,
            self.cfg.max_duration_ms,
        );
        Duration::from_millis(ms)
    }

    /// Victim of round `r`: any node, each equally likely.
    fn victim(&self, r: u64) -> NodeId {
        let i = rng(self.cfg.seed, r, PURPOSE_VICTIM) % self.all.len() as u64;
        self.all[i as usize]
    }
}

/// Bully rule: the lowest ID among this node and the peers it believes alive.
pub fn coordinator(me: NodeId, alive_peers: &[NodeId]) -> NodeId {
    alive_peers.iter().copied().fold(me, NodeId::min)
}

// ---- Seeded randomness -------------------------------------------------------------
// SplitMix64, written out so the numbers never change with a library update
// (rand's StdRng may change algorithm between versions). Each value is derived from
// (seed, round, purpose) alone, so it does not depend on what was drawn before.

const PURPOSE_GAP: u64 = 1;
const PURPOSE_DURATION: u64 = 2;
const PURPOSE_VICTIM: u64 = 3;

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E37_79B9_7F4A_7C15);
    x = (x ^ (x >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^ (x >> 31)
}

fn rng(seed: u64, round: u64, purpose: u64) -> u64 {
    splitmix64(splitmix64(splitmix64(seed) ^ round) ^ purpose)
}

/// A number in [lo, hi] from a random u64 (the tiny modulo bias does not matter here).
fn uniform(x: u64, lo: u64, hi: u64) -> u64 {
    lo + x % (hi - lo + 1)
}

// ---- The background task ------------------------------------------------------------

/// Starts the injector if `[faults] enabled = true`; otherwise does nothing.
/// Every node runs it; only the coordinator ever acts.
pub fn spawn(state: Arc<NodeState>) {
    let cfg = state.config.faults;
    if !cfg.enabled {
        return;
    }
    let all: Vec<NodeId> = state.config.nodes.iter().map(|n| n.id).collect();
    // Part of every fault_id, so ids stay unique if a node restarts with the same seed.
    let started_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    tracing::info!(seed = cfg.seed, "automatic fault injector on");

    tokio::spawn(async move {
        let mut scheduler = Scheduler::new(cfg, state.id, all);
        let mut ticker = interval(POLL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let alive = state.alive_peers();
            if let Some(fault) = scheduler.poll(Instant::now(), state.is_muted(), &alive) {
                inject(&state, fault, cfg.seed, started_ms).await;
            }
        }
    });
}

/// Silences the victim through the same path as the manual CLI.
async fn inject(state: &Arc<NodeState>, fault: PlannedFault, seed: u64, started_ms: u128) {
    let cmd = FaultCommand {
        fault_id: format!("auto-s{seed}-r{}-{started_ms}", fault.round),
        target_node: fault.victim,
        duration_ms: fault.duration.as_millis() as u64,
        issuer_node: state.id,
        term: fault.round,
    };
    tracing::info!(
        round = fault.round,
        victim = fault.victim,
        duration_ms = cmd.duration_ms,
        "automatic fault"
    );

    if fault.victim == state.id {
        // The coordinator picked itself: same handler as an incoming InjectFault.
        if let Err(status) = crate::service::handle_inject_fault(state, cmd).await {
            tracing::warn!(%status, "could not silence this node");
        }
        return;
    }

    // None only if this node was muted in the meantime; then the order is simply not sent.
    let Some(mut client) = state.peer_for_sending(fault.victim) else {
        return;
    };
    match timeout(state.config.timing.rpc_deadline(), client.inject_fault(cmd)).await {
        Ok(Ok(ack)) if ack.get_ref().accepted => {}
        Ok(Ok(ack)) => tracing::warn!(reason = %ack.get_ref().reason, "fault order refused"),
        // Safe either way: busy_until still blocks the next round for this duration.
        Ok(Err(status)) => tracing::warn!(%status, "fault order failed"),
        Err(_) => tracing::warn!("no FaultAck before the deadline"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> Faults {
        Faults {
            enabled: true,
            seed: 42,
            min_duration_ms: 2_000,
            max_duration_ms: 20_000,
            min_gap_ms: 10_000,
            max_gap_ms: 30_000,
        }
    }

    fn ms(n: u64) -> Duration {
        Duration::from_millis(n)
    }

    /// Node 1's scheduler (the coordinator when everyone is alive).
    fn node1() -> Scheduler {
        Scheduler::new(cfg(), 1, vec![1, 2, 3])
    }

    /// Polls every 100 ms with everyone alive, until a fault comes out.
    fn run_until_fault(s: &mut Scheduler, t: &mut Instant) -> PlannedFault {
        for _ in 0..10_000 {
            *t += POLL;
            if let Some(f) = s.poll(*t, false, &[2, 3]) {
                return f;
            }
        }
        panic!("no fault within 1000 s");
    }

    #[test]
    fn coordinator_is_lowest_alive_id() {
        assert_eq!(coordinator(2, &[1, 3]), 1);
        assert_eq!(coordinator(2, &[3]), 2); // node 1 suspected down
        assert_eq!(coordinator(3, &[]), 3);
    }

    #[test]
    fn same_seed_same_sequence_different_seed_different() {
        let a = node1();
        let b = node1();
        let mut c = node1();
        c.cfg.seed = 7;
        for r in 1..50 {
            assert_eq!(a.victim(r), b.victim(r));
            assert_eq!(a.duration(r), b.duration(r));
            assert_eq!(a.gap(r), b.gap(r));
        }
        assert!((1..50).any(|r| a.duration(r) != c.duration(r)));
    }

    #[test]
    fn values_stay_in_range_and_victims_are_spread_evenly() {
        let s = node1();
        let mut picks = [0u32; 4];
        for r in 1..=3000 {
            let d = s.duration(r);
            assert!(d >= ms(2_000) && d <= ms(20_000));
            let g = s.gap(r);
            assert!(g >= ms(10_000) && g <= ms(30_000));
            picks[s.victim(r) as usize] += 1;
        }
        // About 1000 each; allow wide room for randomness.
        for (node, count) in picks.iter().enumerate().skip(1) {
            assert!(
                (850..=1150).contains(count),
                "node {node} picked {count} times"
            );
        }
    }

    #[test]
    fn fires_once_the_gap_has_passed() {
        let mut s = node1();
        let start = Instant::now();
        let mut t = start;
        let f = run_until_fault(&mut s, &mut t);
        assert_eq!(f.round, 1);
        let waited = t.duration_since(start);
        assert!(waited >= s.gap(1) && waited <= s.gap(1) + POLL * 2);
    }

    #[test]
    fn only_the_coordinator_injects() {
        // Node 2 sees everyone alive, but node 1 is the coordinator.
        let mut s = Scheduler::new(cfg(), 2, vec![1, 2, 3]);
        let mut t = Instant::now();
        for _ in 0..5_000 {
            t += POLL;
            assert!(s.poll(t, false, &[1, 3]).is_none());
        }
    }

    #[test]
    fn never_while_a_peer_is_down_or_while_muted() {
        let mut s = node1();
        let mut t = Instant::now();
        for _ in 0..5_000 {
            t += POLL;
            assert!(s.poll(t, false, &[3]).is_none()); // node 2 down
            assert!(s.poll(t, true, &[2, 3]).is_none()); // this node muted
        }
    }

    #[test]
    fn a_peer_going_down_restarts_the_gap() {
        let mut s = node1();
        let gap = s.gap(1);
        let start = Instant::now();
        // Calm for most of the gap, then node 2 goes down briefly.
        assert!(s.poll(start, false, &[2, 3]).is_none());
        assert!(s.poll(start + gap - ms(500), false, &[2, 3]).is_none());
        assert!(s.poll(start + gap - ms(400), false, &[3]).is_none());
        // Back: the full gap starts again, so nothing fires at the old deadline.
        let back = start + gap - ms(300);
        assert!(s.poll(back, false, &[2, 3]).is_none());
        assert!(s.poll(start + gap + ms(100), false, &[2, 3]).is_none());
        assert!(s.poll(back + gap, false, &[2, 3]).is_some());
    }

    #[test]
    fn no_new_fault_before_the_last_one_has_ended() {
        // Short gaps, long faults: without busy_until, a second fault could start before
        // the detector had even noticed the first victim.
        let mut c = cfg();
        c.min_gap_ms = 100;
        c.max_gap_ms = 100;
        let mut s = Scheduler::new(c, 1, vec![1, 2, 3]);
        let mut t = Instant::now();
        let first = run_until_fault(&mut s, &mut t);
        let first_at = t;
        // Everyone still looks alive (detector not caught up), yet nothing fires.
        while t < first_at + first.duration - POLL {
            t += POLL;
            assert!(s.poll(t, false, &[2, 3]).is_none());
        }
        let second = run_until_fault(&mut s, &mut t);
        assert_eq!(second.round, 2);
        assert!(t >= first_at + first.duration);
    }

    // ---- End to end: 3 real nodes on localhost, real clock, short timings.

    use common::config::ClusterConfig;
    use common::pb::node_server::NodeServer;

    use crate::peers::Peers;
    use crate::service::NodeService;

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn three_nodes_never_have_two_silent_at_once() {
        let cfg = ClusterConfig::parse(&format!(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 3\naddr = \"127.0.0.1:{}\"\n\
             [timing]\nheartbeat_interval_ms = 50\nsuspect_after_ms = 150\n\
             rpc_deadline_ms = 100\nconnect_timeout_ms = 100\n\
             [faults]\nenabled = true\nseed = 3\nmin_duration_ms = 200\nmax_duration_ms = 400\n\
             min_gap_ms = 200\nmax_gap_ms = 300\n",
            free_port(),
            free_port(),
            free_port()
        ))
        .unwrap();

        let mut nodes = Vec::new();
        for id in 1..=3 {
            let peers = Peers::new(&cfg, id).unwrap();
            let state = Arc::new(NodeState::new(id, cfg.clone(), peers));
            let addr = cfg.node(id).unwrap().addr.parse().unwrap();
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(NodeServer::new(NodeService::new(state.clone())))
                    .serve(addr),
            );
            crate::heartbeat::spawn_senders(state.clone());
            crate::heartbeat::spawn_ticker(state.clone());
            spawn(state.clone());
            nodes.push(state);
        }

        // Watch for 6 s: count fault starts and check that two are never silent together.
        let mut faults = 0;
        let mut was_muted = [false; 3];
        let end = std::time::Instant::now() + Duration::from_secs(6);
        while std::time::Instant::now() < end {
            let muted: Vec<bool> = nodes.iter().map(|n| n.is_muted()).collect();
            assert!(
                muted.iter().filter(|m| **m).count() <= 1,
                "two nodes silent at once: {muted:?}"
            );
            for i in 0..3 {
                if muted[i] && !was_muted[i] {
                    faults += 1;
                }
                was_muted[i] = muted[i];
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            faults >= 3,
            "expected several automatic faults in 6 s, saw {faults}"
        );
    }
}
