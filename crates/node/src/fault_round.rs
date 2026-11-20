//! Automatic fault injector (P5). Turned on with `[faults] enabled = true`.
//!
//! Spec: "the three cloud servers will periodically elect one machine to ignore
//! communication ... for a random interval of up to 20 seconds and then rejoin."
//!
//! Phase 1 Fix 3: the victim is chosen by P3's election (docs/p3-election-design.md,
//! Section 7), not by a coordinator. Every node runs this module the same way:
//!
//!   1. **Safety rule:** a node takes part only while it is not muted, is ready, and its
//!      failure detector reports every peer alive. If anyone goes down, the wait restarts.
//!   2. **Wait a random gap** (seeded by the fault round), counted from when that holds.
//!      No new round before the previous victim's silence has passed (`busy_until`).
//!   3. **Fault round = an election.** Election ID `fault-<round>`; each node bids a
//!      seeded random score from `(seed, round, node_id)` instead of its load score.
//!      P3's rule picks the lowest score, ties to the lowest node ID.
//!   4. **Only when all three bid, all ready:** a round with a missing or not-ready bid
//!      is skipped (the same round is tried again after a new gap, as a new attempt of
//!      the same election). So a node is never silenced while another is down.
//!   5. **The winner silences itself**, through the same handler as the manual `inject`
//!      CLI (`handle_inject_fault`), for a seeded duration in [min_duration, max_duration].
//!      Nobody sends a FaultCommand over the network.
//!
//! Every node holding the same three bids picks the same winner, so there is no
//! coordinator and no single node that must be up for faults to happen.
//!
//! Round numbers stay in step: a node that receives a bid for a newer fault round
//! jumps to it (`Shared::seen`). A victim checks, just before muting itself, that no
//! newer round has started; the `muting` flag closes the gap between that check and
//! the mute (see `mute_self` and `on_peer_bid`).
//!
//! Knowledge rule (p5-changes.md): who was picked stays inside the injector and the
//! election table. Failover code learns about the failure only from the failure detector.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use common::NodeId;
use common::config::Faults;
use common::pb::{FaultCommand, SubmitRequest};
use tokio::time::{Instant, MissedTickBehavior, interval};

use crate::election::{self, Bid};
use crate::state::NodeState;

/// How often each node re-checks whether a fault round is due. Small next to the gaps.
const POLL: Duration = Duration::from_millis(100);

// ---- Election IDs ---------------------------------------------------------------------

const PREFIX: &str = "fault-";

/// The election ID of a fault round (design doc Section 7).
pub fn election_id(round: u64) -> String {
    format!("{PREFIX}{round}")
}

/// The fault round of an election ID, or None for a client request.
/// Only `fault-` followed by digits counts; client request IDs are UUIDs.
pub fn parse_round(election_id: &str) -> Option<u64> {
    let digits = election_id.strip_prefix(PREFIX)?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

// ---- Bids -------------------------------------------------------------------------------

/// This node's bid in fault round `round`: a seeded random score, so the same seed
/// gives the same victims, and each node wins about a third of the rounds.
pub fn bid(state: &NodeState, round: u64) -> Bid {
    Bid {
        node_id: state.id,
        load_score: score(state.config.faults.seed, round, state.id),
        ready: state.is_ready() && !state.is_muted(),
    }
}

/// Called by `election::handle_exchange_bid` for a peer's bid in a fault round, before
/// it replies with ours. Records that the round exists, and answers "not ready" while
/// this node is about to silence itself for an earlier round, so that peer's round is
/// skipped instead of picking a second victim.
pub fn on_peer_bid(state: &NodeState, round: u64, mine: &mut Bid) {
    state.faults.seen.fetch_max(round, Ordering::SeqCst);
    if state.faults.muting.load(Ordering::SeqCst) {
        mine.ready = false;
    }
}

/// Injector state shared with the bid handler.
#[derive(Default)]
pub struct Shared {
    /// Highest fault round any peer has bid in.
    seen: AtomicU64,
    /// True from a victim's last check until its mute is on.
    muting: AtomicBool,
}

impl Shared {
    pub fn seen(&self) -> u64 {
        self.seen.load(Ordering::SeqCst)
    }
}

// ---- Scheduling (pure logic) ----------------------------------------------------------

/// How a fault round ended, from this node's side.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// All three bid and were ready: `victim` won and goes silent for `duration`.
    Fault { victim: NodeId, duration: Duration },
    /// A bid was missing or not ready: no fault this time.
    Skipped,
}

/// When to hold fault rounds, without network or clock: the caller passes `now`, this
/// node's state, and who its detector reports alive. Tested on its own below.
pub struct Scheduler {
    cfg: Faults,
    /// How many nodes must bid (every node in the config).
    nodes: usize,
    /// The fault round to hold next (starts at 1).
    round: u64,
    /// When the current calm period started (all alive, this node ready). None = not calm.
    calm_since: Option<Instant>,
    /// No new round before this time: the previous victim is still meant to be silent.
    busy_until: Option<Instant>,
}

impl Scheduler {
    pub fn new(cfg: Faults, nodes: usize) -> Self {
        Scheduler {
            cfg,
            nodes,
            round: 1,
            calm_since: None,
            busy_until: None,
        }
    }

    /// Called every POLL. Returns the fault round to hold now, or None.
    /// `seen` is the highest round a peer has bid in: we never stay behind it.
    pub fn poll(
        &mut self,
        now: Instant,
        muted: bool,
        ready: bool,
        alive_peers: &[NodeId],
        seen: u64,
    ) -> Option<u64> {
        if seen > self.round {
            self.round = seen; // a peer is already in a newer round: join it
        }
        // Safety rule: take part only when not muted, ready, and every other node is alive.
        let everyone_alive = alive_peers.len() + 1 == self.nodes;
        if muted || !ready || !everyone_alive {
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
        Some(self.round)
    }

    /// The result of the round `poll` returned.
    pub fn finished(&mut self, round: u64, outcome: Outcome, now: Instant) {
        self.calm_since = None; // either way, the next round waits a full gap
        if let Outcome::Fault { duration, .. } = outcome
            && round >= self.round
        {
            self.round = round + 1;
            self.busy_until = Some(now + duration);
        }
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
}

/// Silence for round `r`, in [min_duration, max_duration]. Every node computes the same.
pub fn duration(cfg: &Faults, r: u64) -> Duration {
    let ms = uniform(
        rng(cfg.seed, r, PURPOSE_DURATION),
        cfg.min_duration_ms,
        cfg.max_duration_ms,
    );
    Duration::from_millis(ms)
}

// ---- Seeded randomness -------------------------------------------------------------
// SplitMix64, written out so the numbers never change with a library update
// (rand's StdRng may change algorithm between versions). Each value is derived from
// (seed, round, purpose) alone, so it does not depend on what was drawn before.

const PURPOSE_GAP: u64 = 1;
const PURPOSE_DURATION: u64 = 2;
/// Bid scores: the node ID goes in the low 32 bits, so each node draws its own number.
const PURPOSE_SCORE: u64 = 3 << 32;

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

/// Node `node`'s bid score in fault round `round`, in [0, 1). Same inputs, same score.
pub fn score(seed: u64, round: u64, node: NodeId) -> f64 {
    let x = rng(seed, round, PURPOSE_SCORE | u64::from(node));
    (x >> 11) as f64 / (1u64 << 53) as f64
}

// ---- The background task ------------------------------------------------------------

/// Starts the injector if `[faults] enabled = true`; otherwise does nothing.
/// Every node runs it the same way; the election picks the victim.
pub fn spawn(state: Arc<NodeState>) {
    let cfg = state.config.faults;
    if !cfg.enabled {
        return;
    }
    let nodes = state.config.nodes.len();
    // Part of every fault_id, so ids stay unique if a node restarts with the same seed.
    let started_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);
    tracing::info!(seed = cfg.seed, "automatic fault injector on (election mode)");

    tokio::spawn(async move {
        let mut scheduler = Scheduler::new(cfg, nodes);
        let mut ticker = interval(POLL);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let due = scheduler.poll(
                Instant::now(),
                state.is_muted(),
                state.is_ready(),
                &state.alive_peers(),
                state.faults.seen(),
            );
            let Some(round) = due else { continue };
            let outcome = hold_round(&state, round, nodes).await;
            scheduler.finished(round, outcome, Instant::now());
            if let Outcome::Fault { victim, duration } = outcome
                && victim == state.id
            {
                mute_self(&state, round, duration, started_ms).await;
            }
        }
    });
}

/// One fault round: P3's election with seeded bids. Fault only if all `nodes` bid, ready.
async fn hold_round(state: &Arc<NodeState>, round: u64, nodes: usize) -> Outcome {
    let id = election_id(round);
    {
        // Tried before and skipped: a new attempt of the same election (P3's next round),
        // so the peers bid again instead of answering with the old decision.
        let mut table = state.election.lock().unwrap();
        if table.winner(&id).is_some() {
            table.start_next_round(&id, || bid(state, round));
        }
    }
    let request = SubmitRequest {
        request_id: id.clone(),
        kind: None, // a fault round carries no incident
    };
    let winner = election::run_election(state, &request).await;
    let bids = state.election.lock().unwrap().bids(&id);

    let complete = bids.len() == nodes && bids.iter().all(|b| b.ready);
    match winner {
        Some(victim) if complete => {
            let duration = duration(&state.config.faults, round);
            tracing::info!(round, victim, duration_ms = duration.as_millis() as u64, "fault round decided");
            Outcome::Fault { victim, duration }
        }
        _ => {
            tracing::info!(round, bids = bids.len(), "fault round skipped: a bid was missing or not ready");
            Outcome::Skipped
        }
    }
}

/// This node won fault round `round`: silence itself through the same handler as the
/// manual CLI, unless a newer round has started meanwhile.
async fn mute_self(state: &Arc<NodeState>, round: u64, duration: Duration, started_ms: u128) {
    // Set first, then check: a peer's bid for a newer round either is seen by the check
    // below, or arrives after `muting` is set and gets a not-ready answer (on_peer_bid).
    // Either way, that newer round cannot pick a second victim while we go silent.
    state.faults.muting.store(true, Ordering::SeqCst);
    if state.faults.seen() > round {
        state.faults.muting.store(false, Ordering::SeqCst);
        tracing::info!(round, "fault round outdated: not muting");
        return;
    }
    let seed = state.config.faults.seed;
    let cmd = FaultCommand {
        fault_id: format!("auto-s{seed}-r{round}-{started_ms}"),
        target_node: state.id,
        duration_ms: duration.as_millis() as u64,
        issuer_node: state.id,
        term: round,
    };
    tracing::info!(round, duration_ms = cmd.duration_ms, "automatic fault: silencing this node");
    if let Err(status) = crate::service::handle_inject_fault(state, cmd).await {
        tracing::warn!(%status, "could not silence this node");
    }
    // Muted now: the mute gate holds every bid call until we are back.
    state.faults.muting.store(false, Ordering::SeqCst);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::election::pick_winner;

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

    fn sched() -> Scheduler {
        Scheduler::new(cfg(), 3)
    }

    /// The victim of round `r` when all three bid, ready: P3's rule on the seeded scores.
    fn victim(seed: u64, r: u64) -> NodeId {
        let bids: Vec<Bid> = (1..=3)
            .map(|n| Bid { node_id: n, load_score: score(seed, r, n), ready: true })
            .collect();
        pick_winner(&bids).unwrap()
    }

    /// Polls every 100 ms with everyone alive, until a round is due.
    fn run_until_due(s: &mut Scheduler, t: &mut Instant) -> u64 {
        for _ in 0..10_000 {
            *t += POLL;
            if let Some(r) = s.poll(*t, false, true, &[2, 3], 0) {
                return r;
            }
        }
        panic!("no round within 1000 s");
    }

    #[test]
    fn election_ids_round_trip_and_client_ids_are_not_fault_rounds() {
        assert_eq!(election_id(17), "fault-17");
        assert_eq!(parse_round("fault-17"), Some(17));
        assert_eq!(parse_round("fault-"), None);
        assert_eq!(parse_round("fault-1x"), None);
        assert_eq!(parse_round("3f2a9c1e-0000-4000-8000-000000000000"), None);
    }

    #[test]
    fn same_seed_same_sequence_different_seed_different() {
        let c = cfg();
        let mut other = cfg();
        other.seed = 7;
        for r in 1..50 {
            assert_eq!(victim(42, r), victim(42, r));
            assert_eq!(duration(&c, r), duration(&c, r));
            for n in 1..=3 {
                assert_eq!(score(42, r, n), score(42, r, n));
            }
        }
        assert!((1..50).any(|r| victim(42, r) != victim(7, r)));
        assert!((1..50).any(|r| duration(&c, r) != duration(&other, r)));
    }

    #[test]
    fn scores_differ_per_node_and_victims_are_spread_evenly() {
        let mut picks = [0u32; 4];
        for r in 1..=3000 {
            let s: Vec<f64> = (1..=3).map(|n| score(42, r, n)).collect();
            assert!(s.iter().all(|x| (0.0..1.0).contains(x)));
            assert!(s[0] != s[1] && s[1] != s[2]);
            picks[victim(42, r) as usize] += 1;
        }
        // About 1000 each; allow wide room for randomness.
        for (node, count) in picks.iter().enumerate().skip(1) {
            assert!((850..=1150).contains(count), "node {node} picked {count} times");
        }
    }

    #[test]
    fn values_stay_in_range() {
        let s = sched();
        for r in 1..=3000 {
            let d = duration(&cfg(), r);
            assert!(d >= ms(2_000) && d <= ms(20_000));
            let g = s.gap(r);
            assert!(g >= ms(10_000) && g <= ms(30_000));
        }
    }

    #[test]
    fn due_once_the_gap_has_passed() {
        let mut s = sched();
        let start = Instant::now();
        let mut t = start;
        assert_eq!(run_until_due(&mut s, &mut t), 1);
        let waited = t.duration_since(start);
        assert!(waited >= s.gap(1) && waited <= s.gap(1) + POLL * 2);
    }

    #[test]
    fn every_node_takes_part_no_coordinator() {
        // Node 3's view is the same as node 1's: no lowest-ID rule decides who acts.
        let mut a = sched();
        let mut b = sched();
        let mut t = Instant::now();
        for _ in 0..1_000 {
            t += POLL;
            assert_eq!(a.poll(t, false, true, &[2, 3], 0), b.poll(t, false, true, &[1, 2], 0));
        }
    }

    #[test]
    fn never_while_a_peer_is_down_muted_or_not_ready() {
        let mut s = sched();
        let mut t = Instant::now();
        for _ in 0..5_000 {
            t += POLL;
            assert!(s.poll(t, false, true, &[3], 0).is_none()); // node 2 down
            assert!(s.poll(t, true, true, &[2, 3], 0).is_none()); // this node muted
            assert!(s.poll(t, false, false, &[2, 3], 0).is_none()); // this node not ready
        }
    }

    #[test]
    fn a_peer_going_down_restarts_the_gap() {
        let mut s = sched();
        let gap = s.gap(1);
        let start = Instant::now();
        assert!(s.poll(start, false, true, &[2, 3], 0).is_none());
        assert!(s.poll(start + gap - ms(500), false, true, &[2, 3], 0).is_none());
        assert!(s.poll(start + gap - ms(400), false, true, &[3], 0).is_none());
        let back = start + gap - ms(300);
        assert!(s.poll(back, false, true, &[2, 3], 0).is_none());
        assert!(s.poll(start + gap + ms(100), false, true, &[2, 3], 0).is_none());
        assert_eq!(s.poll(back + gap, false, true, &[2, 3], 0), Some(1));
    }

    #[test]
    fn a_skipped_round_is_tried_again_after_a_new_gap() {
        let mut s = sched();
        let mut t = Instant::now();
        assert_eq!(run_until_due(&mut s, &mut t), 1);
        s.finished(1, Outcome::Skipped, t);
        let skipped_at = t;
        assert_eq!(run_until_due(&mut s, &mut t), 1); // same round, not round 2
        assert!(t.duration_since(skipped_at) >= s.gap(1));
    }

    #[test]
    fn no_new_round_before_the_last_fault_has_ended() {
        let mut c = cfg();
        c.min_gap_ms = 100;
        c.max_gap_ms = 100;
        let mut s = Scheduler::new(c, 3);
        let mut t = Instant::now();
        assert_eq!(run_until_due(&mut s, &mut t), 1);
        let d = duration(&c, 1);
        s.finished(1, Outcome::Fault { victim: 2, duration: d }, t);
        let first_at = t;
        // Everyone still looks alive (detector not caught up), yet nothing is due.
        while t < first_at + d - POLL {
            t += POLL;
            assert!(s.poll(t, false, true, &[2, 3], 0).is_none());
        }
        assert_eq!(run_until_due(&mut s, &mut t), 2);
        assert!(t >= first_at + d);
    }

    #[test]
    fn a_node_behind_joins_the_newer_round_it_sees() {
        let mut s = sched();
        let mut t = Instant::now();
        for _ in 0..10_000 {
            t += POLL;
            if let Some(r) = s.poll(t, false, true, &[2, 3], 5) {
                assert_eq!(r, 5);
                return;
            }
        }
        panic!("never due");
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

        // Watch for 8 s: count fault starts and check that two are never silent together.
        let mut faults = [0u32; 3];
        let mut was_muted = [false; 3];
        let end = std::time::Instant::now() + Duration::from_secs(8);
        while std::time::Instant::now() < end {
            let muted: Vec<bool> = nodes.iter().map(|n| n.is_muted()).collect();
            assert!(
                muted.iter().filter(|m| **m).count() <= 1,
                "two nodes silent at once: {muted:?}"
            );
            for i in 0..3 {
                if muted[i] && !was_muted[i] {
                    faults[i] += 1;
                }
                was_muted[i] = muted[i];
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let total: u32 = faults.iter().sum();
        assert!(total >= 2, "expected several automatic faults in 8 s, saw {faults:?}");
    }
}
