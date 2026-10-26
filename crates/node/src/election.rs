//! P3: the election rule (docs/p3-election-design.md, Section 1).
//!
//! For each request, every live node bids its load score. Every node applies the
//! same rule to the bids it holds: lowest `load_score` wins, ties go to the lowest
//! `node_id`. Nodes that see the same bids therefore pick the same winner, with no
//! coordinator.
//!
//! The rule (`pick_winner`) and the notebook (`ElectionTable`) are pure logic,
//! like the failure detector: no clock, no network. `run_election` at the end is
//! the network part: it exchanges bids with the peers and decides.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use common::NodeId;
use common::pb::{self, BidRequest, SubmitRequest};

use crate::broadcast::{PeerResult, broadcast};
use crate::state::NodeState;

/// One node's bid for one request and round, as this node holds it.
/// Separate from the proto `Bid` (the wire format), like P2's domain types.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Bid {
    pub node_id: NodeId,
    /// The bidder's load score, 0-1; lower = less loaded.
    pub load_score: f64,
    /// False while the bidder cannot reach the shared state (design doc, Section 4).
    /// It counts as an answer, so nobody waits for it, but it cannot win.
    pub ready: bool,
}

/// The winner among `bids`: lowest `load_score`, ties to the lowest `node_id`.
/// Bids that are not `ready` are skipped. `None` if no bid is ready.
///
/// The result does not depend on the order of `bids`, so every node that holds
/// the same bids picks the same winner.
pub fn pick_winner(bids: &[Bid]) -> Option<NodeId> {
    bids.iter()
        .filter(|b| b.ready)
        .min_by(|a, b| compare(a, b))
        .map(|b| b.node_id)
}

/// Lower is better. `total_cmp` instead of `partial_cmp`: a NaN score (a bug on
/// the bidder's side) then sorts after every real score on every node, instead
/// of making the comparison undefined and the nodes disagree.
fn compare(a: &Bid, b: &Bid) -> Ordering {
    a.load_score
        .total_cmp(&b.load_score)
        .then(a.node_id.cmp(&b.node_id))
}

/// What `record_bid` did with a peer's bid.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Recorded {
    /// Stored: it counts in this round's decision.
    Stored,
    /// Arrived after this round was decided: counted as late, not used.
    Late,
    /// From an earlier round: ignored.
    OldRound,
    /// From a later round than ours (the peer already re-elected). Not stored;
    /// the caller decides whether to follow it into that round.
    NewerRound,
    /// No election for this request yet: the caller fixes its own bid (`my_bid`) first.
    UnknownRequest,
}

/// One request's election, as this node sees it.
struct Entry {
    round: u32,
    /// Fixed once per round, so every peer compares the same number from us.
    my_bid: Bid,
    /// Peers' bids for this round. The first copy of each is kept.
    bids: BTreeMap<NodeId, Bid>,
    /// Set by `decide` and never changed for this round.
    winner: Option<NodeId>,
    /// Bids that arrived after the decision (election metrics, design doc Section 8).
    late_bids: u32,
}

impl Entry {
    fn new(round: u32, my_bid: Bid) -> Self {
        Entry {
            round,
            my_bid,
            bids: BTreeMap::new(),
            winner: None,
            late_bids: 0,
        }
    }
}

/// Every election this node has taken part in, by `request_id`.
///
/// Entries are never removed: fine for experiment-sized runs (thousands of
/// requests), but it grows without limit in a long run. TODO(P3): drop old entries.
#[derive(Default)]
pub struct ElectionTable {
    entries: HashMap<String, Entry>,
}

impl ElectionTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// This node's bid for the request. The first call fixes it from `current`
    /// (the load score right now); every later call returns the same bid, whether
    /// the request came from the client or from a peer's bid call.
    pub fn my_bid(&mut self, request_id: &str, current: impl FnOnce() -> Bid) -> Bid {
        self.entries
            .entry(request_id.to_string())
            .or_insert_with(|| Entry::new(0, current()))
            .my_bid
    }

    /// Stores a peer's bid for `round` of the request, unless it is too old, too
    /// late, or a repeat (the first copy is kept).
    pub fn record_bid(&mut self, request_id: &str, round: u32, bid: Bid) -> Recorded {
        let Some(entry) = self.entries.get_mut(request_id) else {
            return Recorded::UnknownRequest;
        };
        if round < entry.round {
            return Recorded::OldRound;
        }
        if round > entry.round {
            return Recorded::NewerRound;
        }
        if entry.winner.is_some() {
            entry.late_bids += 1;
            return Recorded::Late;
        }
        if bid.node_id != entry.my_bid.node_id {
            entry.bids.entry(bid.node_id).or_insert(bid);
        }
        Recorded::Stored
    }

    /// Decides the current round from the bids held now (own bid included) and
    /// keeps that answer: later calls return the same winner. `None` if the
    /// request is unknown or no bid is ready; nothing is fixed in that case.
    pub fn decide(&mut self, request_id: &str) -> Option<NodeId> {
        let entry = self.entries.get_mut(request_id)?;
        if entry.winner.is_none() {
            let mut all: Vec<Bid> = entry.bids.values().copied().collect();
            all.push(entry.my_bid);
            entry.winner = pick_winner(&all);
        }
        entry.winner
    }

    /// Re-election after the winner failed (design doc Section 4, "Winner fails"):
    /// round + 1, a fresh own bid from `current`, old bids and winner cleared.
    /// Returns the new round, or `None` if the request is unknown.
    pub fn start_next_round(
        &mut self,
        request_id: &str,
        current: impl FnOnce() -> Bid,
    ) -> Option<u32> {
        let entry = self.entries.get_mut(request_id)?;
        *entry = Entry::new(entry.round + 1, current());
        Some(entry.round)
    }

    /// The current round of the request, if this node has seen it.
    pub fn round(&self, request_id: &str) -> Option<u32> {
        self.entries.get(request_id).map(|e| e.round)
    }

    /// The decided winner of the current round, if any.
    pub fn winner(&self, request_id: &str) -> Option<NodeId> {
        self.entries.get(request_id).and_then(|e| e.winner)
    }

    /// Late bids counted in the current round.
    pub fn late_bids(&self, request_id: &str) -> u32 {
        self.entries.get(request_id).map_or(0, |e| e.late_bids)
    }
}

// ---- Network part ----

/// How long to wait for the peers' bids (design doc Section 3).
/// TODO(P3): move to config as `[election] bid_timeout_ms`.
const BID_TIMEOUT: Duration = Duration::from_millis(300);

/// One election for `request`, from this node's side: fix my bid, send it to
/// every peer, store the bids that come back, decide.
///
/// A peer that fails, times out or is not reached simply has no bid in this
/// round (design doc Section 3: a missing bid means that node is out).
/// Not yet: stopping early for peers marked down (part 2), answering peers'
/// calls (part 3), following a peer into a newer round (part 4).
pub async fn run_election(state: &NodeState, request: &SubmitRequest) -> Option<NodeId> {
    let request_id = request.request_id.clone();

    // Lock, read, unlock: the table is never locked while we wait on the network.
    let (round, mine) = {
        let mut table = state.election.lock().unwrap();
        let mine = table.my_bid(&request_id, || own_bid(state));
        (table.round(&request_id).unwrap_or(0), mine)
    };

    let call = BidRequest {
        bid: Some(bid_to_pb(&request_id, round, mine)),
        request: Some(request.clone()),
    };
    let results = broadcast(state, BID_TIMEOUT, move |mut client| {
        let call = call.clone();
        async move { client.exchange_bid(call).await }
    })
    .await;

    let mut table = state.election.lock().unwrap();
    for (peer, result) in results {
        match result {
            PeerResult::Reply(reply) if reply.request_id == request_id => {
                let recorded = table.record_bid(&request_id, reply.round, bid_from_pb(&reply));
                tracing::debug!(%request_id, peer, ?recorded, "bid received");
            }
            other => tracing::debug!(%request_id, peer, ?other, "no bid from peer"),
        }
    }
    table.decide(&request_id)
}

/// My bid right now, from P3's load collector.
fn own_bid(state: &NodeState) -> Bid {
    Bid {
        node_id: state.id,
        load_score: state.load_report().load_score,
        // TODO(P3): false while the shared state (P4's DFS) cannot be reached.
        ready: true,
    }
}

fn bid_to_pb(request_id: &str, round: u32, bid: Bid) -> pb::Bid {
    pb::Bid {
        request_id: request_id.to_string(),
        node_id: bid.node_id,
        load_score: bid.load_score,
        round,
        ready: bid.ready,
    }
}

fn bid_from_pb(bid: &pb::Bid) -> Bid {
    Bid {
        node_id: bid.node_id,
        load_score: bid.load_score,
        ready: bid.ready,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bid(node_id: NodeId, load_score: f64) -> Bid {
        Bid {
            node_id,
            load_score,
            ready: true,
        }
    }

    #[test]
    fn lowest_score_wins() {
        assert_eq!(
            pick_winner(&[bid(1, 0.20), bid(2, 0.45), bid(3, 0.31)]),
            Some(1)
        );
        assert_eq!(
            pick_winner(&[bid(1, 0.50), bid(2, 0.45), bid(3, 0.31)]),
            Some(3)
        );
    }

    #[test]
    fn plan_worked_example_tie_goes_to_lowest_id() {
        // Step B plan: node 1 and node 3 both bid 0.39, node 2 bids 0.52.
        assert_eq!(
            pick_winner(&[bid(1, 0.39), bid(2, 0.52), bid(3, 0.39)]),
            Some(1)
        );
    }

    #[test]
    fn order_of_bids_does_not_matter() {
        let bids = [bid(1, 0.39), bid(2, 0.52), bid(3, 0.39)];
        let reversed = [bids[2], bids[1], bids[0]];
        let rotated = [bids[1], bids[2], bids[0]];
        assert_eq!(pick_winner(&bids), pick_winner(&reversed));
        assert_eq!(pick_winner(&bids), pick_winner(&rotated));
    }

    #[test]
    fn missing_bid_means_that_node_is_out() {
        // Node 1 is silent: the other two elect between themselves.
        assert_eq!(pick_winner(&[bid(2, 0.45), bid(3, 0.31)]), Some(3));
    }

    #[test]
    fn a_node_alone_elects_itself() {
        assert_eq!(pick_winner(&[bid(2, 0.90)]), Some(2));
    }

    #[test]
    fn not_ready_bid_cannot_win_even_with_the_lowest_score() {
        let returning = Bid {
            node_id: 1,
            load_score: 0.0,
            ready: false,
        };
        assert_eq!(
            pick_winner(&[returning, bid(2, 0.45), bid(3, 0.31)]),
            Some(3)
        );
    }

    #[test]
    fn no_ready_bid_means_no_winner() {
        assert_eq!(pick_winner(&[]), None);
        let not_ready = Bid {
            node_id: 1,
            load_score: 0.1,
            ready: false,
        };
        assert_eq!(pick_winner(&[not_ready]), None);
    }

    #[test]
    fn nan_score_loses_to_any_real_score() {
        assert_eq!(pick_winner(&[bid(1, f64::NAN), bid(2, 0.99)]), Some(2));
        assert_eq!(pick_winner(&[bid(2, 0.99), bid(1, f64::NAN)]), Some(2));
    }

    // ---- Election table. Node 2's view in every test. ----

    #[test]
    fn own_bid_is_fixed_once() {
        let mut table = ElectionTable::new();
        assert_eq!(table.my_bid("r7", || bid(2, 0.45)), bid(2, 0.45));
        // Load changed since, but the bid for r7 does not.
        assert_eq!(table.my_bid("r7", || bid(2, 0.90)), bid(2, 0.45));
    }

    #[test]
    fn worked_example_and_the_decision_never_changes() {
        let mut table = ElectionTable::new();
        table.my_bid("r7", || bid(2, 0.45));
        assert_eq!(table.record_bid("r7", 0, bid(1, 0.20)), Recorded::Stored);
        assert_eq!(table.record_bid("r7", 0, bid(3, 0.31)), Recorded::Stored);
        assert_eq!(table.decide("r7"), Some(1));

        // Node 3's second copy arrives after the decision: counted, not used.
        assert_eq!(table.record_bid("r7", 0, bid(3, 0.01)), Recorded::Late);
        assert_eq!(table.late_bids("r7"), 1);
        assert_eq!(table.decide("r7"), Some(1));
        assert_eq!(table.winner("r7"), Some(1));
    }

    #[test]
    fn first_copy_of_a_peer_bid_is_kept() {
        let mut table = ElectionTable::new();
        table.my_bid("r7", || bid(2, 0.45));
        table.record_bid("r7", 0, bid(1, 0.50));
        table.record_bid("r7", 0, bid(1, 0.10)); // a changed repeat must not count
        assert_eq!(table.decide("r7"), Some(2));
    }

    #[test]
    fn bid_for_an_unknown_request_is_not_stored() {
        let mut table = ElectionTable::new();
        assert_eq!(
            table.record_bid("r7", 0, bid(1, 0.20)),
            Recorded::UnknownRequest
        );
        assert_eq!(table.decide("r7"), None);
    }

    #[test]
    fn next_round_clears_old_bids_and_ignores_old_round_bids() {
        let mut table = ElectionTable::new();
        table.my_bid("r7", || bid(2, 0.45));
        table.record_bid("r7", 0, bid(1, 0.20));
        assert_eq!(table.decide("r7"), Some(1));

        // Node 1 failed: round 1 without it, with a fresh own bid.
        assert_eq!(table.start_next_round("r7", || bid(2, 0.60)), Some(1));
        assert_eq!(table.round("r7"), Some(1));
        assert_eq!(table.winner("r7"), None);
        assert_eq!(table.record_bid("r7", 0, bid(1, 0.20)), Recorded::OldRound);
        assert_eq!(table.record_bid("r7", 1, bid(3, 0.31)), Recorded::Stored);
        assert_eq!(table.decide("r7"), Some(3));
    }

    #[test]
    fn bid_from_a_newer_round_is_not_stored() {
        let mut table = ElectionTable::new();
        table.my_bid("r7", || bid(2, 0.45));
        assert_eq!(
            table.record_bid("r7", 1, bid(3, 0.31)),
            Recorded::NewerRound
        );
        assert_eq!(table.decide("r7"), Some(2));
    }

    #[test]
    fn requests_do_not_mix() {
        let mut table = ElectionTable::new();
        table.my_bid("r7", || bid(2, 0.45));
        table.my_bid("r8", || bid(2, 0.45));
        table.record_bid("r7", 0, bid(1, 0.20));
        assert_eq!(table.decide("r7"), Some(1));
        assert_eq!(table.decide("r8"), Some(2));
    }

    #[test]
    fn not_ready_alone_decides_nothing_yet() {
        let mut table = ElectionTable::new();
        table.my_bid("r7", || Bid {
            node_id: 2,
            load_score: 0.1,
            ready: false,
        });
        assert_eq!(table.decide("r7"), None);
        // Not fixed: a ready bid that arrives later can still be used.
        assert_eq!(table.record_bid("r7", 0, bid(3, 0.31)), Recorded::Stored);
        assert_eq!(table.decide("r7"), Some(3));
    }

    // ---- run_election over a real network (localhost) ----

    use common::config::ClusterConfig;

    use crate::peers::Peers;

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn three_node_config() -> ClusterConfig {
        ClusterConfig::parse(&format!(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 3\naddr = \"127.0.0.1:{}\"\n\
             [timing]\nconnect_timeout_ms = 200\n",
            free_port(),
            free_port(),
            free_port()
        ))
        .unwrap()
    }

    fn state_for(id: NodeId, cfg: &ClusterConfig) -> NodeState {
        let peers = Peers::new(cfg, id).unwrap();
        NodeState::new(id, cfg.clone(), peers)
    }

    fn request(request_id: &str) -> SubmitRequest {
        SubmitRequest {
            request_id: request_id.to_string(),
            kind: None,
        }
    }

    #[tokio::test]
    async fn with_no_peer_reachable_a_node_elects_itself() {
        let cfg = three_node_config(); // nodes 2 and 3 are not running
        let node1 = state_for(1, &cfg);
        assert_eq!(run_election(&node1, &request("r7")).await, Some(1));
        // The decision is stored in the table.
        assert_eq!(node1.election.lock().unwrap().winner("r7"), Some(1));
    }
}
