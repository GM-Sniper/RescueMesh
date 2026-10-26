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
use common::pb::{self, BidRequest, Coordinator, CoordinatorAck, SubmitRequest};
use tonic::Status;

use crate::broadcast::{PeerResult, broadcast_until};
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
    /// the caller follows it into that round with `join_round`, then records again.
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
    /// Announcements from a winner other than ours (design doc Section 5).
    splits: u32,
    /// We believed we won, but a lower node ID announced too: we do not plan.
    stepped_aside: bool,
}

impl Entry {
    fn new(round: u32, my_bid: Bid) -> Self {
        Entry {
            round,
            my_bid,
            bids: BTreeMap::new(),
            winner: None,
            late_bids: 0,
            splits: 0,
            stepped_aside: false,
        }
    }
}

/// What a peer's winner announcement meant to us (design doc Section 5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Announced {
    /// We decided the same winner.
    Agrees,
    /// We have not decided this round yet; our own decision still counts.
    BeforeDecision,
    /// We decided a different winner: a split decision. `step_aside` is true when
    /// we believed we won and the announcer has a lower node ID, so it keeps the
    /// request and we do not plan.
    Split { step_aside: bool },
    /// For an earlier round: ignored.
    OldRound,
    /// For a later round than ours: ignored (we join rounds through bids).
    NewerRound,
    /// We have no election for this request: ignored.
    UnknownRequest,
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

    /// Follows a peer into a newer round of the request: the peer saw the winner
    /// fail before we did. Like Raft's term (design doc Section 1), a newer round
    /// replaces ours. Fresh own bid from `current`, old bids and winner cleared.
    /// Returns the new own bid, or `None` if the request is unknown or `round`
    /// is not newer than ours.
    pub fn join_round(
        &mut self,
        request_id: &str,
        round: u32,
        current: impl FnOnce() -> Bid,
    ) -> Option<Bid> {
        let entry = self.entries.get_mut(request_id)?;
        if round <= entry.round {
            return None;
        }
        *entry = Entry::new(round, current());
        Some(entry.my_bid)
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

    /// A peer announced it won `round` of the request. Compares it with our own
    /// decision and counts a split if they differ. Between two nodes that both
    /// believe they won, the lower node ID keeps the request: every node applies
    /// the same rule, so the split settles without more messages.
    pub fn note_announcement(&mut self, request_id: &str, round: u32, from: NodeId) -> Announced {
        let Some(entry) = self.entries.get_mut(request_id) else {
            return Announced::UnknownRequest;
        };
        if round < entry.round {
            return Announced::OldRound;
        }
        if round > entry.round {
            return Announced::NewerRound;
        }
        match entry.winner {
            None => Announced::BeforeDecision,
            Some(winner) if winner == from => Announced::Agrees,
            Some(winner) => {
                entry.splits += 1;
                let me = entry.my_bid.node_id;
                let step_aside = winner == me && from < me;
                if step_aside {
                    entry.stepped_aside = true;
                }
                Announced::Split { step_aside }
            }
        }
    }

    /// Split decisions seen in the current round.
    pub fn splits(&self, request_id: &str) -> u32 {
        self.entries.get(request_id).map_or(0, |e| e.splits)
    }

    /// True if we stepped aside in the current round and must not plan.
    pub fn stepped_aside(&self, request_id: &str) -> bool {
        self.entries
            .get(request_id)
            .is_some_and(|e| e.stepped_aside)
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
/// Every peer gets our bid, but we only wait for the peers the failure detector
/// reports alive, so a peer already marked down does not cost the full timeout.
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
    let alive = state.alive_peers();
    let results = broadcast_until(state, BID_TIMEOUT, &alive, move |mut client| {
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

/// A peer's `ExchangeBid` call (design doc step 4): store its bid, reply with mine.
///
/// If this is the first we hear of the request (the client's copy is late or
/// lost), this is where our bid gets fixed; the client's copy later reuses it.
pub fn handle_exchange_bid(state: &NodeState, call: BidRequest) -> Result<pb::Bid, Status> {
    let Some(theirs) = call.bid else {
        return Err(Status::invalid_argument("bid is missing"));
    };
    let Some(request) = call.request else {
        return Err(Status::invalid_argument("request is missing"));
    };
    if theirs.request_id.is_empty() || theirs.request_id != request.request_id {
        return Err(Status::invalid_argument(
            "bid and request must carry the same non-empty request_id",
        ));
    }
    let request_id = theirs.request_id.clone();

    let mut table = state.election.lock().unwrap();
    // TODO(P3, bid hook with P1): if we had never seen this request, or we just
    // joined a newer round below, also start our own election here. Otherwise, when
    // we have the lowest bid, the peers decide we won but we never plan, and the
    // client has to time out and retry.
    let mut mine = table.my_bid(&request_id, || own_bid(state));
    let mut recorded = table.record_bid(&request_id, theirs.round, bid_from_pb(&theirs));
    if recorded == Recorded::NewerRound {
        // The peer saw the winner fail before we did: follow it (part 4, option A).
        if let Some(fresh) = table.join_round(&request_id, theirs.round, || own_bid(state)) {
            mine = fresh;
            recorded = table.record_bid(&request_id, theirs.round, bid_from_pb(&theirs));
        }
    }
    tracing::debug!(%request_id, from = theirs.node_id, ?recorded, "bid call received");
    let round = table.round(&request_id).unwrap_or(0);
    Ok(bid_to_pb(&request_id, round, mine))
}

/// The winner tells its peers it is handling `round` of the request (the Bully
/// announcement, design doc step 6). Waits only for alive peers; the acks carry
/// nothing we need, so failures are only logged.
pub async fn announce_winner(state: &NodeState, request_id: &str, round: u32) {
    let announcement = Coordinator {
        request_id: request_id.to_string(),
        round,
        node_id: state.id,
    };
    let alive = state.alive_peers();
    let results = broadcast_until(state, BID_TIMEOUT, &alive, move |mut client| {
        let announcement = announcement.clone();
        async move { client.announce_winner(announcement).await }
    })
    .await;
    for (peer, result) in results {
        if !matches!(result, PeerResult::Reply(_)) {
            tracing::debug!(%request_id, peer, ?result, "announcement not acknowledged");
        }
    }
}

/// A peer's `AnnounceWinner` call: compare it with our decision, reply with an ack.
pub fn handle_announce_winner(
    state: &NodeState,
    announcement: Coordinator,
) -> Result<CoordinatorAck, Status> {
    if announcement.request_id.is_empty() || announcement.node_id == 0 {
        return Err(Status::invalid_argument(
            "announcement needs a request_id and a winner node_id",
        ));
    }
    let request_id = &announcement.request_id;
    let announced = state.election.lock().unwrap().note_announcement(
        request_id,
        announcement.round,
        announcement.node_id,
    );
    match announced {
        Announced::Split { step_aside } => tracing::warn!(
            %request_id,
            round = announcement.round,
            from = announcement.node_id,
            step_aside,
            "split_decision"
        ),
        other => {
            tracing::debug!(%request_id, from = announcement.node_id, ?other, "announcement received")
        }
    }
    Ok(CoordinatorAck { node_id: state.id })
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
    fn joining_a_newer_round_clears_old_bids_and_ignores_older_rounds() {
        let mut table = ElectionTable::new();
        table.my_bid("r7", || bid(2, 0.45));
        table.record_bid("r7", 0, bid(1, 0.20));
        assert_eq!(table.decide("r7"), Some(1));

        // Node 3 already re-elected without node 1: follow it into round 1.
        assert_eq!(
            table.join_round("r7", 1, || bid(2, 0.60)),
            Some(bid(2, 0.60))
        );
        assert_eq!((table.round("r7"), table.winner("r7")), (Some(1), None));
        assert_eq!(table.record_bid("r7", 1, bid(3, 0.31)), Recorded::Stored);
        assert_eq!(table.record_bid("r7", 0, bid(1, 0.20)), Recorded::OldRound);
        assert_eq!(table.decide("r7"), Some(3));

        // Not newer, or unknown request: nothing changes.
        assert_eq!(table.join_round("r7", 1, || bid(2, 0.99)), None);
        assert_eq!(table.join_round("r7", 0, || bid(2, 0.99)), None);
        assert_eq!(table.join_round("r8", 1, || bid(2, 0.99)), None);
        assert_eq!(table.winner("r7"), Some(3));
    }

    // ---- Winner announcements (node 2's view) ----

    /// Node 2's table for r7 after deciding `winner`: our bid is 0.45, plus one
    /// peer bid that either beats it (that peer wins) or loses to it (we win).
    fn decided(winner: NodeId) -> ElectionTable {
        let mut table = ElectionTable::new();
        table.my_bid("r7", || bid(2, 0.45));
        let peer_bid = if winner == 2 {
            bid(1, 0.90)
        } else {
            bid(winner, 0.10)
        };
        table.record_bid("r7", 0, peer_bid);
        assert_eq!(table.decide("r7"), Some(winner));
        table
    }

    #[test]
    fn announcement_from_our_winner_agrees() {
        let mut table = decided(1);
        assert_eq!(table.note_announcement("r7", 0, 1), Announced::Agrees);
        assert_eq!(table.splits("r7"), 0);
    }

    #[test]
    fn announcement_before_we_decide_changes_nothing() {
        let mut table = ElectionTable::new();
        table.my_bid("r7", || bid(2, 0.45));
        assert_eq!(
            table.note_announcement("r7", 0, 1),
            Announced::BeforeDecision
        );
        assert_eq!(table.decide("r7"), Some(2)); // our own decision still counts
        assert_eq!(table.splits("r7"), 0);
    }

    #[test]
    fn split_with_a_lower_id_announcer_we_step_aside() {
        let mut table = decided(2); // we believe we won
        assert_eq!(
            table.note_announcement("r7", 0, 1),
            Announced::Split { step_aside: true }
        );
        assert_eq!(table.splits("r7"), 1);
        assert!(table.stepped_aside("r7"));
    }

    #[test]
    fn split_with_a_higher_id_announcer_we_keep_going() {
        let mut table = decided(2);
        assert_eq!(
            table.note_announcement("r7", 0, 3),
            Announced::Split { step_aside: false }
        );
        assert_eq!(table.splits("r7"), 1);
        assert!(!table.stepped_aside("r7"));

        // We decided node 1, node 3 announces: a split, but we were not planning.
        let mut table = decided(1);
        assert_eq!(
            table.note_announcement("r7", 0, 3),
            Announced::Split { step_aside: false }
        );
        assert_eq!(table.splits("r7"), 1);
    }

    #[test]
    fn announcement_for_another_round_or_request_is_ignored() {
        let mut table = decided(2);
        table.start_next_round("r7", || bid(2, 0.45));
        assert_eq!(table.note_announcement("r7", 0, 1), Announced::OldRound);
        assert_eq!(table.note_announcement("r7", 2, 1), Announced::NewerRound);
        assert_eq!(
            table.note_announcement("r8", 0, 1),
            Announced::UnknownRequest
        );
        assert_eq!((table.splits("r7"), table.stepped_aside("r7")), (0, false));
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

    use std::sync::Arc;

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

    fn three_node_config() -> ClusterConfig {
        ClusterConfig::parse(&format!(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 3\naddr = \"127.0.0.1:{}\"\n\
             [timing]\nconnect_timeout_ms = 200\n\
             heartbeat_interval_ms = 50\nsuspect_after_ms = 100\n",
            free_port(),
            free_port(),
            free_port()
        ))
        .unwrap()
    }

    fn state_for(id: NodeId, cfg: &ClusterConfig) -> Arc<NodeState> {
        let peers = Peers::new(cfg, id).unwrap();
        Arc::new(NodeState::new(id, cfg.clone(), peers))
    }

    /// A real server for node `id`, in the background.
    async fn serve(id: NodeId, cfg: &ClusterConfig) -> Arc<NodeState> {
        let state = state_for(id, cfg);
        let addr = cfg.node(id).unwrap().addr.parse().unwrap();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(NodeServer::new(NodeService::new(state.clone())))
                .serve(addr),
        );
        tokio::time::sleep(Duration::from_millis(50)).await; // let it start listening
        state
    }

    /// The pb bid that `from` would send for `request_id`, round 0.
    fn call(request_id: &str, from: NodeId, load_score: f64) -> BidRequest {
        BidRequest {
            bid: Some(bid_to_pb(request_id, 0, bid(from, load_score))),
            request: Some(request(request_id)),
        }
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

    #[tokio::test]
    async fn three_nodes_agree_on_one_winner() {
        let cfg = three_node_config();
        let (n1, n2, n3) = (
            serve(1, &cfg).await,
            serve(2, &cfg).await,
            serve(3, &cfg).await,
        );
        let r = request("r7");
        let winners = tokio::join!(
            run_election(&n1, &r),
            run_election(&n2, &r),
            run_election(&n3, &r)
        );
        assert!(winners.0.is_some());
        assert_eq!(winners.0, winners.1);
        assert_eq!(winners.0, winners.2);
    }

    #[tokio::test]
    async fn least_loaded_node_wins() {
        let cfg = three_node_config();
        let (n1, n2, n3) = (
            serve(1, &cfg).await,
            serve(2, &cfg).await,
            serve(3, &cfg).await,
        );
        // Requests in progress raise the queue part of the score. Node 3 stays idle.
        let _busy1 = [n1.start_request(), n1.start_request()];
        let _busy2 = n2.start_request();
        let r = request("r7");
        let winners = tokio::join!(
            run_election(&n1, &r),
            run_election(&n2, &r),
            run_election(&n3, &r)
        );
        assert_eq!(winners, (Some(3), Some(3), Some(3)));
    }

    #[tokio::test]
    async fn muted_node_is_left_out() {
        let cfg = three_node_config();
        let (n1, n2, n3) = (
            serve(1, &cfg).await,
            serve(2, &cfg).await,
            serve(3, &cfg).await,
        );
        // Node 3 would win (idle), but it is muted.
        let _busy1 = [n1.start_request(), n1.start_request()];
        let _busy2 = n2.start_request();
        n3.mute_for(Duration::from_secs(5));
        let r = request("r7");
        let winners = tokio::join!(run_election(&n1, &r), run_election(&n2, &r));
        assert_eq!(winners, (Some(2), Some(2)));
    }

    // ---- Part 2: waiting only for peers the detector reports alive ----

    #[tokio::test]
    async fn peer_marked_down_costs_no_wait() {
        let cfg = three_node_config();
        serve(2, &cfg).await;
        serve(3, &cfg).await.mute_for(Duration::from_secs(5));
        let n1 = state_for(1, &cfg);
        // Let node 3's silence pass suspect_after (100 ms here); node 2 was heard.
        tokio::time::sleep(Duration::from_millis(150)).await;
        n1.heard_from(2);
        n1.check_peers();
        assert_eq!(n1.alive_peers(), vec![2]);

        let start = std::time::Instant::now();
        assert_eq!(run_election(&n1, &request("r7")).await, Some(1));
        let took = start.elapsed();
        assert!(took < BID_TIMEOUT / 2, "took {took:?}");
    }

    #[tokio::test]
    async fn muted_peer_not_yet_marked_down_costs_the_full_timeout() {
        let cfg = three_node_config();
        serve(2, &cfg).await;
        serve(3, &cfg).await.mute_for(Duration::from_secs(5));
        let n1 = state_for(1, &cfg);
        assert_eq!(n1.alive_peers(), vec![2, 3]); // the detector has not noticed yet

        let start = std::time::Instant::now();
        assert_eq!(run_election(&n1, &request("r7")).await, Some(1));
        let took = start.elapsed();
        assert!(
            took >= BID_TIMEOUT && took < BID_TIMEOUT + Duration::from_millis(150),
            "took {took:?}"
        );
    }

    #[tokio::test]
    async fn peer_call_first_fixes_our_bid_for_the_client_copy() {
        let cfg = three_node_config();
        let n2 = state_for(2, &cfg);
        // Node 1's call arrives before the client's copy: node 2 fixes its bid now.
        let reply = handle_exchange_bid(&n2, call("r7", 1, 0.20)).unwrap();
        assert_eq!((reply.node_id, reply.round), (2, 0));

        // Node 2 gets busier, then node 3's call arrives: same bid as before.
        let _busy = n2.start_request();
        let again = handle_exchange_bid(&n2, call("r7", 3, 0.31)).unwrap();
        assert_eq!(again.load_score, reply.load_score);
        // Node 2 was idle when its bid was fixed (score 0.0), so it beats 0.20 and 0.31.
        assert_eq!(n2.election.lock().unwrap().decide("r7"), Some(2));
    }

    #[tokio::test]
    async fn peer_call_from_a_newer_round_makes_us_join_it() {
        let cfg = three_node_config();
        let n2 = state_for(2, &cfg);
        handle_exchange_bid(&n2, call("r7", 1, 0.20)).unwrap(); // round 0

        // Node 3 saw node 1 fail first and calls with its round-1 bid.
        let mut round1 = call("r7", 3, 0.31);
        round1.bid.as_mut().unwrap().round = 1;
        let reply = handle_exchange_bid(&n2, round1).unwrap();
        assert_eq!((reply.node_id, reply.round), (2, 1));

        let mut table = n2.election.lock().unwrap();
        assert_eq!(table.round("r7"), Some(1));
        // Node 1's round-0 bid no longer counts.
        assert_eq!(table.record_bid("r7", 0, bid(1, 0.20)), Recorded::OldRound);
    }

    #[tokio::test]
    async fn bad_bid_calls_are_rejected() {
        let cfg = three_node_config();
        let n2 = state_for(2, &cfg);
        let mut mismatched = call("r7", 1, 0.20);
        mismatched.request = Some(request("r8"));
        let mut no_bid = call("r7", 1, 0.20);
        no_bid.bid = None;
        let mut no_request = call("r7", 1, 0.20);
        no_request.request = None;
        for bad in [mismatched, no_bid, no_request, call("", 1, 0.20)] {
            let err = handle_exchange_bid(&n2, bad).unwrap_err();
            assert_eq!(
                err.code(),
                tonic::Code::InvalidArgument,
                "{}",
                err.message()
            );
        }
        // Nothing was stored for any of them.
        assert_eq!(n2.election.lock().unwrap().round("r7"), None);
    }

    #[tokio::test]
    async fn announcement_reaches_peers_and_exposes_a_split() {
        let cfg = three_node_config();
        let (n2, n3) = (serve(2, &cfg).await, serve(3, &cfg).await);
        let n1 = state_for(1, &cfg);
        {
            // Node 2 missed node 1's bid and picked itself; node 3 picked node 1.
            let mut t2 = n2.election.lock().unwrap();
            t2.my_bid("r7", || bid(2, 0.30));
            assert_eq!(t2.decide("r7"), Some(2));
            let mut t3 = n3.election.lock().unwrap();
            t3.my_bid("r7", || bid(3, 0.50));
            t3.record_bid("r7", 0, bid(1, 0.10));
            assert_eq!(t3.decide("r7"), Some(1));
        }

        announce_winner(&n1, "r7", 0).await;

        let t2 = n2.election.lock().unwrap();
        assert_eq!((t2.splits("r7"), t2.stepped_aside("r7")), (1, true));
        let t3 = n3.election.lock().unwrap();
        assert_eq!((t3.splits("r7"), t3.stepped_aside("r7")), (0, false));
    }

    #[tokio::test]
    async fn bad_announcements_are_rejected() {
        let cfg = three_node_config();
        let n2 = state_for(2, &cfg);
        for (request_id, node_id) in [("", 1), ("r7", 0)] {
            let bad = Coordinator {
                request_id: request_id.to_string(),
                round: 0,
                node_id,
            };
            let err = handle_announce_winner(&n2, bad).unwrap_err();
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
        }
    }
}
