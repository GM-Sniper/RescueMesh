//! P3: the election rule (docs/p3-election-design.md, Section 1).
//!
//! For each request, every live node bids its load score. Every node applies the
//! same rule to the bids it holds: lowest `load_score` wins, ties go to the lowest
//! `node_id`. Nodes that see the same bids therefore pick the same winner, with no
//! coordinator.
//!
//! This file is pure logic, like the failure detector: no clock, no network.

use std::cmp::Ordering;

use common::NodeId;

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
}
