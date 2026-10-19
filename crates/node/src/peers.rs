//! One connection per peer, set up at startup and shared by every task.
//!
//! - Lazy: nothing is dialed until the first call, so nodes can start in any order.
//! - connect_timeout: dialing an unreachable machine fails fast instead of waiting
//!   for the OS TCP timeout (~21 s on Windows).
//! - No per-call deadline here: callers wrap each call in `tokio::time::timeout`
//!   (heartbeat loop, broadcast). That is what notices a muted peer, which keeps its
//!   connection open but never answers.
//! - Deliberately not used: tonic load balancing, retry layers, HTTP/2 keepalive.
//!   Election, our retry policy and our heartbeats do those jobs.
//! - The channel reconnects by itself after a peer comes back; whether a peer is
//!   alive is decided only by heartbeats and the failure detector, never by this.

use std::collections::BTreeMap;

use common::NodeId;
use common::config::ClusterConfig;
use common::pb::node_client::NodeClient;
use tonic::transport::{Channel, Endpoint};

pub struct Peers {
    /// Sorted by node ID, so iteration order is always the same.
    clients: BTreeMap<NodeId, NodeClient<Channel>>,
}

impl Peers {
    /// Prepares a client for every node except `me`. Dials nothing yet.
    /// Must be called inside the tokio runtime (each channel starts a small background task).
    pub fn new(cfg: &ClusterConfig, me: NodeId) -> anyhow::Result<Self> {
        let mut clients = BTreeMap::new();
        for p in cfg.peers(me) {
            let endpoint = Endpoint::from_shared(format!("http://{}", p.addr))?
                .connect_timeout(cfg.timing.connect_timeout());
            clients.insert(p.id, NodeClient::new(endpoint.connect_lazy()));
        }
        Ok(Peers { clients })
    }

    /// A client for `peer` (a cheap clone sharing the one connection), or None if unknown.
    #[allow(dead_code)] // first used by the heartbeat loop (task 5)
    pub fn client(&self, peer: NodeId) -> Option<NodeClient<Channel>> {
        self.clients.get(&peer).cloned()
    }

    /// Peer IDs, smallest first.
    pub fn ids(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.clients.keys().copied()
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use common::pb::HeartbeatRequest;

    use super::*;

    /// Three nodes on ports where nothing is listening.
    fn config_with_no_servers() -> ClusterConfig {
        // Bind and immediately release ports, so we know they are free (nothing listens).
        let free_port = || {
            std::net::TcpListener::bind("127.0.0.1:0")
                .unwrap()
                .local_addr()
                .unwrap()
                .port()
        };
        let text = format!(
            "[[nodes]]\nid = 1\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 2\naddr = \"127.0.0.1:{}\"\n\
             [[nodes]]\nid = 3\naddr = \"127.0.0.1:{}\"\n\
             [timing]\nconnect_timeout_ms = 300\n",
            free_port(),
            free_port(),
            free_port()
        );
        ClusterConfig::parse(&text).unwrap()
    }

    #[tokio::test]
    async fn builds_without_any_server_running() {
        let peers = Peers::new(&config_with_no_servers(), 2).unwrap();
        assert_eq!(peers.ids().collect::<Vec<_>>(), vec![1, 3]);
        assert!(peers.client(1).is_some());
        assert!(peers.client(2).is_none()); // ourselves
        assert!(peers.client(9).is_none()); // unknown
    }

    #[tokio::test]
    async fn call_to_missing_peer_fails_fast() {
        let peers = Peers::new(&config_with_no_servers(), 2).unwrap();
        let mut c = peers.client(1).unwrap();
        let start = Instant::now();
        let result = tokio::time::timeout(
            Duration::from_secs(5),
            c.heartbeat(HeartbeatRequest { from_node: 2, seq: 1 }),
        )
        .await;
        // Ok(..) = the call itself finished (not our 5 s guard); Err inside = it failed.
        assert!(matches!(result, Ok(Err(_))), "expected a quick error, got {result:?}");
        assert!(start.elapsed() < Duration::from_secs(2));
    }
}
