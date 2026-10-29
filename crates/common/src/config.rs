//! Cluster configuration, read from `config/nodes.toml` (localhost) or
//! `config/nodes.lab.toml` (the 3 real machines). Every program reads the same file:
//! a node finds itself by `--id` and treats the others as peers; a client reads
//! the node addresses.

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail};
use serde::Deserialize;

use crate::NodeId;
use crate::domain::Weights;
use crate::load::LoadConfig;

/// The whole config file.
#[derive(Debug, Clone, Deserialize)]
pub struct ClusterConfig {
    /// The cloud nodes. Clients are not listed: they are not peers.
    pub nodes: Vec<NodeEntry>,
    /// Intervals and timeouts. Optional in the file; missing values use the defaults.
    #[serde(default)]
    pub timing: Timing,
    /// Planner scoring weights (P2). Optional; missing values use the defaults.
    #[serde(default)]
    pub weights: Weights,
    /// Load score inputs and weights (P3). Optional; missing values use the defaults.
    #[serde(default)]
    pub load: LoadConfig,
    /// Planner workload settings (P2). Optional; missing values use defaults.
    #[serde(default)]
    pub planner: PlannerConfig,
    /// Automatic fault injection (P5). Optional; off unless `enabled = true`.
    #[serde(default)]
    pub faults: Faults,
}

/// One `[[nodes]]` entry.
#[derive(Debug, Clone, Deserialize)]
pub struct NodeEntry {
    /// Starts at 1; 0 is reserved for clients.
    pub id: NodeId,
    /// Address that peers and clients dial, e.g. "127.0.0.1:50051" or "10.0.0.11:50051".
    pub addr: String,
}

/// The `[timing]` table. In the file so stress tests can try other values without rebuilding.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct Timing {
    /// How often each node sends a heartbeat to each peer.
    pub heartbeat_interval_ms: u64,
    /// Silence after which a peer is suspected down (P5's detector).
    pub suspect_after_ms: u64,
    /// Longest we wait for any reply from a peer.
    pub rpc_deadline_ms: u64,
    /// Longest we wait to open a connection to a peer.
    pub connect_timeout_ms: u64,
}

impl Default for Timing {
    fn default() -> Self {
        Timing {
            heartbeat_interval_ms: 500,
            suspect_after_ms: 1500,
            rpc_deadline_ms: 1000,
            connect_timeout_ms: 500,
        }
    }
}

impl Timing {
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval_ms)
    }
    pub fn suspect_after(&self) -> Duration {
        Duration::from_millis(self.suspect_after_ms)
    }
    pub fn rpc_deadline(&self) -> Duration {
        Duration::from_millis(self.rpc_deadline_ms)
    }
    pub fn connect_timeout(&self) -> Duration {
        Duration::from_millis(self.connect_timeout_ms)
    }
}

/// The `[faults]` table (P5): automatic fault injection. Off by default, so runs
/// without the table behave exactly as before. Design: `docs/p5-auto-injector-design.md`.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct Faults {
    /// true = the nodes silence one of themselves on a random schedule.
    pub enabled: bool,
    /// Same seed = same victims, durations and gaps, so a run can be repeated.
    pub seed: u64,
    /// Shortest and longest silence. The spec caps it at 20 s.
    pub min_duration_ms: u64,
    pub max_duration_ms: u64,
    /// Wait between faults, counted from when every node is alive again.
    pub min_gap_ms: u64,
    pub max_gap_ms: u64,
}

impl Default for Faults {
    fn default() -> Self {
        Faults {
            enabled: false,
            seed: 42,
            min_duration_ms: 2_000,
            max_duration_ms: 20_000,
            min_gap_ms: 10_000,
            max_gap_ms: 30_000,
        }
    }
}

impl Faults {
    fn check(&self) -> anyhow::Result<()> {
        if self.min_duration_ms == 0 || self.min_duration_ms > self.max_duration_ms {
            bail!("faults: need 0 < min_duration_ms <= max_duration_ms");
        }
        if self.max_duration_ms > 20_000 {
            bail!("faults.max_duration_ms is over 20000 (the spec allows up to 20 s)");
        }
        if self.min_gap_ms == 0 || self.min_gap_ms > self.max_gap_ms {
            bail!("faults: need 0 < min_gap_ms <= max_gap_ms");
        }
        Ok(())
    }
}

impl ClusterConfig {
    /// Reads and checks the file. A mistake fails here, at startup, with a clear message.
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("cannot read config file {}", path.display()))?;
        Self::parse(&text).with_context(|| format!("bad config file {}", path.display()))
    }

    /// Parses and checks config text (separate from `load` so tests need no file).
    pub fn parse(text: &str) -> anyhow::Result<Self> {
        let cfg: ClusterConfig = toml::from_str(text)?;
        cfg.check()?;
        Ok(cfg)
    }

    fn check(&self) -> anyhow::Result<()> {
        if self.nodes.is_empty() {
            bail!("no [[nodes]] listed");
        }
        let mut seen = HashSet::new();
        for n in &self.nodes {
            if n.id == 0 {
                bail!("node id 0 is reserved for clients; ids start at 1");
            }
            if !seen.insert(n.id) {
                bail!("node id {} is listed twice", n.id);
            }
            n.addr
                .parse::<SocketAddr>()
                .with_context(|| format!("node {}: addr {:?} is not ip:port", n.id, n.addr))?;
        }

        if self.timing.suspect_after_ms <= self.timing.heartbeat_interval_ms {
            bail!("timing.suspect_after_ms must be longer than heartbeat_interval_ms");
        }
        self.load.check()?;

        if self.planner.workload_incidents == 0
            || self.planner.workload_resources == 0
            || self.planner.iterations == 0
        {
            bail!("planner workload settings must all be greater than zero");
        }

        self.faults.check()?;
        Ok(())
    }

    /// The entry for node `id`, if it is listed.
    pub fn node(&self, id: NodeId) -> Option<&NodeEntry> {
        self.nodes.iter().find(|n| n.id == id)
    }

    /// Every node except `me`.
    pub fn peers(&self, me: NodeId) -> impl Iterator<Item = &NodeEntry> {
        self.nodes.iter().filter(move |n| n.id != me)
    }
}

/// Controls the extra deterministic work performed by the planner.
#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(default)]
pub struct PlannerConfig {
    /// Number of incidents represented in the workload model.
    pub workload_incidents: usize,

    /// Number of resources represented in the workload model.
    pub workload_resources: usize,

    /// Number of times to process the incident-resource pairs.
    pub iterations: usize,
}

impl Default for PlannerConfig {
    fn default() -> Self {
        Self {
            workload_incidents: 10,
            workload_resources: 20,
            iterations: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const THREE_NODES: &str = r#"
        [[nodes]]
        id = 1
        addr = "127.0.0.1:50051"
        [[nodes]]
        id = 2
        addr = "127.0.0.1:50052"
        [[nodes]]
        id = 3
        addr = "127.0.0.1:50053"
    "#;

    #[test]
    fn faults_are_off_by_default_and_checked() {
        let cfg = ClusterConfig::parse(THREE_NODES).unwrap();
        assert!(!cfg.faults.enabled);
        let on = format!("{THREE_NODES}\n[faults]\nenabled = true\nseed = 7\n");
        let cfg = ClusterConfig::parse(&on).unwrap();
        assert!(cfg.faults.enabled);
        assert_eq!(cfg.faults.seed, 7);
        assert_eq!(cfg.faults.max_duration_ms, 20_000); // other values keep their defaults
        for bad in [
            "max_duration_ms = 25000",
            "min_duration_ms = 0",
            "min_duration_ms = 5000\nmax_duration_ms = 3000",
            "min_gap_ms = 0",
        ] {
            let text = format!("{THREE_NODES}\n[faults]\n{bad}\n");
            assert!(ClusterConfig::parse(&text).is_err(), "should refuse: {bad}");
        }
    }

    #[test]
    fn minimal_file_uses_defaults() {
        let cfg = ClusterConfig::parse(THREE_NODES).unwrap();
        assert_eq!(cfg.timing.heartbeat_interval_ms, 500);
        assert_eq!(cfg.weights, Weights::default());
        let peers: Vec<NodeId> = cfg.peers(2).map(|n| n.id).collect();
        assert_eq!(peers, vec![1, 3]);
        assert!(cfg.node(4).is_none());
    }

    #[test]
    fn partial_tables_keep_other_defaults() {
        let text =
            format!("{THREE_NODES}\n[timing]\nrpc_deadline_ms = 300\n[weights]\nage = 0.9\n");
        let cfg = ClusterConfig::parse(&text).unwrap();
        assert_eq!(cfg.timing.rpc_deadline_ms, 300);
        assert_eq!(cfg.timing.heartbeat_interval_ms, 500);
        assert_eq!(cfg.weights.age, 0.9);
        assert_eq!(cfg.weights.severity, 0.6);
    }

    #[test]
    fn shipped_localhost_file_is_valid() {
        let path = concat!(env!("CARGO_MANIFEST_DIR"), "/../../config/nodes.toml");
        let cfg = ClusterConfig::load(path).unwrap();
        assert_eq!(cfg.nodes.len(), 3);
    }

    #[test]
    fn rejects_mistakes() {
        let dup = THREE_NODES.replace("id = 3", "id = 2");
        assert!(ClusterConfig::parse(&dup).is_err());
        let zero = THREE_NODES.replace("id = 1", "id = 0");
        assert!(ClusterConfig::parse(&zero).is_err());
        let bad_addr = THREE_NODES.replace("127.0.0.1:50053", "localhost");
        assert!(ClusterConfig::parse(&bad_addr).is_err());
        let bad_timing = format!("{THREE_NODES}\n[timing]\nsuspect_after_ms = 400\n");
        assert!(ClusterConfig::parse(&bad_timing).is_err());
        let bad_load = format!("{THREE_NODES}\n[load]\nqueue_max = 0\n");
        assert!(ClusterConfig::parse(&bad_load).is_err());
    }
}
