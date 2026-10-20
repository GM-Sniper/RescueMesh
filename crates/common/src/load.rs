//! P3: the load score a node bids with in Step B's election (lowest score wins).
//! Only the pure parts live here (config, formula, report), so any program can use
//! them; the node samples the live inputs in `node/src/load.rs`.
//!
//! load_score = (wq × queue + wc × cpu + ws × service) / (wq + wc + ws),
//! each input first normalized to 0-1. Dividing by the weight sum keeps the score
//! in 0-1 even if someone edits the weights so they no longer add up to 1.

use anyhow::bail;
use serde::Deserialize;

/// The `[load]` table in the config file. Missing values use the defaults
/// (the Step A plan's worked example).
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(default)]
pub struct LoadConfig {
    /// How often CPU is sampled. sysinfo needs at least 200 ms between two CPU readings.
    pub sample_interval_ms: u64,
    pub queue_weight: f64,
    pub cpu_weight: f64,
    pub service_weight: f64,
    /// Queue length that counts as fully loaded (normalized to 1.0); longer queues are capped.
    pub queue_max: u32,
    /// Service time that counts as fully loaded; slower is capped at 1.0.
    pub service_cap_ms: f64,
    /// Weight of the newest request in the service-time average (EWMA), 0 < alpha <= 1.
    /// 0.2: the average mostly follows the last ~5 requests.
    pub service_ewma_alpha: f64,
}

impl Default for LoadConfig {
    fn default() -> Self {
        LoadConfig {
            sample_interval_ms: 200,
            queue_weight: 0.5,
            cpu_weight: 0.3,
            service_weight: 0.2,
            queue_max: 10,
            service_cap_ms: 100.0,
            service_ewma_alpha: 0.2,
        }
    }
}

/// One node's load at one moment. P6's node_metrics log writes these fields.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct LoadReport {
    /// Requests this node is working on right now.
    pub queue_len: usize,
    /// Whole-machine CPU use, 0-100.
    pub cpu_pct: f64,
    /// Average time per request (EWMA), in ms. 0 before the first request.
    pub service_ms: f64,
    /// The number sent in a `Bid`: 0 = idle, 1 = fully loaded.
    pub load_score: f64,
}

impl LoadConfig {
    /// Combines the raw inputs into a report with its score.
    pub fn report(&self, queue_len: usize, cpu_pct: f64, service_ms: f64) -> LoadReport {
        let queue = clamp01(queue_len as f64 / self.queue_max as f64);
        let cpu = clamp01(cpu_pct / 100.0);
        let service = clamp01(service_ms / self.service_cap_ms);
        let weights = self.queue_weight + self.cpu_weight + self.service_weight;
        let load_score =
            (self.queue_weight * queue + self.cpu_weight * cpu + self.service_weight * service)
                / weights;
        LoadReport {
            queue_len,
            cpu_pct,
            service_ms,
            load_score,
        }
    }

    /// Called by `ClusterConfig::check`, so a bad `[load]` table fails at startup.
    pub fn check(&self) -> anyhow::Result<()> {
        let weights = [self.queue_weight, self.cpu_weight, self.service_weight];
        if weights.iter().any(|w| !w.is_finite() || *w < 0.0) {
            bail!("load weights must be zero or positive numbers");
        }
        if weights.iter().sum::<f64>() <= 0.0 {
            bail!("at least one load weight must be above 0");
        }
        if self.queue_max == 0 {
            bail!("load.queue_max must be at least 1");
        }
        if !(self.service_cap_ms > 0.0) {
            bail!("load.service_cap_ms must be above 0");
        }
        if !(self.service_ewma_alpha > 0.0 && self.service_ewma_alpha <= 1.0) {
            bail!("load.service_ewma_alpha must be in (0, 1]");
        }
        if self.sample_interval_ms == 0 {
            bail!("load.sample_interval_ms must be above 0");
        }
        Ok(())
    }
}

/// Also maps NaN to 0, so one bad reading cannot make the score NaN
/// (a NaN bid would compare unequal to everything and break the election).
fn clamp01(x: f64) -> f64 {
    if x.is_nan() { 0.0 } else { x.clamp(0.0, 1.0) }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn plan_worked_example_gives_0_39() {
        // Queue 4 of 10 -> 0.4, CPU 50% -> 0.5, service 20 of 100 ms -> 0.2.
        let r = LoadConfig::default().report(4, 50.0, 20.0);
        assert!(close(r.load_score, 0.39), "got {}", r.load_score);
    }

    #[test]
    fn idle_is_0_and_overloaded_is_capped_at_1() {
        let cfg = LoadConfig::default();
        assert_eq!(cfg.report(0, 0.0, 0.0).load_score, 0.0);
        assert!(close(cfg.report(500, 100.0, 9000.0).load_score, 1.0));
    }

    #[test]
    fn more_load_never_lowers_the_score() {
        let cfg = LoadConfig::default();
        let base = cfg.report(2, 30.0, 10.0).load_score;
        assert!(cfg.report(3, 30.0, 10.0).load_score > base);
        assert!(cfg.report(2, 31.0, 10.0).load_score > base);
        assert!(cfg.report(2, 30.0, 11.0).load_score > base);
    }

    #[test]
    fn weights_not_summing_to_1_still_give_0_to_1() {
        let cfg = LoadConfig {
            queue_weight: 2.0,
            cpu_weight: 2.0,
            service_weight: 0.0,
            ..LoadConfig::default()
        };
        // (2 × 1.0 + 2 × 0.5) / 4 = 0.75
        assert!(close(cfg.report(10, 50.0, 0.0).load_score, 0.75));
    }

    #[test]
    fn nan_input_does_not_make_a_nan_score() {
        let s = LoadConfig::default().report(0, f64::NAN, 0.0).load_score;
        assert_eq!(s, 0.0);
    }

    #[test]
    fn check_rejects_bad_values() {
        assert!(LoadConfig::default().check().is_ok());
        let bad = [
            LoadConfig {
                queue_weight: -0.1,
                ..LoadConfig::default()
            },
            LoadConfig {
                queue_weight: 0.0,
                cpu_weight: 0.0,
                service_weight: 0.0,
                ..LoadConfig::default()
            },
            LoadConfig {
                queue_max: 0,
                ..LoadConfig::default()
            },
            LoadConfig {
                service_cap_ms: 0.0,
                ..LoadConfig::default()
            },
            LoadConfig {
                service_ewma_alpha: 0.0,
                ..LoadConfig::default()
            },
            LoadConfig {
                service_ewma_alpha: 1.5,
                ..LoadConfig::default()
            },
            LoadConfig {
                sample_interval_ms: 0,
                ..LoadConfig::default()
            },
        ];
        for cfg in bad {
            assert!(cfg.check().is_err(), "{cfg:?} should be rejected");
        }
    }
}
