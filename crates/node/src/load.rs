//! P3: LoadCollector. Keeps the live inputs of this node's load score:
//! - queue length: NodeState's in-flight counter, read at the moment a report is asked for;
//! - CPU: whole-machine use from sysinfo, sampled every `sample_interval_ms` on its own thread;
//! - service time: a moving average (EWMA) of how long each request took.
//!
//! `NodeState::load_report()` combines them with the formula in `common::load`.
//! Watch the live numbers with:  RUST_LOG=info,node::load=debug cargo run -p node -- --id 1

use std::sync::{Arc, Mutex};
use std::time::Duration;

use common::load::{LoadConfig, LoadReport};
use sysinfo::{CpuRefreshKind, MINIMUM_CPU_UPDATE_INTERVAL, RefreshKind, System};

use crate::state::NodeState;

pub struct LoadCollector {
    cfg: LoadConfig,
    samples: Mutex<Samples>,
}

#[derive(Default)]
struct Samples {
    /// Latest whole-machine CPU reading, 0-100.
    cpu_pct: f64,
    /// Average request time in ms; None until the first request finishes.
    service_ms: Option<f64>,
}

impl LoadCollector {
    pub fn new(cfg: LoadConfig) -> Self {
        LoadCollector {
            cfg,
            samples: Mutex::new(Samples::default()),
        }
    }

    /// Called when a request finishes (by the in-flight guard), with how long it took.
    pub fn record_service_time(&self, took: Duration) {
        let ms = took.as_secs_f64() * 1000.0;
        let mut s = self.samples.lock().unwrap();
        let a = self.cfg.service_ewma_alpha;
        // First request: start the average at its value instead of pulling it up from 0.
        s.service_ms = Some(match s.service_ms {
            None => ms,
            Some(avg) => a * ms + (1.0 - a) * avg,
        });
    }

    fn set_cpu(&self, cpu_pct: f64) {
        self.samples.lock().unwrap().cpu_pct = cpu_pct;
    }

    /// The current report, using `queue_len` read live by the caller.
    pub fn current(&self, queue_len: usize) -> LoadReport {
        let s = self.samples.lock().unwrap();
        self.cfg
            .report(queue_len, s.cpu_pct, s.service_ms.unwrap_or(0.0))
    }
}

/// Starts the CPU sampler. A plain thread, not a tokio task: if the planner keeps
/// the async runtime busy, a tokio task would sample late, exactly when load is high.
pub fn spawn_sampler(state: Arc<NodeState>) {
    // sysinfo computes CPU use from the difference between two readings, and needs
    // them at least MINIMUM_CPU_UPDATE_INTERVAL (200 ms) apart to be accurate.
    let interval = Duration::from_millis(state.config.load.sample_interval_ms)
        .max(MINIMUM_CPU_UPDATE_INTERVAL);
    std::thread::Builder::new()
        .name("load-sampler".into())
        .spawn(move || {
            let mut sys = System::new_with_specifics(
                RefreshKind::nothing().with_cpu(CpuRefreshKind::nothing().with_cpu_usage()),
            );
            loop {
                std::thread::sleep(interval);
                sys.refresh_cpu_usage();
                state.load.set_cpu(sys.global_cpu_usage() as f64);
                let r = state.load_report();
                tracing::debug!(
                    queue = r.queue_len,
                    cpu_pct = format_args!("{:.1}", r.cpu_pct),
                    service_ms = format_args!("{:.2}", r.service_ms),
                    load_score = format_args!("{:.3}", r.load_score),
                    "load"
                );
            }
        })
        .expect("cannot start the load sampler thread");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn starts_idle() {
        let c = LoadCollector::new(LoadConfig::default());
        let r = c.current(0);
        assert_eq!((r.cpu_pct, r.service_ms, r.load_score), (0.0, 0.0, 0.0));
    }

    #[test]
    fn first_request_sets_the_average_then_ewma_follows() {
        let c = LoadCollector::new(LoadConfig::default()); // alpha 0.2
        c.record_service_time(Duration::from_millis(50));
        assert!(close(c.current(0).service_ms, 50.0));
        c.record_service_time(Duration::from_millis(100));
        // 0.2 × 100 + 0.8 × 50 = 60
        assert!(close(c.current(0).service_ms, 60.0));
    }

    #[test]
    fn plan_worked_example_through_the_collector() {
        let c = LoadCollector::new(LoadConfig::default());
        c.set_cpu(50.0);
        c.record_service_time(Duration::from_millis(20));
        assert!(close(c.current(4).load_score, 0.39));
    }

    /// Check under artificial load: burns every core for ~1.5 s and expects the
    /// sampled CPU to rise. Ignored by default (slow, depends on the machine);
    /// run with: cargo test -p node -- --ignored --nocapture
    #[test]
    #[ignore]
    fn cpu_reading_rises_under_artificial_load() {
        let mut sys = System::new_with_specifics(
            RefreshKind::nothing().with_cpu(CpuRefreshKind::nothing().with_cpu_usage()),
        );
        std::thread::sleep(MINIMUM_CPU_UPDATE_INTERVAL);
        sys.refresh_cpu_usage();
        let idle = sys.global_cpu_usage();

        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
        let burners: Vec<_> = (0..cores)
            .map(|_| {
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let mut x = 0u64;
                    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                        x = std::hint::black_box(x.wrapping_mul(31).wrapping_add(7));
                    }
                })
            })
            .collect();
        std::thread::sleep(Duration::from_millis(1500));
        sys.refresh_cpu_usage();
        let busy = sys.global_cpu_usage();
        stop.store(true, std::sync::atomic::Ordering::Relaxed);
        burners.into_iter().for_each(|t| t.join().unwrap());

        println!("CPU idle {idle:.1}% -> busy {busy:.1}%");
        assert!(
            busy > 80.0,
            "expected > 80% with every core busy, got {busy:.1}%"
        );
    }
}
