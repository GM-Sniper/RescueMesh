use common::config::PlannerConfig;
use common::domain::Weights;
use common::generators::{generate_incident, generate_snapshot};
use common::planner::plan_with_workload;
use std::hint::black_box;
use std::time::Instant;

fn main() {
    // Use the same generated inputs for every workload configuration.
    let snapshot = generate_snapshot(42, 10, 20);
    let incident = generate_incident(7);
    let weights = Weights::default();

    // (number of incidents, number of resources, iterations)
    let cases: [(usize, usize, usize); 5] = [
        (10, 20, 1),
        (50, 50, 1),
        (100, 100, 1),
        (200, 200, 1),
        (100, 100, 10),
    ];

    println!("RescueMesh planner workload benchmark");
    println!("Release mode recommended");
    println!();

    println!(
        "{:>10} {:>10} {:>10} {:>12} {:>12} {:>14}",
        "Incidents", "Resources", "Iterations", "Work units", "Runs", "Avg time (ms)"
    );

    for (incidents, resources, iterations) in cases {
        let config = PlannerConfig {
            workload_incidents: incidents,
            workload_resources: resources,
            iterations,
        };

        let work_units = incidents * resources * iterations;

        // Run more repetitions for small workloads so their timing
        // is easier to measure reliably.
        let runs = (2_000_000_usize / work_units).clamp(100, 20_000);

        // Warm up the planner before measuring.
        black_box(plan_with_workload(
            &incident, &snapshot, &weights, 1_000, &config,
        ));

        let start = Instant::now();

        for _ in 0..runs {
            black_box(plan_with_workload(
                &incident, &snapshot, &weights, 1_000, &config,
            ));
        }

        let elapsed = start.elapsed();
        let average_ms = elapsed.as_secs_f64() * 1_000.0 / runs as f64;

        println!(
            "{:>10} {:>10} {:>10} {:>12} {:>12} {:>14.6}",
            incidents, resources, iterations, work_units, runs, average_ms
        );
    }
}
