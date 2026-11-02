use crate::config::PlannerConfig;
use crate::domain::*;
use crate::generators::generate_snapshot;

/// Pure planner v1: no clock, no RNG. Ties broken by lowest resource id.
///
/// Preserves the original API and uses the default workload configuration.
pub fn plan(incident: &Incident, snapshot: &Snapshot, w: &Weights, now: u64) -> Plan {
    plan_with_workload(incident, snapshot, w, now, &PlannerConfig::default())
}

/// Plans an assignment after performing configurable deterministic workload.
///
/// The extra work models an incident-resource matching workload. It does not
/// change the assignment algorithm or the returned plan.
pub fn plan_with_workload(
    incident: &Incident,
    snapshot: &Snapshot,
    w: &Weights,
    now: u64,
    config: &PlannerConfig,
) -> Plan {
    if incident.status == IncidentStatus::Resolved {
        return Plan {
            incident_id: incident.id,
            priority: 0.0,
            assignments: Vec::new(),
        };
    }

    perform_matching_workload(config, w, now);

    let age_min = now.saturating_sub(incident.timestamp) as f64 / 60_000.0;
    let priority = w.severity * incident.severity as f64 + w.age * age_min;

    let mut scored: Vec<Assignment> = snapshot
        .resources
        .iter()
        .filter(|r| is_free(r, snapshot, now))
        .map(|r| {
            let m = r.rtype.match_score(incident.category);
            let d = r.location.distance(&incident.location) as f64;
            Assignment {
                incident_id: incident.id,
                resource_id: r.id,
                score: w.capability * m - w.distance * d,
            }
        })
        .collect();

    scored.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then(a.resource_id.cmp(&b.resource_id))
    });
    scored.truncate(1); // v1: one resource per incident

    Plan {
        incident_id: incident.id,
        priority,
        assignments: scored,
    }
}

/// Performs deterministic CPU work proportional to:
/// workload_incidents * workload_resources * iterations.
/// Performs deterministic incident-resource scoring for a configurable
/// workload. The generated workload is separate from the live snapshot, so
/// it adds CPU cost without changing the requested incident's assignment.
fn perform_matching_workload(config: &PlannerConfig, w: &Weights, now: u64) {
    // A fixed seed ensures identical workload inputs on every node.
    let workload = generate_snapshot(42, config.workload_incidents, config.workload_resources);

    let mut accumulator = 0.0_f64;

    for _ in 0..config.iterations {
        for incident in &workload.incidents {
            let age_min = now.saturating_sub(incident.timestamp) as f64 / 60_000.0;

            let priority = w.severity * incident.severity as f64 + w.age * age_min;

            for resource in &workload.resources {
                let capability = resource.rtype.match_score(incident.category);
                let distance = resource.location.distance(&incident.location) as f64;

                let score = w.capability * capability - w.distance * distance;

                accumulator += priority + score;
            }
        }
    }

    std::hint::black_box(accumulator);
}
fn is_free(r: &Resource, s: &Snapshot, now: u64) -> bool {
    r.available
        && r.quantity > 0
        && (r.lease_expiry == 0 || r.lease_expiry <= now)
        && !s.assignments.iter().any(|a| a.resource_id == r.id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generators::*;

    fn inc() -> Incident {
        Incident {
            id: 1,
            owner: 1,
            category: Category::Medical,
            location: Zone::new(0, 0),
            severity: 8,
            timestamp: 0,
            status: IncidentStatus::Open,
            ttl_secs: 600,
        }
    }

    fn res(id: u64, t: ResourceType, x: u8, y: u8) -> Resource {
        Resource {
            id,
            rtype: t,
            quantity: 1,
            available: true,
            location: Zone::new(x, y),
            lease_expiry: 0,
        }
    }

    #[test]
    fn resolved_incident_gets_no_assignments() {
        let mut incident = inc();
        incident.status = IncidentStatus::Resolved;

        let snapshot = Snapshot {
            resources: vec![
                res(1, ResourceType::Ambulance, 0, 0),
                res(2, ResourceType::FireTruck, 1, 1),
            ],
            ..Default::default()
        };

        let result = plan(&incident, &snapshot, &Weights::default(), 1_000);

        assert_eq!(result.incident_id, incident.id);
        assert!(result.assignments.is_empty());
    }

    #[test]
    fn worked_example() {
        // R1 match 1.0 dist 2 -> 0.10 ; R2 match 0.5 dist 1 -> 0.05
        let s = Snapshot {
            resources: vec![
                res(1, ResourceType::Ambulance, 1, 1),
                res(2, ResourceType::RescueTeam, 1, 0),
            ],
            ..Default::default()
        };
        let p = plan(&inc(), &s, &Weights::default(), 5 * 60_000);
        assert!((p.priority - 6.8).abs() < 1e-9);
        assert_eq!(p.assignments[0].resource_id, 1);
        assert!((p.assignments[0].score - 0.10).abs() < 1e-9);
    }

    #[test]
    fn deterministic() {
        let s = generate_snapshot(42, 10, 20);
        let i = generate_incident(7);
        assert_eq!(
            plan(&i, &s, &Weights::default(), 1_000),
            plan(&i, &s, &Weights::default(), 1_000)
        );
    }

    #[test]
    fn ties_lowest_id_regardless_of_order() {
        let a = vec![
            res(5, ResourceType::Ambulance, 1, 0),
            res(3, ResourceType::Ambulance, 0, 1),
        ];
        let mut b = a.clone();
        b.reverse();
        let pa = plan(
            &inc(),
            &Snapshot {
                resources: a,
                ..Default::default()
            },
            &Weights::default(),
            0,
        );
        let pb = plan(
            &inc(),
            &Snapshot {
                resources: b,
                ..Default::default()
            },
            &Weights::default(),
            0,
        );
        assert_eq!(pa.assignments[0].resource_id, 3);
        assert_eq!(pa, pb);
    }

    #[test]
    fn empty_does_not_panic() {
        let p = plan(&inc(), &Snapshot::default(), &Weights::default(), 0);
        assert!(p.assignments.is_empty());
    }

    #[test]
    fn skips_leased_and_taken() {
        let mut leased = res(1, ResourceType::Ambulance, 0, 0);
        leased.lease_expiry = 10_000;
        let taken = res(2, ResourceType::Ambulance, 0, 0);
        let s = Snapshot {
            resources: vec![leased, taken, res(3, ResourceType::FireTruck, 4, 4)],
            assignments: vec![Assignment {
                incident_id: 9,
                resource_id: 2,
                score: 0.0,
            }],
            ..Default::default()
        };
        let p = plan(&inc(), &s, &Weights::default(), 5_000);
        assert_eq!(p.assignments[0].resource_id, 3);
    }

    #[test]
    fn workload_planner_preserves_assignment_result() {
        let snapshot = generate_snapshot(42, 10, 20);
        let incident = generate_incident(7);
        let weights = Weights::default();

        let config = PlannerConfig {
            workload_incidents: 20,
            workload_resources: 30,
            iterations: 2,
        };

        let original = plan(&incident, &snapshot, &weights, 1_000);
        let with_workload = plan_with_workload(&incident, &snapshot, &weights, 1_000, &config);

        assert_eq!(original, with_workload);
    }

    #[test]
    fn workload_planner_is_deterministic() {
        let snapshot = generate_snapshot(42, 10, 20);
        let incident = generate_incident(7);
        let weights = Weights::default();

        let config = PlannerConfig {
            workload_incidents: 20,
            workload_resources: 30,
            iterations: 2,
        };

        let first = plan_with_workload(&incident, &snapshot, &weights, 1_000, &config);
        let second = plan_with_workload(&incident, &snapshot, &weights, 1_000, &config);

        assert_eq!(first, second);
    }
}
