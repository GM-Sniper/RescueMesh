use crate::domain::*;
use rand::{Rng, SeedableRng, rngs::StdRng};

const BASE_TS: u64 = 1_700_000_000_000;

const CATS: [Category; 4] = [
    Category::Medical,
    Category::Fire,
    Category::Rescue,
    Category::Supply,
];
const TYPES: [ResourceType; 4] = [
    ResourceType::Ambulance,
    ResourceType::FireTruck,
    ResourceType::RescueTeam,
    ResourceType::SupplyTruck,
];

fn incident_from(rng: &mut StdRng, id: IncidentId) -> Incident {
    Incident {
        id,
        owner: rng.gen_range(1..=3),
        category: CATS[rng.gen_range(0..CATS.len())],
        location: Zone::new(rng.gen_range(0..5), rng.gen_range(0..5)),
        severity: rng.gen_range(1..=10),
        timestamp: BASE_TS + rng.gen_range(0..600_000),
        status: IncidentStatus::Open,
        ttl_secs: 3600,
    }
}

/// Same seed, same incident. The seed is also used as the incident id.
pub fn generate_incident(seed: u64) -> Incident {
    let mut rng = StdRng::seed_from_u64(seed);
    incident_from(&mut rng, seed)
}

/// Same seed, same snapshot. Ids are 1..=n, sorted.
pub fn generate_snapshot(seed: u64, n_inc: usize, n_res: usize) -> Snapshot {
    let mut rng = StdRng::seed_from_u64(seed);
    let incidents = (1..=n_inc as u64)
        .map(|id| incident_from(&mut rng, id))
        .collect();
    let resources = (1..=n_res as u64)
        .map(|id| Resource {
            id,
            rtype: TYPES[rng.gen_range(0..TYPES.len())],
            quantity: rng.gen_range(1..=5),
            available: true,
            location: Zone::new(rng.gen_range(0..5), rng.gen_range(0..5)),
            lease_expiry: 0,
        })
        .collect();
    Snapshot {
        incidents,
        resources,
        assignments: vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn same_seed_same_output() {
        assert_eq!(generate_snapshot(1, 5, 5), generate_snapshot(1, 5, 5));
        assert_eq!(generate_incident(9), generate_incident(9));
        assert_ne!(generate_incident(9), generate_incident(10));
    }
}
