use serde::{Deserialize, Serialize};

pub type IncidentId = u64;
pub type ResourceId = u64;

/// One cell of the 5x5 grid.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Zone {
    pub x: u8,
    pub y: u8,
}

impl Zone {
    pub const GRID: u8 = 5;

    pub fn new(x: u8, y: u8) -> Self {
        Zone { x: x % Self::GRID, y: y % Self::GRID }
    }

    /// Manhattan distance between two zones.
    pub fn distance(&self, other: &Zone) -> u32 {
        (self.x.abs_diff(other.x) as u32) + (self.y.abs_diff(other.y) as u32)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Category {
    Medical,
    Fire,
    Rescue,
    Supply,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceType {
    Ambulance,
    FireTruck,
    RescueTeam,
    SupplyTruck,
}

impl ResourceType {
    /// 1.0 = right tool, 0.5 = can help, 0.0 = useless.
    pub fn match_score(&self, c: Category) -> f64 {
        use Category::*;
        use ResourceType::*;
        match (self, c) {
            (Ambulance, Medical) | (FireTruck, Fire) | (RescueTeam, Rescue) | (SupplyTruck, Supply) => 1.0,
            (RescueTeam, Medical) | (RescueTeam, Fire) | (Ambulance, Rescue) => 0.5,
            _ => 0.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IncidentStatus {
    Open,
    Assigned,
    Resolved,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Incident {
    pub id: IncidentId,
    pub owner: u32,
    pub category: Category,
    pub location: Zone,
    pub severity: u8,    // 1..=10
    pub timestamp: u64,  // unix ms
    pub status: IncidentStatus,
    pub ttl_secs: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Resource {
    pub id: ResourceId,
    pub rtype: ResourceType,
    pub quantity: u32,
    pub available: bool,
    pub location: Zone,
    pub lease_expiry: u64, // unix ms, 0 = not leased
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub incident_id: IncidentId,
    pub resource_id: ResourceId,
    pub score: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Plan {
    pub incident_id: IncidentId,
    pub priority: f64,
    pub assignments: Vec<Assignment>,
}

/// Read-only view of the store. Vecs are sorted by id so it is deterministic.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub incidents: Vec<Incident>,
    pub resources: Vec<Resource>,
    pub assignments: Vec<Assignment>,
}

/// Scoring weights. Loaded from config, `Default` = the worked example.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(default)]
pub struct Weights {
    pub severity: f64,
    pub age: f64,
    pub capability: f64,
    pub distance: f64,
}

impl Default for Weights {
    fn default() -> Self {
        Weights { severity: 0.6, age: 0.4, capability: 0.7, distance: 0.3 }
    }
}
