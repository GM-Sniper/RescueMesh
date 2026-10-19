use crate::domain::{
    Assignment, Category, Incident, IncidentStatus, Plan, Resource, ResourceType, Zone,
};
use crate::pb;

/// Convert a protobuf zone into a domain zone without wrapping invalid coordinates.
pub fn zone_from_pb(value: pb::Zone) -> Result<Zone, String> {
    if value.x >= u32::from(Zone::GRID) || value.y >= u32::from(Zone::GRID) {
        return Err(format!(
            "Invalid zone coordinates: x={}, y={}; both must be in 0..=4",
            value.x, value.y
        ));
    }

    Ok(Zone {
        x: value.x as u8,
        y: value.y as u8,
    })
}

pub fn zone_to_pb(value: Zone) -> pb::Zone {
    pb::Zone {
        x: u32::from(value.x),
        y: u32::from(value.y),
    }
}

fn category_from_pb(value: i32) -> Result<Category, String> {
    match pb::Category::try_from(value)
        .map_err(|_| format!("Invalid category enum value: {value}"))?
    {
        pb::Category::Unspecified => Err("Category must not be unspecified".into()),
        pb::Category::Medical => Ok(Category::Medical),
        pb::Category::Fire => Ok(Category::Fire),
        pb::Category::Rescue => Ok(Category::Rescue),
        pb::Category::Supply => Ok(Category::Supply),
    }
}

fn category_to_pb(value: Category) -> i32 {
    match value {
        Category::Medical => pb::Category::Medical as i32,
        Category::Fire => pb::Category::Fire as i32,
        Category::Rescue => pb::Category::Rescue as i32,
        Category::Supply => pb::Category::Supply as i32,
    }
}

fn resource_type_from_pb(value: i32) -> Result<ResourceType, String> {
    match pb::ResourceType::try_from(value)
        .map_err(|_| format!("Invalid resource type enum value: {value}"))?
    {
        pb::ResourceType::Unspecified => Err("Resource type must not be unspecified".into()),
        pb::ResourceType::Ambulance => Ok(ResourceType::Ambulance),
        pb::ResourceType::FireTruck => Ok(ResourceType::FireTruck),
        pb::ResourceType::RescueTeam => Ok(ResourceType::RescueTeam),
        pb::ResourceType::SupplyTruck => Ok(ResourceType::SupplyTruck),
    }
}

fn resource_type_to_pb(value: ResourceType) -> i32 {
    match value {
        ResourceType::Ambulance => pb::ResourceType::Ambulance as i32,
        ResourceType::FireTruck => pb::ResourceType::FireTruck as i32,
        ResourceType::RescueTeam => pb::ResourceType::RescueTeam as i32,
        ResourceType::SupplyTruck => pb::ResourceType::SupplyTruck as i32,
    }
}

fn incident_status_from_pb(value: i32) -> Result<IncidentStatus, String> {
    match pb::IncidentStatus::try_from(value)
        .map_err(|_| format!("Invalid incident status enum value: {value}"))?
    {
        pb::IncidentStatus::Unspecified => Err("Incident status must not be unspecified".into()),
        pb::IncidentStatus::Open => Ok(IncidentStatus::Open),
        pb::IncidentStatus::Assigned => Ok(IncidentStatus::Assigned),
        pb::IncidentStatus::Resolved => Ok(IncidentStatus::Resolved),
    }
}

fn incident_status_to_pb(value: IncidentStatus) -> i32 {
    match value {
        IncidentStatus::Open => pb::IncidentStatus::Open as i32,
        IncidentStatus::Assigned => pb::IncidentStatus::Assigned as i32,
        IncidentStatus::Resolved => pb::IncidentStatus::Resolved as i32,
    }
}

pub fn incident_from_pb(value: pb::Incident) -> Result<Incident, String> {
    let location = value
        .location
        .ok_or_else(|| "Incident location is required".to_string())?;

    if !(1..=10).contains(&value.severity) {
        return Err(format!(
            "Invalid incident severity: {}; expected 1..=10",
            value.severity
        ));
    }

    Ok(Incident {
        id: value.id,
        owner: value.owner,
        category: category_from_pb(value.category)?,
        location: zone_from_pb(location)?,
        severity: value.severity as u8,
        timestamp: value.timestamp_ms,
        status: incident_status_from_pb(value.status)?,
        ttl_secs: value.ttl_secs,
    })
}

pub fn incident_to_pb(value: &Incident) -> pb::Incident {
    pb::Incident {
        id: value.id,
        owner: value.owner,
        category: category_to_pb(value.category),
        location: Some(zone_to_pb(value.location)),
        severity: u32::from(value.severity),
        timestamp_ms: value.timestamp,
        status: incident_status_to_pb(value.status),
        ttl_secs: value.ttl_secs,
    }
}

pub fn resource_from_pb(value: pb::Resource) -> Result<Resource, String> {
    let location = value
        .location
        .ok_or_else(|| "Resource location is required".to_string())?;

    Ok(Resource {
        id: value.id,
        rtype: resource_type_from_pb(value.resource_type)?,
        quantity: value.quantity,
        available: value.available,
        location: zone_from_pb(location)?,
        lease_expiry: value.lease_expiry_ms,
    })
}

pub fn resource_to_pb(value: &Resource) -> pb::Resource {
    pb::Resource {
        id: value.id,
        resource_type: resource_type_to_pb(value.rtype),
        quantity: value.quantity,
        available: value.available,
        location: Some(zone_to_pb(value.location)),
        lease_expiry_ms: value.lease_expiry,
    }
}

pub fn assignment_from_pb(value: pb::Assignment) -> Assignment {
    Assignment {
        incident_id: value.incident_id,
        resource_id: value.resource_id,
        score: value.score,
    }
}

pub fn assignment_to_pb(value: &Assignment) -> pb::Assignment {
    pb::Assignment {
        incident_id: value.incident_id,
        resource_id: value.resource_id,
        score: value.score,
    }
}

pub fn plan_from_pb(value: pb::Plan) -> Plan {
    Plan {
        incident_id: value.incident_id,
        priority: value.priority,
        assignments: value
            .assignments
            .into_iter()
            .map(assignment_from_pb)
            .collect(),
    }
}

pub fn plan_to_pb(value: &Plan) -> pb::Plan {
    pb::Plan {
        incident_id: value.incident_id,
        priority: value.priority,
        assignments: value.assignments.iter().map(assignment_to_pb).collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_incident_pb() -> pb::Incident {
        pb::Incident {
            id: 12,
            owner: 3,
            category: pb::Category::Medical as i32,
            location: Some(pb::Zone { x: 2, y: 4 }),
            severity: 8,
            timestamp_ms: 12345,
            status: pb::IncidentStatus::Open as i32,
            ttl_secs: 600,
        }
    }

    fn valid_resource_pb() -> pb::Resource {
        pb::Resource {
            id: 7,
            resource_type: pb::ResourceType::Ambulance as i32,
            quantity: 2,
            available: true,
            location: Some(pb::Zone { x: 1, y: 3 }),
            lease_expiry_ms: 0,
        }
    }

    #[test]
    fn zone_round_trip() {
        let original = Zone { x: 2, y: 4 };
        let encoded = zone_to_pb(original);
        let decoded = zone_from_pb(encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn rejects_invalid_zone_coordinates() {
        assert!(zone_from_pb(pb::Zone { x: 5, y: 0 }).is_err());
        assert!(zone_from_pb(pb::Zone { x: 0, y: 5 }).is_err());
    }

    #[test]
    fn incident_round_trip() {
        let original = incident_from_pb(valid_incident_pb()).unwrap();
        let encoded = incident_to_pb(&original);
        let decoded = incident_from_pb(encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn rejects_missing_incident_location() {
        let mut incident = valid_incident_pb();
        incident.location = None;

        assert!(incident_from_pb(incident).is_err());
    }

    #[test]
    fn rejects_invalid_incident_severity() {
        let mut incident = valid_incident_pb();
        incident.severity = 0;
        assert!(incident_from_pb(incident).is_err());

        let mut incident = valid_incident_pb();
        incident.severity = 11;
        assert!(incident_from_pb(incident).is_err());
    }

    #[test]
    fn rejects_unspecified_and_unknown_incident_enums() {
        let mut incident = valid_incident_pb();
        incident.category = pb::Category::Unspecified as i32;
        assert!(incident_from_pb(incident).is_err());

        let mut incident = valid_incident_pb();
        incident.category = 99;
        assert!(incident_from_pb(incident).is_err());

        let mut incident = valid_incident_pb();
        incident.status = pb::IncidentStatus::Unspecified as i32;
        assert!(incident_from_pb(incident).is_err());

        let mut incident = valid_incident_pb();
        incident.status = 99;
        assert!(incident_from_pb(incident).is_err());
    }

    #[test]
    fn resource_round_trip() {
        let original = resource_from_pb(valid_resource_pb()).unwrap();
        let encoded = resource_to_pb(&original);
        let decoded = resource_from_pb(encoded).unwrap();

        assert_eq!(decoded, original);
    }

    #[test]
    fn rejects_missing_resource_location() {
        let mut resource = valid_resource_pb();
        resource.location = None;

        assert!(resource_from_pb(resource).is_err());
    }

    #[test]
    fn rejects_unspecified_and_unknown_resource_types() {
        let mut resource = valid_resource_pb();
        resource.resource_type = pb::ResourceType::Unspecified as i32;
        assert!(resource_from_pb(resource).is_err());

        let mut resource = valid_resource_pb();
        resource.resource_type = 99;
        assert!(resource_from_pb(resource).is_err());
    }

    #[test]
    fn plan_round_trip() {
        let original = Plan {
            incident_id: 12,
            priority: 0.85,
            assignments: vec![Assignment {
                incident_id: 12,
                resource_id: 7,
                score: 0.75,
            }],
        };

        let encoded = plan_to_pb(&original);
        let decoded = plan_from_pb(encoded);

        assert_eq!(decoded, original);
    }
}
