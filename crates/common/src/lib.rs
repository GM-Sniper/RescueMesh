//! Shared code for every RescueMesh program: the generated protocol types
//! and, from Sat Oct 17, P2's domain types.

/// Code generated from proto/rescuemesh.proto by build.rs.
pub mod pb {
    tonic::include_proto!("rescuemesh.v1");
}

/// Identifies a cloud node. IDs start at 1; 0 means "a client, not a node".
pub type NodeId = u32;

pub mod conversions;
pub mod domain;
pub mod generators;
pub mod planner;
