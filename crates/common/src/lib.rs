//! Shared code for every RescueMesh program: the generated protocol types
//! and, from Sat Oct 17, P2's domain types.

/// Code generated from proto/rescuemesh.proto by build.rs.
pub mod pb {
    tonic::include_proto!("rescuemesh.v1");
}

/// Identifies a cloud node. IDs start at 1; 0 means "a client, not a node".
pub type NodeId = u32;

/// A client's ID (Phase 2). Starts at `config::FIRST_CLIENT_ID`, so it never clashes with a node ID.
pub type ClientId = u32;

pub mod config;
pub mod conversions;
pub mod domain;
pub mod generators;
pub mod load;
pub mod planner;
pub mod shared_state;
