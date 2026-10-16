# RescueMesh

Cloud-P2P campus emergency-response system for CSCE 4411 (Distributed Systems, Fall 2026). Users report incidents, offer or request resources, and get response plans computed by a cloud of 3 equal peer nodes, with no master. All data is synthetic.

Rust + tonic/gRPC. tonic and prost only move and encode bytes. The protocol, heartbeats, failure detection, election, broadcast, timeouts, retries, replication and recovery are our own code. Do not use tower load-balancing or retry layers, or gRPC's built-in health check.

## Setup
1. Install Rust from https://rustup.rs. On Windows, accept the Visual Studio C++ Build Tools when asked.
2. Clone and build:
   ```
   git clone https://github.com/GM-Sniper/RescueMesh
   cd RescueMesh
   cargo build
   ```
No protoc install is needed: the proto is compiled with protox, a pure-Rust compiler that Cargo downloads.

## Layout
| Path | What | Owner |
|---|---|---|
| `proto/rescuemesh.proto` | Wire protocol (messages and RPCs) | P1 |
| `crates/common` | Shared library: generated proto code (`common::pb`), `NodeId`, later the domain types | P1, P2 |
| `crates/node` | Cloud node program | P1 |
| `crates/client` | Client program | P6 |
| `crates/loadgen` | Load generator | P6 |
| `docs/` | Plans and change notes | All |

The generated Rust for the proto is in `target/debug/build/common-*/out/rescuemesh.v1.rs`. Never edit it; change the `.proto` instead.

## Status
- Fri Oct 16: workspace builds; proto v0 has `Heartbeat` only. See `docs/p1-changes.md`.
- Sat Oct 17: proto v1 (SubmitRequest, Bid, Replicate, FetchLog, FaultCommand).
- Wed Oct 21: Step A, one request through 3 nodes on the real machines.

## Team rules
- One branch per task. Your buddy reviews the PR before it merges (P1 with P5, P2 with P6, P3 with P4).
- Every state-changing message carries `request_id`.
- Never change or reuse a proto field number once it is merged.
- One design-log entry per decision: what, the alternatives, and why.
- Blocked? Code against a stub of the other person's interface and say so in the group chat that day.
