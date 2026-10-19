# RescueMesh Design Log

## P2: Domain Types and Planner v1

### 1. Resource Match Scores

- Exact match: 1.0
- Partial match: 0.5
- No match: 0.0

### 2. Incident Age

Incident age is measured in minutes and contributes to the incident priority score.

### 3. Tie-Breaking

When resources receive equal scores, the planner selects the resource with the lowest resource ID to ensure deterministic results.

### 4. Assignment Policy

Planner v1 assigns at most one resource to each incident.

### 5. Determinism

Generators use a seed to produce repeatable outputs, and planner tie-breaking is deterministic.

## P1: Transport and Fault Injection

### 1. Mute Hook

**What.** `InjectFault { fault_id, target_node, duration_ms }` makes a node silent for `duration_ms` (1 to 20 000 ms; anything else is refused with `FaultAck { accepted: false, reason }`). While muted:

- Every incoming request is held at a gate at the start of each request handler until the mute ends, then dropped with `unavailable`. Requests already in progress have their replies dropped the same way.
- The node sends nothing: every outgoing call gets its client from `peer_for_sending`, which returns nothing while muted.
- New fault orders are ignored. The node wakes up by itself when the time is up. The `FaultAck` is sent before the silence starts.

**Alternatives considered.**

- *Reply with an error at once while muted.* Peers would learn of the failure instantly, which no crashed machine does, and the measured detection and failover times would be meaningless. Rejected.
- *Check in a tonic interceptor.* Interceptors are synchronous: they can only pass or reject a request immediately, so they cannot hang without blocking a runtime thread. The check is an async `gate()` in each handler instead.
- *An `AtomicBool` plus a timer task that switches it back off.* With overlapping mutes, the first timer would end the second mute early. We store the end time (`muted_until`) instead: nothing has to switch it off, and a later order can only extend it.
- *Close the TCP connection.* Not possible from a gRPC handler, and the caller would reconnect at once. Holding the request has the same effect for callers: silence until their own deadline.

**Consequences.**

- A dropped `Submit` reply may belong to work already stored. The client retries with the same `request_id` and gets the stored plan (`duplicate = true`).
- Held requests end when the caller's deadline cancels them, so they do not pile up; a caller without a deadline gets `unavailable` at most 20 s later.
- Tested with a fake clock (10 s scenarios in milliseconds) and end to end over a real network (`real_network_mute_round_trip`).
