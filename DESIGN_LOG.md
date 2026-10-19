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