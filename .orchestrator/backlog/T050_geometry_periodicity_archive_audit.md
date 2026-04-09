---
task_id: "T050"
title: "Build geometry, periodicity, and archive audit tables"
workstream: "W3"
task_kind: "derived"
allow_network: false
role: "Worker"
priority: "high"
dependencies:
  - "T035"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "analysis/"
  - "derived/"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
  - "raw/"
  - "restricted/"
outputs:
  - "derived/thread_geometry_simulamet.parquet"
  - "derived/periodicity_input_simulamet.parquet"
  - "derived/archive_metadata_audit.parquet"
  - "derived/submolt_category_dictionary_v1.csv"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T050 — Build geometry, periodicity, and archive audit tables

## Context

The flagship design requires thread geometry, periodicity inputs, archive metadata audit, and a frozen topic dictionary before full analysis and replication.

## Assignment

- Workstream: W3
- Assigned role: Worker
- Suggested branch or worktree name: `T050_geometry_periodicity`
- Allowed paths: `analysis/`, `derived/`
- Disallowed paths: authority docs, contracts, paper, raw, and restricted surfaces
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: `contracts/schemas/thread_geometry_v1.yaml`, `contracts/schemas/periodicity_input_v1.yaml`, `contracts/schemas/archive_metadata_audit_v1.yaml`
- Upstream tasks or manifests: `T035`
- External references or systems: none

## Outputs

- Code: new or rewritten geometry and audit builders under `analysis/`
- Data or manifests: geometry table, periodicity input table, archive audit table, frozen topic dictionary
- Reports or docs: notes on linkage quality and geometry transportability

## Success Criteria

- [ ] All required tables follow the canonical schema contracts
- [ ] The topic dictionary is frozen before H4 estimation
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if downstream replication depends on geometry limitations

## Validation / Commands

- `make gate`
- `make test`
- Add task-specific geometry and audit commands here.

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
