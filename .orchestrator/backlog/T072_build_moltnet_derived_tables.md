---
task_id: "T072"
title: "Build MoltNet derived tables and geometry transport decision"
workstream: "W3"
task_kind: "derived"
allow_network: false
role: "Worker"
priority: "medium"
dependencies:
  - "T070"
  - "T050"
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
  - "derived/parent_units_moltnet.parquet"
  - "derived/control_panel_summary_moltnet.parquet"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T072 — Build MoltNet derived tables and geometry transport decision

## Context

After the MoltNet freeze and QC packet are complete, the flagship repo can build the MoltNet canonical derived tables and record whether geometry transport is valid.

## Assignment

- Workstream: W3
- Assigned role: Worker
- Suggested branch or worktree name: `T072_moltnet_derived`
- Allowed paths: `analysis/`, `derived/`
- Disallowed paths: authority docs, contracts, paper, raw, and restricted surfaces
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: flagship derived-table schemas
- Upstream tasks or manifests: `T070`, `T050`
- External references or systems: none

## Outputs

- Code: MoltNet derived-table builders or harmonization helpers under `analysis/`
- Data or manifests: canonical MoltNet `parent_units` and `control_panel_summary` tables
- Reports or docs: notes on whether MoltNet geometry can be transported or must remain limited

## Success Criteria

- [ ] MoltNet derived tables follow the flagship schema contracts
- [ ] Geometry transport limits are explicitly recorded
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if manuscript integration depends on replication caveats

## Validation / Commands

- `make gate`
- `make test`
- Add task-specific MoltNet derived-table commands here.

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Split out from the original combined MoltNet replication task to preserve W2/W3 ownership boundaries.
