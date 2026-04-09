---
task_id: "T055"
title: "Rewrite legacy estimand scripts against canonical derived tables"
workstream: "W4"
task_kind: "analysis"
allow_network: false
role: "Worker"
priority: "high"
dependencies:
  - "T045"
  - "T050"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "analysis/flagship_control_panel_margins.py"
  - "analysis/incidence_horizon_standardization.py"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
outputs:
  - "analysis/flagship_control_panel_margins.py"
  - "analysis/incidence_horizon_standardization.py"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T055 — Rewrite legacy estimand scripts against canonical derived tables

## Context

The carried-over estimand scripts are legacy helpers. They must be rewritten against the flagship derived tables before they can produce canonical evidence.

## Assignment

- Workstream: W4
- Assigned role: Worker
- Suggested branch or worktree name: `T055_rewrite_estimands`
- Allowed paths: the two carried-over estimand scripts
- Disallowed paths: authority docs, contracts, and paper surfaces
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: `parent_units` and `control_panel_summary` schema contracts
- Upstream tasks or manifests: `T045`, `T050`
- External references or systems: none

## Outputs

- Code: rewritten estimand scripts
- Data or manifests: none required in this task
- Reports or docs: notes on what legacy assumptions were removed

## Success Criteria

- [ ] The scripts no longer assume the old survival-table interface
- [ ] The scripts read the canonical flagship derived tables
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if downstream analysis sequencing matters

## Validation / Commands

- `make gate`
- `make test`
- Add task-specific script rewrite commands here.

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
