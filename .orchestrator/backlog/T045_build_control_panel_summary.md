---
task_id: "T045"
title: "Build canonical control_panel_summary table"
workstream: "W3"
task_kind: "derived"
allow_network: false
role: "Worker"
priority: "high"
dependencies:
  - "T040"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "analysis/build_control_panel_summary.py"
  - "derived/control_panel_summary_simulamet.parquet"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
  - "raw/"
  - "restricted/"
outputs:
  - "derived/control_panel_summary_simulamet.parquet"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T045 — Build canonical control_panel_summary table

## Context

The flagship estimands are reported through the aggregated `control_panel_summary` table, not through legacy survival-table assumptions.

## Assignment

- Workstream: W3
- Assigned role: Worker
- Suggested branch or worktree name: `T045_control_panel_summary`
- Allowed paths: `analysis/`, `derived/`
- Disallowed paths: authority docs, contracts, paper, raw, and restricted surfaces
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: `contracts/schemas/control_panel_summary_v1.yaml`
- Upstream tasks or manifests: `T040`
- External references or systems: none

## Outputs

- Code: summary builder or aggregator under `analysis/`
- Data or manifests: `derived/control_panel_summary_simulamet.parquet`
- Reports or docs: notes on full-window and gap-variant summaries

## Success Criteria

- [ ] The table satisfies the canonical same-risk-set denominator rules
- [ ] `q_h = pi_h * phi_h` holds within machine tolerance
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if downstream analysis depends on implementation details

## Validation / Commands

- `python analysis/build_control_panel_summary.py --parent-units derived/parent_units_simulamet.parquet --out derived/control_panel_summary_simulamet.parquet`
- `make gate`
- `make test`

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
