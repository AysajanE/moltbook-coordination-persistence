---
task_id: "T040"
title: "Build canonical parent_units table"
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
  - "docs/stage3_theory_framework_packet.cleaned.md"
  - "docs/data_acquisition_plan.md"
  - "contracts/"
  - "paper/"
  - "raw/"
  - "restricted/"
outputs:
  - "derived/parent_units_simulamet.parquet"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T040 — Build canonical parent_units table

## Context

`parent_units` is the load-bearing flagship table for reply incidence, conditional speed, and throughput decomposition.

## Assignment

- Workstream: W3
- Assigned role: Worker
- Suggested branch or worktree name: `T040_parent_units`
- Allowed paths: `analysis/`, `derived/`
- Disallowed paths: authority docs, contracts, paper, raw, and restricted surfaces
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority docs: `docs/stage3_theory_framework_packet.cleaned.md`, `docs/data_acquisition_plan.md`
- Contracts: `contracts/schemas/parent_units_v1.yaml`
- Upstream tasks or manifests: `T035`, canonical freeze and QC packet
- External references or systems: none

## Outputs

- Code: new or rewritten builder under `analysis/`
- Data or manifests: `derived/parent_units_simulamet.parquet`
- Reports or docs: notes on censoring, gap overlap, and quality flags

## Success Criteria

- [ ] The table follows the canonical required fields and construction rules
- [ ] Risk-set and within-horizon fields are reproducible from the freeze
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if downstream summary logic relies on implementation details

## Validation / Commands

- `make gate`
- `make test`
- Add task-specific derived-table commands here.

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
