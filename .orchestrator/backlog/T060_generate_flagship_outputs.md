---
task_id: "T060"
title: "Generate fresh flagship outputs for H1-H5 and P2"
workstream: "W4"
task_kind: "analysis"
allow_network: false
role: "Worker"
priority: "high"
dependencies:
  - "T055"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "analysis/flagship_control_panel_margins.py"
  - "analysis/incidence_horizon_standardization.py"
  - "qc/analysis_execution_simulamet.md"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
  - "raw/"
  - "restricted/"
outputs:
  - "qc/analysis_execution_simulamet.md"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T060 — Generate fresh flagship outputs for H1-H5 and P2

## Context

Once the canonical tables and rewritten scripts exist, the flagship repo can generate fresh evidence entirely inside this repo.

## Assignment

- Workstream: W4
- Assigned role: Worker
- Suggested branch or worktree name: `T060_flagship_outputs`
- Allowed paths: `analysis/`, `qc/`
- Disallowed paths: authority docs, contracts, paper, raw, restricted, and canonical derived-table builder surfaces
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: derived-table and runtime contracts
- Upstream tasks or manifests: `T055`
- External references or systems: none

## Outputs

- Code: any analysis helpers needed within `analysis/`
- Data or manifests: analysis execution notes under `qc/`
- Reports or docs: notes on H1-H5 and P2 execution choices

## Success Criteria

- [ ] Fresh flagship outputs are produced from the new canonical tables
- [ ] No old evidence or old results are reused
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if paper integration depends on output naming or ordering

## Validation / Commands

- `python analysis/flagship_control_panel_margins.py --control-panel derived/control_panel_summary_simulamet.parquet --out qc/analysis_execution_simulamet.md`
- `python analysis/incidence_horizon_standardization.py --parent-units derived/parent_units_simulamet.parquet --append-report qc/analysis_execution_simulamet.md`
- `make gate`
- `make test`

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
