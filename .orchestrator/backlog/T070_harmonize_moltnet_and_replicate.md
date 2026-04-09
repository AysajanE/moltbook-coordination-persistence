---
task_id: "T070"
title: "Build MoltNet freezes and QC packet"
workstream: "W2"
task_kind: "freeze_qc"
allow_network: false
role: "Worker"
priority: "medium"
dependencies:
  - "T065"
  - "T050"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "analysis/"
  - "frozen/"
  - "qc/"
  - "manifests/"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
outputs:
  - "frozen/moltnet_firstweek_aligned/..."
  - "frozen/moltnet_fullrelease/..."
  - "qc/archive_qc_report_moltnet.md"
  - "qc/gap_registry_moltnet.csv"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T070 — Build MoltNet freezes and QC packet

## Context

Replication requires aligned MoltNet freeze construction and QC before any MoltNet derived tables are built.

## Assignment

- Workstream: W2
- Assigned role: Worker
- Suggested branch or worktree name: `T070_moltnet_freeze_qc`
- Allowed paths: `analysis/`, `frozen/`, `qc/`, `manifests/`
- Disallowed paths: authority docs, contracts, and paper surfaces
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: acquisition-plan QC requirements and runtime contracts
- Upstream tasks or manifests: `T065`, `T050`
- External references or systems: none

## Outputs

- Code: harmonization and validation logic under `analysis/`
- Data or manifests: aligned and full-release MoltNet freezes plus the MoltNet QC packet
- Reports or docs: notes on archive comparability and any blockers for downstream derived-table work

## Success Criteria

- [ ] MoltNet freeze and QC follow the same flagship provenance discipline
- [ ] Downstream derived-table work is unblocked or clearly constrained
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if downstream MoltNet derived-table work depends on archive caveats

## Validation / Commands

- `make gate`
- `make test`
- Add task-specific MoltNet freeze and QC commands here.

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
