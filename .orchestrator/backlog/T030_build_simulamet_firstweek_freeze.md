---
task_id: "T030"
title: "Construct SimulaMet first-week latest-state freeze"
workstream: "W2"
task_kind: "freeze_qc"
allow_network: false
role: "Worker"
priority: "high"
dependencies:
  - "T020"
  - "T025"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "analysis/hf_archive_curate.py"
  - "frozen/"
  - "qc/"
disallowed_paths:
  - "docs/stage3_theory_framework_packet.cleaned.md"
  - "docs/data_acquisition_plan.md"
  - "contracts/"
  - "paper/"
  - "derived/"
outputs:
  - "frozen/simulamet_firstweek_lateststate/..."
  - "qc/simulamet_dedup_conflicts.csv"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T030 — Construct SimulaMet first-week latest-state freeze

## Context

The canonical analysis window requires a fresh first-week latest-state freeze built from the newly acquired archive.

## Assignment

- Workstream: W2
- Assigned role: Worker
- Suggested branch or worktree name: `T030_simulamet_freeze`
- Allowed paths: `analysis/hf_archive_curate.py`, `frozen/`, `qc/`
- Disallowed paths: authority docs, contracts, paper, and derived outputs
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority docs: `docs/stage3_theory_framework_packet.cleaned.md`, `docs/data_acquisition_plan.md`
- Contracts: schema contracts and runtime contracts
- Upstream tasks or manifests: `T020`, `T025`, `manifests/schema_crosswalk.yaml`
- External references or systems: none

## Outputs

- Code: updated curation script
- Data or manifests: canonical freeze, dedup conflict log
- Reports or docs: notes on latest-state reconstruction decisions

## Success Criteria

- [ ] The first-week canonical freeze is constructed from the fresh archive
- [ ] Deduplication follows the acquisition plan and conflicts are logged
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if freeze semantics affect downstream QC or derived builders

## Validation / Commands

- `make gate`
- `make test`
- Add task-specific freeze commands here.

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
