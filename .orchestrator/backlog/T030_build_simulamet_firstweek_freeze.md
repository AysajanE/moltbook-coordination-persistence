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
  - "frozen/simulamet_firstweek_lateststate/"
  - "qc/simulamet_dedup_conflicts.csv"
  - "manifests/simulamet_firstweek_freeze_manifest.json"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
  - "derived/"
outputs:
  - "frozen/simulamet_firstweek_lateststate/..."
  - "qc/simulamet_dedup_conflicts.csv"
  - "manifests/simulamet_firstweek_freeze_manifest.json"
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
- Allowed paths: `analysis/hf_archive_curate.py`, `frozen/simulamet_firstweek_lateststate/`, `qc/simulamet_dedup_conflicts.csv`, `manifests/simulamet_firstweek_freeze_manifest.json`
- Disallowed paths: authority docs, contracts, paper, and derived outputs
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: schema contracts and runtime contracts
- Upstream tasks or manifests: `T020`, `T025`, `manifests/schema_crosswalk.yaml`
- External references or systems: none

## Outputs

- Code: updated `analysis/hf_archive_curate.py` only if required by the freeze build
- Data or manifests: `frozen/simulamet_firstweek_lateststate/...`, `qc/simulamet_dedup_conflicts.csv`, `manifests/simulamet_firstweek_freeze_manifest.json`
- Reports or docs: none beyond the declared outputs above

## Success Criteria

- [ ] The first-week canonical freeze is constructed from the fresh archive
- [ ] Deduplication follows the acquisition plan and conflicts are logged
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if freeze semantics affect downstream QC or derived builders

## Validation / Commands

- `python analysis/hf_archive_curate.py --raw-manifest manifests/simulamet_manifest.yaml --schema-crosswalk manifests/schema_crosswalk.yaml --archive-name simulamet --out-root frozen/simulamet_firstweek_lateststate --window-start 2026-01-28T00:00:00Z --window-end 2026-02-05T00:00:00Z --dedup-conflicts-out qc/simulamet_dedup_conflicts.csv --freeze-manifest-out manifests/simulamet_firstweek_freeze_manifest.json`
- `make gate`
- `make test`

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
