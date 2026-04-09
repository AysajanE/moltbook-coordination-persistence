---
task_id: "T025"
title: "Build schema crosswalk and required-field audit"
workstream: "W2"
task_kind: "freeze_qc"
allow_network: false
role: "Worker"
priority: "high"
dependencies:
  - "T020"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "analysis/hf_archive_schema_discovery.py"
  - "manifests/"
  - "qc/"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
  - "derived/"
outputs:
  - "manifests/schema_crosswalk.yaml"
  - "qc/field_validation_simulamet.csv"
  - "qc/missingness_simulamet.csv"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T025 — Build schema crosswalk and required-field audit

## Context

The archive-first pipeline requires a harmonized schema contract before freeze construction and downstream QC.

## Assignment

- Workstream: W2
- Assigned role: Worker
- Suggested branch or worktree name: `T025_schema_crosswalk`
- Allowed paths: `analysis/hf_archive_schema_discovery.py`, `manifests/`, `qc/`
- Disallowed paths: authority docs, contracts, paper, and derived outputs
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: derived-table and runtime contracts
- Upstream tasks or manifests: `T020`, `manifests/simulamet_manifest.yaml`
- External references or systems: none

## Outputs

- Code: updated schema discovery script
- Data or manifests: `manifests/schema_crosswalk.yaml`, field validation, missingness audit
- Reports or docs: audit notes if anomalies need escalation

## Success Criteria

- [ ] Required fields are audited against the fresh archive
- [ ] The crosswalk is machine-readable and reproducible
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if field mapping choices affect downstream freeze logic

## Validation / Commands

- `make gate`
- `make test`
- Add task-specific schema audit commands here.

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
