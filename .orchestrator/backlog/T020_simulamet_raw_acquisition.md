---
task_id: "T020"
title: "Acquire canonical SimulaMet archive and provenance manifests"
workstream: "W1"
task_kind: "acquisition"
allow_network: true
role: "Worker"
priority: "high"
dependencies: []
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "scripts/download_moltbook_observatory_archive.py"
  - "raw/"
  - "manifests/"
  - "restricted/"
disallowed_paths:
  - "docs/stage3_theory_framework_packet.cleaned.md"
  - "docs/data_acquisition_plan.md"
  - "contracts/"
  - "paper/"
  - "qc/"
  - "derived/"
outputs:
  - "raw/simulamet/YYYYMMDD/..."
  - "manifests/simulamet_manifest.yaml"
  - "restricted/raw_to_hash_mapping.parquet"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need credentials"
  - "Need to edit outside allowed paths"
---

# Task T020 — Acquire canonical SimulaMet archive and provenance manifests

## Context

The flagship pipeline begins with fresh archive acquisition inside this repo. The Stage 3 packet forbids reuse of old evidence.

## Assignment

- Workstream: W1
- Assigned role: Worker
- Suggested branch or worktree name: `T020_simulamet_acquisition`
- Allowed paths: `scripts/download_moltbook_observatory_archive.py`, `raw/`, `manifests/`, `restricted/`
- Disallowed paths: authority docs, contracts, paper surfaces, QC, and derived outputs
- Stop conditions: authority ambiguity, credentials, or path-boundary breaches

## Inputs

- Authority docs: `docs/stage3_theory_framework_packet.cleaned.md`, `docs/data_acquisition_plan.md`
- Contracts: `contracts/project.yaml`, `contracts/framework.json`
- Upstream tasks or manifests: none
- External references or systems: canonical SimulaMet archive source

## Outputs

- Code: updated acquisition script if needed
- Data or manifests: fresh local archive under `raw/`, manifest under `manifests/`, restricted mapping under `restricted/`
- Reports or docs: acquisition notes in task log or handoff if needed

## Success Criteria

- [ ] The archive is freshly acquired inside this repo
- [ ] Immutable provenance is recorded in `manifests/simulamet_manifest.yaml`
- [ ] Restricted raw-to-hash linkage is recorded separately
- [ ] Gates pass and reproduction commands are logged

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if downstream acquisition quirks matter

## Validation / Commands

- `make gate`
- `make test`
- Add task-specific acquisition commands here.

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
