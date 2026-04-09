---
task_id: "T065"
title: "Acquire MoltNet archive with flagship provenance discipline"
workstream: "W1"
task_kind: "acquisition"
allow_network: true
role: "Worker"
priority: "medium"
dependencies:
  - "T060"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "scripts/run_moltbook_live_campaign.py"
  - "analysis/moltbook_api_collect.py"
  - "analysis/moltbook_api_curate.py"
  - "analysis/moltbook_api_validate.py"
  - "raw/"
  - "manifests/"
  - "restricted/"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
  - "derived/"
outputs:
  - "raw/moltnet/v2026-02-28/..."
  - "manifests/moltnet_manifest.yaml"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need credentials"
  - "Need to edit outside allowed paths"
---

# Task T065 — Acquire MoltNet archive with flagship provenance discipline

## Context

The replication archive is acquired only after the SimulaMet core path is stable.

## Assignment

- Workstream: W1
- Assigned role: Worker
- Suggested branch or worktree name: `T065_moltnet_acquisition`
- Allowed paths: MoltNet acquisition scripts, `raw/`, `manifests/`, `restricted/`
- Disallowed paths: authority docs, contracts, paper, and derived outputs
- Stop conditions: Stage 3 ambiguity, credentials, or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: `contracts/project.yaml`, `contracts/framework.json`
- Upstream tasks or manifests: `T060`
- External references or systems: MoltNet release surfaces or public API

## Outputs

- Code: updated MoltNet acquisition or curation scripts if needed
- Data or manifests: fresh local MoltNet archive and manifest
- Reports or docs: notes on provenance differences relative to SimulaMet

## Success Criteria

- [ ] MoltNet is acquired with the same provenance discipline as SimulaMet
- [ ] The manifest is complete and reproducible
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if archive differences matter for harmonization

## Validation / Commands

- `make gate`
- `make test`
- Add task-specific MoltNet acquisition commands here.

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
