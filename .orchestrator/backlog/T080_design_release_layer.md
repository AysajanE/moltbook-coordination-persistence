---
task_id: "T080"
title: "Design flagship-specific release layer"
workstream: "W9"
task_kind: "ops"
allow_network: false
role: "Operator"
priority: "low"
dependencies:
  - "T075"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "docs/runbook_swarm.md"
  - "docs/runbook_swarm_automation.md"
  - "scripts/quality_gates.py"
  - "scripts/swarm.py"
  - "reports/status/release_layer_design.md"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "paper/sections/model.tex"
  - "paper/sections/methods.tex"
  - "paper/sections/supplementary_material.tex"
outputs:
  - "reports/status/release_layer_design.md"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T080 — Design flagship-specific release layer

## Context

Release assembly is explicitly deferred until the scientific path is stable. When it is introduced, it must be designed for this flagship repo rather than copied from the pilot.

## Assignment

- Workstream: W9
- Assigned role: Operator
- Suggested branch or worktree name: `T080_release_layer`
- Allowed paths: runtime docs, runtime scripts, `reports/status/`, `contracts/`
- Disallowed paths: authority docs and locked paper sections
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: project and framework contracts
- Upstream tasks or manifests: `T075`
- External references or systems: none

## Outputs

- Code: release-layer runtime or gate changes if later authorized
- Data or manifests: release status design artifacts under `reports/status/`
- Reports or docs: revised runbooks or contracts for the release layer

## Success Criteria

- [ ] Any release-layer design is specific to this flagship repo
- [ ] No pilot Quarto or catalog assumptions are imported without explicit authorization
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if a later release task is split out

## Validation / Commands

- `python scripts/quality_gates.py --json > reports/status/release_layer_design.md`
- `make gate`
- `make test`

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
