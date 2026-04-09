---
task_id: "T075"
title: "Integrate fresh evidence into LaTeX paper surface"
workstream: "W5"
task_kind: "writing"
allow_network: false
role: "Worker"
priority: "medium"
dependencies:
  - "T060"
  - "T072"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "paper/main.tex"
  - "paper/references.bib"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "paper/sections/model.tex"
  - "paper/sections/methods.tex"
  - "paper/sections/supplementary_material.tex"
  - "contracts/"
outputs:
  - "paper/main.tex"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T075 — Integrate fresh evidence into LaTeX paper surface

## Context

The manuscript wrapper stays in LaTeX. Fresh evidence should be integrated without rewriting the locked Stage 3 sections.

## Assignment

- Workstream: W5
- Assigned role: Worker
- Suggested branch or worktree name: `T075_paper_integration`
- Allowed paths: `paper/main.tex`, `paper/references.bib`
- Disallowed paths: authority docs, locked paper sections, contracts
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: project and runtime contracts
- Upstream tasks or manifests: `T060`, `T072`
- External references or systems: none

## Outputs

- Code: none
- Data or manifests: none
- Reports or docs: updated manuscript wrapper and citations

## Success Criteria

- [ ] Fresh flagship evidence is integrated without altering locked Stage 3 sections
- [ ] The LaTeX wrapper remains the manuscript entrypoint
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if later release-layer design depends on paper structure

## Validation / Commands

- `latexmk -pdf -interaction=nonstopmode paper/main.tex`
- `make gate`
- `make test`

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
