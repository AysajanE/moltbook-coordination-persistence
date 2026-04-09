---
task_id: T___
title: "<title>"
workstream: W__
task_kind: ""  # contracts|acquisition|freeze_qc|derived|analysis|writing|ops|interface
allow_network: false  # true requires workstream allowlist in contracts/framework.json
role: Worker  # use Operator only for W9 ops tasks
priority: medium
dependencies: []
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "<path/to/file_or_small_prefix>"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "paper/sections/model.tex"
  - "paper/sections/methods.tex"
  - "paper/sections/supplementary_material.tex"
  - "contracts/"
  - ".orchestrator/templates/"
  - ".orchestrator/workstreams.md"
  - "reports/status/"
outputs:
  - "<output path>"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need credentials"
  - "Need to edit outside allowed paths"
---

# Task T___ — <title>

## Context

Describe why this task exists and which Stage 3 artifact or execution surface it advances.

## Assignment

- Workstream:
- Assigned role:
- Suggested branch or worktree name:
- Allowed paths:
- Disallowed paths:
- Stop conditions:

## Inputs

- Authority surfaces:
- Contracts:
- Upstream tasks or manifests:
- External references or systems:

## Outputs

- Code:
- Data or manifests:
- Reports or docs:

## Success Criteria

- [ ] Declared outputs exist at the paths above
- [ ] Reproduction commands are recorded
- [ ] Declared gates pass
- [ ] Assumptions and limitations are recorded

## Review Bundle Requirements

- [ ] If this task produces artifacts, a durable run manifest exists under `reports/status/swarm_runs/`
- [ ] Judge review is recorded under `reports/status/reviews/`
- [ ] Any downstream-critical guidance is captured in `.orchestrator/handoff/`

## Validation / Commands

- `make gate`
- `make test`
- Add task-specific commands here.

## Edit rules

- Workers and Operators edit only `## Status` and `## Notes / Decisions`.
- Planner and Operator handle folder moves via sweep or `git mv`.
- `integration_ready` may be used only for W0 or W9 interface tasks named in downstream `integration_ready_dependencies`.
- Empirical outputs under `raw/`, `frozen/`, `qc/`, `derived/`, or `restricted/` may not use `integration_ready`.

## Status

- State: backlog | active | integration_ready | ready_for_review | blocked | done
- Semantics:
  - `integration_ready`: W0 or W9 interface task only; downstream allowlist required
  - `ready_for_review`: outputs exist, declared gates pass, required manifests exist, and a run manifest exists
  - `done`: Judge-approved
- Last updated: YYYY-MM-DD

## Notes / Decisions

- YYYY-MM-DD: <progress note, decision, or blocker; include `@human` when needed>
