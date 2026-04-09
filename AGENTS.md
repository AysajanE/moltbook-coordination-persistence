# AGENTS.md — Moltbook Coordination Persistence Operating Manual

This repo is the standalone Moltbook coordination persistence research environment. Scientific rigor, provenance, and contract discipline govern all work.

## Source precedence

1. `README.md`
2. `contracts/project.yaml`, `contracts/framework.json`, and applicable `contracts/*`
3. locked paper sections under `paper/sections/`
4. `.orchestrator/workstreams.md`
5. the assigned task file
6. `.orchestrator/handoff/` notes

Detailed private theory and acquisition materials are intentionally not published in this repository.

If guidance still conflicts after applying that order, stop, set `State: blocked`, and record the smallest `@human` question needed to unblock the task.

## Roles

- `Planner`: decomposes work, creates or rewrites task files, maintains `.orchestrator/workstreams.md`, and owns lifecycle projection across `.orchestrator/{backlog,active,integration_ready,ready_for_review,blocked,done}/`.
- `Worker`: executes exactly one assigned task in one isolated branch or worktree, edits only within `allowed_paths`, and may edit only `## Status` and `## Notes / Decisions` in the task file.
- `Judge`: reruns declared gates, verifies outputs and provenance, writes the review decision, and is the only role allowed to set `State: done`.
- `Operator`: owns environment preflight, runtime supervision, Judge deployment, review logging, sweep hygiene, merge preparation, branch cleanup, and launch of the next task. Operator does not redefine scientific contracts and does not mark scientific work `done`.

Default if unclear: `Worker`.

## Non-negotiable repo rules

- Do not import old data, old result tables, old figures, old discussion text, or old conclusions from the parent repo.
- Fresh empirical evidence must be generated in this repo.
- `analysis/flagship_control_panel_margins.py` and `analysis/incidence_horizon_standardization.py` are provisional legacy carryovers. They do not override the public contracts or unpublished internal research materials.
- Do not fabricate results, file contents, data provenance, or manuscript claims.
- Keep edits narrow and additive. Prefer manifests, review logs, and decision entries over silent replacements.
- Do not commit raw or restricted archive material. Keep `raw/`, `frozen/`, `restricted/`, `data_raw/`, `data_curated/`, `data_features/`, and `outputs/` out of git.
- If a script or document is not clearly aligned to the current project design, leave it out rather than porting it forward.
- `State:` inside each task file is authoritative. Folder placement under `.orchestrator/` is only a projection maintained by Planner or Operator.
- `integration_ready` is only for W0 or W9 interface tasks that are explicitly allowlisted downstream. Empirical archive artifacts under `raw/`, `frozen/`, `qc/`, `derived/`, or `restricted/` may not use `integration_ready`.

## Manual operator loop

The current swarm deployment is intentionally semi-automated. Operator remains responsible for:

1. launching the Worker task
2. launching the Judge review
3. reviewing the result bundle
4. merging to `main`
5. sweeping the task file
6. cleaning out the task branch or worktree
7. launching the next task

## Stop conditions

Block immediately with `@human` if:

- the public repo materials and an implementation surface disagree
- a task would require edits outside `allowed_paths`
- credentials, access, or missing tools are required
- a new scientific assumption is needed but not yet recorded in `contracts/assumptions.md`
- a task depends on unpublished internal materials that are not available in this repo
- a proposed shortcut would bypass provenance, QC, or review discipline
