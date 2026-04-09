# AGENTS.md — Moltbook Coordination Persistence Operating Manual

You are operating inside a repo-native research operating system. Coordination happens through files, contracts, and git history. The repo is the shared memory.

This repo is the standalone Moltbook coordination persistence research environment. Scientific rigor, provenance, and contract discipline govern all work.

## Source-of-truth precedence

1. `README.md`
2. `contracts/project.yaml`, `contracts/framework.json`, and applicable `contracts/*`
3. locked paper sections under `paper/sections/`
4. `.orchestrator/workstreams.md`
5. the assigned task file
6. `.orchestrator/handoff/` notes

Detailed private theory and acquisition materials are intentionally not published in this repository.

If guidance still conflicts after applying that order, stop, set `State: blocked`, and record the smallest `@human` question needed to unblock the task.

## Declare exactly one role

- **Operator** — owns environment preflight, runtime supervision, Judge deployment, sweep hygiene, repair handling, run/review/release logging, catalog refresh, merge preparation, branch cleanup, launch of the next task, and release assembly. May set `active` or `blocked` on operational grounds and may set `ready_for_review` on Operator-owned tasks. May never redefine scientific contracts or mark scientific work `done`.

- **Planner** — decomposes work, creates or rewrites task files, maintains `.orchestrator/workstreams.md`, and owns lifecycle projection across `.orchestrator/{backlog,active,integration_ready,ready_for_review,blocked,done}/`.
- **Worker** — executes exactly one assigned task in one isolated branch/worktree, edits only within `allowed_paths` plus task `## Status`, task `## Notes / Decisions`, and optional handoff notes.
- **Judge** — reruns declared gates, verifies outputs and provenance, writes review decisions, and is the only role allowed to mark a task `done`.

Default if unclear: **Worker**.

## Non-negotiable repo rules

- No agent-to-agent chat coordination. Use task files, contracts, manifests, review logs, and handoff notes.
- Do not edit outside `allowed_paths`.
   - Editing your assigned task file in `## Status` and `## Notes / Decisions` and adding a handoff note in `.orchestrator/handoff/` are always allowed.
- Do not import old data, old result tables, old figures, old discussion text, or old conclusions from the parent repo.
- Fresh empirical evidence must be generated in this repo.
- `analysis/flagship_control_panel_margins.py` and `analysis/incidence_horizon_standardization.py` are provisional legacy carryovers. They do not override the public contracts or unpublished internal research materials.
- Do not fabricate results, file contents, data provenance, or manuscript claims.
- Keep edits narrow and additive. Prefer manifests, review logs, and decision entries over silent replacements.
- Do not commit raw or restricted archive material. Keep `raw/`, `frozen/`, `restricted/`, `data_raw/`, `data_curated/`, `data_features/`, and `outputs/` out of git.
- If a script or document is not clearly aligned to the current project design, leave it out rather than porting it forward.
- `State:` inside each task file is authoritative. Folder placement under `.orchestrator/` is only a projection maintained by Planner or Operator.
- `integration_ready` is only for W0 or W9 interface tasks that are explicitly allowlisted downstream. Empirical archive artifacts under `raw/`, `frozen/`, `qc/`, `derived/`, or `restricted/` may not use `integration_ready`.

## State and review semantics

- `State:` inside `## Status` is authoritative.
- Folder placement under `.orchestrator/` is a Planner/Operator-maintained projection.
- Valid states: `backlog`, `active`, `integration_ready`, `ready_for_review`, `blocked`, `done`.
- `integration_ready` is only for interface/export tasks whose downstream consumers explicitly list the task in `integration_ready_dependencies`.
- `ready_for_review` means declared outputs exist, declared gates pass, required manifests exist, and a durable run manifest exists under `reports/status/swarm_runs/`.
- `done` requires Judge approval plus a review bundle: task file + run manifest + Judge review log under `reports/status/reviews/` + handoff note if needed.

## Branch and worktree discipline

- One task, one branch, one worktree.
- Use task-shaped names such as `T040_build_parent_units`.
- Do not bundle multiple tasks into one branch or PR.
- Rebase or restart long-running sessions after mainline changes or repeated gate failures.

## Completion checklist

Before leaving `active`, record:

- files changed or created
- reproduction commands
- gate/test commands run and a brief outcome summary
- assumptions, limitations, and blockers
- downstream handoff notes when another task depends on your outputs

Put the short version in the task file and the durable downstream version in `.orchestrator/handoff/` when needed.

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
