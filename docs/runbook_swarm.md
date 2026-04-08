# Swarm Runbook (manual flagship v1)

Use this runbook for normal flagship repo task delivery. The current deployment is intentionally semi-automated: Operator remains in the loop between every task.

## Preflight

- Work inside a sandboxed environment that contains only this repo.
- Sync the base branch before starting.
- Run `make gate` and `make test`.
- Review `docs/stage3_theory_framework_packet.cleaned.md`, `docs/data_acquisition_plan.md`, `contracts/project.yaml`, and `contracts/framework.json`.
- Verify required tools:
  - always: `git`, `python`
  - when manuscript tasks are in scope: the local LaTeX toolchain used by the repo

## 1) Planner scopes the queue

- Create or update task files using the templates under `.orchestrator/templates/`.
- Keep `allowed_paths` narrow and keep dependencies aligned with the Stage 3 artifact DAG.
- Use `integration_ready` only for W0 or W9 interface tasks that truly need early downstream consumption.

## 2) Operator prepares execution

- Verify git identity and, if needed, GitHub auth.
- Create one worktree per active task.
- Decide whether the run will be manual or via `scripts/swarm.py`.
- Keep local executor logs under `.orchestrator/runtime_logs/`.

Suggested worktree pattern:

    TASK_ID=T020
    git worktree add ../wt-${TASK_ID} -b ${TASK_ID}_short_name .

## 3) Worker executes exactly one task

- Run from the task worktree.
- Edit only allowed repo paths plus task `## Status` and `## Notes / Decisions`.
- Record reproduction commands, assumptions, and blockers.
- Stop instead of improvising on Stage 3 or contract ambiguity.
- Write a handoff note when downstream tasks need durable guidance.

## 4) Judge reviews

- Rerun the declared gates.
- Verify outputs, required manifests, and the task success criteria against the Stage 3 packet and acquisition plan.
- Confirm the review bundle:
  - task markdown
  - run manifest under `reports/status/swarm_runs/`
  - review log under `reports/status/reviews/`
  - handoff note if needed
- Set `State: done` only when the task is scientifically acceptable.

## 5) Operator closes the loop

- Review the Judge output.
- Merge the approved branch to `main`.
- Run `python scripts/sweep_tasks.py`.
- Clean up the finished branch or worktree.
- Launch the next ready task.

## 6) Current flagship queue

1. `T020` SimulaMet raw acquisition
2. `T025` schema discovery and crosswalk
3. `T030` canonical first-week freeze
4. `T035` archive QC packet
5. `T040` `parent_units`
6. `T045` `control_panel_summary`
7. `T050` `thread_geometry`, `periodicity_input`, and `archive_metadata_audit`
8. `T055` rewrite legacy estimand scripts
9. `T060` flagship H1–H5 and P2 outputs
10. `T065` MoltNet acquisition
11. `T070` MoltNet freeze harmonization and QC
12. `T072` MoltNet derived tables and geometry transport decision
13. `T075` LaTeX manuscript integration
14. `T080` deferred release-layer design

## Safety defaults

- Keep unattended execution inside sandboxed environments only.
- Prefer short runs and fresh sessions after merges or repeated gate failures.
- Do not bypass QC, provenance, or Judge review to unblock analysis or writing work.
