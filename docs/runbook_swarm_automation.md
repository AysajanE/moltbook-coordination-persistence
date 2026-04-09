# Swarm Automation Notes

This repo currently uses a manual Operator loop. The automation layer is intentionally limited.

## What is automated now

- file-based task planning under `.orchestrator/`
- deterministic gates via `make gate`
- deterministic tests via `make test`
- runtime task execution helpers in `scripts/swarm.py`
- Judge review logging in `scripts/swarm.py`
- lifecycle projection via `scripts/sweep_tasks.py`

## What is not automated now

- merge-to-main approval
- branch cleanup
- release assembly
- paper build orchestration
- unattended end-to-end research execution

## Safe usage

- Treat `python scripts/swarm.py plan` as the queue view.
- Use `python scripts/swarm.py run-task` and `python scripts/swarm.py judge-task` for task-local execution and review when appropriate.
- Keep Operator review between task completion and merge.
- Do not introduce pilot-repo release assumptions until a dedicated W9 task does so explicitly.

## Rollback expectations

- Automation does not supersede the authoritative task state. Correct `State:` first, then reconcile folder projection.
- Invalid run manifests or review logs should not be deleted silently; supersede them with corrected artifacts and note the reason in the task file.
- If an automated run branches from the wrong base or lands invalid work, stop the loop, revert the merge if needed, clean the affected worktree, and restart from a synchronized base branch.
