# Coordination Persistence Flagship

This is the standalone repo for the new flagship paper on coordination persistence in Moltbook.

Only four things were carried forward from the parent project:

- the locked Stage 3 theory and methods text
- the Stage 3 authority packet
- fresh-data collection, curation, and validation scaffolding
- two compact Stage 3 estimand scripts that remain provisional until they are rewritten against the flagship derived tables

The following were intentionally not carried forward:

- all old raw and curated data
- all old result tables, figures, summaries, and output directories
- the old discussion, conclusion, and manuscript wrappers
- the large legacy end-to-end analysis script and legacy presentation-layer scripts

## Repo surfaces

- `docs/` holds the authority packet, the fresh data acquisition plan, the migration manifest, the deployment plan, and the decision log.
- `scripts/` holds acquisition entrypoints plus the swarm runtime and gate tooling.
- `analysis/` holds audited collection, curation, validation, and analysis scripts. These scripts are not authority documents.
- `paper/` holds the locked theory and methods sections plus the LaTeX manuscript wrapper.
- `.orchestrator/` is the file-based control plane for Planner, Worker, Judge, and Operator.
- `contracts/` is the machine-readable project and runtime contract layer.
- `reports/status/` holds durable run manifests and Judge review logs.

## Authority order

When repo surfaces disagree, use this order:

1. `docs/stage3_theory_framework_packet.cleaned.md`
2. `docs/data_acquisition_plan.md`
3. locked paper sections under `paper/sections/`
4. `contracts/`
5. `.orchestrator/workstreams.md`
6. task files
7. handoff notes

The carried-over analysis scripts are helpers only. If they conflict with the Stage 3 packet or the acquisition plan, the packet and plan win.

## Current execution model

This is not yet a full autonomous research architecture. The current loop is:

1. Planner defines or updates the next task in `.orchestrator/`.
2. Operator launches the Worker task.
3. Worker executes one scoped task and produces a run manifest.
4. Operator launches Judge review.
5. Judge approves or returns the task.
6. Operator reviews, merges, sweeps, cleans the branch, and launches the next task.

## Current status

- Stage 3 theory and methods are locked.
- Old evidence is not active in this repo.
- A flagship-specific research swarm bootstrap is now in place for manual Operator, Worker, and Judge tasking.
- Fresh data acquisition and fresh feature construction remain the next scientific tasks.
- A new feature-builder for Stage 3 analysis has not yet been implemented here.

## Recommended starting sequence

1. Run `make gate` and `make test` to validate the swarm bootstrap.
2. Review `docs/swarm_deployment_plan.md`, `.orchestrator/workstreams.md`, and the seeded backlog.
3. Start with `T020` through `T050` in the archive-first SimulaMet path.
4. Keep `paper/main.tex` as the manuscript entrypoint; do not migrate the paper substrate just to match the pilot swarm.
