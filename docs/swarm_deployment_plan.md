# Flagship Swarm Deployment Plan

## Purpose

Deploy the current research swarm framework into this repo as a flagship-specific project instance. This deployment is not a repo migration into the STR pilot and is not yet a claim of full autonomous research.

## Authority lock

1. `docs/stage3_theory_framework_packet.cleaned.md`
2. `docs/data_acquisition_plan.md`
3. locked paper sections under `paper/sections/`
4. `contracts/project.yaml`, `contracts/framework.json`, and applicable `contracts/*`
5. `.orchestrator/workstreams.md`
6. task files
7. `.orchestrator/handoff/`

If any carried-over implementation contradicts the Stage 3 packet, the packet wins.

## Deployment decision

The swarm framework is reused here as an execution layer. The flagship science stays in this repo, the Operator loop stays manual, and the paper substrate stays LaTeX.

## File migration map

### Ported from the pilot with minimal or no changes

- `Makefile`
- `.orchestrator/README.md`
- `.orchestrator/AGENTS.md`
- lifecycle README stubs under `.orchestrator/`
- `.orchestrator/templates/handoff_template.md`
- `contracts/AGENTS.md`
- `contracts/assumptions.md`
- `contracts/schemas/swarm_run_manifest_v1.yaml`
- `contracts/schemas/judge_review_log_v1.yaml`
- `scripts/AGENTS.md`
- `scripts/sweep_tasks.py`
- `tests/README.md`

### Rewritten for the flagship repo

- root `AGENTS.md`
- root `README.md`
- `.gitignore`
- `.orchestrator/templates/task_template.md`
- `.orchestrator/workstreams.md`
- `contracts/README.md`
- `contracts/CHANGELOG.md`
- `contracts/data_dictionary.md`
- `contracts/framework.json`
- `contracts/project.yaml`
- `contracts/schemas/*.yaml` for flagship derived tables
- `docs/prompts/*.md`
- `docs/runbook_swarm.md`
- `docs/runbook_swarm_automation.md`
- `reports/AGENTS.md`
- `reports/status/README.md`
- `reports/status/swarm_runs/README.md`
- `reports/status/reviews/README.md`
- `scripts/swarm.py`
- `scripts/quality_gates.py`
- runtime tests under `tests/`

### Explicitly deferred

- release assembly and release manifests
- catalog compilation
- Quarto paper surfaces
- pilot STR schemas, registry contracts, and modeling or hybrid contracts

## Workstreams

- `W0 Authority / Contracts`
- `W1 Acquisition / Provenance`
- `W2 Freeze / Schema / QC`
- `W3 Derived Tables`
- `W4 Analysis / Diagnostics`
- `W5 Writing / Paper Integration`
- `W9 Ops / Swarm Runtime`

`integration_ready` is restricted to W0 and W9 interface tasks. Empirical outputs under `raw/`, `frozen/`, `qc/`, `derived/`, or `restricted/` are never integration-ready.

## Seed backlog

- `T020` SimulaMet raw acquisition and manifesting
- `T025` schema discovery and crosswalk
- `T030` first-week latest-state freeze construction
- `T035` archive QC packet and benchmark validation
- `T040` `parent_units` builder
- `T045` `control_panel_summary` builder
- `T050` `thread_geometry`, `periodicity_input`, and `archive_metadata_audit`
- `T055` rewrite legacy estimand scripts against canonical derived tables
- `T060` produce flagship H1–H5 and P2 outputs
- `T065` MoltNet acquisition
- `T070` MoltNet freeze harmonization and QC
- `T072` MoltNet derived tables and geometry transport decision
- `T075` LaTeX paper integration
- `T080` deferred release-layer design

## Phase 1 scope

This deployment executes the bootstrap layer only:

- control plane
- contracts
- prompts and runbooks
- runtime scripts and gates
- workstreams
- backlog seeding
- deterministic verification

It does not yet execute the scientific archive pipeline.

## Phase 1 execution status

- 2026-04-08: deployment plan saved locally in this repo
- 2026-04-08: flagship-specific swarm control plane installed
- 2026-04-08: contracts, prompts, runbooks, and runtime scripts rewritten for Stage 3 precedence
- 2026-04-08: backlog seeded from `T020` through `T080`
- 2026-04-08: verification completed with `make gate`, `make test`, and `python scripts/swarm.py plan`
