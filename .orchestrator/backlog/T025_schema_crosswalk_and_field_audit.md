---
task_id: "T025"
title: "Build schema crosswalk and required-field audit"
workstream: "W2"
task_kind: "freeze_qc"
allow_network: false
role: "Worker"
priority: "high"
dependencies:
  - "T020"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "analysis/hf_archive_schema_discovery.py"
  - "manifests/schema_crosswalk.yaml"
  - "qc/field_validation_simulamet.csv"
  - "qc/missingness_simulamet.csv"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
  - "derived/"
outputs:
  - "manifests/schema_crosswalk.yaml"
  - "qc/field_validation_simulamet.csv"
  - "qc/missingness_simulamet.csv"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T025 — Build schema crosswalk and required-field audit

## Context

The archive-first pipeline requires a harmonized schema contract before freeze construction and downstream QC.

## Assignment

- Workstream: W2
- Assigned role: Worker
- Suggested branch or worktree name: `T025_schema_crosswalk`
- Allowed paths: `analysis/hf_archive_schema_discovery.py`, `manifests/`, `qc/`
- Disallowed paths: authority docs, contracts, paper, and derived outputs
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: derived-table and runtime contracts
- Upstream tasks or manifests: `T020`, `manifests/simulamet_manifest.yaml`
- External references or systems: none

## Outputs

- Code: updated schema discovery script
- Data or manifests: `manifests/schema_crosswalk.yaml`, field validation, missingness audit
- Reports or docs: audit notes if anomalies need escalation

## Success Criteria

- [ ] Required fields are audited against the fresh archive
- [ ] The crosswalk is machine-readable and reproducible
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if field mapping choices affect downstream freeze logic

## Validation / Commands

- `python analysis/hf_archive_schema_discovery.py --raw-manifest manifests/simulamet_manifest.yaml --archive-name simulamet --out-crosswalk manifests/schema_crosswalk.yaml --out-field-validation qc/field_validation_simulamet.csv --out-missingness qc/missingness_simulamet.csv`
- `make gate`
- `make test`

## Status
- State: ready_for_review
- Last updated: 2026-04-09
## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
- 2026-04-09: Claimed by local swarm runtime on branch T025_schema_crosswalk_and_field_audit.
- 2026-04-09: Updated `analysis/hf_archive_schema_discovery.py` to resolve manifest v3 subset paths from `subsets.<subset>.files` as well as legacy `splits`, and to aggregate file-by-file null/type audits so schema drift does not fail Arrow-wide dataset unification.
- 2026-04-09: Generated `manifests/schema_crosswalk.yaml`, `qc/field_validation_simulamet.csv`, and `qc/missingness_simulamet.csv` with `python analysis/hf_archive_schema_discovery.py --raw-manifest manifests/simulamet_manifest.yaml --archive-name simulamet --out-crosswalk manifests/schema_crosswalk.yaml --out-field-validation qc/field_validation_simulamet.csv --out-missingness qc/missingness_simulamet.csv`.
- 2026-04-09: Required fields were found in all audited subsets. The only unmapped field was optional `submolts.community_id`. The crosswalk records type drift for several raw fields, including `comments.created_at` (`int64`, `string`, `large_string`) and `comments.parent_id` (`null`, `string`, `large_string`).
- 2026-04-09: Missingness audit highlights `comments.parent_comment_id` as null in 965067 / 1113910 rows (missing rate `0.8663778940848004`), consistent with many top-level comments in the raw export. Optional fields with large null shares include `agents.created_at_utc` and `agents.owner_x_handle`.
- 2026-04-09: Validation commands passed: `make gate` and `make test`.
- 2026-04-09: Added handoff note `.orchestrator/handoff/H_T025_manifest_v3_crosswalk.md` for downstream freeze construction. As of this run, no `reports/status/swarm_runs/T025*.json` artifact exists in this worktree, so the task remains `active` pending Operator/runtime manifest capture before `ready_for_review`.
- 2026-04-09: @human Runtime blocked: path_ownership_violation. Run manifest: reports/status/swarm_runs/T025_20260409T185549Z.json. ownership=raw[untracked]=outside_allowed_paths; restricted[untracked]=outside_allowed_paths
- 2026-04-09: Refreshed the declared audit outputs with `python analysis/hf_archive_schema_discovery.py --raw-manifest manifests/simulamet_manifest.yaml --archive-name simulamet --out-crosswalk manifests/schema_crosswalk.yaml --out-field-validation qc/field_validation_simulamet.csv --out-missingness qc/missingness_simulamet.csv`. The regenerated CSV audits were byte-stable; `manifests/schema_crosswalk.yaml` changed only in `generated_at_utc`.
- 2026-04-09: Re-ran `make gate` and `make test`; both passed again. `make test` completed with the same non-fatal PyArrow `sysctlbyname` sandbox warnings observed during schema discovery.
- 2026-04-09: The prior ownership block no longer reproduces in this worktree. `raw/` and `restricted/` remain symlinks, but `git status --short --untracked-files=all` reports only the refreshed crosswalk artifact and no out-of-scope untracked paths.
- 2026-04-09: Scientific conclusions are unchanged from the earlier audit: all required fields remain mapped; optional `submolts.community_id` remains unmapped; notable type drift remains in `comments.created_at` and `comments.parent_id`; `comments.parent_comment_id` missingness remains `965067 / 1113910` (`0.8663778940848004`). The existing handoff note remains accurate.
- 2026-04-09: @human Runtime blocked: gates_failed, path_ownership_violation. Run manifest: reports/status/swarm_runs/T025_20260409T190205Z.json. ownership=raw[committed]=outside_allowed_paths; restricted[committed]=outside_allowed_paths
- 2026-04-09: Operational repair reran `python analysis/hf_archive_schema_discovery.py --raw-manifest manifests/simulamet_manifest.yaml --archive-name simulamet --out-crosswalk manifests/schema_crosswalk.yaml --out-field-validation qc/field_validation_simulamet.csv --out-missingness qc/missingness_simulamet.csv`. The regenerated CSV audits remained byte-stable; `manifests/schema_crosswalk.yaml` changed only in `generated_at_utc` (`2026-04-09T19:06:31.167000+00:00`).
- 2026-04-09: Re-ran `make gate` and `make test`; both passed. `make test` completed with `Ran 25 tests in 8.497s` / `OK` and repeated the same non-fatal PyArrow `sysctlbyname` sandbox warnings observed in prior runs.
- 2026-04-09: This repair run was executed directly in the worktree rather than through the local swarm runtime, so no new durable run manifest was recorded under `reports/status/swarm_runs/`. Operator should capture a fresh runtime-owned manifest before review. Declared outputs confirmed present at `manifests/schema_crosswalk.yaml`, `qc/field_validation_simulamet.csv`, and `qc/missingness_simulamet.csv`; the existing handoff note remains accurate.
- 2026-04-09: Runtime passed: preflight, fresh outputs, gates, manifests, and run manifest are present. Ready for Judge review. Run manifest: reports/status/swarm_runs/T025_20260409T190559Z.json
