---
task_id: "T030"
title: "Construct SimulaMet first-week latest-state freeze"
workstream: "W2"
task_kind: "freeze_qc"
allow_network: false
role: "Worker"
priority: "high"
dependencies:
  - "T020"
  - "T025"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "analysis/hf_archive_curate.py"
  - "frozen/simulamet_firstweek_lateststate/"
  - "qc/simulamet_dedup_conflicts.csv"
  - "manifests/simulamet_firstweek_freeze_manifest.json"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
  - "derived/"
outputs:
  - "frozen/simulamet_firstweek_lateststate/..."
  - "qc/simulamet_dedup_conflicts.csv"
  - "manifests/simulamet_firstweek_freeze_manifest.json"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T030 — Construct SimulaMet first-week latest-state freeze

## Context

The canonical analysis window requires a fresh first-week latest-state freeze built from the newly acquired archive.

## Assignment

- Workstream: W2
- Assigned role: Worker
- Suggested branch or worktree name: `T030_simulamet_freeze`
- Allowed paths: `analysis/hf_archive_curate.py`, `frozen/simulamet_firstweek_lateststate/`, `qc/simulamet_dedup_conflicts.csv`, `manifests/simulamet_firstweek_freeze_manifest.json`
- Disallowed paths: authority docs, contracts, paper, and derived outputs
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: schema contracts and runtime contracts
- Upstream tasks or manifests: `T020`, `T025`, `manifests/schema_crosswalk.yaml`
- External references or systems: none

## Outputs

- Code: updated `analysis/hf_archive_curate.py` only if required by the freeze build
- Data or manifests: `frozen/simulamet_firstweek_lateststate/...`, `qc/simulamet_dedup_conflicts.csv`, `manifests/simulamet_firstweek_freeze_manifest.json`
- Reports or docs: none beyond the declared outputs above

## Success Criteria

- [ ] The first-week canonical freeze is constructed from the fresh archive
- [ ] Deduplication follows the acquisition plan and conflicts are logged
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if freeze semantics affect downstream QC or derived builders

## Validation / Commands

- `python analysis/hf_archive_curate.py --raw-manifest manifests/simulamet_manifest.yaml --schema-crosswalk manifests/schema_crosswalk.yaml --archive-name simulamet --out-root frozen/simulamet_firstweek_lateststate --window-start 2026-01-28T00:00:00Z --window-end 2026-02-05T00:00:00Z --dedup-conflicts-out qc/simulamet_dedup_conflicts.csv --freeze-manifest-out manifests/simulamet_firstweek_freeze_manifest.json`
- `make gate`
- `make test`

## Status
- State: done
- Last updated: 2026-04-09
## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
- 2026-04-09: Claimed by local swarm runtime on branch T030_build_simulamet_firstweek_freeze.
- 2026-04-09: Updated `analysis/hf_archive_curate.py` so the freeze builder resolves both manifest subset layouts (`splits` and `files`) and loads per-file parquet snapshots through pandas concatenation before canonical coercion. This avoids the manifest-v3 path mismatch and the raw Arrow schema drift observed in SimulaMet comments.
- 2026-04-09: Ran `python analysis/hf_archive_curate.py --raw-manifest manifests/simulamet_manifest.yaml --schema-crosswalk manifests/schema_crosswalk.yaml --archive-name simulamet --out-root frozen/simulamet_firstweek_lateststate --window-start 2026-01-28T00:00:00Z --window-end 2026-02-05T00:00:00Z --dedup-conflicts-out qc/simulamet_dedup_conflicts.csv --freeze-manifest-out manifests/simulamet_firstweek_freeze_manifest.json` and produced the declared outputs. Freeze manifest row counts: `agents=9794`, `comments=3084`, `posts=23751`, `snapshots=119`, `submolts=638`, `word_frequency=15944`.
- 2026-04-09: Verified the windowed outputs after materialization. `posts` span `2026-01-28T19:41:46.698141+00:00` to `2026-02-04T16:59:21.149510+00:00`; `comments` span `2026-01-31T07:46:02.546988+00:00` to `2026-02-04T23:23:58.384000+00:00`; `snapshots` span `2026-01-30T21:37:46.238243+00:00` to `2026-02-04T23:53:58.343486+00:00`; `word_frequency` spans `2026-01-30T20:00:00+00:00` to `2026-02-04T23:00:00+00:00`.
- 2026-04-09: `qc/simulamet_dedup_conflicts.csv` contains `1260` `same_snapshot_conflict` rows, all in the `snapshots` subset. No same-snapshot conflicts were emitted for `comments`, `posts`, `agents`, `submolts`, or `word_frequency`.
- 2026-04-09: Ran gates successfully with `make gate` and `make test`. `make gate` passed all framework/task ownership checks; `make test` passed `25` unit tests in `8.305s`.
- 2026-04-09: This execution was run directly in the worktree rather than through the local swarm runtime, so no new durable run manifest was recorded under `reports/status/swarm_runs/`. Operator should capture a runtime-owned manifest before moving this task to `ready_for_review`.
- 2026-04-09: Runtime passed: preflight, fresh outputs, gates, manifests, and run manifest are present. Ready for Judge review. Run manifest: reports/status/swarm_runs/T030_20260409T193758Z.json
- 2026-04-09: Judge approved; review log: reports/status/reviews/T030_20260409T194311Z.json
