---
task_id: "T070"
title: "Build MoltNet freezes and QC packet"
workstream: "W2"
task_kind: "freeze_qc"
allow_network: false
role: "Worker"
priority: "medium"
dependencies:
  - "T065"
  - "T050"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "analysis/hf_archive_schema_discovery.py"
  - "analysis/hf_archive_curate.py"
  - "analysis/hf_archive_validate.py"
  - "frozen/moltnet_firstweek_aligned/"
  - "frozen/moltnet_fullrelease/"
  - "manifests/moltnet_schema_crosswalk.yaml"
  - "manifests/moltnet_firstweek_freeze_manifest.json"
  - "manifests/moltnet_fullrelease_freeze_manifest.json"
  - "qc/moltnet_dedup_conflicts.csv"
  - "qc/linkage_audit_moltnet.csv"
  - "qc/gap_registry_moltnet.csv"
  - "qc/gap_disambiguation_moltnet.csv"
  - "qc/benchmark_report_moltnet.md"
  - "qc/archive_qc_report_moltnet.md"
  - "qc/exclusion_log_moltnet.csv"
  - "qc/manual_override_log_moltnet.csv"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
outputs:
  - "frozen/moltnet_firstweek_aligned/..."
  - "frozen/moltnet_fullrelease/..."
  - "manifests/moltnet_schema_crosswalk.yaml"
  - "qc/archive_qc_report_moltnet.md"
  - "qc/gap_registry_moltnet.csv"
  - "qc/manual_override_log_moltnet.csv"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T070 — Build MoltNet freezes and QC packet

## Context

Replication requires aligned MoltNet freeze construction and QC before any MoltNet derived tables are built.

## Assignment

- Workstream: W2
- Assigned role: Worker
- Suggested branch or worktree name: `T070_moltnet_freeze_qc`
- Allowed paths: `analysis/`, `frozen/`, `qc/`, `manifests/`
- Disallowed paths: authority docs, contracts, and paper surfaces
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: acquisition-plan QC requirements and runtime contracts
- Upstream tasks or manifests: `T065`, `T050`
- External references or systems: none

## Outputs

- Code: harmonization and validation logic under `analysis/`
- Data or manifests: aligned and full-release MoltNet freezes plus the MoltNet QC packet
- Reports or docs: notes on archive comparability and any blockers for downstream derived-table work

## Success Criteria

- [ ] MoltNet freeze and QC follow the same flagship provenance discipline
- [ ] Downstream derived-table work is unblocked or clearly constrained
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if downstream MoltNet derived-table work depends on archive caveats

## Validation / Commands

- `python analysis/hf_archive_schema_discovery.py --raw-manifest manifests/moltnet_manifest.yaml --archive-name moltnet --out-crosswalk manifests/moltnet_schema_crosswalk.yaml --out-field-validation qc/field_validation_moltnet.csv --out-missingness qc/missingness_moltnet.csv`
- `python analysis/hf_archive_curate.py --raw-manifest manifests/moltnet_manifest.yaml --schema-crosswalk manifests/moltnet_schema_crosswalk.yaml --archive-name moltnet --out-root frozen/moltnet_firstweek_aligned --window-start 2026-01-28T00:00:00Z --window-end 2026-02-05T00:00:00Z --dedup-conflicts-out qc/moltnet_dedup_conflicts.csv --freeze-manifest-out manifests/moltnet_firstweek_freeze_manifest.json`
- `python analysis/hf_archive_curate.py --raw-manifest manifests/moltnet_manifest.yaml --schema-crosswalk manifests/moltnet_schema_crosswalk.yaml --archive-name moltnet --out-root frozen/moltnet_fullrelease --window-start 2026-01-01T00:00:00Z --window-end 2026-12-31T23:59:59Z --dedup-conflicts-out qc/moltnet_dedup_conflicts.csv --freeze-manifest-out manifests/moltnet_fullrelease_freeze_manifest.json`
- `python analysis/hf_archive_validate.py --freeze-root frozen/moltnet_firstweek_aligned --archive-name moltnet --out-linkage-audit qc/linkage_audit_moltnet.csv --out-gap-registry qc/gap_registry_moltnet.csv --out-gap-disambiguation qc/gap_disambiguation_moltnet.csv --out-benchmark-report qc/benchmark_report_moltnet.md --out-qc-report qc/archive_qc_report_moltnet.md --out-exclusion-log qc/exclusion_log_moltnet.csv --out-manual-override-log qc/manual_override_log_moltnet.csv`
- `make gate`
- `make test`

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
