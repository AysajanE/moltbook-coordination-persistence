---
task_id: "T035"
title: "Produce archive QC packet and benchmark validation"
workstream: "W2"
task_kind: "freeze_qc"
allow_network: false
role: "Worker"
priority: "high"
dependencies:
  - "T030"
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "analysis/hf_archive_validate.py"
  - "qc/linkage_audit_simulamet.csv"
  - "qc/gap_registry_simulamet.csv"
  - "qc/gap_disambiguation_simulamet.csv"
  - "qc/benchmark_report_simulamet.md"
  - "qc/archive_qc_report_simulamet.md"
  - "qc/exclusion_log_simulamet.csv"
  - "qc/manual_override_log_simulamet.csv"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
  - "derived/"
outputs:
  - "qc/linkage_audit_simulamet.csv"
  - "qc/gap_registry_simulamet.csv"
  - "qc/gap_disambiguation_simulamet.csv"
  - "qc/benchmark_report_simulamet.md"
  - "qc/archive_qc_report_simulamet.md"
  - "qc/exclusion_log_simulamet.csv"
  - "qc/manual_override_log_simulamet.csv"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need to edit outside allowed paths"
---

# Task T035 — Produce archive QC packet and benchmark validation

## Context

Analysis does not begin until the canonical freeze has passed the hard QC gates in the acquisition plan.

## Assignment

- Workstream: W2
- Assigned role: Worker
- Suggested branch or worktree name: `T035_archive_qc`
- Allowed paths: `analysis/hf_archive_validate.py`, `qc/`
- Disallowed paths: authority docs, contracts, paper, and derived outputs
- Stop conditions: Stage 3 ambiguity or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: schema contracts and runtime contracts
- Upstream tasks or manifests: `T030`, fresh freeze outputs
- External references or systems: none

## Outputs

- Code: updated validation script
- Data or manifests: linkage audit, gap registry, gap disambiguation, benchmark report, QC packet, exclusion log, manual override log
- Reports or docs: notes on unresolved anomalies

## Success Criteria

- [ ] The archive QC packet is complete and reproducible
- [ ] Gap handling and benchmark reconstruction are explicitly documented
- [ ] Gates pass and commands are recorded

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if any anomalies constrain downstream estimation

## Validation / Commands

- `python analysis/hf_archive_validate.py --freeze-root frozen/simulamet_firstweek_lateststate --archive-name simulamet --out-linkage-audit qc/linkage_audit_simulamet.csv --out-gap-registry qc/gap_registry_simulamet.csv --out-gap-disambiguation qc/gap_disambiguation_simulamet.csv --out-benchmark-report qc/benchmark_report_simulamet.md --out-qc-report qc/archive_qc_report_simulamet.md --out-exclusion-log qc/exclusion_log_simulamet.csv --out-manual-override-log qc/manual_override_log_simulamet.csv`
- `make gate`
- `make test`

## Status

- State: blocked
- Last updated: 2026-04-09

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
- 2026-04-09: Operator blocked this task on canonical `main` after the isolated worker branch `T035_archive_qc_packet_and_benchmarks` reproduced a task/benchmark contradiction. The task-specific validation command failed with `archive_qc_failed`, but the local swarm runtime still promoted the task to `ready_for_review` because it only enforced preflight, declared outputs, gates, manifests, and path ownership. Treat task-branch run manifest `reports/status/swarm_runs/T035_20260409T194610Z.json` as stale/incomplete for review. Planner must resolve the basis mismatch between the first-week freeze metrics and the locked Stage 3 benchmark anchors before rerun.
