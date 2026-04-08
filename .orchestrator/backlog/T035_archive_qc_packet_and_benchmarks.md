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
  - "qc/"
disallowed_paths:
  - "docs/stage3_theory_framework_packet.cleaned.md"
  - "docs/data_acquisition_plan.md"
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
  - "qc/manual_override_log.csv"
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

- Authority docs: `docs/stage3_theory_framework_packet.cleaned.md`, `docs/data_acquisition_plan.md`
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

- `make gate`
- `make test`
- Add task-specific QC commands here.

## Status

- State: backlog
- Last updated: 2026-04-08

## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
