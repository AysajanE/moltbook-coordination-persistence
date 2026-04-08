# Moltbook Coordination Persistence

This repo studies short-horizon continuation in Moltbook discussion threads. The core question is whether throughput within fixed horizons is constrained more by reply incidence, meaning whether a direct reply happens at all, or by conditional reply speed, meaning how quickly the reply arrives once it does.

## Research object

The load-bearing unit is a non-root comment that can receive a direct child reply. For each candidate parent comment, the project constructs:

- the time to first direct reply when one exists
- the observed follow-up window before censoring
- horizon-specific risk-set indicators
- horizon-specific within-window reply indicators

Those parent-level objects feed the main estimands:

- `q_h`: throughput within horizon `h`
- `pi_h`: reply incidence within the common risk set
- `phi_h`: conditional reply speed within the same risk set
- `p_obs`: descriptive reply incidence, reported separately and not substituted for `q_h`

Primary horizons are 5 minutes and 1 hour. A 30-second horizon is retained for diagnostics and boundary checks.

## Data strategy

This project is archive-first and evidence-regenerating. It does not reuse old result tables, figures, or conclusions. The intended empirical path is:

1. acquire a fresh SimulaMet archive with immutable provenance
2. reconstruct the canonical first-week latest-state freeze
3. produce the hard QC packet, including linkage audits, gap diagnostics, and benchmark checks
4. build the canonical derived tables
5. run the primary analysis and diagnostics on those new tables
6. acquire and harmonize MoltNet as a separate replication archive

The project keeps SimulaMet and MoltNet as distinct archives. MoltNet is a replication surface, not a pooled replacement for the canonical SimulaMet reconstruction.

## Canonical artifacts

The main tracked analytic outputs are:

- `manifests/` for acquisition and schema lineage
- `qc/` for archive audits, gap registries, benchmark reports, and exclusion logs
- `derived/parent_units_*.parquet`
- `derived/control_panel_summary_*.parquet`
- `derived/thread_geometry_*.parquet`
- `derived/periodicity_input_*.parquet`
- `derived/archive_metadata_audit.parquet`

Raw and restricted surfaces such as `raw/`, `frozen/`, and `restricted/` remain outside git.

## Authority model

When implementation surfaces disagree, use this order:

1. `docs/stage3_theory_framework_packet.cleaned.md`
2. `docs/data_acquisition_plan.md`
3. locked sections under `paper/sections/`
4. `contracts/`
5. `.orchestrator/workstreams.md`
6. task files
7. handoff notes

Carried-over scripts under `analysis/` are helpers only. They are not scientific authority and must be brought into alignment with the Stage 3 packet before being trusted for canonical outputs.

## Repo layout

- `docs/` holds the theory packet, acquisition plan, migration history, decision log, and swarm deployment plan.
- `scripts/` holds acquisition entrypoints plus the swarm runtime and gate tooling.
- `analysis/` holds collection, curation, validation, and analysis code.
- `contracts/` holds the project and artifact contracts.
- `.orchestrator/` holds the task control plane for the manual swarm workflow.
- `reports/status/` holds durable run manifests and Judge review logs.
- `paper/` holds the manuscript surface, but the research project is not defined by the paper wrapper.

## Execution model

The current swarm deployment is semi-automated. Planner, Worker, Judge, and Operator are coordinated through `.orchestrator/`, while Operator remains responsible for launching tasks, launching Judge review, reviewing the result bundle, merging, sweeping, and starting the next task.

## Current status

- Stage 3 theory and methods are locked.
- The swarm framework bootstrap is in place and verified.
- `T020` is the first ready task in the seeded backlog.
- Fresh archive acquisition, freeze construction, and derived-table building are the next substantive research steps.
