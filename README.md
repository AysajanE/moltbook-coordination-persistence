# Moltbook Coordination Persistence

Moltbook Coordination Persistence is a standalone research repository for studying short-horizon continuation in Moltbook discussion threads. The central question is whether thread continuation within fixed horizons is constrained more by reply incidence, meaning whether a direct reply happens at all, or by reply speed, meaning how quickly that reply arrives once it does.

This repo is designed to regenerate the empirical evidence from fresh archive inputs rather than inherit prior tables, figures, or conclusions. It combines data acquisition, archive validation, derived-table construction, analysis code, and manuscript integration in one reproducible workspace.

## Research Focus

The primary unit of analysis is a non-root comment that can receive a direct child reply. For each candidate parent comment, the project tracks:

- time to first direct reply when one exists
- observed follow-up before censoring
- horizon-specific risk-set membership
- horizon-specific within-window reply indicators

These objects support the main horizon-throughput estimands:

- `q_h`: throughput within horizon `h`
- `pi_h`: reply incidence within the common risk set
- `phi_h`: conditional reply speed within the same risk set
- `p_obs`: descriptive ever-reply incidence, reported separately

The main reporting horizons are 5 minutes and 1 hour. A 30-second horizon is retained as a diagnostic check.

## Evidence Strategy

The project is archive-first and observational. It uses:

- `SimulaMet` as the canonical current archive for parent-linked evidence
- `MoltNet` as a separate replication archive

The intended empirical workflow is:

1. acquire fresh archive snapshots with provenance manifests
2. construct canonical analysis-ready freezes
3. validate coverage, linkage, gaps, and benchmark reconstruction quality
4. build canonical derived tables for continuation and thread geometry
5. run the main analyses and diagnostics
6. integrate verified evidence into the paper surface

SimulaMet and MoltNet are kept distinct throughout this process. The replication archive is not treated as a pooled substitute for the canonical reconstruction.

## Repository Structure

- `docs/`: public runbooks, prompts, and execution notes
- `scripts/`: archive acquisition entrypoints, quality gates, task sweeping, and swarm runtime helpers
- `analysis/`: schema discovery, curation, validation, derived-table builders, and analysis scripts
- `contracts/`: machine-readable project rules and artifact schemas
- `manifests/`: acquisition and schema lineage records
- `qc/`: archive audits, benchmark reports, exclusions, and other quality-control outputs
- `derived/`: canonical analysis tables
- `paper/`: LaTeX manuscript source
- `reports/status/`: durable run manifests and review logs

Raw, frozen, restricted, and other large runtime data surfaces are intentionally kept out of git.

## Reproducibility And Governance

The public repo uses this source order for reproducibility and operational decisions:

1. `README.md`
2. `contracts/project.yaml`, `contracts/framework.json`, and related files under `contracts/`
3. locked manuscript sections under `paper/sections/`
4. task and orchestration metadata under `.orchestrator/`
5. public runbooks under `docs/`

Detailed private research materials are intentionally not published in this repository.

The repository also includes a lightweight local coordination layer under `.orchestrator/` for planning, execution, review, and status tracking. That control plane supports the work, but it is not the project description itself.

## Getting Started

Requirements:

- Python 3.11+
- `git`
- `make`

Install the Python dependencies with your preferred environment manager, then run:

```bash
python -m pip install -e .
make gate
make test
```

Helpful entrypoints:

- `make swarm-plan` to inspect runnable task state
- `make swarm-tick` to launch ready work through the local swarm helper
- `make sweep` to reconcile task-file placement with authoritative task state

Public contributions should also follow [CONTRIBUTING.md](/Users/aeziz-local/Research/moltbook-coordination-persistence/CONTRIBUTING.md).

## Current Project State

This repository already contains the public contracts, runbooks, and reproducibility scaffolding needed to build the study from fresh archive evidence. The next substantive work in the environment is empirical: archive acquisition, canonical freeze construction, validation, derived-table building, analysis, and paper integration.
