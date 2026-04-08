# Flagship Workstreams

| Workstream | Purpose | Owns paths | Does NOT own | Example outputs | Network | integration_ready eligible |
|---|---|---|---|---|---|---|
| W0 | Authority and machine-readable contracts | `AGENTS.md`, `README.md`, `contracts/`, `docs/decisions.md`, `docs/swarm_deployment_plan.md` | `docs/stage3_theory_framework_packet.cleaned.md`, `raw/`, `frozen/`, `qc/`, `derived/`, `paper/` evidence surfaces | contract revisions, schema contracts, decision entries | no | yes |
| W1 | Archive acquisition and provenance capture | `scripts/download_moltbook_observatory_archive.py`, `scripts/run_moltbook_live_campaign.py`, `analysis/moltbook_api_*.py`, `raw/`, `manifests/`, `restricted/` | `contracts/`, locked paper sections, `qc/`, `derived/` | archive manifests, restricted raw-to-hash mapping, acquisition notes | yes | no |
| W2 | Freeze construction, schema audit, and QC packeting | `analysis/hf_archive_schema_discovery.py`, `analysis/hf_archive_curate.py`, `analysis/hf_archive_validate.py`, `frozen/`, `qc/`, `manifests/schema_crosswalk.yaml` | `contracts/`, `paper/`, final derived estimands | canonical freeze, MoltNet aligned slices, QC packet, benchmark report, gap registry | no | no |
| W3 | Canonical derived tables | derived-table builders under `analysis/`, `derived/` | `raw/`, `restricted/`, locked paper sections, runtime surfaces | `parent_units`, `control_panel_summary`, `thread_geometry`, `periodicity_input`, `archive_metadata_audit`, MoltNet derived tables | no | no |
| W4 | Estimation and diagnostics | Stage 3 analysis scripts under `analysis/`, analysis-facing execution notes under `qc/` | authority docs, `contracts/`, canonical freeze builders, canonical derived-table builders | rewritten estimand scripts, diagnostic summaries, analysis notes | no | no |
| W5 | Paper integration and manuscript glue | `paper/main.tex`, unlocked manuscript glue, bibliography updates | `docs/stage3_theory_framework_packet.cleaned.md`, locked paper sections unless explicitly authorized, runtime surfaces | manuscript wrapper updates, evidence insertions, citation updates | no | no |
| W9 | Swarm runtime and operational tooling | `.orchestrator/`, `docs/prompts/`, `docs/runbook_swarm*.md`, `scripts/swarm.py`, `scripts/quality_gates.py`, `scripts/sweep_tasks.py`, `reports/status/`, `tests/`, `Makefile` | scientific definitions, archive evidence outputs, locked paper sections | runtime scripts, gate logic, run or review logs, seeded backlog | no | yes |

## Special rules

- No routine workstream owns `docs/stage3_theory_framework_packet.cleaned.md`. Treat it as immutable unless a human explicitly re-locks the theory packet.
- `integration_ready` is reserved for W0 or W9 interface tasks whose downstream consumers explicitly list them in `integration_ready_dependencies`.
- Tasks that write `raw/`, `frozen/`, `qc/`, `derived/`, or `restricted/` must go through full review; they are never interface checkpoints.
