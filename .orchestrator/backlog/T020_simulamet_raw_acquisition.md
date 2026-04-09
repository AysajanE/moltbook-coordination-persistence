---
task_id: "T020"
title: "Acquire canonical SimulaMet archive and provenance manifests"
workstream: "W1"
task_kind: "acquisition"
allow_network: true
role: "Worker"
priority: "high"
dependencies: []
integration_ready_dependencies: []
requires_tools:
  - "python"
  - "git"
requires_env: []
allowed_paths:
  - "scripts/download_moltbook_observatory_archive.py"
  - "raw/"
  - "manifests/simulamet_manifest.yaml"
  - "restricted/raw_to_hash_mapping.parquet"
disallowed_paths:
  - "README.md"
  - "docs/swarm_deployment_plan.md"
  - "contracts/"
  - "paper/"
  - "qc/"
  - "derived/"
outputs:
  - "raw/simulamet/YYYY-MM-DD/..."
  - "manifests/simulamet_manifest.yaml"
  - "restricted/raw_to_hash_mapping.parquet"
gates:
  - "make gate"
  - "make test"
stop_conditions:
  - "Stage 3 authority ambiguity"
  - "Need credentials"
  - "Need to edit outside allowed paths"
---

# Task T020 — Acquire canonical SimulaMet archive and provenance manifests

## Context

The flagship pipeline begins with fresh archive acquisition inside this repo. The Stage 3 packet forbids reuse of old evidence.

## Assignment

- Workstream: W1
- Assigned role: Worker
- Suggested branch or worktree name: `T020_simulamet_acquisition`
- Allowed paths: `scripts/download_moltbook_observatory_archive.py`, `raw/`, `manifests/`, `restricted/`
- Disallowed paths: authority docs, contracts, paper surfaces, QC, and derived outputs
- Stop conditions: authority ambiguity, credentials, or path-boundary breaches

## Inputs

- Authority surfaces: `README.md`, `contracts/project.yaml`, `contracts/framework.json`
- Contracts: `contracts/project.yaml`, `contracts/framework.json`
- Upstream tasks or manifests: none
- External references or systems: canonical SimulaMet archive source

## Outputs

- Code: updated acquisition script if needed
- Data or manifests: fresh local archive under `raw/`, manifest under `manifests/`, restricted mapping under `restricted/`
- Reports or docs: acquisition notes in task log or handoff if needed

## Success Criteria

- [ ] The archive is freshly acquired inside this repo
- [ ] Immutable provenance is recorded in `manifests/simulamet_manifest.yaml`
- [ ] Restricted raw-to-hash linkage is recorded separately
- [ ] Gates pass and reproduction commands are logged

## Review Bundle Requirements

- [ ] Durable run manifest under `reports/status/swarm_runs/`
- [ ] Judge review under `reports/status/reviews/`
- [ ] Handoff note if downstream acquisition quirks matter

## Validation / Commands

- `python scripts/download_moltbook_observatory_archive.py --dataset SimulaMet/moltbook-observatory-archive --archive-name simulamet --out-root raw/simulamet --snapshot-id $(date -u +%F) --manifest-out manifests/simulamet_manifest.yaml --restricted-hash-out restricted/raw_to_hash_mapping.parquet`
- `make gate`
- `make test`

## Status
- State: ready_for_review
- Last updated: 2026-04-09
## Notes / Decisions

- 2026-04-08: Seeded from the flagship swarm deployment plan.
- 2026-04-09: Claimed by local swarm runtime on branch T020_simulamet_raw_acquisition.
- 2026-04-09: Initial canonical export via `datasets.load_dataset()` failed on `data/comments/2026-01-31.parquet` with `ArrowInvalid: Failed to parse string '2026-01-31T08:39:52.516762+00:00' as a scalar of type int64`. Updated `scripts/download_moltbook_observatory_archive.py` to acquire the immutable Hugging Face dataset repo snapshot directly with `huggingface_hub.snapshot_download`, hash the downloaded raw files, preserve repo-root metadata files, and remove the local `.cache/huggingface/` helper directory from `raw/`.
- 2026-04-09: Reproduction environment for this run used `python -m pip install --target /tmp/moltbook_pydeps datasets numpy pandas huggingface_hub`, `PYTHONPATH=/tmp/moltbook_pydeps`, `HF_HOME=/tmp/huggingface`, and `HF_DATASETS_CACHE=/tmp/huggingface/datasets`. `python -m pip install -e .` failed outside this task's allowed edit surface with `Multiple top-level packages discovered in a flat-layout`, so dependency installation was redirected to `/tmp`.
- 2026-04-09: Canonical acquisition completed successfully with `env PYTHONPATH=/tmp/moltbook_pydeps HF_HOME=/tmp/huggingface HF_DATASETS_CACHE=/tmp/huggingface/datasets python scripts/download_moltbook_observatory_archive.py --dataset SimulaMet/moltbook-observatory-archive --archive-name simulamet --revision main --out-root raw/simulamet --snapshot-id 2026-04-09 --manifest-out manifests/simulamet_manifest.yaml --restricted-hash-out restricted/raw_to_hash_mapping.parquet`. The script resolved `main` to `4ea70791acc3e17bbcbdb168110d71cc2839f85a`, recorded license `MIT`, wrote `raw/simulamet/2026-04-09/`, wrote `manifests/simulamet_manifest.yaml`, and wrote `restricted/raw_to_hash_mapping.parquet`.
- 2026-04-09: Output summary from the successful run: `source_file_count=376`, `source_bytes_total=3494241805`, `subset_count=7`, and restricted hash rows `376`. The seventh subset is `lost_and_found`, which is present in the upstream repo snapshot in addition to `agents`, `comments`, `posts`, `snapshots`, `submolts`, and `word_frequency`. The local raw snapshot also includes `EXPORT_MANIFEST.json`, so `find raw/simulamet/2026-04-09 -maxdepth 3 -type f | wc -l` returns `377`.
- 2026-04-09: Declared gates passed with `env PYTHONPATH=/tmp/moltbook_pydeps make gate` and `env PYTHONPATH=/tmp/moltbook_pydeps make test`.
- 2026-04-09: This run was executed outside the local swarm runtime, so no durable `reports/status/swarm_runs/` manifest was created here. Operator should record the run manifest from the exact commands above before advancing the task to `ready_for_review`.
- 2026-04-09: Runtime passed: preflight, fresh outputs, gates, manifests, and run manifest are present. Ready for Judge review. Run manifest: reports/status/swarm_runs/T020_20260409T175446Z.json
