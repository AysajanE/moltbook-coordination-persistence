# Handoff H_T030 — SimulaMet first-week latest-state freeze

## Summary (1-3 sentences)

T030 built the canonical `frozen/simulamet_firstweek_lateststate/` freeze from the fresh SimulaMet manifest and schema crosswalk, emitted the freeze manifest, and logged same-snapshot dedup conflicts. The curation script now supports manifest-v3 `subsets.<subset>.files` and tolerates mixed parquet physical schemas by concatenating per-file pandas frames before canonical type coercion.

## What changed / what exists now

- Files/paths:
  - `analysis/hf_archive_curate.py`
  - `frozen/simulamet_firstweek_lateststate/agents.parquet`
  - `frozen/simulamet_firstweek_lateststate/comments.parquet`
  - `frozen/simulamet_firstweek_lateststate/posts.parquet`
  - `frozen/simulamet_firstweek_lateststate/snapshots.parquet`
  - `frozen/simulamet_firstweek_lateststate/submolts.parquet`
  - `frozen/simulamet_firstweek_lateststate/word_frequency.parquet`
  - `qc/simulamet_dedup_conflicts.csv`
  - `manifests/simulamet_firstweek_freeze_manifest.json`
- Freeze row counts:
  - `agents=9794`
  - `comments=3084`
  - `posts=23751`
  - `snapshots=119`
  - `submolts=638`
  - `word_frequency=15944`
- Dedup conflict summary:
  - `1260` rows in `qc/simulamet_dedup_conflicts.csv`
  - all conflicts are `same_snapshot_conflict` in `snapshots`
  - no same-snapshot conflicts were logged for the other frozen subsets

## How to reproduce / verify

- Commands:
  - `python analysis/hf_archive_curate.py --raw-manifest manifests/simulamet_manifest.yaml --schema-crosswalk manifests/schema_crosswalk.yaml --archive-name simulamet --out-root frozen/simulamet_firstweek_lateststate --window-start 2026-01-28T00:00:00Z --window-end 2026-02-05T00:00:00Z --dedup-conflicts-out qc/simulamet_dedup_conflicts.csv --freeze-manifest-out manifests/simulamet_firstweek_freeze_manifest.json`
  - `make gate`
  - `make test`
- Expected results:
  - the six parquet outputs exist under `frozen/simulamet_firstweek_lateststate/`
  - `manifests/simulamet_firstweek_freeze_manifest.json` reports the row counts above
  - `qc/simulamet_dedup_conflicts.csv` contains snapshot-only conflicts
  - `make gate` and `make test` complete successfully

## Assumptions / risks

- The raw manifest still points to absolute raw-data paths under the T020 worktree; the freeze build succeeded against those existing paths without mutating the manifest.
- Observed early-window coverage is uneven across subsets: `posts` begin on `2026-01-28`, while `comments` do not appear until `2026-01-31`, and `snapshots` / `word_frequency` begin on `2026-01-30`. This is an observed archive property that downstream QC should keep explicit.
- This execution was run directly in the worktree, not through the local swarm runtime, so Operator still needs to record a durable runtime-owned manifest before review.

## Open questions / next steps

- T035 should treat the comment start-date gap and the snapshot-only dedup conflicts as explicit QC inputs rather than rediscovering them ad hoc.
- T040 and T050 can consume `frozen/simulamet_firstweek_lateststate/` once T035 has documented whether the early-window coverage asymmetry changes any exclusion or interpretation rules.
