# Handoff H_T025 — SimulaMet manifest v3 crosswalk notes

## Summary (1–3 sentences)

T025 generated the SimulaMet schema crosswalk and the required-field and missingness audits from the fresh `2026-04-09` raw manifest. The discovery script now supports manifest v3 subset path layout (`files`) and records per-file observed raw types so schema drift is explicit instead of hidden by dataset-unification failures.

## What changed / what exists now

- Files/paths:
  - `analysis/hf_archive_schema_discovery.py`
  - `manifests/schema_crosswalk.yaml`
  - `qc/field_validation_simulamet.csv`
  - `qc/missingness_simulamet.csv`
- Outputs produced:
  - Crosswalk with per-field `mapped_to`, `matches`, `files_with_match`, and `observed_types`
  - Field validation CSV confirming all required fields are present
  - Missingness CSV quantifying nulls across the full raw export

## How to reproduce / verify

- Commands:
  - `python analysis/hf_archive_schema_discovery.py --raw-manifest manifests/simulamet_manifest.yaml --archive-name simulamet --out-crosswalk manifests/schema_crosswalk.yaml --out-field-validation qc/field_validation_simulamet.csv --out-missingness qc/missingness_simulamet.csv`
  - `make gate`
  - `make test`
- Expected results:
  - `manifests/schema_crosswalk.yaml`, `qc/field_validation_simulamet.csv`, and `qc/missingness_simulamet.csv` exist
  - `qc/field_validation_simulamet.csv` shows no `missing_required` rows
  - `make gate` and `make test` complete successfully

## Assumptions / risks

- `manifests/simulamet_manifest.yaml` uses `subsets.<subset>.files`, not `subsets.<subset>.splits`.
- The raw export has physical type drift across files. Notable examples:
  - `comments.created_at`: `int64`, `string`, `large_string`
  - `comments.parent_id`: `null`, `string`, `large_string`
- `comments.parent_comment_id` has high null share because many comments are top-level; that is descriptive audit output, not a new contract assumption.

## Open questions / next steps

- T030 should update `analysis/hf_archive_curate.py` path resolution before freeze construction; it currently resolves only `splits` and will not consume `manifests/simulamet_manifest.yaml` correctly.
- T030 should coerce `comments.created_at` defensively because the raw files do not have a single physical type for that field.
