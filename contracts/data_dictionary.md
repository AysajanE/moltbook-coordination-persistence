# Flagship Data Dictionary

This file summarizes the canonical artifact zones and the load-bearing flagship tables.

## Artifact zones

- `raw/`: immutable local archive acquisition payloads; ignored from git
- `frozen/`: immutable canonical archive freezes; ignored from git until an explicit publication decision
- `manifests/`: tracked provenance manifests and schema crosswalks
- `qc/`: tracked QC packet outputs
- `derived/`: tracked sanitized derived tables
- `restricted/`: restricted mappings such as raw-to-hash linkage; ignored from git

## Canonical derived tables

- `parent_units`: one row per candidate parent comment; load-bearing input for `q_h`, `pi_h`, `phi_h`, `p_obs`, and conditional timing
- `control_panel_summary`: aggregated estimand table by archive, window, horizon, and optional stratum
- `thread_geometry`: one row per thread for depth, branching, reciprocity, and re-entry outcomes
- `periodicity_input`: diagnostic-only event-time table for H5
- `archive_metadata_audit`: archive-level provenance and harmonization audit

For field-level requirements, use the schema files in `contracts/schemas/`.
