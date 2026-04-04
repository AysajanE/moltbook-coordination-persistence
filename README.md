# Coordination Persistence Flagship

This is the standalone repo for the new flagship paper on coordination persistence in Moltbook.

Only four things were carried forward from the parent project:

- the locked Stage 3 theory and methods text
- the Stage 3 authority packet
- fresh-data collection, curation, and validation scaffolding
- two small Stage 3 estimand scripts that are still methodologically aligned

The following were intentionally not carried forward:

- all old raw and curated data
- all old result tables, figures, summaries, and output directories
- the old discussion, conclusion, and manuscript wrappers
- the large legacy end-to-end analysis script and legacy presentation-layer scripts

The repo boundary is therefore simple:

- `docs/` holds the authority packet, the fresh data acquisition plan, the migration manifest, and the new decision log.
- `scripts/` holds the two acquisition entrypoints.
- `analysis/` holds only audited collection/curation/validation scripts plus two compact Stage 3 estimand scripts.
- `paper/` holds the locked theory/method sections and a minimal working wrapper.

Current status:

- Stage 3 theory and methods are locked.
- Old evidence is not active in this repo.
- Fresh data acquisition and fresh feature construction are the next research tasks.
- A new feature-builder for Stage 3 analysis has not yet been implemented here.

Recommended starting sequence:

1. Download a fresh HF Moltbook archive with `scripts/download_moltbook_observatory_archive.py`.
2. Run schema discovery, curation, and validation with the `analysis/hf_archive_*.py` scripts.
3. Run live public-API collection with `scripts/run_moltbook_live_campaign.py` when needed.
4. Build a new Stage 3 feature layer from the fresh archive surface.
5. Run the carried-forward Stage 3 estimand scripts on the new feature layer.
