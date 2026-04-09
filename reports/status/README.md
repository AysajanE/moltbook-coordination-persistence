# reports/status/

Machine-readable runtime status surfaces for the flagship swarm deployment.

- `swarm_runs/` holds durable runtime run manifests.
- `reviews/` holds durable Judge review logs.
- `moltbook_live_campaign/` holds operational manifests and executor logs for live API campaign runs launched by `scripts/run_moltbook_live_campaign.py`.
- `moltbook_live_campaign/<campaign_id>/campaign_manifest.json` is the campaign-level registry for configuration, git provenance, and daily run outcomes.
- `moltbook_live_campaign/<campaign_id>/<attempt_id>/run_manifest.json` is the per-day run manifest; sibling `*.stdout.log` and `*.stderr.log` files are operational logs, not release artifacts.

This namespace is intentionally narrower than the pilot repo. It exists for runtime status and operations evidence only; release manifests and catalog assembly are still deferred.
