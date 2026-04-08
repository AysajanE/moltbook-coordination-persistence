# analysis/AGENTS.md — Stage 3 alignment rules

`analysis/` contains executable research code, but not scientific authority.

## Rules

- `docs/stage3_theory_framework_packet.cleaned.md` and `docs/data_acquisition_plan.md` outrank every script in this directory.
- The carried-over estimand scripts are provisional. They may be reused only after they are rewritten against the canonical flagship derived tables.
- Do not import legacy result files, cached tables, or old presentation outputs.
- Prefer additive builders, validators, and diagnostics over monolithic end-to-end scripts.
- If a script requires a new scientific assumption, record it in `contracts/assumptions.md` and block with `@human` when necessary.
- Do not edit locked paper sections from this directory.
