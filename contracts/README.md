# contracts/

Machine-readable project, runtime, and artifact contracts for the flagship research repo.

## Authority boundary

- `docs/stage3_theory_framework_packet.cleaned.md` is the top scientific authority.
- `docs/data_acquisition_plan.md` is the operational contract derived from the Stage 3 packet.
- Files in `contracts/` translate that authority into repo-runtime rules and artifact schemas.
- No contract in this directory may contradict the Stage 3 packet or the acquisition plan.

## Change policy

- Only W0 tasks may modify this directory.
- Contract changes require a rationale, expected downstream impact, and an entry in `contracts/CHANGELOG.md`.
- If interfaces change, update the corresponding schema version and downstream tasks together.
