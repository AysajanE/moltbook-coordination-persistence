# contracts/

Machine-readable project, runtime, and artifact contracts for the Moltbook coordination persistence repo.

## Authority boundary

- Detailed internal theory and acquisition materials are maintained outside the public repository.
- Files in `contracts/` define the public runtime rules, artifact schemas, and repo-facing project contract.
- No contract in this directory may contradict the public repo materials or knowingly misstate unpublished internal research decisions.

## Change policy

- Only W0 tasks may modify this directory.
- Contract changes require a rationale, expected downstream impact, and an entry in `contracts/CHANGELOG.md`.
- If interfaces change, update the corresponding schema version and downstream tasks together.
