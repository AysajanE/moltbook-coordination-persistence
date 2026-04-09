# Contributing

Thanks for contributing to Moltbook Coordination Persistence.

This repository is a public research workspace with strict reproducibility and provenance requirements. Contributions are welcome, but they need to preserve the scientific object, the archive discipline, and the narrow scope of each change.

## Before You Start

Read these sources in order before proposing substantive changes:

1. [README.md](/Users/aeziz-local/Research/moltbook-coordination-persistence/README.md)
2. [contracts/project.yaml](/Users/aeziz-local/Research/moltbook-coordination-persistence/contracts/project.yaml)
3. [contracts/framework.json](/Users/aeziz-local/Research/moltbook-coordination-persistence/contracts/framework.json)
4. [docs/runbook_swarm.md](/Users/aeziz-local/Research/moltbook-coordination-persistence/docs/runbook_swarm.md)
5. [AGENTS.md](/Users/aeziz-local/Research/moltbook-coordination-persistence/AGENTS.md)

Detailed internal research materials are intentionally not published in the public repo. If the available public surfaces appear to disagree, stop and open an issue or draft pull request rather than improvising a new project definition.

## Good Contribution Types

- documentation clarifications that improve the public explanation of the project
- reproducibility improvements in scripts, tests, gates, or runbooks
- narrowly scoped bug fixes in acquisition, validation, or analysis code
- contract or schema updates that are justified and propagated consistently
- paper-surface improvements that reflect verified evidence already generated in this repo

## Changes That Need Extra Care

- anything that affects scientific assumptions, estimands, or the interpretation of results
- any edit under `contracts/`
- any change that touches manuscript claims
- any workflow that writes `raw/`, `frozen/`, `restricted/`, `derived/`, `qc/`, or `outputs/`
- anything that could weaken provenance, benchmark checks, or review discipline

## Hard Project Rules

- Do not import old tables, figures, conclusions, or archive outputs from another repo.
- Do not commit raw or restricted archive material.
- Do not fabricate provenance, QC results, or manuscript claims.
- Keep SimulaMet and MoltNet as separate archive surfaces.
- Keep changes narrow and explain the reason for each nontrivial change in the pull request.

## Development Setup

Use Python 3.11 or newer, then install the local package and run the basic checks:

```bash
python -m pip install -e .
make gate
make test
```

## Pull Request Expectations

- Open a topic branch for each change.
- Keep the diff focused on one problem.
- Describe what changed, why it changed, and how you validated it.
- Link the relevant authority docs or contracts when the change is scientifically meaningful.
- Include provenance notes when a change affects acquisition, QC, or derived outputs.

Pull requests should be ready for line-by-line review. If a change is intentionally provisional, say that explicitly.

## Data And Provenance

For any change that touches acquisition, curation, QC, or derived tables, document:

- the affected archive surface
- the intended output artifacts
- the validation commands you ran
- any unresolved assumptions, anomalies, or gaps

## Review Standard

This repository is curated conservatively. A contribution may be declined if it broadens scope, weakens reproducibility, or introduces claims not supported by fresh evidence generated in this repo.
