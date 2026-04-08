from __future__ import annotations

import contextlib
import importlib.util
import json
import os
from pathlib import Path
import sys
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SWARM_PATH = REPO_ROOT / "scripts" / "swarm.py"
QUALITY_GATES_PATH = REPO_ROOT / "scripts" / "quality_gates.py"
SWEEP_TASKS_PATH = REPO_ROOT / "scripts" / "sweep_tasks.py"

SWARM_RUN_MANIFEST_SCHEMA_VERSION = "research_swarm.runtime_run_manifest.v1"
JUDGE_REVIEW_LOG_SCHEMA_VERSION = "research_swarm.judge_review_log.v1"


def _load_module(module_name: str, path: Path):
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = mod
    spec.loader.exec_module(mod)
    return mod


def load_swarm_module():
    return _load_module("flagship_swarm_test_module", SWARM_PATH)


def load_quality_gates_module():
    return _load_module("flagship_quality_gates_test_module", QUALITY_GATES_PATH)


def load_sweep_module():
    return _load_module("flagship_sweep_test_module", SWEEP_TASKS_PATH)


@contextlib.contextmanager
def chdir(path: Path):
    previous = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(previous)


def write_text(root: Path, rel: str, text: str = "") -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def write_json(root: Path, rel: str, data: object) -> Path:
    path = root / rel
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def mkdir(root: Path, rel: str) -> Path:
    path = root / rel
    path.mkdir(parents=True, exist_ok=True)
    return path


def write_project_yaml(root: Path) -> Path:
    return write_text(
        root,
        "contracts/project.yaml",
        "\n".join(
            [
                "project_id: moltbook-coordination-persistence",
                "project_name: moltbook-coordination-persistence",
                "display_name: \"Moltbook Coordination Persistence\"",
                "mode: empirical",
                "status: test",
                "",
                "paper_substrate:",
                "  type: latex",
                "",
                "scientific_contract:",
                "  top_authority: \"docs/stage3_theory_framework_packet.cleaned.md\"",
                "  operational_plan: \"docs/data_acquisition_plan.md\"",
                "",
            ]
        ),
    )


def write_framework_json(root: Path) -> Path:
    payload = {
        "framework_version": "v1",
        "features": {
            "acquisition": True,
            "freeze_qc": True,
            "derived_tables": True,
            "analysis": True,
            "paper": True,
            "release": False,
        },
        "supported_modes": ["empirical"],
        "roles": {
            "allowed": ["Planner", "Worker", "Judge", "Operator"],
            "default_if_unspecified": "Worker",
            "task_execution_roles": ["Worker", "Operator"],
            "scientific_review_role": "Judge",
            "folder_projection_roles": ["Planner", "Operator"],
        },
        "states": {
            "allowed": [
                "backlog",
                "active",
                "integration_ready",
                "ready_for_review",
                "blocked",
                "done",
            ],
            "authoritative_field": "State",
            "projection_dirs": [
                ".orchestrator/backlog",
                ".orchestrator/active",
                ".orchestrator/integration_ready",
                ".orchestrator/ready_for_review",
                ".orchestrator/blocked",
                ".orchestrator/done",
            ],
        },
        "prompt_templates": {
            "planner": "docs/prompts/planner.md",
            "worker": "docs/prompts/worker.md",
            "judge": "docs/prompts/judge.md",
            "operator": "docs/prompts/operator.md",
        },
        "network_workstreams": ["W1"],
        "integration_ready_policy": {
            "eligible_workstreams": ["W0", "W9"],
            "eligible_task_kinds": ["contracts", "ops", "interface"],
            "forbid_unvalidated_empirical_data_outputs": True,
        },
        "review_bundle": {
            "run_manifest_dir": "reports/status/swarm_runs",
            "judge_review_dir": "reports/status/reviews",
        },
        "operator_owned_shared_surfaces": [
            "reports/status/",
            "docs/runbook_swarm.md",
            "docs/runbook_swarm_automation.md",
        ],
        "required_paths": {
            "common": [
                ".orchestrator/",
                "contracts/project.yaml",
                "contracts/framework.json",
                "reports/status/",
            ],
            "empirical": [
                "docs/stage3_theory_framework_packet.cleaned.md",
                "docs/data_acquisition_plan.md",
                "analysis/",
                "paper/main.tex",
                "manifests/",
                "qc/",
                "derived/",
            ],
        },
        "release_policy": {
            "paper_substrate": "latex",
            "paper_entrypoint": "paper/main.tex",
            "assembly_status": "deferred",
        },
    }
    return write_json(root, "contracts/framework.json", payload)


def scaffold_runtime_repo(root: Path) -> None:
    write_text(
        root,
        "AGENTS.md",
        "\n".join(
            [
                "# AGENTS.md",
                "",
                "Use docs/stage3_theory_framework_packet.cleaned.md first.",
                "Use docs/data_acquisition_plan.md second.",
                "analysis/flagship_control_panel_margins.py is provisional.",
                "",
            ]
        ),
    )
    write_text(root, "README.md", "# repo\n")
    write_text(root, "Makefile", ".PHONY: gate test\n")

    for rel in (
        ".orchestrator/backlog",
        ".orchestrator/active",
        ".orchestrator/integration_ready",
        ".orchestrator/ready_for_review",
        ".orchestrator/blocked",
        ".orchestrator/done",
        ".orchestrator/handoff",
        ".orchestrator/templates",
        "contracts/schemas",
        "docs/prompts",
        "analysis",
        "paper",
        "manifests",
        "qc",
        "derived",
        "reports/status/swarm_runs",
        "reports/status/reviews",
        "scripts",
        "tests",
    ):
        mkdir(root, rel)

    for rel in (
        ".orchestrator/README.md",
        ".orchestrator/AGENTS.md",
        ".orchestrator/backlog/README.md",
        ".orchestrator/active/README.md",
        ".orchestrator/integration_ready/README.md",
        ".orchestrator/ready_for_review/README.md",
        ".orchestrator/blocked/README.md",
        ".orchestrator/done/README.md",
        ".orchestrator/handoff/README.md",
        ".orchestrator/templates/handoff_template.md",
        ".orchestrator/templates/task_template.md",
        "contracts/README.md",
        "contracts/CHANGELOG.md",
        "contracts/AGENTS.md",
        "contracts/assumptions.md",
        "contracts/data_dictionary.md",
        "contracts/schemas/README.md",
        "contracts/schemas/swarm_run_manifest_v1.yaml",
        "contracts/schemas/judge_review_log_v1.yaml",
        "contracts/schemas/parent_units_v1.yaml",
        "contracts/schemas/control_panel_summary_v1.yaml",
        "contracts/schemas/thread_geometry_v1.yaml",
        "contracts/schemas/periodicity_input_v1.yaml",
        "contracts/schemas/archive_metadata_audit_v1.yaml",
        "docs/decisions.md",
        "docs/swarm_deployment_plan.md",
        "docs/prompts/planner.md",
        "docs/prompts/worker.md",
        "docs/prompts/judge.md",
        "docs/prompts/operator.md",
        "docs/runbook_swarm.md",
        "docs/runbook_swarm_automation.md",
        "paper/main.tex",
        "manifests/README.md",
        "qc/README.md",
        "derived/README.md",
        "reports/AGENTS.md",
        "reports/status/README.md",
        "reports/status/swarm_runs/README.md",
        "reports/status/reviews/README.md",
        "scripts/AGENTS.md",
        "tests/README.md",
        "tests/runtime_test_utils.py",
        "tests/test_quality_gates_repo_structure.py",
        "tests/test_quality_gates_integration_ready.py",
        "tests/test_quality_gates_judge_operator.py",
        "tests/test_quality_gates_processed_manifests.py",
        "tests/test_swarm_role_state_semantics.py",
    ):
        write_text(root, rel, "# placeholder\n")

    write_text(root, "docs/stage3_theory_framework_packet.cleaned.md", "# Stage 3\n")
    write_text(root, "docs/data_acquisition_plan.md", "# Acquisition plan\n")
    write_text(root, "analysis/AGENTS.md", "The authority docs outrank every script in this directory.\n")
    write_text(
        root,
        ".orchestrator/workstreams.md",
        "\n".join(
            [
                "# Workstreams",
                "",
                "| Workstream | Purpose | Owns paths | Does NOT own | Example outputs | Network | integration_ready eligible |",
                "|---|---|---|---|---|---|---|",
                "| W0 | Contracts | contracts/ | raw/ | contracts | no | yes |",
                "| W1 | Acquisition | raw/, manifests/ | paper/ | manifests | yes | no |",
                "| W2 | QC | frozen/, qc/ | paper/ | qc | no | no |",
                "| W3 | Derived | derived/ | raw/ | derived tables | no | no |",
                "| W4 | Analysis | analysis/ | contracts/ | scripts | no | no |",
                "| W5 | Writing | paper/main.tex | locked sections | paper wrapper | no | no |",
                "| W9 | Ops | .orchestrator/, reports/status/ | raw/ | runtime logs | no | yes |",
                "",
            ]
        ),
    )

    write_project_yaml(root)
    write_framework_json(root)

    write_text(root, "scripts/swarm.py", SWARM_PATH.read_text(encoding="utf-8"))
    write_text(root, "scripts/quality_gates.py", QUALITY_GATES_PATH.read_text(encoding="utf-8"))
    write_text(root, "scripts/sweep_tasks.py", SWEEP_TASKS_PATH.read_text(encoding="utf-8"))


def _emit_list(key: str, values: list[str]) -> str:
    if not values:
        return f"{key}: []"
    lines = [f"{key}:"]
    lines.extend(f'  - "{value}"' for value in values)
    return "\n".join(lines)


def write_task(
    root: Path,
    folder: str,
    task_id: str,
    *,
    title: str | None = None,
    workstream: str = "W1",
    task_kind: str | None = "acquisition",
    role: str = "Worker",
    priority: str = "medium",
    dependencies: list[str] | None = None,
    integration_ready_dependencies: list[str] | None = None,
    allow_network: bool = False,
    allowed_paths: list[str] | None = None,
    disallowed_paths: list[str] | None = None,
    outputs: list[str] | None = None,
    gates: list[str] | None = None,
    stop_conditions: list[str] | None = None,
    state: str = "backlog",
    last_updated: str = "2026-04-08",
    slug: str = "task",
) -> Path:
    title = title or f"{task_id} title"
    dependencies = dependencies or []
    integration_ready_dependencies = integration_ready_dependencies or []
    allowed_paths = allowed_paths or ["analysis/"]
    disallowed_paths = disallowed_paths or ["contracts/"]
    outputs = outputs or ["analysis/example.py"]
    gates = gates or ["make gate", "make test"]
    stop_conditions = stop_conditions or ["Need @human"]

    frontmatter = "\n".join(
        [
            "---",
            f'task_id: "{task_id}"',
            f'title: "{title}"',
            f'workstream: "{workstream}"',
            f'task_kind: "{task_kind or ""}"',
            f"allow_network: {'true' if allow_network else 'false'}",
            f'role: "{role}"',
            f'priority: "{priority}"',
            _emit_list("dependencies", dependencies),
            _emit_list("integration_ready_dependencies", integration_ready_dependencies),
            _emit_list("allowed_paths", allowed_paths),
            _emit_list("disallowed_paths", disallowed_paths),
            _emit_list("outputs", outputs),
            _emit_list("gates", gates),
            _emit_list("stop_conditions", stop_conditions),
            "---",
        ]
    )

    body = "\n".join(
        [
            f"# Task {task_id} — {title}",
            "",
            "## Context",
            "",
            "Context.",
            "",
            "## Inputs",
            "",
            "- input",
            "",
            "## Outputs",
            "",
            "- output",
            "",
            "## Success Criteria",
            "",
            "- [ ] done",
            "",
            "## Review Bundle Requirements",
            "",
            "- [ ] run manifest",
            "",
            "## Validation / Commands",
            "",
            "- `make gate`",
            "- `make test`",
            "",
            "## Status",
            "",
            f"- State: {state}",
            f"- Last updated: {last_updated}",
            "",
            "## Notes / Decisions",
            "",
            "- 2026-04-08: note",
            "",
        ]
    )

    rel = f".orchestrator/{folder}/{task_id}_{slug}.md"
    return write_text(root, rel, frontmatter + "\n" + body)


def write_run_manifest(
    root: Path,
    task_id: str,
    *,
    task_path: str,
    task_role: str = "Worker",
    workstream: str = "W1",
    state_before: str = "active",
    state_after: str = "ready_for_review",
) -> Path:
    rel = f"reports/status/swarm_runs/{task_id}_20260408T000000Z.json"
    payload = {
        "schema_version": SWARM_RUN_MANIFEST_SCHEMA_VERSION,
        "run_id": f"{task_id}_20260408T000000Z",
        "generated_at_utc": "2026-04-08T00:00:00Z",
        "task": {
            "task_id": task_id,
            "task_path": task_path,
            "title": f"{task_id} title",
            "role": task_role,
            "workstream": workstream,
            "task_kind": "acquisition",
            "dependencies": [],
            "integration_ready_dependencies": [],
            "state_before": state_before,
            "state_after": state_after,
        },
        "repo": {
            "branch": f"{task_id}_branch",
            "git_sha": "0123456789abcdef0123456789abcdef01234567",
            "base_branch": "main",
            "remote": "origin",
        },
        "executor": {
            "role": task_role,
            "runner": "local_swarm",
            "tool": "manual",
            "model": None,
            "sandbox": "workspace-write",
            "allow_network": False,
            "repair_context": None,
            "returncode": 0,
            "error": "executor_skipped",
        },
        "commands": {
            "executor": [],
            "executor_log_path": None,
            "gates": ["make gate", "make test"],
        },
        "gates": [],
        "ownership": {
            "ok": True,
            "changed_paths": [task_path],
            "violations": [],
        },
        "artifacts": {
            "outputs_ok": True,
            "missing_outputs": [],
            "required_manifests_ok": True,
            "missing_manifests": [],
        },
        "result": {
            "status": "ok",
            "blocked_reasons": [],
        },
    }
    return write_json(root, rel, payload)


def write_review_log(
    root: Path,
    task_id: str,
    *,
    task_path: str,
    run_manifest_path: str,
    task_role: str = "Worker",
    reviewer_role: str = "Judge",
    state_before: str = "ready_for_review",
    state_after: str = "done",
    outcome: str = "approve",
) -> Path:
    rel = f"reports/status/reviews/{task_id}_20260408T010000Z.json"
    payload = {
        "schema_version": JUDGE_REVIEW_LOG_SCHEMA_VERSION,
        "review_id": f"{task_id}_20260408T010000Z",
        "generated_at_utc": "2026-04-08T01:00:00Z",
        "reviewer": {
            "role": reviewer_role,
        },
        "task": {
            "task_id": task_id,
            "task_path": task_path,
            "role": task_role,
            "state_before": state_before,
            "state_after": state_after,
            "run_manifest_path": run_manifest_path,
        },
        "checks": {
            "gates_ok": True,
            "outputs_ok": True,
            "required_manifests_ok": True,
            "review_bundle_ok": True,
            "failures": [],
        },
        "decision": {
            "outcome": outcome,
            "note": "review note",
        },
    }
    return write_json(root, rel, payload)
