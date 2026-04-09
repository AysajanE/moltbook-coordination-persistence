#!/usr/bin/env python3
"""
Deterministic runtime and repository quality gates for the flagship swarm deployment.

Scope:
- enforce Stage 3 source precedence and flagship repo structure
- enforce task hygiene, workstream boundaries, and review-bundle integrity
- validate runtime run manifests and Judge review logs
- stay offline and sample-safe
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
import importlib.util
import json
from pathlib import Path
import re
import sys
from typing import Any


SWARM_RUN_MANIFEST_SCHEMA_VERSION = "research_swarm.runtime_run_manifest.v1"
JUDGE_REVIEW_LOG_SCHEMA_VERSION = "research_swarm.judge_review_log.v1"

REQUIRED_TASK_HEADINGS = (
    "## Context",
    "## Inputs",
    "## Outputs",
    "## Success Criteria",
    "## Review Bundle Requirements",
    "## Validation / Commands",
    "## Status",
    "## Notes / Decisions",
)


@dataclass(frozen=True)
class GateResult:
    ok: bool
    details: dict[str, object]


_SWARM_MODULE = None


def _repo_root() -> Path:
    return Path.cwd()


def _read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _load_json(path: Path) -> tuple[dict[str, Any] | None, str | None]:
    try:
        payload = json.loads(_read_text(path))
    except FileNotFoundError:
        return None, "missing"
    except json.JSONDecodeError as exc:
        return None, f"invalid_json:{exc}"
    if not isinstance(payload, dict):
        return None, "not_json_object"
    return payload, None


def _normalize_path(value: str) -> str:
    out = value.strip().replace("\\", "/")
    while out.startswith("./"):
        out = out[2:]
    return out


def _path_matches_prefix(value: str, prefix: str) -> bool:
    norm_value = _normalize_path(value)
    norm_prefix = _normalize_path(prefix)
    if norm_value == norm_prefix.rstrip("/"):
        return True
    return norm_value.startswith(norm_prefix)


def _parse_simple_yaml_scalar(text: str, key: str) -> str | None:
    pattern = re.compile(rf"^\s*{re.escape(key)}:\s*(.+?)\s*$", flags=re.MULTILINE)
    match = pattern.search(text)
    if match is None:
        return None
    return match.group(1).strip().strip("'\"")


def _load_swarm_module():
    global _SWARM_MODULE
    if _SWARM_MODULE is not None:
        return _SWARM_MODULE

    swarm_path = _repo_root() / "scripts" / "swarm.py"
    spec = importlib.util.spec_from_file_location("flagship_swarm_module", swarm_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable_to_load_swarm_module:{swarm_path}")
    mod = importlib.util.module_from_spec(spec)
    sys.modules["flagship_swarm_module"] = mod
    spec.loader.exec_module(mod)
    _SWARM_MODULE = mod
    return mod


def _load_framework_payload() -> dict[str, Any]:
    path = _repo_root() / "contracts" / "framework.json"
    payload, error = _load_json(path)
    if error is not None or payload is None:
        raise ValueError(f"invalid_framework_json:{path}:{error}")
    return payload


def _load_project_text() -> str:
    path = _repo_root() / "contracts" / "project.yaml"
    if not path.exists():
        raise ValueError(f"missing_project_contract:{path}")
    return _read_text(path)


def _load_contract():
    swarm = _load_swarm_module()
    return swarm.load_framework_contract(_repo_root())


def _flatten_required_paths(payload: dict[str, Any], project_mode: str | None) -> list[str]:
    required = payload.get("required_paths")
    if not isinstance(required, dict):
        return []
    out: list[str] = []
    for key in ("common", project_mode or ""):
        value = required.get(key)
        if not isinstance(value, list):
            continue
        for item in value:
            if isinstance(item, str) and item.strip():
                out.append(_normalize_path(item))
    return out


def _collect_tasks(contract) -> tuple[dict[str, Any], list[str]]:
    swarm = _load_swarm_module()
    tasks: dict[str, Any] = {}
    failures: list[str] = []
    for path in swarm._iter_task_files(contract):
        try:
            task = swarm.load_task(path, contract)
        except Exception as exc:
            failures.append(str(exc))
            continue
        tasks[task.task_id] = task
    return tasks, failures


def _validate_required_keys(payload: object, keys: set[str], label: str) -> list[str]:
    if not isinstance(payload, dict):
        return [f"{label}_not_object"]
    failures: list[str] = []
    for key in sorted(keys):
        if key not in payload:
            failures.append(f"{label}_missing_key:{key}")
    return failures


def _matching_task_jsons(directory: Path, task_id: str) -> list[Path]:
    if not directory.exists():
        return []
    return sorted(path for path in directory.glob(f"{task_id}_*.json") if path.is_file())


def _validate_swarm_run_manifest(path: Path, contract) -> list[str]:
    payload, error = _load_json(path)
    if error is not None or payload is None:
        return [f"{path}:{error}"]

    failures: list[str] = []
    if payload.get("schema_version") != SWARM_RUN_MANIFEST_SCHEMA_VERSION:
        failures.append(f"{path}:invalid_schema_version:{payload.get('schema_version')}")

    failures.extend(
        f"{path}:{failure}"
        for failure in _validate_required_keys(
            payload,
            {
                "schema_version",
                "run_id",
                "generated_at_utc",
                "task",
                "repo",
                "executor",
                "commands",
                "gates",
                "ownership",
                "artifacts",
                "result",
            },
            "top",
        )
    )

    task = payload.get("task")
    failures.extend(
        f"{path}:{failure}"
        for failure in _validate_required_keys(
            task,
            {"task_id", "task_path", "title", "role", "workstream", "state_before", "state_after"},
            "task",
        )
    )
    if isinstance(task, dict):
        if task.get("role") not in set(contract.allowed_roles):
            failures.append(f"{path}:invalid_task_role:{task.get('role')}")
        if task.get("state_after") not in set(contract.allowed_states):
            failures.append(f"{path}:invalid_task_state_after:{task.get('state_after')}")

    repo = payload.get("repo")
    failures.extend(
        f"{path}:{failure}"
        for failure in _validate_required_keys(repo, {"branch", "git_sha", "base_branch", "remote"}, "repo")
    )

    executor = payload.get("executor")
    failures.extend(
        f"{path}:{failure}"
        for failure in _validate_required_keys(executor, {"role", "runner", "tool", "allow_network"}, "executor")
    )

    result = payload.get("result")
    failures.extend(
        f"{path}:{failure}"
        for failure in _validate_required_keys(result, {"status", "blocked_reasons"}, "result")
    )
    if isinstance(result, dict) and result.get("status") not in {"ok", "blocked"}:
        failures.append(f"{path}:invalid_result_status:{result.get('status')}")

    return failures


def _validate_judge_review_log(path: Path, contract) -> list[str]:
    payload, error = _load_json(path)
    if error is not None or payload is None:
        return [f"{path}:{error}"]

    failures: list[str] = []
    if payload.get("schema_version") != JUDGE_REVIEW_LOG_SCHEMA_VERSION:
        failures.append(f"{path}:invalid_schema_version:{payload.get('schema_version')}")

    failures.extend(
        f"{path}:{failure}"
        for failure in _validate_required_keys(
            payload,
            {"schema_version", "review_id", "generated_at_utc", "reviewer", "task", "checks", "decision"},
            "top",
        )
    )

    reviewer = payload.get("reviewer")
    failures.extend(
        f"{path}:{failure}" for failure in _validate_required_keys(reviewer, {"role"}, "reviewer")
    )
    if isinstance(reviewer, dict) and reviewer.get("role") != contract.scientific_review_role:
        failures.append(f"{path}:invalid_reviewer_role:{reviewer.get('role')}")

    task = payload.get("task")
    failures.extend(
        f"{path}:{failure}"
        for failure in _validate_required_keys(
            task, {"task_id", "task_path", "role", "state_before", "state_after", "run_manifest_path"}, "task"
        )
    )
    if isinstance(task, dict):
        if task.get("state_after") not in set(contract.allowed_states):
            failures.append(f"{path}:invalid_task_state_after:{task.get('state_after')}")

    decision = payload.get("decision")
    failures.extend(
        f"{path}:{failure}" for failure in _validate_required_keys(decision, {"outcome", "note"}, "decision")
    )
    if isinstance(decision, dict):
        if decision.get("outcome") not in {"approve", "revise", "block"}:
            failures.append(f"{path}:invalid_decision_outcome:{decision.get('outcome')}")
        if decision.get("outcome") == "approve" and isinstance(task, dict) and task.get("state_after") != "done":
            failures.append(f"{path}:approve_without_done")

    return failures


def gate_framework_contract() -> GateResult:
    try:
        payload = _load_framework_payload()
        contract = _load_contract()
    except Exception as exc:
        return GateResult(ok=False, details={"failures": [str(exc)]})

    failures: list[str] = []

    roles = payload.get("roles")
    if not isinstance(roles, dict):
        failures.append("missing_roles_section")
    else:
        allowed = set(roles.get("allowed") or [])
        for role in ("Planner", "Worker", "Judge", "Operator"):
            if role not in allowed:
                failures.append(f"missing_role:{role}")

    states = payload.get("states")
    if not isinstance(states, dict):
        failures.append("missing_states_section")
    else:
        allowed_states = set(states.get("allowed") or [])
        for state in ("backlog", "active", "integration_ready", "ready_for_review", "blocked", "done"):
            if state not in allowed_states:
                failures.append(f"missing_state:{state}")

    if contract.scientific_review_role != "Judge":
        failures.append(f"invalid_scientific_review_role:{contract.scientific_review_role}")
    if contract.run_manifest_dir.relative_to(_repo_root()).as_posix() != "reports/status/swarm_runs":
        failures.append(f"invalid_run_manifest_dir:{contract.run_manifest_dir}")
    if contract.judge_review_dir.relative_to(_repo_root()).as_posix() != "reports/status/reviews":
        failures.append(f"invalid_judge_review_dir:{contract.judge_review_dir}")
    if set(contract.network_workstreams) != {"W1"}:
        failures.append(f"invalid_network_workstreams:{list(contract.network_workstreams)}")
    if set(contract.integration_ready_eligible_workstreams) != {"W0", "W9"}:
        failures.append(
            f"invalid_integration_ready_workstreams:{list(contract.integration_ready_eligible_workstreams)}"
        )

    prompt_templates = payload.get("prompt_templates")
    if not isinstance(prompt_templates, dict):
        failures.append("missing_prompt_templates")
    else:
        for key in ("planner", "worker", "judge", "operator"):
            if key not in prompt_templates:
                failures.append(f"missing_prompt_template:{key}")

    return GateResult(ok=len(failures) == 0, details={"failures": failures})


def gate_repo_structure() -> GateResult:
    try:
        payload = _load_framework_payload()
        project_text = _load_project_text()
    except Exception as exc:
        return GateResult(ok=False, details={"failures": [str(exc)]})

    project_mode = _parse_simple_yaml_scalar(project_text, "mode")
    required = [
        "AGENTS.md",
        "README.md",
        "Makefile",
        ".orchestrator/README.md",
        ".orchestrator/AGENTS.md",
        ".orchestrator/workstreams.md",
        ".orchestrator/templates/task_template.md",
        ".orchestrator/templates/handoff_template.md",
        "contracts/README.md",
        "contracts/CHANGELOG.md",
        "contracts/AGENTS.md",
        "contracts/assumptions.md",
        "contracts/data_dictionary.md",
        "contracts/project.yaml",
        "contracts/framework.json",
        "contracts/schemas/README.md",
        "contracts/schemas/swarm_run_manifest_v1.yaml",
        "contracts/schemas/judge_review_log_v1.yaml",
        "contracts/schemas/parent_units_v1.yaml",
        "contracts/schemas/control_panel_summary_v1.yaml",
        "contracts/schemas/thread_geometry_v1.yaml",
        "contracts/schemas/periodicity_input_v1.yaml",
        "contracts/schemas/archive_metadata_audit_v1.yaml",
        "docs/swarm_deployment_plan.md",
        "docs/prompts/planner.md",
        "docs/prompts/worker.md",
        "docs/prompts/judge.md",
        "docs/prompts/operator.md",
        "docs/runbook_swarm.md",
        "docs/runbook_swarm_automation.md",
        "analysis/AGENTS.md",
        "paper/main.tex",
        "manifests/README.md",
        "qc/README.md",
        "derived/README.md",
        "reports/AGENTS.md",
        "reports/status/README.md",
        "reports/status/swarm_runs/README.md",
        "reports/status/reviews/README.md",
        "scripts/AGENTS.md",
        "scripts/swarm.py",
        "scripts/quality_gates.py",
        "scripts/sweep_tasks.py",
        "tests/README.md",
        "tests/runtime_test_utils.py",
        "tests/test_quality_gates_repo_structure.py",
        "tests/test_quality_gates_integration_ready.py",
        "tests/test_quality_gates_judge_operator.py",
        "tests/test_quality_gates_processed_manifests.py",
        "tests/test_swarm_role_state_semantics.py",
    ]
    required.extend(_flatten_required_paths(payload, project_mode))

    missing = sorted({path for path in required if not (_repo_root() / path).exists()})
    return GateResult(ok=len(missing) == 0, details={"missing": missing})


def gate_project_contract() -> GateResult:
    try:
        text = _load_project_text()
    except Exception as exc:
        return GateResult(ok=False, details={"failures": [str(exc)]})

    failures: list[str] = []
    if _parse_simple_yaml_scalar(text, "mode") != "empirical":
        failures.append("project_mode_must_be_empirical")
    top_authority = _parse_simple_yaml_scalar(text, "top_authority")
    operational_plan = _parse_simple_yaml_scalar(text, "operational_plan")
    if not top_authority:
        failures.append("missing_top_authority")
    if not operational_plan:
        failures.append("missing_operational_plan")
    if "public_authority_surfaces:" not in text:
        failures.append("missing_public_authority_surfaces")
    if "type: latex" not in text:
        failures.append("paper_substrate_must_be_latex")
    return GateResult(ok=len(failures) == 0, details={"failures": failures})


def gate_authority_sources() -> GateResult:
    failures: list[str] = []

    agents_text = _read_text(_repo_root() / "AGENTS.md")
    if "README.md" not in agents_text:
        failures.append("root_agents_missing_readme_precedence")
    if "contracts/project.yaml" not in agents_text or "contracts/framework.json" not in agents_text:
        failures.append("root_agents_missing_contract_precedence")
    if "analysis/flagship_control_panel_margins.py" not in agents_text:
        failures.append("root_agents_missing_legacy_script_warning")

    analysis_agents = _read_text(_repo_root() / "analysis" / "AGENTS.md")
    if "README.md" not in analysis_agents or "outrank every script" not in analysis_agents:
        failures.append("analysis_agents_missing_authority_boundary")

    task_template = _read_text(_repo_root() / ".orchestrator" / "templates" / "task_template.md")
    for path in (
        "README.md",
        "docs/swarm_deployment_plan.md",
        "paper/sections/model.tex",
        "paper/sections/methods.tex",
    ):
        if path not in task_template:
            failures.append(f"task_template_missing_locked_disallowed_path:{path}")

    return GateResult(ok=len(failures) == 0, details={"failures": failures})


def gate_workstreams_complete() -> GateResult:
    path = _repo_root() / ".orchestrator" / "workstreams.md"
    if not path.exists():
        return GateResult(ok=False, details={"failures": [f"missing_workstreams:{path}"]})

    failures: list[str] = []
    seen: set[str] = set()
    for line in _read_text(path).splitlines():
        if not re.match(r"^\|\s*W\d+\s+\|", line):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if len(cells) < 7:
            failures.append(f"malformed_row:{line.strip()}")
            continue
        workstream = cells[0]
        seen.add(workstream)
        if not cells[1]:
            failures.append(f"blank_purpose:{workstream}")
        if not cells[2]:
            failures.append(f"blank_owns_paths:{workstream}")
        if not cells[3]:
            failures.append(f"blank_does_not_own:{workstream}")
    for workstream in ("W0", "W1", "W2", "W3", "W4", "W5", "W9"):
        if workstream not in seen:
            failures.append(f"missing_workstream:{workstream}")
    return GateResult(ok=len(failures) == 0, details={"failures": failures})


def gate_task_hygiene() -> GateResult:
    try:
        contract = _load_contract()
    except Exception as exc:
        return GateResult(ok=False, details={"failures": [str(exc)]})

    swarm = _load_swarm_module()
    failures: list[str] = []
    task_paths = list(swarm._iter_task_files(contract))
    if not task_paths:
        failures.append("no_task_files_found")

    for path in task_paths:
        text = _read_text(path)
        frontmatter = swarm._parse_task_frontmatter(text)
        if frontmatter is None:
            failures.append(f"{path}:missing_yaml_frontmatter")
            continue

        for key in swarm.REQUIRED_FRONTMATTER_KEYS:
            if key not in frontmatter:
                failures.append(f"{path}:frontmatter_missing_key:{key}")

        task_id = frontmatter.get("task_id")
        if isinstance(task_id, str) and not path.name.startswith(task_id):
            failures.append(f"{path}:task_id_filename_mismatch:{task_id}")

        workstream = frontmatter.get("workstream")
        allow_network = swarm._coerce_bool(frontmatter.get("allow_network"), default=False)
        if allow_network and workstream != "W1":
            failures.append(f"{path}:network_workstream_not_allowlisted:{workstream}")

        outputs = swarm._coerce_str_list(frontmatter.get("outputs"))
        if any(_path_matches_prefix(output, "raw/") for output in outputs) and not any(
            _path_matches_prefix(output, "manifests/") for output in outputs
        ):
            failures.append(f"{path}:raw_outputs_missing_manifest_output")

        allowed_paths = swarm._coerce_str_list(frontmatter.get("allowed_paths"))
        if workstream != "W0":
            for protected in (
                "README.md",
                "docs/swarm_deployment_plan.md",
                "paper/sections/model.tex",
                "paper/sections/methods.tex",
                "paper/sections/supplementary_material.tex",
            ):
                if any(_path_matches_prefix(item, protected) for item in allowed_paths):
                    failures.append(f"{path}:non_w0_task_claims_locked_surface:{protected}")

        for heading in REQUIRED_TASK_HEADINGS:
            if heading not in text:
                failures.append(f"{path}:missing_heading:{heading}")

        state = swarm._parse_status_value(text, "State")
        if state is None:
            failures.append(f"{path}:missing_state")
        elif state not in set(contract.allowed_states):
            failures.append(f"{path}:invalid_state:{state}")

        last_updated = swarm._parse_status_value(text, "Last updated")
        if last_updated is None or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", last_updated):
            failures.append(f"{path}:invalid_last_updated:{last_updated}")

    return GateResult(ok=len(failures) == 0, details={"failures": failures})


def gate_task_dependencies() -> GateResult:
    try:
        contract = _load_contract()
    except Exception as exc:
        return GateResult(ok=False, details={"failures": [str(exc)]})

    tasks, parse_failures = _collect_tasks(contract)
    failures = list(parse_failures)

    for task in tasks.values():
        for dep in task.dependencies:
            if not re.fullmatch(r"T\d{3}", dep):
                failures.append(f"{task.path}:invalid_dependency_id:{dep}")
            elif dep == task.task_id:
                failures.append(f"{task.path}:self_dependency:{dep}")
            elif dep not in tasks:
                failures.append(f"{task.path}:missing_dependency:{dep}")

    visiting: set[str] = set()
    visited: set[str] = set()

    def dfs(task_id: str, stack: list[str]) -> None:
        if task_id in visited:
            return
        if task_id in visiting:
            if task_id in stack:
                cycle = stack[stack.index(task_id) :] + [task_id]
                failures.append(f"dependency_cycle:{'->'.join(cycle)}")
            return
        visiting.add(task_id)
        stack.append(task_id)
        for dep in tasks[task_id].dependencies:
            if dep in tasks:
                dfs(dep, stack)
        stack.pop()
        visiting.remove(task_id)
        visited.add(task_id)

    for task_id in sorted(tasks):
        dfs(task_id, [])

    return GateResult(ok=len(failures) == 0, details={"failures": failures})


def gate_integration_ready_policy() -> GateResult:
    try:
        contract = _load_contract()
    except Exception as exc:
        return GateResult(ok=False, details={"failures": [str(exc)]})

    swarm = _load_swarm_module()
    tasks, parse_failures = _collect_tasks(contract)
    failures = list(parse_failures)

    for task in tasks.values():
        for dep in task.integration_ready_dependencies:
            if dep not in task.dependencies:
                failures.append(f"{task.path}:integration_ready_dependency_not_in_dependencies:{dep}")
            if dep not in tasks:
                failures.append(f"{task.path}:integration_ready_dependency_missing_task:{dep}")

        if task.state != "integration_ready":
            continue

        if not swarm.task_is_integration_ready_eligible(task, contract):
            failures.append(f"{task.path}:integration_ready_ineligible")

        if not any(task.task_id in other.integration_ready_dependencies for other in tasks.values()):
            failures.append(f"{task.path}:integration_ready_missing_downstream_allowlist")

    return GateResult(ok=len(failures) == 0, details={"failures": failures})


def gate_operator_surface_ownership() -> GateResult:
    try:
        contract = _load_contract()
    except Exception as exc:
        return GateResult(ok=False, details={"failures": [str(exc)]})

    tasks, parse_failures = _collect_tasks(contract)
    failures = list(parse_failures)

    for task in tasks.values():
        if task.role == "Operator":
            if task.workstream != "W9" and task.task_kind != "ops":
                failures.append(f"{task.path}:operator_role_outside_ops_boundary")
            continue

        for surface in contract.operator_owned_shared_surfaces:
            for raw_path in [*task.allowed_paths, *task.outputs]:
                if _path_matches_prefix(raw_path, surface):
                    failures.append(f"{task.path}:operator_owned_surface:{surface}:{raw_path}")

    return GateResult(ok=len(failures) == 0, details={"failures": failures})


def gate_swarm_run_manifest_validity() -> GateResult:
    try:
        contract = _load_contract()
    except Exception as exc:
        return GateResult(ok=False, details={"failures": [str(exc)]})

    manifest_dir = contract.run_manifest_dir
    if not manifest_dir.exists():
        return GateResult(ok=True, details={"skipped": True, "reason": "run_manifest_dir_missing"})

    failures: list[str] = []
    for path in sorted(manifest_dir.glob("*.json")):
        failures.extend(_validate_swarm_run_manifest(path, contract))
    return GateResult(ok=len(failures) == 0, details={"failures": failures})


def gate_judge_review_log_validity() -> GateResult:
    try:
        contract = _load_contract()
    except Exception as exc:
        return GateResult(ok=False, details={"failures": [str(exc)]})

    review_dir = contract.judge_review_dir
    if not review_dir.exists():
        return GateResult(ok=True, details={"skipped": True, "reason": "judge_review_dir_missing"})

    failures: list[str] = []
    for path in sorted(review_dir.glob("*.json")):
        failures.extend(_validate_judge_review_log(path, contract))
    return GateResult(ok=len(failures) == 0, details={"failures": failures})


def gate_review_bundle_integrity() -> GateResult:
    try:
        contract = _load_contract()
    except Exception as exc:
        return GateResult(ok=False, details={"failures": [str(exc)]})

    swarm = _load_swarm_module()
    tasks, parse_failures = _collect_tasks(contract)
    failures = list(parse_failures)
    repo = _repo_root()

    for task in tasks.values():
        if task.state not in {"integration_ready", "ready_for_review", "done"}:
            continue

        outputs_ok, output_failures = swarm._check_declared_outputs_exist(repo=repo, task=task)
        if not outputs_ok:
            failures.append(
                f"{task.path}:missing_outputs:"
                + ";".join(f"{item['output']}={item['reason']}" for item in output_failures)
            )

        for reason in swarm.required_manifest_failures(repo, task):
            failures.append(f"{task.path}:required_manifest_failure:{reason}")

        run_manifests = _matching_task_jsons(contract.run_manifest_dir, task.task_id)
        valid_run_manifests = [path for path in run_manifests if not _validate_swarm_run_manifest(path, contract)]
        if not valid_run_manifests:
            failures.append(
                f"{task.path}:" + ("invalid_run_manifest" if run_manifests else "missing_run_manifest")
            )

        if task.state == "done":
            review_logs = _matching_task_jsons(contract.judge_review_dir, task.task_id)
            valid_review_logs = [path for path in review_logs if not _validate_judge_review_log(path, contract)]
            if not valid_review_logs:
                failures.append(
                    f"{task.path}:" + ("invalid_review_log" if review_logs else "missing_review_log")
                )

    return GateResult(ok=len(failures) == 0, details={"failures": failures})


def _collect_gate_results() -> dict[str, GateResult]:
    return {
        "framework_contract": gate_framework_contract(),
        "repo_structure": gate_repo_structure(),
        "project_contract": gate_project_contract(),
        "authority_sources": gate_authority_sources(),
        "workstreams_complete": gate_workstreams_complete(),
        "task_hygiene": gate_task_hygiene(),
        "task_dependencies": gate_task_dependencies(),
        "integration_ready_policy": gate_integration_ready_policy(),
        "operator_surface_ownership": gate_operator_surface_ownership(),
        "swarm_run_manifest_validity": gate_swarm_run_manifest_validity(),
        "judge_review_log_validity": gate_judge_review_log_validity(),
        "review_bundle_integrity": gate_review_bundle_integrity(),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="quality_gates.py")
    parser.add_argument("--json", action="store_true", help="Print machine-readable output")
    args = parser.parse_args(argv)

    results = _collect_gate_results()
    overall_ok = all(result.ok for result in results.values())

    if args.json:
        payload = {
            "ok": overall_ok,
            "results": {
                name: {"ok": result.ok, "details": result.details}
                for name, result in results.items()
            },
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        for name, result in results.items():
            print(f"[{name}] ok={result.ok} details={result.details}")

    return 0 if overall_ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
