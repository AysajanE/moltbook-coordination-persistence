from pathlib import Path
import tempfile
import unittest

from runtime_test_utils import (
    chdir,
    load_quality_gates_module,
    scaffold_runtime_repo,
    write_review_log,
    write_run_manifest,
    write_task,
    write_text,
)


quality_gates = load_quality_gates_module()


class JudgeOperatorGateTest(unittest.TestCase):
    def test_worker_task_cannot_claim_reports_status(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            write_task(
                root,
                "backlog",
                "T400",
                workstream="W4",
                task_kind="analysis",
                role="Worker",
                allowed_paths=["reports/status/"],
                disallowed_paths=["contracts/"],
                outputs=["reports/status/README.md"],
                state="backlog",
                slug="status",
            )

            with chdir(root):
                result = quality_gates.gate_operator_surface_ownership()

            self.assertFalse(result.ok)
            failures = result.details.get("failures") or []
            self.assertTrue(any("operator_owned_surface" in failure for failure in failures))

    def test_done_task_requires_valid_judge_review_log(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            output_path = "docs/swarm_deployment_plan.md"
            write_text(root, output_path, "# plan\n")

            task_path = write_task(
                root,
                "done",
                "T401",
                workstream="W9",
                task_kind="ops",
                role="Operator",
                allowed_paths=["docs/runbook_swarm.md"],
                disallowed_paths=["contracts/"],
                outputs=[output_path],
                state="done",
                slug="done",
            )

            run_manifest_path = write_run_manifest(
                root,
                "T401",
                task_path=task_path.relative_to(root).as_posix(),
                task_role="Operator",
                workstream="W9",
                state_after="ready_for_review",
            )

            with chdir(root):
                missing_review = quality_gates.gate_review_bundle_integrity()

            self.assertFalse(missing_review.ok)
            self.assertTrue(
                any("missing_review_log" in failure for failure in (missing_review.details.get("failures") or []))
            )

            write_review_log(
                root,
                "T401",
                task_path=task_path.relative_to(root).as_posix(),
                run_manifest_path=run_manifest_path.relative_to(root).as_posix(),
                reviewer_role="Worker",
                outcome="approve",
            )

            with chdir(root):
                invalid_review = quality_gates.gate_judge_review_log_validity()

            self.assertFalse(invalid_review.ok)
            self.assertTrue(
                any("invalid_reviewer_role" in failure for failure in (invalid_review.details.get("failures") or []))
            )

            for path in (root / "reports" / "status" / "reviews").glob("*.json"):
                path.unlink()

            write_review_log(
                root,
                "T401",
                task_path=task_path.relative_to(root).as_posix(),
                run_manifest_path=run_manifest_path.relative_to(root).as_posix(),
                reviewer_role="Judge",
                outcome="approve",
            )

            with chdir(root):
                valid_review = quality_gates.gate_judge_review_log_validity()
                valid_bundle = quality_gates.gate_review_bundle_integrity()

            self.assertTrue(valid_review.ok, valid_review.details)
            self.assertTrue(valid_bundle.ok, valid_bundle.details)


if __name__ == "__main__":
    unittest.main()
