from pathlib import Path
import tempfile
import unittest

from runtime_test_utils import (
    chdir,
    load_quality_gates_module,
    scaffold_runtime_repo,
    write_run_manifest,
    write_task,
    write_text,
)


quality_gates = load_quality_gates_module()


class RawManifestGateTest(unittest.TestCase):
    def test_ready_for_review_raw_output_requires_manifest_output(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            write_text(root, "raw/simulamet/20260408/master.json", "{}\n")

            task_path = write_task(
                root,
                "ready_for_review",
                "T300",
                workstream="W1",
                task_kind="acquisition",
                role="Worker",
                allow_network=True,
                allowed_paths=["raw/"],
                disallowed_paths=["contracts/"],
                outputs=["raw/simulamet/20260408/..."],
                state="ready_for_review",
                slug="raw",
            )
            write_run_manifest(
                root,
                "T300",
                task_path=task_path.relative_to(root).as_posix(),
                workstream="W1",
            )

            with chdir(root):
                result = quality_gates.gate_review_bundle_integrity()

            self.assertFalse(result.ok)
            failures = result.details.get("failures") or []
            self.assertTrue(any("missing_declared_raw_manifest_output" in failure for failure in failures))

    def test_valid_raw_manifest_allows_review_bundle_to_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            write_text(root, "raw/simulamet/20260408/master.json", "{}\n")
            write_text(root, "manifests/simulamet_manifest.yaml", "archive_name: simulamet\n")

            task_path = write_task(
                root,
                "ready_for_review",
                "T301",
                workstream="W1",
                task_kind="acquisition",
                role="Worker",
                allow_network=True,
                allowed_paths=["raw/", "manifests/"],
                disallowed_paths=["contracts/"],
                outputs=["raw/simulamet/20260408/...", "manifests/simulamet_manifest.yaml"],
                state="ready_for_review",
                slug="raw",
            )
            write_run_manifest(
                root,
                "T301",
                task_path=task_path.relative_to(root).as_posix(),
                workstream="W1",
            )

            with chdir(root):
                result = quality_gates.gate_review_bundle_integrity()

            self.assertTrue(result.ok, result.details)

    def test_blocked_run_manifest_does_not_count_as_reviewable_success(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            write_text(root, "raw/simulamet/2026-04-08/master.json", "{}\n")
            write_text(root, "manifests/simulamet_manifest.yaml", "archive_name: simulamet\n")

            task_path = write_task(
                root,
                "ready_for_review",
                "T302",
                workstream="W1",
                task_kind="acquisition",
                role="Worker",
                allow_network=True,
                allowed_paths=["raw/", "manifests/"],
                disallowed_paths=["contracts/"],
                outputs=["raw/simulamet/2026-04-08/...", "manifests/simulamet_manifest.yaml"],
                state="ready_for_review",
                slug="blocked-manifest",
            )
            write_run_manifest(
                root,
                "T302",
                task_path=task_path.relative_to(root).as_posix(),
                workstream="W1",
                state_after="blocked",
                result_status="blocked",
                blocked_reasons=["stale_outputs"],
            )

            with chdir(root):
                result = quality_gates.gate_review_bundle_integrity()

            self.assertFalse(result.ok)
            failures = result.details.get("failures") or []
            self.assertTrue(any("missing_reviewable_run_manifest" in failure for failure in failures))


if __name__ == "__main__":
    unittest.main()
