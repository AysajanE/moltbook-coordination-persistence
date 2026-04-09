from pathlib import Path
import tempfile
import unittest

from runtime_test_utils import chdir, load_quality_gates_module, scaffold_runtime_repo, write_task


quality_gates = load_quality_gates_module()


class TaskHygieneGateTest(unittest.TestCase):
    def test_placeholder_validation_text_fails_task_hygiene(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            write_task(
                root,
                "backlog",
                "T500",
                workstream="W1",
                allow_network=True,
                allowed_paths=["scripts/download_moltbook_observatory_archive.py"],
                disallowed_paths=["contracts/"],
                outputs=["manifests/simulamet_manifest.yaml"],
                validation_commands=["Add task-specific acquisition commands here."],
                slug="placeholder",
            )

            with chdir(root):
                result = quality_gates.gate_task_hygiene()

            self.assertFalse(result.ok)
            failures = result.details.get("failures") or []
            self.assertTrue(any("placeholder_validation_text" in failure for failure in failures))

    def test_broad_allowed_path_and_directory_output_fail_task_hygiene(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            write_task(
                root,
                "backlog",
                "T501",
                workstream="W4",
                task_kind="analysis",
                allowed_paths=["analysis/"],
                disallowed_paths=["contracts/"],
                outputs=["reports/status/"],
                validation_commands=["`python analysis/flagship_control_panel_margins.py`"],
                slug="broad",
            )

            with chdir(root):
                result = quality_gates.gate_task_hygiene()

            self.assertFalse(result.ok)
            failures = result.details.get("failures") or []
            self.assertTrue(any("broad_allowed_path_requires_narrower_scope" in failure for failure in failures))
            self.assertTrue(any("directory_output_requires_pattern" in failure for failure in failures))

    def test_workstream_path_ownership_rejects_cross_stream_script(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            write_task(
                root,
                "backlog",
                "T502",
                workstream="W4",
                task_kind="analysis",
                allowed_paths=["analysis/hf_archive_validate.py"],
                disallowed_paths=["contracts/"],
                outputs=["qc/analysis_execution_simulamet.md"],
                validation_commands=["`python analysis/hf_archive_validate.py`"],
                slug="ownership",
            )

            with chdir(root):
                result = quality_gates.gate_workstream_path_ownership()

            self.assertFalse(result.ok)
            failures = result.details.get("failures") or []
            self.assertTrue(any("path_outside_workstream_surface" in failure for failure in failures))


if __name__ == "__main__":
    unittest.main()
