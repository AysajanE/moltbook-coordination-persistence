from pathlib import Path
import tempfile
import unittest

from runtime_test_utils import chdir, load_quality_gates_module, scaffold_runtime_repo, write_task


quality_gates = load_quality_gates_module()


class IntegrationReadyGateTest(unittest.TestCase):
    def test_gate_accepts_w9_allowlisted_interface_flow(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            write_task(
                root,
                "integration_ready",
                "T200",
                workstream="W9",
                task_kind="ops",
                role="Operator",
                allowed_paths=["docs/runbook_swarm.md"],
                disallowed_paths=["raw/"],
                outputs=["docs/runbook_swarm.md"],
                state="integration_ready",
                slug="ops",
            )
            write_task(
                root,
                "backlog",
                "T201",
                workstream="W9",
                task_kind="ops",
                role="Operator",
                dependencies=["T200"],
                integration_ready_dependencies=["T200"],
                allowed_paths=["reports/status/"],
                disallowed_paths=["raw/"],
                outputs=["reports/status/README.md"],
                state="backlog",
                slug="consumer",
            )

            with chdir(root):
                result = quality_gates.gate_integration_ready_policy()

            self.assertTrue(result.ok, result.details)

    def test_gate_rejects_empirical_raw_task_in_integration_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            write_task(
                root,
                "integration_ready",
                "T210",
                workstream="W1",
                task_kind="acquisition",
                role="Worker",
                allow_network=True,
                allowed_paths=["raw/", "manifests/"],
                disallowed_paths=["contracts/"],
                outputs=["raw/simulamet/20260408/...", "manifests/simulamet_manifest.yaml"],
                state="integration_ready",
                slug="raw",
            )
            write_task(
                root,
                "backlog",
                "T211",
                workstream="W2",
                task_kind="freeze_qc",
                role="Worker",
                dependencies=["T210"],
                integration_ready_dependencies=["T210"],
                allowed_paths=["qc/"],
                disallowed_paths=["contracts/"],
                outputs=["qc/archive_qc_report_simulamet.md"],
                state="backlog",
                slug="consumer",
            )

            with chdir(root):
                result = quality_gates.gate_integration_ready_policy()

            self.assertFalse(result.ok)
            failures = result.details.get("failures") or []
            self.assertTrue(any("integration_ready_ineligible" in failure for failure in failures))


if __name__ == "__main__":
    unittest.main()
