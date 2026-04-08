from pathlib import Path
import tempfile
import unittest

from runtime_test_utils import load_swarm_module, load_sweep_module, scaffold_runtime_repo, write_task


swarm = load_swarm_module()
sweep = load_sweep_module()


class SwarmRoleStateSemanticsTest(unittest.TestCase):
    def test_operator_role_and_integration_ready_state_parse(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            task_path = write_task(
                root,
                "integration_ready",
                "T900",
                workstream="W9",
                task_kind="ops",
                role="Operator",
                allowed_paths=["docs/runbook_swarm.md"],
                disallowed_paths=["raw/"],
                outputs=["docs/runbook_swarm.md"],
                state="integration_ready",
            )

            contract = swarm.load_framework_contract(root)
            task = swarm.load_task(task_path, contract)

            self.assertEqual(task.role, "Operator")
            self.assertEqual(task.state, "integration_ready")
            self.assertTrue(swarm.task_is_integration_ready_eligible(task, contract))

    def test_dependency_requires_done_or_allowlisted_integration_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            write_task(
                root,
                "integration_ready",
                "T100",
                workstream="W0",
                task_kind="contracts",
                role="Worker",
                allowed_paths=["contracts/"],
                disallowed_paths=["raw/"],
                outputs=["contracts/README.md"],
                state="integration_ready",
                slug="contracts",
            )
            write_task(
                root,
                "backlog",
                "T110",
                workstream="W9",
                task_kind="ops",
                role="Operator",
                dependencies=["T100"],
                integration_ready_dependencies=["T100"],
                allowed_paths=["reports/status/"],
                disallowed_paths=["raw/"],
                outputs=["reports/status/README.md"],
                state="backlog",
                slug="consumer",
            )

            write_task(
                root,
                "integration_ready",
                "T120",
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
                "T130",
                workstream="W2",
                task_kind="freeze_qc",
                role="Worker",
                dependencies=["T120"],
                integration_ready_dependencies=["T120"],
                allowed_paths=["qc/"],
                disallowed_paths=["contracts/"],
                outputs=["qc/archive_qc_report_simulamet.md"],
                state="backlog",
                slug="qc",
            )

            contract = swarm.load_framework_contract(root)
            tasks = swarm.load_tasks(contract)

            self.assertTrue(swarm.dependency_is_satisfied("T100", tasks["T110"], tasks, contract))
            self.assertFalse(swarm.dependency_is_satisfied("T120", tasks["T130"], tasks, contract))

    def test_sweep_plans_projection_move(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            task_path = write_task(
                root,
                "backlog",
                "T140",
                workstream="W4",
                task_kind="analysis",
                role="Worker",
                allowed_paths=["analysis/"],
                disallowed_paths=["contracts/"],
                outputs=["analysis/flagship_control_panel_margins.py"],
                state="ready_for_review",
                slug="projection",
            )

            moves, problems = sweep.plan_sweep(root)

            self.assertEqual(problems, [])
            self.assertEqual(len(moves), 1)
            source, target = moves[0]
            self.assertEqual(source, task_path)
            self.assertEqual(target.parent.name, "ready_for_review")


if __name__ == "__main__":
    unittest.main()
