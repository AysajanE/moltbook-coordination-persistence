from __future__ import annotations

from pathlib import Path
import unittest

from runtime_test_utils import load_swarm_module


REPO_ROOT = Path(__file__).resolve().parents[1]
swarm = load_swarm_module()


class RepoTaskContractTest(unittest.TestCase):
    def test_manuscript_task_declares_latexmk_when_validation_uses_it(self) -> None:
        task_path = REPO_ROOT / ".orchestrator" / "backlog" / "T075_integrate_fresh_evidence_into_latex_paper.md"
        contract = swarm.load_framework_contract(REPO_ROOT)
        task = swarm.load_task(task_path, contract)
        task_text = task_path.read_text(encoding="utf-8")

        self.assertIn("latexmk -pdf -interaction=nonstopmode paper/main.tex", task_text)
        self.assertIn("latexmk", task.requires_tools)

    def test_release_layer_task_writes_json_to_json_artifact(self) -> None:
        task_path = REPO_ROOT / ".orchestrator" / "backlog" / "T080_design_release_layer.md"
        contract = swarm.load_framework_contract(REPO_ROOT)
        task = swarm.load_task(task_path, contract)
        task_text = task_path.read_text(encoding="utf-8")

        self.assertEqual(task.outputs, ["reports/status/release_layer_design.json"])
        self.assertIn("reports/status/release_layer_design.json", task.allowed_paths)
        self.assertIn("python scripts/quality_gates.py --json > reports/status/release_layer_design.json", task_text)
        self.assertNotIn("reports/status/release_layer_design.md", task_text)


if __name__ == "__main__":
    unittest.main()
