from pathlib import Path
import tempfile
import unittest

from runtime_test_utils import chdir, load_quality_gates_module, scaffold_runtime_repo


quality_gates = load_quality_gates_module()


class GateRepoStructureRuntimeTest(unittest.TestCase):
    def test_min_runtime_repo_passes(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            with chdir(root):
                result = quality_gates.gate_repo_structure()

            self.assertTrue(result.ok, result.details)

    def test_missing_operator_prompt_and_schema_fail(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            scaffold_runtime_repo(root)

            (root / "docs" / "prompts" / "operator.md").unlink()
            (root / "contracts" / "schemas" / "parent_units_v1.yaml").unlink()

            with chdir(root):
                result = quality_gates.gate_repo_structure()

            self.assertFalse(result.ok)
            missing = set(result.details.get("missing") or [])
            self.assertIn("docs/prompts/operator.md", missing)
            self.assertIn("contracts/schemas/parent_units_v1.yaml", missing)


if __name__ == "__main__":
    unittest.main()
