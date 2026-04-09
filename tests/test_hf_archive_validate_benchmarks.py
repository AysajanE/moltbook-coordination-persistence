from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import unittest


REPO_ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = REPO_ROOT / "analysis" / "hf_archive_validate.py"


def load_module():
    spec = importlib.util.spec_from_file_location("hf_archive_validate_test_module", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["hf_archive_validate_test_module"] = module
    spec.loader.exec_module(module)
    return module


hf_archive_validate = load_module()


class ArchiveBenchmarkConfigTest(unittest.TestCase):
    def test_simulamet_benchmarks_load_from_manifest(self) -> None:
        config_path = REPO_ROOT / "manifests" / "simulamet_qc_benchmarks.json"
        benchmarks, resolved_path = hf_archive_validate._load_benchmark_expectations(
            "simulamet",
            config_path,
        )

        self.assertFalse(hasattr(hf_archive_validate, "SIMULAMET_BENCHMARKS"))
        self.assertEqual(resolved_path, config_path)
        self.assertEqual(int(benchmarks["comments_rows"]), 226173)
        self.assertAlmostEqual(float(benchmarks["q_5m"]), 0.0942)


if __name__ == "__main__":
    unittest.main()
