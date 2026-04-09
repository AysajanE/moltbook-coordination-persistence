from __future__ import annotations

import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

import pandas as pd

from runtime_test_utils import REPO_ROOT


COLLECT_SCRIPT = REPO_ROOT / "analysis" / "moltbook_api_collect.py"
VALIDATE_SCRIPT = REPO_ROOT / "analysis" / "moltbook_api_validate.py"


def _write_partition(root: Path, subset: str, run_id: str, frame: pd.DataFrame) -> None:
    out_dir = root / subset / f"run_id={run_id}"
    out_dir.mkdir(parents=True, exist_ok=True)
    frame.to_parquet(out_dir / "part-0.parquet", index=False)


class MoltbookApiModeSafetyTest(unittest.TestCase):
    def test_collector_rejects_stub_mode_without_smoke_test(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            out_root = root / "raw" / "moltbook_api"
            result = subprocess.run(
                [
                    sys.executable,
                    str(COLLECT_SCRIPT),
                    "--attempt-id",
                    "run-1",
                    "--date",
                    "2026-04-09",
                    "--mode",
                    "stub",
                    "--sorts",
                    "hot",
                    "--limit",
                    "1",
                    "--snapshots",
                    "1",
                    "--out-raw-root",
                    str(out_root),
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertNotEqual(result.returncode, 0)

    def test_validator_fails_synthetic_data_without_allow_synthetic(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            curated_root = root / "frozen" / "moltbook_api"
            run_id = "run-1"

            _write_partition(
                curated_root,
                "feed_snapshots",
                run_id,
                pd.DataFrame(
                    [
                        {
                            "run_id": run_id,
                            "sort": "hot",
                            "limit": 1,
                            "rank": 1,
                            "post_id": "p1",
                            "snapshot_time_raw": "2026-04-09T00:00:00Z",
                            "snapshot_time_utc": "2026-04-09T00:00:00Z",
                        }
                    ]
                ),
            )
            _write_partition(
                curated_root,
                "posts",
                run_id,
                pd.DataFrame(
                    [
                        {
                            "run_id": run_id,
                            "post_id": "p1",
                            "created_at_raw": "2026-04-09T00:00:00Z",
                            "created_at_utc": "2026-04-09T00:00:00Z",
                        }
                    ]
                ),
            )
            _write_partition(
                curated_root,
                "comments",
                run_id,
                pd.DataFrame(
                    [
                        {
                            "run_id": run_id,
                            "post_id": "p1",
                            "comment_id": "c1",
                            "created_at_raw": "2026-04-09T00:01:00Z",
                            "created_at_utc": "2026-04-09T00:01:00Z",
                        }
                    ]
                ),
            )

            request_log = root / "request_log.jsonl"
            request_log.write_text(
                json.dumps(
                    {
                        "attempt_id": run_id,
                        "mode": "stub",
                        "synthetic": True,
                        "endpoint": "/posts",
                        "params": {"sort": "hot"},
                        "http_status": 200,
                        "retrieved_at_utc": "2026-04-09T00:00:00Z",
                    }
                )
                + "\n",
                encoding="utf-8",
            )
            out_path = root / "validation.json"

            result = subprocess.run(
                [
                    sys.executable,
                    str(VALIDATE_SCRIPT),
                    "--curated-root",
                    str(curated_root),
                    "--run-id",
                    run_id,
                    "--request-log",
                    str(request_log),
                    "--out",
                    str(out_path),
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            self.assertEqual(result.returncode, 0, result.stderr)
            payload = json.loads(out_path.read_text(encoding="utf-8"))
            self.assertEqual(
                payload["checks"]["synthetic_data_forbidden_in_production"]["status"],
                "FAIL",
            )
            self.assertEqual(payload["summary"]["status"], "FAIL")


if __name__ == "__main__":
    unittest.main()
