from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]


def write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_parquet(path, index=False)


def write_json_as_yaml(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class W3BuilderTests(unittest.TestCase):
    maxDiff = None

    def run_script(self, *args: str) -> None:
        result = subprocess.run(
            [sys.executable, *args],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            self.fail(
                f"command failed: {' '.join(args)}\nstdout:\n{result.stdout}\nstderr:\n{result.stderr}"
            )

    def build_fixture(self, root: Path, archive_name: str, include_source_snapshot_id: bool) -> dict[str, Path]:
        freeze_dir_name = "simulamet_firstweek_lateststate" if archive_name == "simulamet" else "moltnet_firstweek_aligned"
        freeze_root = root / "frozen" / freeze_dir_name
        raw_root = root / "raw" / archive_name / "snapshot_001"
        qc_root = root / "qc"
        manifests_root = root / "manifests"

        comments = [
            {
                "thread_id": "thread1",
                "comment_id": "c1",
                "parent_comment_id": None,
                "author_id": "a1",
                "created_at_utc": "2026-01-01T00:00:00Z",
                "depth": 0,
                "source_snapshot_id": "2026-01-02",
            },
            {
                "thread_id": "thread1",
                "comment_id": "c2",
                "parent_comment_id": "c1",
                "author_id": "a2",
                "created_at_utc": "2026-01-01T00:00:05Z",
                "depth": 1,
                "source_snapshot_id": "2026-01-02",
            },
            {
                "thread_id": "thread1",
                "comment_id": "c2b",
                "parent_comment_id": "c1",
                "author_id": "a3",
                "created_at_utc": "2026-01-01T00:00:05Z",
                "depth": 1,
                "source_snapshot_id": "2026-01-02",
            },
            {
                "thread_id": "thread1",
                "comment_id": "c3",
                "parent_comment_id": None,
                "author_id": "a1",
                "created_at_utc": "2026-01-01T00:02:00Z",
                "depth": 0,
                "source_snapshot_id": "2026-01-02",
            },
            {
                "thread_id": "thread1",
                "comment_id": "c4",
                "parent_comment_id": "c3",
                "author_id": "a2",
                "created_at_utc": "2026-01-01T00:02:20Z",
                "depth": 1,
                "source_snapshot_id": "2026-01-02",
            },
            {
                "thread_id": "thread1",
                "comment_id": "c5",
                "parent_comment_id": None,
                "author_id": "a2",
                "created_at_utc": "2026-01-01T00:10:00Z",
                "depth": 0,
                "source_snapshot_id": "2026-01-02",
            },
            {
                "thread_id": "thread1",
                "comment_id": "c6",
                "parent_comment_id": None,
                "author_id": "a1",
                "created_at_utc": "2026-01-01T08:00:00Z",
                "depth": 0,
                "source_snapshot_id": "2026-01-03",
            },
            {
                "thread_id": "thread1",
                "comment_id": "c7",
                "parent_comment_id": "c6",
                "author_id": "a2",
                "created_at_utc": "2026-01-01T09:00:00Z",
                "depth": 1,
                "source_snapshot_id": "2026-01-03",
            },
            {
                "thread_id": "thread1",
                "comment_id": "c8",
                "parent_comment_id": "c7",
                "author_id": "a1",
                "created_at_utc": "2026-01-01T10:00:00Z",
                "depth": 2,
                "source_snapshot_id": "2026-01-03",
            },
        ]
        if not include_source_snapshot_id:
            for row in comments:
                row.pop("source_snapshot_id", None)

        posts = [
            {
                "thread_id": "thread1",
                "post_author_id": "root1",
                "community_label": "builder-lab",
                "post_created_at_utc": "2025-12-31T23:00:00Z",
                "source_snapshot_id": "2026-01-03",
            },
            {
                "thread_id": "thread2",
                "post_author_id": "root2",
                "community_label": "creative-studio",
                "post_created_at_utc": "2025-12-31T22:00:00Z",
                "source_snapshot_id": "2026-01-03",
            },
        ]
        agents = [
            {"author_id": "a1", "claimed_status_raw": 1, "source_snapshot_id": "2026-01-03"},
            {"author_id": "a2", "claimed_status_raw": 0, "source_snapshot_id": "2026-01-03"},
            {"author_id": "a3", "claimed_status_raw": None, "source_snapshot_id": "2026-01-03"},
            {"author_id": "root1", "claimed_status_raw": 1, "source_snapshot_id": "2026-01-03"},
            {"author_id": "root2", "claimed_status_raw": 0, "source_snapshot_id": "2026-01-03"},
        ]
        submolts = [
            {
                "community_label": "builder-lab",
                "community_description": "Builder technical coding forum",
                "source_snapshot_id": "2026-01-03",
            },
            {
                "community_label": "creative-studio",
                "community_description": "Creative art and design",
                "source_snapshot_id": "2026-01-03",
            },
        ]
        snapshots = [
            {
                "snapshot_timestamp_utc": "2026-01-01T12:00:00Z",
                "source_snapshot_id": "2026-01-03",
            }
        ]
        word_frequency = [
            {
                "word": "builder",
                "hour_utc": "2026-01-01T12:00:00Z",
                "source_snapshot_id": "2026-01-03",
            }
        ]

        write_parquet(freeze_root / "comments.parquet", comments)
        write_parquet(freeze_root / "posts.parquet", posts)
        write_parquet(freeze_root / "agents.parquet", agents)
        write_parquet(freeze_root / "submolts.parquet", submolts)
        write_parquet(freeze_root / "snapshots.parquet", snapshots)
        write_parquet(freeze_root / "word_frequency.parquet", word_frequency)

        raw_comments = comments + [dict(comments[0])]
        if not include_source_snapshot_id:
            for row in raw_comments:
                row["source_snapshot_id"] = "2026-01-03"
        raw_posts = posts
        raw_agents = agents
        raw_submolts = submolts
        raw_snapshots = snapshots
        raw_word_frequency = word_frequency

        raw_paths = {
            "comments": raw_root / "comments.parquet",
            "posts": raw_root / "posts.parquet",
            "agents": raw_root / "agents.parquet",
            "submolts": raw_root / "submolts.parquet",
            "snapshots": raw_root / "snapshots.parquet",
            "word_frequency": raw_root / "word_frequency.parquet",
        }
        write_parquet(raw_paths["comments"], raw_comments)
        write_parquet(raw_paths["posts"], raw_posts)
        write_parquet(raw_paths["agents"], raw_agents)
        write_parquet(raw_paths["submolts"], raw_submolts)
        write_parquet(raw_paths["snapshots"], raw_snapshots)
        write_parquet(raw_paths["word_frequency"], raw_word_frequency)

        raw_manifest = {
            "archive_name": archive_name,
            "requested_revision": f"{archive_name}-r1",
            "resolved_revision": f"{archive_name}-r1",
            "license": "cc-by-4.0",
            "exported_at_utc": "2026-04-08T00:00:00+00:00",
            "subsets": {
                subset: {
                    "rows_exported_total": len(rows),
                    "splits": {"train": {"path": str(raw_paths[subset])}},
                }
                for subset, rows in {
                    "comments": raw_comments,
                    "posts": raw_posts,
                    "agents": raw_agents,
                    "submolts": raw_submolts,
                    "snapshots": raw_snapshots,
                    "word_frequency": raw_word_frequency,
                }.items()
            },
        }
        raw_manifest_path = manifests_root / f"{archive_name}_manifest.yaml"
        write_json_as_yaml(raw_manifest_path, raw_manifest)

        freeze_manifest_path = manifests_root / f"{archive_name}_freeze_manifest.json"
        freeze_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        freeze_manifest_path.write_text(
            json.dumps({"freeze_root": str(freeze_root)}, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        qc_report_path = qc_root / f"archive_qc_report_{archive_name}.md"
        qc_report_path.parent.mkdir(parents=True, exist_ok=True)
        qc_report_path.write_text("# QC\n", encoding="utf-8")

        pd.DataFrame(
            [
                {
                    "check_name": "comments_thread_id_resolves_to_posts",
                    "resolution_rate": 1.0,
                },
                {
                    "check_name": "parent_comment_resolves_same_thread",
                    "resolution_rate": 0.99,
                },
            ]
        ).to_csv(qc_root / f"linkage_audit_{archive_name}.csv", index=False)
        pd.DataFrame([{"severity": "severe"}, {"severity": "minor"}]).to_csv(
            qc_root / f"gap_registry_{archive_name}.csv",
            index=False,
        )

        return {
            "freeze_root": freeze_root,
            "qc_report_path": qc_report_path,
            "raw_manifest_path": raw_manifest_path,
            "freeze_manifest_path": freeze_manifest_path,
            "derived_root": root / "derived",
        }

    def test_simulamet_w3_builder_commands(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture = self.build_fixture(Path(tmpdir), archive_name="simulamet", include_source_snapshot_id=True)
            parent_units_path = fixture["derived_root"] / "parent_units_simulamet.parquet"
            control_panel_path = fixture["derived_root"] / "control_panel_summary_simulamet.parquet"
            thread_geometry_path = fixture["derived_root"] / "thread_geometry_simulamet.parquet"
            periodicity_path = fixture["derived_root"] / "periodicity_input_simulamet.parquet"
            audit_path = fixture["derived_root"] / "archive_metadata_audit.parquet"
            dictionary_path = fixture["derived_root"] / "submolt_category_dictionary_v1.csv"

            self.run_script(
                "analysis/build_parent_units.py",
                "--freeze-root",
                str(fixture["freeze_root"]),
                "--qc-report",
                str(fixture["qc_report_path"]),
                "--out",
                str(parent_units_path),
            )
            self.run_script(
                "analysis/build_control_panel_summary.py",
                "--parent-units",
                str(parent_units_path),
                "--out",
                str(control_panel_path),
            )
            self.run_script(
                "analysis/build_thread_geometry.py",
                "--freeze-root",
                str(fixture["freeze_root"]),
                "--out",
                str(thread_geometry_path),
            )
            self.run_script(
                "analysis/build_periodicity_input.py",
                "--freeze-root",
                str(fixture["freeze_root"]),
                "--out",
                str(periodicity_path),
            )
            self.run_script(
                "analysis/build_archive_metadata_audit.py",
                "--raw-manifest",
                str(fixture["raw_manifest_path"]),
                "--freeze-manifest",
                str(fixture["freeze_manifest_path"]),
                "--qc-report",
                str(fixture["qc_report_path"]),
                "--out",
                str(audit_path),
                "--topic-dictionary-out",
                str(dictionary_path),
            )

            parent_units = pd.read_parquet(parent_units_path)
            self.assertEqual(len(parent_units), 9)
            self.assertEqual(parent_units.loc[parent_units["comment_id"] == "c1", "first_child_comment_id"].iloc[0], "c2")
            self.assertEqual(parent_units.loc[parent_units["comment_id"] == "c5", "gap_overlap_6h_flag"].iloc[0], 1)
            self.assertEqual(parent_units.loc[parent_units["comment_id"] == "c5", "gap_overlap_24h_flag"].iloc[0], 1)
            self.assertEqual(parent_units.loc[parent_units["comment_id"] == "c7", "Y_5m"].iloc[0], 0)
            self.assertEqual(parent_units.loc[parent_units["comment_id"] == "c7", "Y_1h"].iloc[0], 1)
            self.assertTrue(parent_units["source_snapshot_id"].notna().all())
            self.assertEqual(
                parent_units.loc[parent_units["comment_id"] == "c2b", "claimed_status_group"].iloc[0],
                "unknown",
            )
            self.assertEqual(
                parent_units.loc[parent_units["comment_id"] == "c1", "topic_category"].iloc[0],
                "BuilderTechnical",
            )

            control_panel = pd.read_parquet(control_panel_path)
            overall_5m = control_panel.loc[
                (control_panel["window_variant"] == "full_window")
                & (control_panel["horizon_label"] == "5m")
                & (control_panel["stratum_family"] == "overall")
            ].iloc[0]
            self.assertEqual(int(overall_5m["n_parents"]), 9)
            self.assertAlmostEqual(float(overall_5m["q_h"]), float(overall_5m["pi_h"]) * float(overall_5m["phi_h"]))
            self.assertTrue(bool(overall_5m["benchmark_flag"]))

            thread_geometry = pd.read_parquet(thread_geometry_path)
            geometry = thread_geometry.loc[thread_geometry["thread_id"] == "thread1"].iloc[0]
            self.assertEqual(int(geometry["max_depth"]), 3)
            self.assertEqual(int(geometry["root_direct_child_count"]), 4)
            self.assertAlmostEqual(float(geometry["reciprocity_rate"]), 0.25)
            self.assertAlmostEqual(float(geometry["reentry_rate_paper"]), 6.0 / 9.0)
            self.assertEqual(str(geometry["quality_flag"]), "ok")

            periodicity = pd.read_parquet(periodicity_path)
            self.assertEqual(set(periodicity["comment_id"].astype(str)), {"c6", "c7", "c8"})
            self.assertTrue((periodicity["window_end_utc"] == pd.Timestamp("2026-01-01T10:00:00Z")).all())

            audit = pd.read_parquet(audit_path)
            audit_row = audit.iloc[0]
            self.assertEqual(str(audit_row["archive_name"]), "simulamet")
            self.assertEqual(int(audit_row["severe_gap_count"]), 1)
            self.assertAlmostEqual(float(audit_row["timestamp_parse_success_rate"]), 1.0)
            self.assertAlmostEqual(float(audit_row["parent_link_success_rate"]), 0.99)
            duplicate_counts = json.loads(str(audit_row["duplicate_count_by_table_json"]))
            self.assertEqual(int(duplicate_counts["comments"]), 1)

            dictionary = pd.read_csv(dictionary_path)
            self.assertEqual(
                set(dictionary["community_label"]),
                {"builder-lab", "creative-studio"},
            )

    def test_moltnet_wrapper_commands_emit_schema_compatible_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture = self.build_fixture(Path(tmpdir), archive_name="moltnet", include_source_snapshot_id=False)
            parent_units_path = fixture["derived_root"] / "parent_units_moltnet.parquet"
            control_panel_path = fixture["derived_root"] / "control_panel_summary_moltnet.parquet"

            self.run_script(
                "analysis/build_moltnet_parent_units.py",
                "--freeze-root",
                str(fixture["freeze_root"]),
                "--qc-report",
                str(fixture["qc_report_path"]),
                "--out",
                str(parent_units_path),
            )
            self.run_script(
                "analysis/build_moltnet_control_panel_summary.py",
                "--parent-units",
                str(parent_units_path),
                "--out",
                str(control_panel_path),
            )

            parent_units = pd.read_parquet(parent_units_path)
            control_panel = pd.read_parquet(control_panel_path)
            self.assertEqual(set(parent_units["archive_name"].astype(str)), {"moltnet"})
            self.assertTrue(parent_units["source_snapshot_id"].notna().all())
            self.assertEqual(set(control_panel["archive_name"].astype(str)), {"moltnet"})

    def test_t060_flagship_output_commands_write_markdown_report(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fixture = self.build_fixture(Path(tmpdir), archive_name="simulamet", include_source_snapshot_id=True)
            parent_units_path = fixture["derived_root"] / "parent_units_simulamet.parquet"
            control_panel_path = fixture["derived_root"] / "control_panel_summary_simulamet.parquet"
            thread_geometry_path = fixture["derived_root"] / "thread_geometry_simulamet.parquet"
            periodicity_path = fixture["derived_root"] / "periodicity_input_simulamet.parquet"
            report_path = Path(tmpdir) / "qc" / "analysis_execution_simulamet.md"

            self.run_script(
                "analysis/build_parent_units.py",
                "--freeze-root",
                str(fixture["freeze_root"]),
                "--qc-report",
                str(fixture["qc_report_path"]),
                "--out",
                str(parent_units_path),
            )
            self.run_script(
                "analysis/build_control_panel_summary.py",
                "--parent-units",
                str(parent_units_path),
                "--out",
                str(control_panel_path),
            )
            self.run_script(
                "analysis/build_thread_geometry.py",
                "--freeze-root",
                str(fixture["freeze_root"]),
                "--out",
                str(thread_geometry_path),
            )
            self.run_script(
                "analysis/build_periodicity_input.py",
                "--freeze-root",
                str(fixture["freeze_root"]),
                "--out",
                str(periodicity_path),
            )

            self.run_script(
                "analysis/flagship_control_panel_margins.py",
                "--control-panel",
                str(control_panel_path),
                "--out",
                str(report_path),
            )
            self.run_script(
                "analysis/incidence_horizon_standardization.py",
                "--parent-units",
                str(parent_units_path),
                "--append-report",
                str(report_path),
            )

            report_text = report_path.read_text(encoding="utf-8")
            self.assertIn("## H1-H2 Control Panel", report_text)
            self.assertIn("## P1-P2 Local Gain Readout", report_text)
            self.assertIn("## Horizon Standardization", report_text)
            self.assertIn("## H3 Geometry Diagnostic", report_text)
            self.assertIn("## H5 Periodicity Diagnostic", report_text)
            self.assertIn(str(control_panel_path), report_text)
            self.assertIn(str(parent_units_path), report_text)


if __name__ == "__main__":
    unittest.main()
