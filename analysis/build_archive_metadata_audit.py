#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd

from _derived_builders import (
    build_topic_dictionary_frame,
    duplicate_count_by_table,
    infer_archive_name,
    load_freeze_frame,
    load_json_document,
    min_max_time,
    parse_linkage_rates,
    severe_gap_count,
    timestamp_parse_success_rate,
    total_rows_by_table,
    write_parquet,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the canonical archive_metadata_audit table.")
    parser.add_argument("--raw-manifest", type=Path, required=True)
    parser.add_argument("--freeze-manifest", type=Path, required=True)
    parser.add_argument("--qc-report", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--topic-dictionary-out", type=Path, required=True)
    return parser.parse_args()


def build_archive_metadata_audit_frame(
    *,
    raw_manifest_path: Path,
    freeze_manifest_path: Path,
    qc_report_path: Path,
    topic_dictionary_out: Path,
) -> pd.DataFrame:
    raw_manifest = load_json_document(raw_manifest_path)
    freeze_manifest = load_json_document(freeze_manifest_path)
    archive_name = str(raw_manifest.get("archive_name") or infer_archive_name(raw_manifest_path))
    archive_revision = str(raw_manifest.get("resolved_revision") or raw_manifest.get("requested_revision") or "unknown")
    freeze_root = Path(str(freeze_manifest["freeze_root"]))

    posts = load_freeze_frame(freeze_root, "posts")
    comments = load_freeze_frame(freeze_root, "comments")
    snapshots = load_freeze_frame(freeze_root, "snapshots")
    word_frequency = load_freeze_frame(freeze_root, "word_frequency")
    submolts = load_freeze_frame(freeze_root, "submolts")

    topic_dictionary = build_topic_dictionary_frame(posts, submolts)
    topic_dictionary_out.parent.mkdir(parents=True, exist_ok=True)
    topic_dictionary.to_csv(topic_dictionary_out, index=False)

    linkage_path = qc_report_path.parent / f"linkage_audit_{archive_name}.csv"
    gap_registry_path = qc_report_path.parent / f"gap_registry_{archive_name}.csv"
    linkage_rates = parse_linkage_rates(linkage_path)

    time_start, time_end = min_max_time(
        [
            comments.get("created_at_utc", pd.Series(dtype="datetime64[ns, UTC]")),
            posts.get("post_created_at_utc", pd.Series(dtype="datetime64[ns, UTC]")),
            snapshots.get("snapshot_timestamp_utc", pd.Series(dtype="datetime64[ns, UTC]")),
            word_frequency.get("hour_utc", pd.Series(dtype="datetime64[ns, UTC]")),
        ]
    )
    missing_author_rate = (
        float(comments["author_id"].isna().mean()) if not comments.empty and "author_id" in comments.columns else None
    )
    unresolved_community_rate = (
        float(posts["community_label"].isna().mean()) if not posts.empty and "community_label" in posts.columns else None
    )

    frame = pd.DataFrame(
        [
            {
                "archive_name": archive_name,
                "archive_revision": archive_revision,
                "release_tag": str(raw_manifest.get("requested_revision") or raw_manifest.get("snapshot_id") or "unknown"),
                "acquisition_utc": raw_manifest.get("exported_at_utc"),
                "license": raw_manifest.get("license"),
                "total_rows_by_table_json": json.dumps(total_rows_by_table(raw_manifest), sort_keys=True),
                "time_span_start_utc": None if time_start is None else time_start.isoformat(),
                "time_span_end_utc": None if time_end is None else time_end.isoformat(),
                "duplicate_count_by_table_json": json.dumps(duplicate_count_by_table(raw_manifest), sort_keys=True),
                "timestamp_parse_success_rate": timestamp_parse_success_rate(raw_manifest),
                "post_link_success_rate": linkage_rates["post_link_success_rate"],
                "parent_link_success_rate": linkage_rates["parent_link_success_rate"],
                "missing_author_rate": missing_author_rate,
                "unresolved_community_rate": unresolved_community_rate,
                "severe_gap_count": severe_gap_count(gap_registry_path),
                "notes": f"freeze_manifest={freeze_manifest_path}; qc_report={qc_report_path}; topic_dictionary={topic_dictionary_out}",
            }
        ]
    )
    return frame


def main() -> None:
    args = parse_args()
    frame = build_archive_metadata_audit_frame(
        raw_manifest_path=args.raw_manifest,
        freeze_manifest_path=args.freeze_manifest,
        qc_report_path=args.qc_report,
        topic_dictionary_out=args.topic_dictionary_out,
    )
    write_parquet(args.out, frame)


if __name__ == "__main__":
    main()
