#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _derived_builders import (
    attach_segments,
    archive_revision_for,
    claimed_status_group,
    compute_depths,
    compute_segments,
    deterministic_hash,
    infer_archive_name,
    load_freeze_frame,
    repo_root_from,
    resolve_admin_end,
    topic_mapping,
    write_parquet,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the canonical parent_units table.")
    parser.add_argument("--freeze-root", type=Path, required=True)
    parser.add_argument("--qc-report", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser.parse_args()


def build_parent_units_frame(freeze_root: Path, qc_report: Path) -> pd.DataFrame:
    comments = load_freeze_frame(freeze_root, "comments")
    posts = load_freeze_frame(freeze_root, "posts")
    agents = load_freeze_frame(freeze_root, "agents")
    submolts = load_freeze_frame(freeze_root, "submolts")
    snapshots = load_freeze_frame(freeze_root, "snapshots")
    word_frequency = load_freeze_frame(freeze_root, "word_frequency")

    if comments.empty:
        raise SystemExit(f"Missing freeze comments table: {freeze_root / 'comments.parquet'}")
    if posts.empty:
        raise SystemExit(f"Missing freeze posts table: {freeze_root / 'posts.parquet'}")
    if not qc_report.exists():
        raise SystemExit(f"Missing QC report: {qc_report}")

    repo_root = repo_root_from(freeze_root)
    archive_name = infer_archive_name(freeze_root)
    archive_revision = archive_revision_for(repo_root, archive_name)

    comments = comments.copy()
    posts = posts.copy()
    agents = agents.copy()
    comments["created_at_utc"] = pd.to_datetime(comments["created_at_utc"], errors="coerce", utc=True)
    posts["post_created_at_utc"] = pd.to_datetime(posts["post_created_at_utc"], errors="coerce", utc=True)

    admin_end = resolve_admin_end(
        comments=comments,
        posts=posts,
        snapshots=snapshots,
        word_frequency=word_frequency,
    )
    segments = compute_segments(comments, admin_end)
    comments = attach_segments(comments, segments)

    depths = compute_depths(comments)
    comments = comments.merge(depths, on=["thread_id", "comment_id"], how="left")

    post_lookup = posts.loc[:, ["thread_id", "post_author_id", "community_label"]].copy()
    comments = comments.merge(post_lookup, on="thread_id", how="left", validate="many_to_one")

    claim_columns = [column for column in ["author_id", "claimed_status_raw"] if column in agents.columns]
    if "author_id" in claim_columns:
        claim_lookup = agents.loc[:, claim_columns].drop_duplicates("author_id", keep="last").copy()
    else:
        claim_lookup = pd.DataFrame(columns=["author_id", "claimed_status_raw"])
    comments = comments.merge(claim_lookup, on="author_id", how="left", validate="many_to_one")
    comments["claimed_status_group"] = comments["claimed_status_raw"].map(claimed_status_group)

    topic_map = topic_mapping(posts, submolts)
    comments["topic_category"] = comments["community_label"].map(topic_map).fillna("Unknown")

    children = comments.loc[
        comments["parent_comment_id"].notna(),
        ["thread_id", "comment_id", "parent_comment_id", "created_at_utc"],
    ].copy()
    children.rename(
        columns={
            "comment_id": "first_child_comment_id",
            "created_at_utc": "first_child_created_at_utc",
        },
        inplace=True,
    )
    children.sort_values(
        ["thread_id", "parent_comment_id", "first_child_created_at_utc", "first_child_comment_id"],
        inplace=True,
        kind="stable",
    )
    first_children = children.drop_duplicates(["thread_id", "parent_comment_id"], keep="first")

    parents = comments.copy()
    parents = parents.merge(
        first_children,
        left_on=["thread_id", "comment_id"],
        right_on=["thread_id", "parent_comment_id"],
        how="left",
        suffixes=("", "_child_match"),
    )
    if "parent_comment_id_child_match" in parents.columns:
        parents.drop(columns=["parent_comment_id_child_match"], inplace=True)

    parents["T_seconds"] = (
        parents["first_child_created_at_utc"] - parents["created_at_utc"]
    ).dt.total_seconds()
    parents["C_seconds"] = (
        pd.to_datetime(parents["segment_end_utc"], errors="coerce", utc=True) - parents["created_at_utc"]
    ).dt.total_seconds()
    parents["delta"] = (
        parents["T_seconds"].notna() & parents["C_seconds"].notna() & (parents["T_seconds"] <= parents["C_seconds"])
    ).astype(int)

    for seconds, suffix in [(30, "30s"), (300, "5m"), (3600, "1h")]:
        parents[f"R_{suffix}"] = (
            (parents["C_seconds"] >= seconds) | ((parents["delta"] == 1) & (parents["T_seconds"] <= seconds))
        ).astype(int)
        parents[f"Y_{suffix}"] = (
            (parents["delta"] == 1) & (parents["T_seconds"] <= seconds)
        ).astype(int)

    final_segment_id = segments.sort_values("segment_start_utc").iloc[-1]["segment_id"]
    parents["gap_overlap_6h_flag"] = (
        (parents["segment_id"] != final_segment_id) & (parents["C_seconds"] < 6 * 3600)
    ).astype(int)
    parents["gap_overlap_24h_flag"] = (
        (parents["segment_id"] != final_segment_id) & (parents["C_seconds"] < 24 * 3600)
    ).astype(int)

    parents["author_id_hash"] = parents["author_id"].map(deterministic_hash)
    parents["post_author_id_hash"] = parents["post_author_id"].map(deterministic_hash)
    parents["community_label"] = parents["community_label"].fillna("__UNKNOWN__")

    quality_flags = []
    for row in parents.itertuples(index=False):
        flags: list[str] = []
        if pd.isna(row.author_id):
            flags.append("missing_author")
        if str(row.community_label) == "__UNKNOWN__":
            flags.append("unresolved_community")
        if pd.isna(row.depth_from_root):
            flags.append("depth_unresolved")
        quality_flags.append(";".join(flags) if flags else "ok")
    parents["quality_flag"] = quality_flags

    source_snapshot = parents.get("source_snapshot_id")
    if source_snapshot is None:
        parents["source_snapshot_id"] = archive_revision
    else:
        normalized_snapshot_ids = source_snapshot.astype("string")
        parents["source_snapshot_id"] = normalized_snapshot_ids.fillna(archive_revision)

    frame = pd.DataFrame(
        {
            "archive_name": archive_name,
            "archive_revision": archive_revision,
            "source_snapshot_id": parents["source_snapshot_id"].astype("string"),
            "thread_id": parents["thread_id"].astype("string"),
            "comment_id": parents["comment_id"].astype("string"),
            "parent_comment_id": parents["parent_comment_id"].astype("string"),
            "author_id_hash": parents["author_id_hash"].astype("string"),
            "post_author_id_hash": parents["post_author_id_hash"].astype("string"),
            "community_label": parents["community_label"].astype("string"),
            "claimed_status_group": parents["claimed_status_group"].astype("string"),
            "topic_category": parents["topic_category"].astype("string"),
            "created_at_utc": parents["created_at_utc"],
            "segment_id": parents["segment_id"].astype("string"),
            "segment_end_utc": pd.to_datetime(parents["segment_end_utc"], errors="coerce", utc=True),
            "first_child_comment_id": parents["first_child_comment_id"].astype("string"),
            "first_child_created_at_utc": pd.to_datetime(
                parents["first_child_created_at_utc"], errors="coerce", utc=True
            ),
            "T_seconds": pd.to_numeric(parents["T_seconds"], errors="coerce"),
            "C_seconds": pd.to_numeric(parents["C_seconds"], errors="coerce"),
            "delta": parents["delta"].astype(int),
            "R_30s": parents["R_30s"].astype(int),
            "Y_30s": parents["Y_30s"].astype(int),
            "R_5m": parents["R_5m"].astype(int),
            "Y_5m": parents["Y_5m"].astype(int),
            "R_1h": parents["R_1h"].astype(int),
            "Y_1h": parents["Y_1h"].astype(int),
            "depth_from_root": pd.to_numeric(parents["depth_from_root"], errors="coerce"),
            "raw_depth": pd.to_numeric(parents["raw_depth"], errors="coerce"),
            "gap_overlap_6h_flag": parents["gap_overlap_6h_flag"].astype(int),
            "gap_overlap_24h_flag": parents["gap_overlap_24h_flag"].astype(int),
            "quality_flag": parents["quality_flag"].astype("string"),
        }
    )
    frame.sort_values(["created_at_utc", "comment_id"], inplace=True, kind="stable")
    return frame


def main() -> None:
    args = parse_args()
    frame = build_parent_units_frame(args.freeze_root, args.qc_report)
    write_parquet(args.out, frame)


if __name__ == "__main__":
    main()
