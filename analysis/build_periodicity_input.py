#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from _derived_builders import (
    archive_revision_for,
    attach_segments,
    compute_segments,
    infer_archive_name,
    load_freeze_frame,
    repo_root_from,
    write_parquet,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the canonical periodicity_input table.")
    parser.add_argument("--freeze-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser.parse_args()


def build_periodicity_input_frame(freeze_root: Path) -> pd.DataFrame:
    comments = load_freeze_frame(freeze_root, "comments")
    if comments.empty:
        raise SystemExit("periodicity_input requires comments.parquet")

    repo_root = repo_root_from(freeze_root)
    archive_name = infer_archive_name(freeze_root)
    archive_revision = archive_revision_for(repo_root, archive_name)

    comments = comments.copy()
    comments["created_at_utc"] = pd.to_datetime(comments["created_at_utc"], errors="coerce", utc=True)
    comment_end = comments["created_at_utc"].dropna().max()
    if pd.isna(comment_end):
        raise SystemExit("periodicity_input requires at least one parseable comment timestamp")
    segments = compute_segments(comments, comment_end)
    comments = attach_segments(comments, segments)

    segments = segments.copy()
    segments["duration_seconds"] = (
        pd.to_datetime(segments["segment_end_utc"], errors="coerce", utc=True)
        - pd.to_datetime(segments["segment_start_utc"], errors="coerce", utc=True)
    ).dt.total_seconds()
    segment_sizes = comments.groupby("segment_id").size().rename("n_events").reset_index()
    segments = segments.merge(segment_sizes, on="segment_id", how="left")
    segments["n_events"] = segments["n_events"].fillna(0).astype(int)
    segments.sort_values(
        ["duration_seconds", "n_events", "segment_id"],
        ascending=[False, False, True],
        inplace=True,
        kind="stable",
    )
    longest_segment_id = str(segments.iloc[0]["segment_id"])
    subset = comments.loc[comments["segment_id"].astype("string") == longest_segment_id].copy()
    subset.sort_values(["created_at_utc", "comment_id"], inplace=True, kind="stable")
    window_start = subset["created_at_utc"].min()
    window_end = pd.to_datetime(segments.iloc[0]["segment_end_utc"], errors="coerce", utc=True)

    seconds_since_epoch = subset["created_at_utc"].astype("int64") // 10**9
    seconds_since_start = (subset["created_at_utc"] - window_start).dt.total_seconds()

    frame = pd.DataFrame(
        {
            "archive_name": archive_name,
            "archive_revision": archive_revision,
            "segment_id": subset["segment_id"].astype("string"),
            "comment_id": subset["comment_id"].astype("string"),
            "created_at_utc": subset["created_at_utc"],
            "phase_mod_4h_seconds": seconds_since_epoch % (4 * 3600),
            "bin_5m": (seconds_since_start // 300).astype(int),
            "bin_15m": (seconds_since_start // 900).astype(int),
            "bin_30m": (seconds_since_start // 1800).astype(int),
            "window_start_utc": window_start,
            "window_end_utc": window_end,
        }
    )
    return frame


def main() -> None:
    args = parse_args()
    frame = build_periodicity_input_frame(args.freeze_root)
    write_parquet(args.out, frame)


if __name__ == "__main__":
    main()
