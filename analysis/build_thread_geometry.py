#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from _derived_builders import (
    archive_revision_for,
    compute_depths,
    deterministic_hash,
    infer_archive_name,
    load_freeze_frame,
    repo_root_from,
    write_parquet,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the canonical thread_geometry table.")
    parser.add_argument("--freeze-root", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser.parse_args()


def build_thread_geometry_frame(freeze_root: Path) -> pd.DataFrame:
    comments = load_freeze_frame(freeze_root, "comments")
    posts = load_freeze_frame(freeze_root, "posts")
    if comments.empty or posts.empty:
        raise SystemExit("thread_geometry requires comments.parquet and posts.parquet")

    repo_root = repo_root_from(freeze_root)
    archive_name = infer_archive_name(freeze_root)
    archive_revision = archive_revision_for(repo_root, archive_name)

    comments = comments.copy()
    comments["created_at_utc"] = pd.to_datetime(comments["created_at_utc"], errors="coerce", utc=True)
    depths = compute_depths(comments)
    comments = comments.merge(depths, on=["thread_id", "comment_id"], how="left")

    post_lookup = posts.loc[:, ["thread_id", "community_label", "post_created_at_utc", "post_author_id"]].copy()
    comments = comments.merge(post_lookup, on="thread_id", how="left", validate="many_to_one")

    child_counts = (
        comments.loc[comments["parent_comment_id"].notna()]
        .groupby(["thread_id", "parent_comment_id"])
        .size()
        .rename("direct_child_count")
        .reset_index()
    )
    comment_counts = comments.loc[:, ["thread_id", "comment_id"]].merge(
        child_counts,
        left_on=["thread_id", "comment_id"],
        right_on=["thread_id", "parent_comment_id"],
        how="left",
    )
    comment_counts["direct_child_count"] = comment_counts["direct_child_count"].fillna(0)

    rows: list[dict[str, Any]] = []
    for thread_id, subset in comments.groupby("thread_id", dropna=False, sort=False):
        subset = subset.sort_values(["created_at_utc", "comment_id"], kind="stable").copy()
        direct_counts = comment_counts.loc[comment_counts["thread_id"] == thread_id, ["comment_id", "direct_child_count"]]
        direct_count_map = {
            str(row.comment_id): float(row.direct_child_count)
            for row in direct_counts.itertuples(index=False)
        }
        root_direct_child_count = int(subset["parent_comment_id"].isna().sum())
        nonroot_values = [count for count in direct_count_map.values()]
        nonroot_branching_mean = float(np.mean(nonroot_values)) if nonroot_values else np.nan

        branching_by_depth: dict[str, float] = {}
        for depth, depth_subset in subset.groupby("depth_from_root", dropna=False):
            comment_ids = depth_subset["comment_id"].astype("string").tolist()
            counts = [direct_count_map.get(str(comment_id), 0.0) for comment_id in comment_ids]
            key = "unknown" if pd.isna(depth) else str(int(depth))
            branching_by_depth[key] = float(np.mean(counts)) if counts else 0.0
        branching_by_depth["0"] = float(root_direct_child_count)

        parent_author = subset.loc[:, ["comment_id", "author_id"]].rename(
            columns={"comment_id": "parent_comment_id", "author_id": "parent_author_id"}
        )
        edges = subset.loc[subset["parent_comment_id"].notna(), ["comment_id", "parent_comment_id", "author_id"]].merge(
            parent_author,
            on="parent_comment_id",
            how="left",
        )
        top_level = subset.loc[subset["parent_comment_id"].isna(), ["comment_id", "author_id"]].copy()
        root_author = subset["post_author_id"].iloc[0] if "post_author_id" in subset.columns else None
        top_level["parent_author_id"] = root_author
        top_level["parent_comment_id"] = pd.NA
        edges = pd.concat([edges, top_level], ignore_index=True, sort=False)
        edges = edges.loc[
            edges["author_id"].notna()
            & edges["parent_author_id"].notna()
            & (edges["author_id"].astype("string") != edges["parent_author_id"].astype("string"))
        ].copy()
        directed = {
            (str(row.author_id), str(row.parent_author_id))
            for row in edges.loc[:, ["author_id", "parent_author_id"]].itertuples(index=False)
        }
        dyads = {tuple(sorted(pair)) for pair in directed}
        reciprocity_rate = (
            float(
                np.mean(
                    [
                        ((a, b) in directed) and ((b, a) in directed)
                        for a, b in dyads
                    ]
                )
            )
            if dyads
            else np.nan
        )

        authors = subset["author_id"].astype("string")
        seen_paper: set[str] = set()
        reentry_flags_paper: list[bool] = []
        for author in authors:
            if author == "<NA>":
                reentry_flags_paper.append(False)
                continue
            reentry_flags_paper.append(author in seen_paper)
            seen_paper.add(author)
        reentry_rate_paper = float(np.mean(reentry_flags_paper)) if reentry_flags_paper else np.nan

        resolved = subset.loc[subset["author_id"].notna(), ["comment_id", "author_id"]].copy()
        seen_resolved: set[str] = set()
        reentry_flags_resolved: list[bool] = []
        for author in resolved["author_id"].astype("string"):
            reentry_flags_resolved.append(author in seen_resolved)
            seen_resolved.add(author)
        reentry_rate_resolved = (
            float(np.mean(reentry_flags_resolved)) if reentry_flags_resolved else np.nan
        )

        missing_author_count = int(subset["author_id"].isna().sum())
        rows.append(
            {
                "archive_name": archive_name,
                "archive_revision": archive_revision,
                "thread_id": thread_id,
                "community_label": subset["community_label"].iloc[0] if "community_label" in subset.columns else "__UNKNOWN__",
                "root_post_created_at_utc": pd.to_datetime(
                    subset["post_created_at_utc"].iloc[0], errors="coerce", utc=True
                ),
                "post_author_id_hash": deterministic_hash(root_author),
                "n_comments": int(len(subset)),
                "max_depth": int(subset["depth_from_root"].max()) if subset["depth_from_root"].notna().any() else 0,
                "root_direct_child_count": root_direct_child_count,
                "nonroot_branching_mean": nonroot_branching_mean,
                "branching_by_depth_json": json.dumps(branching_by_depth, sort_keys=True),
                "reciprocity_rate": reciprocity_rate,
                "reentry_rate_paper": reentry_rate_paper,
                "reentry_rate_resolvedauthors": reentry_rate_resolved,
                "resolved_author_comment_count": int(subset["author_id"].notna().sum()),
                "missing_author_comment_count": missing_author_count,
                "quality_flag": "missing_author_present" if missing_author_count else "ok",
            }
        )

    return pd.DataFrame(rows)


def main() -> None:
    args = parse_args()
    frame = build_thread_geometry_frame(args.freeze_root)
    write_parquet(args.out, frame)


if __name__ == "__main__":
    main()
