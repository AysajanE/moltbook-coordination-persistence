#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

GAP_LOG_HOURS = 6.0
SEVERE_GAP_HOURS = 24.0
SIMULAMET_BENCHMARKS = {
    "comments_rows": 226_173,
    "unique_comment_ids": 223_317,
    "candidate_parent_units": 223_316,
    "missing_author_ids": 906,
    "gap_start": "2026-01-31T10:37:53+00:00",
    "gap_end": "2026-02-02T04:20:50+00:00",
    "gap_hours": 41.7,
    "gap_posts": 38_166,
    "gap_snapshots": 39,
    "gap_word_frequency": 5_039,
    "q_5m": 0.0942,
    "q_1h": 0.0982,
    "p_obs": 0.0960,
    "t50_seconds": 4.55,
    "t90_seconds": 50.05,
}
EMPTY_EXCLUSION_COLUMNS = [
    "archive_name",
    "subset",
    "entity_id",
    "exclusion_reason",
    "notes",
]
EMPTY_OVERRIDE_COLUMNS = [
    "archive_name",
    "override_type",
    "target_id",
    "field_name",
    "original_value",
    "override_value",
    "justification",
    "approved_by",
    "approved_at_utc",
]


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _load_frame(root: Path, subset: str) -> pd.DataFrame:
    path = root / f"{subset}.parquet"
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def _coerce_timestamp(series: pd.Series | None) -> pd.Series:
    if series is None:
        return pd.Series(dtype="datetime64[ns, UTC]")
    return pd.to_datetime(series, errors="coerce", utc=True)


def _status(*, violations: int, hard_fail: bool = True, warn_threshold: float | None = None, rate: float | None = None) -> str:
    if violations > 0 and hard_fail:
        return "FAIL"
    if warn_threshold is not None and rate is not None and rate < warn_threshold:
        return "WARN"
    return "PASS"


def _write_csv(path: Path, rows: list[dict[str, Any]], columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame = pd.DataFrame(rows)
    if frame.empty:
        frame = pd.DataFrame(columns=columns)
    else:
        for column in columns:
            if column not in frame.columns:
                frame[column] = pd.NA
        frame = frame.loc[:, columns]
    frame.to_csv(path, index=False)


def _markdown_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]]) -> str:
    headers = [label for _, label in columns]
    out = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        rendered: list[str] = []
        for key, _ in columns:
            value = row.get(key)
            if isinstance(value, float):
                rendered.append(f"{value:.4f}")
            else:
                rendered.append("" if value is None else str(value))
        out.append("| " + " | ".join(rendered) + " |")
    return "\n".join(out)


def _check_required_columns(frame: pd.DataFrame, subset: str, required: list[str]) -> None:
    missing = [column for column in required if column not in frame.columns]
    if missing:
        raise SystemExit(f"{subset}:missing_required_columns:{','.join(missing)}")


def _cycle_count(parent_map: dict[str, str]) -> int:
    visited: dict[str, str] = {}
    cycles = 0
    for node in parent_map:
        if visited.get(node) == "done":
            continue
        current = node
        path: set[str] = set()
        while current in parent_map:
            if current in path:
                cycles += 1
                break
            if visited.get(current) == "done":
                break
            path.add(current)
            current = parent_map[current]
        for item in path:
            visited[item] = "done"
    return cycles


def _build_linkage_audit(
    *,
    archive_name: str,
    comments: pd.DataFrame,
    posts: pd.DataFrame,
    agents: pd.DataFrame,
) -> tuple[list[dict[str, Any]], bool]:
    linkage_rows: list[dict[str, Any]] = []
    hard_fail = False

    comment_threads = comments["thread_id"].astype("string")
    post_threads = set(posts["thread_id"].dropna().astype("string"))
    unresolved_post_links = int((comment_threads.notna() & ~comment_threads.isin(post_threads)).sum())
    checked_post_links = int(comment_threads.notna().sum())
    post_resolution_rate = (
        (checked_post_links - unresolved_post_links) / checked_post_links if checked_post_links else None
    )
    post_status = _status(violations=unresolved_post_links, hard_fail=True)
    hard_fail = hard_fail or post_status == "FAIL"
    linkage_rows.append(
        {
            "archive_name": archive_name,
            "check_name": "comments_thread_id_resolves_to_posts",
            "status": post_status,
            "rows_checked": checked_post_links,
            "violations": unresolved_post_links,
            "resolution_rate": post_resolution_rate,
            "notes": "Every curated comment thread_id must resolve to a canonical post row.",
        }
    )

    parent_rows = comments.loc[comments["parent_comment_id"].notna(), ["comment_id", "thread_id", "parent_comment_id"]].copy()
    parent_lookup = comments.loc[:, ["comment_id", "thread_id", "created_at_utc"]].rename(
        columns={
            "comment_id": "parent_comment_id",
            "thread_id": "parent_thread_id",
            "created_at_utc": "parent_created_at_utc",
        }
    )
    parent_rows = parent_rows.merge(parent_lookup, on="parent_comment_id", how="left")
    unresolved_parent = int(parent_rows["parent_thread_id"].isna().sum())
    wrong_thread = int(
        (
            parent_rows["parent_thread_id"].notna()
            & (parent_rows["thread_id"].astype("string") != parent_rows["parent_thread_id"].astype("string"))
        ).sum()
    )
    parent_status = _status(violations=unresolved_parent + wrong_thread, hard_fail=True)
    hard_fail = hard_fail or parent_status == "FAIL"
    linkage_rows.append(
        {
            "archive_name": archive_name,
            "check_name": "parent_comment_resolves_same_thread",
            "status": parent_status,
            "rows_checked": int(parent_rows.shape[0]),
            "violations": unresolved_parent + wrong_thread,
            "resolution_rate": (
                ((int(parent_rows.shape[0]) - unresolved_parent - wrong_thread) / int(parent_rows.shape[0]))
                if int(parent_rows.shape[0])
                else None
            ),
            "notes": f"unresolved_parent={unresolved_parent}; parent_wrong_thread={wrong_thread}",
        }
    )

    comment_author_nonnull = comments["author_id"].notna()
    agent_ids = set(agents["author_id"].dropna().astype("string"))
    unresolved_comment_authors = int(
        (
            comment_author_nonnull
            & ~comments["author_id"].astype("string").isin(agent_ids)
        ).sum()
    )
    checked_comment_authors = int(comment_author_nonnull.sum())
    comment_author_rate = (
        (checked_comment_authors - unresolved_comment_authors) / checked_comment_authors
        if checked_comment_authors
        else None
    )
    linkage_rows.append(
        {
            "archive_name": archive_name,
            "check_name": "comment_author_ids_resolve_when_present",
            "status": _status(
                violations=0,
                hard_fail=False,
                warn_threshold=0.99,
                rate=comment_author_rate,
            ),
            "rows_checked": checked_comment_authors,
            "violations": unresolved_comment_authors,
            "resolution_rate": comment_author_rate,
            "notes": "Null author_id is tolerated; non-null author ids should resolve to agents.",
        }
    )

    post_author_nonnull = posts["post_author_id"].notna()
    unresolved_post_authors = int(
        (
            post_author_nonnull
            & ~posts["post_author_id"].astype("string").isin(agent_ids)
        ).sum()
    )
    checked_post_authors = int(post_author_nonnull.sum())
    post_author_rate = (
        (checked_post_authors - unresolved_post_authors) / checked_post_authors
        if checked_post_authors
        else None
    )
    linkage_rows.append(
        {
            "archive_name": archive_name,
            "check_name": "post_author_ids_resolve_when_present",
            "status": _status(
                violations=0,
                hard_fail=False,
                warn_threshold=0.99,
                rate=post_author_rate,
            ),
            "rows_checked": checked_post_authors,
            "violations": unresolved_post_authors,
            "resolution_rate": post_author_rate,
            "notes": "Post author resolution is a warning surface rather than a freeze blocker.",
        }
    )

    chronology = comments.loc[:, ["comment_id", "parent_comment_id", "thread_id", "created_at_utc"]].copy()
    chronology = chronology.merge(parent_lookup, on="parent_comment_id", how="left")
    parent_comparable = chronology["parent_created_at_utc"].notna() & chronology["created_at_utc"].notna()
    parent_lag_violations = int(
        (chronology.loc[parent_comparable, "created_at_utc"] < chronology.loc[parent_comparable, "parent_created_at_utc"]).sum()
    )
    parent_lag_status = _status(violations=parent_lag_violations, hard_fail=True)
    hard_fail = hard_fail or parent_lag_status == "FAIL"
    linkage_rows.append(
        {
            "archive_name": archive_name,
            "check_name": "non_negative_child_parent_lag",
            "status": parent_lag_status,
            "rows_checked": int(parent_comparable.sum()),
            "violations": parent_lag_violations,
            "resolution_rate": None,
            "notes": "Negative child-parent lag is a hard fail.",
        }
    )

    post_lookup = posts.loc[:, ["thread_id", "post_created_at_utc"]].copy()
    chronology = chronology.merge(post_lookup, on="thread_id", how="left")
    post_comparable = chronology["post_created_at_utc"].notna() & chronology["created_at_utc"].notna()
    post_lag_violations = int(
        (chronology.loc[post_comparable, "created_at_utc"] < chronology.loc[post_comparable, "post_created_at_utc"]).sum()
    )
    post_lag_status = _status(violations=post_lag_violations, hard_fail=True)
    hard_fail = hard_fail or post_lag_status == "FAIL"
    linkage_rows.append(
        {
            "archive_name": archive_name,
            "check_name": "non_negative_comment_post_lag",
            "status": post_lag_status,
            "rows_checked": int(post_comparable.sum()),
            "violations": post_lag_violations,
            "resolution_rate": None,
            "notes": "Negative comment-post lag is a hard fail.",
        }
    )

    parent_map = {
        str(row.comment_id): str(row.parent_comment_id)
        for row in comments.loc[
            comments["comment_id"].notna() & comments["parent_comment_id"].notna(),
            ["comment_id", "parent_comment_id"],
        ].itertuples(index=False)
    }
    cycle_count = _cycle_count(parent_map)
    cycle_status = _status(violations=cycle_count, hard_fail=True)
    hard_fail = hard_fail or cycle_status == "FAIL"
    linkage_rows.append(
        {
            "archive_name": archive_name,
            "check_name": "parent_graph_cycles",
            "status": cycle_status,
            "rows_checked": len(parent_map),
            "violations": cycle_count,
            "resolution_rate": None,
            "notes": "Any cycle in the parent graph is a hard fail.",
        }
    )

    return linkage_rows, hard_fail


def _build_gap_registry(archive_name: str, comments: pd.DataFrame) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    timestamps = (
        comments["created_at_utc"]
        .dropna()
        .sort_values()
        .drop_duplicates()
        .reset_index(drop=True)
    )
    gap_rows: list[dict[str, Any]] = []
    severe_rows: list[dict[str, Any]] = []
    if timestamps.shape[0] < 2:
        return gap_rows, severe_rows

    previous = timestamps.shift(1)
    gaps = timestamps - previous
    for idx in range(1, len(timestamps)):
        gap = gaps.iloc[idx]
        if pd.isna(gap):
            continue
        gap_hours = float(gap.total_seconds() / 3600.0)
        if gap_hours <= GAP_LOG_HOURS:
            continue
        row = {
            "archive_name": archive_name,
            "gap_index": len(gap_rows) + 1,
            "previous_comment_created_at_utc": previous.iloc[idx].isoformat(),
            "next_comment_created_at_utc": timestamps.iloc[idx].isoformat(),
            "gap_seconds": int(gap.total_seconds()),
            "gap_hours": gap_hours,
            "severity": "severe" if gap_hours > SEVERE_GAP_HOURS else "logged",
        }
        gap_rows.append(row)
        if row["severity"] == "severe":
            severe_rows.append(row)
    return gap_rows, severe_rows


def _count_in_gap(frame: pd.DataFrame, timestamp_column: str, start: pd.Timestamp, end: pd.Timestamp) -> int:
    if frame.empty or timestamp_column not in frame.columns:
        return 0
    series = frame[timestamp_column]
    return int((series.notna() & (series >= start) & (series <= end)).sum())


def _build_gap_disambiguation(
    *,
    archive_name: str,
    severe_gaps: list[dict[str, Any]],
    posts: pd.DataFrame,
    snapshots: pd.DataFrame,
    word_frequency: pd.DataFrame,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for gap in severe_gaps:
        start = pd.Timestamp(gap["previous_comment_created_at_utc"])
        end = pd.Timestamp(gap["next_comment_created_at_utc"])
        posts_count = _count_in_gap(posts, "post_created_at_utc", start, end)
        snapshots_count = _count_in_gap(snapshots, "snapshot_timestamp_utc", start, end)
        word_frequency_count = _count_in_gap(word_frequency, "hour_utc", start, end)
        rows.append(
            {
                "archive_name": archive_name,
                "gap_index": gap["gap_index"],
                "gap_start_utc": start.isoformat(),
                "gap_end_utc": end.isoformat(),
                "gap_hours": gap["gap_hours"],
                "posts_in_gap": posts_count,
                "snapshots_in_gap": snapshots_count,
                "word_frequency_records_in_gap": word_frequency_count,
                "likely_archive_interruption": bool(posts_count or snapshots_count or word_frequency_count),
            }
        )
    return rows


def _earliest_reply_metrics(comments: pd.DataFrame) -> dict[str, Any]:
    parents = comments.loc[comments["comment_id"].notna() & comments["created_at_utc"].notna(), ["comment_id", "created_at_utc"]].copy()
    parents.rename(columns={"comment_id": "parent_comment_id", "created_at_utc": "parent_created_at_utc"}, inplace=True)

    children = comments.loc[
        comments["parent_comment_id"].notna() & comments["created_at_utc"].notna(),
        ["comment_id", "parent_comment_id", "created_at_utc"],
    ].copy()
    merged = children.merge(parents, on="parent_comment_id", how="inner")
    merged["reply_lag_seconds"] = (
        merged["created_at_utc"] - merged["parent_created_at_utc"]
    ).dt.total_seconds()
    merged = merged.loc[merged["reply_lag_seconds"] >= 0].copy()

    first_reply = merged.groupby("parent_comment_id", as_index=False)["reply_lag_seconds"].min()
    parent_ids = set(parents["parent_comment_id"].astype("string"))
    replied_ids = set(first_reply["parent_comment_id"].astype("string"))
    candidate_parent_units = int(parents.shape[0])
    p_obs = (len(replied_ids) / candidate_parent_units) if candidate_parent_units else None
    q_5m = (
        int((first_reply["reply_lag_seconds"] <= 300).sum()) / candidate_parent_units
        if candidate_parent_units
        else None
    )
    q_1h = (
        int((first_reply["reply_lag_seconds"] <= 3600).sum()) / candidate_parent_units
        if candidate_parent_units
        else None
    )
    quantiles = first_reply["reply_lag_seconds"].quantile([0.5, 0.9]) if not first_reply.empty else pd.Series(dtype=float)
    return {
        "candidate_parent_units": candidate_parent_units,
        "ever_replied_units": len(replied_ids),
        "p_obs": p_obs,
        "q_5m": q_5m,
        "q_1h": q_1h,
        "t50_seconds": float(quantiles.get(0.5)) if 0.5 in quantiles.index else None,
        "t90_seconds": float(quantiles.get(0.9)) if 0.9 in quantiles.index else None,
        "reply_lag_rows": int(first_reply.shape[0]),
    }


def _benchmark_rows(
    *,
    archive_name: str,
    comments: pd.DataFrame,
    agents: pd.DataFrame,
    posts: pd.DataFrame,
    snapshots: pd.DataFrame,
    submolts: pd.DataFrame,
    word_frequency: pd.DataFrame,
    severe_gaps: list[dict[str, Any]],
    gap_disambiguation: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], str]:
    metrics = _earliest_reply_metrics(comments)
    rows: list[dict[str, Any]] = []
    benchmark_status = "PASS"

    actuals: dict[str, Any] = {
        "comments_rows": int(comments.shape[0]),
        "unique_comment_ids": int(comments["comment_id"].dropna().astype("string").nunique()),
        "candidate_parent_units": int(metrics["candidate_parent_units"]),
        "missing_author_ids": int(comments["author_id"].isna().sum()),
        "agents_rows": int(agents.shape[0]),
        "posts_rows": int(posts.shape[0]),
        "snapshots_rows": int(snapshots.shape[0]),
        "submolts_rows": int(submolts.shape[0]),
        "word_frequency_rows": int(word_frequency.shape[0]),
        "q_5m": metrics["q_5m"],
        "q_1h": metrics["q_1h"],
        "p_obs": metrics["p_obs"],
        "t50_seconds": metrics["t50_seconds"],
        "t90_seconds": metrics["t90_seconds"],
    }

    largest_gap = severe_gaps[0] if severe_gaps else None
    largest_gap_disambiguation = (
        next((row for row in gap_disambiguation if row["gap_index"] == largest_gap["gap_index"]), None)
        if largest_gap is not None
        else None
    )
    if largest_gap is not None:
        actuals["gap_start"] = largest_gap["previous_comment_created_at_utc"]
        actuals["gap_end"] = largest_gap["next_comment_created_at_utc"]
        actuals["gap_hours"] = largest_gap["gap_hours"]
    if largest_gap_disambiguation is not None:
        actuals["gap_posts"] = largest_gap_disambiguation["posts_in_gap"]
        actuals["gap_snapshots"] = largest_gap_disambiguation["snapshots_in_gap"]
        actuals["gap_word_frequency"] = largest_gap_disambiguation["word_frequency_records_in_gap"]

    benchmarks = SIMULAMET_BENCHMARKS if archive_name == "simulamet" else {}
    for metric_name, actual in actuals.items():
        expected = benchmarks.get(metric_name)
        tolerance = 0.0
        status = "INFO"
        if expected is not None:
            if isinstance(expected, float):
                tolerance = 0.005 if metric_name.startswith("q_") or metric_name == "p_obs" else 1.0
                status = "PASS" if actual is not None and abs(float(actual) - expected) <= tolerance else "FAIL"
            else:
                status = "PASS" if actual == expected else "FAIL"
            if status == "FAIL":
                benchmark_status = "FAIL"
        rows.append(
            {
                "metric_name": metric_name,
                "actual": actual,
                "expected": expected,
                "tolerance": tolerance if expected is not None else None,
                "status": status,
            }
        )

    return rows, benchmark_status


def _qc_summary_status(linkage_rows: list[dict[str, Any]], benchmark_status: str) -> str:
    if any(row["status"] == "FAIL" for row in linkage_rows):
        return "FAIL"
    if benchmark_status == "FAIL":
        return "FAIL"
    if any(row["status"] == "WARN" for row in linkage_rows):
        return "WARN"
    return "PASS"


def _write_benchmark_report(
    *,
    path: Path,
    archive_name: str,
    benchmark_rows: list[dict[str, Any]],
    overall_status: str,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    contents = [
        f"# Benchmark Report: {archive_name}",
        "",
        f"- Generated at UTC: {_utc_now_iso()}",
        f"- Overall status: {overall_status}",
        "",
        _markdown_table(
            benchmark_rows,
            [
                ("metric_name", "Metric"),
                ("actual", "Actual"),
                ("expected", "Expected"),
                ("tolerance", "Tolerance"),
                ("status", "Status"),
            ],
        ),
        "",
    ]
    path.write_text("\n".join(contents), encoding="utf-8")


def _write_qc_report(
    *,
    path: Path,
    archive_name: str,
    overall_status: str,
    linkage_rows: list[dict[str, Any]],
    gap_rows: list[dict[str, Any]],
    severe_gap_rows: list[dict[str, Any]],
    benchmark_rows: list[dict[str, Any]],
    benchmark_report_path: Path,
    linkage_path: Path,
    gap_registry_path: Path,
    gap_disambiguation_path: Path,
    exclusion_path: Path,
    manual_override_path: Path,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"# Archive QC Report: {archive_name}",
        "",
        f"- Generated at UTC: {_utc_now_iso()}",
        f"- Overall status: {overall_status}",
        f"- Linkage audit: {linkage_path}",
        f"- Gap registry: {gap_registry_path}",
        f"- Gap disambiguation: {gap_disambiguation_path}",
        f"- Benchmark report: {benchmark_report_path}",
        f"- Exclusion log: {exclusion_path}",
        f"- Manual override log: {manual_override_path}",
        "",
        "## Linkage Summary",
        "",
        _markdown_table(
            linkage_rows,
            [
                ("check_name", "Check"),
                ("status", "Status"),
                ("rows_checked", "Rows Checked"),
                ("violations", "Violations"),
                ("resolution_rate", "Resolution Rate"),
                ("notes", "Notes"),
            ],
        ),
        "",
        "## Gap Summary",
        "",
        f"- Logged gaps > 6h: {len(gap_rows)}",
        f"- Severe gaps > 24h: {len(severe_gap_rows)}",
        "",
        "## Benchmark Summary",
        "",
        _markdown_table(
            benchmark_rows,
            [
                ("metric_name", "Metric"),
                ("actual", "Actual"),
                ("expected", "Expected"),
                ("status", "Status"),
            ],
        ),
        "",
        "## Sign-off",
        "",
        "- Data Acquisition Lead: pending",
        "- QA Lead: pending",
        "- PI: pending",
        "",
    ]
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate a canonical archive freeze, emit the tracked QC packet, and hard-fail "
            "when linkage or benchmark checks do not meet flagship requirements."
        )
    )
    parser.add_argument("--freeze-root", type=Path, required=True, help="Freeze root containing canonical parquet tables.")
    parser.add_argument("--archive-name", default="simulamet", help="Archive label for QC outputs.")
    parser.add_argument("--out-linkage-audit", type=Path, required=True, help="Output CSV path for linkage audit.")
    parser.add_argument("--out-gap-registry", type=Path, required=True, help="Output CSV path for gap registry.")
    parser.add_argument(
        "--out-gap-disambiguation",
        type=Path,
        required=True,
        help="Output CSV path for severe-gap disambiguation.",
    )
    parser.add_argument("--out-benchmark-report", type=Path, required=True, help="Output markdown path for benchmark report.")
    parser.add_argument("--out-qc-report", type=Path, required=True, help="Output markdown path for the QC packet.")
    parser.add_argument("--out-exclusion-log", type=Path, required=True, help="Output CSV path for explicit exclusions.")
    parser.add_argument(
        "--out-manual-override-log",
        type=Path,
        required=True,
        help="Output CSV path for manual overrides.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    archive_name = str(args.archive_name).strip().lower()

    comments = _load_frame(args.freeze_root, "comments")
    posts = _load_frame(args.freeze_root, "posts")
    agents = _load_frame(args.freeze_root, "agents")
    snapshots = _load_frame(args.freeze_root, "snapshots")
    submolts = _load_frame(args.freeze_root, "submolts")
    word_frequency = _load_frame(args.freeze_root, "word_frequency")

    if comments.empty or posts.empty:
        raise SystemExit("freeze_root_missing_core_tables:comments_or_posts")

    _check_required_columns(comments, "comments", ["comment_id", "thread_id", "parent_comment_id", "author_id", "created_at_utc"])
    _check_required_columns(posts, "posts", ["thread_id", "post_author_id", "community_label", "post_created_at_utc"])
    _check_required_columns(agents, "agents", ["author_id"])

    comments = comments.copy()
    posts = posts.copy()
    agents = agents.copy()
    snapshots = snapshots.copy()
    submolts = submolts.copy()
    word_frequency = word_frequency.copy()

    comments["created_at_utc"] = _coerce_timestamp(comments.get("created_at_utc"))
    posts["post_created_at_utc"] = _coerce_timestamp(posts.get("post_created_at_utc"))
    snapshots["snapshot_timestamp_utc"] = _coerce_timestamp(snapshots.get("snapshot_timestamp_utc"))
    word_frequency["hour_utc"] = _coerce_timestamp(word_frequency.get("hour_utc"))

    linkage_rows, linkage_hard_fail = _build_linkage_audit(
        archive_name=archive_name,
        comments=comments,
        posts=posts,
        agents=agents,
    )
    gap_rows, severe_gap_rows = _build_gap_registry(archive_name, comments)
    gap_disambiguation_rows = _build_gap_disambiguation(
        archive_name=archive_name,
        severe_gaps=severe_gap_rows,
        posts=posts,
        snapshots=snapshots,
        word_frequency=word_frequency,
    )
    benchmark_rows, benchmark_status = _benchmark_rows(
        archive_name=archive_name,
        comments=comments,
        agents=agents,
        posts=posts,
        snapshots=snapshots,
        submolts=submolts,
        word_frequency=word_frequency,
        severe_gaps=severe_gap_rows,
        gap_disambiguation=gap_disambiguation_rows,
    )
    overall_status = _qc_summary_status(linkage_rows, benchmark_status)

    linkage_columns = [
        "archive_name",
        "check_name",
        "status",
        "rows_checked",
        "violations",
        "resolution_rate",
        "notes",
    ]
    gap_columns = [
        "archive_name",
        "gap_index",
        "previous_comment_created_at_utc",
        "next_comment_created_at_utc",
        "gap_seconds",
        "gap_hours",
        "severity",
    ]
    gap_disambiguation_columns = [
        "archive_name",
        "gap_index",
        "gap_start_utc",
        "gap_end_utc",
        "gap_hours",
        "posts_in_gap",
        "snapshots_in_gap",
        "word_frequency_records_in_gap",
        "likely_archive_interruption",
    ]

    _write_csv(args.out_linkage_audit, linkage_rows, linkage_columns)
    _write_csv(args.out_gap_registry, gap_rows, gap_columns)
    _write_csv(args.out_gap_disambiguation, gap_disambiguation_rows, gap_disambiguation_columns)
    _write_csv(args.out_exclusion_log, [], EMPTY_EXCLUSION_COLUMNS)
    _write_csv(args.out_manual_override_log, [], EMPTY_OVERRIDE_COLUMNS)
    _write_benchmark_report(
        path=args.out_benchmark_report,
        archive_name=archive_name,
        benchmark_rows=benchmark_rows,
        overall_status=benchmark_status,
    )
    _write_qc_report(
        path=args.out_qc_report,
        archive_name=archive_name,
        overall_status=overall_status,
        linkage_rows=linkage_rows,
        gap_rows=gap_rows,
        severe_gap_rows=severe_gap_rows,
        benchmark_rows=benchmark_rows,
        benchmark_report_path=args.out_benchmark_report,
        linkage_path=args.out_linkage_audit,
        gap_registry_path=args.out_gap_registry,
        gap_disambiguation_path=args.out_gap_disambiguation,
        exclusion_path=args.out_exclusion_log,
        manual_override_path=args.out_manual_override_log,
    )

    summary = {
        "archive_name": archive_name,
        "overall_status": overall_status,
        "linkage_hard_fail": linkage_hard_fail,
        "benchmark_status": benchmark_status,
        "gap_count": len(gap_rows),
        "severe_gap_count": len(severe_gap_rows),
        "outputs": {
            "linkage_audit": str(args.out_linkage_audit),
            "gap_registry": str(args.out_gap_registry),
            "gap_disambiguation": str(args.out_gap_disambiguation),
            "benchmark_report": str(args.out_benchmark_report),
            "qc_report": str(args.out_qc_report),
            "exclusion_log": str(args.out_exclusion_log),
            "manual_override_log": str(args.out_manual_override_log),
        },
    }
    print(json.dumps(summary, indent=2, sort_keys=True))

    if overall_status == "FAIL":
        raise SystemExit("archive_qc_failed")


if __name__ == "__main__":
    main()
