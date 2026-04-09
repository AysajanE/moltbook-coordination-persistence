#!/usr/bin/env python3
"""Append horizon-standardization, geometry, and periodicity notes to the flagship report."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

import numpy as np
import pandas as pd

PRIMARY_WINDOWS = (
    "full_window",
    "pre_gap_contiguous",
    "post_gap_contiguous",
    "exclude_gap_overlap_6h",
    "exclude_gap_overlap_24h",
)
PRIMARY_HORIZONS = (
    ("5m", "R_5m", "Y_5m"),
    ("1h", "R_1h", "Y_1h"),
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Append horizon-standardization diagnostics to the flagship markdown report."
    )
    parser.add_argument("--parent-units", type=Path, required=True)
    parser.add_argument("--append-report", type=Path, required=True)
    return parser.parse_args()


def load_parent_units(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path).copy()
    required = {
        "archive_name",
        "archive_revision",
        "thread_id",
        "segment_id",
        "delta",
        "R_5m",
        "Y_5m",
        "R_1h",
        "Y_1h",
        "gap_overlap_6h_flag",
        "gap_overlap_24h_flag",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise SystemExit(f"parent_units missing required columns: {', '.join(missing)}")
    return frame


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def fmt(value: object, digits: int = 4) -> str:
    if value is None or pd.isna(value):
        return ""
    if isinstance(value, bool):
        return "yes" if value else "no"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.{digits}f}"
    return str(value)


def markdown_table(frame: pd.DataFrame, columns: list[tuple[str, str]]) -> str:
    lines = [
        "| " + " | ".join(label for _, label in columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for row in frame.itertuples(index=False):
        values = [fmt(getattr(row, key)) for key, _ in columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def window_variants(frame: pd.DataFrame) -> list[tuple[str, pd.Series]]:
    variants: list[tuple[str, pd.Series]] = [("full_window", pd.Series(True, index=frame.index))]
    segment_ids = sorted(str(value) for value in frame["segment_id"].dropna().unique())
    if len(segment_ids) > 1:
        variants.append(("pre_gap_contiguous", frame["segment_id"].astype("string") == segment_ids[0]))
        variants.append(("post_gap_contiguous", frame["segment_id"].astype("string") == segment_ids[-1]))
    variants.append(("exclude_gap_overlap_6h", frame["gap_overlap_6h_flag"] == 0))
    variants.append(("exclude_gap_overlap_24h", frame["gap_overlap_24h_flag"] == 0))
    return variants


def horizon_standardization_table(frame: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for window_name, mask in window_variants(frame):
        subset = frame.loc[mask].copy()
        if subset.empty:
            continue
        row: dict[str, object] = {
            "window_variant": window_name,
            "n_parents": int(len(subset)),
            "p_obs": float(subset["delta"].mean()) if len(subset) else np.nan,
        }
        for horizon_label, risk_col, within_col in PRIMARY_HORIZONS:
            risk_n = int(subset[risk_col].sum())
            within_n = int(subset[within_col].sum())
            row[f"n_riskset_{horizon_label}"] = risk_n
            row[f"q_{horizon_label}"] = (within_n / risk_n) if risk_n else np.nan
        rows.append(row)
    out = pd.DataFrame(rows)
    order = {name: i for i, name in enumerate(PRIMARY_WINDOWS)}
    out.sort_values("window_variant", key=lambda s: s.map(order).fillna(99), inplace=True, kind="stable")
    return out


def thread_level_geometry_diagnostic(parent_units: pd.DataFrame, derived_root: Path, archive_name: str) -> str:
    path = derived_root / f"thread_geometry_{archive_name}.parquet"
    if not path.exists():
        return "_No thread_geometry artifact was found beside parent_units, so H3 geometry notes were skipped._"

    geometry = pd.read_parquet(path).copy()
    if geometry.empty:
        return "_thread_geometry was present but empty._"

    thread_rows: list[dict[str, object]] = []
    for thread_id, subset in parent_units.groupby("thread_id", dropna=False, sort=False):
        row: dict[str, object] = {"thread_id": thread_id}
        row["q_5m"] = float(subset["Y_5m"].sum()) / float(subset["R_5m"].sum()) if int(subset["R_5m"].sum()) else np.nan
        row["q_1h"] = float(subset["Y_1h"].sum()) / float(subset["R_1h"].sum()) if int(subset["R_1h"].sum()) else np.nan
        row["p_obs"] = float(subset["delta"].mean()) if len(subset) else np.nan
        thread_rows.append(row)

    thread_panel = pd.DataFrame(thread_rows)
    joined = geometry.merge(thread_panel, on="thread_id", how="left", validate="one_to_one")
    summary = pd.DataFrame(
        [
            {
                "n_threads": int(len(joined)),
                "mean_max_depth": joined["max_depth"].mean(),
                "mean_nonroot_branching": joined["nonroot_branching_mean"].mean(),
                "mean_reciprocity_rate": joined["reciprocity_rate"].mean(),
                "mean_reentry_rate": joined["reentry_rate_paper"].mean(),
                "corr_q5_max_depth": joined["q_5m"].corr(joined["max_depth"], method="spearman"),
                "corr_q5_reentry": joined["q_5m"].corr(joined["reentry_rate_paper"], method="spearman"),
                "corr_q1_max_depth": joined["q_1h"].corr(joined["max_depth"], method="spearman"),
                "threads_with_missing_author_comments": int((joined["missing_author_comment_count"] > 0).sum()),
            }
        ]
    )
    return markdown_table(
        summary,
        [
            ("n_threads", "Threads"),
            ("mean_max_depth", "Mean max depth"),
            ("mean_nonroot_branching", "Mean branching"),
            ("mean_reciprocity_rate", "Mean reciprocity"),
            ("mean_reentry_rate", "Mean reentry"),
            ("corr_q5_max_depth", "Spearman(q_5m,max_depth)"),
            ("corr_q5_reentry", "Spearman(q_5m,reentry)"),
            ("corr_q1_max_depth", "Spearman(q_1h,max_depth)"),
            ("threads_with_missing_author_comments", "Threads w/ missing author"),
        ],
    )


def periodicity_diagnostic(derived_root: Path, archive_name: str) -> str:
    path = derived_root / f"periodicity_input_{archive_name}.parquet"
    if not path.exists():
        return "_No periodicity_input artifact was found beside parent_units, so H5 notes were skipped._"

    periodicity = pd.read_parquet(path).copy()
    if periodicity.empty:
        return "_periodicity_input was present but empty._"

    window_start = pd.to_datetime(periodicity["window_start_utc"].iloc[0], utc=True)
    window_end = pd.to_datetime(periodicity["window_end_utc"].iloc[0], utc=True)
    duration_hours = (window_end - window_start).total_seconds() / 3600.0
    phases = pd.to_numeric(periodicity["phase_mod_4h_seconds"], errors="coerce").dropna().to_numpy()
    angles = 2.0 * np.pi * phases / (4.0 * 3600.0)
    resultant_length = float(np.abs(np.exp(1j * angles).mean())) if len(angles) else np.nan
    bin_counts_5m = periodicity.groupby("bin_5m").size()

    summary = pd.DataFrame(
        [
            {
                "segment_id": str(periodicity["segment_id"].iloc[0]),
                "n_events": int(len(periodicity)),
                "duration_hours": duration_hours,
                "resultant_length": resultant_length,
                "min_5m_bin_count": int(bin_counts_5m.min()) if not bin_counts_5m.empty else 0,
                "max_5m_bin_count": int(bin_counts_5m.max()) if not bin_counts_5m.empty else 0,
                "meets_48h_target": duration_hours >= 48.0,
            }
        ]
    )
    return markdown_table(
        summary,
        [
            ("segment_id", "Segment"),
            ("n_events", "Events"),
            ("duration_hours", "Duration_h"),
            ("resultant_length", "Resultant"),
            ("min_5m_bin_count", "Min 5m bin"),
            ("max_5m_bin_count", "Max 5m bin"),
            ("meets_48h_target", "Meets 48h target"),
        ],
    )


def build_appendix(parent_units: pd.DataFrame, input_path: Path) -> str:
    archive_name = str(parent_units["archive_name"].iloc[0])
    archive_revision = str(parent_units["archive_revision"].iloc[0])
    derived_root = input_path.parent
    standardization = horizon_standardization_table(parent_units)

    parts = [
        "",
        "## Horizon Standardization",
        "",
        f"Appended at: {datetime.now(UTC).isoformat()}",
        f"Archive: {archive_name}",
        f"Archive revision: {archive_revision}",
        f"Input parent_units: {input_path}",
        "",
        markdown_table(
            standardization,
            [
                ("window_variant", "Window"),
                ("n_parents", "Parents"),
                ("p_obs", "p_obs"),
                ("n_riskset_5m", "Risk set 5m"),
                ("q_5m", "q_5m"),
                ("n_riskset_1h", "Risk set 1h"),
                ("q_1h", "q_1h"),
            ],
        ),
        "",
        "This section recomputes the headline horizon-standardized completion rates directly from parent_units so the markdown report is not coupled to any legacy survival-table surface.",
        "",
        "## H3 Geometry Diagnostic",
        "",
        thread_level_geometry_diagnostic(parent_units, derived_root, archive_name),
        "",
        "## H5 Periodicity Diagnostic",
        "",
        periodicity_diagnostic(derived_root, archive_name),
    ]
    return "\n".join(parts).rstrip() + "\n"


def main() -> None:
    args = parse_args()
    parent_units = load_parent_units(args.parent_units)
    appendix = build_appendix(parent_units, args.parent_units)
    ensure_parent(args.append_report)
    existing = args.append_report.read_text(encoding="utf-8") if args.append_report.exists() else ""
    args.append_report.write_text(existing.rstrip() + appendix, encoding="utf-8")


if __name__ == "__main__":
    main()
