#!/usr/bin/env python3

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from _derived_builders import write_parquet

HORIZONS = [
    ("30s", "R_30s", "Y_30s", 30),
    ("5m", "R_5m", "Y_5m", 300),
    ("1h", "R_1h", "Y_1h", 3600),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build the canonical control_panel_summary table.")
    parser.add_argument("--parent-units", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser.parse_args()


def _window_variants(frame: pd.DataFrame) -> list[tuple[str, pd.Series]]:
    variants: list[tuple[str, pd.Series]] = [("full_window", pd.Series(True, index=frame.index))]
    segment_ids = sorted(str(value) for value in frame["segment_id"].dropna().unique())
    if len(segment_ids) > 1:
        variants.append(("pre_gap_contiguous", frame["segment_id"].astype("string") == segment_ids[0]))
        variants.append(("post_gap_contiguous", frame["segment_id"].astype("string") == segment_ids[-1]))
    variants.append(("exclude_gap_overlap_6h", frame["gap_overlap_6h_flag"] == 0))
    variants.append(("exclude_gap_overlap_24h", frame["gap_overlap_24h_flag"] == 0))
    return variants


def _strata(frame: pd.DataFrame) -> list[tuple[str, str, pd.Series]]:
    items: list[tuple[str, str, pd.Series]] = [("overall", "Overall", pd.Series(True, index=frame.index))]
    for family in ["claimed_status_group", "topic_category"]:
        for value in sorted(str(item) for item in frame[family].dropna().unique()):
            items.append((family, value, frame[family].astype("string") == value))
    return items


def build_control_panel_summary_frame(parent_units: Path) -> pd.DataFrame:
    frame = pd.read_parquet(parent_units).copy()
    if frame.empty:
        raise SystemExit(f"Missing parent_units rows: {parent_units}")

    rows: list[dict[str, Any]] = []
    archive_name = str(frame["archive_name"].iloc[0])
    archive_revision = str(frame["archive_revision"].iloc[0])

    for window_variant, window_mask in _window_variants(frame):
        window_frame = frame.loc[window_mask].copy()
        if window_frame.empty:
            continue
        replies = window_frame.loc[window_frame["delta"] == 1, "T_seconds"].dropna()
        p_obs = float(window_frame["delta"].mean()) if len(window_frame) else np.nan
        t50 = float(replies.quantile(0.5)) if not replies.empty else np.nan
        t90 = float(replies.quantile(0.9)) if not replies.empty else np.nan
        t95 = float(replies.quantile(0.95)) if not replies.empty else np.nan
        reply_le_30s = float((replies <= 30).mean()) if not replies.empty else np.nan
        reply_le_5m = float((replies <= 300).mean()) if not replies.empty else np.nan

        for stratum_family, stratum_value, stratum_mask in _strata(window_frame):
            subset = window_frame.loc[stratum_mask].copy()
            if subset.empty:
                continue
            subset_replies = subset.loc[subset["delta"] == 1, "T_seconds"].dropna()
            subset_t50 = float(subset_replies.quantile(0.5)) if not subset_replies.empty else np.nan
            subset_t90 = float(subset_replies.quantile(0.9)) if not subset_replies.empty else np.nan
            subset_t95 = float(subset_replies.quantile(0.95)) if not subset_replies.empty else np.nan
            subset_reply_le_30s = float((subset_replies <= 30).mean()) if not subset_replies.empty else np.nan
            subset_reply_le_5m = float((subset_replies <= 300).mean()) if not subset_replies.empty else np.nan
            subset_p_obs = float(subset["delta"].mean()) if len(subset) else np.nan

            for horizon_label, risk_col, within_col, _seconds in HORIZONS:
                n_parents = int(len(subset))
                n_riskset = int(subset[risk_col].sum())
                replied_mask = (subset["delta"] == 1) & (subset[risk_col] == 1)
                n_replied = int(replied_mask.sum())
                n_within_h = int(subset[within_col].sum())
                pi_h = (n_replied / n_riskset) if n_riskset else np.nan
                phi_h = (n_within_h / n_replied) if n_replied else np.nan
                q_h = (n_within_h / n_riskset) if n_riskset else np.nan
                benchmark_flag = bool(
                    archive_name == "simulamet"
                    and stratum_family == "overall"
                    and window_variant in {"full_window", "post_gap_contiguous", "exclude_gap_overlap_6h", "exclude_gap_overlap_24h"}
                )
                rows.append(
                    {
                        "archive_name": archive_name,
                        "archive_revision": archive_revision,
                        "window_variant": window_variant,
                        "horizon_label": horizon_label,
                        "stratum_family": stratum_family,
                        "stratum_value": stratum_value,
                        "n_parents": n_parents,
                        "n_riskset": n_riskset,
                        "n_replied": n_replied,
                        "n_within_h": n_within_h,
                        "p_obs": subset_p_obs,
                        "pi_h": pi_h,
                        "phi_h": phi_h,
                        "q_h": q_h,
                        "t50_seconds": subset_t50,
                        "t90_seconds": subset_t90,
                        "t95_seconds": subset_t95,
                        "reply_le_30s_given_reply": subset_reply_le_30s,
                        "reply_le_5m_given_reply": subset_reply_le_5m,
                        "benchmark_flag": benchmark_flag,
                    }
                )

    summary = pd.DataFrame(rows)
    if summary.empty:
        raise SystemExit("No control_panel_summary rows generated.")
    summary.sort_values(
        ["window_variant", "stratum_family", "stratum_value", "horizon_label"],
        inplace=True,
        kind="stable",
    )
    return summary


def main() -> None:
    args = parse_args()
    summary = build_control_panel_summary_frame(args.parent_units)
    write_parquet(args.out, summary)


if __name__ == "__main__":
    main()
