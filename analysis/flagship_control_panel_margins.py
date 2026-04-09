#!/usr/bin/env python3
"""Generate the markdown control-panel section for the flagship analysis report."""

from __future__ import annotations

import argparse
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd

PRIMARY_HORIZONS = ("5m", "1h")
ROBUSTNESS_WINDOWS = (
    "full_window",
    "pre_gap_contiguous",
    "post_gap_contiguous",
    "exclude_gap_overlap_6h",
    "exclude_gap_overlap_24h",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Render the flagship control-panel markdown report from control_panel_summary."
    )
    parser.add_argument("--control-panel", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    return parser.parse_args()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def load_control_panel(path: Path) -> pd.DataFrame:
    frame = pd.read_parquet(path).copy()
    required = {
        "archive_name",
        "archive_revision",
        "window_variant",
        "horizon_label",
        "stratum_family",
        "stratum_value",
        "n_parents",
        "n_riskset",
        "n_replied",
        "n_within_h",
        "p_obs",
        "pi_h",
        "phi_h",
        "q_h",
        "t50_seconds",
        "t90_seconds",
        "t95_seconds",
        "reply_le_30s_given_reply",
        "reply_le_5m_given_reply",
        "benchmark_flag",
    }
    missing = sorted(required - set(frame.columns))
    if missing:
        raise SystemExit(f"control_panel_summary missing required columns: {', '.join(missing)}")
    return frame


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


def overall_primary_table(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.loc[
        (frame["window_variant"] == "full_window")
        & (frame["stratum_family"] == "overall")
        & frame["horizon_label"].isin(PRIMARY_HORIZONS),
        [
            "horizon_label",
            "n_parents",
            "n_riskset",
            "p_obs",
            "pi_h",
            "phi_h",
            "q_h",
            "t50_seconds",
            "t90_seconds",
            "reply_le_30s_given_reply",
            "reply_le_5m_given_reply",
        ],
    ].copy()
    out.sort_values("horizon_label", key=lambda s: s.map({"5m": 0, "1h": 1}), inplace=True, kind="stable")
    return out


def robustness_table(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.loc[
        frame["window_variant"].isin(ROBUSTNESS_WINDOWS)
        & (frame["stratum_family"] == "overall")
        & frame["horizon_label"].isin(PRIMARY_HORIZONS),
        ["window_variant", "horizon_label", "pi_h", "phi_h", "q_h", "benchmark_flag"],
    ].copy()
    out.sort_values(
        ["window_variant", "horizon_label"],
        key=lambda s: s.map({name: i for i, name in enumerate(ROBUSTNESS_WINDOWS)}).fillna(99)
        if s.name == "window_variant"
        else s.map({"5m": 0, "1h": 1}).fillna(99),
        inplace=True,
        kind="stable",
    )
    return out


def topic_table(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.loc[
        (frame["window_variant"] == "full_window")
        & (frame["stratum_family"] == "topic_category")
        & frame["horizon_label"].isin(PRIMARY_HORIZONS),
        ["stratum_value", "horizon_label", "n_riskset", "pi_h", "phi_h", "q_h", "t90_seconds"],
    ].copy()
    out.rename(columns={"stratum_value": "topic_category"}, inplace=True)
    out.sort_values(["horizon_label", "q_h", "topic_category"], ascending=[True, False, True], inplace=True, kind="stable")
    return out


def p2_priority_table(frame: pd.DataFrame) -> pd.DataFrame:
    overall = frame.loc[
        (frame["window_variant"] == "full_window")
        & (frame["stratum_family"] == "overall")
        & frame["horizon_label"].isin(PRIMARY_HORIZONS),
        ["horizon_label", "pi_h", "phi_h", "q_h"],
    ].copy()
    if overall.empty:
        return overall
    overall["incidence_gain_per_unit_dpi"] = overall["phi_h"]
    overall["timing_gain_per_unit_dphi"] = overall["pi_h"]
    overall["equal_delta_priority"] = overall.apply(
        lambda row: "incidence"
        if float(row["phi_h"]) > float(row["pi_h"])
        else ("timing" if float(row["phi_h"]) < float(row["pi_h"]) else "tie"),
        axis=1,
    )
    overall["equal_delta_cost_ratio_threshold"] = overall["phi_h"] / overall["pi_h"]
    return overall.loc[
        :,
        [
            "horizon_label",
            "q_h",
            "pi_h",
            "phi_h",
            "incidence_gain_per_unit_dpi",
            "timing_gain_per_unit_dphi",
            "equal_delta_priority",
            "equal_delta_cost_ratio_threshold",
        ],
    ]


def interpretation_lines(primary: pd.DataFrame) -> list[str]:
    if primary.empty:
        return ["- No overall full-window primary-horizon rows were found."]

    lines: list[str] = []
    for row in primary.itertuples(index=False):
        if float(row.phi_h) > float(row.pi_h):
            margin_note = "phi_h exceeds pi_h"
        elif float(row.phi_h) < float(row.pi_h):
            margin_note = "pi_h exceeds phi_h"
        else:
            margin_note = "pi_h equals phi_h"
        lines.append(
            "- "
            f"{row.horizon_label}: q_h={fmt(row.q_h)}, pi_h={fmt(row.pi_h)}, phi_h={fmt(row.phi_h)}; "
            f"{margin_note}; t50={fmt(row.t50_seconds, 2)}s and t90={fmt(row.t90_seconds, 2)}s."
        )
    return lines


def build_report(frame: pd.DataFrame, input_path: Path) -> str:
    archive_name = str(frame["archive_name"].iloc[0])
    archive_revision = str(frame["archive_revision"].iloc[0])
    primary = overall_primary_table(frame)
    robustness = robustness_table(frame)
    topics = topic_table(frame)
    p2 = p2_priority_table(frame)

    parts = [
        "# Flagship Analysis Execution Report",
        "",
        f"Generated at: {datetime.now(UTC).isoformat()}",
        f"Archive: {archive_name}",
        f"Archive revision: {archive_revision}",
        f"Input control panel: {input_path}",
        "",
        "## H1-H2 Control Panel",
        "",
        markdown_table(
            primary,
            [
                ("horizon_label", "Horizon"),
                ("n_parents", "Parents"),
                ("n_riskset", "Risk set"),
                ("p_obs", "p_obs"),
                ("pi_h", "pi_h"),
                ("phi_h", "phi_h"),
                ("q_h", "q_h"),
                ("t50_seconds", "t50_s"),
                ("t90_seconds", "t90_s"),
                ("reply_le_30s_given_reply", "Pr(T<=30s|reply)"),
                ("reply_le_5m_given_reply", "Pr(T<=5m|reply)"),
            ],
        ),
        "",
        *interpretation_lines(primary),
        "",
        "## Gap And Window Robustness",
        "",
        markdown_table(
            robustness,
            [
                ("window_variant", "Window"),
                ("horizon_label", "Horizon"),
                ("pi_h", "pi_h"),
                ("phi_h", "phi_h"),
                ("q_h", "q_h"),
                ("benchmark_flag", "Benchmark"),
            ],
        ),
        "",
        "## H4 Topic Heterogeneity",
        "",
        markdown_table(
            topics,
            [
                ("topic_category", "Topic"),
                ("horizon_label", "Horizon"),
                ("n_riskset", "Risk set"),
                ("pi_h", "pi_h"),
                ("phi_h", "phi_h"),
                ("q_h", "q_h"),
                ("t90_seconds", "t90_s"),
            ],
        )
        if not topics.empty
        else "_No topic-category rows were available in the supplied control panel._",
        "",
        "## P1-P2 Local Gain Readout",
        "",
        markdown_table(
            p2,
            [
                ("horizon_label", "Horizon"),
                ("q_h", "q_h"),
                ("pi_h", "pi_h"),
                ("phi_h", "phi_h"),
                ("incidence_gain_per_unit_dpi", "dQ/dPi"),
                ("timing_gain_per_unit_dphi", "dQ/dPhi"),
                ("equal_delta_priority", "Equal-delta priority"),
                ("equal_delta_cost_ratio_threshold", "Cost ratio threshold"),
            ],
        ),
        "",
        "The P2 rows above report the exact one-margin gain multipliers implied by the observed control panel.",
        "A numeric cost-normalized priority comparison still requires an explicit external assumptions sheet for perturbation sizes and intervention costs.",
        "",
        "## H3-H5 Pending Sections",
        "",
        "Geometry, horizon-standardization diagnostics, and periodicity notes are appended by `analysis/incidence_horizon_standardization.py`.",
    ]
    return "\n".join(parts).rstrip() + "\n"


def main() -> None:
    args = parse_args()
    frame = load_control_panel(args.control_panel)
    report = build_report(frame, args.control_panel)
    ensure_parent(args.out)
    args.out.write_text(report, encoding="utf-8")


if __name__ == "__main__":
    main()
