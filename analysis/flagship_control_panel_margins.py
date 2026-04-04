#!/usr/bin/env python3
"""Compute Stage 3 control-panel margins on common risk sets.

This script publishes the canonical Moltbook control panel
(`q_h`, `pi_h`, `phi_h`) for the approved primary horizons using the exact
risk-set definition from `paper/sections/methods.tex`.
"""

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

DEFAULT_HORIZONS = ((300, "5m"), (3600, "1h"))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Compute Stage 3 control-panel margins for the canonical Moltbook run."
    )
    parser.add_argument("--survival-path", type=Path, required=True)
    parser.add_argument("--raw-agents-path", type=Path, required=True)
    parser.add_argument("--out-csv", type=Path, default=None)
    parser.add_argument("--out-summary", type=Path, default=None)
    return parser.parse_args()


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def infer_run_id(survival_path: Path) -> str:
    return survival_path.parent.name


def default_output_paths(survival_path: Path) -> tuple[Path, Path]:
    run_id = infer_run_id(survival_path)
    base = Path("outputs/analysis")
    csv_path = base / f"moltbook_control_panel_margins_{run_id}.csv"
    summary_path = base / f"moltbook_control_panel_margins_{run_id}.json"
    return csv_path, summary_path


def load_survival(path: Path) -> pd.DataFrame:
    df = pd.read_parquet(path).copy()
    required = {
        "comment_agent_id",
        "submolt_category",
        "created_at_utc",
        "first_reply_at",
        "event_observed",
        "duration_hours",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Missing required survival columns: {sorted(missing)}")

    df["created_at_utc"] = pd.to_datetime(df["created_at_utc"], utc=True)
    df["first_reply_at"] = pd.to_datetime(df["first_reply_at"], utc=True, errors="coerce")
    df["event_observed"] = (
        pd.to_numeric(df["event_observed"], errors="coerce").fillna(0).astype(int)
    )
    df["duration_hours"] = pd.to_numeric(df["duration_hours"], errors="coerce")
    df["reply_seconds"] = np.where(
        df["event_observed"].astype(bool),
        (df["first_reply_at"] - df["created_at_utc"]).dt.total_seconds(),
        np.nan,
    )
    df["followup_seconds"] = df["duration_hours"] * 3600.0
    return df


def attach_claim_status(survival: pd.DataFrame, raw_agents_path: Path) -> pd.DataFrame:
    agents = pd.read_parquet(
        raw_agents_path,
        columns=["id", "is_claimed", "dump_date"],
    ).copy()
    agents["dump_date"] = pd.to_datetime(agents["dump_date"], errors="coerce")
    agents = (
        agents.sort_values(["id", "dump_date"], kind="stable")
        .drop_duplicates("id", keep="last")
    )
    claims = agents.rename(columns={"id": "comment_agent_id"})[["comment_agent_id", "is_claimed"]]

    out = survival.merge(claims, on="comment_agent_id", how="left", validate="many_to_one")
    out["claimed_group"] = "Unknown"
    out.loc[out["is_claimed"] == 1, "claimed_group"] = "Claimed"
    out.loc[out["is_claimed"] == 0, "claimed_group"] = "Unclaimed"
    return out


def summarize_group(
    df: pd.DataFrame,
    group_family: str,
    group_label: str,
    source_run_id: str,
    survival_path: Path,
    raw_agents_path: Path,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    event = df["event_observed"].astype(bool)
    reply_seconds = pd.to_numeric(df["reply_seconds"], errors="coerce")
    followup_seconds = pd.to_numeric(df["followup_seconds"], errors="coerce")
    n_parents = int(len(df))
    n_reply_any = int(event.sum())
    p_obs = float(event.mean()) if n_parents else np.nan

    for horizon_seconds, horizon_label in DEFAULT_HORIZONS:
        risk_set = (followup_seconds >= horizon_seconds) | (
            event & (reply_seconds <= horizon_seconds)
        )
        replied_in_risk = event & risk_set
        replied_within_horizon = event & (reply_seconds <= horizon_seconds) & risk_set

        risk_set_n = int(risk_set.sum())
        replied_in_risk_n = int(replied_in_risk.sum())
        replied_within_horizon_n = int(replied_within_horizon.sum())

        if risk_set_n == 0:
            q_hat = np.nan
            pi_hat = np.nan
        else:
            q_hat = float(replied_within_horizon_n) / float(risk_set_n)
            pi_hat = float(replied_in_risk_n) / float(risk_set_n)

        if replied_in_risk_n == 0:
            phi_hat = np.nan
        else:
            phi_hat = float(replied_within_horizon_n) / float(replied_in_risk_n)

        identity_abs_error = (
            np.nan
            if np.isnan(q_hat) or np.isnan(pi_hat) or np.isnan(phi_hat)
            else abs(q_hat - (pi_hat * phi_hat))
        )

        rows.append(
            {
                "group_family": group_family,
                "group_label": group_label,
                "horizon_seconds": horizon_seconds,
                "horizon_label": horizon_label,
                "n_parents": n_parents,
                "n_reply_any": n_reply_any,
                "p_obs_any_reply_prob": p_obs,
                "risk_set_n": risk_set_n,
                "replied_in_risk_n": replied_in_risk_n,
                "replied_within_horizon_n": replied_within_horizon_n,
                "q_h_hat": q_hat,
                "pi_h_hat": pi_hat,
                "phi_h_hat": phi_hat,
                "q_equals_pi_phi_abs_error": identity_abs_error,
                "source_run_id": source_run_id,
                "input_survival_path": str(survival_path),
                "input_raw_agents_path": str(raw_agents_path),
            }
        )

    return rows


def main() -> None:
    args = parse_args()
    out_csv, out_summary = default_output_paths(args.survival_path)
    if args.out_csv is not None:
        out_csv = args.out_csv
    if args.out_summary is not None:
        out_summary = args.out_summary

    source_run_id = infer_run_id(args.survival_path)
    survival = load_survival(args.survival_path)
    survival = attach_claim_status(survival, raw_agents_path=args.raw_agents_path)

    rows: list[dict[str, Any]] = []
    rows.extend(
        summarize_group(
            survival,
            group_family="overall",
            group_label="Overall",
            source_run_id=source_run_id,
            survival_path=args.survival_path,
            raw_agents_path=args.raw_agents_path,
        )
    )

    claimable = survival[survival["claimed_group"].isin(["Claimed", "Unclaimed"])].copy()
    for label in ["Claimed", "Unclaimed"]:
        subset = claimable[claimable["claimed_group"] == label].copy()
        rows.extend(
            summarize_group(
                subset,
                group_family="claimed_status",
                group_label=label,
                source_run_id=source_run_id,
                survival_path=args.survival_path,
                raw_agents_path=args.raw_agents_path,
            )
        )

    for label, subset in survival.groupby("submolt_category", sort=True):
        rows.extend(
            summarize_group(
                subset.copy(),
                group_family="submolt_category",
                group_label=str(label),
                source_run_id=source_run_id,
                survival_path=args.survival_path,
                raw_agents_path=args.raw_agents_path,
            )
        )

    out_df = pd.DataFrame(rows)
    order = {"overall": 0, "claimed_status": 1, "submolt_category": 2}
    out_df["__order"] = out_df["group_family"].map(order).fillna(99).astype(int)
    out_df = out_df.sort_values(
        ["__order", "group_label", "horizon_seconds"],
        kind="stable",
    ).drop(columns="__order")

    ensure_parent(out_csv)
    out_df.to_csv(out_csv, index=False)

    summary = {
        "generated_at_utc": datetime.now(UTC).isoformat(),
        "survival_input_path": str(args.survival_path),
        "raw_agents_input_path": str(args.raw_agents_path),
        "source_run_id": source_run_id,
        "horizons_seconds": [h for h, _ in DEFAULT_HORIZONS],
        "output_table_csv": str(out_csv),
    }
    ensure_parent(out_summary)
    out_summary.write_text(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
