#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow.dataset as ds
import pyarrow.parquet as pq

FIELD_SPECS: dict[str, list[dict[str, object]]] = {
    "comments": [
        {"canonical_name": "comment_id", "aliases": ["id", "comment_id"], "required": True},
        {"canonical_name": "thread_id", "aliases": ["post_id", "thread_id"], "required": True},
        {"canonical_name": "parent_comment_id", "aliases": ["parent_id", "parent_comment_id"], "required": True},
        {"canonical_name": "author_id", "aliases": ["author_id", "agent_id"], "required": True},
        {"canonical_name": "created_at_utc", "aliases": ["created_at_utc", "created_at"], "required": True},
        {"canonical_name": "score_snapshot", "aliases": ["score"], "required": False},
        {"canonical_name": "source_snapshot_id", "aliases": ["dump_date"], "required": True},
    ],
    "posts": [
        {"canonical_name": "thread_id", "aliases": ["id", "thread_id"], "required": True},
        {"canonical_name": "post_author_id", "aliases": ["agent_id", "author_id", "post_author_id"], "required": True},
        {"canonical_name": "community_label", "aliases": ["submolt", "submolt_name", "community_label"], "required": True},
        {"canonical_name": "post_created_at_utc", "aliases": ["created_at_utc", "created_at"], "required": True},
        {"canonical_name": "post_score", "aliases": ["score"], "required": False},
        {"canonical_name": "post_comment_count", "aliases": ["comment_count"], "required": False},
        {"canonical_name": "source_snapshot_id", "aliases": ["dump_date"], "required": True},
    ],
    "agents": [
        {"canonical_name": "author_id", "aliases": ["id", "author_id"], "required": True},
        {"canonical_name": "author_name", "aliases": ["name", "author_name"], "required": False},
        {"canonical_name": "claimed_status_raw", "aliases": ["is_claimed", "claimed_status_raw"], "required": False},
        {"canonical_name": "karma", "aliases": ["karma"], "required": False},
        {"canonical_name": "follower_count", "aliases": ["follower_count"], "required": False},
        {"canonical_name": "following_count", "aliases": ["following_count"], "required": False},
        {"canonical_name": "owner_x_handle", "aliases": ["owner_x_handle"], "required": False},
        {"canonical_name": "first_seen_at_utc", "aliases": ["first_seen_at_utc", "first_seen_at"], "required": False},
        {"canonical_name": "last_seen_at_utc", "aliases": ["last_seen_at_utc", "last_seen_at"], "required": False},
        {"canonical_name": "created_at_utc", "aliases": ["created_at_utc", "created_at"], "required": False},
        {"canonical_name": "source_snapshot_id", "aliases": ["dump_date"], "required": True},
    ],
    "submolts": [
        {"canonical_name": "community_id", "aliases": ["id", "community_id"], "required": False},
        {"canonical_name": "community_label", "aliases": ["name", "community_label"], "required": True},
        {"canonical_name": "community_description", "aliases": ["description", "community_description"], "required": False},
        {"canonical_name": "subscriber_count", "aliases": ["subscriber_count"], "required": False},
        {"canonical_name": "source_snapshot_id", "aliases": ["dump_date"], "required": True},
    ],
    "snapshots": [
        {"canonical_name": "snapshot_timestamp_utc", "aliases": ["timestamp", "snapshot_timestamp_utc"], "required": True},
        {"canonical_name": "total_agents", "aliases": ["total_agents"], "required": False},
        {"canonical_name": "active_agents_24h", "aliases": ["active_agents_24h"], "required": False},
        {"canonical_name": "source_snapshot_id", "aliases": ["dump_date"], "required": True},
    ],
    "word_frequency": [
        {"canonical_name": "word", "aliases": ["word"], "required": True},
        {"canonical_name": "hour_utc", "aliases": ["hour", "hour_utc"], "required": True},
        {"canonical_name": "count", "aliases": ["count"], "required": True},
        {"canonical_name": "source_snapshot_id", "aliases": ["dump_date"], "required": True},
    ],
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _load_json_document(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"Expected JSON object in {path}")
    return payload


def _write_yaml_document(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _resolve_subset_paths(raw_manifest: dict[str, Any], subset: str) -> list[Path]:
    subset_payload = raw_manifest.get("subsets", {}).get(subset, {})
    splits = subset_payload.get("splits", {})
    if not isinstance(splits, dict):
        return []
    out: list[Path] = []
    for split_payload in splits.values():
        if not isinstance(split_payload, dict):
            continue
        raw_path = split_payload.get("path")
        if isinstance(raw_path, str) and raw_path.strip():
            out.append(Path(raw_path))
    return out


def _read_subset_table(paths: list[Path]):
    if not paths:
        return None
    if len(paths) == 1:
        return pq.read_table(paths[0])
    return ds.dataset([str(path) for path in paths], format="parquet").to_table()


def _normalize_columns(columns: list[str]) -> dict[str, str]:
    return {column.lower(): column for column in columns}


def _pick_column(normalized: dict[str, str], aliases: list[str]) -> tuple[str | None, list[str]]:
    matches: list[str] = []
    for alias in aliases:
        found = normalized.get(alias.lower())
        if found is not None:
            matches.append(found)
    if not matches:
        return None, []
    return matches[0], matches


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Read a canonical HF raw-export manifest, build the machine-readable schema crosswalk, "
            "and emit field-validation and missingness audits for downstream freeze construction."
        )
    )
    parser.add_argument("--raw-manifest", type=Path, required=True, help="Path to manifests/<archive>_manifest.yaml.")
    parser.add_argument("--archive-name", default="simulamet", help="Archive name for labels and output defaults.")
    parser.add_argument("--out-crosswalk", type=Path, required=True, help="Output path for manifests/schema_crosswalk.yaml.")
    parser.add_argument(
        "--out-field-validation",
        type=Path,
        required=True,
        help="Output path for qc/field_validation_<archive>.csv.",
    )
    parser.add_argument(
        "--out-missingness",
        type=Path,
        required=True,
        help="Output path for qc/missingness_<archive>.csv.",
    )
    parser.add_argument("--subset", action="append", help="Subset(s) to audit. Defaults to all known tables.")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_manifest = _load_json_document(args.raw_manifest)
    subsets = args.subset or sorted(FIELD_SPECS)

    crosswalk: dict[str, Any] = {
        "schema_version": "hf_archive_schema_crosswalk.v2",
        "archive_name": str(args.archive_name),
        "generated_at_utc": _utc_now_iso(),
        "raw_manifest_path": str(args.raw_manifest),
        "raw_snapshot_id": raw_manifest.get("snapshot_id"),
        "tables": {},
    }
    field_rows: list[dict[str, Any]] = []
    missingness_rows: list[dict[str, Any]] = []

    for subset in subsets:
        spec_rows = FIELD_SPECS.get(subset)
        if spec_rows is None:
            raise SystemExit(f"Unknown subset for schema audit: {subset}")

        raw_paths = _resolve_subset_paths(raw_manifest, subset)
        if not raw_paths:
            crosswalk["tables"][subset] = {
                "status": "missing_raw_files",
                "raw_files": [],
                "fields": {},
            }
            for spec in spec_rows:
                canonical_name = str(spec["canonical_name"])
                field_rows.append(
                    {
                        "subset": subset,
                        "canonical_field": canonical_name,
                        "required": bool(spec["required"]),
                        "mapped_to": None,
                        "status": "missing_raw_files",
                        "aliases": "|".join(str(item) for item in spec["aliases"]),
                        "matches": "",
                        "total_rows": 0,
                    }
                )
                missingness_rows.append(
                    {
                        "subset": subset,
                        "canonical_field": canonical_name,
                        "mapped_to": None,
                        "required": bool(spec["required"]),
                        "total_rows": 0,
                        "nonnull_rows": None,
                        "missing_rows": None,
                        "missing_rate": None,
                        "status": "missing_raw_files",
                    }
                )
            continue

        table = _read_subset_table(raw_paths)
        if table is None:
            raise SystemExit(f"Failed to read raw files for subset={subset}")
        columns = list(table.column_names)
        normalized = _normalize_columns(columns)

        table_payload: dict[str, Any] = {
            "status": "ok",
            "raw_files": [str(path) for path in raw_paths],
            "rows": int(table.num_rows),
            "columns": columns,
            "fields": {},
        }

        for spec in spec_rows:
            canonical_name = str(spec["canonical_name"])
            aliases = [str(item) for item in spec["aliases"]]
            required = bool(spec["required"])
            mapped_to, matches = _pick_column(normalized, aliases)
            status = "found" if mapped_to is not None else ("missing_required" if required else "missing_optional")
            nonnull_rows: int | None = None
            missing_rows: int | None = None
            missing_rate: float | None = None
            if mapped_to is not None:
                column = table[mapped_to]
                nonnull_rows = int(table.num_rows - column.null_count)
                missing_rows = int(column.null_count)
                missing_rate = (missing_rows / int(table.num_rows)) if int(table.num_rows) else None

            table_payload["fields"][canonical_name] = {
                "required": required,
                "aliases": aliases,
                "mapped_to": mapped_to,
                "matches": matches,
                "status": status,
                "nonnull_rows": nonnull_rows,
                "missing_rows": missing_rows,
                "missing_rate": missing_rate,
            }
            field_rows.append(
                {
                    "subset": subset,
                    "canonical_field": canonical_name,
                    "required": required,
                    "mapped_to": mapped_to,
                    "status": status,
                    "aliases": "|".join(aliases),
                    "matches": "|".join(matches),
                    "total_rows": int(table.num_rows),
                }
            )
            missingness_rows.append(
                {
                    "subset": subset,
                    "canonical_field": canonical_name,
                    "mapped_to": mapped_to,
                    "required": required,
                    "total_rows": int(table.num_rows),
                    "nonnull_rows": nonnull_rows,
                    "missing_rows": missing_rows,
                    "missing_rate": missing_rate,
                    "status": status,
                }
            )

        crosswalk["tables"][subset] = table_payload

    _write_yaml_document(args.out_crosswalk, crosswalk)
    args.out_field_validation.parent.mkdir(parents=True, exist_ok=True)
    args.out_missingness.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(field_rows).sort_values(["subset", "canonical_field"]).to_csv(
        args.out_field_validation,
        index=False,
    )
    pd.DataFrame(missingness_rows).sort_values(["subset", "canonical_field"]).to_csv(
        args.out_missingness,
        index=False,
    )


if __name__ == "__main__":
    main()
