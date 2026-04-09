#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
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
    out: list[Path] = []
    if isinstance(splits, dict):
        for split_payload in splits.values():
            if not isinstance(split_payload, dict):
                continue
            raw_path = split_payload.get("path")
            if isinstance(raw_path, str) and raw_path.strip():
                out.append(Path(raw_path))

    files = subset_payload.get("files", {})
    if isinstance(files, dict):
        for file_payload in files.values():
            if not isinstance(file_payload, dict):
                continue
            raw_path = file_payload.get("path")
            if isinstance(raw_path, str) and raw_path.strip():
                out.append(Path(raw_path))

    return sorted(set(out))


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


def _load_file_infos(paths: list[Path]) -> list[dict[str, Any]]:
    infos: list[dict[str, Any]] = []
    for path in paths:
        parquet_file = pq.ParquetFile(path)
        schema = parquet_file.schema_arrow
        columns = list(schema.names)
        infos.append(
            {
                "path": path,
                "rows": int(parquet_file.metadata.num_rows),
                "columns": columns,
                "normalized": _normalize_columns(columns),
                "types": {field.name: str(field.type) for field in schema},
            }
        )
    return infos


def _union_columns(file_infos: list[dict[str, Any]]) -> list[str]:
    seen: set[str] = set()
    ordered: list[str] = []
    for info in file_infos:
        for column in info["columns"]:
            if column in seen:
                continue
            seen.add(column)
            ordered.append(column)
    return ordered


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

        file_infos = _load_file_infos(raw_paths)
        if not file_infos:
            raise SystemExit(f"Failed to read raw files for subset={subset}")
        columns = _union_columns(file_infos)
        normalized = _normalize_columns(columns)
        total_rows = sum(int(info["rows"]) for info in file_infos)
        file_count = len(file_infos)

        spec_state: dict[str, dict[str, Any]] = {}
        for spec in spec_rows:
            canonical_name = str(spec["canonical_name"])
            aliases = [str(item) for item in spec["aliases"]]
            required = bool(spec["required"])
            mapped_to, matches = _pick_column(normalized, aliases)
            spec_state[canonical_name] = {
                "aliases": aliases,
                "required": required,
                "mapped_to": mapped_to,
                "matches": matches,
                "nonnull_rows": 0,
                "files_with_match": 0,
                "observed_types": {},
            }

        for info in file_infos:
            file_columns: set[str] = set()
            file_matches: dict[str, str] = {}
            for canonical_name, state in spec_state.items():
                file_match, _ = _pick_column(info["normalized"], state["aliases"])
                if file_match is None:
                    continue
                file_columns.add(file_match)
                file_matches[canonical_name] = file_match
                state["files_with_match"] += 1
                state["observed_types"].setdefault(file_match, set()).add(info["types"][file_match])

            file_table = pq.read_table(info["path"], columns=sorted(file_columns)) if file_columns else None
            for canonical_name, file_match in file_matches.items():
                state = spec_state[canonical_name]
                assert file_table is not None
                state["nonnull_rows"] += int(info["rows"] - file_table[file_match].null_count)

        table_payload: dict[str, Any] = {
            "status": "ok",
            "raw_files": [str(path) for path in raw_paths],
            "file_count": file_count,
            "rows": total_rows,
            "columns": columns,
            "fields": {},
        }

        for spec in spec_rows:
            canonical_name = str(spec["canonical_name"])
            state = spec_state[canonical_name]
            aliases = state["aliases"]
            required = state["required"]
            mapped_to = state["mapped_to"]
            matches = state["matches"]
            status = "found" if mapped_to is not None else ("missing_required" if required else "missing_optional")
            nonnull_rows: int | None = None
            missing_rows: int | None = None
            missing_rate: float | None = None
            files_with_match = int(state["files_with_match"])
            observed_types = {
                raw_name: sorted(type_names) for raw_name, type_names in state["observed_types"].items()
            }
            if mapped_to is not None:
                nonnull_rows = int(state["nonnull_rows"])
                missing_rows = int(total_rows - nonnull_rows)
                missing_rate = (missing_rows / total_rows) if total_rows else None

            table_payload["fields"][canonical_name] = {
                "required": required,
                "aliases": aliases,
                "mapped_to": mapped_to,
                "matches": matches,
                "files_with_match": files_with_match,
                "files_without_match": file_count - files_with_match,
                "observed_types": observed_types,
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
                    "files_with_match": files_with_match,
                    "file_count": file_count,
                    "total_rows": total_rows,
                }
            )
            missingness_rows.append(
                {
                    "subset": subset,
                    "canonical_field": canonical_name,
                    "mapped_to": mapped_to,
                    "required": required,
                    "files_with_match": files_with_match,
                    "file_count": file_count,
                    "total_rows": total_rows,
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
