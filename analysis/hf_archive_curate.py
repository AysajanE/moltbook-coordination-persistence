#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

PRIMARY_KEY_OPTIONS: dict[str, list[list[str]]] = {
    "comments": [["comment_id"]],
    "posts": [["thread_id"]],
    "agents": [["author_id"]],
    "submolts": [["community_id"], ["community_label"]],
    "snapshots": [["snapshot_timestamp_utc"]],
    "word_frequency": [["word", "hour_utc", "source_snapshot_id"]],
}
WINDOW_COLUMNS = {
    "comments": "created_at_utc",
    "posts": "post_created_at_utc",
    "snapshots": "snapshot_timestamp_utc",
    "word_frequency": "hour_utc",
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _load_json_document(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise SystemExit(f"Expected JSON object in {path}")
    return payload


def _resolve_raw_files(raw_manifest: dict[str, Any], subset: str) -> list[Path]:
    subset_payload = raw_manifest.get("subsets", {}).get(subset, {})
    paths: list[Path] = []
    for container_name in ("splits", "files"):
        container = subset_payload.get(container_name, {})
        if not isinstance(container, dict):
            continue
        for file_payload in container.values():
            if not isinstance(file_payload, dict):
                continue
            raw_path = file_payload.get("path")
            if isinstance(raw_path, str) and raw_path.strip():
                paths.append(Path(raw_path))
    return sorted(set(paths))


def _load_frame(paths: list[Path]) -> pd.DataFrame:
    if not paths:
        return pd.DataFrame()
    # Raw archive parquet files can drift across snapshots (for example:
    # string vs large_string or string vs int64 for timestamp-like fields).
    # Loading to pandas per file and concatenating there keeps all rows while
    # deferring canonical type coercion to the subset mapper.
    frames = [pq.read_table(path).to_pandas() for path in paths]
    return pd.concat(frames, ignore_index=True, sort=False)


def _coerce_timestamp(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce", utc=True)


def _field_map(crosswalk: dict[str, Any], subset: str) -> dict[str, str]:
    table_payload = crosswalk.get("tables", {}).get(subset, {})
    fields = table_payload.get("fields", {})
    if not isinstance(fields, dict):
        return {}
    mapping: dict[str, str] = {}
    for canonical_name, field_payload in fields.items():
        if not isinstance(field_payload, dict):
            continue
        mapped_to = field_payload.get("mapped_to")
        if isinstance(mapped_to, str) and mapped_to.strip():
            mapping[str(canonical_name)] = mapped_to
    return mapping


def _canonicalize_subset(df: pd.DataFrame, subset: str, mapping: dict[str, str]) -> pd.DataFrame:
    out = pd.DataFrame()
    for canonical_name, raw_name in mapping.items():
        if raw_name in df.columns:
            out[canonical_name] = df[raw_name]

    for column in [name for name in out.columns if name.endswith("_utc")]:
        out[column] = _coerce_timestamp(out[column])

    if "source_snapshot_id" in out.columns:
        out["source_snapshot_id"] = out["source_snapshot_id"].astype("string")

    if subset == "comments":
        out["parent_comment_id"] = out.get("parent_comment_id", pd.Series(dtype="string")).astype("string")
        out["thread_id"] = out.get("thread_id", pd.Series(dtype="string")).astype("string")
        out["comment_id"] = out.get("comment_id", pd.Series(dtype="string")).astype("string")
    if subset == "posts":
        out["thread_id"] = out.get("thread_id", pd.Series(dtype="string")).astype("string")
        out["community_label"] = out.get("community_label", pd.Series(dtype="string")).astype("string")
    if subset == "agents":
        out["author_id"] = out.get("author_id", pd.Series(dtype="string")).astype("string")
    if subset == "submolts":
        if "community_id" in out.columns:
            out["community_id"] = out["community_id"].astype("string")
        if "community_label" in out.columns:
            out["community_label"] = out["community_label"].astype("string")

    return out


def _primary_keys(subset: str, df: pd.DataFrame) -> list[str]:
    for option in PRIMARY_KEY_OPTIONS.get(subset, []):
        if all(column in df.columns for column in option):
            return option
    raise SystemExit(f"Unable to determine primary keys for subset={subset}")


def _row_signature(row: pd.Series) -> str:
    payload = {}
    for key, value in row.to_dict().items():
        if hasattr(value, "isoformat"):
            payload[key] = value.isoformat()
        elif pd.isna(value):
            payload[key] = None
        else:
            payload[key] = value
    return json.dumps(payload, sort_keys=True)


def _dedupe_latest_state(df: pd.DataFrame, subset: str) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    if df.empty:
        return df, []

    primary_keys = _primary_keys(subset, df)
    work = df.copy()
    work["_row_order"] = range(len(work))

    conflict_rows: list[dict[str, Any]] = []
    if "source_snapshot_id" in work.columns:
        work["_source_snapshot_rank"] = pd.to_datetime(
            work["source_snapshot_id"],
            errors="coerce",
            utc=True,
        )
        grouped = work.groupby(primary_keys + ["source_snapshot_id"], dropna=False)
        for key, group in grouped:
            if len(group) < 2:
                continue
            if len({_row_signature(row) for _, row in group.iterrows()}) > 1:
                key_values = key if isinstance(key, tuple) else (key,)
                conflict_rows.append(
                    {
                        "subset": subset,
                        "primary_keys": "|".join(primary_keys),
                        "primary_key_value": "|".join("" if pd.isna(value) else str(value) for value in key_values[: len(primary_keys)]),
                        "source_snapshot_id": key_values[-1],
                        "conflict_type": "same_snapshot_conflict",
                        "rows_in_conflict": int(len(group)),
                    }
                )
        work.sort_values(primary_keys + ["_source_snapshot_rank", "_row_order"], inplace=True)
    else:
        work.sort_values(primary_keys + ["_row_order"], inplace=True)

    deduped = work.groupby(primary_keys, dropna=False, as_index=False).tail(1).copy()
    deduped.drop(columns=[column for column in ["_row_order", "_source_snapshot_rank"] if column in deduped.columns], inplace=True)
    return deduped, conflict_rows


def _filter_window(df: pd.DataFrame, subset: str, start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    column = WINDOW_COLUMNS.get(subset)
    if column is None or column not in df.columns:
        return df
    mask = df[column].notna() & (df[column] >= start) & (df[column] < end)
    return df.loc[mask].copy()


def _write_subset(out_root: Path, subset: str, df: pd.DataFrame) -> Path:
    out_root.mkdir(parents=True, exist_ok=True)
    out_path = out_root / f"{subset}.parquet"
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, out_path)
    return out_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Consume the raw archive manifest and schema crosswalk, normalize canonical field names, "
            "deduplicate latest-state rows, and materialize a canonical freeze for a fixed UTC window."
        )
    )
    parser.add_argument("--raw-manifest", type=Path, required=True, help="Path to manifests/<archive>_manifest.yaml.")
    parser.add_argument("--schema-crosswalk", type=Path, required=True, help="Path to manifests/schema_crosswalk.yaml.")
    parser.add_argument("--archive-name", default="simulamet", help="Archive label for manifests and reports.")
    parser.add_argument("--out-root", type=Path, required=True, help="Freeze output root (for example: frozen/simulamet_firstweek_lateststate).")
    parser.add_argument("--window-start", required=True, help="Inclusive UTC window start.")
    parser.add_argument("--window-end", required=True, help="Exclusive UTC window end.")
    parser.add_argument(
        "--dedup-conflicts-out",
        type=Path,
        required=True,
        help="Output CSV path for deduplication conflicts.",
    )
    parser.add_argument(
        "--freeze-manifest-out",
        type=Path,
        default=None,
        help="Optional JSON freeze manifest path.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    raw_manifest = _load_json_document(args.raw_manifest)
    crosswalk = _load_json_document(args.schema_crosswalk)
    window_start = pd.to_datetime(args.window_start, errors="raise", utc=True)
    window_end = pd.to_datetime(args.window_end, errors="raise", utc=True)

    dedup_conflicts: list[dict[str, Any]] = []
    freeze_counts: dict[str, int] = {}
    referenced_author_ids: set[str] = set()
    referenced_communities: set[str] = set()

    subset_frames: dict[str, pd.DataFrame] = {}
    for subset in sorted(crosswalk.get("tables", {})):
        raw_files = _resolve_raw_files(raw_manifest, subset)
        frame = _load_frame(raw_files)
        mapping = _field_map(crosswalk, subset)
        canonical = _canonicalize_subset(frame, subset, mapping)
        canonical, subset_conflicts = _dedupe_latest_state(canonical, subset)
        dedup_conflicts.extend(subset_conflicts)
        subset_frames[subset] = canonical

    posts = _filter_window(subset_frames.get("posts", pd.DataFrame()), "posts", window_start, window_end)
    comments = _filter_window(subset_frames.get("comments", pd.DataFrame()), "comments", window_start, window_end)
    snapshots = _filter_window(subset_frames.get("snapshots", pd.DataFrame()), "snapshots", window_start, window_end)
    word_frequency = _filter_window(
        subset_frames.get("word_frequency", pd.DataFrame()),
        "word_frequency",
        window_start,
        window_end,
    )

    if not comments.empty and "author_id" in comments.columns:
        referenced_author_ids.update(str(value) for value in comments["author_id"].dropna().astype("string"))
    if not posts.empty:
        if "post_author_id" in posts.columns:
            referenced_author_ids.update(str(value) for value in posts["post_author_id"].dropna().astype("string"))
        if "community_label" in posts.columns:
            referenced_communities.update(str(value) for value in posts["community_label"].dropna().astype("string"))

    agents = subset_frames.get("agents", pd.DataFrame()).copy()
    if not agents.empty and referenced_author_ids and "author_id" in agents.columns:
        agents = agents.loc[agents["author_id"].astype("string").isin(referenced_author_ids)].copy()

    submolts = subset_frames.get("submolts", pd.DataFrame()).copy()
    if not submolts.empty and referenced_communities:
        label_column = "community_label" if "community_label" in submolts.columns else "community_id"
        submolts = submolts.loc[submolts[label_column].astype("string").isin(referenced_communities)].copy()

    freeze_outputs = {
        "agents": agents,
        "comments": comments,
        "posts": posts,
        "snapshots": snapshots,
        "submolts": submolts,
        "word_frequency": word_frequency,
    }

    for subset, frame in freeze_outputs.items():
        _write_subset(args.out_root, subset, frame)
        freeze_counts[subset] = int(frame.shape[0])

    args.dedup_conflicts_out.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(dedup_conflicts).to_csv(args.dedup_conflicts_out, index=False)

    if args.freeze_manifest_out is not None:
        payload = {
            "schema_version": "hf_archive_freeze_manifest.v2",
            "archive_name": str(args.archive_name),
            "generated_at_utc": _utc_now_iso(),
            "raw_manifest_path": str(args.raw_manifest),
            "schema_crosswalk_path": str(args.schema_crosswalk),
            "freeze_root": str(args.out_root),
            "window_start": window_start.isoformat(),
            "window_end": window_end.isoformat(),
            "row_counts": freeze_counts,
            "dedup_conflicts_path": str(args.dedup_conflicts_out),
        }
        args.freeze_manifest_out.parent.mkdir(parents=True, exist_ok=True)
        args.freeze_manifest_out.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
