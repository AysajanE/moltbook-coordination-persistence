#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

DEFAULT_DATASET = "SimulaMet/moltbook-observatory-archive"
DEFAULT_SIMULAMET_SUBSETS = [
    "agents",
    "comments",
    "posts",
    "snapshots",
    "submolts",
    "word_frequency",
]
DEFAULT_LICENSE_HINTS = {
    "simulamet": "MIT",
    "moltnet": "CC-BY-4.0",
}
RAW_HASH_SCHEMA = pa.schema(
    [
        ("archive_name", pa.string()),
        ("snapshot_id", pa.string()),
        ("subset", pa.string()),
        ("split", pa.string()),
        ("raw_path", pa.string()),
        ("sha256", pa.string()),
        ("size_bytes", pa.int64()),
    ]
)


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _utc_today() -> str:
    return datetime.now(UTC).date().isoformat()


def _default_archive_name(dataset: str) -> str:
    if dataset == DEFAULT_DATASET:
        return "simulamet"
    tail = dataset.rsplit("/", 1)[-1].strip().lower()
    return tail.replace("-", "_") or "archive"


def _default_manifest_path(archive_name: str) -> Path:
    return Path("manifests") / f"{archive_name}_manifest.yaml"


def _default_hash_path(archive_name: str) -> Path:
    if archive_name == "simulamet":
        return Path("restricted") / "raw_to_hash_mapping.parquet"
    return Path("restricted") / f"{archive_name}_raw_to_hash_mapping.parquet"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Download a public Hugging Face archive into the repo's canonical raw zone, "
            "capture immutable provenance, and emit a tracked archive manifest plus a "
            "restricted raw-to-hash mapping."
        )
    )
    parser.add_argument(
        "--dataset",
        default=DEFAULT_DATASET,
        help=f'Hugging Face dataset name (default: "{DEFAULT_DATASET}")',
    )
    parser.add_argument(
        "--archive-name",
        default=None,
        help="Logical archive name used in output paths and manifests (default: derived from dataset).",
    )
    parser.add_argument(
        "--revision",
        default="main",
        help='Dataset revision or branch to pin (default: "main").',
    )
    parser.add_argument(
        "--out-root",
        type=Path,
        default=None,
        help="Canonical raw root (for example: raw/simulamet or raw/moltnet).",
    )
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=None,
        help="Explicit snapshot directory. Deprecated alias for --out-root + --snapshot-id.",
    )
    parser.add_argument(
        "--snapshot-id",
        default=_utc_today(),
        help="Snapshot identifier under --out-root (default: today's UTC date, YYYY-MM-DD).",
    )
    parser.add_argument(
        "--manifest-out",
        type=Path,
        default=None,
        help="Tracked archive manifest path (default: manifests/<archive_name>_manifest.yaml).",
    )
    parser.add_argument(
        "--restricted-hash-out",
        type=Path,
        default=None,
        help="Restricted raw-to-hash mapping parquet path.",
    )
    parser.add_argument(
        "--subset",
        action="append",
        help="Dataset config/subset to export (repeatable). Defaults to all discovered configs.",
    )
    parser.add_argument(
        "--format",
        choices=["parquet", "csv"],
        default="parquet",
        help="Export format for raw files (default: parquet).",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional sample cap per split. Canonical acquisition rejects this unless explicitly allowlisted.",
    )
    parser.add_argument(
        "--allow-sampled-export",
        action="store_true",
        help="Allow --max-rows for smoke or sample exports. Never use for canonical acquisition.",
    )
    return parser.parse_args()


def _resolve_snapshot_dir(args: argparse.Namespace) -> Path:
    if args.out_dir is not None and args.out_root is not None:
        raise SystemExit("Specify either --out-dir or --out-root, not both.")
    if args.out_dir is not None:
        return args.out_dir
    if args.out_root is None:
        raise SystemExit("Canonical acquisition requires --out-root or --out-dir.")
    return args.out_root / str(args.snapshot_id)


def _iter_dataset_splits(dataset_obj: object) -> list[tuple[str, object]]:
    if hasattr(dataset_obj, "items"):
        return [(str(name), split) for name, split in dataset_obj.items()]
    return [("train", dataset_obj)]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_export(split: object, out_path: Path, fmt: str) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if fmt == "parquet":
        split.to_parquet(str(out_path))
        return
    if fmt == "csv":
        split.to_csv(str(out_path))
        return
    raise ValueError(f"Unsupported format: {fmt}")


def _write_yaml_document(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # JSON is valid YAML 1.2 and keeps the manifest dependency-free.
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _write_hash_mapping(path: Path, rows: list[dict[str, object]]) -> None:
    arrays = {
        "archive_name": [str(row["archive_name"]) for row in rows],
        "snapshot_id": [str(row["snapshot_id"]) for row in rows],
        "subset": [str(row["subset"]) for row in rows],
        "split": [str(row["split"]) for row in rows],
        "raw_path": [str(row["raw_path"]) for row in rows],
        "sha256": [str(row["sha256"]) for row in rows],
        "size_bytes": [int(row["size_bytes"]) for row in rows],
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.table(arrays, schema=RAW_HASH_SCHEMA)
    pq.write_table(table, path)


def _normalize_license(value: object, archive_name: str) -> str:
    if isinstance(value, list):
        cleaned = [str(item).strip() for item in value if str(item).strip()]
        if cleaned:
            return ", ".join(cleaned)
    if isinstance(value, str) and value.strip():
        return value.strip()
    hint = DEFAULT_LICENSE_HINTS.get(archive_name)
    if hint is not None:
        return hint
    raise SystemExit(f"Could not determine dataset license for archive={archive_name}.")


def _resolve_dataset_metadata(*, dataset: str, revision: str, archive_name: str) -> dict[str, str]:
    try:
        from huggingface_hub import HfApi
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Missing huggingface_hub dependency for provenance capture: {exc}") from exc

    api = HfApi()
    info = api.dataset_info(dataset, revision=revision)
    card_data = getattr(info, "cardData", None)
    if not isinstance(card_data, dict):
        card_data = getattr(info, "card_data", None)
    if not isinstance(card_data, dict):
        card_data = {}

    resolved_revision = getattr(info, "sha", None) or revision
    license_value = card_data.get("license")
    return {
        "requested_revision": revision,
        "resolved_revision": str(resolved_revision),
        "license": _normalize_license(license_value, archive_name),
    }


def _resolve_subsets(*, dataset: str, revision: str, requested: list[str] | None, archive_name: str) -> list[str]:
    if requested:
        return [item for item in requested if item]

    try:
        from datasets import get_dataset_config_names
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Missing datasets dependency for config discovery: {exc}") from exc

    try:
        discovered = [name for name in get_dataset_config_names(dataset, revision=revision) if name]
    except Exception:
        discovered = []

    if discovered:
        return discovered
    if archive_name == "simulamet":
        return list(DEFAULT_SIMULAMET_SUBSETS)
    return [""]


def main() -> None:
    args = parse_args()
    if args.max_rows is not None and not args.allow_sampled_export:
        raise SystemExit("Canonical acquisition forbids --max-rows unless --allow-sampled-export is set.")

    archive_name = (args.archive_name or _default_archive_name(args.dataset)).strip().lower()
    snapshot_dir = _resolve_snapshot_dir(args).resolve()
    manifest_out = (args.manifest_out or _default_manifest_path(archive_name)).resolve()
    restricted_hash_out = (args.restricted_hash_out or _default_hash_path(archive_name)).resolve()

    try:
        from datasets import __version__ as datasets_version
        from datasets import load_dataset
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Missing datasets dependency: {exc}") from exc

    dataset_meta = _resolve_dataset_metadata(
        dataset=args.dataset,
        revision=str(args.revision),
        archive_name=archive_name,
    )
    subset_names = _resolve_subsets(
        dataset=args.dataset,
        revision=dataset_meta["resolved_revision"],
        requested=args.subset,
        archive_name=archive_name,
    )

    export_manifest: dict[str, Any] = {
        "schema_version": "hf_archive_export_manifest.v2",
        "archive_name": archive_name,
        "dataset": args.dataset,
        "requested_revision": dataset_meta["requested_revision"],
        "resolved_revision": dataset_meta["resolved_revision"],
        "license": dataset_meta["license"],
        "exported_at_utc": _utc_now_iso(),
        "format": args.format,
        "sampled_export": args.max_rows is not None,
        "max_rows": args.max_rows,
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": str(snapshot_dir),
        "datasets_version": datasets_version,
        "subsets": {},
    }
    hash_rows: list[dict[str, object]] = []

    for subset_name in subset_names:
        config_name = subset_name or None
        dataset_obj = load_dataset(args.dataset, config_name, revision=dataset_meta["resolved_revision"])
        subset_key = subset_name or "default"
        subset_payload: dict[str, Any] = {
            "config_name": config_name,
            "splits": {},
            "rows_exported_total": 0,
        }

        for split_name, split in _iter_dataset_splits(dataset_obj):
            exported = split
            exported_rows = int(split.num_rows)
            if args.max_rows is not None:
                exported_rows = min(int(args.max_rows), int(split.num_rows))
                exported = split.select(range(exported_rows))

            out_path = snapshot_dir / subset_key / f"{split_name}.{args.format}"
            _write_export(exported, out_path, args.format)
            sha256 = _sha256_file(out_path)
            size_bytes = int(out_path.stat().st_size)

            subset_payload["splits"][split_name] = {
                "source_rows": int(split.num_rows),
                "exported_rows": exported_rows,
                "path": str(out_path),
                "sha256": sha256,
                "size_bytes": size_bytes,
            }
            subset_payload["rows_exported_total"] += exported_rows
            hash_rows.append(
                {
                    "archive_name": archive_name,
                    "snapshot_id": snapshot_dir.name,
                    "subset": subset_key,
                    "split": split_name,
                    "raw_path": str(out_path),
                    "sha256": sha256,
                    "size_bytes": size_bytes,
                }
            )

        export_manifest["subsets"][subset_key] = subset_payload

    snapshot_dir.mkdir(parents=True, exist_ok=True)
    export_manifest_path = snapshot_dir / "EXPORT_MANIFEST.json"
    export_manifest_path.write_text(json.dumps(export_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    canonical_manifest = {
        "schema_version": "hf_archive_manifest.v2",
        "archive_name": archive_name,
        "dataset": args.dataset,
        "requested_revision": dataset_meta["requested_revision"],
        "resolved_revision": dataset_meta["resolved_revision"],
        "license": dataset_meta["license"],
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": str(snapshot_dir),
        "exported_at_utc": export_manifest["exported_at_utc"],
        "sampled_export": export_manifest["sampled_export"],
        "max_rows": export_manifest["max_rows"],
        "export_manifest_path": str(export_manifest_path),
        "restricted_hash_mapping_path": str(restricted_hash_out),
        "subsets": export_manifest["subsets"],
    }

    _write_yaml_document(manifest_out, canonical_manifest)
    _write_hash_mapping(restricted_hash_out, hash_rows)

    print(
        json.dumps(
            {
                "archive_name": archive_name,
                "snapshot_dir": str(snapshot_dir),
                "manifest_out": str(manifest_out),
                "restricted_hash_out": str(restricted_hash_out),
                "subset_count": len(export_manifest["subsets"]),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
