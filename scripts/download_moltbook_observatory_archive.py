#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
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
REPO_ROOT_GROUP = "repo_root"
RAW_HASH_SCHEMA = pa.schema(
    [
        ("archive_name", pa.string()),
        ("snapshot_id", pa.string()),
        ("subset", pa.string()),
        ("split", pa.string()),
        ("repo_path", pa.string()),
        ("raw_path", pa.string()),
        ("sha256", pa.string()),
        ("size_bytes", pa.int64()),
        ("upstream_blob_id", pa.string()),
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
            "Download a public Hugging Face dataset repo snapshot into the repo's "
            "canonical raw zone, capture immutable provenance, and emit a tracked "
            "archive manifest plus a restricted raw-to-hash mapping."
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
        help=(
            "Repo data subset to acquire (repeatable). When omitted, the full dataset "
            "repo snapshot is acquired, including root metadata files."
        ),
    )
    parser.add_argument(
        "--format",
        choices=["parquet", "csv"],
        default="parquet",
        help=(
            "Deprecated export format flag. Canonical acquisition now preserves the "
            "upstream repo snapshot as-is; only the default parquet mode is supported."
        ),
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Unsupported for snapshot acquisition. Reserved for non-canonical smoke exports.",
    )
    parser.add_argument(
        "--allow-sampled-export",
        action="store_true",
        help="Accepted for compatibility, but sampled snapshot acquisition is not supported.",
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


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
        "repo_path": [str(row["repo_path"]) for row in rows],
        "raw_path": [str(row["raw_path"]) for row in rows],
        "sha256": [str(row["sha256"]) for row in rows],
        "size_bytes": [int(row["size_bytes"]) for row in rows],
        "upstream_blob_id": [str(row["upstream_blob_id"]) for row in rows],
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


def _repo_group(repo_path: str) -> str:
    parts = Path(repo_path).parts
    if len(parts) >= 3 and parts[0] == "data":
        return parts[1]
    return REPO_ROOT_GROUP


def _resolve_dataset_metadata(
    *,
    dataset: str,
    revision: str,
    archive_name: str,
) -> tuple[dict[str, str], list[dict[str, object]], str]:
    try:
        from huggingface_hub import HfApi
        from huggingface_hub import __version__ as huggingface_hub_version
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Missing huggingface_hub dependency for provenance capture: {exc}") from exc

    api = HfApi()
    info = api.dataset_info(dataset, revision=revision, files_metadata=True)
    card_data = getattr(info, "cardData", None)
    if not isinstance(card_data, dict):
        card_data = getattr(info, "card_data", None)
    if not isinstance(card_data, dict):
        card_data = {}

    resolved_revision = getattr(info, "sha", None) or revision
    license_value = card_data.get("license")
    repo_entries: list[dict[str, object]] = []
    for sibling in getattr(info, "siblings", []) or []:
        repo_path = getattr(sibling, "rfilename", None)
        if not repo_path:
            continue
        repo_entries.append(
            {
                "repo_path": str(repo_path),
                "size_bytes": int(getattr(sibling, "size", 0) or 0),
                "upstream_blob_id": str(getattr(sibling, "blob_id", "") or ""),
            }
        )

    if not repo_entries:
        raise SystemExit(f"Could not enumerate repo files for dataset={dataset}@{resolved_revision}.")

    return (
        {
            "requested_revision": revision,
            "resolved_revision": str(resolved_revision),
            "license": _normalize_license(license_value, archive_name),
        },
        repo_entries,
        huggingface_hub_version,
    )


def _resolve_subsets(
    *,
    repo_entries: list[dict[str, object]],
    requested: list[str] | None,
    archive_name: str,
) -> list[str]:
    discovered = sorted(
        {
            _repo_group(str(entry["repo_path"]))
            for entry in repo_entries
            if _repo_group(str(entry["repo_path"])) != REPO_ROOT_GROUP
        }
    )
    if requested:
        cleaned = [item.strip() for item in requested if item and item.strip()]
        missing = sorted(set(cleaned) - set(discovered))
        if missing:
            raise SystemExit(
                "Requested subset(s) not present in repo snapshot: " + ", ".join(missing)
            )
        return cleaned
    if discovered:
        return discovered
    if archive_name == "simulamet":
        return list(DEFAULT_SIMULAMET_SUBSETS)
    return []


def _select_repo_entries(
    *,
    repo_entries: list[dict[str, object]],
    subsets: list[str],
) -> list[dict[str, object]]:
    selected: list[dict[str, object]] = []
    allowed_subsets = set(subsets)
    for entry in repo_entries:
        repo_path = str(entry["repo_path"])
        group = _repo_group(repo_path)
        if group == REPO_ROOT_GROUP or group in allowed_subsets:
            selected.append(entry)
    if not selected:
        raise SystemExit("No repo files matched the selected subset filter.")
    return sorted(selected, key=lambda entry: str(entry["repo_path"]))


def _require_empty_snapshot_dir(snapshot_dir: Path) -> None:
    if snapshot_dir.exists() and any(snapshot_dir.iterdir()):
        raise SystemExit(
            f"Snapshot directory already exists and is non-empty: {snapshot_dir}. "
            "Remove or rename it before rerunning canonical acquisition."
        )


def _download_repo_snapshot(
    *,
    dataset: str,
    revision: str,
    snapshot_dir: Path,
    repo_entries: list[dict[str, object]],
) -> None:
    try:
        from huggingface_hub import snapshot_download
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Missing huggingface_hub dependency for snapshot download: {exc}") from exc

    snapshot_dir.mkdir(parents=True, exist_ok=True)
    snapshot_download(
        repo_id=dataset,
        repo_type="dataset",
        revision=revision,
        local_dir=snapshot_dir,
        allow_patterns=[str(entry["repo_path"]) for entry in repo_entries],
    )
    local_cache_dir = snapshot_dir / ".cache"
    if local_cache_dir.exists():
        shutil.rmtree(local_cache_dir)


def main() -> None:
    args = parse_args()
    if args.format != "parquet":
        raise SystemExit(
            "Canonical repo snapshot acquisition preserves upstream files as-is. "
            "Only the default --format parquet mode is supported."
        )
    if args.max_rows is not None:
        raise SystemExit("Snapshot acquisition does not support --max-rows or sampled exports.")
    if args.allow_sampled_export:
        raise SystemExit("Snapshot acquisition does not support sampled exports.")

    archive_name = (args.archive_name or _default_archive_name(args.dataset)).strip().lower()
    snapshot_dir = _resolve_snapshot_dir(args).resolve()
    manifest_out = (args.manifest_out or _default_manifest_path(archive_name)).resolve()
    restricted_hash_out = (args.restricted_hash_out or _default_hash_path(archive_name)).resolve()

    dataset_meta, repo_entries, huggingface_hub_version = _resolve_dataset_metadata(
        dataset=args.dataset,
        revision=str(args.revision),
        archive_name=archive_name,
    )
    subset_names = _resolve_subsets(
        repo_entries=repo_entries,
        requested=args.subset,
        archive_name=archive_name,
    )
    selected_entries = _select_repo_entries(repo_entries=repo_entries, subsets=subset_names)
    _require_empty_snapshot_dir(snapshot_dir)
    _download_repo_snapshot(
        dataset=args.dataset,
        revision=dataset_meta["resolved_revision"],
        snapshot_dir=snapshot_dir,
        repo_entries=selected_entries,
    )

    export_manifest: dict[str, Any] = {
        "schema_version": "hf_archive_export_manifest.v3",
        "archive_name": archive_name,
        "dataset": args.dataset,
        "requested_revision": dataset_meta["requested_revision"],
        "resolved_revision": dataset_meta["resolved_revision"],
        "license": dataset_meta["license"],
        "exported_at_utc": _utc_now_iso(),
        "format": "raw_snapshot",
        "sampled_export": False,
        "max_rows": None,
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": str(snapshot_dir),
        "download_transport": "huggingface_hub.snapshot_download",
        "huggingface_hub_version": huggingface_hub_version,
        "repo_root_files": {},
        "subsets": {},
        "source_file_count": len(selected_entries),
        "source_bytes_total": 0,
    }
    hash_rows: list[dict[str, object]] = []

    for entry in selected_entries:
        repo_path = str(entry["repo_path"])
        group = _repo_group(repo_path)
        local_path = snapshot_dir / repo_path
        sha256 = _sha256_file(local_path)
        size_bytes = int(local_path.stat().st_size)
        blob_id = str(entry["upstream_blob_id"])
        details = {
            "path": str(local_path),
            "repo_path": repo_path,
            "sha256": sha256,
            "size_bytes": size_bytes,
            "upstream_blob_id": blob_id,
        }
        export_manifest["source_bytes_total"] += size_bytes

        if group == REPO_ROOT_GROUP:
            export_manifest["repo_root_files"][repo_path] = details
        else:
            subset_payload = export_manifest["subsets"].setdefault(
                group,
                {
                    "file_count": 0,
                    "total_bytes": 0,
                    "files": {},
                },
            )
            subset_payload["file_count"] += 1
            subset_payload["total_bytes"] += size_bytes
            subset_payload["files"][repo_path] = details

        hash_rows.append(
            {
                "archive_name": archive_name,
                "snapshot_id": snapshot_dir.name,
                "subset": group,
                "split": Path(repo_path).name,
                "repo_path": repo_path,
                "raw_path": str(local_path),
                "sha256": sha256,
                "size_bytes": size_bytes,
                "upstream_blob_id": blob_id,
            }
        )

    export_manifest_path = snapshot_dir / "EXPORT_MANIFEST.json"
    export_manifest_path.write_text(json.dumps(export_manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    canonical_manifest = {
        "schema_version": "hf_archive_manifest.v3",
        "archive_name": archive_name,
        "dataset": args.dataset,
        "requested_revision": dataset_meta["requested_revision"],
        "resolved_revision": dataset_meta["resolved_revision"],
        "license": dataset_meta["license"],
        "snapshot_id": snapshot_dir.name,
        "snapshot_dir": str(snapshot_dir),
        "exported_at_utc": export_manifest["exported_at_utc"],
        "format": export_manifest["format"],
        "download_transport": export_manifest["download_transport"],
        "huggingface_hub_version": huggingface_hub_version,
        "sampled_export": False,
        "max_rows": None,
        "source_file_count": export_manifest["source_file_count"],
        "source_bytes_total": export_manifest["source_bytes_total"],
        "repo_root_files": export_manifest["repo_root_files"],
        "subsets": export_manifest["subsets"],
        "export_manifest_path": str(export_manifest_path),
        "restricted_hash_mapping_path": str(restricted_hash_out),
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
                "source_file_count": export_manifest["source_file_count"],
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
