from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

GAP_LOG_SECONDS = 6 * 3600
SEVERE_GAP_SECONDS = 24 * 3600
TOPIC_DICTIONARY_VERSION = "v1"
HASH_SALT = "moltbook_coordination_persistence_v1"

TOPIC_RULES: list[dict[str, Any]] = [
    {
        "category": "SpamLowSignal",
        "precedence": 1,
        "keywords": (
            "spam",
            "low signal",
            "lowsignal",
            "shitpost",
            "meme",
            "memes",
            "promo",
            "advert",
            "bot",
            "nsfw",
            "nsfl",
        ),
    },
    {
        "category": "BuilderTechnical",
        "precedence": 2,
        "keywords": (
            "builder",
            "build",
            "technical",
            "tech",
            "code",
            "coding",
            "program",
            "developer",
            "dev",
            "software",
            "engineering",
            "api",
            "robot",
            "agent",
            "model",
            "prompt",
        ),
    },
    {
        "category": "PhilosophyMeta",
        "precedence": 3,
        "keywords": (
            "philosophy",
            "meta",
            "theory",
            "ethic",
            "alignment",
            "policy",
            "politic",
            "society",
            "culture",
            "epistem",
            "debate",
        ),
    },
    {
        "category": "Creative",
        "precedence": 4,
        "keywords": (
            "art",
            "creative",
            "music",
            "poetry",
            "story",
            "stories",
            "fiction",
            "design",
            "image",
            "video",
            "film",
            "drawing",
        ),
    },
    {
        "category": "SocialCasual",
        "precedence": 5,
        "keywords": (
            "social",
            "casual",
            "chat",
            "hangout",
            "lounge",
            "fun",
            "life",
            "daily",
            "friends",
            "general",
        ),
    },
]

RAW_PRIMARY_KEY_ALIASES: dict[str, list[list[str]]] = {
    "comments": [["id"], ["comment_id"]],
    "posts": [["id"], ["thread_id"]],
    "agents": [["id"], ["author_id"]],
    "submolts": [["id"], ["name"], ["community_label"]],
    "snapshots": [["timestamp"], ["snapshot_timestamp_utc"]],
    "word_frequency": [["word", "hour", "dump_date"], ["word", "hour_utc", "source_snapshot_id"]],
}

TIMESTAMP_ALIASES: dict[str, list[str]] = {
    "comments": ["created_at_utc", "created_at"],
    "posts": ["post_created_at_utc", "created_at_utc", "created_at"],
    "snapshots": ["snapshot_timestamp_utc", "timestamp"],
    "word_frequency": ["hour_utc", "hour"],
}


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat()


def load_json_document(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return payload


def write_parquet(path: Path, frame: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    table = pa.Table.from_pandas(frame, preserve_index=False)
    pq.write_table(table, path)


def load_parquet(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    return pd.read_parquet(path)


def load_freeze_frame(freeze_root: Path, subset: str) -> pd.DataFrame:
    return load_parquet(freeze_root / f"{subset}.parquet")


def normalize_text(value: Any) -> str | None:
    if value is None or pd.isna(value):
        return None
    return str(value).strip()


def deterministic_hash(value: Any) -> str | None:
    normalized = normalize_text(value)
    if normalized is None:
        return None
    return hashlib.sha256(f"{HASH_SALT}:{normalized}".encode("utf-8")).hexdigest()


def infer_archive_name(path_value: Path | str) -> str:
    text = str(path_value).lower()
    if "moltnet" in text:
        return "moltnet"
    return "simulamet"


def repo_root_from(path_value: Path) -> Path:
    for candidate in [path_value.resolve(), *path_value.resolve().parents]:
        if (candidate / "contracts" / "project.yaml").exists():
            return candidate
    return Path.cwd().resolve()


def load_archive_manifest(repo_root: Path, archive_name: str) -> dict[str, Any] | None:
    manifest_path = repo_root / "manifests" / f"{archive_name}_manifest.yaml"
    if not manifest_path.exists():
        return None
    return load_json_document(manifest_path)


def archive_revision_for(repo_root: Path, archive_name: str) -> str:
    manifest = load_archive_manifest(repo_root, archive_name)
    if manifest is None:
        return "unknown"
    revision = manifest.get("resolved_revision") or manifest.get("requested_revision") or manifest.get(
        "snapshot_id"
    )
    return str(revision) if revision is not None else "unknown"


def resolve_admin_end(
    *,
    comments: pd.DataFrame,
    posts: pd.DataFrame,
    snapshots: pd.DataFrame,
    word_frequency: pd.DataFrame,
) -> pd.Timestamp:
    candidates: list[pd.Timestamp] = []
    for frame, column in [
        (comments, "created_at_utc"),
        (posts, "post_created_at_utc"),
        (snapshots, "snapshot_timestamp_utc"),
        (word_frequency, "hour_utc"),
    ]:
        if frame.empty or column not in frame.columns:
            continue
        series = pd.to_datetime(frame[column], errors="coerce", utc=True).dropna()
        if not series.empty:
            candidates.append(series.max())
    if not candidates:
        raise ValueError("No timestamps available to resolve administrative window end.")
    return max(candidates)


def compute_segments(comments: pd.DataFrame, admin_end_utc: pd.Timestamp) -> pd.DataFrame:
    timeline = pd.to_datetime(comments["created_at_utc"], errors="coerce", utc=True).dropna().sort_values()
    if timeline.empty:
        return pd.DataFrame(columns=["segment_id", "segment_start_utc", "segment_end_utc", "is_final_segment"])

    unique_times = list(pd.Index(timeline.drop_duplicates()))
    segments: list[dict[str, Any]] = []
    seg_start = unique_times[0]
    seg_last = unique_times[0]
    segment_index = 1
    for current in unique_times[1:]:
        if (current - seg_last).total_seconds() > GAP_LOG_SECONDS:
            segments.append(
                {
                    "segment_id": f"segment_{segment_index:02d}",
                    "segment_start_utc": seg_start,
                    "segment_end_utc": seg_last,
                    "is_final_segment": False,
                }
            )
            segment_index += 1
            seg_start = current
        seg_last = current

    final_end = max(pd.Timestamp(admin_end_utc), pd.Timestamp(seg_last))
    segments.append(
        {
            "segment_id": f"segment_{segment_index:02d}",
            "segment_start_utc": seg_start,
            "segment_end_utc": final_end,
            "is_final_segment": True,
        }
    )
    return pd.DataFrame(segments)


def attach_segments(comments: pd.DataFrame, segments: pd.DataFrame) -> pd.DataFrame:
    if comments.empty:
        return comments.copy()

    enriched = comments.copy()
    enriched["created_at_utc"] = pd.to_datetime(enriched["created_at_utc"], errors="coerce", utc=True)

    segments = segments.sort_values("segment_start_utc").reset_index(drop=True)
    bounds = segments["segment_start_utc"].tolist()

    def locate_segment(ts: pd.Timestamp) -> int:
        if pd.isna(ts):
            return 0
        idx = 0
        for i, start in enumerate(bounds):
            if ts >= start:
                idx = i
            else:
                break
        return idx

    seg_indexes = enriched["created_at_utc"].apply(locate_segment)
    enriched = enriched.reset_index(drop=True)
    enriched["segment_id"] = seg_indexes.map(segments["segment_id"])
    enriched["segment_end_utc"] = seg_indexes.map(segments["segment_end_utc"])
    enriched["is_final_segment"] = seg_indexes.map(segments["is_final_segment"])
    return enriched


def compute_depths(comments: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    for thread_id, subset in comments.groupby("thread_id", dropna=False, sort=False):
        parent_map = {
            str(row.comment_id): normalize_text(row.parent_comment_id)
            for row in subset.loc[:, ["comment_id", "parent_comment_id"]].itertuples(index=False)
            if normalize_text(row.comment_id) is not None
        }
        raw_depths = {}
        if "depth" in subset.columns:
            raw_depths = {
                str(row.comment_id): row.depth
                for row in subset.loc[:, ["comment_id", "depth"]].itertuples(index=False)
                if normalize_text(row.comment_id) is not None
            }

        cache: dict[str, int | None] = {}

        def depth(comment_id: str, visiting: set[str] | None = None) -> int | None:
            if comment_id in cache:
                return cache[comment_id]
            visiting = visiting or set()
            if comment_id in visiting:
                cache[comment_id] = None
                return None
            visiting.add(comment_id)
            parent_id = parent_map.get(comment_id)
            if parent_id in {None, "", "nan"}:
                cache[comment_id] = 1
                return 1
            if parent_id not in parent_map:
                cache[comment_id] = None
                return None
            parent_depth = depth(parent_id, visiting)
            cache[comment_id] = None if parent_depth is None else parent_depth + 1
            return cache[comment_id]

        for comment_id in parent_map:
            rows.append(
                {
                    "thread_id": thread_id,
                    "comment_id": comment_id,
                    "depth_from_root": depth(comment_id),
                    "raw_depth": raw_depths.get(comment_id),
                }
            )

    return pd.DataFrame(rows)


def classify_topic(
    *,
    community_label: str | None,
    community_description: str | None = None,
) -> tuple[str, str, str, int]:
    label = normalize_text(community_label)
    description = normalize_text(community_description)
    if label is None:
        return "Unknown", "__unresolved__", "missing_label", 0

    haystack = " ".join(part for part in [label.lower(), (description or "").lower()] if part)
    for rule in TOPIC_RULES:
        for keyword in rule["keywords"]:
            if keyword in haystack:
                return str(rule["category"]), keyword, "keyword", int(rule["precedence"])
    return "Other", "__fallback__", "fallback", 99


def build_topic_dictionary_frame(posts: pd.DataFrame, submolts: pd.DataFrame) -> pd.DataFrame:
    descriptions: dict[str, str | None] = {}
    if not submolts.empty and "community_label" in submolts.columns:
        for row in submolts.loc[:, [column for column in ["community_label", "community_description"] if column in submolts.columns]].itertuples(index=False):
            label = normalize_text(getattr(row, "community_label", None))
            if label is None:
                continue
            descriptions[label] = normalize_text(getattr(row, "community_description", None))

    labels = set()
    if "community_label" in posts.columns:
        labels.update(str(value) for value in posts["community_label"].dropna().astype("string"))
    labels.update(descriptions.keys())

    rows: list[dict[str, Any]] = []
    for label in sorted(labels):
        category, trigger, trigger_type, precedence = classify_topic(
            community_label=label,
            community_description=descriptions.get(label),
        )
        rows.append(
            {
                "community_label": label,
                "dictionary_version": TOPIC_DICTIONARY_VERSION,
                "category": category,
                "trigger": trigger,
                "trigger_type": trigger_type,
                "precedence": precedence,
                "rationale": "Auto-generated deterministic category from community label/description.",
                "approver": "pending",
                "approval_date": "",
            }
        )

    return pd.DataFrame(rows)


def topic_mapping(posts: pd.DataFrame, submolts: pd.DataFrame) -> dict[str, str]:
    dictionary = build_topic_dictionary_frame(posts, submolts)
    if dictionary.empty:
        return {}
    return {
        str(row.community_label): str(row.category)
        for row in dictionary.loc[:, ["community_label", "category"]].itertuples(index=False)
    }


def claimed_status_group(value: Any) -> str:
    if value is None or pd.isna(value):
        return "unknown"
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "1", "claimed", "yes"}:
            return "claimed"
        if lowered in {"false", "0", "unclaimed", "no"}:
            return "unclaimed"
        return "unknown"
    if bool(value):
        return "claimed"
    return "unclaimed"


def parse_raw_subset(raw_manifest: dict[str, Any], subset: str) -> pd.DataFrame:
    subset_payload = raw_manifest.get("subsets", {}).get(subset, {})
    splits = subset_payload.get("splits", {})
    paths = []
    if isinstance(splits, dict):
        for split in splits.values():
            if isinstance(split, dict) and split.get("path"):
                paths.append(Path(str(split["path"])))
    if not paths:
        return pd.DataFrame()
    tables = [pq.read_table(path) for path in paths]
    return pa.concat_tables(tables, promote_options="default").to_pandas()


def pick_primary_key_columns(frame: pd.DataFrame, subset: str) -> list[str]:
    for option in RAW_PRIMARY_KEY_ALIASES.get(subset, []):
        if all(column in frame.columns for column in option):
            return option
    return []


def timestamp_parse_success_rate(raw_manifest: dict[str, Any]) -> float | None:
    parsed = 0
    total = 0
    for subset, aliases in TIMESTAMP_ALIASES.items():
        frame = parse_raw_subset(raw_manifest, subset)
        if frame.empty:
            continue
        column = next((alias for alias in aliases if alias in frame.columns), None)
        if column is None:
            continue
        raw = frame[column]
        nonnull = int(raw.notna().sum())
        if nonnull == 0:
            continue
        total += nonnull
        parsed += int(pd.to_datetime(raw, errors="coerce", utc=True).notna().sum())
    if total == 0:
        return None
    return parsed / total


def duplicate_count_by_table(raw_manifest: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for subset in raw_manifest.get("subsets", {}):
        frame = parse_raw_subset(raw_manifest, subset)
        if frame.empty:
            counts[str(subset)] = 0
            continue
        primary_keys = pick_primary_key_columns(frame, str(subset))
        if not primary_keys:
            counts[str(subset)] = 0
            continue
        duplicates = int(frame.shape[0] - frame.drop_duplicates(primary_keys).shape[0])
        counts[str(subset)] = duplicates
    return counts


def total_rows_by_table(raw_manifest: dict[str, Any]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for subset, payload in raw_manifest.get("subsets", {}).items():
        if isinstance(payload, dict):
            counts[str(subset)] = int(payload.get("rows_exported_total", 0))
    return counts


def parse_linkage_rates(linkage_path: Path) -> dict[str, float | None]:
    if not linkage_path.exists():
        return {
            "post_link_success_rate": None,
            "parent_link_success_rate": None,
        }
    frame = pd.read_csv(linkage_path)
    mapping = {
        "comments_thread_id_resolves_to_posts": "post_link_success_rate",
        "parent_comment_resolves_same_thread": "parent_link_success_rate",
    }
    result = {value: None for value in mapping.values()}
    for row in frame.itertuples(index=False):
        key = mapping.get(str(row.check_name))
        if key is not None:
            rate = getattr(row, "resolution_rate", None)
            result[key] = None if pd.isna(rate) else float(rate)
    return result


def severe_gap_count(gap_registry_path: Path) -> int:
    if not gap_registry_path.exists():
        return 0
    frame = pd.read_csv(gap_registry_path)
    if "severity" not in frame.columns:
        return 0
    return int((frame["severity"].astype("string") == "severe").sum())


def min_max_time(values: list[pd.Series]) -> tuple[pd.Timestamp | None, pd.Timestamp | None]:
    mins: list[pd.Timestamp] = []
    maxs: list[pd.Timestamp] = []
    for series in values:
        parsed = pd.to_datetime(series, errors="coerce", utc=True).dropna()
        if parsed.empty:
            continue
        mins.append(parsed.min())
        maxs.append(parsed.max())
    if not mins:
        return None, None
    return min(mins), max(maxs)
