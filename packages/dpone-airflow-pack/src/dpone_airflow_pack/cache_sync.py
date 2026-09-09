"""Fail-open friendly remote Airflow pack cache synchronization."""

from __future__ import annotations

import json
import os
import random
import time
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dpone_airflow_pack.artifact_store import ArtifactReader, ArtifactReadPort
from dpone_airflow_pack.cache_activation_contract import cache_write_lease
from dpone_airflow_pack.cache_generation_budget import cache_capacity_reservation
from dpone_airflow_pack.cache_generation_preparation import prepared_generation
from dpone_airflow_pack.cache_generation_retention import CacheRetentionResult, enforce_cache_retention
from dpone_airflow_pack.cache_generation_store import (
    LegacyCacheCommitReceipt,
    commit_generation,
    inspect_existing_generation,
    recover_interrupted_commit,
)
from dpone_airflow_pack.cache_layout import (
    LAYOUT_MARKER_NAME,
    LEGACY_PACK_INDEX_LAYOUT,
    assert_cache_layout_compatible,
    ensure_cache_layout,
)
from dpone_airflow_pack.cache_sync_evidence import publish_sync_evidence as _publish_sync_evidence
from dpone_airflow_pack.cache_sync_evidence import write_sync_warning
from dpone_airflow_pack.cache_writer_coordination import cache_evidence_lease
from dpone_airflow_pack.diagnostic_warnings import emit_nonfatal_runtime_warning
from dpone_airflow_pack.pack_index import (
    dag_spec_index_entries,
    index_entries,
    index_generation,
)
from dpone_airflow_pack.pack_index_security import (
    validate_index_generation,
)


@dataclass(frozen=True)
class AirflowPackSyncOptions:
    index_uri: str
    cache_dir: Path
    reader_connection_id: str | None = None
    max_total_bytes: int | None = 512 * 1024 * 1024
    max_pack_bytes: int | None = 10 * 1024 * 1024
    max_index_bytes: int | None = 25 * 1024 * 1024
    keep_generations: int = 3
    high_watermark_pct: int = 80
    low_watermark_pct: int = 60
    partial_download_ttl_minutes: int = 30
    status_path: Path | None = None
    airflow_variable_key: str | None = None


def sync_airflow_pack_cache(
    options: AirflowPackSyncOptions,
    *,
    reader: ArtifactReadPort | None = None,
) -> dict[str, Any]:
    """Download latest remote pack index and packs into a bounded local generation."""

    validate_airflow_pack_sync_options(options)
    assert_cache_layout_compatible(options.cache_dir, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    started_at = _utc_now()
    with cache_evidence_lease(options.cache_dir):
        with cache_write_lease(options.cache_dir):
            if os.path.lexists(options.cache_dir / LAYOUT_MARKER_NAME):
                ensure_cache_layout(options.cache_dir, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
                base_commit = recover_interrupted_commit(
                    options.cache_dir,
                    max_generation_bytes=options.max_total_bytes,
                )
            else:
                base_commit = recover_interrupted_commit(
                    options.cache_dir,
                    max_generation_bytes=options.max_total_bytes,
                )
                ensure_cache_layout(options.cache_dir, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
    artifact_reader = reader or ArtifactReader(reader_connection_id=options.reader_connection_id)
    index_bytes = artifact_reader.read_bytes(options.index_uri, max_bytes=options.max_index_bytes)
    _check_size(index_bytes, options.max_index_bytes, "airflow_pack_index_too_large")
    index = _json_mapping(index_bytes.decode("utf-8"), options.index_uri)
    generation = index_generation(index)
    if not generation:
        raise ValueError("pack-index.json has no git_sha/generation")
    validate_index_generation(generation)
    pre_retention = _enforce_retention(options)
    warnings = list(pre_retention.warnings)
    if pre_retention.blockers:
        return _publish_sync_evidence(
            options,
            started_at=started_at,
            attempt_generation=generation,
            downloaded=0,
            downloaded_specs=0,
            committed=False,
            warnings=warnings,
            blockers=list(pre_retention.blockers),
            cache_bytes=pre_retention.total_bytes,
        )
    reservation_bytes = _reserved_generation_bytes(index_bytes, index=index, options=options)
    with cache_capacity_reservation(
        options.cache_dir,
        requested_payload_bytes=reservation_bytes,
        max_total_bytes=options.max_total_bytes,
    ):
        with prepared_generation(
            options=options,
            reader=artifact_reader,
            index_bytes=index_bytes,
            index=index,
            generation=generation,
        ) as preparation:
            candidate = preparation.candidate
            existing = inspect_existing_generation(
                options.cache_dir / "generations" / generation,
                max_generation_bytes=options.max_total_bytes,
            )
            committed = False
            receipt: LegacyCacheCommitReceipt
            with cache_evidence_lease(options.cache_dir):
                with cache_write_lease(options.cache_dir):
                    ensure_cache_layout(options.cache_dir, expected_layout=LEGACY_PACK_INDEX_LAYOUT)
                    receipt, committed = commit_generation(
                        options.cache_dir,
                        candidate=candidate,
                        expected_commit_id=base_commit.commit_id if base_commit is not None else None,
                        existing=existing,
                        committed_at=_utc_now(),
                    )
        warnings.extend(preparation.warnings)
    retention = _enforce_retention(options)
    warnings.extend(retention.warnings)
    if receipt.generation != generation:
        warnings.append("sync_superseded_by_concurrent_commit")
    return _publish_sync_evidence(
        options,
        started_at=started_at,
        attempt_generation=generation,
        downloaded=preparation.downloaded_packs,
        downloaded_specs=preparation.downloaded_dag_specs,
        committed=committed,
        warnings=warnings,
        blockers=list(retention.blockers),
        cache_bytes=retention.total_bytes,
    )


def watch_airflow_pack_cache(options: AirflowPackSyncOptions, *, interval_seconds: int, jitter_seconds: int) -> None:
    """Run sync repeatedly; intended for fail-open sidecars."""

    while True:
        try:
            sync_airflow_pack_cache(options)
        except Exception as exc:  # noqa: BLE001 - sidecar wrapper decides whether this is fatal.
            try:
                write_sync_warning(options, reason="watch_sync_failed", message=str(exc))
            except Exception as evidence_exc:  # noqa: BLE001 - diagnostics must not stop a fail-open watcher.
                emit_nonfatal_runtime_warning(
                    f"DPONE_AIRFLOW_PACK_SYNC_WARNING_PUBLISH_FAILED:{evidence_exc.__class__.__name__}",
                    stacklevel=2,
                )
        sleep_for = interval_seconds + (random.randint(0, jitter_seconds) if jitter_seconds > 0 else 0)
        time.sleep(max(1, sleep_for))


def _reserved_generation_bytes(
    index_bytes: bytes,
    *,
    index: Mapping[str, Any],
    options: AirflowPackSyncOptions,
) -> int:
    total = len(index_bytes)
    for entry in (*index_entries(index), *dag_spec_index_entries(index)):
        if entry.bytes is not None:
            if entry.bytes < 0:
                raise ValueError("airflow_pack_index_size_invalid: artifact bytes must be non-negative")
            if options.max_pack_bytes is not None and entry.bytes > options.max_pack_bytes:
                raise ValueError(f"airflow_pack_too_large: {entry.bytes} > {options.max_pack_bytes}")
            total += entry.bytes
        elif options.max_pack_bytes is not None:
            total += options.max_pack_bytes
        elif options.max_total_bytes is not None:
            raise ValueError(
                "airflow_pack_cache_capacity_unknown: artifact size is absent and no per-pack limit exists"
            )
    return total


def _check_size(payload: bytes, max_bytes: int | None, code: str) -> None:
    if max_bytes is not None and len(payload) > max_bytes:
        raise ValueError(f"{code}: {len(payload)} > {max_bytes}")


def _enforce_retention(options: AirflowPackSyncOptions) -> CacheRetentionResult:
    try:
        return enforce_cache_retention(
            options.cache_dir,
            keep_generations=options.keep_generations,
            max_total_bytes=options.max_total_bytes,
            high_watermark_pct=options.high_watermark_pct,
            low_watermark_pct=options.low_watermark_pct,
            partial_download_ttl_minutes=options.partial_download_ttl_minutes,
        )
    except (OSError, ValueError) as exc:
        return CacheRetentionResult(
            total_bytes=-1,
            deleted_paths=(),
            warnings=("cache_generation_prune_failed",),
            blockers=(
                {
                    "code": "airflow_pack_cache_retention_unavailable",
                    "message": f"safe cache retention could not be enforced: {exc.__class__.__name__}",
                },
            ),
        )


def validate_airflow_pack_sync_options(options: AirflowPackSyncOptions) -> None:
    """Validate sync policy without touching cache or remote storage."""

    positive_limits = {
        "max_total_bytes": options.max_total_bytes,
        "max_pack_bytes": options.max_pack_bytes,
        "max_index_bytes": options.max_index_bytes,
    }
    for name, value in positive_limits.items():
        if value is not None and value <= 0:
            raise ValueError(f"airflow_pack_cache_policy_invalid: {name} must be positive or null")
    if options.keep_generations < 1:
        raise ValueError("airflow_pack_cache_policy_invalid: keep_generations must be at least 1")
    if not 0 < options.low_watermark_pct < options.high_watermark_pct <= 100:
        raise ValueError(
            "airflow_pack_cache_policy_invalid: watermarks must satisfy "
            "0 < low_watermark_pct < high_watermark_pct <= 100"
        )
    if options.partial_download_ttl_minutes < 1:
        raise ValueError("airflow_pack_cache_policy_invalid: partial_download_ttl_minutes must be positive")


def _json_mapping(text: str, location: str) -> dict[str, Any]:
    payload = json.loads(text)
    if not isinstance(payload, Mapping):
        raise ValueError(f"JSON object expected: {location}")
    return dict(payload)


def _utc_now() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")  # noqa: UP017


__all__ = [
    "AirflowPackSyncOptions",
    "sync_airflow_pack_cache",
    "validate_airflow_pack_sync_options",
    "watch_airflow_pack_cache",
    "write_sync_warning",
]
