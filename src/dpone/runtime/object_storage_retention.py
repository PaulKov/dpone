"""Object-storage retention, budget and cleanup governance."""

from __future__ import annotations

import json
import tempfile
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from dpone.runtime.object_storage_lifecycle import ObjectStorageLifecycleAdvisor
from dpone.runtime.storage_policy import parse_byte_size
from dpone.storage import ObjectStorageObject, ObjectStorageUri

RETENTION_SCHEMA = "dpone.object_storage.retention_policy.v1"
MARKER_SCHEMA = "dpone.object_storage.run_marker.v1"
BUDGET_SCHEMA = "dpone.object_storage.budget_guard.v1"
SWEEP_SCHEMA = "dpone.object_storage.cleanup_sweep.v1"
MARKER_NAME = "__dpone_run_marker.json"
CANONICAL_CLEANUP_POLICIES = {"eager", "on_success", "keep_on_failure", "retain"}


@dataclass(frozen=True, slots=True)
class ObjectStorageRetentionPolicy:
    enabled: bool = True
    bucket_limit_bytes: int = 200 * 1024**3
    warn_usage_pct: int = 70
    block_usage_pct: int = 85
    active_run_max_hours: int = 12
    failed_run_ttl_hours: int = 24
    orphan_ttl_hours: int = 12
    lifecycle_expiration_days: int = 2
    abort_incomplete_multipart_days: int = 1
    require_lifecycle_rule: str = "warn"

    @classmethod
    def from_options(cls, value: Mapping[str, Any] | None = None) -> ObjectStorageRetentionPolicy:
        raw = dict(value or {})
        return cls(
            enabled=_bool(raw.get("enabled"), default=True),
            bucket_limit_bytes=parse_byte_size(raw.get("bucket_limit_bytes", "200GiB")),
            warn_usage_pct=int(raw.get("warn_usage_pct") or 70),
            block_usage_pct=int(raw.get("block_usage_pct") or 85),
            active_run_max_hours=int(raw.get("active_run_max_hours") or 12),
            failed_run_ttl_hours=int(raw.get("failed_run_ttl_hours") or 24),
            orphan_ttl_hours=int(raw.get("orphan_ttl_hours") or 12),
            lifecycle_expiration_days=int(raw.get("lifecycle_expiration_days") or 2),
            abort_incomplete_multipart_days=int(raw.get("abort_incomplete_multipart_days") or 1),
            require_lifecycle_rule=str(raw.get("require_lifecycle_rule") or "warn"),
        )

    def to_evidence(self) -> dict[str, object]:
        return {
            "schema_version": RETENTION_SCHEMA,
            "enabled": self.enabled,
            "bucket_limit_bytes": self.bucket_limit_bytes,
            "warn_usage_pct": self.warn_usage_pct,
            "block_usage_pct": self.block_usage_pct,
            "active_run_max_hours": self.active_run_max_hours,
            "failed_run_ttl_hours": self.failed_run_ttl_hours,
            "orphan_ttl_hours": self.orphan_ttl_hours,
            "lifecycle_expiration_days": self.lifecycle_expiration_days,
            "abort_incomplete_multipart_days": self.abort_incomplete_multipart_days,
            "require_lifecycle_rule": self.require_lifecycle_rule,
        }


@dataclass(frozen=True, slots=True)
class ObjectStorageRunMarker:
    run_id: str
    workload_id: str
    table: str
    created_at: datetime
    expires_at: datetime
    status: str = "running"

    @classmethod
    def running(
        cls,
        *,
        run_id: str,
        workload_id: str,
        table: str,
        policy: ObjectStorageRetentionPolicy,
        now: datetime | None = None,
    ) -> ObjectStorageRunMarker:
        current = _utc(now)
        return cls(
            run_id=run_id,
            workload_id=workload_id,
            table=table,
            created_at=current,
            expires_at=current + timedelta(hours=policy.active_run_max_hours),
            status="running",
        )

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> ObjectStorageRunMarker:
        return cls(
            run_id=str(payload["run_id"]),
            workload_id=str(payload.get("workload_id") or ""),
            table=str(payload.get("table") or ""),
            created_at=_parse_time(str(payload["created_at"])),
            expires_at=_parse_time(str(payload["expires_at"])),
            status=str(payload.get("status") or "running"),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": MARKER_SCHEMA,
            "run_id": self.run_id,
            "workload_id": self.workload_id,
            "table": self.table,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "status": self.status,
        }


@dataclass(frozen=True, slots=True)
class ObjectStorageBudgetResult:
    used_bytes: int
    limit_bytes: int
    usage_pct: float
    status: str
    warnings: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": BUDGET_SCHEMA,
            "used_bytes": self.used_bytes,
            "limit_bytes": self.limit_bytes,
            "usage_pct": round(self.usage_pct, 4),
            "status": self.status,
            "warnings": list(self.warnings),
            "blockers": list(self.blockers),
        }


class ObjectStorageBudgetProbe:
    def __init__(self, client: Any) -> None:
        self._client = client

    def check(
        self,
        prefix: ObjectStorageUri,
        *,
        policy: ObjectStorageRetentionPolicy,
        expected_run_bytes: int = 0,
    ) -> ObjectStorageBudgetResult:
        used = sum(item.size_bytes for item in _list_objects(self._client, prefix))
        projected = used + max(0, expected_run_bytes)
        usage = (projected / policy.bucket_limit_bytes * 100) if policy.bucket_limit_bytes > 0 else 0.0
        if usage >= policy.block_usage_pct:
            return ObjectStorageBudgetResult(
                used_bytes=used,
                limit_bytes=policy.bucket_limit_bytes,
                usage_pct=usage,
                status="blocked",
                blockers=("object_storage_budget_block_threshold_exceeded",),
            )
        if usage >= policy.warn_usage_pct:
            return ObjectStorageBudgetResult(
                used_bytes=used,
                limit_bytes=policy.bucket_limit_bytes,
                usage_pct=usage,
                status="warning",
                warnings=("object_storage_budget_warning_threshold_exceeded",),
            )
        return ObjectStorageBudgetResult(
            used_bytes=used, limit_bytes=policy.bucket_limit_bytes, usage_pct=usage, status="green"
        )


@dataclass(frozen=True, slots=True)
class ObjectStorageCleanupCandidate:
    prefix: str
    reason: str
    object_count: int
    size_bytes: int

    def to_dict(self) -> dict[str, object]:
        return {
            "prefix": _with_trailing_slash(self.prefix),
            "reason": self.reason,
            "object_count": self.object_count,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True, slots=True)
class ObjectStorageSweepReport:
    dry_run: bool
    candidates: tuple[ObjectStorageCleanupCandidate, ...]
    deleted_prefixes: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()

    @property
    def candidate_count(self) -> int:
        return len(self.candidates)

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SWEEP_SCHEMA,
            "dry_run": self.dry_run,
            "candidate_count": self.candidate_count,
            "deleted_prefixes": list(self.deleted_prefixes),
            "blockers": list(self.blockers),
            "candidates": [candidate.to_dict() for candidate in self.candidates],
        }


class ObjectStorageSweeper:
    def __init__(self, client: Any) -> None:
        self._client = client

    def plan(
        self,
        root: ObjectStorageUri,
        *,
        policy: ObjectStorageRetentionPolicy,
        now: datetime | None = None,
    ) -> ObjectStorageSweepReport:
        if _unsafe_sweep_root(root):
            return ObjectStorageSweepReport(
                dry_run=True,
                candidates=(),
                blockers=("object_storage_cleanup_prefix_not_specific_enough",),
            )
        current = _utc(now)
        candidates = tuple(
            _candidate(root, group, self._client, policy, current) for group in _groups(root, self._client)
        )
        return ObjectStorageSweepReport(dry_run=True, candidates=tuple(item for item in candidates if item))

    def sweep(
        self,
        root: ObjectStorageUri,
        *,
        policy: ObjectStorageRetentionPolicy,
        now: datetime | None = None,
        dry_run: bool = True,
    ) -> ObjectStorageSweepReport:
        planned = self.plan(root, policy=policy, now=now)
        if dry_run or planned.blockers:
            return planned
        deleted: list[str] = []
        for candidate in planned.candidates:
            uri = ObjectStorageUri.parse(candidate.prefix)
            self._client.delete_prefix(uri)
            deleted.append(_with_trailing_slash(candidate.prefix))
        return ObjectStorageSweepReport(dry_run=False, candidates=planned.candidates, deleted_prefixes=tuple(deleted))


def write_run_marker(
    client: Any,
    prefix: ObjectStorageUri,
    *,
    marker: ObjectStorageRunMarker,
) -> None:
    with tempfile.TemporaryDirectory(prefix="dpone-object-marker-") as tmp:
        path = Path(tmp) / MARKER_NAME
        path.write_text(json.dumps(marker.to_dict(), sort_keys=True), encoding="utf-8")
        client.put_file(path, prefix.prefix().child(MARKER_NAME), content_type="application/json")


def canonical_cleanup_policy(value: str | None) -> str:
    aliases = {"delete_on_success": "on_success", "delete_always": "eager", "keep": "keep_on_failure"}
    normalized = aliases.get(str(value or "eager").strip().lower(), str(value or "eager").strip().lower())
    if normalized not in CANONICAL_CLEANUP_POLICIES:
        raise ValueError(f"Unsupported object storage cleanup_policy: {value}")
    return normalized


def _list_objects(client: Any, prefix: ObjectStorageUri) -> tuple[ObjectStorageObject, ...]:
    if hasattr(client, "list_objects"):
        return tuple(client.list_objects(prefix))
    return tuple(ObjectStorageObject(uri=uri, size_bytes=0, sha256="") for uri in client.list_prefix(prefix))


def _groups(
    root: ObjectStorageUri, client: Any
) -> tuple[tuple[ObjectStorageUri, tuple[ObjectStorageObject, ...]], ...]:
    grouped: dict[str, list[ObjectStorageObject]] = {}
    root_parts = [part for part in root.prefix().key.split("/") if part]
    for item in _list_objects(client, root.prefix()):
        uri = ObjectStorageUri.parse(item.uri)
        parts = [part for part in uri.key.split("/") if part]
        if len(parts) < len(root_parts) + 3:
            continue
        run_prefix = "/".join(parts[: len(root_parts) + 3]) + "/"
        grouped.setdefault(run_prefix, []).append(item)
    return tuple(
        (ObjectStorageUri(root.provider, root.bucket, prefix, account=root.account).prefix(), tuple(items))
        for prefix, items in sorted(grouped.items())
    )


def _candidate(
    root: ObjectStorageUri,
    group: tuple[ObjectStorageUri, tuple[ObjectStorageObject, ...]],
    client: Any,
    policy: ObjectStorageRetentionPolicy,
    now: datetime,
) -> ObjectStorageCleanupCandidate | None:
    prefix, objects = group
    if not _safe_candidate(root, prefix):
        return None
    marker = _read_marker(client, prefix)
    reason: str | None = None
    if marker and marker.expires_at <= now:
        reason = f"expired_{marker.status}_run_marker"
    if marker is None and _oldest_object_time(objects) + timedelta(hours=policy.orphan_ttl_hours) <= now:
        reason = "expired_orphan_prefix"
    if reason is None:
        return None
    return ObjectStorageCleanupCandidate(
        prefix=_with_trailing_slash(str(prefix)),
        reason=reason,
        object_count=len(objects),
        size_bytes=sum(item.size_bytes for item in objects),
    )


def _read_marker(client: Any, prefix: ObjectStorageUri) -> ObjectStorageRunMarker | None:
    marker_uri = prefix.child(MARKER_NAME)
    if hasattr(client, "read_marker"):
        return client.read_marker(marker_uri)
    if not hasattr(client, "get_file"):
        return None
    with tempfile.TemporaryDirectory(prefix="dpone-marker-read-") as tmp:
        path = Path(tmp) / MARKER_NAME
        try:
            client.get_file(marker_uri, path)
        except Exception:
            return None
        return ObjectStorageRunMarker.from_dict(json.loads(path.read_text(encoding="utf-8")))


def _unsafe_sweep_root(root: ObjectStorageUri) -> bool:
    parts = [part for part in root.prefix().key.split("/") if part]
    return len(parts) < 2 or any(part in {"..", "."} for part in parts)


def _safe_candidate(root: ObjectStorageUri, prefix: ObjectStorageUri) -> bool:
    root_parts = [part for part in root.prefix().key.split("/") if part]
    parts = [part for part in prefix.prefix().key.split("/") if part]
    return len(parts) >= len(root_parts) + 3 and parts[: len(root_parts)] == root_parts


def _oldest_object_time(objects: tuple[ObjectStorageObject, ...]) -> datetime:
    values = [
        _parse_time(str(item.metadata.get("last_modified"))) for item in objects if item.metadata.get("last_modified")
    ]
    return min(values) if values else datetime.now(tz=UTC)


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _utc(value: datetime | None) -> datetime:
    if value is None:
        return datetime.now(tz=UTC)
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)


def _with_trailing_slash(value: str) -> str:
    return value if value.endswith("/") else f"{value}/"


def _bool(value: Any, *, default: bool) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "y", "on"}


__all__ = [
    "ObjectStorageBudgetProbe",
    "ObjectStorageLifecycleAdvisor",
    "ObjectStorageRetentionPolicy",
    "ObjectStorageRunMarker",
    "ObjectStorageSweeper",
    "canonical_cleanup_policy",
    "write_run_marker",
]
