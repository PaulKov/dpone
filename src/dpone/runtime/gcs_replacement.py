"""Deterministic, non-destructive GCS object replacement for runtime routes."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)

CLEANUP_DEBT_SCHEMA = "dpone.gcs.cleanup_debt.v1"
_ATTEMPT_SEGMENT = re.compile(r"^[\w.-]+$")


@dataclass(frozen=True, slots=True)
class GcsAttemptScope:
    """Attempt-owned object identity derived before source or target I/O."""

    run_id: str
    load_id: str

    def __post_init__(self) -> None:
        for label, value in (("run_id", self.run_id), ("load_id", self.load_id)):
            if not value or not _ATTEMPT_SEGMENT.fullmatch(value):
                raise ValueError(f"Invalid GCS attempt scope {label}: {value!r}")


@dataclass(frozen=True, slots=True)
class GcsObjectIdentity:
    bucket: str
    prefix: str

    @property
    def gcs_uri(self) -> str:
        normalized = self.prefix.rstrip("/")
        return f"gs://{self.bucket}/{normalized}"


@dataclass(frozen=True, slots=True)
class GcsCleanupDebtRecord:
    schema_version: str
    bucket: str
    prefix: str
    run_id: str
    load_id: str
    reason: str
    recorded_at: str

    @classmethod
    def create(
        cls,
        *,
        bucket: str,
        prefix: str,
        scope: GcsAttemptScope,
        reason: str,
        recorded_at: datetime | None = None,
    ) -> GcsCleanupDebtRecord:
        current = recorded_at or datetime.now(UTC)
        return cls(
            schema_version=CLEANUP_DEBT_SCHEMA,
            bucket=bucket,
            prefix=_normalize_prefix(prefix),
            run_id=scope.run_id,
            load_id=scope.load_id,
            reason=reason,
            recorded_at=current.isoformat(),
        )


class GcsCleanupDebtJournal(Protocol):
    def record(self, debt: GcsCleanupDebtRecord) -> None: ...


class InMemoryGcsCleanupDebtJournal:
    def __init__(self) -> None:
        self.records: list[GcsCleanupDebtRecord] = []

    def record(self, debt: GcsCleanupDebtRecord) -> None:
        self.records.append(debt)


class FileGcsCleanupDebtJournal:
    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)

    def record(self, debt: GcsCleanupDebtRecord) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        with self._path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(asdict(debt), sort_keys=True))
            handle.write("\n")


class CommitOutcomeUnknownError(RuntimeError):
    """Raised when target commit succeeded but cleanup-debt evidence could not be persisted."""


def resolve_gcs_attempt_scope(load_config: Any, *, load_record: Any | None = None) -> GcsAttemptScope:
    options = dict(getattr(load_config, "options", {}) or {})
    identity = dict(options.get("__dpone_load_identity") or {})
    run_id = str(options.get("run_id") or identity.get("run_id") or getattr(load_record, "run_id", "") or "").strip()
    load_id = str(
        options.get("load_id") or identity.get("load_id") or getattr(load_record, "load_id", "") or ""
    ).strip()
    if not run_id or not load_id:
        raise ValueError("GCS replacement requires run_id and load_id before object-storage I/O.")
    return GcsAttemptScope(run_id=run_id, load_id=load_id)


def derive_attempt_table_prefix(base_table_path: str, scope: GcsAttemptScope) -> str:
    normalized = base_table_path.strip("/")
    return f"{normalized}/attempts/{scope.run_id}/{scope.load_id}"


def derive_partition_attempt_uri(
    bucket_name: str,
    base_table_path: str,
    scope: GcsAttemptScope,
    partition_label: str,
) -> str:
    prefix = f"{derive_attempt_table_prefix(base_table_path, scope)}/dt_date={partition_label}"
    return f"gs://{bucket_name}/{prefix}"


def derive_table_attempt_uri(bucket_name: str, base_table_path: str, scope: GcsAttemptScope) -> str:
    prefix = derive_attempt_table_prefix(base_table_path, scope)
    return f"gs://{bucket_name}/{prefix}"


def derive_reconciliation_batch_path(table_id: str, batch_num: int, scope: GcsAttemptScope) -> str:
    safe_table = re.sub(r"[^A-Za-z0-9._-]+", "_", table_id.strip())
    return (
        f"reconciliation_tmp/attempts/{scope.run_id}/{scope.load_id}/{safe_table}/snapshot_batch_{batch_num:04d}.csv.gz"
    )


def canonical_partition_prefix(base_table_path: str, partition_label: str) -> str:
    normalized = base_table_path.strip("/")
    return _normalize_prefix(f"{normalized}/dt_date={partition_label}")


def canonical_table_prefix(base_table_path: str) -> str:
    return _normalize_prefix(base_table_path)


def plan_prior_generation_prefixes(
    base_table_path: str,
    *,
    partition_labels: list[str] | None = None,
    legacy_cleanup_prefix: str | None = None,
) -> tuple[str, ...]:
    planned: list[str] = []
    if legacy_cleanup_prefix:
        planned.append(_normalize_prefix(legacy_cleanup_prefix))
    labels = partition_labels or []
    if labels:
        planned.extend(canonical_partition_prefix(base_table_path, label) for label in labels)
    else:
        planned.append(canonical_table_prefix(base_table_path))
    return tuple(dict.fromkeys(planned))


def default_cleanup_debt_journal() -> GcsCleanupDebtJournal:
    import os

    configured = (Path.cwd() / ".dpone" / "gcs_cleanup_debt.jsonl").resolve()
    env_path = os.environ.get("DPONE_GCS_CLEANUP_DEBT_JOURNAL", "").strip()
    if env_path:
        return FileGcsCleanupDebtJournal(Path(env_path).expanduser())
    state_root = Path.home() / ".dpone" / "state" / "gcs_cleanup_debt.jsonl"
    if state_root.exists():
        return FileGcsCleanupDebtJournal(state_root)
    return FileGcsCleanupDebtJournal(configured)


def record_cleanup_debt(
    prefixes: tuple[str, ...],
    *,
    bucket: str,
    scope: GcsAttemptScope,
    reason: str,
    journal: GcsCleanupDebtJournal | None = None,
) -> list[GcsCleanupDebtRecord]:
    if not prefixes:
        return []
    writer = journal or default_cleanup_debt_journal()
    records = [
        GcsCleanupDebtRecord.create(bucket=bucket, prefix=prefix, scope=scope, reason=reason) for prefix in prefixes
    ]
    try:
        for record in records:
            writer.record(record)
    except Exception as exc:  # pragma: no cover - defensive boundary
        raise CommitOutcomeUnknownError(
            "Target commit succeeded but durable GCS cleanup debt could not be recorded."
        ) from exc
    return records


def retire_prior_generations(
    storage_client: Any,
    *,
    bucket: str,
    prefixes: tuple[str, ...],
    logger_instance: logging.Logger | None = None,
) -> int:
    if not prefixes or not storage_client:
        return 0
    from dpone.runtime.support.gcs import GCSCleaner

    deleted_total = 0
    for prefix in prefixes:
        deleted_total += GCSCleaner.delete_prefix(
            storage_client,
            bucket,
            prefix,
            logger_instance=logger_instance,
        )
    return deleted_total


def cleanup_attempt_prefix(
    storage_client: Any,
    *,
    bucket: str,
    attempt_prefix: str,
    logger_instance: logging.Logger | None = None,
) -> int:
    if not storage_client:
        return 0
    from dpone.runtime.support.gcs import GCSCleaner

    return GCSCleaner.delete_prefix(
        storage_client,
        bucket,
        _normalize_prefix(attempt_prefix),
        logger_instance=logger_instance,
    )


def finalize_gcs_replacement(
    *,
    bucket: str,
    scope: GcsAttemptScope,
    prior_generation_prefixes: tuple[str, ...],
    attempt_table_prefix: str,
    storage_client: Any | None = None,
    journal: GcsCleanupDebtJournal | None = None,
    logger_instance: logging.Logger | None = None,
    retire_after_debt: bool = True,
) -> dict[str, Any]:
    """Record cleanup debt and retire prior generations only after target commit."""

    records = record_cleanup_debt(
        prior_generation_prefixes,
        bucket=bucket,
        scope=scope,
        reason="gcs_replacement_after_target_commit",
        journal=journal,
    )
    deleted = 0
    if retire_after_debt and storage_client is not None and prior_generation_prefixes:
        deleted = retire_prior_generations(
            storage_client,
            bucket=bucket,
            prefixes=prior_generation_prefixes,
            logger_instance=logger_instance,
        )
    return {
        "cleanup_debt_records": len(records),
        "prior_generations_retired": deleted,
        "attempt_table_prefix": _normalize_prefix(attempt_table_prefix),
    }


def _normalize_prefix(prefix: str) -> str:
    normalized = prefix.strip("/")
    return f"{normalized}/" if normalized else ""


def build_gcs_export_identity(
    load_config: Any,
    *,
    bucket_name: str,
    base_table_path: str,
    partition_labels: list[str] | None = None,
) -> tuple[GcsAttemptScope, str, str, tuple[str, ...]]:
    scope = resolve_gcs_attempt_scope(load_config)
    attempt_prefix = derive_attempt_table_prefix(base_table_path, scope)
    attempt_uri = derive_table_attempt_uri(bucket_name, base_table_path, scope)
    options = dict(getattr(load_config, "options", {}) or {})
    prior = plan_prior_generation_prefixes(
        base_table_path,
        partition_labels=partition_labels,
        legacy_cleanup_prefix=options.get("cleanup_prefix"),
    )
    return scope, attempt_prefix, attempt_uri, prior


__all__ = [
    "build_gcs_export_identity",
    "CommitOutcomeUnknownError",
    "FileGcsCleanupDebtJournal",
    "GcsAttemptScope",
    "GcsCleanupDebtJournal",
    "GcsCleanupDebtRecord",
    "GcsObjectIdentity",
    "InMemoryGcsCleanupDebtJournal",
    "canonical_partition_prefix",
    "canonical_table_prefix",
    "cleanup_attempt_prefix",
    "default_cleanup_debt_journal",
    "derive_attempt_table_prefix",
    "derive_partition_attempt_uri",
    "derive_reconciliation_batch_path",
    "derive_table_attempt_uri",
    "finalize_gcs_replacement",
    "plan_prior_generation_prefixes",
    "record_cleanup_debt",
    "resolve_gcs_attempt_scope",
    "retire_prior_generations",
]
