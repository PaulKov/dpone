"""Bounded last-known-good publication for Airflow cache status evidence."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime  # type: ignore[attr-defined]
from pathlib import Path, PurePath
from typing import Any

from dpone.ports.airflow_cache_status_publication import (
    AirflowCacheStatusFileStorage,
    AirflowCacheStatusFileTransaction,
    AirflowCacheStatusSchemaValidator,
    AirflowCacheStatusStorageError,
)

CACHE_STATUS_MAX_BYTES = 8 * 1024 * 1024
_PUBLICATION_SCHEMA = "dpone.airflow-cache-status-publication.v1"
_FAILURE_SCHEMA = "dpone.airflow-cache-status-publication-failure.v1"


@dataclass(frozen=True)
class AirflowCacheStatusPublicationRequest:
    status_root: Path
    source_name: str
    target_name: str
    failure_marker_name: str
    expected_schema: str


@dataclass(frozen=True)
class CacheStatusPublicationReport:
    passed: bool
    status: str
    source: str
    target: str
    failure_marker: str
    expected_schema: str
    attempted_at: str
    exit_code: int
    source_sha256: str | None = None
    target_sha256: str | None = None
    error_code: str | None = None
    diagnostic_error_code: str | None = None

    def to_dict(self) -> dict[str, object]:
        payload: dict[str, object] = {
            "schema": _PUBLICATION_SCHEMA,
            "passed": self.passed,
            "status": self.status,
            "source": self.source,
            "target": self.target,
            "failure_marker": self.failure_marker,
            "expected_schema": self.expected_schema,
            "attempted_at": self.attempted_at,
        }
        if self.source_sha256 is not None:
            payload["source_sha256"] = self.source_sha256
        if self.target_sha256 is not None:
            payload["target_sha256"] = self.target_sha256
        if self.error_code is not None:
            payload["error_code"] = self.error_code
        if self.diagnostic_error_code is not None:
            payload["diagnostic_error_code"] = self.diagnostic_error_code
        return payload


class AirflowCacheStatusPublisher:
    """Validate producer evidence before replacing its last-known-good status."""

    def __init__(
        self,
        *,
        validator: AirflowCacheStatusSchemaValidator,
        storage: AirflowCacheStatusFileStorage,
        now: Callable[[], str] | None = None,
        lock_timeout_seconds: float = 5.0,
    ) -> None:
        if lock_timeout_seconds <= 0:
            raise ValueError("lock timeout must be positive")
        self._now = now or _utc_now
        self._validator = validator
        self._storage = storage
        self._lock_timeout_seconds = lock_timeout_seconds

    def publish(self, request: AirflowCacheStatusPublicationRequest) -> CacheStatusPublicationReport:
        attempted_at = self._now()
        try:
            names = _validated_names(request)
            with self._storage.transaction(
                request.status_root,
                lock_timeout_seconds=self._lock_timeout_seconds,
            ) as transaction:
                return self._publish_locked(
                    request,
                    transaction=transaction,
                    names=names,
                    attempted_at=attempted_at,
                )
        except _PublicationFailure as exc:
            return _report(
                request,
                attempted_at=attempted_at,
                code=exc.code,
                exit_code=exc.exit_code,
                status=exc.status,
            )
        except AirflowCacheStatusStorageError as exc:
            return _report(
                request,
                attempted_at=attempted_at,
                code=exc.code,
                exit_code=_storage_exit_code(exc.code),
                status="commit_unknown" if exc.state_may_have_changed else "rejected",
                target_sha256=exc.target_sha256,
            )

    def _publish_locked(
        self,
        request: AirflowCacheStatusPublicationRequest,
        *,
        transaction: AirflowCacheStatusFileTransaction,
        names: tuple[str, str, str],
        attempted_at: str,
    ) -> CacheStatusPublicationReport:
        source_name, target_name, marker_name = names
        try:
            transaction.require_safe_destination(target_name)
            transaction.require_safe_destination(marker_name)
            document = transaction.read_source(source_name, max_bytes=CACHE_STATUS_MAX_BYTES)
            payload = document.payload
            self._validate_payload(payload, request.expected_schema)
        except _PublicationFailure as validation_failure:
            return self._reject(
                request,
                transaction=transaction,
                names=names,
                attempted_at=attempted_at,
                failure=validation_failure,
            )
        except AirflowCacheStatusStorageError as exc:
            storage_failure = _PublicationFailure(exc.code, _storage_exit_code(exc.code))
            return self._reject(
                request,
                transaction=transaction,
                names=names,
                attempted_at=attempted_at,
                failure=storage_failure,
            )
        try:
            target_sha256 = transaction.commit(target_name, payload)
        except AirflowCacheStatusStorageError as exc:
            commit_failure = _PublicationFailure(
                exc.code,
                4,
                status="commit_unknown" if exc.state_may_have_changed else "rejected",
            )
            return self._reject(
                request,
                transaction=transaction,
                names=names,
                attempted_at=attempted_at,
                failure=commit_failure,
                source_sha256=document.sha256,
                target_sha256=exc.target_sha256,
            )
        try:
            transaction.remove_and_sync(marker_name)
        except AirflowCacheStatusStorageError:
            return CacheStatusPublicationReport(
                passed=True,
                status="published_with_warning",
                source=source_name,
                target=target_name,
                failure_marker=marker_name,
                expected_schema=request.expected_schema,
                attempted_at=attempted_at,
                exit_code=1,
                source_sha256=document.sha256,
                target_sha256=target_sha256,
                error_code="DPONE_AIRFLOW_CACHE_STATUS_MARKER_CLEANUP_FAILED",
            )
        return CacheStatusPublicationReport(
            passed=True,
            status="published",
            source=source_name,
            target=target_name,
            failure_marker=marker_name,
            expected_schema=request.expected_schema,
            attempted_at=attempted_at,
            exit_code=0,
            source_sha256=document.sha256,
            target_sha256=target_sha256,
        )

    def _validate_payload(self, payload: dict[str, Any], expected_schema: str) -> None:
        if not self._validator.supports(expected_schema):
            raise _PublicationFailure("DPONE_AIRFLOW_CACHE_STATUS_SCHEMA_UNKNOWN", 2)
        if payload.get("schema") != expected_schema:
            raise _PublicationFailure("DPONE_AIRFLOW_CACHE_STATUS_SCHEMA_MISMATCH", 1)
        if self._validator.has_violations(payload, expected_schema=expected_schema):
            raise _PublicationFailure("DPONE_AIRFLOW_CACHE_STATUS_SCHEMA_INVALID", 1)

    @staticmethod
    def _reject(
        request: AirflowCacheStatusPublicationRequest,
        *,
        transaction: AirflowCacheStatusFileTransaction,
        names: tuple[str, str, str],
        attempted_at: str,
        failure: _PublicationFailure,
        source_sha256: str | None = None,
        target_sha256: str | None = None,
    ) -> CacheStatusPublicationReport:
        source_name, target_name, marker_name = names
        marker_payload = {
            "schema": _FAILURE_SCHEMA,
            "status": failure.status,
            "source": source_name,
            "target": target_name,
            "expected_schema": request.expected_schema,
            "attempted_at": attempted_at,
            "error_code": failure.code,
        }
        diagnostic_error_code: str | None = None
        try:
            transaction.write_diagnostic(marker_name, marker_payload)
        except AirflowCacheStatusStorageError:
            diagnostic_error_code = "DPONE_AIRFLOW_CACHE_STATUS_FAILURE_MARKER_UNAVAILABLE"
        return _report(
            request,
            attempted_at=attempted_at,
            code=failure.code,
            exit_code=failure.exit_code,
            status=failure.status,
            diagnostic_error_code=diagnostic_error_code,
            source_sha256=source_sha256,
            target_sha256=target_sha256,
        )


class _PublicationFailure(RuntimeError):
    def __init__(self, code: str, exit_code: int, *, status: str = "rejected") -> None:
        super().__init__(code)
        self.code = code
        self.exit_code = exit_code
        self.status = status


def _validated_names(request: AirflowCacheStatusPublicationRequest) -> tuple[str, str, str]:
    names = (request.source_name, request.target_name, request.failure_marker_name)
    if any(not _is_basename(name) for name in names) or len(set(names)) != len(names):
        raise _PublicationFailure("DPONE_AIRFLOW_CACHE_STATUS_PATH_UNSAFE", 4)
    if not request.expected_schema or len(request.expected_schema) > 256:
        raise _PublicationFailure("DPONE_AIRFLOW_CACHE_STATUS_SCHEMA_UNKNOWN", 2)
    return names


def _is_basename(value: str) -> bool:
    return (
        bool(value) and len(value.encode("utf-8")) <= 255 and PurePath(value).name == value and value not in {".", ".."}
    )


def _report(
    request: AirflowCacheStatusPublicationRequest,
    *,
    attempted_at: str,
    code: str,
    exit_code: int,
    status: str = "rejected",
    diagnostic_error_code: str | None = None,
    source_sha256: str | None = None,
    target_sha256: str | None = None,
) -> CacheStatusPublicationReport:
    return CacheStatusPublicationReport(
        passed=False,
        status=status,
        source=request.source_name,
        target=request.target_name,
        failure_marker=request.failure_marker_name,
        expected_schema=request.expected_schema,
        attempted_at=attempted_at,
        exit_code=exit_code,
        error_code=code,
        diagnostic_error_code=diagnostic_error_code,
        source_sha256=source_sha256,
        target_sha256=target_sha256,
    )


def _utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _storage_exit_code(code: str) -> int:
    return 4 if code.endswith(("_UNSAFE", "_OVERSIZED", "_UNAVAILABLE", "_UNKNOWN")) else 1


__all__ = [
    "CACHE_STATUS_MAX_BYTES",
    "AirflowCacheStatusPublicationRequest",
    "AirflowCacheStatusPublisher",
    "CacheStatusPublicationReport",
]
