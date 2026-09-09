"""Bounded MSSQL -> ClickHouse safe-sample copy executor.

The executor is deliberately small and dependency-injected. It coordinates
already-resolved runtime ports, enforces the certified request budgets, and
returns only secret-free evidence. It does not know how credentials are resolved
and it never serializes sampled rows.
"""

from __future__ import annotations

import importlib
from collections.abc import Mapping
from typing import Any, Protocol

from dpone.services.safe_sample_execution_common import (
    MssqlSafeSampleBatch,
)
from dpone.services.safe_sample_execution_common import (
    credential_resolution_error as _credential_resolution_error,
)
from dpone.services.safe_sample_execution_common import (
    non_negative_int as _non_negative_int,
)
from dpone.services.safe_sample_execution_common import (
    redact_mapping as _redact_mapping,
)

_SQL_READER_EXPORTS = (
    "MssqlSafeSampleReadPlan",
    "MssqlSafeSampleSqlClient",
    "MssqlSafeSampleSqlReader",
)


class MssqlSafeSampleReader(Protocol):
    """Read a bounded, read-only sample from MSSQL using resolved runtime credentials."""

    def read(self, request: dict[str, Any]) -> MssqlSafeSampleBatch:
        """Return sampled rows and safe source-side metrics."""


class ClickHouseSafeSampleWriter(Protocol):
    """Write a bounded sample batch into an already prepared ClickHouse temporary table."""

    def write(self, request: dict[str, Any], batch: MssqlSafeSampleBatch) -> Mapping[str, Any]:
        """Return safe sink-side metrics."""


class RuntimeCredentialResolver(Protocol):
    """Resolve a connection ref inside the runtime plane."""

    def resolve(self, connection_ref: str) -> Any:
        """Return an object with ``credentials`` and ``safe_metadata`` attributes."""


class MssqlSafeSampleReaderFactory(Protocol):
    """Build an MSSQL sample reader from resolved runtime credentials."""

    def create(self, credentials: Any) -> MssqlSafeSampleReader:
        """Return a reader bound to resolved MSSQL credentials."""


class ClickHouseSafeSampleWriterFactory(Protocol):
    """Build a ClickHouse sample writer from resolved runtime credentials."""

    def create(self, credentials: Any) -> ClickHouseSafeSampleWriter:
        """Return a writer bound to resolved ClickHouse credentials."""


class MssqlClickHouseBoundedSafeSampleCopyExecutor:
    """Execute the certified copy request through injected source/sink ports."""

    def __init__(
        self,
        *,
        reader: MssqlSafeSampleReader,
        writer: ClickHouseSafeSampleWriter,
    ) -> None:
        self._reader = reader
        self._writer = writer

    def copy(self, request: dict[str, Any]) -> Mapping[str, Any]:
        request_errors = _request_errors(request)
        if request_errors:
            return _failed(rows_read=0, bytes_read=0, errors=request_errors)

        try:
            batch = self._reader.read(request)
        except Exception as exc:  # noqa: BLE001 - runtime evidence needs stable error codes.
            return _failed(
                rows_read=0,
                bytes_read=0,
                errors=[_error("DPONE_SAFE_SAMPLE_SOURCE_READ_FAILED", f"MSSQL safe sample read failed: {exc}")],
            )

        budget_errors = _budget_errors(request, batch)
        source_diagnostics = _redact_mapping(batch.diagnostics)
        if budget_errors:
            return _failed(
                rows_read=_non_negative_int(batch.rows_read),
                bytes_read=_non_negative_int(batch.bytes_read),
                errors=budget_errors,
                diagnostics={"source": source_diagnostics} if source_diagnostics else None,
            )

        try:
            writer_result = dict(self._writer.write(request, batch))
        except Exception as exc:  # noqa: BLE001 - runtime evidence needs stable error codes.
            return _failed(
                rows_read=_non_negative_int(batch.rows_read),
                bytes_read=_non_negative_int(batch.bytes_read),
                errors=[_error("DPONE_SAFE_SAMPLE_TARGET_WRITE_FAILED", f"ClickHouse safe sample write failed: {exc}")],
                diagnostics={"source": source_diagnostics} if source_diagnostics else None,
            )

        rows_written = _non_negative_int(writer_result.get("rows_written"))
        write_errors = _write_errors(batch, rows_written)
        diagnostics = _diagnostics(source_diagnostics, _redact_mapping(writer_result.get("diagnostics")))
        if write_errors:
            return _failed(
                rows_read=_non_negative_int(batch.rows_read),
                bytes_read=_non_negative_int(batch.bytes_read),
                rows_written=rows_written,
                errors=write_errors,
                diagnostics=diagnostics,
            )
        return {
            "status": "copied",
            "rows_read": _non_negative_int(batch.rows_read),
            "rows_written": rows_written,
            "bytes_read": _non_negative_int(batch.bytes_read),
            "diagnostics": diagnostics,
            "errors": [],
        }


class CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor:
    """Resolve connection refs at runtime, then delegate to the bounded copy executor."""

    def __init__(
        self,
        *,
        credential_resolver: RuntimeCredentialResolver,
        reader_factory: MssqlSafeSampleReaderFactory,
        writer_factory: ClickHouseSafeSampleWriterFactory,
    ) -> None:
        self._credential_resolver = credential_resolver
        self._reader_factory = reader_factory
        self._writer_factory = writer_factory

    def copy(self, request: dict[str, Any]) -> Mapping[str, Any]:
        source_ref = _connection_ref(request, "source")
        sink_ref = _connection_ref(request, "sink")
        source = self._resolve("source", source_ref)
        if source["error"] is not None:
            return _credential_resolution_failed(source)
        sink = self._resolve("sink", sink_ref)
        if sink["error"] is not None:
            return _credential_resolution_failed(sink)

        reader = self._reader_factory.create(source["credentials"])
        writer = self._writer_factory.create(sink["credentials"])
        result = dict(MssqlClickHouseBoundedSafeSampleCopyExecutor(reader=reader, writer=writer).copy(request))
        diagnostics = _redact_mapping(result.get("diagnostics"))
        diagnostics["credential_resolution"] = {
            "source": _redact_mapping(source["safe_metadata"]),
            "sink": _redact_mapping(sink["safe_metadata"]),
        }
        result["diagnostics"] = diagnostics
        return result

    def _resolve(self, role: str, connection_ref: str) -> dict[str, Any]:
        try:
            resolved = self._credential_resolver.resolve(connection_ref)
        except Exception as exc:  # noqa: BLE001 - error detail may contain secret material.
            safe_error = _credential_resolution_error(exc)
            return {
                "role": role,
                "connection_ref": connection_ref,
                "credentials": None,
                "safe_metadata": {},
                "error": (
                    _error(*safe_error)
                    if safe_error is not None
                    else _error(
                        "DPONE_SAFE_SAMPLE_CREDENTIAL_RESOLUTION_FAILED",
                        f"Safe sample credential resolution failed for {role} connection_ref.",
                    )
                ),
            }
        return {
            "role": role,
            "connection_ref": connection_ref,
            "credentials": getattr(resolved, "credentials", None),
            "safe_metadata": _redact_mapping(getattr(resolved, "safe_metadata", {})),
            "error": None,
        }


def _request_errors(request: Mapping[str, Any]) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    if request.get("source_read_only") is not True:
        errors.append(
            _error(
                "DPONE_SAFE_SAMPLE_COPY_REQUEST_NOT_READ_ONLY",
                "Certified MSSQL -> ClickHouse safe sample copy requires source_read_only=true.",
            )
        )
    if _non_negative_int(request.get("sample_rows")) <= 0:
        errors.append(
            _error(
                "DPONE_SAFE_SAMPLE_ROW_BUDGET_INVALID",
                "Certified MSSQL -> ClickHouse safe sample copy requires a positive sample_rows budget.",
            )
        )
    if _non_negative_int(request.get("max_bytes")) <= 0:
        errors.append(
            _error(
                "DPONE_SAFE_SAMPLE_BYTE_BUDGET_INVALID",
                "Certified MSSQL -> ClickHouse safe sample copy requires a positive max_bytes budget.",
            )
        )
    if _non_negative_int(request.get("timeout_seconds")) <= 0:
        errors.append(
            _error(
                "DPONE_SAFE_SAMPLE_TIMEOUT_INVALID",
                "Certified MSSQL -> ClickHouse safe sample copy requires a positive timeout budget.",
            )
        )
    return errors


def _connection_ref(request: Mapping[str, Any], role: str) -> str:
    section = request.get(role)
    if not isinstance(section, Mapping):
        return ""
    return str(section.get("connection_ref") or "")


def _credential_resolution_failed(resolution: Mapping[str, Any]) -> dict[str, Any]:
    error = resolution.get("error")
    errors = [dict(error)] if isinstance(error, Mapping) else []
    return _failed(
        rows_read=0,
        bytes_read=0,
        errors=errors,
        diagnostics={
            "credential_resolution": {
                "failed_role": str(resolution.get("role") or ""),
                "connection_ref": str(resolution.get("connection_ref") or ""),
            }
        },
    )


def _budget_errors(request: Mapping[str, Any], batch: MssqlSafeSampleBatch) -> list[dict[str, Any]]:
    errors: list[dict[str, Any]] = []
    if _non_negative_int(batch.rows_read) > _non_negative_int(request.get("sample_rows")):
        errors.append(
            _error(
                "DPONE_SAFE_SAMPLE_ROW_BUDGET_EXCEEDED",
                "MSSQL sample reader returned more rows than the certified copy request allowed.",
            )
        )
    if _non_negative_int(batch.bytes_read) > _non_negative_int(request.get("max_bytes")):
        errors.append(
            _error(
                "DPONE_SAFE_SAMPLE_BYTE_BUDGET_EXCEEDED",
                "MSSQL sample reader returned more bytes than the certified copy request allowed.",
            )
        )
    return errors


def _write_errors(batch: MssqlSafeSampleBatch, rows_written: int) -> list[dict[str, Any]]:
    if rows_written == _non_negative_int(batch.rows_read):
        return []
    return [
        _error(
            "DPONE_SAFE_SAMPLE_WRITE_ROW_MISMATCH",
            "ClickHouse temporary target write count must match the bounded MSSQL sample row count.",
        )
    ]


def _failed(
    *,
    rows_read: int,
    bytes_read: int,
    errors: list[dict[str, Any]],
    rows_written: int = 0,
    diagnostics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "status": "failed",
        "rows_read": _non_negative_int(rows_read),
        "rows_written": _non_negative_int(rows_written),
        "bytes_read": _non_negative_int(bytes_read),
        "diagnostics": dict(diagnostics or {}),
        "errors": errors,
    }


def _diagnostics(source: Mapping[str, Any], sink: Mapping[str, Any]) -> dict[str, Any]:
    diagnostics: dict[str, Any] = {}
    if source:
        diagnostics["source"] = dict(source)
    if sink:
        diagnostics["sink"] = dict(sink)
    return diagnostics


def _error(code: str, message: str) -> dict[str, Any]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "mssql_clickhouse_bounded_safe_sample_copy",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


def __getattr__(name: str) -> Any:
    if name not in _SQL_READER_EXPORTS:
        raise AttributeError(name)
    mssql_safe_sample_sql_reader = importlib.import_module("dpone.services.mssql_safe_sample_sql_reader")
    return getattr(mssql_safe_sample_sql_reader, name)


__all__ = [
    "ClickHouseSafeSampleWriter",
    "ClickHouseSafeSampleWriterFactory",
    "CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor",
    "MssqlClickHouseBoundedSafeSampleCopyExecutor",
    "MssqlSafeSampleBatch",
    "MssqlSafeSampleReader",
    "MssqlSafeSampleReaderFactory",
    *_SQL_READER_EXPORTS,
    "RuntimeCredentialResolver",
]
