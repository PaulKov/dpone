"""Owned materialization boundary for MSSQL bulk-text decoding."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import replace
from hashlib import sha256
from time import perf_counter, sleep
from typing import Any, Literal, NoReturn

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.sink_logging import etl_logger
from dpone.runtime.sinks.strategies.mssql.mssql_decoded_staging_capacity import (
    MssqlDecodedStagingCapacityPolicy,
    _DecodedStagingAdmission,
    _MssqlDecodedStagingAdmissionService,
)
from dpone.runtime.support.mssql_native_projection import decoded_staging_expression

_MAX_STAGING_TABLE_LENGTH = 120
_DERIVED_NAME_DIGEST_LENGTH = 12
_CLEANUP_ATTEMPTS = 3
_CLEANUP_BACKOFF_SECONDS = (0.05, 0.1)


class MssqlDecodedStagingMaterializer:
    """Own a raw-shaped table whose text codec is evaluated exactly once.

    SQL Server can inline CTE and ``APPLY`` scalar expressions.  A physical
    staging boundary prevents the lossless conversion guards from evaluating
    the nested bulk-text decoder repeatedly for every predicate.  This owner
    eagerly attempts cleanup and the table never becomes target evidence
    authority.
    """

    def __init__(
        self,
        strategy: Any,
        *,
        capacity_policy: MssqlDecodedStagingCapacityPolicy | None = None,
    ) -> None:
        self._strategy = strategy
        self._connector = strategy.connector
        self._staging = strategy.staging_manager
        self._admission = _MssqlDecodedStagingAdmissionService(
            strategy,
            capacity_policy or MssqlDecodedStagingCapacityPolicy(),
        )

    @contextmanager
    def materialize(
        self,
        load_config: Any,
        raw: StagingTableArtifact,
        *,
        error_prefix: str,
    ) -> Iterator[StagingTableArtifact]:
        """Yield a decoded stage, or the unchanged raw stage when unnecessary."""

        raw_row_count = _require_row_count(raw.row_count, error_prefix=error_prefix)
        if raw.bulk_text_codec is None:
            yield raw
            return

        with self._staging.database_authority_scope(raw):
            admission = self._admission.decide(raw, row_count=raw_row_count)
        _log_decoded_admission(self._strategy, admission, rows=raw_row_count)
        if not admission.materialize:
            yield raw
            return

        schema = self._raw_physical_schema(raw, error_prefix=error_prefix)
        options = dict(getattr(load_config, "options", {}) or {})
        options.update(
            {
                "__dpone_mssql_native_staging": True,
                "__dpone_mssql_native_column_types": dict(schema),
                "__dpone_mssql_native_not_null_columns": [],
                "__dpone_mssql_native_collations": dict(raw.target_column_collations),
            }
        )
        config = replace(
            load_config,
            staging_table=derived_staging_table_name(raw.table, "decoded"),
            options=options,
        )
        decoded = self._staging.create(config, schema)
        try:
            with self._staging.database_authority_scope(decoded):
                with mssql_native_phase(self._strategy, "decode_materialize", rows=raw_row_count):
                    columns = ", ".join(self._connector.quote_identifier(column) for column, _dtype in schema)
                    values = ", ".join(
                        f"{decoded_staging_expression(self._strategy, raw, column, 'r')} "
                        f"AS {self._connector.quote_identifier(column)}"
                        for column, _dtype in schema
                    )
                    self._connector.execute_query(
                        f"INSERT INTO {self._strategy._staging_name(decoded)} WITH (TABLOCK) ({columns}) "
                        f"SELECT {values} FROM {self._strategy._staging_name(raw)} AS r"
                    )
                    decoded.row_count = self._count_rows(decoded, error_prefix=error_prefix)
                    if decoded.row_count != raw_row_count:
                        _raise(error_prefix, "decoded_staging_count_mismatch")
                    decoded.bulk_text_codec = None
                yield decoded
        except BaseException:
            cleanup_staging_after_primary(self._strategy, decoded, phase="decoded_cleanup")
            raise
        else:
            _cleanup_staging(
                self._strategy,
                decoded,
                phase="decoded_cleanup",
                preserve_primary=False,
            )

    def _count_rows(self, artifact: StagingTableArtifact, *, error_prefix: str) -> int:
        rows = self._connector.get_records(
            f"SELECT COUNT_BIG(*) AS row_count FROM {self._strategy._staging_name(artifact)}",
            as_dict=True,
        )
        if len(rows) != 1:
            _raise(error_prefix, "decoded_staging_count_unavailable")
        row = rows[0]
        value = row.get("row_count") if isinstance(row, Mapping) else row[0]
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            _raise(error_prefix, "decoded_staging_count_invalid")
        return value

    @staticmethod
    def _raw_physical_schema(
        raw: StagingTableArtifact,
        *,
        error_prefix: str,
    ) -> tuple[tuple[str, str], ...]:
        schema: list[tuple[str, str]] = []
        for value in raw.columns:
            column = str(value)
            dtype = raw.column_types.get(column)
            if not isinstance(dtype, str) or not dtype.strip():
                _raise(error_prefix, "decoded_staging_shape_invalid")
            schema.append((column, dtype))
        return tuple(schema)


def derived_staging_table_name(raw_table: str, role: Literal["decoded", "native"]) -> str:
    """Derive a bounded, collision-resistant name without truncating run identity."""

    raw = str(raw_table)
    digest = sha256(f"{role}\0{raw}".encode()).hexdigest()[:_DERIVED_NAME_DIGEST_LENGTH]
    suffix = f"_{role}_{digest}"
    prefix_length = _MAX_STAGING_TABLE_LENGTH - len(suffix)
    return f"{raw[:prefix_length]}{suffix}"


def _require_row_count(value: object, *, error_prefix: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        _raise(error_prefix, "decoded_staging_raw_count_invalid")
    return value


def cleanup_staging_after_primary(strategy: Any, artifact: StagingTableArtifact, *, phase: str) -> None:
    """Bound cleanup without replacing an already authoritative error."""

    _cleanup_staging(
        strategy,
        artifact,
        phase=phase,
        preserve_primary=True,
    )


def _cleanup_staging(
    strategy: Any,
    artifact: StagingTableArtifact,
    *,
    phase: str,
    preserve_primary: bool,
) -> None:
    """Retry an idempotent DROP within a fixed budget and expose residue."""

    last_error: BaseException | None = None
    for attempt in range(1, _CLEANUP_ATTEMPTS + 1):
        try:
            artifact.cleanup()
        except BaseException as error:  # noqa: BLE001 - cleanup must preserve the primary failure.
            last_error = error
            if attempt < _CLEANUP_ATTEMPTS:
                sleep(_CLEANUP_BACKOFF_SECONDS[attempt - 1])
                continue
        else:
            if attempt > 1:
                _log_native_phase(
                    strategy,
                    "info",
                    f"MSSQL native phase: phase={phase} status=cleanup_recovered attempts={attempt} "
                    f"table={_artifact_log_name(artifact)}",
                )
            return
        break
    if last_error is None:  # pragma: no cover - loop structure guarantees an error here.
        return
    _log_native_phase(
        strategy,
        "warning",
        f"MSSQL native phase: phase={phase} status=residue_retained attempts={_CLEANUP_ATTEMPTS} "
        f"table={_artifact_log_name(artifact)} error_type={type(last_error).__name__}",
    )
    if not preserve_primary:
        raise last_error


def _log_decoded_admission(
    strategy: Any,
    admission: _DecodedStagingAdmission,
    *,
    rows: int,
) -> None:
    status = "materialized" if admission.materialize else "inline_fallback"
    details = ""
    if admission.required_data_bytes is not None:
        details = (
            f" raw_reserved_bytes={admission.raw_reserved_bytes}"
            f" required_data_bytes={admission.required_data_bytes}"
            f" available_data_bytes={admission.available_data_bytes}"
        )
    _log_native_phase(
        strategy,
        "info",
        f"MSSQL native phase: phase=decode_admission status={status} reason={admission.reason} rows={rows}{details}",
    )


def _artifact_log_name(artifact: Any) -> str:
    parts = [getattr(artifact, "database", None), getattr(artifact, "schema", None), getattr(artifact, "table", None)]
    return ".".join(_quote_log_identifier(part) for part in parts if part)


def _quote_log_identifier(value: object) -> str:
    return f"[{str(value).replace(']', ']]')}]"


@contextmanager
def mssql_native_phase(strategy: Any, phase: str, *, rows: int) -> Iterator[None]:
    """Emit a common monotonic phase duration without changing evidence."""

    started_at = perf_counter()
    try:
        yield
    except Exception:
        _log_native_phase(
            strategy,
            "warning",
            f"MSSQL native phase: phase={phase} status=failed rows={rows} "
            f"duration_seconds={perf_counter() - started_at:.6f}",
        )
        raise
    _log_native_phase(
        strategy,
        "info",
        f"MSSQL native phase: phase={phase} status=success rows={rows} "
        f"duration_seconds={perf_counter() - started_at:.6f}",
    )


def _log_native_phase(strategy: Any, level: str, message: str) -> None:
    """Best-effort single-message logging that cannot change load outcome."""

    logger = getattr(strategy, "logger", etl_logger)
    log = getattr(logger, level, None)
    if not callable(log):
        return
    try:
        log(message)
    except BaseException:  # noqa: BLE001 - phase telemetry is never an execution authority.
        pass


def _raise(prefix: str, suffix: str) -> NoReturn:
    separator = "" if prefix.endswith((".", "_")) else "."
    raise SnapshotReconciliationError(f"{prefix}{separator}{suffix}")


__all__ = [
    "MssqlDecodedStagingCapacityPolicy",
    "MssqlDecodedStagingMaterializer",
    "cleanup_staging_after_primary",
    "derived_staging_table_name",
    "mssql_native_phase",
]
