from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Lock, Semaphore
from typing import Any

from dpone.runtime.columnar_range_parallelism import RangeParallelismPreflight
from dpone.runtime.partitioning import RangePartition, RangePartitioner
from dpone.runtime.partitioning_predicates import MssqlPartitionPredicateRenderer


class ColumnarRangeSession:
    """Independently owned MSSQL session with a best-effort cancellation port."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def __getattr__(self, name: str) -> Any:
        return getattr(self._connector, name)

    def cancel(self) -> None:
        cancel = getattr(self._connector, "cancel", None)
        if callable(cancel):
            cancel()
            return
        connection = getattr(self._connector, "_connection", None)
        connection_cancel = getattr(connection, "cancel", None)
        if callable(connection_cancel):
            connection_cancel()

    def close(self) -> None:
        close = getattr(self._connector, "close", None)
        if callable(close):
            close()


class BoundedColumnarUploadLane:
    """Execute uploads on an independent bounded worker lane."""

    def __init__(self, workers: int) -> None:
        self._semaphore = Semaphore(workers)
        self._executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dpone-columnar-upload")
        self._lock = Lock()
        self._active = 0
        self.observed = 0

    def run(self, operation: Callable[[], Any]) -> Any:
        self._semaphore.acquire()
        try:
            return self._executor.submit(self._run_one, operation).result()
        finally:
            self._semaphore.release()

    def _run_one(self, operation: Callable[[], Any]) -> Any:
        with self._lock:
            self._active += 1
            self.observed = max(self.observed, self._active)
        try:
            return operation()
        finally:
            with self._lock:
                self._active -= 1

    def close(self, *, cancel: bool) -> None:
        self._executor.shutdown(wait=True, cancel_futures=cancel)


def resolve_range_consistency(options: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    partitioning = options.get("partitioning")
    partitioning = dict(partitioning) if isinstance(partitioning, dict) else {}
    parallelism = partitioning.get("range_parallelism")
    parallelism = dict(parallelism) if isinstance(parallelism, dict) else {}
    if str(parallelism.get("mode") or "off").strip().lower() == "off":
        return "immutable", {}
    if "consistency" not in parallelism:
        raise ValueError("columnar_range_parallelism_requires_explicit_consistency")
    consistency = str(parallelism["consistency"]).strip().lower()
    raw_authority = parallelism.get("consistency_authority")
    authority = dict(raw_authority) if isinstance(raw_authority, dict) else {}
    if consistency == "temporal_as_of":
        as_of = str(authority.get("as_of") or "").strip()
        pattern = r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.(\d{1,7}))?(?:Z|[+-]\d{2}:\d{2})?"
        match = re.fullmatch(pattern, as_of)
        try:
            parseable = as_of.replace("Z", "+00:00")
            fraction = match.group(1) if match is not None else None
            if fraction and len(fraction) == 7:
                parseable = parseable.replace(f".{fraction}", f".{fraction[:6]}", 1)
            datetime.fromisoformat(parseable)
        except ValueError:
            match = None
        if match is None:
            raise ValueError("columnar_temporal_as_of_invalid")
    return consistency, authority


def authority_database(consistency: str, authority: dict[str, Any], default: str | None) -> str | None:
    if consistency != "database_snapshot":
        return default
    database = str(authority.get("database_snapshot") or "").strip()
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_$#@]{0,127}", database):
        raise ValueError("columnar_database_snapshot_name_invalid")
    return database


def verify_write_exclusion(
    consistency: str,
    authority: dict[str, Any],
    *,
    verifier: Callable[..., bool] | None,
    connector: Any,
    load_config: Any,
) -> None:
    if consistency != "write_exclusion":
        return
    reference = str(authority.get("write_exclusion_ref") or "").strip()
    if not reference or verifier is None:
        raise RuntimeError("columnar_write_exclusion_verifier_missing")
    if not verifier(reference=reference, connector=connector, load_config=load_config):
        raise RuntimeError("columnar_write_exclusion_not_verified")


def consistency_binding(consistency: str, authority: dict[str, Any], database: str | None) -> dict[str, object]:
    if consistency == "database_snapshot":
        return {"mode": consistency, "database_snapshot": database or ""}
    if consistency == "temporal_as_of":
        return {"mode": consistency, "as_of": str(authority.get("as_of") or "")}
    if consistency == "write_exclusion":
        return {
            "mode": consistency,
            "write_exclusion_ref": str(authority.get("write_exclusion_ref") or ""),
            "verified": True,
        }
    return {"mode": "immutable"}


def resolve_partition_bounds(connector: Any, *, query: str, column: str, source_type: str) -> tuple[Any, ...]:
    normalized = str(source_type).lower().replace(" nullable", "").strip()
    if re.match(r"datetime(?:2|offset)\(7\)", normalized):
        raise ValueError("columnar_auto_bounds_100ns_requires_explicit_ranges")
    quote = getattr(connector, "quote_identifier", None)
    quoted = quote(column) if callable(quote) else "[" + column.replace("]", "]]") + "]"
    expression = quoted
    if normalized.startswith("datetime2"):
        expression = f"CONVERT(varchar(33), {quoted}, 121)"
    elif normalized.startswith("datetimeoffset"):
        expression = f"CONVERT(varchar(40), SWITCHOFFSET({quoted}, '+00:00'), 127)"
    rows = connector.get_records(
        "SELECT "
        f"MIN({expression}), MAX({expression}), COUNT_BIG(1), "
        f"SUM(CASE WHEN {quoted} IS NULL THEN 1 ELSE 0 END) "
        f"FROM ({query}) AS dpone_bounds"
    )
    if not rows or rows[0][0] is None or rows[0][1] is None:
        raise ValueError("columnar_partition_bounds_unavailable")
    lower, upper, count, null_count = rows[0]
    return lower, upper, int(count), int(null_count or 0)


def range_read_blockers(request: Any, connector: Any) -> tuple[str, ...]:
    """Return fail-closed range admission blockers before any source I/O."""

    if request.range_plan is None:
        return ()
    blockers: list[str] = []
    if not hasattr(connector, "open_session"):
        blockers.append("mssql_independent_range_sessions_unavailable")
    if request.uri_prefix.startswith("local://"):
        blockers.append("columnar_range_parallelism_requires_object_storage")
    if "{run_id}" not in request.uri_prefix:
        blockers.append("columnar_range_uri_prefix_must_include_run_id")
    partitioner = request.range_partitioner
    if partitioner is None:
        blockers.append("columnar_range_partitioner_missing")
        return tuple(blockers)
    if partitioner.column.casefold() not in {column.casefold() for column, _ in request.schema}:
        blockers.append("columnar_range_column_not_projected")
    policy = request.range_plan.policy
    binding = dict(dict(request.options or {}).get("range_consistency_binding") or {})
    authority = dict(policy.consistency_authority)
    if policy.consistency == "database_snapshot" and binding.get("database_snapshot") != authority.get(
        "database_snapshot"
    ):
        blockers.append("columnar_database_snapshot_not_bound")
    if policy.consistency == "temporal_as_of" and binding.get("as_of") != authority.get("as_of"):
        blockers.append("columnar_temporal_as_of_not_bound")
    if policy.consistency == "write_exclusion" and (
        binding.get("write_exclusion_ref") != authority.get("write_exclusion_ref")
        or binding.get("verified") is not True
    ):
        blockers.append("columnar_write_exclusion_not_verified")
    if request.max_chunk_bytes > policy.max_inflight_bytes:
        blockers.append("columnar_range_max_chunk_exceeds_inflight_byte_budget")
    batch_size = dict(request.options or {}).get("batch_size", 10_000)
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size <= 0:
        blockers.append("columnar_range_batch_size_invalid")
    elif batch_size > policy.max_inflight_rows:
        blockers.append("columnar_range_batch_size_exceeds_inflight_row_budget")
    try:
        RangeParallelismPreflight.validate(
            request.range_plan.policy,
            partition_column=partitioner.column,
            query_has_window_functions=bool(re.search(r"\bover\s*\(", request.query, flags=re.IGNORECASE)),
            supported_topologies={"shared_per_run", "per_partition"},
        )
    except ValueError as exc:
        blockers.append(f"columnar_range_preflight:{exc}")
    return tuple(blockers)


def iter_columnar_batches(
    connector: Any,
    *,
    query: str,
    schema: Sequence[tuple[str, str]],
    batch_size: int,
    partitioner: RangePartitioner | None = None,
    partition: RangePartition | None = None,
):
    """Stream values after SQL-side projection of ODBC-lossy temporal types."""

    read_query = parquet_read_query(query, schema)
    if partitioner is not None or partition is not None:
        if partitioner is None or partition is None:
            raise ValueError("Both partitioner and partition are required for a ranged columnar read.")
        read_query = partitioner.wrap_query(
            read_query,
            _quote_identifier(partitioner.column),
            partition,
            renderer=MssqlPartitionPredicateRenderer(),
        )
    yield from connector.get_records_streaming(
        read_query,
        batch_size=batch_size,
        as_dict=False,
    )


def parquet_read_query(query: str, schema: Sequence[tuple[str, str]]) -> str:
    """Project 100ns MSSQL values to exact text before pyodbc truncates them."""

    if not any(_requires_text_projection(source_type) for _, source_type in schema):
        return query
    source_query = query.strip().removesuffix(";").strip()
    columns = ",\n    ".join(_projection(column, source_type) for column, source_type in schema)
    return f"SELECT\n    {columns}\nFROM (\n{source_query}\n) AS dpone_columnar_src"


def _projection(column: str, source_type: str) -> str:
    quoted = _quote_identifier(column)
    source = f"dpone_columnar_src.{quoted}"
    normalized = _normalize_source_type(source_type)
    base = normalized.split("(", 1)[0]
    if base == "datetime2":
        return f"CONVERT(VARCHAR(33), {source}, 121) AS {quoted}"
    if base == "time":
        return f"CONVERT(VARCHAR(16), {source}) AS {quoted}"
    if base == "datetimeoffset":
        utc = f"CAST(SWITCHOFFSET(CAST({source} AS datetimeoffset), '+00:00') AS datetime2(7))"
        return f"CONVERT(VARCHAR(33), {utc}, 121) AS {quoted}"
    return f"{source} AS {quoted}"


def _requires_text_projection(source_type: str) -> bool:
    base = _normalize_source_type(source_type).split("(", 1)[0]
    return base in {"datetime2", "datetimeoffset", "time"}


def _normalize_source_type(source_type: str) -> str:
    normalized = str(source_type).strip().lower().replace(" nullable", "").strip()
    if normalized.startswith("nullable(") and normalized.endswith(")"):
        normalized = normalized.removeprefix("nullable(").removesuffix(")").strip()
    return re.sub(r"\s+", "", normalized)


def _quote_identifier(value: str) -> str:
    return "[" + str(value).replace("]", "]]") + "]"


__all__ = [
    "BoundedColumnarUploadLane",
    "ColumnarRangeSession",
    "iter_columnar_batches",
    "parquet_read_query",
    "range_read_blockers",
    "authority_database",
    "consistency_binding",
    "resolve_partition_bounds",
    "resolve_range_consistency",
    "verify_write_exclusion",
]
