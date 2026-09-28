"""Artifact classification and optional loader factories for ClickHouse ingestion."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from threading import Lock
from typing import Any


@dataclass(frozen=True, slots=True)
class ClickHouseRangeStagingCallbacks:
    """Injected ClickHouse operations for range-owned staging resources."""

    clone_connector: Callable[[int], Any]
    plan_partition: Callable[[Any, int], Any]
    create_partition: Callable[[Any, Any], None]
    assemble_partitions: Callable[[tuple[Any, ...], Any], int]
    drop_partition: Callable[[Any], None]


@dataclass(frozen=True, slots=True)
class RangeWindowGroup:
    range_id: str
    ordinal: int
    windows: tuple[Any, ...]

    @property
    def expected_rows(self) -> int:
        return sum(int(getattr(window, "row_count", 0) or 0) for window in self.windows)


class RangeLoadConcurrencyTracker:
    def __init__(self) -> None:
        self._lock = Lock()
        self._active = 0
        self.maximum = 0

    def enter(self) -> None:
        with self._lock:
            self._active += 1
            self.maximum = max(self.maximum, self._active)

    def leave(self) -> None:
        with self._lock:
            self._active -= 1


def is_source_native_artifact(artifact: Any) -> bool:
    return getattr(artifact, "native_wire_contract", None) is not None


def native_wire_transcoder() -> Any:
    return import_module("dpone.runtime.native_wire_transcoder").NativeWireTranscoder()


def native_wire_source_schema(artifact: Any, schema: Sequence[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    """Read physical export types only after matching artifact and payload column order.

    Logical payload types can be normalized for the destination. They must not
    describe BCP framing, whose authoritative types are sealed in the export.
    """
    source_schema = tuple((column.name, column.source_type) for column in artifact.native_wire_contract.columns)
    source_columns = tuple(column for column, _ in source_schema)
    if source_columns != tuple(artifact.columns) or source_columns != tuple(column for column, _ in schema):
        raise ValueError("native_wire_source_schema_mismatch:column_identity")
    return source_schema


def columnar_pull_loader(sink: Any) -> Any:
    module = import_module("dpone.runtime.sinks.clickhouse_columnar_pull")
    return module.ClickHouseColumnarPullLoader(
        connector=sink.connector,
        table_name=sink._table,
        count_rows=sink._count,
        range_staging=_range_staging_callbacks(sink),
    )


def columnar_direct_push_loader(sink: Any) -> Any:
    module = import_module("dpone.runtime.sinks.clickhouse_columnar_direct_push")
    return module.ClickHouseColumnarDirectPushLoader(
        table_name=sink._table,
        count_rows=sink._count,
        client_runner_factory=sink._build_client_runner,
        http_runner_factory=sink._build_http_runner,
    )


def is_object_storage_staging_manifest(artifact: Any) -> bool:
    return artifact.__class__.__name__ == "ObjectStorageStagingManifest" and hasattr(artifact, "chunks")


def is_object_storage_columnar_chunked_artifact(artifact: Any) -> bool:
    return artifact.__class__.__name__ == "ObjectStorageColumnarChunkedArtifact" and hasattr(artifact, "iter_windows")


def is_local_columnar_staging_manifest(artifact: Any) -> bool:
    return artifact.__class__.__name__ == "LocalColumnarStagingManifest" and hasattr(artifact, "chunks")


def is_local_columnar_chunked_artifact(artifact: Any) -> bool:
    return artifact.__class__.__name__ == "LocalColumnarChunkedArtifact" and hasattr(artifact, "iter_chunks")


def range_staging_policy(artifact: Any) -> tuple[str, int] | None:
    request = getattr(artifact, "request", None)
    plan = getattr(request, "range_plan", None)
    candidates = (
        getattr(artifact, "range_parallelism_policy", None),
        getattr(request, "range_parallelism_policy", None),
        getattr(plan, "policy", None),
    )
    policy = next((item for item in candidates if item is not None), None)
    if policy is None:
        options = getattr(request, "options", None)
        if isinstance(options, Mapping):
            partitioning = options.get("partitioning")
            if isinstance(partitioning, Mapping):
                policy = partitioning.get("range_parallelism")
            if policy is None:
                policy = options.get("range_parallelism")
    if policy is None:
        return None
    if isinstance(policy, Mapping):
        topology = policy.get("staging_topology", "shared_per_run")
        workers = policy.get("load_workers", 1)
    else:
        topology = getattr(policy, "staging_topology", "shared_per_run")
        workers = getattr(policy, "load_workers", 1)
    if topology not in {"shared_per_run", "per_partition"}:
        raise ValueError("clickhouse_range_staging_topology_invalid")
    if isinstance(workers, bool) or not isinstance(workers, int) or workers < 1:
        raise ValueError("clickhouse_range_load_workers_invalid")
    return str(topology), workers


def group_range_windows(windows: tuple[Any, ...]) -> tuple[RangeWindowGroup, ...]:
    grouped: dict[int, list[Any]] = defaultdict(list)
    identities: dict[int, str] = {}
    for window in windows:
        range_id = getattr(window, "range_id", None)
        ordinal = getattr(window, "range_ordinal", None)
        if not isinstance(range_id, str) or not range_id or isinstance(ordinal, bool) or not isinstance(ordinal, int):
            raise ValueError("clickhouse_range_window_identity_missing")
        if ordinal < 0 or (ordinal in identities and identities[ordinal] != range_id):
            raise ValueError("clickhouse_range_window_identity_invalid")
        identities[ordinal] = range_id
        grouped[ordinal].append(window)
    ordinals = tuple(sorted(grouped))
    if ordinals != tuple(range(len(ordinals))) or len(set(identities.values())) != len(identities):
        raise ValueError("clickhouse_range_window_ordinals_invalid")
    return tuple(RangeWindowGroup(identities[ordinal], ordinal, tuple(grouped[ordinal])) for ordinal in ordinals)


def count_connector_rows(connector: Any, table_name: str) -> int:
    rows = connector.get_records(f"SELECT count() FROM {table_name}")
    return int(rows[0][0]) if rows else 0


def cleanup_range_resources(
    windows: Sequence[Any],
    partitions: Sequence[Any],
    *,
    drop_partition: Callable[[Any], None] | None,
) -> BaseException | None:
    failure: BaseException | None = None
    for partition in reversed(tuple(partitions)):
        if drop_partition is None:
            break
        try:
            drop_partition(partition)
        except BaseException as error:
            failure = failure or error
    for window in windows:
        cleanup = getattr(window, "cleanup", None)
        if not callable(cleanup):
            continue
        try:
            cleanup()
        except BaseException as error:
            failure = failure or error
    return failure


def mark_source_byte_measurement_complete(artifact: Any) -> None:
    mark_complete = getattr(artifact, "mark_source_byte_measurement_complete", None)
    if callable(mark_complete):
        mark_complete()


def record_range_staging_metric(
    artifact: Any,
    *,
    topology: str,
    requested_workers: int,
    observed_workers: int,
    range_count: int,
    rows: int,
) -> None:
    recorder = getattr(artifact, "record_range_staging_metric", None)
    if callable(recorder):
        recorder(
            {
                "schema_version": "dpone.native_transfer.columnar_range_staging.v1",
                "staging_topology": topology,
                "requested_load_workers": requested_workers,
                "observed_load_workers": observed_workers,
                "range_count": range_count,
                "rows": rows,
            }
        )


def rows_per_second(rows: int, seconds: float) -> float | None:
    if seconds <= 0:
        return None
    return rows / seconds


def elapsed(started: float, finished: float) -> float:
    return max(0.0, finished - started)


def _create_range_partition(sink: Any, parent: Any, partition: Any) -> None:
    sink._ensure_database(partition)
    sink.connector.execute_query(
        f"CREATE TABLE {sink._table(partition)}{sink._cluster_ddl_clause(parent)} AS {sink._table(parent)}"
    )


def _assemble_range_partitions(sink: Any, partitions: tuple[Any, ...], target: Any) -> int:
    if not partitions:
        return 0
    select_sql = " UNION ALL ".join(f"SELECT * FROM {sink._table(config)}" for config in partitions)
    sink.connector.execute_query(f"INSERT INTO {sink._table(target)} {select_sql}")
    return int(sink._count(target))


def _range_staging_callbacks(sink: Any) -> ClickHouseRangeStagingCallbacks | None:
    required = ("_clone_connector", "_operation_table_config", "_drop_table", "_ensure_database")
    if any(not callable(getattr(sink, name, None)) for name in required):
        return None
    return ClickHouseRangeStagingCallbacks(
        clone_connector=sink._clone_connector,
        plan_partition=lambda config, ordinal: sink._operation_table_config(config, f"range_{ordinal:06d}"),
        create_partition=lambda parent, partition: _create_range_partition(sink, parent, partition),
        assemble_partitions=lambda partitions, target: _assemble_range_partitions(sink, partitions, target),
        drop_partition=lambda config: sink._drop_table(sink._table(config), config),
    )
