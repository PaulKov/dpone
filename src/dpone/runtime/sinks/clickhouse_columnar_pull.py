"""ClickHouse object-storage pull loader for columnar staging."""

from __future__ import annotations

import re
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Event
from time import perf_counter
from typing import TYPE_CHECKING, Any

from dpone.runtime.sinks.clickhouse_payload_support import (
    ClickHouseRangeStagingCallbacks,
    RangeLoadConcurrencyTracker,
    RangeWindowGroup,
    cleanup_range_resources,
    count_connector_rows,
    elapsed,
    group_range_windows,
    mark_source_byte_measurement_complete,
    range_staging_policy,
    record_range_staging_metric,
    rows_per_second,
)

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig

IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True, slots=True)
class ClickHouseColumnarPullConfig:
    use_cluster_function: str = "auto"
    cluster: str | None = None
    auth_mode: str = "named_collection"
    settings: dict[str, object] | None = None

    @classmethod
    def from_load_config(cls, load_config: LoadConfig) -> ClickHouseColumnarPullConfig:
        options = getattr(load_config, "options", {}) or {}
        bulk = _mapping(options.get("clickhouse_bulk"))
        pull = _mapping(bulk.get("columnar_pull"))
        return cls(
            use_cluster_function=str(pull.get("use_cluster_function") or "auto"),
            cluster=_text(pull.get("cluster")),
            auth_mode=str(pull.get("auth_mode") or "named_collection"),
            settings=_mapping(pull.get("settings")),
        )

    def table_function(self) -> str:
        if self.use_cluster_function == "s3Cluster" or (self.use_cluster_function == "auto" and self.cluster):
            return "s3Cluster"
        return "s3"


class ClickHouseColumnarPullLoader:
    """Stage Parquet objects by asking ClickHouse to pull from object storage."""

    def __init__(
        self,
        *,
        connector: Any,
        table_name: Callable[[LoadConfig], str],
        count_rows: Callable[[LoadConfig], int],
        clock: Callable[[], float] | None = None,
        range_staging: ClickHouseRangeStagingCallbacks | None = None,
    ) -> None:
        self._connector = connector
        self._table_name = table_name
        self._count_rows = count_rows
        self._clock = clock or perf_counter
        self._range_staging = range_staging

    def load(
        self,
        load_config: LoadConfig,
        manifest: Any,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        sql = self.render_insert_sql(load_config, manifest, schema)
        result = self._connector.execute_query(sql)
        return int(result) if isinstance(result, int) else self._count_rows(load_config)

    def load_windowed(
        self,
        load_config: LoadConfig,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        policy = range_staging_policy(artifact)
        if policy is None:
            return self._load_legacy_windowed(load_config, artifact, schema)
        windows = tuple(artifact.iter_windows())
        groups = group_range_windows(windows, artifact=artifact)
        if not groups:
            mark_source_byte_measurement_complete(artifact)
            return 0
        topology, load_workers = policy
        if topology == "per_partition" and self._range_staging is None:
            raise ValueError("clickhouse_per_partition_staging_callbacks_missing")
        tracker = RangeLoadConcurrencyTracker()
        primary: BaseException | None = None
        partitions: tuple[Any, ...] = ()
        try:
            if topology == "shared_per_run":
                self._load_groups(load_config, artifact, schema, groups, load_workers, tracker)
            else:
                assert self._range_staging is not None
                partitions = tuple(self._range_staging.plan_partition(load_config, group.ordinal) for group in groups)
                for partition in partitions:
                    self._range_staging.create_partition(load_config, partition)
                self._load_partition_groups(partitions, artifact, schema, groups, load_workers, tracker)
                assembled = int(self._range_staging.assemble_partitions(partitions, load_config))
                expected = sum(group.expected_rows for group in groups)
                if assembled != expected or self._count_rows(load_config) != expected:
                    raise ValueError("clickhouse_range_partition_assembly_row_count_mismatch")
            expected_rows = sum(group.expected_rows for group in groups)
            if self._count_rows(load_config) != expected_rows:
                raise ValueError("clickhouse_range_staging_row_count_mismatch")
            record_range_staging_metric(
                artifact,
                topology=topology,
                requested_workers=load_workers,
                observed_workers=tracker.maximum,
                range_count=len(groups),
                rows=expected_rows,
            )
            mark_source_byte_measurement_complete(artifact)
        except BaseException as error:
            primary = error
        cleanup_error = cleanup_range_resources(
            windows,
            partitions,
            drop_partition=self._range_staging.drop_partition if self._range_staging is not None else None,
        )
        if primary is not None:
            if cleanup_error is not None:
                primary.add_note(f"range staging cleanup failed: {type(cleanup_error).__name__}")
            raise primary
        if cleanup_error is not None:
            raise cleanup_error
        return sum(group.expected_rows for group in groups)

    def _load_legacy_windowed(
        self,
        load_config: LoadConfig,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
    ) -> int:
        loaded_any = False
        for window_index, window in enumerate(artifact.iter_windows(), start=1):
            sql = self.render_insert_sql(load_config, window, schema)
            pull_started = self._clock()
            self._connector.execute_query(sql)
            pull_seconds = elapsed(pull_started, self._clock())
            loaded_any = True
            cleanup = getattr(window, "cleanup", None)
            cleanup_seconds = 0.0
            if callable(cleanup):
                cleanup()
                cleanup_seconds = elapsed(pull_started + pull_seconds, self._clock())
            _record_window_metric(
                artifact,
                window=window,
                window_index=window_index,
                clickhouse_pull_seconds=pull_seconds,
                window_cleanup_seconds=cleanup_seconds,
            )
        mark_complete = getattr(artifact, "mark_source_byte_measurement_complete", None)
        if callable(mark_complete):
            mark_complete()
        return self._count_rows(load_config) if loaded_any else 0

    def _load_groups(
        self,
        load_config: LoadConfig,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
        groups: tuple[RangeWindowGroup, ...],
        load_workers: int,
        tracker: RangeLoadConcurrencyTracker,
    ) -> None:
        self._run_group_lanes(
            groups,
            load_workers,
            lambda connector, group: self._load_group(connector, load_config, artifact, schema, group, tracker),
        )

    def _load_partition_groups(
        self,
        partitions: tuple[Any, ...],
        artifact: Any,
        schema: Sequence[tuple[str, str]],
        groups: tuple[RangeWindowGroup, ...],
        load_workers: int,
        tracker: RangeLoadConcurrencyTracker,
    ) -> None:
        by_ordinal = {group.ordinal: partition for group, partition in zip(groups, partitions, strict=True)}

        def load(connector: Any, group: RangeWindowGroup) -> None:
            partition = by_ordinal[group.ordinal]
            self._load_group(connector, partition, artifact, schema, group, tracker)
            if count_connector_rows(connector, self._table_name(partition)) != group.expected_rows:
                raise ValueError(f"clickhouse_range_staging_row_count_mismatch:{group.range_id}")

        self._run_group_lanes(groups, load_workers, load)

    def _run_group_lanes(
        self,
        groups: tuple[RangeWindowGroup, ...],
        requested_workers: int,
        load: Callable[[Any, RangeWindowGroup], None],
    ) -> None:
        workers = min(requested_workers, len(groups))
        if workers == 1:
            for group in groups:
                load(self._connector, group)
            return
        if self._range_staging is None:
            raise ValueError("clickhouse_parallel_range_load_callbacks_missing")
        connectors = tuple(self._range_staging.clone_connector(index) for index in range(workers))
        if len({id(connector) for connector in connectors}) != workers or any(
            connector is self._connector for connector in connectors
        ):
            raise ValueError("clickhouse_range_load_workers_require_independent_connectors")
        lanes = tuple(groups[index::workers] for index in range(workers))
        stop = Event()

        def run_lane(index: int) -> None:
            try:
                for group in lanes[index]:
                    if stop.is_set():
                        return
                    try:
                        load(connectors[index], group)
                    except BaseException:
                        stop.set()
                        raise
            finally:
                close = getattr(connectors[index], "close", None)
                if callable(close):
                    close()

        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="dpone-ch-range-load") as executor:
            futures = tuple(executor.submit(run_lane, index) for index in range(workers))
            failures: list[tuple[int, BaseException]] = []
            for index, future in enumerate(futures):
                try:
                    future.result()
                except BaseException as error:
                    failures.append((lanes[index][0].ordinal, error))
            if failures:
                raise min(failures, key=lambda item: item[0])[1]

    def _load_group(
        self,
        connector: Any,
        load_config: Any,
        artifact: Any,
        schema: Sequence[tuple[str, str]],
        group: RangeWindowGroup,
        tracker: RangeLoadConcurrencyTracker,
    ) -> None:
        for window_index, window in enumerate(group.windows, start=1):
            sql = self.render_insert_sql(load_config, window, schema)
            started = self._clock()
            tracker.enter()
            try:
                connector.execute_query(sql)
            finally:
                tracker.leave()
            _record_window_metric(
                artifact,
                window=window,
                window_index=window_index,
                clickhouse_pull_seconds=elapsed(started, self._clock()),
                window_cleanup_seconds=0.0,
            )

    def render_insert_sql(
        self,
        load_config: LoadConfig,
        manifest: Any,
        schema: Sequence[tuple[str, str]],
    ) -> str:
        config = ClickHouseColumnarPullConfig.from_load_config(load_config)
        columns = [column for column, _ in schema]
        column_sql = ", ".join(_quote_identifier(column) for column in columns)
        return (
            f"INSERT INTO {self._table_name(load_config)} ({column_sql}) "
            f"SELECT {column_sql} FROM {self._table_function_sql(config, manifest)}"
            f"{_settings_clause(config.settings)}"
        )

    def _table_function_sql(
        self,
        config: ClickHouseColumnarPullConfig,
        manifest: Any,
    ) -> str:
        if manifest.read_contract.mode != "named_collection":
            raise ValueError("ClickHouse columnar pull v1 requires named_collection read access")
        collection = _safe_named_collection(manifest.read_contract.named_collection)
        key_pattern = manifest.object_key_pattern().replace("'", "''")
        function = config.table_function()
        if function == "s3Cluster":
            if not config.cluster:
                raise ValueError("clickhouse_bulk.columnar_pull.cluster is required for s3Cluster")
            return f"s3Cluster('{_escape_literal(config.cluster)}', {collection}, filename='{key_pattern}')"
        return f"s3({collection}, filename='{key_pattern}')"


def _quote_identifier(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _safe_named_collection(value: str | None) -> str:
    if not value or not IDENTIFIER_RE.match(value):
        raise ValueError("ClickHouse named_collection must be a safe identifier")
    return value


def _escape_literal(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _settings_clause(settings: dict[str, object] | None) -> str:
    merged = {"input_format_parquet_allow_missing_columns": False, **(settings or {})}
    if not merged:
        return ""
    rendered = ", ".join(f"{key} = {_setting_value(value)}" for key, value in sorted(merged.items()))
    return f" SETTINGS {rendered}"


def _setting_value(value: object) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    return "'" + _escape_literal(str(value)) + "'"


def _mapping(value: object | None) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _text(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _record_window_metric(
    artifact: Any,
    *,
    window: Any,
    window_index: int,
    clickhouse_pull_seconds: float,
    window_cleanup_seconds: float,
) -> None:
    recorder = getattr(artifact, "record_window_metric", None)
    if not callable(recorder):
        return
    row_count = int(getattr(window, "row_count", 0) or 0)
    metric = {
        "schema_version": "dpone.native_transfer.columnar_window_metrics.v1",
        "window_index": window_index,
        "row_count": row_count,
        "size_bytes": int(getattr(window, "size_bytes", 0) or 0),
        "uri_prefix": str(getattr(window, "uri_prefix", "")),
        "producer_metrics": dict(getattr(window, "producer_metrics", {}) or {}),
        "clickhouse_pull_seconds": clickhouse_pull_seconds,
        "window_cleanup_seconds": window_cleanup_seconds,
        "rows_per_second": rows_per_second(row_count, clickhouse_pull_seconds),
    }
    range_id = getattr(window, "range_id", None)
    range_ordinal = getattr(window, "range_ordinal", None)
    if isinstance(range_id, str) and range_id:
        metric["range_id"] = range_id
    if isinstance(range_ordinal, int) and not isinstance(range_ordinal, bool) and range_ordinal >= 0:
        metric["range_ordinal"] = range_ordinal
    recorder(metric)


__all__ = [
    "ClickHouseColumnarPullConfig",
    "ClickHouseColumnarPullLoader",
    "ClickHouseRangeStagingCallbacks",
]
