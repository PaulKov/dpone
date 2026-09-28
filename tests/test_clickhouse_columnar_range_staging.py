from __future__ import annotations

from dataclasses import dataclass
from threading import Event
from types import SimpleNamespace
from typing import Any, cast

import pytest

from dpone.config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.runtime.columnar_fast_path_models import ObjectStorageChunk
from dpone.runtime.columnar_object_storage_windows import (
    ObjectStorageChunkWindow,
    ObjectStorageColumnarChunkedArtifact,
)
from dpone.runtime.sinks.clickhouse_columnar_pull import (
    ClickHouseColumnarPullLoader,
    ClickHouseRangeStagingCallbacks,
)
from dpone.runtime.sinks.clickhouse_payload_support import columnar_pull_loader
from dpone.runtime.sinks.clickhouse_staged_load import ClickHouseStagedLoadService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult


@dataclass(frozen=True)
class _Config:
    table: str
    options: dict[str, object]


class _Window:
    read_contract = SimpleNamespace(mode="named_collection", named_collection="dpone_stage")
    columns = ("id",)
    producer_metrics: dict[str, object] = {}
    size_bytes = 8

    def __init__(self, ordinal: int, *, rows: int = 1) -> None:
        self.range_id = f"range-{ordinal}"
        self.range_ordinal = ordinal
        self.row_count = rows
        self.uri_prefix = f"s3://stage/run/range-{ordinal}/"
        self.cleaned = 0

    def object_key_pattern(self) -> str:
        return f"run/range-{self.range_ordinal}/*.parquet"

    def cleanup(self) -> None:
        self.cleaned += 1


class _Artifact:
    def __init__(
        self,
        windows: list[_Window],
        *,
        topology: str,
        load_workers: int,
        planned_count: int | None = None,
    ) -> None:
        self._windows = windows
        self.range_parallelism_policy = SimpleNamespace(
            staging_topology=topology,
            load_workers=load_workers,
        )
        count = planned_count if planned_count is not None else len({window.range_ordinal for window in windows})
        ranges = tuple(SimpleNamespace(range_id=f"range-{ordinal}", ordinal=ordinal) for ordinal in range(count))
        self.request = SimpleNamespace(range_plan=SimpleNamespace(policy=self.range_parallelism_policy, ranges=ranges))
        rows_by_ordinal = {
            ordinal: sum(window.row_count for window in windows if window.range_ordinal == ordinal)
            for ordinal in range(count)
        }
        self.range_execution_evidence = {
            "all_ranges_confirmed": True,
            "ranges": [
                {
                    "range_id": descriptor.range_id,
                    "rows": rows_by_ordinal[descriptor.ordinal],
                    "eof_confirmed": True,
                }
                for descriptor in ranges
            ],
        }
        self.metrics: list[dict[str, object]] = []
        self.staging_metrics: list[dict[str, object]] = []
        self.measurement_complete = False

    def iter_windows(self):
        yield from self._windows

    def record_window_metric(self, metric: dict[str, object]) -> None:
        self.metrics.append(metric)

    def mark_source_byte_measurement_complete(self) -> None:
        self.measurement_complete = True

    def record_range_staging_metric(self, metric: dict[str, object]) -> None:
        self.staging_metrics.append(metric)


class _Connector:
    def __init__(self, counts: dict[str, int], *, fail_range: int | None = None) -> None:
        self.counts = counts
        self.fail_range = fail_range
        self.queries: list[str] = []

    def execute_query(self, sql: str) -> None:
        self.queries.append(sql)
        target = sql.split(" ", 3)[2]
        for ordinal in range(16):
            if f"range-{ordinal}" not in sql:
                continue
            if ordinal == self.fail_range:
                raise RuntimeError(f"load-{ordinal}-failed")
            self.counts[target] = self.counts.get(target, 0) + 1

    def get_records(self, sql: str):
        table = sql.rsplit(" ", 1)[-1]
        return [(self.counts.get(table, 0),)]


class _Harness:
    def __init__(self, *, fail_range: int | None = None, fail_assembly: bool = False) -> None:
        self.counts = {"authoritative": 0}
        self.root = _Connector(self.counts, fail_range=fail_range)
        self.fail_range = fail_range
        self.fail_assembly = fail_assembly
        self.created: list[str] = []
        self.dropped: list[str] = []
        self.assembly_calls = 0

    def loader(self) -> ClickHouseColumnarPullLoader:
        return ClickHouseColumnarPullLoader(
            connector=self.root,
            table_name=lambda config: cast(_Config, config).table,
            count_rows=lambda config: self.counts.get(cast(_Config, config).table, 0),
            range_staging=ClickHouseRangeStagingCallbacks(
                clone_connector=lambda _ordinal: _Connector(self.counts, fail_range=self.fail_range),
                plan_partition=self._plan,
                create_partition=self._create,
                assemble_partitions=self._assemble,
                drop_partition=self._drop,
            ),
        )

    def _plan(self, config: _Config, ordinal: int) -> _Config:
        return _Config(f"{config.table}__range_{ordinal}", config.options)

    def _create(self, _parent: _Config, config: _Config) -> None:
        self.created.append(config.table)
        self.counts[config.table] = 0

    def _assemble(self, partitions: tuple[_Config, ...], target: _Config) -> int:
        self.assembly_calls += 1
        if self.fail_assembly:
            raise RuntimeError("assembly-failed")
        self.counts[target.table] += sum(self.counts[item.table] for item in partitions)
        return self.counts[target.table]

    def _drop(self, config: _Config) -> None:
        self.dropped.append(config.table)


@pytest.mark.parametrize("count", [1, 2, 4])
@pytest.mark.parametrize("topology", ["shared_per_run", "per_partition"])
def test_range_windows_stage_with_configured_topology_and_one_authoritative_result(
    count: int,
    topology: str,
) -> None:
    harness = _Harness()
    artifact = _Artifact([_Window(ordinal) for ordinal in range(count)], topology=topology, load_workers=count)

    loaded = harness.loader().load_windowed(
        cast(Any, _Config("authoritative", {})),
        artifact,
        (("id", "Int64"),),
    )

    assert loaded == count
    assert harness.counts["authoritative"] == count
    assert harness.assembly_calls == (1 if topology == "per_partition" else 0)
    assert sorted(harness.dropped) == sorted(harness.created)
    assert all(window.cleaned == 0 for window in artifact._windows)
    assert artifact.measurement_complete is True


@pytest.mark.parametrize("topology", ["shared_per_run", "per_partition"])
def test_range_load_failure_cleans_owned_resources_without_assembly(topology: str) -> None:
    harness = _Harness(fail_range=1)
    artifact = _Artifact([_Window(0), _Window(1), _Window(2)], topology=topology, load_workers=2)

    with pytest.raises(RuntimeError, match="load-1-failed"):
        harness.loader().load_windowed(
            cast(Any, _Config("authoritative", {})),
            artifact,
            (("id", "Int64"),),
        )

    assert harness.assembly_calls == 0
    assert sorted(harness.dropped) == sorted(harness.created)
    assert all(window.cleaned == 0 for window in artifact._windows)


def test_partition_assembly_failure_drops_every_partition_and_does_not_report_success() -> None:
    harness = _Harness(fail_assembly=True)
    artifact = _Artifact([_Window(0), _Window(1)], topology="per_partition", load_workers=2)

    with pytest.raises(RuntimeError, match="assembly-failed"):
        harness.loader().load_windowed(
            cast(Any, _Config("authoritative", {})),
            artifact,
            (("id", "Int64"),),
        )

    assert harness.assembly_calls == 1
    assert harness.counts["authoritative"] == 0
    assert sorted(harness.dropped) == sorted(harness.created)
    assert all(window.cleaned == 0 for window in artifact._windows)


def test_sink_callbacks_use_cluster_ddl_and_one_partition_assembly_statement() -> None:
    connector = _SqlConnector()
    sink = _SqlSink(connector)
    artifact = _Artifact([_Window(0), _Window(1)], topology="per_partition", load_workers=1)

    loaded = columnar_pull_loader(sink).load_windowed(_Config("authoritative", {}), artifact, (("id", "Int64"),))

    assert loaded == 2
    assert len([sql for sql in connector.queries if sql.startswith("CREATE TABLE")]) == 2
    assert all(" ON CLUSTER 'dwh' AS authoritative" in sql for sql in connector.queries if sql.startswith("CREATE"))
    assembly = [sql for sql in connector.queries if " UNION ALL " in sql]
    assert len(assembly) == 1
    assert assembly[0].startswith("INSERT INTO authoritative SELECT * FROM authoritative__range_000000")


@pytest.mark.parametrize("topology", ["shared_per_run", "per_partition"])
def test_authoritative_plan_preserves_mixed_empty_ranges(topology: str) -> None:
    harness = _Harness()
    artifact = _Artifact([_Window(1)], topology=topology, load_workers=2, planned_count=3)

    loaded = harness.loader().load_windowed(cast(Any, _Config("authoritative", {})), artifact, (("id", "Int64"),))

    assert loaded == 1
    if topology == "per_partition":
        assert len(harness.created) == 3
    assert artifact.staging_metrics[0]["range_count"] == 3
    assert artifact.staging_metrics[0]["rows"] == 1
    assert artifact.staging_metrics[0]["range_confirmations"] == [
        {"range_id": "range-0", "range_ordinal": 0, "rows": 0, "stage_confirmed": True},
        {"range_id": "range-1", "range_ordinal": 1, "rows": 1, "stage_confirmed": True},
        {"range_id": "range-2", "range_ordinal": 2, "rows": 0, "stage_confirmed": True},
    ]


@pytest.mark.parametrize("topology", ["shared_per_run", "per_partition"])
def test_authoritative_plan_records_all_empty_ranges(topology: str) -> None:
    harness = _Harness()
    artifact = _Artifact([], topology=topology, load_workers=2, planned_count=4)

    loaded = harness.loader().load_windowed(cast(Any, _Config("authoritative", {})), artifact, (("id", "Int64"),))

    assert loaded == 0
    if topology == "per_partition":
        assert len(harness.created) == 4
    assert harness.counts["authoritative"] == 0
    assert artifact.measurement_complete is True
    assert artifact.staging_metrics[0]["range_count"] == 4
    assert artifact.staging_metrics[0]["rows"] == 0
    assert all(item["stage_confirmed"] is True for item in artifact.staging_metrics[0]["range_confirmations"])


@pytest.mark.parametrize(
    ("window", "error"),
    [
        (_Window(1), "clickhouse_range_window_not_planned"),
        (_Window(0), "clickhouse_range_window_identity_mismatch"),
    ],
)
def test_authoritative_plan_rejects_unexpected_or_mismatched_window_identity(
    window: _Window,
    error: str,
) -> None:
    if window.range_ordinal == 0:
        window.range_id = "wrong-range"
    artifact = _Artifact([window], topology="shared_per_run", load_workers=1, planned_count=1)

    with pytest.raises(ValueError, match=error):
        _Harness().loader().load_windowed(cast(Any, _Config("authoritative", {})), artifact, (("id", "Int64"),))

    assert window.cleaned == 0


def test_authoritative_plan_rejects_missing_range_confirmation() -> None:
    artifact = _Artifact([], topology="shared_per_run", load_workers=1, planned_count=2)
    confirmations = cast(list[dict[str, object]], artifact.range_execution_evidence["ranges"])
    artifact.range_execution_evidence["ranges"] = confirmations[:1]

    with pytest.raises(ValueError, match="clickhouse_range_execution_confirmation_set_mismatch"):
        _Harness().loader().load_windowed(cast(Any, _Config("authoritative", {})), artifact, (("id", "Int64"),))


def test_first_range_failure_actively_cancels_running_connectors_and_preserves_primary() -> None:
    started = Event()
    cancelled = Event()
    workers = (_CancellableConnector(0, started, cancelled), _CancellableConnector(1, started, cancelled))
    callbacks = ClickHouseRangeStagingCallbacks(
        clone_connector=lambda ordinal: workers[ordinal],
        plan_partition=lambda config, _ordinal: config,
        create_partition=lambda _parent, _partition: None,
        assemble_partitions=lambda _partitions, _target: 0,
        drop_partition=lambda _partition: None,
    )
    loader = ClickHouseColumnarPullLoader(
        connector=_Connector({}, fail_range=None),
        table_name=lambda _config: "authoritative",
        count_rows=lambda _config: 0,
        range_staging=callbacks,
    )
    artifact = _Artifact([_Window(0), _Window(1)], topology="shared_per_run", load_workers=2)

    with pytest.raises(RuntimeError, match="primary-range-failure"):
        loader.load_windowed(cast(Any, _Config("authoritative", {})), artifact, (("id", "Int64"),))

    assert all(worker.cancel_calls == 1 for worker in workers)
    assert all(worker.close_calls >= 1 for worker in workers)


class _CancellableConnector:
    def __init__(self, ordinal: int, started: Event, cancelled: Event) -> None:
        self.ordinal = ordinal
        self.started = started
        self.cancelled = cancelled
        self.cancel_calls = 0
        self.close_calls = 0

    def execute_query(self, _sql: str) -> None:
        if self.ordinal == 0:
            self.started.set()
            if not self.cancelled.wait(timeout=5):
                raise RuntimeError("range-cancel-timeout")
            raise RuntimeError("cancelled-running-range")
        assert self.started.wait(timeout=5)
        raise RuntimeError("primary-range-failure")

    def cancel(self) -> None:
        self.cancel_calls += 1
        self.cancelled.set()

    def close(self) -> None:
        self.close_calls += 1


@pytest.mark.parametrize(
    ("failure", "cleaned"),
    [(None, True), ("stage", True), ("validate", True), ("publish", False)],
)
def test_object_prefix_cleanup_is_owned_by_staged_reconciliation(failure: str | None, cleaned: bool) -> None:
    events: list[object] = []
    provider = _LifecycleProvider(events)
    artifact = ObjectStorageColumnarChunkedArtifact(
        provider=provider,
        request=SimpleNamespace(range_plan=None, options={"cleanup_policy": "eager"}),
        columns=("id",),
        schema_hash="schema",
        cleanup_policy="eager",
    )
    sink = _LifecycleSink(provider.objects, events, fail_insert=failure == "stage")
    service = _LifecycleService(sink, events, failure=failure)
    payload = LoadPayload(artifact=artifact, schema=(("id", "int"),))

    if failure is None:
        result = service.load(_lifecycle_config(), payload)
        assert result.inserted_rows == 1
    else:
        with pytest.raises(RuntimeError, match=f"{failure}-failure") as raised:
            service.load(_lifecycle_config(), payload)
        if failure == "publish":
            details = cast(dict[str, object], getattr(raised.value, "details"))
            assert details["target_outcome"] == "commit_unknown"

    assert (not provider.objects) is cleaned
    assert ("insert", "objects_present") in events
    if failure is None:
        handle_ids = [event[1] for event in events if isinstance(event, tuple) and event[0] in {"validate", "publish"}]
        assert len(handle_ids) == 2 and len(set(handle_ids)) == 1
        assert [event[0] for event in events if isinstance(event, tuple)] == [
            "insert",
            "validate",
            "publish",
            "drop",
            "object_cleanup",
        ]
    if failure == "publish":
        assert not any(isinstance(event, tuple) and event[0] == "object_cleanup" for event in events)


class _LifecycleProvider:
    def __init__(self, events: list[object]) -> None:
        self.events = events
        self.objects = {"run/_dpone_run.json", "run/range-0/chunk.parquet"}

    def iter_object_storage_windows(self, _request: object):
        yield ObjectStorageChunkWindow(
            uri_prefix="s3://stage/run/range-0/",
            columns=("id",),
            chunks=(ObjectStorageChunk("s3://stage/run/range-0/chunk.parquet", 0, 1, 8, "a" * 64, "schema"),),
            read_contract=SimpleNamespace(mode="named_collection", named_collection="dpone_stage"),
            schema_hash="schema",
            cleanup_policy="eager",
        )

    def cleanup_object_storage_run(self, _request: object) -> None:
        self.events.append(("object_cleanup", None))
        self.objects.clear()


class _LifecycleConnector:
    def __init__(self, objects: set[str], events: list[object], *, fail_insert: bool) -> None:
        self.objects = objects
        self.events = events
        self.fail_insert = fail_insert

    def execute_query(self, _sql: str) -> None:
        assert self.objects == {"run/_dpone_run.json", "run/range-0/chunk.parquet"}
        self.events.append(("insert", "objects_present"))
        if self.fail_insert:
            raise RuntimeError("stage-failure")


class _LifecycleSink:
    def __init__(self, objects: set[str], events: list[object], *, fail_insert: bool) -> None:
        self.connector = _LifecycleConnector(objects, events, fail_insert=fail_insert)
        self.events = events
        self._staging_decoder = SimpleNamespace(prepare=lambda *_args: (None, None))
        self._staging_finalizer = SimpleNamespace()

    def _insert_payload(self, config: LoadConfig, payload: LoadPayload) -> int:
        loader = ClickHouseColumnarPullLoader(
            connector=self.connector,
            table_name=lambda _config: "staging",
            count_rows=lambda _config: 1,
        )
        return loader.load_windowed(config, payload.artifact, payload.schema)


class _LifecycleService(ClickHouseStagedLoadService):
    def __init__(self, sink: _LifecycleSink, events: list[object], *, failure: str | None) -> None:
        super().__init__(sink)
        self.events = events
        self.failure = failure

    @staticmethod
    def _create_staging(load_config: LoadConfig, _payload: LoadPayload) -> LoadConfig:
        return load_config

    def validate(self, _load_config: LoadConfig, handle: object) -> object:
        self.events.append(("validate", id(handle)))
        if self.failure == "validate":
            raise RuntimeError("validate-failure")
        return "token"

    def finalize_validated(self, _config: LoadConfig, handle: object, _token: object) -> LoadResult:
        self.events.append(("publish", id(handle)))
        if self.failure == "publish":
            raise RuntimeError("publish-failure")
        return LoadResult(1, 0, 1, staging_rows=1)

    def _drop_configs(self, *_configs: object) -> None:
        self.events.append(("drop", None))


def _lifecycle_config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema="stage",
        target_table="orders_stage",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
    )


class _SqlConnector:
    def __init__(self) -> None:
        self.counts = {"authoritative": 0}
        self.queries: list[str] = []

    def execute_query(self, sql: str) -> None:
        self.queries.append(sql)
        fields = sql.split()
        if sql.startswith("CREATE TABLE"):
            self.counts[fields[2]] = 0
        elif sql.startswith("DROP TABLE"):
            self.counts.pop(fields[4], None)
        elif " UNION ALL " in sql:
            target = fields[2]
            sources = [part.split()[0] for part in sql.split("FROM ")[1:]]
            self.counts[target] = sum(self.counts[source] for source in sources)
        elif sql.startswith("INSERT INTO"):
            self.counts[fields[2]] = self.counts.get(fields[2], 0) + 1

    def get_records(self, sql: str):
        return [(self.counts.get(sql.rsplit(" ", 1)[-1], 0),)]


class _SqlSink:
    def __init__(self, connector: _SqlConnector) -> None:
        self.connector = connector

    @staticmethod
    def _table(config: _Config) -> str:
        return config.table

    def _count(self, config: _Config) -> int:
        return self.connector.counts.get(config.table, 0)

    def _clone_connector(self, _ordinal: int):
        return self.connector

    @staticmethod
    def _operation_table_config(config: _Config, operation: str) -> _Config:
        return _Config(f"{config.table}__{operation}", config.options)

    @staticmethod
    def _ensure_database(_config: _Config) -> None:
        return None

    @staticmethod
    def _cluster_ddl_clause(_config: _Config) -> str:
        return " ON CLUSTER 'dwh'"

    def _drop_table(self, table: str, config: _Config) -> None:
        self.connector.execute_query(f"DROP TABLE IF EXISTS {table}{self._cluster_ddl_clause(config)}")
