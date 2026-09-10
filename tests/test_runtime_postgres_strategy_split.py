from __future__ import annotations

import csv
import gzip
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

import pytest

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.artifacts import FileExportArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.load_result import LoadResult
from dpone.runtime.sinks.strategies.postgres.file_export_loader import PostgresFileExportLoader
from dpone.runtime.sinks.strategies.postgres.postgres_base import PostgresStrategyBase
from dpone.runtime.sinks.strategies.postgres.staging_sql_helper import PostgresStagingSqlHelper
from dpone.runtime.sinks.strategies.postgres.target_table_manager import PostgresTargetTableManager
from dpone.runtime.sources.strategies.postgres.postgres_base import PostgresBaseStrategy


class StubLogger:
    def __init__(self):
        self.progress = []

    def log_etl_progress(self, event, payload):
        self.progress.append((event, payload))

    def log_data_sample(self, kind, rows, max_rows):
        self.progress.append((kind, {"rows": rows, "max_rows": max_rows}))


class StubConnector:
    def __init__(self, get_records_responses=None, table_types=None):
        self.connection = object()
        self.get_records_responses = list(get_records_responses or [])
        self.executed = []
        self.table_types = table_types or {}

    def get_records(self, query, params=None, as_dict=False):
        self.executed.append(("get_records", params, as_dict))
        rendered = str(query)
        if "pg_export_snapshot" in rendered:
            return [{"snapshot_token": "00000003-00000001-1"}]
        if "txid_current_snapshot" in rendered:
            return [{"snapshot_token": "100:200:", "extraction_horizon": 200}] if as_dict else [("100:200:", 200)]
        if self.get_records_responses:
            return self.get_records_responses.pop(0)
        return []

    def execute_query(self, query, params=None):
        self.executed.append(("execute_query", params))
        return 0

    def begin(self):
        self.executed.append(("begin", None))

    def commit_transaction(self):
        self.executed.append(("commit", None))

    def rollback(self):
        self.executed.append(("rollback", None))

    def get_table_column_types(self, schema, table):
        self.executed.append(("get_table_column_types", (schema, table)))
        return dict(self.table_types)


class StubCopy:
    def __init__(self):
        self.writes = []
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def write(self, value):
        self.writes.append(value)

    def write_row(self, row):
        self.rows.append(row)


class StubCursor:
    def __init__(self, copy_obj):
        self.copy_obj = copy_obj
        self.copy_sql = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def copy(self, copy_sql):
        self.copy_sql = copy_sql
        return self.copy_obj


class StubConnectorWithCopy(StubConnector):
    def __init__(self, get_records_responses=None, table_types=None):
        super().__init__(get_records_responses=get_records_responses, table_types=table_types)
        self.copy = StubCopy()
        self._cursor = StubCursor(self.copy)
        self.connection = SimpleNamespace(cursor=lambda: self._cursor)


class DummyPostgresStrategy(PostgresBaseStrategy):
    def get_state(self, load_config):
        del load_config
        return None

    def extract(self, load_config, last_state):
        del load_config, last_state
        raise NotImplementedError


def test_target_table_manager_builds_schema_with_missing_technical_columns() -> None:
    manager = PostgresTargetTableManager(
        connector=StubConnector(),
        logger=StubLogger(),
        include_technical_columns=lambda _cfg: True,
    )
    cfg = SimpleNamespace()

    schema_with_tech, tech_columns = manager.build_schema_with_technical_columns(
        cfg,
        [("id", "integer"), ("__dpone__deleted_at", "timestamp with time zone")],
    )

    assert schema_with_tech == [
        ("id", "integer"),
        ("__dpone__deleted_at", "timestamp with time zone"),
        ("__dpone__loaded_at", "timestamp with time zone"),
    ]
    assert tech_columns == [("__dpone__loaded_at", "timestamp with time zone")]


def test_target_table_manager_ensures_only_missing_technical_columns() -> None:
    connector = StubConnector(get_records_responses=[[("__dpone__loaded_at",)]])
    logger = StubLogger()
    manager = PostgresTargetTableManager(
        connector=connector,
        logger=logger,
        include_technical_columns=lambda _cfg: True,
    )
    cfg = SimpleNamespace(target_schema="landing", target_table="orders")

    manager.ensure_technical_columns(cfg)

    # one check + one add for __dpone__deleted_at only
    execute_ops = [op for op in connector.executed if op[0] == "execute_query"]
    assert len(execute_ops) == 1
    assert logger.progress == [
        (
            "TECHNICAL_COLUMN_ADDED",
            {"Target": "landing.orders", "Column": "__dpone__deleted_at"},
        )
    ]


def test_postgres_file_export_loader_dispatches_by_overwrite_type() -> None:
    loader = PostgresFileExportLoader(
        connector=StubConnector(),
        logger=StubLogger(),
        target_table_manager=SimpleNamespace(),
        log_target_sample=lambda *_args, **_kwargs: None,
    )
    artifact = SimpleNamespace(file_path="/tmp/x.csv", compressed=False, format="csv")
    payload = SimpleNamespace(artifact=artifact, schema=[])

    calls = []
    loader.load_standard = lambda *args: calls.append("standard") or "standard"
    loader.load_with_truncate = lambda *args: calls.append("truncate") or "truncate"
    loader.load_with_exchange = lambda *args: calls.append("exchange") or "exchange"

    assert loader.load(SimpleNamespace(overwrite_type=None), payload) == "standard"
    assert loader.load(SimpleNamespace(overwrite_type="truncate_insert"), payload) == "truncate"
    assert loader.load(SimpleNamespace(overwrite_type="exchange"), payload) == "exchange"
    assert calls == ["standard", "truncate", "exchange"]


def test_postgres_consume_with_staging_materializes_file_export_artifact() -> None:
    """File CSV artifacts must hit staging + strategy handler, not DROP+CREATE short-circuit."""

    calls = {"materialize": 0, "handler": 0, "file_export": 0}

    class Strat(PostgresStrategyBase):
        def load(self, load_config, payload):  # pragma: no cover - unused in unit test
            raise NotImplementedError

    staging_manager = SimpleNamespace(drop=lambda *_args, **_kwargs: None)
    staging_handle = StagingTableArtifact(
        schema="stg",
        table="orders_stg",
        columns=["id"],
        staging_manager=staging_manager,
        row_count=2,
    )
    strategy = Strat(connector=SimpleNamespace(), logger=StubLogger(), staging_manager=staging_manager)

    with TemporaryDirectory() as tmp_dir:
        csv_path = Path(tmp_dir) / "orders.csv"
        csv_path.write_text("1\n2\n", encoding="utf-8")
        artifact = FileExportArtifact(str(csv_path), columns=["id"], compressed=False, format="csv")

        def _materialize(staging_mgr, load_config, schema):
            del staging_mgr, load_config, schema
            calls["materialize"] += 1
            return staging_handle

        artifact.materialize = _materialize  # type: ignore[method-assign]
        artifact.cleanup = lambda: None  # type: ignore[method-assign]

        def handler(staging: StagingTableArtifact) -> LoadResult:
            calls["handler"] += 1
            assert staging is staging_handle
            return LoadResult(inserted_rows=2, updated_rows=0, total_rows=2)

        result = strategy._consume_with_staging(  # noqa: SLF001
            SimpleNamespace(target_schema="public", target_table="orders", staging_schema="stg"),
            LoadPayload(artifact=artifact, schema=[("id", "integer")]),
            handler,
        )

    assert calls == {"materialize": 1, "handler": 1, "file_export": 0}
    assert result.total_rows == 2
    assert result.staging_rows == 2


def test_postgres_file_export_loader_streams_csv_into_copy() -> None:
    connector = StubConnectorWithCopy(get_records_responses=[[(2,)]])
    loader = PostgresFileExportLoader(
        connector=connector,
        logger=StubLogger(),
        target_table_manager=SimpleNamespace(),
        log_target_sample=lambda *_args, **_kwargs: None,
    )

    with TemporaryDirectory() as tmp_dir:
        csv_path = Path(tmp_dir) / "orders.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([1, "Paris"])
            writer.writerow([2, "Berlin"])

        artifact = FileExportArtifact(str(csv_path), columns=["id", "city"], compressed=False, format="csv")
        inserted = loader.copy_from_artifact(
            target_schema="stg",
            target_table="orders",
            schema=[("id", "integer"), ("city", "text")],
            artifact=artifact,
        )

    assert inserted == 2
    assert connector.copy.rows == []
    payload = "".join(connector.copy.writes)
    assert "1,Paris" in payload
    assert "2,Berlin" in payload


def test_staging_sql_helper_caches_target_types_on_load_config() -> None:
    connector = StubConnector(table_types={"id": "integer"})
    helper = PostgresStagingSqlHelper(
        connector=connector,
        logger=StubLogger(),
        include_technical_columns=lambda _cfg: True,
        log_target_sample=lambda *_args, **_kwargs: None,
    )
    cfg = SimpleNamespace(target_schema="landing", target_table="orders")

    first = helper.get_target_column_types(cfg)
    second = helper.get_target_column_types(cfg)

    assert first == {"id": "integer"}
    assert second == {"id": "integer"}
    get_types_ops = [op for op in connector.executed if op[0] == "get_table_column_types"]
    assert len(get_types_ops) == 1


def test_postgres_base_strategy_treats_single_row_gzip_csv_as_non_empty() -> None:
    strategy = DummyPostgresStrategy(connector=SimpleNamespace(), logger=StubLogger())

    with TemporaryDirectory() as tmp_dir:
        path = Path(tmp_dir) / "batch.csv.gz"
        with gzip.open(path, "wt", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([1, "Paris"])

        assert strategy._is_file_empty(str(path)) is False  # noqa: SLF001


def test_postgres_mssql_partitioned_export_fails_before_copy_without_shared_snapshot(tmp_path: Path) -> None:
    class CopyConnector(StubConnector):
        def __init__(self):
            super().__init__(get_records_responses=[[{"lower_bound": 1, "upper_bound": 10, "row_count": 9}]])
            self.exports: list[dict[str, object]] = []

        def copy_to_file(self, **kwargs):
            self.exports.append(kwargs)
            Path(str(kwargs["output_path"])).write_text("1\talpha\n", encoding="utf-8")
            return {"total_bytes": 8, "chunk_count": 1, "elapsed": 0.1, "throughput": 1.0}

    connector = CopyConnector()
    strategy = DummyPostgresStrategy(connector=connector, logger=StubLogger())
    load_config = SimpleNamespace(
        source_schema="public",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=SimpleNamespace(value="full_refresh"),
        options={
            "sink_type": "mssql",
            "export_format": "csv",
            "compress_export": False,
            "partition_tmp_dir": str(tmp_path),
            "partitioning": {
                "column": "id",
                "bounds": "auto",
                "target_rows_per_partition": 3,
                "max_partitions": 8,
                "export_workers": 1,
                "load_workers": 2,
            },
        },
        export_format="csv",
        compress_export=False,
    )

    with pytest.raises(
        ValueError,
        match="DPONE_POSTGRES_MSSQL_WIRE_CONTRACT_BLOCKED: postgres_mssql.partitioning",
    ):
        strategy._export_to_file(  # noqa: SLF001
            "SELECT id, name FROM public.orders",
            [("id", "integer"), ("name", "text")],
            load_config,
            batch_size=1000,
        )

    assert connector.exports == []
    assert list(tmp_path.iterdir()) == []


def test_postgres_partition_export_uses_connector_clone_hook(tmp_path: Path) -> None:
    class CloneableCopyConnector(StubConnector):
        def __init__(self):
            super().__init__(get_records_responses=[[{"lower_bound": 1, "upper_bound": 10, "row_count": 9}]])
            self.exports: list[dict[str, object]] = []
            self.clone_calls: list[int] = []

        def clone_for_partition(self, partition_index: int):
            self.clone_calls.append(partition_index)
            return self

        def copy_to_file(self, **kwargs):
            self.exports.append(kwargs)
            Path(str(kwargs["output_path"])).write_text("1\talpha\n", encoding="utf-8")
            return {"total_bytes": 8, "chunk_count": 1, "elapsed": 0.1, "throughput": 1.0}

    connector = CloneableCopyConnector()
    strategy = DummyPostgresStrategy(connector=connector, logger=StubLogger())
    load_config = SimpleNamespace(
        source_schema="public",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=SimpleNamespace(value="full_refresh"),
        options={
            "sink_type": "postgres",
            "export_format": "csv",
            "compress_export": False,
            "partition_tmp_dir": str(tmp_path),
            "partitioning": {
                "column": "id",
                "bounds": "auto",
                "target_rows_per_partition": 3,
                "max_partitions": 8,
                "export_workers": 1,
                "load_workers": 2,
            },
        },
        export_format="csv",
        compress_export=False,
    )

    strategy._export_to_file(  # noqa: SLF001
        "SELECT id, name FROM public.orders",
        [("id", "integer"), ("name", "text")],
        load_config,
        batch_size=1000,
    )

    assert connector.clone_calls == [0, 1, 2]


def test_postgres_mssql_batched_export_attaches_bulk_text_codec(tmp_path: Path) -> None:
    class CopyConnector(StubConnector):
        def __init__(self):
            super().__init__(get_records_responses=[[(2,)]])
            self.exports: list[dict[str, object]] = []

        def copy_to_file(self, **kwargs):
            self.exports.append(kwargs)
            Path(str(kwargs["output_path"])).write_text("1\talpha\n", encoding="utf-8")
            return {"total_bytes": 8, "chunk_count": 1, "elapsed": 0.1, "throughput": 1.0}

    connector = CopyConnector()
    strategy = DummyPostgresStrategy(connector=connector, logger=StubLogger())
    load_config = SimpleNamespace(
        source_schema="public",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=SimpleNamespace(value="full_refresh"),
        options={
            "sink_type": "mssql",
            "export_format": "csv",
            "compress_export": False,
            "partition_tmp_dir": str(tmp_path),
            "batch_commit_mode": "whole",
        },
        export_format="csv",
        compress_export=False,
    )

    artifact = strategy._export_query_to_file_single(  # noqa: SLF001
        "SELECT id, name FROM public.orders",
        [("id", "integer"), ("name", "text")],
        load_config,
        batch_num=1,
    )

    assert artifact.bulk_text_codec is not None
    assert artifact.format == "mssql-delimited"
    assert "replace(" in str(connector.exports[0]["query_sql"])


def test_postgres_whole_copy_failure_removes_partial_file(tmp_path: Path) -> None:
    class FailingCopyConnector(StubConnector):
        def copy_to_file(self, **kwargs):
            Path(str(kwargs["output_path"])).write_bytes(b"partial")
            raise RuntimeError("copy failed after bytes")

    connector = FailingCopyConnector()
    strategy = DummyPostgresStrategy(connector=connector, logger=StubLogger())

    class SourceAuthority:
        @staticmethod
        def verify_snapshot(*, connector, snapshot_lease, load_config):
            del load_config
            snapshot_lease.require_for(connector)
            return SimpleNamespace(verified=True)

    strategy.bind_postgres_source_authority(SourceAuthority())
    load_config = SimpleNamespace(
        source_table="orders",
        options={"sink_type": "mssql", "partition_tmp_dir": str(tmp_path), "batch_commit_mode": "whole"},
        export_format="csv",
        compress_export=False,
    )

    with pytest.raises(RuntimeError, match="copy failed after bytes"):
        strategy._export_to_file_whole(  # noqa: SLF001
            "SELECT 1",
            [("id", "integer")],
            load_config,
            relation_schema=(("id", "integer"),),
        )

    assert list(tmp_path.iterdir()) == []


def test_postgres_batched_copy_failure_removes_partial_file(tmp_path: Path) -> None:
    class FailingCopyConnector(StubConnector):
        def copy_to_file(self, **kwargs):
            Path(str(kwargs["output_path"])).write_bytes(b"partial")
            raise RuntimeError("batch copy failed after bytes")

    strategy = DummyPostgresStrategy(connector=FailingCopyConnector(), logger=StubLogger())
    load_config = SimpleNamespace(
        source_table="orders",
        options={"sink_type": "postgres", "partition_tmp_dir": str(tmp_path)},
        export_format="csv",
        compress_export=False,
    )

    with pytest.raises(RuntimeError, match="batch copy failed after bytes"):
        strategy._export_query_to_file_single(  # noqa: SLF001
            "SELECT 1", [("id", "integer")], load_config, batch_num=1
        )

    assert list(tmp_path.iterdir()) == []


def test_postgres_partition_sibling_failure_removes_all_owned_files(tmp_path: Path) -> None:
    class FailingSecondPartition(StubConnector):
        def __init__(self):
            super().__init__(get_records_responses=[[{"lower_bound": 1, "upper_bound": 4, "row_count": 4}]])
            self.calls = 0

        def clone_for_partition(self, _partition_index: int):
            return self

        def copy_to_file(self, **kwargs):
            self.calls += 1
            Path(str(kwargs["output_path"])).write_text(f"{self.calls}\n", encoding="utf-8")
            if self.calls == 2:
                raise RuntimeError("partition sibling failed")
            return {"total_bytes": 2, "chunk_count": 1, "elapsed": 0.1, "throughput": 1.0}

    connector = FailingSecondPartition()
    strategy = DummyPostgresStrategy(connector=connector, logger=StubLogger())
    load_config = SimpleNamespace(
        source_schema="public",
        source_table="orders",
        target_schema="dbo",
        target_table="orders",
        load_strategy=SimpleNamespace(value="full_refresh"),
        options={
            "sink_type": "postgres",
            "partition_tmp_dir": str(tmp_path),
            "batch_commit_mode": "whole",
            "partitioning": {
                "column": "id",
                "bounds": "auto",
                "target_rows_per_partition": 2,
                "max_partitions": 4,
                "export_workers": 1,
            },
        },
        export_format="csv",
        compress_export=False,
    )

    with pytest.raises(RuntimeError, match="partition sibling failed"):
        strategy._export_to_file(  # noqa: SLF001
            "SELECT id FROM public.orders", [("id", "integer")], load_config, batch_size=2
        )

    assert list(tmp_path.iterdir()) == []


def test_postgres_batched_terminal_empty_file_is_not_leaked(tmp_path: Path) -> None:
    class OneThenEmptyConnector(StubConnector):
        def __init__(self):
            super().__init__(get_records_responses=[[(1,)]])
            self.calls = 0

        def copy_to_file(self, **kwargs):
            self.calls += 1
            Path(str(kwargs["output_path"])).write_text("1\n" if self.calls == 1 else "", encoding="utf-8")
            return {"total_bytes": 2 if self.calls == 1 else 0, "chunk_count": 1, "elapsed": 0.1, "throughput": 1.0}

    strategy = DummyPostgresStrategy(connector=OneThenEmptyConnector(), logger=StubLogger())
    strategy._render_query = lambda _connector, query: str(query)  # type: ignore[method-assign]  # noqa: SLF001
    load_config = SimpleNamespace(
        source_table="orders",
        options={"sink_type": "postgres", "partition_tmp_dir": str(tmp_path)},
        export_format="csv",
        compress_export=False,
    )
    artifact = strategy._export_to_file_batched(  # noqa: SLF001
        "SELECT id FROM public.orders", [("id", "integer")], load_config, batch_size=1
    )

    batches = list(artifact.batch_generator())
    assert len(batches) == 1
    assert len(list(tmp_path.iterdir())) == 1
    batches[0].cleanup()
    assert list(tmp_path.iterdir()) == []


def test_postgres_mssql_public_wire_rejects_non_csv_before_export() -> None:
    strategy = DummyPostgresStrategy(connector=StubConnector(), logger=StubLogger())
    load_config = SimpleNamespace(
        options={"sink_type": "mssql"},
        export_format="mssql-delimited",
        compress_export=False,
    )

    with pytest.raises(
        ValueError,
        match="DPONE_POSTGRES_MSSQL_WIRE_CONTRACT_BLOCKED: postgres_mssql.export_format",
    ):
        strategy._effective_postgres_file_wire(load_config)  # noqa: SLF001


def test_postgres_mssql_public_csv_projects_to_internal_codec_wire() -> None:
    strategy = DummyPostgresStrategy(connector=StubConnector(), logger=StubLogger())
    load_config = SimpleNamespace(
        options={"sink_type": "mssql"},
        export_format="csv",
        compress_export=False,
    )

    assert strategy._effective_postgres_file_wire(load_config) == ("mssql-delimited", False)  # noqa: SLF001


def test_postgres_mssql_bulk_wrap_projects_bit_and_datetimeoffset() -> None:
    strategy = DummyPostgresStrategy(connector=StubConnector(), logger=StubLogger())
    from dpone.runtime.support.bulk_text_codec import BulkTextCodec

    wrapped = strategy._wrap_mssql_bulk_text_query(  # noqa: SLF001
        "SELECT * FROM src",
        [
            ("id", "int"),
            ("c_bool", "bit"),
            ("c_ts", "datetimeoffset(6)"),
            ("c_name", "nvarchar(max)"),
        ],
        BulkTextCodec(),
    )

    assert "WHEN dpone_src.\"c_bool\" THEN '1' ELSE '0' END" in wrapped
    assert "to_char(dpone_src.\"c_ts\" AT TIME ZONE 'UTC'" in wrapped
    assert "YYYY-MM-DD HH24:MI:SS.US+00:00" in wrapped
    assert "WHEN (dpone_src.\"c_name\")::text = ''" in wrapped


def test_postgres_clickhouse_bulk_wrap_projects_bool_and_datetime64() -> None:
    strategy = DummyPostgresStrategy(connector=StubConnector(), logger=StubLogger())
    from dpone.runtime.support.bulk_text_codec import BulkTextCodec

    wrapped = strategy._wrap_mssql_bulk_text_query(  # noqa: SLF001
        "SELECT * FROM src",
        [
            ("c_bool", "Bool"),
            ("c_ts", "DateTime64(6, 'UTC')"),
            ("c_naive", "DateTime64(6)"),
        ],
        BulkTextCodec(),
    )

    assert "WHEN dpone_src.\"c_bool\" THEN '1' ELSE '0' END" in wrapped
    assert "to_char(dpone_src.\"c_ts\" AT TIME ZONE 'UTC'" in wrapped
    assert "YYYY-MM-DD HH24:MI:SS.US')" in wrapped
    assert "to_char(dpone_src.\"c_naive\", 'YYYY-MM-DD HH24:MI:SS.US')" in wrapped
