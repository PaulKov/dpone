"""

Rebase note: CI re-check after master tip includes #421.
ClickHouse server-side SQL extract must publish quality source authority."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dpone.config import LoadConfig, LoadStrategy
from dpone.governance.quality import QualityGatePolicy, QualityGateRunner
from dpone.runtime.governance.quality_probe_snapshots import source_snapshot
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.sinks.clickhouse_payload_ingestion import ClickHousePayloadIngestionService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sources.strategies.clickhouse.clickhouse_full_extract import (
    ClickHouseFullExtractStrategy,
)
from dpone.runtime.sql_query_artifact import SqlQueryArtifact
from dpone.runtime.streaming_rows import StreamingRowsArtifact


def test_clickhouse_sql_query_extract_publishes_rows_exported_for_quality_probe(
    tmp_path: Path,
) -> None:
    """COUNT at extract is independent source authority; estimated_rows alone is not."""

    sql_file = tmp_path / "example_customer_mart.sql"
    sql_file.write_text("SELECT 1 AS id", encoding="utf-8")
    connector = _QueryConnector()
    strategy = ClickHouseFullExtractStrategy(
        connector=connector,
        logger=SimpleNamespace(log_etl_progress=lambda *a: None),
    )

    result = strategy.extract(
        _load_config(
            options={
                "manifest_dir": str(tmp_path),
                "query": {"mode": "sql_file", "sql_file": "example_customer_mart.sql"},
            }
        ),
        last_state=None,
    )

    assert isinstance(result.artifact, SqlQueryArtifact)
    assert result.artifact.estimated_rows == 2
    assert result.artifact.rows_exported == 2
    assert result.artifact.row_count == 2
    assert source_snapshot(result).row_count == 2


def test_clickhouse_sql_query_insert_select_publishes_rows_exported_when_count_missing() -> None:
    """Sink-side INSERT…SELECT must still publish authority if extract COUNT was unavailable."""

    connector = _StagingConnector()
    sink = _StagingSink(connector)
    service = ClickHousePayloadIngestionService(sink, sink_factory=lambda _connector: sink)
    artifact = SqlQueryArtifact(
        sql="SELECT id FROM DWH_Raw.orders",
        dialect="clickhouse",
        sql_hash="sha256:abc",
        estimated_rows=None,
    )
    extract_result = SimpleNamespace(artifact=artifact, typed_hash=None)

    inserted = service.insert_payload(
        _load_config(target_schema="Example_Datamarts", target_table="orders__dpone_staging"),
        LoadPayload(artifact=artifact, schema=[("id", "UInt64")]),
    )

    assert inserted == 2
    assert artifact.rows_exported == 2
    assert artifact.row_count == 2
    assert source_snapshot(extract_result).row_count == 2
    assert any("SELECT count()" in query for query in connector.queries)


def test_clickhouse_sql_query_source_target_count_passes_with_independent_source_count(
    tmp_path: Path,
) -> None:
    sql_file = tmp_path / "example_customer_mart.sql"
    sql_file.write_text("SELECT 1 AS id", encoding="utf-8")
    strategy = ClickHouseFullExtractStrategy(
        connector=_QueryConnector(),
        logger=SimpleNamespace(log_etl_progress=lambda *a: None),
    )
    extract = strategy.extract(
        _load_config(
            options={
                "manifest_dir": str(tmp_path),
                "query": {"mode": "sql_file", "sql_file": "example_customer_mart.sql"},
            }
        ),
        last_state=None,
    )

    source, target = LoadGovernanceService().quality_probe_snapshots(
        extract_result=extract,
        load_result=SimpleNamespace(staging_rows=2, total_rows=2, typed_hash=None),
    )
    assert source.row_count == 2
    assert target.row_count == 2
    report = QualityGateRunner().run(
        QualityGatePolicy.from_config({"gates": [{"id": "source_target_count", "type": "row_count_reconciliation"}]}),
        source=source,
        target=target,
    )
    assert report.passed is True


def test_clickhouse_to_mssql_table_extract_streams_without_preflight_count() -> None:
    for sink_type in ("mssql", "sqlserver", "sql_server"):
        connector = _StreamingConnector()
        strategy = ClickHouseFullExtractStrategy(
            connector=connector,
            logger=SimpleNamespace(log_etl_progress=lambda *a: None),
        )

        extract = strategy.extract(
            _load_config(
                options={
                    "source_type": "clickhouse",
                    "sink_type": sink_type,
                }
            ),
            last_state=None,
        )

        assert isinstance(extract.artifact, StreamingRowsArtifact)
        assert not any("estimated_count" in query for query in connector.queries)
        assert extract.artifact.estimated_rows is None
        handle = extract.artifact.materialize(
            _StreamingManager(),
            load_config=SimpleNamespace(),
            schema=extract.schema,
        )
        assert handle.row_count == 2
        assert extract.artifact.rows_exported == 2


def _load_config(
    *,
    target_schema: str = "analytics",
    target_table: str = "wide_mart",
    options: dict[str, object] | None = None,
) -> LoadConfig:
    return LoadConfig(
        source_conn_id="ClickHouse",
        target_conn_id="ClickHouse",
        source_schema="DWH_Raw",
        source_table="wide_mart_query",
        target_schema=target_schema,
        target_table=target_table,
        load_strategy=LoadStrategy.FULL_REFRESH,
        options=options or {},
    )


class _QueryConnector:
    database = "DWH_Raw"

    def __init__(self) -> None:
        self.queries: list[str] = []

    def get_records(self, query: str, _params: object = None, *, as_dict: bool = False) -> list[object]:
        self.queries.append(query)
        if "DESCRIBE SELECT" in query:
            rows = [{"name": "id", "type": "UInt8"}, {"name": "name", "type": "String"}]
            return rows if as_dict else [(row["name"], row["type"]) for row in rows]
        if "count()" in query:
            return [(2,)]
        raise AssertionError(query)


class _StagingConnector:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute_query(self, query: str) -> int:
        self.queries.append(query)
        return 0

    def get_records(self, query: str) -> list[tuple[int]]:
        self.queries.append(query)
        return [(2,)]


class _StreamingConnector:
    database = "DWH_Raw"

    def __init__(self) -> None:
        self.queries: list[str] = []

    def get_records(self, query: str, params=None, *, as_dict: bool = False):  # noqa: ANN001
        del params
        self.queries.append(str(query))
        if "system.columns" in query:
            assert as_dict is True
            return [{"name": "id", "type": "UInt64"}]
        raise AssertionError(query)

    def get_records_streaming(self, query, params=None, batch_size=10_000, as_dict=False):  # noqa: ANN001
        del params, batch_size
        self.queries.append(str(query))
        assert as_dict is True
        yield [{"id": 1}, {"id": 2}]


class _StreamingManager:
    @staticmethod
    def create(load_config, schema):  # noqa: ANN001
        del load_config
        return SimpleNamespace(
            row_count=0,
            columns=[name for name, _dtype in schema],
            qualified_name=lambda: "DWH_Dev.ch.work",
        )

    @staticmethod
    def insert_streaming_rows(handle, rows):  # noqa: ANN001
        materialized = tuple(rows)
        handle.row_count = len(materialized)
        return len(materialized)


class _StagingSink:
    def __init__(self, connector: _StagingConnector) -> None:
        self.connector = connector

    @staticmethod
    def _table(load_config: LoadConfig) -> str:
        return f"`{load_config.target_schema}`.`{load_config.target_table}`"

    def _count(self, load_config: LoadConfig) -> int:
        del load_config
        return 2
