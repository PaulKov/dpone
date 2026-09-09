"""MSSQL schema evolution must compare the physical staging contract."""

from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.etl.mssql_fresh_target_preplan import resolve_mssql_target_columns
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.schema_evolution import SchemaEvolutionService
from dpone.runtime.schema_evolution_options import SchemaEvolutionError
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sinks.strategies.mssql.mssql_native_staging import MssqlNativeStagingNormalizer
from dpone.runtime.support.bulk_text_codec import BulkTextCodec
from dpone.type_system.source_sink.provenance import SourceColumnProvenance, SourceRelationDialect


class _CatalogConnector:
    """Minimal exact target catalog used by ``MSSQLSink`` introspection."""

    def __init__(self, columns: tuple[MssqlCatalogColumn, ...]) -> None:
        self.columns = columns

    @staticmethod
    def table_exists(_schema: str, _table: str, *, database: str | None = None) -> bool:
        return database == "DWH"

    def fetch_schema_columns(
        self,
        _schema: str,
        _table: str,
        *,
        database: str | None = None,
    ) -> tuple[MssqlCatalogColumn, ...]:
        assert database == "DWH"
        return self.columns


def test_non_frozen_schema_evolution_honors_value_guarded_clickhouse_text_overrides() -> None:
    """Regression: explicit bounded text is desired physical shape, not drift."""

    sink = _sink(
        MssqlCatalogColumn("sessionId", "nvarchar(128)", False, "Latin1_General_100_BIN2"),
        MssqlCatalogColumn("GUID", "nvarchar(36)", False, "Latin1_General_100_BIN2"),
    )
    config = _config(
        {
            "sessionId": {"target_type": {"mssql": "nvarchar(128)"}},
            "GUID": {"target_type": {"mssql": "nvarchar(36)"}},
        }
    )
    payload = _clickhouse_payload(("sessionId", "String", False), ("GUID", "String", False))

    prepared = SchemaEvolutionService().prepare_payload(config, sink, payload)

    assert prepared is payload


def test_non_frozen_schema_evolution_rejects_structurally_lossy_override() -> None:
    sink = _sink(MssqlCatalogColumn("counter", "int", False))
    config = _config({"counter": {"target_type": {"mssql": "int"}}}, unique_key=("counter",))
    payload = _clickhouse_payload(("counter", "UInt64", False))

    with pytest.raises(SnapshotReconciliationError, match="lossy_target_type_forbidden:counter"):
        SchemaEvolutionService().prepare_payload(config, sink, payload)


def test_non_frozen_schema_evolution_still_blocks_target_drift_from_override() -> None:
    """An override acknowledges one exact contract; it never ignores target drift."""

    sink = _sink(MssqlCatalogColumn("sessionId", "nvarchar(max)", False, "Latin1_General_100_BIN2"))
    config = _config(
        {"sessionId": {"target_type": {"mssql": "nvarchar(128)"}}},
        unique_key=("sessionId",),
    )
    payload = _clickhouse_payload(("sessionId", "String", False))

    with pytest.raises(SchemaEvolutionError, match="DPONE_SCHEMA_EVOLUTION_BLOCKED"):
        SchemaEvolutionService().prepare_payload(config, sink, payload)


def test_non_mssql_sink_keeps_source_native_schema_evolution_contract() -> None:
    """The projection is sink-scoped and cannot change another sink family."""

    sink = SimpleNamespace(
        get_target_schema=lambda _config: [("sessionId", "String", False)],
        target_table_exists=lambda _config: True,
    )
    config = _config({"sessionId": {"target_type": {"mssql": "nvarchar(128)"}}})
    payload = _clickhouse_payload(("sessionId", "String", False))

    prepared = SchemaEvolutionService().prepare_payload(config, sink, payload)

    assert prepared is payload


def test_schema_evolution_projection_matches_preplanner_for_every_column_attribute() -> None:
    """Default and explicit columns share preplanner type/nullability/collation."""

    source = [
        ColumnDef("sessionId", "String", nullable=False),
        ColumnDef(
            "GUID",
            "String",
            nullable=True,
            collation="SQL_Latin1_General_CP1_CI_AS",
        ),
        ColumnDef("note", "String", nullable=True),
    ]
    config = _config(
        {
            "sessionId": {"target_type": {"mssql": "nvarchar(128)"}},
            "GUID": {"target_type": {"mssql": "nvarchar(36)"}},
        },
        unique_key=("sessionId",),
    )

    projected = _sink().project_schema_evolution_source_columns(
        config,
        tuple((column.name, column.dtype, column.nullable, column.collation) for column in source),
    )

    assert [
        ColumnDef(name, dtype, nullable=nullable, collation=collation) for name, dtype, nullable, collation in projected
    ] == list(resolve_mssql_target_columns(config, source))
    assert projected == (
        ("sessionId", "nvarchar(128)", False, "Latin1_General_100_BIN2"),
        ("GUID", "nvarchar(36)", True, "SQL_Latin1_General_CP1_CI_AS"),
        ("note", "nvarchar(max)", True, None),
    )


def test_raw_staging_rejects_overwidth_override_before_native_or_business_mutation() -> None:
    """The schema projection is safe only while the native value gate is first."""

    events: list[str] = []

    class Connector:
        @staticmethod
        def quote_identifier(value: str) -> str:
            return f"[{value}]"

        @staticmethod
        def get_records(query: object, **_kwargs: object) -> list[dict[str, int]] | list[tuple[int]]:
            if "AS raw_reserved_bytes" in str(query):
                return [{"raw_reserved_bytes": 65_536, "available_data_bytes": 1_073_741_824}]
            if "COUNT_BIG" in str(query):
                events.append("decoded_count")
                return [(1,)]
            events.append("value_guard")
            return [{"invalid_hash": 0, "too_long": 1, "invalid": 0, "lossy": 0}]

        @staticmethod
        def execute_query(query: object, *_args: object, **_kwargs: object) -> None:
            events.append("decoded_insert" if "_decoded_" in str(query) else "native_insert")

        @staticmethod
        def commit() -> None:
            events.append("commit")

    class StagingManager:
        def create(self, config: object, schema: object) -> StagingTableArtifact:
            table = str(getattr(config, "staging_table"))
            if "_decoded_" not in table:
                events.append("native_staging_create")
                raise AssertionError("native staging must not be created after a failed value guard")
            events.append("decoded_staging_create")
            types = dict(schema)
            return StagingTableArtifact(
                database=str(getattr(config, "staging_database")),
                schema=str(getattr(config, "staging_schema")),
                table=table,
                columns=tuple(types),
                staging_manager=self,
                column_types=types,
                target_column_types=types,
            )

        @contextmanager
        def database_authority_scope(self, _artifact: object):
            yield

        @staticmethod
        def drop(_artifact: object) -> None:
            events.append("decoded_cleanup")

    connector = Connector()
    staging_manager = StagingManager()
    strategy = SimpleNamespace(
        connector=connector,
        staging_manager=staging_manager,
        _staging_name=lambda artifact: f"[{artifact.schema}].[{artifact.table}]",
    )
    normalizer = MssqlNativeStagingNormalizer(strategy)
    config = _config(
        {"sessionId": {"target_type": {"mssql": "nvarchar(3)"}}},
        unique_key=("sessionId",),
    )
    resolved = normalizer.resolve_schema(
        config,
        (("sessionId", "String"),),
        relation_schema=(("sessionId", "String"),),
        relation_metadata=None,
        relation_dialect=SourceRelationDialect.CLICKHOUSE,
        target_projection=None,
    )
    raw = StagingTableArtifact(
        schema="staging",
        table="events_raw",
        columns=("sessionId",),
        staging_manager=staging_manager,
        column_types={"sessionId": "nvarchar(max)"},
        bulk_text_codec=BulkTextCodec(),
        row_count=1,
    )

    with pytest.raises(SnapshotReconciliationError, match="mssql_native_projection.value_too_long"):
        normalizer.normalize(
            config,
            raw,
            (("sessionId", "String"),),
            resolved,
            lineage=SimpleNamespace(columns=()),
        )

    assert events == [
        "decoded_staging_create",
        "decoded_insert",
        "decoded_count",
        "value_guard",
        "decoded_cleanup",
    ]


def _sink(*columns: MssqlCatalogColumn) -> MSSQLSink:
    return MSSQLSink(_CatalogConnector(tuple(columns)))


def _config(
    columns: dict[str, object],
    *,
    unique_key: tuple[str, ...] = ("sessionId", "GUID"),
) -> LoadConfig:
    return LoadConfig(
        source_conn_id="clickhouse",
        target_conn_id="mssql",
        source_schema="marketing_datamarts",
        source_table="events",
        target_database="DWH",
        target_schema="ch",
        target_table="events",
        load_strategy=LoadStrategy.FULL_REFRESH,
        unique_key=list(unique_key),
        options={
            "source_type": "clickhouse",
            "sink_type": "mssql",
            "lineage": False,
            "physical_design": {"columns": columns},
            "schema_evolution": {"enabled": True, "mode": "additive"},
        },
    )


def _clickhouse_payload(*columns: tuple[str, str, bool]) -> LoadPayload:
    metadata = tuple(
        SourceColumnProvenance(name=name, declared_type=dtype, nullable=nullable) for name, dtype, nullable in columns
    )
    return LoadPayload(
        artifact=SimpleNamespace(),
        schema=[(name, dtype) for name, dtype, _nullable in columns],
        relation_schema=[(name, dtype) for name, dtype, _nullable in columns],
        relation_metadata=metadata,
        relation_dialect=SourceRelationDialect.CLICKHOUSE,
    )
