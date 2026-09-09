"""Focused PostgreSQL→MSSQL provenance and schema-evolution regressions."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.incremental_snapshot import MSSQL_TEXT_KEY_COLLATION
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.contracts.postgres_mssql_type_policy import declared_postgres_mssql_contract_blockers
from dpone.readiness.schema_evolution import (
    ColumnDef,
    SchemaComparator,
    SchemaComparisonError,
    SchemaEvolutionPolicy,
)
from dpone.readiness.schema_type_compatibility import MssqlSchemaTypeCompatibility
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.connectors.mssql_sql import MSSQLSqlRenderer
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.schema_evolution import SchemaEvolutionError, SchemaEvolutionService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.support.postgres_mssql_projection import (
    PostgresMssqlProjectionError,
    ensure_postgres_mssql_payload_projection,
    project_postgres_mssql_relation,
)
from dpone.type_system.source_sink.provenance import SourceColumnProvenance, SourceRelationDialect


def _config(*, options: dict | None = None, unique_key: str | None = None) -> LoadConfig:
    return LoadConfig(
        source_conn_id="postgres",
        target_conn_id="mssql",
        source_schema="public",
        source_table="items",
        target_schema="dbo",
        target_table="items",
        load_strategy=LoadStrategy.FULL_REFRESH,
        unique_key=unique_key,
        options=options or {},
    )


def _metadata(name: str, dtype: str, *, nullable: bool) -> SourceColumnProvenance:
    return SourceColumnProvenance(name=name, declared_type=dtype, nullable=nullable)


@pytest.mark.parametrize(
    "options",
    [
        {},
        {"physical_design": {"columns": {"tags": {"target_type": {"mssql": "nvarchar(max)"}}}}},
        {"schema_contract": {"columns": {"tags": {"type": "integer", "nullable": True}}}},
        {"schema_contract": {"columns": {"tags": {"type": "json", "nullable": True}}}},
    ],
)
def test_complex_postgres_family_requires_semantic_string_contract_before_export(options: dict) -> None:
    schema = (("tags", 'array:{"name":"_text","schema":"pg_catalog"}'),)

    with pytest.raises(PostgresMssqlProjectionError, match="explicit_contract_required:tags"):
        project_postgres_mssql_relation(
            schema,
            _config(options=options),
            relation_metadata=(_metadata("tags", schema[0][1], nullable=True),),
        )


def test_complex_postgres_string_contract_requires_textual_physical_target() -> None:
    dtype = 'enum:{"name":"status","schema":"public"}'
    options = {
        "schema_contract": {"columns": {"status": {"type": "string", "nullable": False}}},
        "physical_design": {"columns": {"status": {"target_type": {"mssql": "int"}}}},
    }

    with pytest.raises(PostgresMssqlProjectionError, match="textual_target_required:status"):
        project_postgres_mssql_relation(
            (("status", dtype),),
            _config(options=options),
            relation_metadata=(_metadata("status", dtype, nullable=False),),
        )


@pytest.mark.parametrize("enforcement", ["coerce", "quarantine", "warn"])
def test_opaque_postgres_mssql_file_blocks_unimplemented_contract_enforcement_before_export(
    enforcement: str,
) -> None:
    options = {
        "schema_contract": {
            "enforcement": enforcement,
            "columns": {"value": {"type": "string", "nullable": True}},
        }
    }

    with pytest.raises(
        PostgresMssqlProjectionError,
        match=f"file_enforcement_unsupported:{enforcement}",
    ):
        project_postgres_mssql_relation(
            (("value", "text"),),
            _config(options=options),
            relation_metadata=(_metadata("value", "text", nullable=True),),
        )

    assert declared_postgres_mssql_contract_blockers(
        (("value", "text"),),
        options,
    )[0].endswith(f"file_enforcement_unsupported:{enforcement}")


def test_projection_carries_observed_nullability_and_one_key_collation_authority() -> None:
    schema = (("metric_code", "character varying(20)"), ("value", "numeric(3,5)"))
    projection = project_postgres_mssql_relation(
        schema,
        _config(unique_key="metric_code"),
        relation_metadata=(
            _metadata("metric_code", schema[0][1], nullable=False),
            _metadata("value", schema[1][1], nullable=True),
        ),
    )

    assert projection.target_schema == (("metric_code", "nvarchar(40)"), ("value", "decimal(5,5)"))
    assert projection.target_nullability == {"metric_code": False, "value": True}
    assert projection.target_collations == {"metric_code": MSSQL_TEXT_KEY_COLLATION}


def test_conflicting_explicit_text_key_collation_is_rejected_before_export() -> None:
    options = {
        "physical_design": {"columns": {"metric_code": {"collation": {"mssql": "SQL_Latin1_General_CP1_CI_AS"}}}}
    }

    with pytest.raises(PostgresMssqlProjectionError, match="unique_key_collation_conflict:metric_code"):
        project_postgres_mssql_relation(
            (("metric_code", "text"),),
            _config(options=options, unique_key="metric_code"),
            relation_metadata=(_metadata("metric_code", "text", nullable=False),),
        )


@pytest.mark.parametrize("schema", [(("Foo", "integer"), ("foo", "integer")), (("x", "integer"), ("x", "bigint"))])
def test_casefold_or_exact_source_identity_collision_is_rejected_before_export(schema) -> None:
    assert declared_postgres_mssql_contract_blockers(schema, {}) == (
        "postgres_mssql.type_contract.source_provenance_invalid",
    )
    with pytest.raises(PostgresMssqlProjectionError, match="source_provenance_invalid"):
        project_postgres_mssql_relation(schema, _config())


def test_postgres_clickhouse_payload_does_not_enter_mssql_projection() -> None:
    config = _config(options={"source_type": "postgres", "sink_type": "clickhouse"})
    payload = LoadPayload(
        artifact=SimpleNamespace(),
        schema=(("id", "Int32"),),
        relation_schema=(("id", "integer"),),
        relation_metadata=(_metadata("id", "integer", nullable=False),),
        relation_dialect=SourceRelationDialect.POSTGRES,
    )

    assert ensure_postgres_mssql_payload_projection(config, payload) is payload


def test_schema_comparator_rejects_casefold_ambiguity_and_case_only_drift() -> None:
    comparator = SchemaComparator(type_compatibility=MssqlSchemaTypeCompatibility())

    with pytest.raises(SchemaComparisonError, match="identifier_case_ambiguous:source:Foo/foo"):
        comparator.compare([ColumnDef("Foo", "int"), ColumnDef("foo", "int")], [])

    plan = comparator.compare([ColumnDef("Foo", "int")], [ColumnDef("foo", "int")])
    assert plan.changes[0].change_type == "column_case_change"
    assert plan.has_breaking_changes is True


class _MSSQLSchemaSink:
    def __init__(self, columns: list[MssqlCatalogColumn]) -> None:
        self.columns = columns
        self.connector = SimpleNamespace()
        self.applied: list[str] = []

    def get_target_columns(self, _load_config):
        return self.columns

    def target_table_exists(self, _load_config):
        return True

    def apply_schema_plan(self, load_config, plan):
        self.applied.extend(plan.ddl_sql("mssql", f"{load_config.target_schema}.{load_config.target_table}"))


def test_variant_column_binds_wire_position_to_generated_target_without_touching_bytes(tmp_path) -> None:
    path = tmp_path / "payload.tsv"
    path.write_text("00000000-0000-0000-0000-000000000001\t9.99\n", encoding="utf-8")
    schema = (("id", "uuid"), ("amount", "character varying(10)"))
    config = _config(
        unique_key="id",
        options={"schema_evolution": {"on_type_change": "new_column"}},
    )
    metadata = (_metadata("id", "uuid", nullable=False), _metadata("amount", schema[1][1], nullable=True))
    projection = project_postgres_mssql_relation(schema, config, relation_metadata=metadata)
    payload = LoadPayload(
        artifact=FileExportArtifact(str(path), ["id", "amount"], rows_exported=1),
        schema=projection.projected_schema,
        relation_schema=schema,
        relation_metadata=metadata,
        relation_dialect=SourceRelationDialect.POSTGRES,
        target_projection=projection,
    )
    sink = _MSSQLSchemaSink(
        [
            MssqlCatalogColumn("id", "uniqueidentifier", False),
            MssqlCatalogColumn("amount", "int", True),
        ]
    )
    before = path.read_bytes()

    prepared = SchemaEvolutionService().prepare_payload(config, sink, payload)

    assert prepared.schema == (("id", "uniqueidentifier"), ("amount", "nvarchar(20)"))
    assert tuple(prepared.artifact.columns) == ("id", "amount")
    assert prepared.target_projection.relation_schema == schema
    assert prepared.target_projection.projected_schema == tuple(prepared.schema)
    assert prepared.target_projection.target_projected_schema == (
        ("id", "uniqueidentifier"),
        ("__dpone__nc__amount", "nvarchar(20)"),
    )
    assert prepared.target_projection.columns[1].source_name == "amount"
    assert path.read_bytes() == before
    assert sink.applied == ["ALTER TABLE [dbo].[items] ADD [__dpone__nc__amount] nvarchar(20) NULL"]


def test_variant_column_existing_compatibility_column_rebinds_without_ddl(tmp_path) -> None:
    path = tmp_path / "payload.tsv"
    path.write_text("9.99\n", encoding="utf-8")
    schema = (("amount", "character varying(10)"),)
    config = _config(options={"schema_evolution": {"on_type_change": "new_column"}})
    metadata = (_metadata("amount", schema[0][1], nullable=True),)
    projection = project_postgres_mssql_relation(schema, config, relation_metadata=metadata)
    payload = LoadPayload(
        artifact=FileExportArtifact(str(path), ["amount"], rows_exported=1),
        schema=projection.projected_schema,
        relation_schema=schema,
        relation_metadata=metadata,
        relation_dialect=SourceRelationDialect.POSTGRES,
        target_projection=projection,
    )
    sink = _MSSQLSchemaSink(
        [
            MssqlCatalogColumn("amount", "int", True),
            MssqlCatalogColumn("__dpone__nc__amount", "nvarchar(20)", True),
        ]
    )

    prepared = SchemaEvolutionService().prepare_payload(config, sink, payload)

    assert prepared.target_projection.projected_schema == (("amount", "nvarchar(20)"),)
    assert prepared.target_projection.target_projected_schema == (("__dpone__nc__amount", "nvarchar(20)"),)
    assert tuple(prepared.artifact.columns) == ("amount",)
    assert sink.applied == []


def test_incompatible_unique_key_cannot_route_to_generated_column() -> None:
    plan = SchemaComparator(
        policy=SchemaEvolutionPolicy(
            on_type_change="new_column",
            protected_columns=("id",),
        ),
        type_compatibility=MssqlSchemaTypeCompatibility(),
    ).compare(
        [ColumnDef("id", "nvarchar(20)", nullable=False)],
        [ColumnDef("id", "int", nullable=False)],
    )

    assert plan.column_mapping == {}
    assert plan.changes[0].change_type == "protected_column_type_change"
    assert plan.changes[0].breaking is True


def test_unique_key_variant_is_blocked_by_catalog_preflight_before_row_export() -> None:
    config = _config(
        unique_key="id",
        options={"schema_evolution": {"on_type_change": "new_column"}},
    )
    schema = (("id", "character varying(10)"),)
    projection = project_postgres_mssql_relation(
        schema,
        config,
        relation_metadata=(_metadata("id", schema[0][1], nullable=False),),
    )

    class PostgresCatalogSource:
        rows_exported = 0

        def fetch_schema_projection(self, _load_config):
            return SimpleNamespace(target_projection=projection)

        def extract(self, *_args):
            self.rows_exported += 1
            raise AssertionError("row export must not start")

    source = PostgresCatalogSource()
    sink = _MSSQLSchemaSink([MssqlCatalogColumn("id", "int", False)])

    with pytest.raises(
        SchemaEvolutionError,
        match="DPONE_SCHEMA_EVOLUTION_BLOCKED: schema_evolution.protected_column_type_change:id",
    ):
        SchemaEvolutionService().preflight_before_extract(
            load_config=config,
            source=source,
            sink=sink,
        )

    assert source.rows_exported == 0


def test_mssql_schema_evolution_ddl_uses_configured_database_not_connector_default() -> None:
    config = _config(options={"schema_evolution": {"apply_safe": True}})
    config.target_database = "TargetB"

    class Connector:
        database = "DefaultA"

        def __init__(self) -> None:
            self.statements: list[str] = []

        def execute_query(self, statement: str) -> None:
            self.statements.append(statement)

    class MSSQLCatalogSink:
        def __init__(self) -> None:
            self.connector = Connector()

        def get_target_columns(self, _load_config):
            return [MssqlCatalogColumn("id", "int", True)]

        def target_table_exists(self, _load_config):
            return True

    sink = MSSQLCatalogSink()
    payload = LoadPayload(
        artifact=SimpleNamespace(),
        schema=(("id", "int"), ("new_value", "nvarchar(20)")),
    )

    SchemaEvolutionService().prepare_payload(config, sink, payload)

    ddl = "\n".join(sink.connector.statements)
    assert "ALTER TABLE [TargetB].[dbo].[items] ADD [new_value] nvarchar(20) NULL" in ddl
    assert "ALTER TABLE [dbo].[items]" not in ddl
    assert "DefaultA" not in ddl


def test_mssql_catalog_projection_preserves_nullability_collation_and_temporal_scale() -> None:
    captured = {}

    def get_records(query, params, *, as_dict):
        captured["query"] = query
        captured["params"] = params
        captured["as_dict"] = as_dict
        return [
            {
                "COLUMN_NAME": "ObservedAt",
                "DATA_TYPE": "datetimeoffset",
                "CHARACTER_MAXIMUM_LENGTH": None,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": 6,
                "DATETIME_PRECISION": 6,
                "IS_NULLABLE": "NO",
                "COLLATION_NAME": None,
            },
            {
                "COLUMN_NAME": "Code",
                "DATA_TYPE": "nvarchar",
                "CHARACTER_MAXIMUM_LENGTH": 40,
                "NUMERIC_PRECISION": None,
                "NUMERIC_SCALE": None,
                "DATETIME_PRECISION": None,
                "IS_NULLABLE": "YES",
                "COLLATION_NAME": MSSQL_TEXT_KEY_COLLATION,
            },
        ]

    connector = SimpleNamespace(get_records=get_records, _sql=MSSQLSqlRenderer())
    columns = MSSQLConnector.fetch_schema_columns(connector, "dbo", "items")

    assert columns == [
        MssqlCatalogColumn("ObservedAt", "datetimeoffset(6)", False),
        MssqlCatalogColumn("Code", "nvarchar(40)", True, MSSQL_TEXT_KEY_COLLATION),
    ]
    assert "DATETIME_PRECISION" in captured["query"]
    assert captured["params"] == ("dbo", "items")
    assert captured["as_dict"] is True


def test_runtime_blocks_nullability_tightening_before_ddl_or_load() -> None:
    config = _config()
    metadata = (_metadata("id", "integer", nullable=False),)
    projection = project_postgres_mssql_relation((("id", "integer"),), config, relation_metadata=metadata)
    payload = LoadPayload(
        artifact=SimpleNamespace(),
        schema=projection.projected_schema,
        relation_schema=(("id", "integer"),),
        relation_metadata=metadata,
        relation_dialect=SourceRelationDialect.POSTGRES,
        target_projection=projection,
    )
    sink = _MSSQLSchemaSink([MssqlCatalogColumn("id", "int", True)])

    with pytest.raises(SchemaEvolutionError, match="DPONE_SCHEMA_EVOLUTION_BLOCKED"):
        SchemaEvolutionService().prepare_payload(config, sink, payload)

    assert sink.applied == []


def test_runtime_relaxes_target_not_null_only_in_authorized_safe_window() -> None:
    config = _config(options={"schema_evolution": {"ddl_mode": "safe_window"}})
    metadata = (_metadata("id", "integer", nullable=True),)
    projection = project_postgres_mssql_relation((("id", "integer"),), config, relation_metadata=metadata)
    payload = LoadPayload(
        artifact=SimpleNamespace(),
        schema=projection.projected_schema,
        relation_schema=(("id", "integer"),),
        relation_metadata=metadata,
        relation_dialect=SourceRelationDialect.POSTGRES,
        target_projection=projection,
    )
    sink = _MSSQLSchemaSink([MssqlCatalogColumn("id", "int", False)])

    SchemaEvolutionService().prepare_payload(config, sink, payload)

    assert sink.applied == ["ALTER TABLE [dbo].[items] ALTER COLUMN [id] int NULL"]


@pytest.mark.parametrize(
    ("desired", "existing", "equal", "widening"),
    [
        ("datetimeoffset(6)", "datetime2(6)", False, False),
        ("datetime2(6)", "datetime2(3)", False, True),
        ("datetime2(3)", "datetime2(6)", False, False),
        ("float(53)", "real", False, True),
        ("real", "float(53)", False, False),
        ("nvarchar(200)", "nvarchar(100)", False, True),
        ("nvarchar(100)", "nvarchar(200)", False, False),
        ("nvarchar(100)", "varchar(100)", False, False),
        ("decimal(16,8)", "decimal(10,2)", False, True),
        ("decimal(12,8)", "decimal(10,2)", False, False),
        ("numeric(10,2)", "decimal(10,2)", True, False),
    ],
)
def test_exact_mssql_evolution_type_semantics(
    desired: str,
    existing: str,
    equal: bool,
    widening: bool,
) -> None:
    compatibility = MssqlSchemaTypeCompatibility()

    assert compatibility.types_equal(desired, existing) is equal
    assert compatibility.is_widening(desired, existing) is widening
