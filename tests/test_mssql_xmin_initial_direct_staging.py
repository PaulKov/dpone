"""Direct typed-staging contracts for PostgreSQL XMin initial loads."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact
from dpone.runtime.etl.file_contract_validation import validate_mssql_delimited_file_contract
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager
from dpone.runtime.sinks.staging_managers.mssql_staging_support import (
    internal_direct_native_staging,
)
from dpone.runtime.sinks.strategies.mssql import mssql_native_staging as native_staging_module
from dpone.runtime.sinks.strategies.mssql.mssql_initial_typed_staging import (
    plan_direct_xmin_initial_staging,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_schema import ResolvedMssqlNativeSchema
from dpone.runtime.sinks.strategies.mssql.mssql_native_staging import MssqlNativeStagingNormalizer
from dpone.runtime.support.bulk_text_codec import BulkTextCodec


def _config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="events",
        target_schema="dbo",
        target_table="events",
        staging_schema="staging",
        load_strategy=LoadStrategy.BACKFILL,
        unique_key=["id"],
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "incremental_strategy": "xmin",
            "xmin_execution": {"mode": "initial", "handoff_id": "events_v1"},
            "backfill": {
                "inner_mode": "incremental_merge",
                "parallel_workers": 4,
                "state": {"backend": "audit_schema", "require_distributed_lock": True},
                "chunk": {"column": "id", "kind": "uuid", "buckets": 512},
            },
        },
    )


def _artifact(tmp_path) -> FileExportArtifact:
    path = tmp_path / "events.bcp"
    path.write_text("00000000-0000-0000-0000-000000000001\talpha\n", encoding="utf-8")
    return FileExportArtifact(
        str(path),
        ("id", "name"),
        format="mssql-delimited",
        bulk_text_codec=BulkTextCodec(),
        rows_exported=1,
    )


def _resolved(*, renamed: bool = False, name_type: str = "nvarchar(100)") -> ResolvedMssqlNativeSchema:
    target_name = "display_name" if renamed else "name"
    generated = (
        SimpleNamespace(
            name="__dpone__row_hash",
            target_type="varchar(64)",
            nullable=False,
            placeholder_sql="CONVERT(varchar(64), REPLICATE('0', 64))",
            generation_contract="dpone.mssql.native-strategy-metadata.v1",
        ),
        SimpleNamespace(
            name="__dpone__deleted_at",
            target_type="datetime2(7)",
            nullable=True,
            placeholder_sql="CONVERT(datetime2(7), NULL)",
            generation_contract="dpone.mssql.native-strategy-metadata.v1",
        ),
    )
    return ResolvedMssqlNativeSchema(
        types={
            "id": "uniqueidentifier",
            target_name: name_type,
            "__dpone__row_hash": "varchar(64)",
            "__dpone__deleted_at": "datetime2(7)",
        },
        source_types={"id": "uniqueidentifier", "name": name_type},
        nullability={
            "id": False,
            target_name: True,
            "__dpone__row_hash": False,
            "__dpone__deleted_at": True,
        },
        collations={},
        wire_to_target={"id": "id", "name": target_name},
        generated_columns=generated,
    )


def test_xmin_initial_plan_materializes_one_native_table_with_business_wire_prefix(tmp_path) -> None:
    plan = plan_direct_xmin_initial_staging(
        _config(),
        _artifact(tmp_path),
        (("id", "uuid"), ("name", "text")),
        _resolved(),
    )

    assert plan is not None
    assert plan.staging_schema == (
        ("id", "uniqueidentifier"),
        ("name", "nvarchar(100)"),
        ("__dpone__row_hash", "varchar(64)"),
        ("__dpone__deleted_at", "datetime2(7)"),
    )
    options = plan.load_config.options
    assert internal_direct_native_staging(plan.load_config) is True
    assert options["__dpone_mssql_typed_file_staging_v1"] is True
    assert options["__dpone_mssql_native_omitted_columns"] == [
        "__dpone__row_hash",
        "__dpone__deleted_at",
    ]
    assert options["__dpone_mssql_wire_schema"] == [["id", "uuid"], ["name", "text"]]


def test_xmin_initial_plan_accepts_source_validated_file_wrapper(tmp_path) -> None:
    artifact = _artifact(tmp_path)
    schema = (("id", "uuid"), ("name", "text"))
    contract = SchemaContract.from_config(
        {
            "enforcement": "strict",
            "columns": {
                "id": {"type": "uuid", "nullable": False},
                "name": {"type": "string", "nullable": False},
            },
        }
    )
    validate_mssql_delimited_file_contract(artifact, schema=schema, contract=contract)
    validated = ContractValidatedFileArtifact(
        artifact,
        contract=contract,
        schema=schema,
        run_id="run-validated",
        load_id="load-validated",
    )

    plan = plan_direct_xmin_initial_staging(
        _config(),
        validated,
        schema,
        _resolved(),
    )

    assert plan is not None
    assert internal_direct_native_staging(plan.load_config) is True


def test_xmin_initial_plan_falls_back_before_ddl_for_rename_or_unsupported_type(tmp_path) -> None:
    artifact = _artifact(tmp_path)
    wire_schema = (("id", "uuid"), ("name", "text"))

    assert plan_direct_xmin_initial_staging(_config(), artifact, wire_schema, _resolved(renamed=True)) is None
    assert (
        plan_direct_xmin_initial_staging(
            _config(),
            artifact,
            wire_schema,
            _resolved(name_type="xml"),
        )
        is None
    )


def test_manifest_like_boolean_cannot_forge_direct_native_authority(tmp_path) -> None:
    plan = plan_direct_xmin_initial_staging(
        _config(),
        _artifact(tmp_path),
        (("id", "uuid"), ("name", "text")),
        _resolved(),
    )
    assert plan is not None
    forged_options = dict(plan.load_config.options)
    forged_options["__dpone_mssql_direct_native_staging_v1"] = True

    assert internal_direct_native_staging(replace(plan.load_config, options=forged_options)) is False


def test_direct_plan_creates_native_business_columns_and_nullable_omitted_suffix(tmp_path) -> None:
    plan = plan_direct_xmin_initial_staging(
        _config(),
        _artifact(tmp_path),
        (("id", "uuid"), ("name", "text")),
        _resolved(),
    )
    assert plan is not None
    connector = _DdlConnector()

    staging = MSSQLStagingManager(connector).create(plan.load_config, plan.staging_schema)

    create_sql = next(query for query in connector.queries if query.startswith("CREATE TABLE"))
    assert "[id] uniqueidentifier NOT NULL" in create_sql
    assert "[__dpone__row_hash] varchar(64) NULL" in create_sql
    assert staging.target_column_nullability["__dpone__row_hash"] is False
    assert staging.wire_schema == (("id", "uuid"), ("name", "text"))
    assert staging.typed_file_row_hash_validation is False
    assert staging.typed_file_deferred_native_evidence is True
    assert staging.direct_native_staging is True


class _DdlConnector:
    trust_server_certificate = "yes"

    def __init__(self) -> None:
        self.queries: list[str] = []

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    @staticmethod
    def qualified_name(schema: str, table: str, *, database: str | None = None) -> str:
        prefix = f"[{database}]." if database else ""
        return f"{prefix}[{schema}].[{table}]"

    def execute_query(self, query: str, *_args, **_kwargs) -> int:
        self.queries.append(str(query))
        return 1


def test_direct_native_normalizer_reuses_one_table_and_projects_metadata_once(monkeypatch) -> None:
    monkeypatch.setattr(
        native_staging_module,
        "validate_native_conversions",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("direct native staging must not run raw SQL conversion validation")
        ),
    )
    connector = _NativeConnector()
    staging_manager = _NativeStaging()
    strategy = SimpleNamespace(
        connector=connector,
        staging_manager=staging_manager,
        _staging_name=lambda artifact: f"[{artifact.schema}].[{artifact.table}]",
    )
    raw = StagingTableArtifact(
        schema="staging",
        table="events_native",
        columns=("id", "name", "__dpone__row_hash", "__dpone__deleted_at"),
        staging_manager=staging_manager,
        row_count=2,
        column_types={name: dtype for name, dtype in _resolved().target_schema},
        target_column_types={name: dtype for name, dtype in _resolved().target_schema},
        target_column_nullability=dict(_resolved().nullability),
        direct_native_staging=True,
        typed_file_ingestion=True,
        typed_file_deferred_native_evidence=True,
    )

    normalized = MssqlNativeStagingNormalizer(strategy).normalize(
        _config(),
        raw,
        (("id", "uuid"), ("name", "text")),
        _resolved(),
        lineage=SimpleNamespace(columns=()),
    )

    assert normalized is raw
    sql = "\n".join(connector.queries)
    assert "INSERT INTO" not in sql
    assert "TRY_CONVERT(" not in sql
    assert "NCHAR(29)" not in sql
    metadata_updates = [query for query in connector.queries if query.startswith("UPDATE n SET")]
    assert len(metadata_updates) == 1
    assert "[__dpone__row_hash] =" in metadata_updates[0]
    assert "HASHBYTES('SHA2_256'" in metadata_updates[0]
    assert staging_manager.finalized == (raw, raw)


def test_direct_native_rejects_bulk_text_codec_leak_before_sql() -> None:
    connector = _NativeConnector()
    staging_manager = _NativeStaging()
    strategy = SimpleNamespace(
        connector=connector,
        staging_manager=staging_manager,
        _staging_name=lambda artifact: f"[{artifact.schema}].[{artifact.table}]",
    )
    raw = StagingTableArtifact(
        schema="staging",
        table="events_native",
        columns=("id", "name", "__dpone__row_hash", "__dpone__deleted_at"),
        staging_manager=staging_manager,
        row_count=2,
        column_types={name: dtype for name, dtype in _resolved().target_schema},
        target_column_types={name: dtype for name, dtype in _resolved().target_schema},
        target_column_nullability=dict(_resolved().nullability),
        direct_native_staging=True,
        typed_file_ingestion=True,
        typed_file_deferred_native_evidence=True,
        bulk_text_codec=BulkTextCodec(),
    )

    with pytest.raises(
        RuntimeError,
        match="mssql_native_projection.direct_staging_contract_invalid",
    ):
        MssqlNativeStagingNormalizer(strategy).normalize(
            _config(),
            raw,
            (("id", "uuid"), ("name", "text")),
            _resolved(),
            lineage=SimpleNamespace(columns=()),
        )

    assert connector.queries == []


class _NativeConnector(_DdlConnector):
    def get_records(self, query: str, *_args, **_kwargs):
        self.queries.append(str(query))
        return [(2,)] if str(query).startswith("SELECT COUNT_BIG(*)") else []


class _NativeStaging:
    def __init__(self) -> None:
        self.finalized = None

    @contextmanager
    def database_authority_scope(self, _artifact):
        yield

    def create(self, *_args, **_kwargs):
        raise AssertionError("direct native staging must not create a second table")

    def finalize_native_evidence(self, raw, native, *, columns) -> None:
        assert columns
        self.finalized = (raw, native)

    def drop(self, _artifact) -> None:
        return None
