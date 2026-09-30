"""Selected step-audit storage must not follow the business connection database."""

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.runtime.state.load_step_audit import MSSQLLoadStepAuditStorage as RouteStepStorage
from dpone.runtime.state.mssql_load_step_audit import MSSQLLoadStepAuditStorage


def test_external_step_audit_inspects_selected_database_and_only_inserts() -> None:
    connector = _CatalogConnector()
    storage = _storage(connector)

    storage.record_step(_record())
    storage.record_step(_record())

    assert len(connector.reads) == 2  # columns and indexes, admitted only once
    columns_sql, params = connector.reads[0]
    assert "[Example_Metadata].sys.columns" in columns_sql
    assert params == ("Example_Metadata", "ops", "step_events")
    assert len(connector.writes) == 2
    for sql, values in connector.writes:
        assert sql.strip().startswith("INSERT INTO [Example_Metadata].[ops].[step_events]")
        assert "__dpone__loaded_at" in sql
        assert "SYSUTCDATETIME()" in sql  # no dependency on an external DEFAULT
        assert values[:6] == ("run-1", "load-1", "step-1", "load", "governance", "failed")
        assert values[-2:] == ("quoted ' failure", '{"rows":3}')


@pytest.mark.parametrize(
    ("column", "field", "value"),
    [
        ("run_id", "max_length", 64),
        ("details_json", "type_name", "varchar"),
        ("finished_at", "is_nullable", False),
        ("load_id", "is_identity", True),
        ("__dpone__loaded_at", "scale", 3),
    ],
)
def test_external_step_audit_rejects_drift_before_any_write(column, field, value) -> None:
    connector = _CatalogConnector()
    next(row for row in connector.columns if row["column_name"] == column)[field] = value

    with pytest.raises(RuntimeError, match="mssql_external_state_contract_shape"):
        _storage(connector).record_step(_record())

    assert connector.writes == []


@pytest.mark.parametrize("missing_table", [True, False])
def test_external_step_audit_missing_table_or_column_never_creates(missing_table) -> None:
    connector = _CatalogConnector()
    connector.columns = [] if missing_table else _columns()[:-1]

    with pytest.raises(RuntimeError, match="mssql_external_state_contract_missing"):
        _storage(connector).record_step(_record())

    assert connector.writes == []


def test_runtime_step_audit_scopes_all_ddl_and_catalog_queries_to_selected_database() -> None:
    connector = _CatalogConnector()
    storage = _storage(connector, provisioning="runtime")

    storage.record_step(_record())

    rendered = "\n".join(sql for sql, _ in connector.writes)
    assert "[Example_Metadata].sys.schemas" in rendered
    assert "[Example_Metadata].sys.sp_executesql" in rendered
    assert "[Example_Metadata].sys.columns" in rendered
    assert "CREATE TABLE [Example_Metadata].[ops].[step_events]" in rendered
    assert "ALTER TABLE [Example_Metadata].[ops].[step_events]" in rendered
    assert connector.writes[-1][0].strip().startswith("INSERT INTO [Example_Metadata].[ops].[step_events]")
    assert connector.reads == []


def test_invalid_provisioning_cannot_fall_back_to_runtime_ddl() -> None:
    connector = _CatalogConnector()
    with pytest.raises(ValueError, match="provisioning"):
        _storage(connector, provisioning="externla")
    assert connector.writes == connector.reads == []


def test_external_admission_failure_is_not_cached_as_table_ready() -> None:
    connector = _CatalogConnector()
    connector.columns = []
    storage = _storage(connector)
    with pytest.raises(RuntimeError, match="mssql_external_state_contract_missing"):
        storage.create_step_table()
    connector.columns = _columns()

    storage.record_step(_record())

    assert len(connector.reads) == 3
    assert len(connector.writes) == 1
    assert connector.writes[0][0].strip().startswith("INSERT INTO")


def test_external_step_audit_rejects_unexpected_writable_column() -> None:
    connector = _CatalogConnector()
    connector.columns.append(dict(connector.columns[0], column_name="unmanaged_required"))
    with pytest.raises(RuntimeError, match="mssql_external_state_contract_unexpected"):
        _storage(connector).record_step(_record())
    assert connector.writes == []


def test_compact_location_reads_the_same_selected_database() -> None:
    connector = _CatalogConnector()
    storage = MSSQLLoadStepAuditStorage(
        connector, schema="Example_Metadata.ops", table="step_events", provisioning="external"
    )
    storage.record_step(_record())
    assert connector.reads[0][1] == ("Example_Metadata", "ops", "step_events")
    assert "INSERT INTO [Example_Metadata].[ops].[step_events]" in connector.writes[0][0]


def test_contradictory_database_and_compact_schema_fail_without_io() -> None:
    connector = _CatalogConnector()
    with pytest.raises(ValueError):
        MSSQLLoadStepAuditStorage(connector, database="Wrong_Metadata", schema="Example_Metadata.ops")
    assert connector.writes == connector.reads == []


def test_route_step_wrapper_retains_external_location_and_real_json_insert() -> None:
    connector = _CatalogConnector()
    storage = RouteStepStorage(
        connector, database="Example_Metadata", schema="ops", table="step_events", provisioning="external"
    )
    record = _record()
    record.details_json = record.details
    del record.details

    storage.record_load_step(record)

    assert len(connector.reads) == 2
    assert len(connector.writes) == 1
    sql, params = connector.writes[0]
    assert sql.strip().startswith("INSERT INTO [Example_Metadata].[ops].[step_events]")
    assert params[-1] == '{"rows":3}'


def _storage(connector, *, provisioning="external"):
    return MSSQLLoadStepAuditStorage(
        connector, database="Example_Metadata", schema="ops", table="step_events", provisioning=provisioning
    )


def _record():
    return SimpleNamespace(
        run_id="run-1",
        load_id="load-1",
        step_id="step-1",
        phase="load",
        kind="governance",
        status="failed",
        started_at=datetime(2026, 1, 1, tzinfo=UTC),
        finished_at=None,
        error_message="quoted ' failure",
        details={"rows": 3},
    )


def _columns():
    # Hand-specified SQL Server catalog values; not generated from product contracts.
    shapes = [
        ("run_id", "nvarchar", 128, 0, False),
        ("load_id", "nvarchar", 128, 0, False),
        ("step_id", "nvarchar", 512, 0, False),
        ("phase", "nvarchar", 128, 0, False),
        ("kind", "nvarchar", 256, 0, False),
        ("status", "nvarchar", 64, 0, False),
        ("started_at", "datetime2", 8, 7, False),
        ("finished_at", "datetime2", 8, 7, True),
        ("error_message", "nvarchar", -1, 0, True),
        ("details_json", "nvarchar", -1, 0, False),
        ("__dpone__loaded_at", "datetime2", 8, 7, False),
    ]
    return [
        dict(
            column_name=name,
            type_name=kind,
            max_length=length,
            precision=0,
            scale=scale,
            is_nullable=nullable,
            is_identity=False,
            is_computed=False,
            is_sparse=False,
            is_rowguidcol=False,
            generated_always_type=0,
            is_hidden=False,
            is_masked=False,
            is_ansi_padded=kind == "nvarchar",
            is_filestream=False,
            is_column_set=False,
            is_encrypted=False,
            uses_database_default_collation=True,
            is_user_defined=False,
            is_assembly_type=False,
            has_bound_rule=False,
            has_bound_default=False,
        )
        for name, kind, length, scale, nullable in shapes
    ]


class _CatalogConnector:
    def __init__(self):
        self.columns = _columns()
        self.reads = []
        self.writes = []

    @staticmethod
    def quote_identifier(value):
        return "[" + value.replace("]", "]]") + "]"

    def get_records(self, sql, params, *, as_dict):
        assert as_dict
        self.reads.append((sql, params))
        if "FROM [Example_Metadata].sys.columns AS c" in sql:
            return self.columns
        assert "FROM [Example_Metadata].sys.indexes AS i" in sql
        assert params == ("ops", "step_events")
        return []

    def execute_query(self, sql, params=None):
        self.writes.append((sql, params))
