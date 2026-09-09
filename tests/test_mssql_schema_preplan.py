from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_physical_design import MssqlPhysicalDesignContract
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlTransactionAdmission,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
)
from dpone.contracts.mssql_type_contract import MssqlCatalogColumn
from dpone.readiness.physical_state import PhysicalColumnState, PhysicalTableState
from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.etl.mssql_fresh_target_preplan import (
    plan_fresh_mssql_target,
    resolve_mssql_target_columns,
)
from dpone.runtime.etl.mssql_schema_preplan import MSSQL_SCHEMA_PREPLAN_OPTION, MssqlSchemaPreplanner
from dpone.runtime.etl.physical_design_lifecycle import RuntimePhysicalDesignService
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.schema_evolution import SchemaEvolutionService
from dpone.runtime.schema_evolution_options import SchemaEvolutionError
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.mssql_physical_introspection import (
    MssqlPhysicalIntrospector,
    expected_mssql_created_physical_state,
)
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import (
    assert_target_catalog_expectations,
    exact_schema_catalog_transition,
)
from dpone.runtime.sinks.mssql_target_catalog_model import (
    MssqlForeignKeyState,
    MssqlIndexState,
    MssqlSchemaCatalogSnapshot,
    MssqlTableBehaviorState,
    MssqlTriggerState,
    catalog_column_from_definition,
)
from dpone.runtime.sinks.mssql_target_catalog_reader import read_schema_catalog_snapshot
from dpone.runtime.sinks.mssql_target_mutation_plan import MssqlTargetMutationPlan
from dpone.runtime.sinks.strategies.mssql.mssql_native_lineage import (
    resolve_mssql_native_lineage_columns,
)
from dpone.runtime.sinks.strategies.mssql.mssql_strategy_metadata import (
    resolve_mssql_strategy_metadata_columns,
)


class _CatalogSource:
    def __init__(self, schema: tuple[tuple[str, str], ...]) -> None:
        self.schema = schema
        self.catalog_calls = 0
        self.row_calls = 0

    def fetch_schema_projection(self, _load_config):
        self.catalog_calls += 1
        return SimpleNamespace(projected_schema=self.schema, target_projection=None)

    def extract(self, *_args):
        self.row_calls += 1
        raise AssertionError("row extraction must not run during catalog preplan")


class _CatalogSink:
    def __init__(
        self,
        columns: list[MssqlCatalogColumn],
        *,
        exists: bool = True,
        physical: PhysicalTableState | None = None,
        indexes: tuple[MssqlIndexState, ...] = (),
    ) -> None:
        self.columns = columns
        self.exists = exists
        self.physical = physical
        self.indexes = indexes
        self.catalog_calls = 0

    def get_target_columns(self, _load_config):
        self.catalog_calls += 1
        return list(self.columns)

    def get_target_catalog_snapshot(self, _load_config):
        self.catalog_calls += 1
        if not self.exists:
            return MssqlSchemaCatalogSnapshot(False, "Latin1_General_100_BIN2")
        states = tuple(
            catalog_column_from_definition(
                ColumnDef(column.name, column.dtype, column.nullable, column.collation),
                ordinal=index,
                database_collation="Latin1_General_100_BIN2",
            )
            for index, column in enumerate(self.columns, start=1)
        )
        return MssqlSchemaCatalogSnapshot(
            True,
            "Latin1_General_100_BIN2",
            states,
            indexes=self.indexes,
        )

    def target_table_exists(self, _load_config):
        return self.exists

    def inspect_physical_design(self, _load_config):
        if self.physical is None:
            raise AssertionError("physical introspection was not expected")
        return self.physical

    @staticmethod
    def target_dialect() -> str:
        return "mssql"


class _VendorCatalogConnector:
    """Minimal ``sys`` catalog exposing ODBC-shaped identity variants."""

    def __init__(
        self,
        *,
        identity: bool = False,
        identity_seed: object = "",
        identity_increment: object = "",
        product_major_version: int = 16,
        engine_edition: int = 3,
    ) -> None:
        self.identity = identity
        self.identity_seed = identity_seed
        self.identity_increment = identity_increment
        self.product_major_version = product_major_version
        self.engine_edition = engine_edition
        self.column_query = ""
        self.filegroup_query = ""
        self.behavior_query = ""

    def get_records(self, query, _params=None, *, as_dict=False):
        assert as_dict
        rendered = str(query)
        if "AS schema_exists" in rendered:
            return [{"schema_exists": 1}]
        if "DATABASEPROPERTYEX" in rendered:
            return [{"database_collation": "Latin1_General_100_BIN2"}]
        if "AS row_filegroup" in rendered:
            self.filegroup_query = rendered
            return [
                {"row_filegroup": "PRIMARY", "is_default": True},
                {"row_filegroup": "DATA", "is_default": False},
            ]
        if "ProductMajorVersion" in rendered:
            return [
                {
                    "product_major_version": self.product_major_version,
                    "engine_edition": self.engine_edition,
                }
            ]
        if "AS identity_seed" in rendered:
            self.column_query = rendered
            return [
                {
                    "ordinal": 1,
                    "name": "id",
                    "system_type_schema": "sys",
                    "system_type_name": "int",
                    "user_type_schema": "sys",
                    "user_type_name": "int",
                    "max_length": 4,
                    "precision": 10,
                    "scale": 0,
                    "is_nullable": False,
                    "collation_name": None,
                    "is_identity": self.identity,
                    "is_computed": False,
                    "is_sparse": False,
                    "is_rowguidcol": False,
                    "generated_always_type": 0,
                    "default_definition": None,
                    "computed_definition": None,
                    "identity_seed": self.identity_seed,
                    "identity_increment": self.identity_increment,
                }
            ]
        if "tab.durability_desc" in rendered:
            self.behavior_query = rendered
            return [
                {
                    "temporal_type": 0,
                    "history_schema": None,
                    "history_table": None,
                    "ledger_type": 0,
                    "is_memory_optimized": False,
                    "durability_desc": "SCHEMA_AND_DATA",
                    "is_filetable": False,
                    "is_node": False,
                    "is_edge": False,
                }
            ]
        return []


class _VendorPhysicalConnector:
    """Minimal SQL Server physical catalog with expanded float precision."""

    @staticmethod
    def table_exists(_schema, _table, *, database=None):
        return database == "DWH"

    @staticmethod
    def get_records(query):
        rendered = str(query)
        if "data_compression_desc" in rendered:
            return [("NONE",)]
        if "i.type = 5" in rendered:
            return []
        if "c.name, t.name AS type_name" in rendered:
            return [("reading", "float", 8, 53, 0, True, 1)]
        if "i.is_primary_key = 1" in rendered:
            return []
        if "ds.name, lob.name" in rendered:
            return [("PRIMARY", None)]
        raise AssertionError(f"unexpected physical catalog query: {rendered}")


def test_plan_only_add_column_blocks_before_source_rows() -> None:
    source = _CatalogSource((("id", "bigint"), ("note", "nvarchar(50)")))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])
    config = _config({"schema_evolution": {"ddl_mode": "plan_only", "apply_safe": True}})

    with pytest.raises(SchemaEvolutionError, match="DPONE_SCHEMA_EVOLUTION_BLOCKED"):
        MssqlSchemaPreplanner().plan(
            config,
            source=source,
            sink=sink,
            admission=_admission(),
        )

    assert source.catalog_calls == 1
    assert source.row_calls == 0
    assert sink.catalog_calls == 1


def test_disabled_schema_evolution_rejects_drift_before_source_rows() -> None:
    source = _CatalogSource((("id", "bigint"), ("note", "nvarchar(50)")))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])

    with pytest.raises(SchemaEvolutionError, match="schema_evolution.disabled:add_column"):
        MssqlSchemaPreplanner().plan(
            _config({"schema_evolution": {"enabled": False}}),
            source=source,
            sink=sink,
            admission=_admission(),
        )

    assert source.row_calls == 0


def test_disabled_schema_evolution_still_binds_exact_noop_catalog() -> None:
    source = _CatalogSource((("id", "bigint"),))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])

    frozen = MssqlSchemaPreplanner().plan(
        _config({"schema_evolution": {"enabled": False}}),
        source=source,
        sink=sink,
        admission=_admission(),
    )

    assert frozen.target_mutation_plan.actions == ()
    assert [item.kind for item in frozen.target_mutation_plan.expectations] == ["schema_columns"]
    expectation = frozen.target_mutation_plan.expectations[0]
    assert expectation.before_sha256 == expectation.after_sha256


def test_explicitly_accepts_existing_nullable_target_without_schema_mutation() -> None:
    """A legacy nullable target is a safe superset of a required source field."""

    source = _CatalogSource((("id", "bigint", False),))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])

    frozen = MssqlSchemaPreplanner().plan(
        _config({"schema_evolution": {"target_nullability": "accept_existing_nullable"}}),
        source=source,
        sink=sink,
        admission=_admission(),
    )

    assert frozen.target_mutation_plan.actions == ()
    assert [item.kind for item in frozen.target_mutation_plan.expectations] == ["schema_columns"]
    assert source.row_calls == 0


def test_clickhouse_bounded_text_key_override_is_applied_before_existing_target_preflight() -> None:
    """The catalog plan must use the same bounded key type as native staging."""

    source = _CatalogSource((("sessionId", "String", False),))
    sink = _CatalogSink(
        [
            MssqlCatalogColumn(
                "sessionId",
                "nvarchar(128)",
                False,
                collation="Latin1_General_100_BIN2",
            )
        ],
        indexes=(
            MssqlIndexState(
                name="ux_dpone_events_sessionId",
                type_desc="NONCLUSTERED",
                unique=True,
                primary_key=False,
                unique_constraint=False,
                disabled=False,
                hypothetical=False,
                ignore_dup_key=False,
                filter_definition=None,
                key_columns=("sessionId",),
                included_columns=(),
                descending_keys=(False,),
                data_space_type_desc="ROWS_FILEGROUP",
                partition_compression=("NONE",),
            ),
        ),
        physical=PhysicalTableState(
            sink_type="mssql",
            table="[DWH].[dbo].[events]",
            columns={
                "sessionId": PhysicalColumnState(
                    "sessionId",
                    "nvarchar(128)",
                    nullable=False,
                    position=1,
                )
            },
            table_settings={
                "compression": "NONE",
                "clustered_columnstore": False,
                "filegroup": "PRIMARY",
            },
        ),
    )
    config = replace(
        _config(
            {
                "physical_design": {
                    "columns": {
                        "sessionId": {"target_type": {"mssql": "nvarchar(128)"}},
                    }
                }
            }
        ),
        load_strategy=LoadStrategy.PARTITION_REPLACE,
        unique_key=["sessionId"],
        partition={"column": "sessionId", "values_from_staging": True},
    )

    frozen = MssqlSchemaPreplanner().plan(
        config,
        source=source,
        sink=sink,
        admission=_admission(),
    )

    assert frozen.target_column_types == (("sessionId", "nvarchar(128)"),)
    assert source.catalog_calls == 1
    assert source.row_calls == 0


def test_clickhouse_lossy_physical_override_is_rejected_before_existing_target_preflight() -> None:
    source = _CatalogSource((("counter", "UInt64", False),))
    sink = _CatalogSink([MssqlCatalogColumn("counter", "int", False)])
    config = _config(
        {
            "physical_design": {
                "columns": {
                    "counter": {"target_type": {"mssql": "int"}},
                }
            }
        }
    )

    with pytest.raises(SnapshotReconciliationError, match="lossy_target_type_forbidden:counter"):
        MssqlSchemaPreplanner().plan(
            config,
            source=source,
            sink=sink,
            admission=_admission(),
        )

    assert source.catalog_calls == 1
    assert source.row_calls == 0


def test_disabled_schema_evolution_accepts_fixed_framework_char_storage() -> None:
    """External targets may use exact CHAR widths for invariant framework values."""

    source = _CatalogSource((("id", "uuid"),))
    sink = _fixed_framework_target()
    config = _fixed_framework_config()

    frozen = MssqlSchemaPreplanner().plan(
        config,
        source=source,
        sink=sink,
        admission=_admission(),
    )

    assert frozen.target_mutation_plan.actions == ()
    assert dict(frozen.target_column_types) == {
        "id": "uniqueidentifier",
        "__dpone__run_id": "char(26)",
        "__dpone__load_id": "char(26)",
        "__dpone__row_hash": "char(64)",
        "__dpone__deleted_at": "datetime2(7)",
        "__dpone__loaded_at": "datetime2(7)",
        "__dpone__extracted_at": "datetime2(7)",
    }
    bound_config = replace(config, options={**config.options, MSSQL_SCHEMA_PREPLAN_OPTION: frozen})
    assert (
        dict((name, dtype) for name, dtype, _nullable in resolve_mssql_strategy_metadata_columns(bound_config))[
            "__dpone__row_hash"
        ]
        == "char(64)"
    )
    lineage_types = dict(
        (name, dtype)
        for name, dtype, _nullable, _collation, _role in resolve_mssql_native_lineage_columns(bound_config)
    )
    assert lineage_types["__dpone__run_id"] == "char(26)"
    assert lineage_types["__dpone__load_id"] == "char(26)"
    assert source.row_calls == 0


def test_xmin_initial_preflight_accepts_shared_snapshot_target_before_source_rows() -> None:
    """The initial DML wrapper cannot erase the incremental target contract."""

    source = _CatalogSource((("id", "uuid"),))
    config = replace(
        _config(
            {
                "incremental_strategy": "xmin",
                "xmin_execution": {"mode": "initial", "handoff_id": "events_v1"},
                "backfill": {
                    "inner_mode": "incremental_merge",
                    "parallel_workers": 4,
                    "state": {"backend": "audit_schema", "require_distributed_lock": True},
                    "chunk": {"column": "id", "kind": "uuid", "buckets": 512},
                },
                "lineage": {"enabled": True, "preset": "bulk_standard"},
                "schema_evolution": {"enabled": False},
            }
        ),
        load_strategy=LoadStrategy.BACKFILL,
        unique_key=["id"],
    )

    frozen = MssqlSchemaPreplanner().plan(
        config,
        source=source,
        sink=_fixed_framework_target(),
        admission=_admission(),
    )

    assert dict(frozen.target_column_types) == {
        "id": "uniqueidentifier",
        "__dpone__run_id": "char(26)",
        "__dpone__load_id": "char(26)",
        "__dpone__row_hash": "char(64)",
        "__dpone__deleted_at": "datetime2(7)",
        "__dpone__loaded_at": "datetime2(7)",
        "__dpone__extracted_at": "datetime2(7)",
    }
    assert source.catalog_calls == 1
    assert source.row_calls == 0


def test_existing_target_rejects_descending_unique_key_before_source_rows() -> None:
    source = _CatalogSource((("id", "uuid"),))

    with pytest.raises(SnapshotReconciliationError, match="target_unique_authority_missing"):
        MssqlSchemaPreplanner().plan(
            _fixed_framework_config(),
            source=source,
            sink=_fixed_framework_target(descending=True),
            admission=_admission(),
        )

    assert source.row_calls == 0


def test_external_physical_contract_is_verified_and_fingerprinted_before_source_rows() -> None:
    source = _CatalogSource((("id", "bigint"),))
    physical = PhysicalTableState(
        sink_type="mssql",
        table="[DWH].[dbo].[events]",
        columns={"id": PhysicalColumnState("id", "bigint", nullable=True, position=1)},
        table_settings={
            "compression": "NONE",
            "clustered_columnstore": True,
            "filegroup": "PRIMARY",
        },
    )
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)], physical=physical)
    config = _config(
        {
            "physical_design": {
                "apply_runtime": False,
                "storage": {"mssql": {"compression": "none", "clustered_columnstore": True}},
            }
        }
    )

    frozen = MssqlSchemaPreplanner().plan(config, source=source, sink=sink, admission=_admission())

    assert [expectation.kind for expectation in frozen.target_mutation_plan.expectations] == [
        "schema_columns",
        "physical_design",
    ]
    assert frozen.physical_report == {
        "applied": False,
        "external_provisioning": True,
        "table": "[DWH].[dbo].[events]",
        "blockers": [],
        "executed": [],
        "preplanned": True,
    }
    assert source.row_calls == 0


def test_external_physical_drift_fails_before_source_rows() -> None:
    source = _CatalogSource((("id", "bigint"),))
    physical = PhysicalTableState(
        sink_type="mssql",
        table="[DWH].[dbo].[events]",
        columns={"id": PhysicalColumnState("id", "bigint", nullable=True, position=1)},
        table_settings={
            "compression": "NONE",
            "clustered_columnstore": False,
            "filegroup": "PRIMARY",
        },
    )
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)], physical=physical)
    config = _config(
        {
            "physical_design": {
                "apply_runtime": False,
                "storage": {"mssql": {"compression": "none", "clustered_columnstore": True}},
            }
        }
    )

    with pytest.raises(RuntimeError, match="external_contract_drift:table_settings.clustered_columnstore"):
        MssqlSchemaPreplanner().plan(config, source=source, sink=sink, admission=_admission())

    assert source.row_calls == 0


def test_disabled_schema_evolution_still_rejects_business_char_varchar_drift() -> None:
    source = _CatalogSource((("business_code", "varchar(26)"),))
    sink = _CatalogSink([MssqlCatalogColumn("business_code", "char(26)", True)])

    with pytest.raises(SchemaEvolutionError, match="schema_evolution.disabled:type_change"):
        MssqlSchemaPreplanner().plan(
            _config({"schema_evolution": {"enabled": False}}),
            source=source,
            sink=sink,
            admission=_admission(),
        )

    assert source.row_calls == 0


def test_disabled_schema_evolution_revalidates_frozen_source_projection_after_extract() -> None:
    source = _CatalogSource((("id", "bigint"),))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])
    config = _config({"schema_evolution": {"enabled": False}})
    frozen = MssqlSchemaPreplanner().plan(config, source=source, sink=sink, admission=_admission())
    config = replace(config, options={**config.options, MSSQL_SCHEMA_PREPLAN_OPTION: frozen})

    unchanged = SchemaEvolutionService().prepare_payload(
        config,
        sink,
        LoadPayload(artifact=object(), schema=[("id", "bigint")]),
    )

    assert unchanged.mssql_target_mutation_plan is frozen.target_mutation_plan
    with pytest.raises(SchemaEvolutionError, match="source_catalog_changed_after_preflight"):
        SchemaEvolutionService().prepare_payload(
            config,
            sink,
            LoadPayload(artifact=object(), schema=[("id", "int")]),
        )


def test_apply_add_column_freezes_catalog_expectation_and_ordered_ddl() -> None:
    source = _CatalogSource((("id", "bigint"), ("note", "nvarchar(50)")))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])

    frozen = MssqlSchemaPreplanner().plan(
        _config({"schema_evolution": {"ddl_mode": "online", "apply_safe": True}}),
        source=source,
        sink=sink,
        admission=_admission(),
    )

    plan = frozen.target_mutation_plan
    assert [action.kind for action in plan.actions] == ["schema_evolution"]
    assert "ADD [note] nvarchar(50) NULL" in plan.actions[0].sql
    assert [item.kind for item in plan.expectations] == ["schema_columns"]
    assert source.row_calls == 0


def test_missing_target_create_is_fully_planned_with_lineage_before_source() -> None:
    admission = _admission()
    before = MssqlSchemaCatalogSnapshot(False, "Latin1_General_100_BIN2")

    fresh = plan_fresh_mssql_target(
        _config({"lineage": True}),
        source_columns=[ColumnDef("id", "bigint")],
        before=before,
        admission=admission,
        mutation_plan=MssqlTargetMutationPlan.from_admission(admission),
        qualified_target="[DWH].[dbo].[events]",
    )

    assert [action.kind for action in fresh.mutation_plan.actions] == ["schema_evolution"]
    assert fresh.mutation_plan.actions[0].sql.startswith("CREATE TABLE [DWH].[dbo].[events]")
    assert {column.name for column in fresh.schema_after.columns} >= {
        "id",
        "__dpone__load_id",
        "__dpone__loaded_at",
        "__dpone__row_id",
        "__dpone__extracted_at",
    }
    assert [item.kind for item in fresh.mutation_plan.expectations] == [
        "schema_columns",
        "physical_design",
    ]
    assert fresh.physical_after.table_settings["compression"] == "NONE"


def test_missing_target_preplan_keeps_transfer_partitioning_outside_physical_design() -> None:
    admission = _admission()
    fresh = plan_fresh_mssql_target(
        _config(
            {
                "partitioning": {
                    "strategy": "range",
                    "column": "id",
                    "bounds": {"lower": 1, "upper": 100},
                    "num_partitions": 4,
                }
            }
        ),
        source_columns=[ColumnDef("id", "bigint")],
        before=MssqlSchemaCatalogSnapshot(False, "Latin1_General_100_BIN2"),
        admission=admission,
        mutation_plan=MssqlTargetMutationPlan.from_admission(admission),
        qualified_target="[DWH].[dbo].[events]",
    )

    assert fresh.mutation_plan.actions[0].sql.startswith("CREATE TABLE [DWH].[dbo].[events]")
    assert fresh.physical_after.table_settings["compression"] == "NONE"


def test_missing_target_after_image_matches_sql_server_disk_table_durability() -> None:
    """Fresh CREATE TABLE must match the exact ``sys.tables`` after-image."""

    config = _config({"lineage": False})
    admission = _admission()
    before = MssqlSchemaCatalogSnapshot(False, "Latin1_General_100_BIN2")
    fresh = plan_fresh_mssql_target(
        config,
        source_columns=[ColumnDef("id", "bigint")],
        before=before,
        admission=admission,
        mutation_plan=MssqlTargetMutationPlan.from_admission(admission),
        qualified_target="[DWH].[dbo].[events]",
    )
    vendor_after = replace(
        fresh.schema_after,
        behavior=MssqlTableBehaviorState(durability_desc="SCHEMA_AND_DATA"),
    )
    catalog_owner = SimpleNamespace(
        get_target_catalog_snapshot=lambda _load_config: vendor_after,
    )

    assert fresh.schema_after.behavior == MssqlTableBehaviorState.ordinary_disk_table()
    assert_target_catalog_expectations(
        catalog_owner,
        config,
        fresh.mutation_plan.expectations,
        boundary="after",
        kinds=frozenset({"schema_columns"}),
    )


def test_missing_partition_target_uses_same_bin2_equality_collation_as_native_staging() -> None:
    """A textual partition identity must not inherit the database collation."""

    config = _config({"lineage": False})
    config.load_strategy = LoadStrategy.PARTITION_REPLACE
    config.unique_key = ["id"]
    config.partition = {"column": "partition_code", "values_from_staging": True}
    admission = _admission()

    fresh = plan_fresh_mssql_target(
        config,
        source_columns=[
            ColumnDef("id", "bigint", nullable=False),
            ColumnDef(
                "partition_code",
                "nvarchar(64)",
                nullable=False,
                collation="SQL_Latin1_General_CP1_CI_AS",
            ),
        ],
        before=MssqlSchemaCatalogSnapshot(False, "SQL_Latin1_General_CP1_CI_AS"),
        admission=admission,
        mutation_plan=MssqlTargetMutationPlan.from_admission(admission),
        qualified_target="[DWH].[dbo].[events]",
    )

    planned = {column.name: column for column in fresh.columns}
    catalog = {column.name: column for column in fresh.schema_after.columns}
    assert planned["partition_code"].collation == "Latin1_General_100_BIN2"
    assert catalog["partition_code"].collation == "Latin1_General_100_BIN2"
    assert "[partition_code] nvarchar(64) COLLATE Latin1_General_100_BIN2 NOT NULL" in "\n".join(
        action.sql for action in fresh.mutation_plan.actions
    )


def test_target_projection_rejects_missing_partition_equality_key() -> None:
    config = _config({"lineage": False})
    config.load_strategy = LoadStrategy.PARTITION_REPLACE
    config.partition = {"column": "partition_code", "values_from_staging": True}

    with pytest.raises(
        SnapshotReconciliationError,
        match="mssql_native_projection.equality_key_missing_from_schema",
    ):
        resolve_mssql_target_columns(config, [ColumnDef("id", "bigint", nullable=False)])


def test_fresh_float_physical_after_image_matches_vendor_catalog_precision() -> None:
    """A bare SQL Server ``float`` is catalogued as the exact ``float(53)``."""

    columns = (ColumnDef("reading", "float"),)
    expected = expected_mssql_created_physical_state(
        table="[DWH].[dbo].[events]",
        columns=columns,
        contract=MssqlPhysicalDesignContract(active=False),
        default_filegroup="PRIMARY",
    )
    actual = MssqlPhysicalIntrospector(_VendorPhysicalConnector()).inspect(_config({}))

    assert actual.columns["reading"].target_type == "float(53)"
    assert expected == actual


def test_clickhouse_uint8_is_projected_once_to_tinyint_in_fresh_target_ddl() -> None:
    """A mapped physical type must not be reinterpreted as a MySQL source type."""

    admission = _admission()
    fresh = plan_fresh_mssql_target(
        _config({"lineage": False}),
        source_columns=[ColumnDef("counter", "UInt8", nullable=False)],
        before=MssqlSchemaCatalogSnapshot(False, "Latin1_General_100_BIN2"),
        admission=admission,
        mutation_plan=MssqlTargetMutationPlan.from_admission(admission),
        qualified_target="[DWH].[dbo].[events]",
    )

    assert fresh.columns == (ColumnDef("counter", "tinyint", nullable=False),)
    assert "[counter] tinyint NOT NULL" in fresh.mutation_plan.actions[0].sql
    assert fresh.schema_after.columns[0].user_type_name == "tinyint"


def test_missing_scd2_target_create_binds_filtered_unique_and_strategy_columns() -> None:
    admission = _admission()
    config = _config({"lineage": True})
    config.load_strategy = LoadStrategy.SCD2
    config.unique_key = ["id"]

    fresh = plan_fresh_mssql_target(
        config,
        source_columns=[ColumnDef("id", "bigint", nullable=False)],
        before=MssqlSchemaCatalogSnapshot(False, "Latin1_General_100_BIN2"),
        admission=admission,
        mutation_plan=MssqlTargetMutationPlan.from_admission(admission),
        qualified_target="[DWH].[dbo].[events]",
    )

    assert len(fresh.mutation_plan.actions) == 2
    assert "CREATE UNIQUE NONCLUSTERED INDEX" in fresh.mutation_plan.actions[1].sql
    assert "WHERE [__dpone__is_current] = 1" in fresh.mutation_plan.actions[1].sql
    by_name = {column.name: column for column in fresh.schema_after.columns}
    assert by_name["__dpone__row_hash"].nullable is False
    assert by_name["__dpone__is_current"].nullable is False
    assert fresh.schema_after.indexes[0].filter_definition == "[__dpone__is_current] = 1"


def test_missing_target_primary_key_is_the_only_unique_authority() -> None:
    admission = _admission()
    config = _config(
        {
            "physical_design": {
                "indexes": {"primary_key": ["id"]},
                "storage": {
                    "mssql": {
                        "compression": "PAGE",
                        "index_fillfactor": 90,
                        "filegroup": "PRIMARY",
                    }
                },
            }
        }
    )
    config.unique_key = ["id"]

    fresh = plan_fresh_mssql_target(
        config,
        source_columns=[ColumnDef("id", "bigint", nullable=False)],
        before=MssqlSchemaCatalogSnapshot(False, "Latin1_General_100_BIN2"),
        admission=admission,
        mutation_plan=MssqlTargetMutationPlan.from_admission(admission),
        qualified_target="[DWH].[dbo].[events]",
    )

    assert len(fresh.schema_after.indexes) == 1
    primary = fresh.schema_after.indexes[0]
    assert primary.primary_key is True
    assert primary.unique is True
    assert primary.fill_factor == 90
    assert primary.data_space_name == "PRIMARY"
    assert primary.partition_compression == ("PAGE",)
    assert sum("CREATE UNIQUE" in action.sql for action in fresh.mutation_plan.actions) == 0
    assert fresh.physical_after.primary_key == ("id",)
    assert fresh.physical_after.table_settings["compression"] == "PAGE"


def test_missing_target_resolves_filegroup_placement_from_exact_catalog_authority() -> None:
    admission = _admission()
    config = _config(
        {
            "physical_design": {
                "storage": {
                    "mssql": {
                        "filegroup": "DATA",
                        "textimage_filegroup": "LOB",
                    }
                },
            }
        }
    )
    before = MssqlSchemaCatalogSnapshot(
        False,
        "Latin1_General_100_BIN2",
        available_row_filegroups=("PRIMARY", "DATA", "LOB"),
    )

    fresh = plan_fresh_mssql_target(
        config,
        source_columns=[ColumnDef("payload", "nvarchar(max)")],
        before=before,
        admission=admission,
        mutation_plan=MssqlTargetMutationPlan.from_admission(admission),
        qualified_target="[DWH].[dbo].[events]",
    )

    assert "ON [DATA] TEXTIMAGE_ON [LOB]" in fresh.mutation_plan.actions[0].sql
    assert fresh.physical_after.table_settings["filegroup"] == "DATA"
    assert fresh.physical_after.table_settings["textimage_filegroup"] == "LOB"
    assert fresh.schema_after.available_row_filegroups == ("DATA", "LOB", "PRIMARY")


def test_filegroup_authority_preserves_case_distinct_catalog_names() -> None:
    before = MssqlSchemaCatalogSnapshot(
        False,
        "Latin1_General_100_CS_AS",
        default_filegroup="Data",
        available_row_filegroups=("Data", "data"),
    )
    config = _config(
        {
            "physical_design": {
                "storage": {"mssql": {"filegroup": "data"}},
            }
        }
    )

    fresh = plan_fresh_mssql_target(
        config,
        source_columns=[ColumnDef("id", "bigint")],
        before=before,
        admission=_admission(),
        mutation_plan=MssqlTargetMutationPlan.from_admission(_admission()),
        qualified_target="[DWH].[dbo].[events]",
    )

    assert " ON [data]" in fresh.mutation_plan.actions[0].sql
    assert fresh.physical_after.table_settings["filegroup"] == "data"


def test_filegroup_authority_does_not_emulate_database_collation_with_casefold() -> None:
    config = _config(
        {
            "physical_design": {
                "storage": {"mssql": {"filegroup": "DATA"}},
            }
        }
    )

    with pytest.raises(RuntimeError, match="target_row_filegroup_unavailable:filegroup:DATA"):
        plan_fresh_mssql_target(
            config,
            source_columns=[ColumnDef("id", "bigint")],
            before=MssqlSchemaCatalogSnapshot(
                False,
                "Latin1_General_100_CS_AS",
                default_filegroup="Data",
                available_row_filegroups=("Data", "data"),
            ),
            admission=_admission(),
            mutation_plan=MssqlTargetMutationPlan.from_admission(_admission()),
            qualified_target="[DWH].[dbo].[events]",
        )


@pytest.mark.parametrize("field", ["filegroup", "textimage_filegroup"])
def test_missing_target_rejects_unavailable_filegroup_before_create_plan(field: str) -> None:
    storage = {"filegroup": "PRIMARY", "textimage_filegroup": "PRIMARY"}
    storage[field] = "MISSING"
    config = _config({"physical_design": {"storage": {"mssql": storage}}})

    with pytest.raises(
        RuntimeError,
        match=rf"target_row_filegroup_unavailable:{field}:MISSING",
    ):
        plan_fresh_mssql_target(
            config,
            source_columns=[ColumnDef("payload", "nvarchar(max)")],
            before=MssqlSchemaCatalogSnapshot(False, "Latin1_General_100_BIN2"),
            admission=_admission(),
            mutation_plan=MssqlTargetMutationPlan.from_admission(_admission()),
            qualified_target="[DWH].[dbo].[events]",
        )


def test_missing_target_clustered_columnstore_has_exact_catalog_after_image() -> None:
    admission = _admission()
    config = _config(
        {
            "physical_design": {
                "storage": {
                    "mssql": {
                        "clustered_columnstore": True,
                        "filegroup": "PRIMARY",
                    }
                },
            }
        }
    )

    fresh = plan_fresh_mssql_target(
        config,
        source_columns=[ColumnDef("id", "bigint", nullable=False)],
        before=MssqlSchemaCatalogSnapshot(False, "Latin1_General_100_BIN2"),
        admission=admission,
        mutation_plan=MssqlTargetMutationPlan.from_admission(admission),
        qualified_target="[DWH].[dbo].[events]",
    )

    assert len(fresh.schema_after.indexes) == 1
    columnstore = fresh.schema_after.indexes[0]
    assert columnstore.type_desc == "CLUSTERED COLUMNSTORE"
    assert columnstore.data_space_name == "PRIMARY"
    assert columnstore.partition_compression == ("COLUMNSTORE",)
    assert fresh.physical_after.table_settings["clustered_columnstore"] is True


def test_missing_target_physical_plan_only_blocks_before_create_plan() -> None:
    admission = _admission()
    config = _config(
        {
            "physical_design": {
                "mode": "explicit",
                "apply": "plan_only",
                "storage": {"mssql": {"compression": "PAGE"}},
            }
        }
    )

    with pytest.raises(RuntimeError, match="physical_design.plan_only:missing_target"):
        plan_fresh_mssql_target(
            config,
            source_columns=[ColumnDef("id", "bigint")],
            before=MssqlSchemaCatalogSnapshot(False, "Latin1_General_100_BIN2"),
            admission=admission,
            mutation_plan=MssqlTargetMutationPlan.from_admission(admission),
            qualified_target="[DWH].[dbo].[events]",
        )


def test_existing_physical_drift_blocks_before_source_rows() -> None:
    source = _CatalogSource((("id", "bigint"),))
    sink = _CatalogSink(
        [MssqlCatalogColumn("id", "bigint", True)],
        physical=_physical_state("NONE"),
    )
    config = _config(
        {
            "physical_design": {
                "apply_runtime": True,
                "storage": {"mssql": {"compression": "PAGE"}},
            }
        }
    )

    with pytest.raises(RuntimeError, match="physical_design.blocking:table_settings.compression"):
        MssqlSchemaPreplanner().plan(config, source=source, sink=sink, admission=_admission())

    assert source.catalog_calls == 1
    assert source.row_calls == 0


def test_existing_physical_safe_window_is_frozen_before_source_rows() -> None:
    source = _CatalogSource((("id", "bigint"),))
    sink = _CatalogSink(
        [MssqlCatalogColumn("id", "bigint", True)],
        physical=_physical_state("NONE"),
    )
    table = "[DWH].[dbo].[events]"
    config = _config(
        {
            "physical_design": {
                "apply_runtime": True,
                "apply": "safe_window",
                "storage": {"mssql": {"compression": "PAGE"}},
                "reconciliation": {
                    "mode": "safe_window",
                    "approval": {
                        "approved_by": "test",
                        "approved_risks": ["table_settings.compression"],
                        "table": table,
                    },
                },
            }
        }
    )

    frozen = MssqlSchemaPreplanner().plan(config, source=source, sink=sink, admission=_admission())

    plan = frozen.target_mutation_plan
    assert plan.actions[-1].kind == "physical_design"
    assert plan.actions[-1].sql == f"ALTER TABLE {table} REBUILD WITH (DATA_COMPRESSION = PAGE)"
    assert plan.expectations[-1].kind == "physical_design"
    assert frozen.physical_report is not None
    assert frozen.physical_report["deferred_to_target_transaction"] is True
    assert source.row_calls == 0


def test_payload_lifecycle_reuses_pre_source_physical_plan_without_reinspection() -> None:
    source = _CatalogSource((("id", "bigint"),))
    sink = _CatalogSink(
        [MssqlCatalogColumn("id", "bigint", True)],
        physical=_physical_state("NONE"),
    )
    config = _config({"physical_design": {"apply_runtime": True, "mode": "off"}})
    admission = _admission()
    frozen = MssqlSchemaPreplanner().plan(config, source=source, sink=sink, admission=admission)
    config = replace(config, options={**config.options, MSSQL_SCHEMA_PREPLAN_OPTION: frozen})
    payload = LoadPayload(
        artifact=object(),
        schema=[("id", "bigint")],
        mssql_transaction_admission=admission,
        mssql_target_mutation_plan=frozen.target_mutation_plan,
    )
    sink.physical = None

    outcome = RuntimePhysicalDesignService().prepare(config, sink, payload)

    assert outcome.payload is payload
    assert outcome.report == frozen.physical_report


def test_opaque_source_schema_capability_blocks_before_any_row_access() -> None:
    class OpaqueSource:
        row_calls = 0

        def extract(self, *_args):
            self.row_calls += 1
            raise AssertionError("row extraction must not run")

    source = OpaqueSource()
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])

    with pytest.raises(RuntimeError, match="source_schema_preflight_capability_required"):
        MssqlSchemaPreplanner().plan(_config({}), source=source, sink=sink, admission=_admission())

    assert source.row_calls == 0


def test_absent_target_schema_is_a_preprovisioning_blocker() -> None:
    class Connector:
        calls = 0

        def get_records(self, *_args, **_kwargs):
            self.calls += 1
            return []

    connector = Connector()

    with pytest.raises(RuntimeError, match="target_schema_preprovision_required"):
        read_schema_catalog_snapshot(SimpleNamespace(connector=connector), _config({}))

    assert connector.calls == 1


def test_catalog_reader_normalizes_odbc_empty_identity_variants_for_nonidentity_column() -> None:
    connector = _VendorCatalogConnector()

    snapshot = read_schema_catalog_snapshot(SimpleNamespace(connector=connector), _config({}))

    column = snapshot.columns[0]
    assert column.identity is False
    assert column.identity_seed is None
    assert column.identity_increment is None
    assert "CONVERT(nvarchar(128), ic.seed_value)" in connector.column_query
    assert "CONVERT(nvarchar(128), ic.increment_value)" in connector.column_query


def test_catalog_reader_binds_exact_writable_rows_filegroup_authority() -> None:
    connector = _VendorCatalogConnector()

    snapshot = read_schema_catalog_snapshot(SimpleNamespace(connector=connector), _config({}))

    assert snapshot.default_filegroup == "PRIMARY"
    assert snapshot.available_row_filegroups == ("DATA", "PRIMARY")
    assert "type_desc = N'ROWS_FILEGROUP'" in connector.filegroup_query
    assert "is_read_only = 0" in connector.filegroup_query
    assert "ORDER BY data_space_id" in connector.filegroup_query


def test_catalog_reader_requires_complete_identity_metadata() -> None:
    connector = _VendorCatalogConnector(
        identity=True,
        identity_seed="",
        identity_increment="1",
    )

    with pytest.raises(RuntimeError, match="target_identity_metadata_unavailable"):
        read_schema_catalog_snapshot(SimpleNamespace(connector=connector), _config({}))


def test_catalog_reader_canonicalizes_complete_identity_metadata() -> None:
    connector = _VendorCatalogConnector(
        identity=True,
        identity_seed=" 1 ",
        identity_increment=" 2 ",
    )

    snapshot = read_schema_catalog_snapshot(SimpleNamespace(connector=connector), _config({}))

    assert snapshot.columns[0].identity_seed == "1"
    assert snapshot.columns[0].identity_increment == "2"


def test_sql_server_2019_catalog_projects_impossible_ledger_as_zero() -> None:
    connector = _VendorCatalogConnector(product_major_version=15)

    snapshot = read_schema_catalog_snapshot(SimpleNamespace(connector=connector), _config({}))

    assert snapshot.behavior.ledger_type == 0
    assert "CONVERT(int, 0) AS ledger_type" in connector.behavior_query
    assert "tab.ledger_type" not in connector.behavior_query


def test_sql_server_2022_catalog_reads_real_ledger_metadata() -> None:
    connector = _VendorCatalogConnector(product_major_version=16)

    read_schema_catalog_snapshot(SimpleNamespace(connector=connector), _config({}))

    assert "tab.ledger_type AS ledger_type" in connector.behavior_query


@pytest.mark.parametrize(
    "behavior",
    [
        MssqlTableBehaviorState(temporal_type=2),
        MssqlTableBehaviorState(ledger_type=2),
        MssqlTableBehaviorState(memory_optimized=True),
        MssqlTableBehaviorState(filetable=True),
        MssqlTableBehaviorState(graph_node=True),
        MssqlTableBehaviorState(graph_edge=True),
    ],
)
def test_unsupported_target_behavior_blocks_before_source_rows(behavior: MssqlTableBehaviorState) -> None:
    source = _CatalogSource((("id", "bigint"),))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])
    sink.get_target_catalog_snapshot = lambda _config: _snapshot(behavior=behavior)  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="mssql_target_contract.unsupported_table_behavior"):
        MssqlSchemaPreplanner().plan(_config({}), source=source, sink=sink, admission=_admission())

    assert source.row_calls == 0


def test_enabled_dml_trigger_blocks_before_source_rows() -> None:
    source = _CatalogSource((("id", "bigint"),))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])
    trigger = MssqlTriggerState(
        name="tr_events_audit",
        disabled=False,
        instead_of=False,
        not_for_replication=False,
        system_shipped=False,
        events=("INSERT",),
        execute_as_principal=None,
        definition="CREATE TRIGGER ...",
    )
    sink.get_target_catalog_snapshot = lambda _config: _snapshot(triggers=(trigger,))  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="enabled_dml_trigger:tr_events_audit"):
        MssqlSchemaPreplanner().plan(_config({}), source=source, sink=sink, admission=_admission())

    assert source.row_calls == 0


@pytest.mark.parametrize(
    ("action", "blocker"),
    [
        ("CASCADE", "inbound_cascading_foreign_key"),
        ("SET_NULL", "inbound_cascading_foreign_key"),
        ("NO_ACTION", "inbound_foreign_key_destructive_strategy"),
    ],
)
def test_full_refresh_inbound_foreign_key_policy_is_catalog_first(action: str, blocker: str) -> None:
    source = _CatalogSource((("id", "bigint"),))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])
    foreign_key = MssqlForeignKeyState(
        name="fk_child_events",
        direction="inbound",
        parent_schema="dbo",
        parent_table="child",
        parent_columns=("event_id",),
        referenced_schema="dbo",
        referenced_table="events",
        referenced_columns=("id",),
        update_action="NO_ACTION",
        delete_action=action,
        disabled=False,
        not_trusted=False,
        not_for_replication=False,
    )
    sink.get_target_catalog_snapshot = lambda _config: _snapshot(  # type: ignore[method-assign]
        foreign_keys=(foreign_key,)
    )

    with pytest.raises(RuntimeError, match=rf"{blocker}:fk_child_events"):
        MssqlSchemaPreplanner().plan(_config({}), source=source, sink=sink, admission=_admission())

    assert source.row_calls == 0


def test_incremental_append_allows_enabled_no_action_inbound_foreign_key() -> None:
    source = _CatalogSource((("id", "bigint"),))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])
    foreign_key = _inbound_foreign_key(delete_action="NO_ACTION")
    sink.get_target_catalog_snapshot = lambda _config: _snapshot(  # type: ignore[method-assign]
        foreign_keys=(foreign_key,)
    )
    config = replace(_config({}), load_strategy=LoadStrategy.INCREMENTAL_APPEND)

    MssqlSchemaPreplanner().plan(config, source=source, sink=sink, admission=_admission())

    assert source.row_calls == 0


@pytest.mark.parametrize("not_trusted", [False, True])
def test_enabled_cascading_inbound_foreign_key_blocks_regardless_of_trust(not_trusted: bool) -> None:
    source = _CatalogSource((("id", "bigint"),))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])
    foreign_key = replace(
        _inbound_foreign_key(delete_action="CASCADE"),
        not_trusted=not_trusted,
    )
    sink.get_target_catalog_snapshot = lambda _config: _snapshot(  # type: ignore[method-assign]
        foreign_keys=(foreign_key,)
    )

    with pytest.raises(RuntimeError, match="inbound_cascading_foreign_key:fk_child_events"):
        MssqlSchemaPreplanner().plan(_config({}), source=source, sink=sink, admission=_admission())

    assert source.row_calls == 0


def test_disabled_inbound_foreign_key_is_catalog_bound_but_non_blocking() -> None:
    source = _CatalogSource((("id", "bigint"),))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])
    foreign_key = replace(
        _inbound_foreign_key(delete_action="CASCADE"),
        disabled=True,
    )
    sink.get_target_catalog_snapshot = lambda _config: _snapshot(  # type: ignore[method-assign]
        foreign_keys=(foreign_key,)
    )

    MssqlSchemaPreplanner().plan(_config({}), source=source, sink=sink, admission=_admission())

    assert source.row_calls == 0


def test_self_referencing_cascade_is_treated_as_inbound_behavior() -> None:
    source = _CatalogSource((("id", "bigint"),))
    sink = _CatalogSink([MssqlCatalogColumn("id", "bigint", True)])
    foreign_key = replace(
        _inbound_foreign_key(delete_action="CASCADE"),
        direction="self",
        parent_table="events",
    )
    sink.get_target_catalog_snapshot = lambda _config: _snapshot(  # type: ignore[method-assign]
        foreign_keys=(foreign_key,)
    )

    with pytest.raises(RuntimeError, match="inbound_cascading_foreign_key:fk_child_events"):
        MssqlSchemaPreplanner().plan(_config({}), source=source, sink=sink, admission=_admission())

    assert source.row_calls == 0


@pytest.mark.parametrize(
    ("field", "value", "marker"),
    [
        ("direction", "sideways", "direction_invalid"),
        ("delete_action", "RESTRICT", "action_invalid"),
        ("parent_columns", (), "columns_invalid"),
    ],
)
def test_foreign_key_catalog_model_rejects_non_finite_identity(
    field: str,
    value: object,
    marker: str,
) -> None:
    with pytest.raises(ValueError, match=marker):
        replace(_inbound_foreign_key(delete_action="NO_ACTION"), **{field: value})


@pytest.mark.parametrize(
    ("dtype", "scale", "expected_precision", "expected_length"),
    [
        (dtype, scale, precision, length)
        for family, precision_for in (
            ("time", lambda value: 8 if value == 0 else 9 + value),
            ("datetime2", lambda value: 19 if value == 0 else 20 + value),
            ("datetimeoffset", lambda value: 26 if value == 0 else 27 + value),
        )
        for scale in range(8)
        for precision in (precision_for(scale),)
        for length in (
            {"time": 3, "datetime2": 6, "datetimeoffset": 8}[family] + (0 if scale <= 2 else 1 if scale <= 4 else 2),
        )
        for dtype in (f"{family}({scale})",)
    ],
)
def test_temporal_catalog_projection_matches_sql_server_scale_metadata(
    dtype: str,
    scale: int,
    expected_precision: int,
    expected_length: int,
) -> None:
    column = catalog_column_from_definition(
        ColumnDef("observed_at", dtype),
        ordinal=1,
        database_collation="Latin1_General_100_BIN2",
    )

    assert column.scale == scale
    assert column.precision == expected_precision
    assert column.max_length == expected_length


def test_schema_catalog_partition_is_stable_across_authorized_compression_rebuild() -> None:
    column = catalog_column_from_definition(
        ColumnDef("id", "bigint", nullable=False),
        ordinal=1,
        database_collation="Latin1_General_100_BIN2",
    )
    common = {
        "name": "pk_events_id",
        "type_desc": "CLUSTERED",
        "unique": True,
        "primary_key": True,
        "unique_constraint": False,
        "disabled": False,
        "hypothetical": False,
        "ignore_dup_key": False,
        "filter_definition": None,
        "key_columns": ("id",),
        "included_columns": (),
        "data_space_name": "PRIMARY",
        "data_space_type_desc": "ROWS_FILEGROUP",
    }
    row = MssqlSchemaCatalogSnapshot(
        True,
        "Latin1_General_100_BIN2",
        columns=(column,),
        indexes=(MssqlIndexState(**common, partition_compression=("ROW",)),),
    )
    page = MssqlSchemaCatalogSnapshot(
        True,
        "Latin1_General_100_BIN2",
        columns=(column,),
        indexes=(MssqlIndexState(**common, partition_compression=("PAGE",)),),
    )

    transition = exact_schema_catalog_transition(row, row)

    assert exact_schema_catalog_transition(page, page).before_sha256 == transition.after_sha256
    assert page.to_dict() != row.to_dict()


def test_schema_catalog_digest_binds_writable_rows_filegroup_authority() -> None:
    primary_only = MssqlSchemaCatalogSnapshot(False, "Latin1_General_100_BIN2")
    with_data = MssqlSchemaCatalogSnapshot(
        False,
        "Latin1_General_100_BIN2",
        available_row_filegroups=("PRIMARY", "DATA"),
    )

    primary_transition = exact_schema_catalog_transition(primary_only, primary_only)
    assert primary_transition.representation == "exact_sys_catalog_v5"
    assert primary_transition.before_sha256 != exact_schema_catalog_transition(with_data, with_data).before_sha256


def _fixed_framework_target(*, descending: bool = False) -> _CatalogSink:
    return _CatalogSink(
        [
            MssqlCatalogColumn("id", "uniqueidentifier", True),
            MssqlCatalogColumn("__dpone__run_id", "char(26)", False),
            MssqlCatalogColumn("__dpone__load_id", "char(26)", False),
            MssqlCatalogColumn("__dpone__row_hash", "char(64)", False),
            MssqlCatalogColumn("__dpone__deleted_at", "datetime2(7)", True),
            MssqlCatalogColumn("__dpone__loaded_at", "datetime2(7)", False),
            MssqlCatalogColumn("__dpone__extracted_at", "datetime2(7)", False),
        ],
        indexes=(
            MssqlIndexState(
                name="ux_dpone_events_id",
                type_desc="NONCLUSTERED",
                unique=True,
                primary_key=False,
                unique_constraint=False,
                disabled=False,
                hypothetical=False,
                ignore_dup_key=False,
                filter_definition=None,
                key_columns=("id",),
                included_columns=(),
                descending_keys=(descending,),
                data_space_type_desc="ROWS_FILEGROUP",
                partition_compression=("NONE",),
            ),
        ),
    )


def _fixed_framework_config() -> LoadConfig:
    return replace(
        _config(
            {
                "lineage": {"enabled": True, "preset": "bulk_standard"},
                "schema_evolution": {"enabled": False},
            }
        ),
        load_strategy=LoadStrategy.SNAPSHOT_DIFF,
        unique_key=["id"],
    )


def _config(options: dict[str, object]) -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="public",
        source_table="events",
        target_database="DWH",
        target_schema="dbo",
        target_table="events",
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={"source_type": "postgres", "sink_type": "mssql", "lineage": False, **options},
    )


def _physical_state(compression: str) -> PhysicalTableState:
    return PhysicalTableState(
        sink_type="mssql",
        table="[DWH].[dbo].[events]",
        columns={
            "id": PhysicalColumnState("id", "bigint", nullable=True, position=1),
        },
        table_settings={
            "compression": compression,
            "clustered_columnstore": False,
            "filegroup": "PRIMARY",
        },
    )


def _inbound_foreign_key(*, delete_action: str) -> MssqlForeignKeyState:
    return MssqlForeignKeyState(
        name="fk_child_events",
        direction="inbound",
        parent_schema="dbo",
        parent_table="child",
        parent_columns=("event_id",),
        referenced_schema="dbo",
        referenced_table="events",
        referenced_columns=("id",),
        update_action="NO_ACTION",
        delete_action=delete_action,
        disabled=False,
        not_trusted=False,
        not_for_replication=False,
    )


def _snapshot(
    *,
    behavior: MssqlTableBehaviorState = MssqlTableBehaviorState(),
    triggers: tuple[MssqlTriggerState, ...] = (),
    foreign_keys: tuple[MssqlForeignKeyState, ...] = (),
) -> MssqlSchemaCatalogSnapshot:
    return MssqlSchemaCatalogSnapshot(
        True,
        "Latin1_General_100_BIN2",
        columns=(
            catalog_column_from_definition(
                ColumnDef("id", "bigint"),
                ordinal=1,
                database_collation="Latin1_General_100_BIN2",
            ),
        ),
        triggers=triggers,
        foreign_keys=foreign_keys,
        behavior=behavior,
    )


def _admission() -> MssqlTransactionAdmission:
    request = MssqlAttemptRequest(
        invocation=InvocationIdentity("run", "process", "single"),
        target_identity=b"t" * 32,
        route_fingerprint=b"r" * 32,
        load_id="load",
        target_database="DWH",
        target_schema="dbo",
        target_table="events",
        strategy="full_refresh",
    )
    attempt = MssqlTransactionAttempt(request, 1)
    operation = MssqlTransactionOperation(
        attempt,
        b"o" * 32,
        b"s" * 32,
        b"w" * 32,
        1,
    )
    return MssqlTransactionAdmission(operation=operation)
