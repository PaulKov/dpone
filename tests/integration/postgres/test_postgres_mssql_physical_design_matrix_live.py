"""Disposable SQL Server proof for the PostgreSQL→MSSQL physical-design matrix.

The matrix is intentionally finite and contract-derived.  Every valid value of
the MSSQL physical-design surface executes against SQL Server; combinations
that would silently discard an authored option must fail before any object is
created.  This is stronger than string-matching renderer tests because the
assertions read ``sys.*`` after the vendor accepted the DDL.
"""

from __future__ import annotations

import json
import uuid
from itertools import product
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder
from tools.route_live_certification.reviewed_cases_physical import (
    reviewed_strategy_cross_parameters,
)

from dpone.config import LoadConfig, LoadStrategy
from dpone.config.mssql_strategy_contract import MSSQLStrategyContractError
from dpone.contracts.mssql_object_name import MSSQLObjectName
from dpone.contracts.mssql_physical_design import MssqlPhysicalDesignContract
from dpone.readiness.physical_apply import PhysicalDdlApplyService
from dpone.readiness.physical_design import PhysicalDesignOptions, PhysicalDesignPlanner
from dpone.readiness.physical_design_models import PhysicalReconciliationOptions
from dpone.readiness.physical_reconciliation import (
    PhysicalDesignReconciler,
    PhysicalReconciliationApplyService,
)
from dpone.readiness.physical_state import PhysicalTableState
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.sinks.mssql_physical_introspection import MssqlPhysicalMigrationDialect
from dpone.runtime.sinks.mssql_target_catalog_reader import read_schema_catalog_snapshot
from dpone.runtime.sinks.strategies.mssql.mssql_generic_target_contract import (
    MssqlGenericTargetContract,
)
from tests.integration.postgres.postgres_live_support import (
    ensure_mssql_database_and_schemas,
    ensure_postgres_schemas,
    mssql_connector,
    postgres_connector,
    postgres_mssql_enabled,
    wait_until_ready,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedMssqlCampaign,
    GovernedPostgresSnapshotSource,
    GovernedStandardEtlRunner,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import QuietIntegrationLogger

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
    pytest.mark.route_live_wide,
]

_EVIDENCE_PATH = Path("test_artifacts/live_certification/postgres_mssql_physical_design_matrix.json")
_COMPRESSIONS = ("none", "row", "page")
_FILLFACTORS = (None, 1, 80, 100)
_MODES = ("auto", "explicit", "off")
_APPLY_MODES = ("online", "safe_window", "plan_only", "manual_approval")
_RECONCILIATION_MODES = ("block", "auto_safe", "plan_only", "safe_window")
_MIGRATION_MODES = ("block", "online_safe", "shadow")
_INVALID_PHYSICAL_CASES: tuple[tuple[str, dict[str, object]], ...] = (
    ("unknown_compression", {"storage": {"mssql": {"compression": "invalid"}}}),
    (
        "row_compression_with_cci",
        {"storage": {"mssql": {"compression": "row", "clustered_columnstore": True}}},
    ),
    (
        "page_compression_with_cci",
        {"storage": {"mssql": {"compression": "page", "clustered_columnstore": True}}},
    ),
    (
        "primary_key_with_cci",
        {
            "indexes": {"primary_key": ["id"]},
            "storage": {"mssql": {"clustered_columnstore": True}},
        },
    ),
    ("textimage_without_filegroup", {"storage": {"mssql": {"textimage_filegroup": "PRIMARY"}}}),
    ("fillfactor_without_primary_key", {"storage": {"mssql": {"index_fillfactor": 80}}}),
    (
        "fillfactor_zero",
        {"indexes": {"primary_key": ["id"]}, "storage": {"mssql": {"index_fillfactor": 0}}},
    ),
    (
        "fillfactor_101",
        {"indexes": {"primary_key": ["id"]}, "storage": {"mssql": {"index_fillfactor": 101}}},
    ),
    (
        "fillfactor_conflicting_alias",
        {
            "indexes": {"primary_key": ["id"]},
            "storage": {"mssql": {"index_fillfactor": 80, "fillfactor": 90}},
        },
    ),
    (
        "filegroup_injection",
        {"storage": {"mssql": {"filegroup": "PRIMARY]; DROP TABLE [dpone_it].[sentinel];--"}}},
    ),
    ("unknown_storage_option", {"storage": {"mssql": {"unsupported_setting": True}}}),
    ("unsupported_partitioning", {"partitioning": {"column": "id"}}),
    ("unknown_index_option", {"indexes": {"unsupported_index": ["id"]}}),
    (
        "target_type_injection",
        {"columns": {"payload": {"target_type": {"mssql": "nvarchar(10)); DROP TABLE [dpone_it].[sentinel];--"}}}},
    ),
)


class _LiveExecutor:
    def __init__(self, connector: Any) -> None:
        self.connector = connector

    def execute(self, request: Any) -> None:
        self.connector.execute_query(request.sql)


class _LiveCatalogContractStrategy:
    """Minimal production-contract adapter over one real SQL Server target."""

    def __init__(self, connector: Any) -> None:
        self.connector = connector

    def _table_exists(self, load_config: LoadConfig) -> bool:
        return _object_count(self.connector, str(load_config.target_table)) == 1

    def _target_name(self, load_config: LoadConfig) -> str:
        return MSSQLObjectName.from_parts(
            database=load_config.target_database,
            schema=load_config.target_schema,
            table=load_config.target_table,
        ).quoted()


@pytest.fixture(scope="module", autouse=True)
def _discard_stale_evidence() -> None:
    """A previous interrupted run must never satisfy this certification."""

    _EVIDENCE_PATH.unlink(missing_ok=True)


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_all_valid_rowstore_physical_design_combinations_live(
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Execute compression × key × fillfactor combinations and inspect them."""

    ensure_mssql_database_and_schemas()
    connector = mssql_connector()
    wait_until_ready("mssql", lambda: connector.get_records("SELECT 1"))
    prefix = f"pd_{uuid.uuid4().hex[:8]}"
    completed: list[dict[str, object]] = []
    try:
        cases = [
            (compression, has_primary, fillfactor)
            for compression, has_primary in product(_COMPRESSIONS, (False, True))
            for fillfactor in (_FILLFACTORS if has_primary else (None,))
        ]
        for ordinal, (compression, has_primary, fillfactor) in enumerate(cases):
            table = f"{prefix}_{ordinal:02d}"
            storage: dict[str, object] = {
                "compression": compression,
                "clustered_columnstore": False,
            }
            if fillfactor is not None:
                storage["index_fillfactor"] = fillfactor
            options: dict[str, object] = {"storage": {"mssql": storage}}
            if has_primary:
                options["indexes"] = {"primary_key": ["id"]}
            plan = PhysicalDesignPlanner().plan(
                sink_type="mssql",
                table=f"dpone_it.{table}",
                source_schema=[("id", "bigint"), ("payload", "nvarchar(max)")],
                options=PhysicalDesignOptions.from_config(options),
            )
            report = PhysicalDdlApplyService(executor=_LiveExecutor(connector)).apply(plan, table_exists=False)
            assert report.applied is True
            catalog = _catalog(connector, table)
            assert catalog["compression"] == compression.upper()
            if has_primary:
                index = catalog["key_index"]
                assert index is not None
                assert bool(index["is_unique"]) is True
                assert bool(index["is_primary_key"]) is True
                assert index["type_desc"] == "CLUSTERED"
                expected_fillfactor = 0 if fillfactor is None else fillfactor
                assert int(index["fill_factor"]) == expected_fillfactor
            else:
                assert catalog["key_index"] is None
            route_live_recorder.observe_parameters(
                "physical_design",
                {
                    "enabled": True,
                    "mode": "explicit",
                    "apply": "online",
                    "compression": compression,
                    "primary_key": ["id"] if has_primary else [],
                    "fillfactor": fillfactor,
                },
                before_image={"object_count": 0},
                after_image={"object_count": 1, "catalog": catalog},
                observations={"vendor_ddl_applied": True},
            )
            completed.append(
                {
                    "compression": compression,
                    "primary_key": has_primary,
                    "fillfactor": fillfactor,
                }
            )
            connector.execute_query(f"DROP TABLE [dpone_it].[{table}]")

        assert len(completed) == 15
        _write_evidence("rowstore", completed)
    finally:
        _drop_prefix(connector, prefix)
        connector.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_filegroup_lob_and_columnstore_design_live(
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Execute the remaining valid placement and columnstore capabilities."""

    ensure_mssql_database_and_schemas()
    connector = mssql_connector()
    wait_until_ready("mssql", lambda: connector.get_records("SELECT 1"))
    prefix = f"pd_extra_{uuid.uuid4().hex[:8]}"
    try:
        placement = PhysicalDesignPlanner().plan(
            sink_type="mssql",
            table=f"dpone_it.{prefix}_placement",
            source_schema=[("id", "bigint"), ("payload", "nvarchar(max)")],
            options=PhysicalDesignOptions.from_config(
                {
                    "indexes": {"primary_key": ["id"]},
                    "storage": {
                        "mssql": {
                            "compression": "row",
                            "filegroup": "PRIMARY",
                            "textimage_filegroup": "PRIMARY",
                            "index_fillfactor": 80,
                        }
                    },
                }
            ),
        )
        assert PhysicalDdlApplyService(executor=_LiveExecutor(connector)).apply(placement, table_exists=False).applied
        placement_catalog = _catalog(connector, f"{prefix}_placement")
        assert placement_catalog["data_space"] == "PRIMARY"
        assert placement_catalog["lob_data_space"] == "PRIMARY"
        assert placement_catalog["compression"] == "ROW"
        route_live_recorder.observe_case(
            "physical_design",
            "placement__rowstore_filegroup_textimage",
            before_image={"object_count": 0},
            after_image={"object_count": 1, "catalog": placement_catalog},
            observations={"filegroup": "PRIMARY", "textimage_filegroup": "PRIMARY"},
        )

        columnstore = PhysicalDesignPlanner().plan(
            sink_type="mssql",
            table=f"dpone_it.{prefix}_columnstore",
            source_schema=[("id", "bigint"), ("amount", "decimal(18,4)")],
            options=PhysicalDesignOptions.from_config(
                {"storage": {"mssql": {"compression": "none", "clustered_columnstore": True}}}
            ),
        )
        assert PhysicalDdlApplyService(executor=_LiveExecutor(connector)).apply(columnstore, table_exists=False).applied
        columnstore_catalog = _catalog(connector, f"{prefix}_columnstore")
        assert columnstore_catalog["columnstore_count"] == 1
        route_live_recorder.observe_case(
            "physical_design",
            "placement__clustered_columnstore",
            before_image={"object_count": 0},
            after_image={"object_count": 1, "catalog": columnstore_catalog},
            observations={"clustered_columnstore": True},
        )
        _write_evidence(
            "placement_and_columnstore",
            [
                {"filegroup": "PRIMARY", "textimage_filegroup": "PRIMARY", "compression": "row"},
                {"clustered_columnstore": True, "compression": "none"},
            ],
        )
    finally:
        _drop_prefix(connector, prefix)
        connector.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_invalid_physical_design_fails_before_vendor_mutation_live(
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Reject incompatible/ignored/injection-prone options before SQL execution."""

    ensure_mssql_database_and_schemas()
    connector = mssql_connector()
    wait_until_ready("mssql", lambda: connector.get_records("SELECT 1"))
    completed: list[dict[str, object]] = []
    try:
        for label, physical in _INVALID_PHYSICAL_CASES:
            table = f"pd_invalid_{uuid.uuid4().hex[:10]}"
            before = _object_count(connector, table)
            with pytest.raises(ValueError):
                PhysicalDesignPlanner().plan(
                    sink_type="mssql",
                    table=f"dpone_it.{table}",
                    source_schema=[("id", "bigint"), ("payload", "nvarchar(max)")],
                    options=PhysicalDesignOptions.from_config(physical),
                )
            assert _object_count(connector, table) == before == 0
            route_live_recorder.observe_case(
                "physical_design",
                f"reject__{label}",
                before_image={"object_count": before},
                after_image={"object_count": _object_count(connector, table)},
                observations={"blocked_before_vendor_mutation": True},
            )
            completed.append({"case": label, "blocked_before_vendor_mutation": True})
        assert len(completed) == len(_INVALID_PHYSICAL_CASES) == 14
        _write_evidence("unsupported_fail_closed", completed)
    finally:
        connector.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_unknown_filegroups_fail_before_source_copy_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Bind authored filegroups to exact writable ROWS catalog authority."""

    postgres = postgres_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema="dpone_src")
    suffix = uuid.uuid4().hex[:8]
    source_table = f"physical_filegroup_source_{suffix}"
    postgres.execute_query(f'CREATE TABLE "dpone_src"."{source_table}" (id bigint NOT NULL, payload text NULL)')
    postgres.execute_query(
        f'INSERT INTO "dpone_src"."{source_table}" (id, payload) VALUES (1, %s)',
        ("lob-payload",),
    )
    mssql = governed_mssql_live_campaign.target
    logger = QuietIntegrationLogger()
    try:
        for case_id, storage in (
            ("filegroup_missing", {"filegroup": "MISSING"}),
            (
                "textimage_wrong_filegroup",
                {"filegroup": "PRIMARY", "textimage_filegroup": "MISSING"},
            ),
        ):
            target = f"physical_{case_id}_{suffix}"
            config = LoadConfig(
                source_conn_id="postgres_source",
                target_conn_id="mssql_target",
                source_schema="dpone_src",
                source_table=source_table,
                target_schema="dpone_it",
                target_table=target,
                target_database=governed_mssql_live_campaign.target_database,
                staging_schema="staging",
                staging_database=governed_mssql_live_campaign.target_database,
                load_strategy=LoadStrategy.FULL_REFRESH,
                export_format="csv",
                compress_export=False,
                options={
                    "sink_type": "mssql",
                    "technical_columns": "required",
                    "partition_tmp_dir": str(tmp_path),
                    "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000}},
                    "physical_design": {
                        "mode": "explicit",
                        "apply": "online",
                        "apply_runtime": True,
                        "storage": {"mssql": storage},
                    },
                },
            )
            route = governed_mssql_live_campaign.route(
                target_schema=config.target_schema,
                target_table=config.target_table,
            )
            runner = GovernedStandardEtlRunner(
                route,
                postgres,
                logger=logger,
                source_type=GovernedPostgresSnapshotSource,
            )
            before = {
                "target_object_count": _object_count(mssql, target),
                "staging_object_count": _staging_count(mssql, target),
                "artifact_entries": sorted(path.name for path in tmp_path.iterdir()),
            }
            field = "textimage_filegroup" if case_id.startswith("textimage") else "filegroup"
            with pytest.raises(
                RuntimeError,
                match=rf"target_row_filegroup_unavailable:{field}:MISSING",
            ):
                runner.run(config, label=f"physical_{case_id}")
            after = {
                "target_object_count": _object_count(mssql, target),
                "staging_object_count": _staging_count(mssql, target),
                "artifact_entries": sorted(path.name for path in tmp_path.iterdir()),
            }
            assert after == before
            route_live_recorder.observe_case(
                "physical_design",
                f"reject__{case_id}",
                before_image=before,
                after_image=after,
                observations={
                    "blocker": f"target_row_filegroup_unavailable:{field}:MISSING",
                    "blocked_before_source_copy": True,
                    "available_row_filegroups": ["PRIMARY"],
                },
            )
    finally:
        postgres.execute_query(f'DROP TABLE IF EXISTS "dpone_src"."{source_table}" CASCADE')
        postgres.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_runtime_activation_modes_have_exact_vendor_effect_live(
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Treat apply_runtime as an activation switch, never as invalid authoring."""

    ensure_mssql_database_and_schemas()
    connector = mssql_connector()
    wait_until_ready("mssql", lambda: connector.get_records("SELECT 1"))
    prefix = f"pd_runtime_{uuid.uuid4().hex[:8]}"
    try:
        enabled_table = f"{prefix}_enabled"
        enabled_raw = {
            "physical_design": {
                "mode": "explicit",
                "apply": "online",
                "apply_runtime": True,
                "indexes": {"primary_key": ["id"]},
                "storage": {"mssql": {"compression": "row"}},
            }
        }
        enabled_contract = MssqlPhysicalDesignContract.from_options(enabled_raw)
        assert enabled_contract.active is True
        enabled_plan = PhysicalDesignPlanner().plan(
            sink_type="mssql",
            table=f"dpone_it.{enabled_table}",
            source_schema=[("id", "bigint"), ("payload", "text")],
            options=PhysicalDesignOptions.from_config(enabled_raw["physical_design"]),
        )
        before_enabled = {"object_count": _object_count(connector, enabled_table)}
        report = PhysicalDdlApplyService(executor=_LiveExecutor(connector)).apply(
            enabled_plan,
            table_exists=False,
        )
        assert report.applied is True
        after_enabled = {
            "object_count": _object_count(connector, enabled_table),
            "catalog": _catalog(connector, enabled_table),
        }
        assert after_enabled["object_count"] == 1
        assert after_enabled["catalog"]["compression"] == "ROW"
        route_live_recorder.observe_case(
            "physical_design",
            "runtime_activation__enabled",
            before_image=before_enabled,
            after_image=after_enabled,
            observations={"apply_runtime": True, "vendor_ddl_applied": True},
        )

        disabled_table = f"{prefix}_disabled"
        disabled_raw = {
            "physical_design": {
                "mode": "explicit",
                "apply": "online",
                "apply_runtime": False,
                "indexes": {"primary_key": ["id"]},
                "storage": {"mssql": {"compression": "row"}},
            }
        }
        disabled_contract = MssqlPhysicalDesignContract.from_options(disabled_raw)
        assert disabled_contract.active is False
        disabled_image = {"object_count": _object_count(connector, disabled_table)}
        assert disabled_image == {"object_count": 0}
        route_live_recorder.observe_case(
            "physical_design",
            "runtime_activation__disabled_with_online_apply",
            before_image=disabled_image,
            after_image={"object_count": _object_count(connector, disabled_table)},
            observations={"apply_runtime": False, "vendor_ddl_applied": False},
        )
    finally:
        _drop_prefix(connector, prefix)
        connector.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_primary_key_capability_boundaries_live(
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Execute every supported PK edge and vendor-prove every rejected edge."""

    ensure_mssql_database_and_schemas()
    connector = mssql_connector()
    wait_until_ready("mssql", lambda: connector.get_records("SELECT 1"))
    prefix = f"pd_pk_{uuid.uuid4().hex[:8]}"
    cases: tuple[
        tuple[str, list[str] | None, list[tuple[str, str]], dict[str, object], bool],
        ...,
    ] = (
        ("absent", None, [("id", "bigint")], {}, True),
        ("single", ["id"], [("id", "bigint")], {}, True),
        (
            "composite_16",
            [f"k{i:02d}" for i in range(16)],
            [(f"k{i:02d}", "bigint") for i in range(16)],
            {},
            True,
        ),
        (
            "composite_17",
            [f"k{i:02d}" for i in range(17)],
            [(f"k{i:02d}", "bigint") for i in range(17)],
            {},
            False,
        ),
        ("duplicate", ["id", "id"], [("id", "bigint")], {}, False),
        ("missing_column", ["not_present"], [("id", "bigint")], {}, False),
        (
            "nullable_source_column_becomes_not_null",
            ["nullable_key"],
            [("nullable_key", "text")],
            {"nullable_key": {"target_type": {"mssql": "nvarchar(100)"}}},
            True,
        ),
        (
            "clustered_bytes_equal_900",
            ["nvarchar_450"],
            [("nvarchar_450", "text")],
            {"nvarchar_450": {"target_type": {"mssql": "nvarchar(450)"}}},
            True,
        ),
        (
            "clustered_bytes_902",
            ["nvarchar_451"],
            [("nvarchar_451", "text")],
            {"nvarchar_451": {"target_type": {"mssql": "nvarchar(451)"}}},
            False,
        ),
        (
            "max_type",
            ["nvarchar_max"],
            [("nvarchar_max", "text")],
            {"nvarchar_max": {"target_type": {"mssql": "nvarchar(max)"}}},
            False,
        ),
    )
    try:
        for ordinal, (name, primary_key, source_schema, columns, accepted) in enumerate(cases):
            table = f"{prefix}_{ordinal:02d}"
            raw: dict[str, object] = {
                "mode": "explicit",
                "apply": "online",
                "columns": columns,
                "storage": {"mssql": {"compression": "none"}},
            }
            if primary_key is not None:
                raw["indexes"] = {"primary_key": primary_key}
            before = {"object_count": _object_count(connector, table)}
            if accepted:
                plan = PhysicalDesignPlanner().plan(
                    sink_type="mssql",
                    table=f"dpone_it.{table}",
                    source_schema=source_schema,
                    options=PhysicalDesignOptions.from_config(raw),
                )
                report = PhysicalDdlApplyService(executor=_LiveExecutor(connector)).apply(
                    plan,
                    table_exists=False,
                )
                assert report.applied is True
                primary_catalog = _primary_key_catalog(connector, table)
                assert [str(row["column_name"]) for row in primary_catalog] == (primary_key or [])
                assert all(not bool(row["is_nullable"]) for row in primary_catalog)
                after = {
                    "object_count": _object_count(connector, table),
                    "primary_key_catalog": primary_catalog,
                }
                assert after["object_count"] == 1
                observations = {
                    "vendor_ddl_applied": True,
                    "primary_key_columns": primary_key or [],
                }
            else:
                error: Exception | None = None
                try:
                    PhysicalDesignPlanner().plan(
                        sink_type="mssql",
                        table=f"dpone_it.{table}",
                        source_schema=source_schema,
                        options=PhysicalDesignOptions.from_config(raw),
                    )
                except ValueError as exc:
                    error = exc
                assert error is not None, name
                after = {"object_count": _object_count(connector, table)}
                assert after == before
                observations = {
                    "vendor_ddl_applied": False,
                    "error_type": type(error).__name__,
                    "error_code": getattr(error, "code", None),
                }
            route_live_recorder.observe_case(
                "physical_design",
                f"primary_key__{name}",
                before_image=before,
                after_image=after,
                observations=observations,
            )
    finally:
        _drop_prefix(connector, prefix)
        connector.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_existing_unique_catalog_states_are_exact_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Vendor-prove every reviewed existing-index authority state.

    Arrangement uses a real governed incremental-merge load so the target,
    lineage columns, strategy-owned unique index, and target identity are the
    same objects production creates.  Each case then changes exactly one
    ``sys.indexes``/``sys.partitions`` dimension and invokes the public target
    contract against that real catalog without performing business DML.
    """

    suffix = uuid.uuid4().hex[:8]
    source_table = f"physical_catalog_source_{suffix}"
    postgres = postgres_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema="dpone_src")
    postgres.execute_query(
        f'CREATE TABLE "dpone_src"."{source_table}" '
        "(id bigint NOT NULL, partition_id bigint NOT NULL, payload text NULL)"
    )
    postgres.execute_query(
        f'INSERT INTO "dpone_src"."{source_table}" (id, partition_id, payload) VALUES (1, 10, \'catalog-authority\')'
    )
    mssql = governed_mssql_live_campaign.target
    logger = QuietIntegrationLogger()
    partition_cleanup: list[tuple[str, str]] = []
    cases = (
        ("exact_rerun", ("id",), "exact", True),
        ("disabled_filtered", ("id",), "disabled_filtered", False),
        ("hypothetical", ("id",), "hypothetical", False),
        ("partitioned", ("id",), "partitioned", False),
        ("wrong_order", ("id", "partition_id"), "wrong_order", False),
        ("wrong_compression", ("id",), "wrong_compression", False),
        ("ignore_dup_key", ("id",), "ignore_dup_key", False),
        ("filtered_unique", ("id",), "filtered_unique", False),
        ("included_columns", ("id",), "included_columns", True),
    )
    try:
        for ordinal, (case_id, unique_key, mutation, accepted) in enumerate(cases):
            target_table = f"physical_catalog_{suffix}_{ordinal}"
            case_work_dir = tmp_path / f"catalog_{ordinal:02d}"
            case_work_dir.mkdir()
            config = _catalog_case_config(
                source_table=source_table,
                target_table=target_table,
                target_database=governed_mssql_live_campaign.target_database,
                work_dir=case_work_dir,
                unique_key=unique_key,
            )
            route = governed_mssql_live_campaign.route(
                target_schema=config.target_schema,
                target_table=config.target_table,
            )
            runner = GovernedStandardEtlRunner(
                route,
                postgres,
                logger=logger,
                source_type=GovernedPostgresSnapshotSource,
            )
            baseline = runner.run(config, label=f"physical_catalog_baseline_{ordinal}_{suffix}")
            assert baseline["status"] == "success"
            assert baseline["loaded_rows"] == 1
            assert _staging_count(mssql, target_table) == 0
            assert not tuple(case_work_dir.iterdir())

            index_name = _strategy_unique_index_name(mssql, target_table)
            _assert_fresh_framework_unique_catalog(
                _unique_index_catalog(mssql, target_table),
                index_name=index_name,
                key_columns=unique_key,
            )
            cleanup_objects = _apply_catalog_index_state(
                mssql,
                target_table=target_table,
                index_name=index_name,
                mutation=mutation,
                key_columns=unique_key,
                suffix=f"{ordinal}_{suffix}",
            )
            partition_cleanup.extend(cleanup_objects)
            before = _catalog_case_image(mssql, target_table, case_work_dir)
            strategy = _LiveCatalogContractStrategy(mssql)
            staging = _catalog_contract_staging(strategy, config)
            error: SnapshotReconciliationError | None = None
            try:
                MssqlGenericTargetContract(strategy).validate_existing(config, staging)
            except SnapshotReconciliationError as exc:
                error = exc

            if accepted:
                assert error is None, (case_id, error)
            else:
                assert error is not None, case_id
                assert str(error) == "mssql_native_projection.target_unique_authority_missing"
            after = _catalog_case_image(mssql, target_table, case_work_dir)
            assert after == before, case_id
            route_live_recorder.observe_case(
                "physical_design",
                f"catalog__{case_id}",
                before_image=before,
                after_image=after,
                observations={
                    "catalog_state": mutation,
                    "contract_accepted": accepted,
                    "typed_blocker": str(error) if error is not None else None,
                    "business_dml_executed": False,
                    "staging_cleanup_asserted": after["staging_objects"] == 0,
                    "artifact_cleanup_asserted": after["artifact_entries"] == [],
                },
            )

            mssql.execute_query(f"DROP TABLE [dpone_it].[{target_table}]")
    finally:
        postgres.execute_query(f'DROP TABLE IF EXISTS "dpone_src"."{source_table}" CASCADE')
        postgres.close()
        _drop_prefix(mssql, f"physical_catalog_{suffix}_")
        _drop_partition_objects(mssql, partition_cleanup)


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_strategy_physical_design_cross_contract_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Execute every reviewed keyed sink strategy against the same design."""

    suffix = uuid.uuid4().hex[:8]
    source_table = f"physical_cross_source_{suffix}"
    postgres = postgres_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema="dpone_src")
    postgres.execute_query(f'CREATE TABLE "dpone_src"."{source_table}" (id text NOT NULL, payload text NULL)')
    mssql = governed_mssql_live_campaign.target
    logger = QuietIntegrationLogger()
    target_prefix = f"physical_cross_{suffix}_"
    try:
        for ordinal, parameters in enumerate(reviewed_strategy_cross_parameters()):
            target_table = f"{target_prefix}{ordinal}"
            case_work_dir = tmp_path / f"strategy_cross_{ordinal:02d}"
            case_work_dir.mkdir()
            config = _strategy_cross_config(
                source_table=source_table,
                target_table=target_table,
                target_database=governed_mssql_live_campaign.target_database,
                work_dir=case_work_dir,
                parameters=parameters,
            )
            route = governed_mssql_live_campaign.route(
                target_schema=config.target_schema,
                target_table=config.target_table,
            )
            runner = GovernedStandardEtlRunner(
                route,
                postgres,
                logger=logger,
                source_type=GovernedPostgresSnapshotSource,
            )
            _set_strategy_cross_source_rows(postgres, source_table, (("Alpha", "old"),))
            if parameters["target_state"] == "existing":
                baseline = runner.run(config, label=f"physical_cross_baseline_{ordinal}_{suffix}")
                assert baseline["status"] == "success"
                _assert_strategy_cross_catalog(mssql, target_table, parameters)
            before = _strategy_cross_image(mssql, target_table, case_work_dir)
            _set_strategy_cross_source_rows(
                postgres,
                source_table,
                (("Alpha", "new"), ("Beta", "inserted")),
            )
            result = runner.run(config, label=f"physical_cross_load_{ordinal}_{suffix}")
            assert result["status"] == "success"
            after = _strategy_cross_image(mssql, target_table, case_work_dir)
            assert after != before
            assert after["staging_objects"] == 0
            assert after["artifact_entries"] == []
            _assert_strategy_cross_catalog(mssql, target_table, parameters)
            route_live_recorder.observe_parameters(
                "physical_design",
                parameters,
                before_image=before,
                after_image=after,
                observations={
                    "source_boundary": "complete_relation_snapshot",
                    "certification_scope": "governed_mssql_sink_physical_interaction",
                    "production_column_cursor_support_claimed": False,
                    "staging_cleanup_asserted": True,
                    "artifact_cleanup_asserted": True,
                },
            )
            mssql.execute_query(f"DROP TABLE [dpone_it].[{target_table}]")

        reject_target = f"{target_prefix}reject"
        reject_work_dir = tmp_path / "strategy_cross_reject"
        reject_work_dir.mkdir()
        reject_parameters = {
            "strategy": "scd2",
            "unique_key": ["id"],
            "primary_key": ["id"],
            "source_boundary": "complete_relation_snapshot",
            "certification_scope": "governed_mssql_sink_physical_interaction",
        }
        reject_config = _strategy_cross_config(
            source_table=source_table,
            target_table=reject_target,
            target_database=governed_mssql_live_campaign.target_database,
            work_dir=reject_work_dir,
            parameters={
                **reject_parameters,
                "submode": "expire",
                "target_state": "missing",
                "text_key_collation": "Latin1_General_100_BIN2",
                "compression": "row",
            },
            force_primary_key=("id",),
        )
        reject_route = governed_mssql_live_campaign.route(
            target_schema=reject_config.target_schema,
            target_table=reject_config.target_table,
        )
        reject_runner = GovernedStandardEtlRunner(
            reject_route,
            postgres,
            logger=logger,
            source_type=GovernedPostgresSnapshotSource,
        )
        _set_strategy_cross_source_rows(postgres, source_table, (("Alpha", "old"),))
        before_reject = _strategy_cross_image(mssql, reject_target, reject_work_dir)
        with pytest.raises(MSSQLStrategyContractError) as raised:
            reject_runner.run(reject_config, label=f"physical_cross_reject_{suffix}")
        assert raised.value.blocker == "mssql.strategy.scd2.physical_primary_key"
        after_reject = _strategy_cross_image(mssql, reject_target, reject_work_dir)
        assert after_reject == before_reject
        route_live_recorder.observe_case(
            "physical_design",
            "strategy_cross__scd2_business_key_primary_key_reject",
            before_image=before_reject,
            after_image=after_reject,
            observations={
                "typed_blocker": str(raised.value),
                "blocked_before_source_copy": True,
                "staging_cleanup_asserted": after_reject["staging_objects"] == 0,
                "artifact_cleanup_asserted": after_reject["artifact_entries"] == [],
            },
        )
    finally:
        postgres.execute_query(f'DROP TABLE IF EXISTS "dpone_src"."{source_table}" CASCADE')
        postgres.close()
        _drop_prefix(mssql, target_prefix)


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_physical_policy_cross_product_live(
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Cover every enum combination; apply or prove a zero-object before-image."""

    ensure_mssql_database_and_schemas()
    connector = mssql_connector()
    wait_until_ready("mssql", lambda: connector.get_records("SELECT 1"))
    prefix = f"pd_policy_{uuid.uuid4().hex[:8]}"
    completed: list[dict[str, object]] = []
    cases = product(_MODES, _APPLY_MODES, _RECONCILIATION_MODES, _MIGRATION_MODES)
    try:
        for ordinal, (mode, apply, reconciliation, migration) in enumerate(cases):
            table = f"{prefix}_{ordinal:03d}"
            options = PhysicalDesignOptions.from_config(
                {
                    "mode": mode,
                    "apply": apply,
                    "columns": {"payload": {"target_type": {"mssql": "nvarchar(100)"}}},
                    "storage": {"mssql": {"compression": "none"}},
                    "reconciliation": {"mode": reconciliation},
                    "migration": {"strategy": migration},
                }
            )
            plan = PhysicalDesignPlanner().plan(
                sink_type="mssql",
                table=f"dpone_it.{table}",
                source_schema=[("id", "bigint"), ("payload", "text")],
                options=options,
            )
            assert plan.options.reconciliation.mode == reconciliation
            assert plan.options.migration.strategy == migration
            report = PhysicalDdlApplyService(executor=_LiveExecutor(connector)).apply(plan, table_exists=False)
            should_apply = mode in {"auto", "explicit"} and apply in {"online", "safe_window"}
            assert report.applied is should_apply
            assert _object_count(connector, table) == int(should_apply)
            if should_apply:
                metadata = connector.get_records(
                    "SELECT c.max_length FROM sys.columns AS c "
                    "INNER JOIN sys.tables AS t ON t.object_id = c.object_id "
                    "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
                    "WHERE s.name = N'dpone_it' AND t.name = ? AND c.name = N'payload'",
                    (table,),
                    as_dict=True,
                )
                assert int(metadata[0]["max_length"]) == 200
                after_image = {
                    "object_count": 1,
                    "payload_max_length": int(metadata[0]["max_length"]),
                }
                connector.execute_query(f"DROP TABLE [dpone_it].[{table}]")
            else:
                after_image = {"object_count": 0}
            route_live_recorder.observe_parameters(
                "physical_design",
                {
                    "mode": mode,
                    "apply": apply,
                    "reconciliation": reconciliation,
                    "migration": migration,
                    "target_state": "missing",
                },
                before_image={"object_count": 0},
                after_image=after_image,
                observations={"applied": should_apply},
            )
            completed.append(
                {
                    "mode": mode,
                    "apply": apply,
                    "reconciliation": reconciliation,
                    "migration": migration,
                    "applied": should_apply,
                }
            )

        assert len(completed) == 144
        assert sum(bool(case["applied"]) for case in completed) == 48
        _write_evidence("policy_cross_product", completed)
    finally:
        _drop_prefix(connector, prefix)
        connector.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_existing_table_compression_reconciliation_matrix_live(
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Exercise every MSSQL compression transition and approval outcome.

    SQL Server compression rebuilds are intentionally classified as blocking.
    Therefore only an exact, live, unexpired ``safe_window`` approval may
    mutate the table.  Every negative case compares both catalog state and
    business rows before and after reconciliation.
    """

    ensure_mssql_database_and_schemas()
    connector = mssql_connector()
    wait_until_ready("mssql", lambda: connector.get_records("SELECT 1"))
    prefix = f"pd_reconcile_{uuid.uuid4().hex[:8]}"
    approval_cases = (
        ("block", None, False),
        ("auto_safe", None, False),
        ("plan_only", None, False),
        ("safe_window", None, False),
        (
            "safe_window",
            {
                "approved_by": "route-live-certification",
                "approved_risks": ["table_settings.compression"],
                "expires_at": "2099-01-01T00:00:00Z",
            },
            True,
        ),
        (
            "safe_window",
            {
                "approved_by": "route-live-certification",
                "approved_risks": ["table_settings.compression"],
                "expires_at": "2099-01-01T00:00:00Z",
                "table": "dpone_it.not_the_target",
            },
            False,
        ),
        (
            "safe_window",
            {
                "approved_by": "route-live-certification",
                "approved_risks": ["table_settings.compression"],
                "expires_at": "2000-01-01T00:00:00Z",
            },
            False,
        ),
        (
            "safe_window",
            {
                "approved_by": "route-live-certification",
                "approved_risks": ["table_settings.clustered_columnstore"],
                "expires_at": "2099-01-01T00:00:00Z",
            },
            False,
        ),
    )
    completed: list[dict[str, object]] = []
    try:
        cases = product(_COMPRESSIONS, _COMPRESSIONS, approval_cases)
        for ordinal, (actual, desired, approval_case) in enumerate(cases):
            mode, approval, approval_is_exact = approval_case
            table = f"{prefix}_{ordinal:03d}"
            _create_compressed_table(connector, table, actual)
            before_rows = _business_rows(connector, table)
            before_catalog = _catalog(connector, table)
            desired_state = PhysicalTableState(
                sink_type="mssql",
                table=f"dpone_it.{table}",
                table_settings={"compression": desired.upper()},
            )
            actual_state = PhysicalTableState(
                sink_type="mssql",
                table=f"dpone_it.{table}",
                table_settings={"compression": actual.upper()},
            )
            raw_options: dict[str, object] = {"mode": mode}
            if approval is not None:
                raw_options["approval"] = approval
            options = PhysicalReconciliationOptions.from_config(raw_options)
            plan = PhysicalDesignReconciler().reconcile(
                desired=desired_state,
                actual=actual_state,
                options=options,
                dialect=MssqlPhysicalMigrationDialect(),
                execution_apply_mode="safe_window",
            )
            report = PhysicalReconciliationApplyService(executor=_LiveExecutor(connector)).apply(plan)
            has_drift = actual != desired
            should_apply = has_drift and mode == "safe_window" and approval_is_exact
            assert report.applied is should_apply
            assert _catalog(connector, table)["compression"] == (desired if should_apply else actual).upper()
            assert _business_rows(connector, table) == before_rows == [(1, "preserve-me")]
            if has_drift and not should_apply:
                assert report.blockers
                assert not report.executed
            if not has_drift:
                assert not report.has_drift
                assert not report.executed
            after_catalog = _catalog(connector, table)
            after_rows = _business_rows(connector, table)
            route_live_recorder.observe_parameters(
                "physical_design",
                {
                    "actual": actual,
                    "desired": desired,
                    "reconciliation": mode,
                    "approval": _reviewed_approval_label(approval),
                },
                before_image={"catalog": before_catalog, "rows": before_rows},
                after_image={"catalog": after_catalog, "rows": after_rows},
                observations={
                    "applied": should_apply,
                    "blocker_count": len(report.blockers),
                },
            )
            completed.append(
                {
                    "actual": actual,
                    "desired": desired,
                    "mode": mode,
                    "approval": _approval_label(approval),
                    "applied": should_apply,
                }
            )
            connector.execute_query(f"DROP TABLE [dpone_it].[{table}]")

        assert len(completed) == 72
        assert sum(bool(case["applied"]) for case in completed) == 6
        _write_evidence("existing_table_compression_reconciliation", completed)
    finally:
        _drop_prefix(connector, prefix)
        connector.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_zz_postgres_mssql_physical_design_evidence_is_complete() -> None:
    """Reject a partial run even when its earlier individual cases passed."""

    payload = json.loads(_EVIDENCE_PATH.read_text(encoding="utf-8"))
    assert payload["status"] == "passed_partial"
    assert payload["release_ready"] is False
    assert set(payload["sections"]) == {
        "rowstore",
        "placement_and_columnstore",
        "unsupported_fail_closed",
        "policy_cross_product",
        "existing_table_compression_reconciliation",
    }


def _catalog_case_config(
    *,
    source_table: str,
    target_table: str,
    target_database: str,
    work_dir: Path,
    unique_key: tuple[str, ...],
) -> LoadConfig:
    return LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_target",
        source_schema="dpone_src",
        source_table=source_table,
        target_database=target_database,
        target_schema="dpone_it",
        target_table=target_table,
        staging_database=target_database,
        staging_schema="staging",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=list(unique_key),
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "partition_tmp_dir": str(work_dir),
            "technical_columns": "required",
            "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000}},
            "physical_design": {
                "mode": "explicit",
                "apply": "online",
                "apply_runtime": True,
                "storage": {"mssql": {"compression": "none"}},
            },
        },
    )


def _strategy_cross_config(
    *,
    source_table: str,
    target_table: str,
    target_database: str,
    work_dir: Path,
    parameters: dict[str, object],
    force_primary_key: tuple[str, ...] | None = None,
) -> LoadConfig:
    strategy_name = str(parameters["strategy"])
    strategy = {
        "incremental_append": LoadStrategy.INCREMENTAL_APPEND,
        "incremental_merge": LoadStrategy.INCREMENTAL_MERGE,
        "snapshot_diff": LoadStrategy.SNAPSHOT_DIFF,
        "scd2": LoadStrategy.SCD2,
    }[strategy_name]
    primary_key = force_primary_key
    if primary_key is None:
        primary_key = tuple(str(value) for value in parameters.get("primary_key", []))
    physical_design: dict[str, object] = {
        "mode": "explicit",
        "apply": "online",
        "apply_runtime": True,
        "columns": {"id": {"target_type": {"mssql": "nvarchar(100)"}}},
        "storage": {"mssql": {"compression": "row"}},
    }
    if primary_key:
        physical_design["indexes"] = {"primary_key": list(primary_key)}
    options: dict[str, object] = {
        "source_type": "postgres",
        "sink_type": "mssql",
        "batch_commit_mode": "whole",
        "partition_tmp_dir": str(work_dir),
        "technical_columns": "required",
        "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000}},
        "physical_design": physical_design,
    }
    if strategy is LoadStrategy.SNAPSHOT_DIFF:
        options["diff"] = {"compare": "row_hash", "delete_policy": "ignore"}
    elif strategy is LoadStrategy.SCD2:
        options["scd2"] = {"delete_policy": "expire"}
    return LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_target",
        source_schema="dpone_src",
        source_table=source_table,
        target_database=target_database,
        target_schema="dpone_it",
        target_table=target_table,
        staging_database=target_database,
        staging_schema="staging",
        load_strategy=strategy,
        unique_key=["id"],
        only_new_rows=strategy is LoadStrategy.INCREMENTAL_APPEND,
        merge_policy="update_insert" if strategy is LoadStrategy.INCREMENTAL_MERGE else "auto",
        export_format="csv",
        compress_export=False,
        options=options,
    )


def _set_strategy_cross_source_rows(
    postgres: Any,
    table: str,
    rows: tuple[tuple[str, str], ...],
) -> None:
    postgres.execute_query(f'DELETE FROM "dpone_src"."{table}"')
    for row in rows:
        postgres.execute_query(
            f'INSERT INTO "dpone_src"."{table}" (id, payload) VALUES (%s, %s)',
            row,
        )


def _strategy_cross_image(
    connector: Any,
    table: str,
    work_dir: Path,
) -> dict[str, object]:
    exists = _object_count(connector, table) == 1
    rows = (
        connector.get_records(
            f"SELECT [id], [payload] FROM [dpone_it].[{table}] ORDER BY [id], [payload]",
            as_dict=True,
        )
        if exists
        else []
    )
    return {
        "target_exists": exists,
        "business_rows": rows,
        "physical_catalog": _catalog(connector, table) if exists else None,
        "primary_key_catalog": _primary_key_catalog(connector, table) if exists else [],
        "unique_index_catalog": _unique_index_catalog(connector, table) if exists else [],
        "id_column_catalog": _strategy_cross_id_catalog(connector, table) if exists else None,
        "staging_objects": _staging_count(connector, table),
        "artifact_entries": sorted(path.name for path in work_dir.iterdir()),
    }


def _strategy_cross_id_catalog(connector: Any, table: str) -> dict[str, object]:
    rows = connector.get_records(
        "SELECT ty.name AS type_name, c.max_length, c.is_nullable, c.collation_name "
        "FROM sys.tables AS t INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "INNER JOIN sys.columns AS c ON c.object_id = t.object_id "
        "INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id "
        "WHERE s.name = N'dpone_it' AND t.name = ? AND c.name = N'id'",
        (table,),
        as_dict=True,
    )
    assert len(rows) == 1
    return dict(rows[0])


def _assert_strategy_cross_catalog(
    connector: Any,
    table: str,
    parameters: dict[str, object],
) -> None:
    column = _strategy_cross_id_catalog(connector, table)
    assert str(column["type_name"]).lower() == "nvarchar"
    assert int(column["max_length"]) == 200
    assert bool(column["is_nullable"]) is False
    assert str(column["collation_name"]) == "Latin1_General_100_BIN2"
    physical = _catalog(connector, table)
    assert physical["compression"] == "ROW"
    primary = _primary_key_catalog(connector, table)
    unique = _unique_index_catalog(connector, table)
    if parameters["strategy"] == "scd2":
        assert primary == []
        assert unique
        assert {bool(row["has_filter"]) for row in unique} == {True}
        assert {
            "".join(
                character for character in str(row["filter_definition"]).casefold() if character not in "[]() \t\r\n"
            )
            for row in unique
        } == {"__dpone__is_current=1"}
        assert {str(row["data_compression_desc"]) for row in unique} == {"NONE"}
    else:
        assert [str(row["column_name"]) for row in primary] == ["id"]
        assert all(bool(row["is_primary_key"]) for row in primary)
        assert unique == []


def _catalog_contract_staging(
    strategy: _LiveCatalogContractStrategy,
    load_config: LoadConfig,
) -> StagingTableArtifact:
    snapshot = read_schema_catalog_snapshot(strategy, load_config)
    assert snapshot.exists
    columns = tuple(column.name for column in snapshot.columns)
    definitions = {column.name: column.to_column_def() for column in snapshot.columns}
    return StagingTableArtifact(
        schema="staging",
        table="not_materialized_for_catalog_preflight",
        columns=columns,
        staging_manager=object(),
        database=str(load_config.target_database),
        target_schema=str(load_config.target_schema),
        target_column_types={name: definition.dtype for name, definition in definitions.items()},
        target_column_nullability={name: definition.nullable for name, definition in definitions.items()},
        target_column_collations={
            name: definition.collation for name, definition in definitions.items() if definition.collation is not None
        },
    )


def _strategy_unique_index_name(connector: Any, table: str) -> str:
    rows = connector.get_records(
        "SELECT i.name FROM sys.indexes AS i "
        "INNER JOIN sys.tables AS t ON t.object_id = i.object_id "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = N'dpone_it' AND t.name = ? AND i.is_unique = 1 "
        "AND i.is_primary_key = 0 ORDER BY i.index_id",
        (table,),
        as_dict=True,
    )
    assert len(rows) == 1, (table, rows)
    return str(rows[0]["name"])


def _apply_catalog_index_state(
    connector: Any,
    *,
    target_table: str,
    index_name: str,
    mutation: str,
    key_columns: tuple[str, ...],
    suffix: str,
) -> tuple[tuple[str, str], ...]:
    if mutation == "exact":
        return ()
    table = f"[dpone_it].[{target_table}]"
    index = f"[{index_name}]"
    connector.execute_query(f"DROP INDEX {index} ON {table}")
    keys = key_columns
    options = "DATA_COMPRESSION = NONE"
    predicate = ""
    include = ""
    placement = ""
    cleanup: tuple[tuple[str, str], ...] = ()
    if mutation == "disabled_filtered":
        predicate = " WHERE [id] > 0"
    elif mutation == "hypothetical":
        options = "STATISTICS_ONLY = -1"
    elif mutation == "partitioned":
        function = f"pf_dpone_catalog_{suffix}"
        scheme = f"ps_dpone_catalog_{suffix}"
        connector.execute_query(f"CREATE PARTITION FUNCTION [{function}] (bigint) AS RANGE RIGHT FOR VALUES (100)")
        connector.execute_query(f"CREATE PARTITION SCHEME [{scheme}] AS PARTITION [{function}] ALL TO ([PRIMARY])")
        placement = f" ON [{scheme}] ([id])"
        cleanup = (("SCHEME", scheme), ("FUNCTION", function))
    elif mutation == "wrong_order":
        keys = tuple(reversed(key_columns))
    elif mutation == "wrong_compression":
        options = "DATA_COMPRESSION = ROW"
    elif mutation == "ignore_dup_key":
        options = "DATA_COMPRESSION = NONE, IGNORE_DUP_KEY = ON"
    elif mutation == "filtered_unique":
        predicate = " WHERE [id] > 0"
    elif mutation == "included_columns":
        include = " INCLUDE ([payload])"
    else:  # pragma: no cover - finite reviewed case table owns completeness.
        raise AssertionError(f"unknown catalog index state: {mutation}")
    columns = ", ".join(f"[{column}]" for column in keys)
    connector.execute_query(
        f"CREATE UNIQUE NONCLUSTERED INDEX {index} ON {table} ({columns})"
        f"{include}{predicate} WITH ({options}){placement}"
    )
    if mutation == "disabled_filtered":
        connector.execute_query(f"ALTER INDEX {index} ON {table} DISABLE")
    return cleanup


def _catalog_case_image(connector: Any, table: str, work_dir: Path) -> dict[str, object]:
    return {
        "business_rows": connector.get_records(
            f"SELECT [id], [partition_id], [payload] FROM [dpone_it].[{table}] ORDER BY [id]",
            as_dict=True,
        ),
        "unique_index_catalog": _unique_index_catalog(connector, table),
        "staging_objects": _staging_count(connector, table),
        "artifact_entries": sorted(path.name for path in work_dir.iterdir()),
    }


def _unique_index_catalog(connector: Any, table: str) -> list[dict[str, object]]:
    return list(
        connector.get_records(
            "SELECT i.name AS index_name, i.type_desc, i.is_unique, i.is_primary_key, "
            "i.is_disabled, i.is_hypothetical, i.ignore_dup_key, i.has_filter, "
            "i.filter_definition, ds.name AS data_space_name, ds.type_desc AS data_space_type_desc, "
            "c.name AS column_name, ic.key_ordinal, ic.is_included_column, "
            "p.partition_number, p.data_compression_desc "
            "FROM sys.indexes AS i "
            "INNER JOIN sys.tables AS t ON t.object_id = i.object_id "
            "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
            "LEFT JOIN sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id "
            "LEFT JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            "LEFT JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            "LEFT JOIN sys.partitions AS p ON p.object_id = i.object_id AND p.index_id = i.index_id "
            "WHERE s.name = N'dpone_it' AND t.name = ? AND i.is_unique = 1 "
            "AND i.is_primary_key = 0 "
            "ORDER BY i.index_id, p.partition_number, ic.is_included_column, "
            "ic.key_ordinal, ic.index_column_id",
            (table,),
            as_dict=True,
        )
    )


def _assert_fresh_framework_unique_catalog(
    rows: list[dict[str, object]],
    *,
    index_name: str,
    key_columns: tuple[str, ...],
) -> None:
    assert rows
    assert {str(row["index_name"]) for row in rows} == {index_name}
    assert {str(row["type_desc"]) for row in rows} == {"NONCLUSTERED"}
    assert {bool(row["is_primary_key"]) for row in rows} == {False}
    assert {bool(row["is_disabled"]) for row in rows} == {False}
    assert {bool(row["is_hypothetical"]) for row in rows} == {False}
    assert {bool(row["ignore_dup_key"]) for row in rows} == {False}
    assert {bool(row["has_filter"]) for row in rows} == {False}
    assert {str(row["data_space_type_desc"]) for row in rows} == {"ROWS_FILEGROUP"}
    assert {int(row["partition_number"]) for row in rows} == {1}
    assert {str(row["data_compression_desc"]) for row in rows} == {"NONE"}
    keys = tuple(
        str(row["column_name"]) for row in rows if int(row["key_ordinal"]) > 0 and not bool(row["is_included_column"])
    )
    assert keys == key_columns


def _drop_partition_objects(
    connector: Any,
    objects: list[tuple[str, str]],
) -> None:
    for object_type, object_name in objects:
        catalog = "sys.partition_schemes" if object_type == "SCHEME" else "sys.partition_functions"
        exists = connector.get_records(
            f"SELECT COUNT_BIG(*) AS object_count FROM {catalog} WHERE name = ?",
            (object_name,),
            as_dict=True,
        )
        if int(exists[0]["object_count"]):
            connector.execute_query(f"DROP PARTITION {object_type} [{object_name}]")


def _catalog(connector: Any, table: str) -> dict[str, Any]:
    row = connector.get_records(
        """
        SELECT
            MAX(CASE WHEN p.index_id IN (0, 1) THEN UPPER(p.data_compression_desc) END) AS compression,
            MAX(ds.name) AS data_space,
            MAX(lob.name) AS lob_data_space,
            SUM(CASE WHEN i.type = 5 THEN 1 ELSE 0 END) AS columnstore_count
        FROM sys.tables AS t
        INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
        LEFT JOIN sys.indexes AS i ON i.object_id = t.object_id
        LEFT JOIN sys.partitions AS p ON p.object_id = i.object_id AND p.index_id = i.index_id
        LEFT JOIN sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id
        LEFT JOIN sys.data_spaces AS lob ON lob.data_space_id = t.lob_data_space_id
        WHERE s.name = N'dpone_it' AND t.name = ?
        GROUP BY t.object_id
        """,
        (table,),
        as_dict=True,
    )[0]
    indexes = connector.get_records(
        """
        SELECT i.is_unique, i.is_primary_key, i.type_desc, i.fill_factor
        FROM sys.indexes AS i
        INNER JOIN sys.tables AS t ON t.object_id = i.object_id
        INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id
        WHERE s.name = N'dpone_it' AND t.name = ? AND i.index_id > 0 AND i.type <> 5
        """,
        (table,),
        as_dict=True,
    )
    return {
        "compression": str(row["compression"] or "NONE").upper(),
        "data_space": row["data_space"],
        "lob_data_space": row["lob_data_space"],
        "columnstore_count": int(row["columnstore_count"] or 0),
        "key_index": indexes[0] if indexes else None,
    }


def _primary_key_catalog(connector: Any, table: str) -> list[dict[str, object]]:
    return list(
        connector.get_records(
            "SELECT c.name AS column_name, ic.key_ordinal, c.is_nullable, ty.name AS type_name, "
            "c.max_length, i.type_desc, i.is_primary_key, i.is_unique "
            "FROM sys.tables AS t INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
            "INNER JOIN sys.indexes AS i ON i.object_id = t.object_id AND i.is_primary_key = 1 "
            "INNER JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id "
            "INNER JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
            "INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id "
            "WHERE s.name = N'dpone_it' AND t.name = ? ORDER BY ic.key_ordinal",
            (table,),
            as_dict=True,
        )
    )


def _object_count(connector: Any, table: str) -> int:
    rows = connector.get_records(
        "SELECT COUNT_BIG(*) AS object_count FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = N'dpone_it' AND t.name = ?",
        (table,),
        as_dict=True,
    )
    return int(rows[0]["object_count"])


def _staging_count(connector: Any, table: str) -> int:
    rows = connector.get_records(
        "SELECT COUNT_BIG(*) AS object_count FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = N'staging' AND t.name LIKE ?",
        (f"stg_{table}_%",),
        as_dict=True,
    )
    return int(rows[0]["object_count"])


def _create_compressed_table(connector: Any, table: str, compression: str) -> None:
    normalized = compression.upper()
    assert normalized in {"NONE", "ROW", "PAGE"}
    connector.execute_query(
        f"CREATE TABLE [dpone_it].[{table}] "
        "([id] bigint NOT NULL, [payload] nvarchar(100) NULL) "
        f"WITH (DATA_COMPRESSION = {normalized})"
    )
    connector.execute_query(f"INSERT INTO [dpone_it].[{table}] ([id], [payload]) VALUES (1, N'preserve-me')")


def _business_rows(connector: Any, table: str) -> list[tuple[object, ...]]:
    return list(connector.get_records(f"SELECT [id], [payload] FROM [dpone_it].[{table}] ORDER BY [id]"))


def _approval_label(approval: object) -> str:
    if approval is None:
        return "missing"
    assert isinstance(approval, dict)
    if approval.get("table"):
        return "table_mismatch"
    if approval.get("expires_at") == "2000-01-01T00:00:00Z":
        return "expired"
    if approval.get("approved_risks") != ["table_settings.compression"]:
        return "wrong_risk"
    return "exact"


def _reviewed_approval_label(approval: object) -> str:
    labels = {
        "missing": "absent",
        "table_mismatch": "wrong_table",
        "expired": "expired",
        "wrong_risk": "wrong_risk",
        "exact": "exact_live",
    }
    return labels[_approval_label(approval)]


def _drop_prefix(connector: Any, prefix: str) -> None:
    rows = connector.get_records(
        "SELECT t.name FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = N'dpone_it' AND t.name LIKE ?",
        (f"{prefix}%",),
        as_dict=True,
    )
    for row in rows:
        safe = str(row["name"]).replace("]", "]]")
        connector.execute_query(f"DROP TABLE [dpone_it].[{safe}]")


def _write_evidence(section: str, cases: list[dict[str, object]]) -> None:
    _EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload: dict[str, Any]
    if _EVIDENCE_PATH.exists():
        payload = json.loads(_EVIDENCE_PATH.read_text(encoding="utf-8"))
    else:
        payload = {
            "schema_version": "dpone.postgres_mssql.physical_design_matrix.v1",
            "status": "passed_partial",
            "release_ready": False,
            "vendor": "Microsoft SQL Server",
            "connector_doubles": False,
            "sections": {},
            "remaining_release_gates": [
                "production_compression_estimator_and_five_warm_benchmarks",
                "published_release_pin",
            ],
        }
    sections = payload.setdefault("sections", {})
    assert isinstance(sections, dict)
    sections[section] = {"passed": True, "cases": cases}
    _EVIDENCE_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
