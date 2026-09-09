"""PostgreSQL→MSSQL vendor-live schema-evolution capability matrix.

The suite executes the complete Cartesian product of the public governance
enums against a disposable SQL Server target.  Supported decisions execute
real DDL; every non-apply decision proves that both the vendor catalog and the
pre-existing business row remain unchanged.  Source schemas and values come
from disposable PostgreSQL tables, so this is a route test rather than a DDL
renderer test with connector doubles.
"""

from __future__ import annotations

import json
import uuid
from itertools import product
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.config import LoadConfig, LoadStrategy
from dpone.readiness.ddl_governance import DdlGovernancePolicy, OnlineSchemaPlanner
from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.schema_evolution import SchemaEvolutionError, SchemaEvolutionService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sinks.mssql_target_catalog_types import mssql_catalog_type_shape
from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
from dpone.type_system.source_sink.provenance import SourceRelationDialect
from tests.integration.postgres.postgres_live_support import (
    NoopLogger,
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
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    QuietIntegrationLogger,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
    pytest.mark.route_live_wide,
]

_SOURCE_SCHEMA = "dpone_src"
_TARGET_SCHEMA = "dpone_it"
_STAGING_SCHEMA = "staging"
_EVIDENCE_PATH = Path("test_artifacts/live_certification/postgres_mssql_schema_evolution_matrix.json")
_TABLE_MODES = ("evolve", "freeze", "ignore")
_COLUMN_MODES = ("evolve", "freeze", "ignore", "quarantine")
_DATA_TYPE_MODES = ("widen", "variant_column", "freeze", "quarantine")
_DDL_MODES = ("online", "safe_window", "plan_only", "manual_approval")
_CHANGE_BEHAVIORS = ("apply", "notify", "fail", "disable_pipeline")
_CHANGE_FAMILIES = ("add_column", "add_generated_column", "type_widen")


class _MssqlSinkWithoutRowCount:
    """Real-vendor sink facade used to prove an absent probe fails closed."""

    def __init__(self, delegate: MSSQLSink) -> None:
        self.connector = delegate.connector
        self._delegate = delegate

    def target_table_exists(self, load_config: LoadConfig) -> bool:
        return self._delegate.target_table_exists(load_config)

    def get_target_schema(self, load_config: LoadConfig) -> list[tuple[str, str]]:
        return self._delegate.get_target_schema(load_config)

    def get_target_columns(self, load_config: LoadConfig) -> list[Any]:
        return self._delegate.get_target_columns(load_config)


class _MssqlSinkUnavailableRowCount(MSSQLSink):
    def get_target_row_count(self, load_config: LoadConfig) -> int:
        del load_config
        raise RuntimeError("injected target count probe failure")


class _MssqlSinkInvalidRowCount(MSSQLSink):
    def get_target_row_count(self, load_config: LoadConfig) -> int:
        del load_config
        return -1


@pytest.fixture(scope="module", autouse=True)
def _discard_stale_evidence() -> None:
    """A previous interrupted run must never satisfy this certification."""

    _EVIDENCE_PATH.unlink(missing_ok=True)


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_complete_schema_governance_enum_cross_product_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Run 3 change families × all 1,536 governance/apply combinations."""

    postgres, mssql, prefix = _ready()
    sink = MSSQLSink(mssql, logger=NoopLogger())
    completed: list[dict[str, object]] = []
    applied_count = 0
    blocked_count = 0
    try:
        fixtures = {
            family: _create_source_fixture(postgres, prefix=prefix, family=family, tmp_path=tmp_path)
            for family in _CHANGE_FAMILIES
        }
        targets = {family: f"{prefix}_{family}" for family in _CHANGE_FAMILIES}
        for family, target in targets.items():
            _reset_target(mssql, target=target, family=family)
            baseline = _target_snapshot(mssql, target)
            config, payload = fixtures[family]
            config.target_table = target
            cases = product(_TABLE_MODES, _COLUMN_MODES, _DATA_TYPE_MODES, _DDL_MODES, _CHANGE_BEHAVIORS, (False, True))
            for family_case_index, (
                tables,
                columns,
                data_type,
                ddl_mode,
                on_schema_change,
                apply_safe,
            ) in enumerate(cases):
                schema_evolution: dict[str, object] = {
                    "tables": tables,
                    "columns": columns,
                    "data_type": data_type,
                    "ddl_mode": ddl_mode,
                    "on_schema_change": on_schema_change,
                    "apply_safe": apply_safe,
                }
                if family == "add_generated_column":
                    schema_evolution["on_type_change"] = "new_column"
                config.options = {**config.options, "schema_evolution": schema_evolution}
                expected = _expected_decision(
                    family=family,
                    tables=tables,
                    columns=columns,
                    data_type=data_type,
                    ddl_mode=ddl_mode,
                    on_schema_change=on_schema_change,
                    apply_safe=apply_safe,
                )
                if expected == "apply":
                    prepared = SchemaEvolutionService().prepare_payload(config, sink, payload)
                    _assert_applied_shape(mssql, target=target, family=family, prepared=prepared)
                    assert _business_rows(mssql, target=target, family=family) == baseline["rows"]
                    after = _target_snapshot(mssql, target)
                    applied_count += 1
                else:
                    with pytest.raises(SchemaEvolutionError) as exc_info:
                        SchemaEvolutionService().prepare_payload(config, sink, payload)
                    assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
                    assert "schema_evolution." in str(exc_info.value)
                    after = _target_snapshot(mssql, target)
                    blocked_count += 1
                parameters = {
                    "target_state": "existing",
                    "change_family": family,
                    "tables": tables,
                    "columns": columns,
                    "data_type": data_type,
                    "ddl_mode": ddl_mode,
                    "on_schema_change": on_schema_change,
                    "apply_safe": apply_safe,
                }
                route_live_recorder.observe_parameters(
                    "schema_evolution",
                    parameters,
                    before_image=baseline,
                    after_image=after,
                    observations={"decision": expected, "staging_objects_after": 0},
                )
                if expected == "apply":
                    _reset_target(mssql, target=target, family=family)
                if family_case_index % 128 == 127:
                    assert _target_snapshot(mssql, target) == baseline
                completed.append(
                    {
                        "family": family,
                        "tables": tables,
                        "columns": columns,
                        "data_type": data_type,
                        "ddl_mode": ddl_mode,
                        "on_schema_change": on_schema_change,
                        "apply_safe": apply_safe,
                        "decision": expected,
                    }
                )
            assert _target_snapshot(mssql, target) == baseline

        assert len(completed) == 4608
        assert applied_count > 0
        assert blocked_count > applied_count
        _write_evidence(
            "governance_enum_cross_product",
            {
                "passed": True,
                "case_count": len(completed),
                "applied": applied_count,
                "blocked_pre_mutation": blocked_count,
                "enum_domains": {
                    "tables": list(_TABLE_MODES),
                    "columns": list(_COLUMN_MODES),
                    "data_type": list(_DATA_TYPE_MODES),
                    "ddl_mode": list(_DDL_MODES),
                    "on_schema_change": list(_CHANGE_BEHAVIORS),
                    "apply_safe": [False, True],
                },
                "change_families": list(_CHANGE_FAMILIES),
            },
        )
    finally:
        _drop_prefix(postgres, mssql, prefix)
        postgres.close()
        mssql.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_missing_target_table_policy_cross_product_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Run the full missing-table policy product and load only authorized cases."""

    postgres, legacy_mssql, prefix = _ready()
    legacy_mssql.close()
    mssql = governed_mssql_live_campaign.target
    logger = QuietIntegrationLogger()
    completed: list[dict[str, object]] = []
    try:
        config, payload = _create_source_fixture(postgres, prefix=prefix, family="missing_table", tmp_path=tmp_path)
        config.target_database = governed_mssql_live_campaign.target_database
        config.staging_database = governed_mssql_live_campaign.target_database
        config.export_format = "csv"
        config.options = {
            **config.options,
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "work_dir": str(tmp_path),
        }
        cases = product(_TABLE_MODES, _DDL_MODES, _CHANGE_BEHAVIORS, (False, True))
        for ordinal, (tables, ddl_mode, on_schema_change, apply_safe) in enumerate(cases):
            target = f"{prefix}_missing_{ordinal:03d}"
            config.target_table = target
            config.options = {
                **config.options,
                "schema_evolution": {
                    "tables": tables,
                    "ddl_mode": ddl_mode,
                    "on_schema_change": on_schema_change,
                    "apply_safe": apply_safe,
                },
            }
            route = governed_mssql_live_campaign.route(
                target_schema=config.target_schema,
                target_table=config.target_table,
            )
            sink = route.sink(logger=logger)
            may_create = (
                tables == "evolve"
                and ddl_mode in {"online", "safe_window"}
                and on_schema_change == "apply"
                and apply_safe
            )
            assert _object_count(mssql, target) == 0
            before = {"object_count": 0, "staging_object_count": 0}
            if may_create:
                result = GovernedStandardEtlRunner(
                    route,
                    postgres,
                    logger=logger,
                    source_type=GovernedPostgresSnapshotSource,
                ).run(config, label=f"schema_missing_target_{ordinal:03d}")
                assert result["loaded_rows"] == 1
                assert _object_count(mssql, target) == 1
                rows = mssql.get_records(
                    f"SELECT [id], [payload] FROM [{_TARGET_SCHEMA}].[{target}]",
                    as_dict=True,
                )
                assert rows == [{"id": 1, "payload": "from-postgres"}]
                after = {
                    "object_count": _object_count(mssql, target),
                    "columns": _column_catalog(mssql, target),
                    "rows": rows,
                    "staging_object_count": _staging_object_count(mssql, target),
                }
                mssql.execute_query(f"DROP TABLE [{_TARGET_SCHEMA}].[{target}]")
            else:
                with pytest.raises(SchemaEvolutionError) as exc_info:
                    SchemaEvolutionService().prepare_payload(config, sink, payload)
                assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
                assert "create_table" in str(exc_info.value)
                assert _object_count(mssql, target) == 0
                assert _staging_object_count(mssql, target) == 0
                after = {"object_count": 0, "staging_object_count": 0}
            route_live_recorder.observe_parameters(
                "schema_evolution",
                {
                    "target_state": "missing",
                    "tables": tables,
                    "ddl_mode": ddl_mode,
                    "on_schema_change": on_schema_change,
                    "apply_safe": apply_safe,
                },
                before_image=before,
                after_image=after,
                observations={"created_and_loaded": may_create},
            )
            completed.append(
                {
                    "tables": tables,
                    "ddl_mode": ddl_mode,
                    "on_schema_change": on_schema_change,
                    "apply_safe": apply_safe,
                    "created_and_loaded": may_create,
                }
            )

        assert len(completed) == 96
        assert sum(bool(case["created_and_loaded"]) for case in completed) == 2
        _write_evidence(
            "missing_target_cross_product",
            {
                "passed": True,
                "case_count": len(completed),
                "created_and_loaded": 2,
                "blocked_pre_mutation": 94,
            },
        )
    finally:
        _drop_prefix(postgres, mssql, prefix)
        postgres.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_schema_evolution_runtime_boundary_parameters_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Prove size/timeout parameters are enforced instead of silently ignored."""

    postgres, mssql, prefix = _ready()
    sink = MSSQLSink(mssql, logger=NoopLogger())
    target = f"{prefix}_add_column"
    try:
        config, payload = _create_source_fixture(postgres, prefix=prefix, family="add_column", tmp_path=tmp_path)
        config.target_table = target

        _reset_target(mssql, target=target, family="add_column")
        before = _target_snapshot(mssql, target)
        config.options = {
            **config.options,
            "schema_evolution": {"max_table_size_for_inline_ddl": 0},
        }
        with pytest.raises(SchemaEvolutionError) as exc_info:
            SchemaEvolutionService().prepare_payload(config, sink, payload)
        assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
        assert "table_size_budget" in str(exc_info.value)
        after = _target_snapshot(mssql, target)
        assert after == before
        route_live_recorder.observe_case(
            "schema_evolution",
            "runtime__table_size_budget_exceeded",
            before_image=before,
            after_image=after,
            observations={"blocker": "table_size_budget"},
        )

        for guarded_sink, blocker in (
            (_MssqlSinkWithoutRowCount(sink), "table_size_unknown"),
            (_MssqlSinkUnavailableRowCount(mssql, logger=NoopLogger()), "table_size_unavailable"),
            (_MssqlSinkInvalidRowCount(mssql, logger=NoopLogger()), "table_size_invalid"),
        ):
            config.options = {
                **config.options,
                "schema_evolution": {"max_table_size_for_inline_ddl": 1},
            }
            with pytest.raises(SchemaEvolutionError) as exc_info:
                SchemaEvolutionService().prepare_payload(config, guarded_sink, payload)
            assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
            assert blocker in str(exc_info.value)
            after = _target_snapshot(mssql, target)
            assert after == before
            case_name = {
                "table_size_unknown": "runtime__table_size_probe_missing",
                "table_size_unavailable": "runtime__table_size_probe_failure",
                "table_size_invalid": "runtime__table_size_probe_invalid",
            }[blocker]
            route_live_recorder.observe_case(
                "schema_evolution",
                case_name,
                before_image=before,
                after_image=after,
                observations={"blocker": blocker},
            )

        config.options = {
            **config.options,
            "schema_evolution": {"max_table_size_for_inline_ddl": 1},
        }
        prepared = SchemaEvolutionService().prepare_payload(config, sink, payload)
        _assert_applied_shape(mssql, target=target, family="add_column", prepared=prepared)
        assert _business_rows(mssql, target=target, family="add_column") == before["rows"]
        route_live_recorder.observe_case(
            "schema_evolution",
            "runtime__table_size_budget_equal",
            before_image=before,
            after_image=_target_snapshot(mssql, target),
            observations={"maximum_rows": 1, "observed_rows": 1},
        )

        _reset_target(mssql, target=target, family="add_column")
        before = _target_snapshot(mssql, target)
        config.options = {
            **config.options,
            "schema_evolution": {"statement_timeout_seconds": 1},
        }
        with pytest.raises(SchemaEvolutionError) as exc_info:
            SchemaEvolutionService().prepare_payload(config, sink, payload)
        assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
        assert "statement_timeout" in str(exc_info.value)
        after = _target_snapshot(mssql, target)
        assert after == before
        route_live_recorder.observe_case(
            "schema_evolution",
            "runtime__mssql_statement_timeout",
            before_image=before,
            after_image=after,
            observations={"blocker": "unsupported_statement_timeout"},
        )

        config.options = {
            **config.options,
            "schema_evolution": {"lock_timeout_seconds": 1},
        }
        prepared = SchemaEvolutionService().prepare_payload(config, sink, payload)
        _assert_applied_shape(mssql, target=target, family="add_column", prepared=prepared)
        assert _business_rows(mssql, target=target, family="add_column") == before["rows"]
        route_live_recorder.observe_case(
            "schema_evolution",
            "runtime__mssql_lock_timeout",
            before_image=before,
            after_image=_target_snapshot(mssql, target),
            observations={"lock_timeout_seconds": 1},
        )

        _write_evidence(
            "runtime_boundary_parameters",
            {
                "passed": True,
                "cases": [
                    "table_size_budget_exceeded_blocks",
                    "table_size_budget_equal_applies",
                    "table_size_probe_missing_blocks",
                    "table_size_probe_failure_blocks",
                    "table_size_probe_invalid_blocks",
                    "mssql_statement_timeout_unsupported_blocks",
                    "mssql_lock_timeout_applies",
                ],
            },
        )
    finally:
        _drop_prefix(postgres, mssql, prefix)
        postgres.close()
        mssql.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_generated_column_executes_sink_and_reruns_idempotently_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Load through the retained predecessor after real generated-column DDL."""

    postgres, _legacy_mssql, prefix = _ready()
    _legacy_mssql.close()
    mssql = governed_mssql_live_campaign.target
    target = f"{prefix}_add_generated_column"
    try:
        config, _payload = _create_source_fixture(
            postgres,
            prefix=prefix,
            family="add_generated_column",
            tmp_path=tmp_path,
        )
        config.target_table = target
        config.target_database = governed_mssql_live_campaign.target_database
        config.staging_database = governed_mssql_live_campaign.target_database
        config.export_format = "csv"
        config.options = {
            **config.options,
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "work_dir": str(tmp_path),
            "schema_evolution": {
                "on_type_change": "new_column",
                "tables": "evolve",
                "columns": "evolve",
                "data_type": "variant_column",
                "ddl_mode": "online",
                "on_schema_change": "apply",
                "apply_safe": True,
            },
        }
        # Birth the target through the same governed standard-ETL route.  The
        # first reviewed observation begins only after this exact baseline,
        # so the generated-column case contains one isolated schema family.
        source = f"{prefix}_src_add_generated_column"
        postgres.execute_query(
            f'UPDATE "{_SOURCE_SCHEMA}"."{source}" SET "amount" = %s',
            ("42",),
        )
        postgres.execute_query(
            f'ALTER TABLE "{_SOURCE_SCHEMA}"."{source}" ALTER COLUMN "amount" TYPE integer USING "amount"::integer'
        )
        logger = QuietIntegrationLogger()
        route = governed_mssql_live_campaign.route(
            target_schema=config.target_schema,
            target_table=target,
        )
        runner = GovernedStandardEtlRunner(
            route,
            postgres,
            logger=logger,
            source_type=GovernedPostgresSnapshotSource,
        )
        baseline = runner.run(config, label="generated_column_baseline")
        assert baseline["status"] == "success"
        postgres.execute_query(
            f'ALTER TABLE "{_SOURCE_SCHEMA}"."{source}" ALTER COLUMN "amount" TYPE text USING "amount"::text'
        )
        postgres.execute_query(
            f'UPDATE "{_SOURCE_SCHEMA}"."{source}" SET "amount" = %s',
            ("42-text",),
        )
        before_first = {
            "catalog": _column_catalog(mssql, target),
            "rows": mssql.get_records(
                f"SELECT [amount] FROM [{_TARGET_SCHEMA}].[{target}]",
                as_dict=True,
            ),
        }

        first = runner.run(config, label="generated_column_first")
        first_catalog = _generated_business_catalog(mssql, target)
        first_rows = mssql.get_records(
            f"SELECT [amount], [__dpone__nc__amount] FROM [{_TARGET_SCHEMA}].[{target}]",
            as_dict=True,
        )
        second = runner.run(config, label="generated_column_rerun")
        second_catalog = _generated_business_catalog(mssql, target)
        second_rows = mssql.get_records(
            f"SELECT [amount], [__dpone__nc__amount] FROM [{_TARGET_SCHEMA}].[{target}]",
            as_dict=True,
        )

        assert first["status"] == second["status"] == "success"
        assert (
            first_catalog
            == second_catalog
            == {
                "amount": "int",
                "__dpone__nc__amount": "nvarchar(max)",
            }
        )
        assert (
            first_rows
            == second_rows
            == [
                {"amount": None, "__dpone__nc__amount": "42-text"},
            ]
        )
        assert _staging_object_count(mssql, target) == 0
        first_image = {"catalog": first_catalog, "rows": first_rows}
        second_image = {"catalog": second_catalog, "rows": second_rows}
        route_live_recorder.observe_case(
            "schema_evolution",
            "generated_column__first_standard_etl",
            before_image=before_first,
            after_image=first_image,
            observations={"loaded_rows": 1, "staging_objects_after": 0},
        )
        mssql.execute_query(f"ALTER TABLE [{_TARGET_SCHEMA}].[{target}] ADD [unrelated_extra] nvarchar(10) NULL")
        drift_before = {
            "catalog": _column_catalog(mssql, target),
            "rows": second_rows,
        }
        with pytest.raises(SchemaEvolutionError) as exc_info:
            runner.run(config, label="generated_column_unrelated_extra")
        assert exc_info.value.code == "DPONE_SCHEMA_EVOLUTION_BLOCKED"
        assert "drop_column" in str(exc_info.value)
        drift_after = {
            "catalog": _column_catalog(mssql, target),
            "rows": mssql.get_records(
                f"SELECT [amount], [__dpone__nc__amount] FROM [{_TARGET_SCHEMA}].[{target}]",
                as_dict=True,
            ),
        }
        assert drift_after == drift_before
        assert _staging_object_count(mssql, target) == 0
        route_live_recorder.observe_case(
            "schema_evolution",
            "generated_column__unrelated_extra_rejected",
            before_image=drift_before,
            after_image=drift_after,
            observations={
                "blocker": "drop_column",
                "blocked_before_source_copy": True,
                "staging_objects_after": 0,
            },
        )
        route_live_recorder.observe_case(
            "schema_evolution",
            "generated_column__idempotent_rerun",
            before_image=first_image,
            after_image=second_image,
            observations={"loaded_rows": 1, "staging_objects_after": 0},
        )
        _write_evidence(
            "generated_column_sink_lifecycle",
            {
                "passed": True,
                "connector_doubles": False,
                "first_load_rows": 1,
                "idempotent_rerun_rows": 1,
                "retained_predecessor": "amount",
                "managed_companion": "__dpone__nc__amount",
            },
        )
    finally:
        _drop_prefix(postgres, mssql, prefix)
        postgres.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_mssql_temporal_catalog_add_alter_scale_matrix_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Apply all temporal add/alter scales and compare exact ``sys.columns``.

    Every cell is a real PostgreSQL snapshot followed by governed SQL Server
    admission, schema DDL, business mutation, catalog verification, receipt,
    and staging cleanup.  The ``alter`` cells isolate a nullability relaxation
    while retaining the reviewed temporal type/scale, including scale zero.
    """

    postgres = postgres_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=_SOURCE_SCHEMA)
    mssql = governed_mssql_live_campaign.target
    suffix = uuid.uuid4().hex[:8]
    logger = QuietIntegrationLogger()
    try:
        for target_family, scale, operation in product(
            ("time", "datetime2", "datetimeoffset"),
            range(8),
            ("add", "alter"),
        ):
            target_type = f"{target_family}({scale})"
            token = f"temporal_{operation}_{target_family}_{scale}_{suffix}"
            source_table = f"se_{token}_src"
            target_table = f"se_{token}_tgt"
            postgres.execute_query(f'DROP TABLE IF EXISTS "{_SOURCE_SCHEMA}"."{source_table}" CASCADE')
            source_type, literal = _postgres_temporal_fixture(target_family, scale)
            if operation == "add":
                postgres.execute_query(f'CREATE TABLE "{_SOURCE_SCHEMA}"."{source_table}" ("id" integer NOT NULL)')
                postgres.execute_query(f'INSERT INTO "{_SOURCE_SCHEMA}"."{source_table}" ("id") VALUES (1)')
            else:
                postgres.execute_query(
                    f'CREATE TABLE "{_SOURCE_SCHEMA}"."{source_table}" '
                    f'("id" integer NOT NULL, "temporal_value" {source_type} NOT NULL)'
                )
                postgres.execute_query(
                    f'INSERT INTO "{_SOURCE_SCHEMA}"."{source_table}" ("id", "temporal_value") VALUES (1, {literal})'
                )
            config = _temporal_load_config(
                source_table=source_table,
                target_table=target_table,
                target_database=governed_mssql_live_campaign.target_database,
                tmp_path=tmp_path,
                target_type=target_type if operation == "alter" else None,
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
            baseline = runner.run(config, label=f"{token}_baseline")
            assert baseline["status"] == "success"
            if operation == "add":
                postgres.execute_query(
                    f'ALTER TABLE "{_SOURCE_SCHEMA}"."{source_table}" ADD COLUMN "temporal_value" {source_type} NULL'
                )
                postgres.execute_query(f'UPDATE "{_SOURCE_SCHEMA}"."{source_table}" SET "temporal_value" = {literal}')
                config.options = {
                    **config.options,
                    "physical_design": _temporal_physical_design(target_type),
                }
            else:
                postgres.execute_query(
                    f'ALTER TABLE "{_SOURCE_SCHEMA}"."{source_table}" ALTER COLUMN "temporal_value" DROP NOT NULL'
                )
            before = _exact_column_catalog(mssql, target_table)
            result = runner.run(config, label=f"{token}_case")
            assert result["status"] == "success"
            after = _exact_column_catalog(mssql, target_table)
            actual = next(column for column in after if column["name"] == "temporal_value")
            expected = _expected_temporal_catalog_shape(target_type, nullable=True)
            assert {key: actual[key] for key in expected} == expected
            assert _staging_object_count(mssql, target_table) == 0
            route_live_recorder.observe_case(
                "schema_evolution",
                f"temporal_catalog__{operation}__{target_family}_{scale}",
                before_image={"columns": before},
                after_image={"columns": after},
                observations={
                    "predicted_expected_after": expected,
                    "actual_sys_columns": {key: actual[key] for key in expected},
                    "loaded_rows": int(result["loaded_rows"]),
                    "staging_objects_after": 0,
                },
            )
            mssql.execute_query(f"DROP TABLE [{_TARGET_SCHEMA}].[{target_table}]")
            postgres.execute_query(f'DROP TABLE "{_SOURCE_SCHEMA}"."{source_table}"')
        _write_evidence(
            "temporal_catalog_matrix",
            {
                "passed": True,
                "case_count": 48,
                "operations": ["add", "alter"],
                "target_families": ["time", "datetime2", "datetimeoffset"],
                "scales": list(range(8)),
            },
        )
    finally:
        _drop_prefix(postgres, mssql, "se_temporal_")
        postgres.close()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_zz_postgres_mssql_schema_evolution_evidence_is_complete() -> None:
    """Reject a partial run even when an earlier matrix section passed."""

    payload = json.loads(_EVIDENCE_PATH.read_text(encoding="utf-8"))
    assert payload["status"] == "passed_partial"
    assert payload["release_ready"] is False
    assert set(payload["sections"]) == {
        "governance_enum_cross_product",
        "generated_column_sink_lifecycle",
        "missing_target_cross_product",
        "runtime_boundary_parameters",
        "temporal_catalog_matrix",
    }


def _ready() -> tuple[Any, Any, str]:
    postgres = postgres_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=_SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas(target_schema=_TARGET_SCHEMA, staging_schema=_STAGING_SCHEMA)
    mssql = mssql_connector()
    wait_until_ready("mssql", lambda: mssql.get_records("SELECT 1"))
    return postgres, mssql, f"se_{uuid.uuid4().hex[:8]}"


def _create_source_fixture(
    postgres: Any,
    *,
    prefix: str,
    family: str,
    tmp_path: Path,
) -> tuple[LoadConfig, LoadPayload]:
    table = f"{prefix}_src_{family}"
    definitions = {
        "add_column": '"id" bigint NOT NULL, "new_value" text NULL',
        "add_generated_column": '"amount" text NULL',
        "type_widen": '"id" bigint NOT NULL',
        "missing_table": '"id" bigint NOT NULL, "payload" text NULL',
    }
    inserts = {
        "add_column": "VALUES (1, 'new-from-postgres')",
        "add_generated_column": "VALUES ('42-text')",
        "type_widen": "VALUES (1)",
        "missing_table": "VALUES (1, 'from-postgres')",
    }
    postgres.execute_query(f'DROP TABLE IF EXISTS "{_SOURCE_SCHEMA}"."{table}" CASCADE')
    postgres.execute_query(f'CREATE TABLE "{_SOURCE_SCHEMA}"."{table}" ({definitions[family]})')
    postgres.execute_query(f'INSERT INTO "{_SOURCE_SCHEMA}"."{table}" {inserts[family]}')
    config = LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_target",
        source_schema=_SOURCE_SCHEMA,
        source_table=table,
        target_schema=_TARGET_SCHEMA,
        target_table=table,
        staging_schema=_STAGING_SCHEMA,
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="mssql-delimited",
        options={
            "sink_type": "mssql",
            "partition_tmp_dir": str(tmp_path),
            "technical_columns": "forbidden",
        },
    )
    strategy = PostgresFullExtractStrategy(postgres, logger=NoopLogger())
    projection = strategy.fetch_schema_projection(config)
    rows = postgres.get_records(
        f'SELECT * FROM "{_SOURCE_SCHEMA}"."{table}" ORDER BY 1',
        as_dict=True,
    )
    return config, LoadPayload(
        artifact=InMemoryRowsArtifact(rows),
        schema=projection.projected_schema,
        relation_schema=projection.relation_schema,
        relation_metadata=projection.relation_metadata,
        relation_dialect=SourceRelationDialect.POSTGRES,
        target_projection=projection.target_projection,
    )


def _reset_target(mssql: Any, *, target: str, family: str) -> None:
    mssql.execute_query(f"DROP TABLE IF EXISTS [{_TARGET_SCHEMA}].[{target}]")
    definitions = {
        "add_column": "[id] bigint NOT NULL",
        "add_generated_column": "[amount] int NULL",
        "type_widen": "[id] int NOT NULL",
    }
    inserts = {
        "add_column": "([id]) VALUES (900)",
        "add_generated_column": "([amount]) VALUES (7)",
        "type_widen": "([id]) VALUES (900)",
    }
    mssql.execute_query(f"CREATE TABLE [{_TARGET_SCHEMA}].[{target}] ({definitions[family]})")
    mssql.execute_query(f"INSERT INTO [{_TARGET_SCHEMA}].[{target}] {inserts[family]}")


def _expected_decision(
    *,
    family: str,
    tables: str,
    columns: str,
    data_type: str,
    ddl_mode: str,
    on_schema_change: str,
    apply_safe: bool,
) -> str:
    policy = DdlGovernancePolicy(
        tables=tables,
        columns=columns,
        data_type=data_type,
        ddl_mode=ddl_mode,
        on_schema_change=on_schema_change,
    )
    governed = OnlineSchemaPlanner().plan(
        schema_plan=_isolated_family_plan(family),
        dialect="mssql",
        table="dpone_it.route_live_schema_case",
        policy=policy,
        table_row_count=1,
    )
    assert len(governed.actions) == 1
    decision = governed.actions[0].decision
    if decision == "apply" and not apply_safe:
        return "apply_safe_block"
    return decision


def _isolated_family_plan(family: str):
    if family == "add_column":
        return SchemaComparator(SchemaEvolutionPolicy()).compare(
            source=[ColumnDef("id", "bigint", False), ColumnDef("new_value", "nvarchar(max)")],
            target=[ColumnDef("id", "bigint", False)],
        )
    if family == "add_generated_column":
        return SchemaComparator(SchemaEvolutionPolicy(on_type_change="new_column")).compare(
            source=[ColumnDef("amount", "nvarchar(max)")],
            target=[ColumnDef("amount", "int")],
        )
    if family == "type_widen":
        return SchemaComparator(SchemaEvolutionPolicy()).compare(
            source=[ColumnDef("id", "bigint", False)],
            target=[ColumnDef("id", "int", False)],
        )
    raise AssertionError(f"unsupported schema family: {family}")


def _assert_applied_shape(mssql: Any, *, target: str, family: str, prepared: LoadPayload) -> None:
    columns = _column_catalog(mssql, target)
    if family == "add_column":
        assert columns["new_value"] == "nvarchar(max)"
        assert [name for name, _dtype in prepared.schema] == ["id", "new_value"]
    elif family == "add_generated_column":
        assert columns["__dpone__nc__amount"] == "nvarchar(max)"
        assert list(prepared.schema) == [("amount", "nvarchar(max)")]
        assert list(prepared.artifact._rows) == [{"amount": "42-text"}]
        assert prepared.target_projection is not None
        assert prepared.target_projection.target_projected_schema == (("__dpone__nc__amount", "nvarchar(max)"),)
        assert tuple(
            (column.name, column.target_type, column.nullable)
            for column in prepared.target_projection.retained_target_columns
        ) == (("amount", "int", True),)
    elif family == "type_widen":
        assert columns["id"] == "bigint"
    else:  # pragma: no cover - test authoring guard
        raise AssertionError(f"unknown schema family: {family}")


def _target_snapshot(mssql: Any, target: str) -> dict[str, object]:
    return {
        "columns": _column_catalog(mssql, target),
        "rows": _business_rows(mssql, target=target, family=_family_from_target(target)),
    }


def _family_from_target(target: str) -> str:
    for family in _CHANGE_FAMILIES:
        if target.endswith(f"_{family}"):
            return family
    raise AssertionError(f"target does not encode a known family: {target}")


def _column_catalog(mssql: Any, target: str) -> dict[str, str]:
    rows = mssql.get_records(
        "SELECT c.name, ty.name AS type_name, c.max_length "
        "FROM sys.columns AS c "
        "INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id "
        "INNER JOIN sys.tables AS t ON t.object_id = c.object_id "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name = ? ORDER BY c.column_id",
        (_TARGET_SCHEMA, target),
        as_dict=True,
    )
    return {str(row["name"]): _render_catalog_type(str(row["type_name"]), int(row["max_length"])) for row in rows}


def _generated_business_catalog(mssql: Any, target: str) -> dict[str, str]:
    catalog = _column_catalog(mssql, target)
    return {name: catalog[name] for name in ("amount", "__dpone__nc__amount") if name in catalog}


def _temporal_load_config(
    *,
    source_table: str,
    target_table: str,
    target_database: str,
    tmp_path: Path,
    target_type: str | None,
) -> LoadConfig:
    options: dict[str, object] = {
        "source_type": "postgres",
        "sink_type": "mssql",
        "batch_commit_mode": "whole",
        "partition_tmp_dir": str(tmp_path),
        "technical_columns": "required",
        "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000}},
        "schema_evolution": {
            "tables": "evolve",
            "columns": "evolve",
            "data_type": "widen",
            "ddl_mode": "safe_window",
            "on_schema_change": "apply",
            "apply_safe": True,
        },
    }
    if target_type is not None:
        options["physical_design"] = _temporal_physical_design(target_type)
    return LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_target",
        source_schema=_SOURCE_SCHEMA,
        source_table=source_table,
        target_schema=_TARGET_SCHEMA,
        target_table=target_table,
        target_database=target_database,
        staging_schema=_STAGING_SCHEMA,
        staging_database=target_database,
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="csv",
        compress_export=False,
        options=options,
    )


def _temporal_physical_design(target_type: str) -> dict[str, object]:
    return {
        "mode": "explicit",
        "apply": "online",
        "apply_runtime": True,
        "columns": {
            "temporal_value": {
                "target_type": {"mssql": target_type},
            }
        },
    }


def _postgres_temporal_fixture(target_family: str, target_scale: int) -> tuple[str, str]:
    source_scale = min(target_scale, 6)
    fraction = "" if source_scale == 0 else f".{('123456'[:source_scale])}"
    fixtures = {
        "time": (f"time({source_scale})", f"TIME '12:34:56{fraction}'"),
        "datetime2": (
            f"timestamp({source_scale})",
            f"TIMESTAMP '2026-08-16 12:34:56{fraction}'",
        ),
        "datetimeoffset": (
            f"timestamp({source_scale}) with time zone",
            f"TIMESTAMP WITH TIME ZONE '2026-08-16 12:34:56{fraction}+03:00'",
        ),
    }
    return fixtures[target_family]


def _exact_column_catalog(mssql: Any, target: str) -> list[dict[str, object]]:
    rows = mssql.get_records(
        "SELECT c.column_id AS ordinal, c.name, ty.name AS type_name, "
        "c.max_length, c.precision, c.scale, c.is_nullable "
        "FROM sys.columns AS c "
        "INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id "
        "INNER JOIN sys.tables AS t ON t.object_id = c.object_id "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name = ? ORDER BY c.column_id",
        (_TARGET_SCHEMA, target),
        as_dict=True,
    )
    return [
        {
            "ordinal": int(row["ordinal"]),
            "name": str(row["name"]),
            "type_name": str(row["type_name"]).lower(),
            "max_length": int(row["max_length"]),
            "precision": int(row["precision"]),
            "scale": int(row["scale"]),
            "nullable": bool(row["is_nullable"]),
        }
        for row in rows
    ]


def _expected_temporal_catalog_shape(target_type: str, *, nullable: bool) -> dict[str, object]:
    type_name, max_length, precision, scale = mssql_catalog_type_shape(target_type)
    return {
        "type_name": type_name,
        "max_length": max_length,
        "precision": precision,
        "scale": scale,
        "nullable": nullable,
    }


def _render_catalog_type(type_name: str, max_length: int) -> str:
    normalized = type_name.lower()
    if normalized in {"nvarchar", "nchar"}:
        length = "max" if max_length == -1 else str(max_length // 2)
        return f"{normalized}({length})"
    if normalized in {"varchar", "char", "varbinary", "binary"}:
        length = "max" if max_length == -1 else str(max_length)
        return f"{normalized}({length})"
    return normalized


def _business_rows(mssql: Any, *, target: str, family: str) -> list[tuple[object, ...]]:
    column = "amount" if family == "add_generated_column" else "id"
    return list(mssql.get_records(f"SELECT [{column}] FROM [{_TARGET_SCHEMA}].[{target}] ORDER BY [{column}]"))


def _object_count(mssql: Any, target: str) -> int:
    rows = mssql.get_records(
        "SELECT COUNT_BIG(*) AS object_count FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name = ?",
        (_TARGET_SCHEMA, target),
        as_dict=True,
    )
    return int(rows[0]["object_count"])


def _staging_object_count(mssql: Any, target: str) -> int:
    rows = mssql.get_records(
        "SELECT COUNT_BIG(*) AS object_count FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name LIKE ?",
        (_STAGING_SCHEMA, f"stg_{target}_%"),
        as_dict=True,
    )
    return int(rows[0]["object_count"])


def _drop_prefix(postgres: Any, mssql: Any, prefix: str) -> None:
    pg_tables = postgres.get_records(
        "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = %s AND tablename LIKE %s",
        (_SOURCE_SCHEMA, f"{prefix}%"),
        as_dict=True,
    )
    for row in pg_tables:
        safe = str(row["tablename"]).replace('"', '""')
        postgres.execute_query(f'DROP TABLE IF EXISTS "{_SOURCE_SCHEMA}"."{safe}" CASCADE')
    target_tables = mssql.get_records(
        "SELECT t.name FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name IN (?, ?) AND t.name LIKE ?",
        (_TARGET_SCHEMA, _STAGING_SCHEMA, f"{prefix}%"),
        as_dict=True,
    )
    for row in target_tables:
        safe = str(row["name"]).replace("]", "]]")
        for schema in (_TARGET_SCHEMA, _STAGING_SCHEMA):
            mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{safe}]")


def _write_evidence(section: str, value: dict[str, object]) -> None:
    _EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if _EVIDENCE_PATH.exists():
        payload = json.loads(_EVIDENCE_PATH.read_text(encoding="utf-8"))
    else:
        payload = {
            "schema_version": "dpone.postgres_mssql.schema_evolution_matrix.v1",
            "status": "passed_partial",
            "release_ready": False,
            "source_vendor": "PostgreSQL",
            "sink_vendor": "Microsoft SQL Server",
            "connector_doubles": False,
            "sections": {},
            "remaining_release_gates": ["published_release_pin", "production_soak"],
        }
    sections = payload.setdefault("sections", {})
    assert isinstance(sections, dict)
    sections[section] = value
    _EVIDENCE_PATH.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
