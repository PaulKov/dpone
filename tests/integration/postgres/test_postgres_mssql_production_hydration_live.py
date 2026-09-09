"""Real production composition proof for generic PostgreSQL→MSSQL state.

The test intentionally starts at the public CLI and strict runtime context,
not at connector or state-storage constructors.  It proves that a stateless
source strategy still receives the mandatory four-object target-atomic MSSQL
governance catalog, and that no XMin/run/audit tables are silently required.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.contracts.run_context import RunContext
from dpone.readiness.physical_state import PhysicalTableState
from dpone.runtime.bootstrap_hydrator import DefaultRuntimeHydrator
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from dpone.runtime.sinks.mssql_physical_introspection import MssqlPhysicalIntrospector
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import (
    aggregate_expectations,
    exact_schema_catalog_transition,
    physical_catalog_expectation,
)
from dpone.runtime.sinks.mssql_target_catalog_model import MssqlTableBehaviorState
from dpone.runtime.sinks.mssql_target_catalog_reader import read_schema_catalog_snapshot
from dpone.runtime.state.mssql_generic_transaction_contract import (
    require_generic_transaction_catalog,
)
from dpone.runtime.state.mssql_generic_transaction_names import GENERIC_TRANSACTION_TABLES
from dpone.runtime.state.mssql_generic_transaction_storage import (
    MssqlGenericTransactionStateStorage,
)
from dpone.runtime.state.mssql_route_preflight import MssqlSessionIdentity
from dpone.runtime.state.mssql_target_identity import resolve_mssql_physical_target_identity
from tests.integration.postgres.postgres_live_support import postgres_mssql_enabled
from tests.integration.postgres.postgres_mssql_production_hydration_live_support import (
    STATE_SCHEMA,
    TARGET_SCHEMA,
    close_runtime_bindings,
    database_identity,
    generic_receipt_readback,
    generic_state_row_counts,
    production_hydration_live_fixture,
    state_user_tables,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
]


@pytest.mark.skipif(
    not postgres_mssql_enabled(),
    reason="PostgreSQL/MSSQL Docker integration is not configured",
)
def test_default_runtime_hydrator_commits_with_exact_generic_four_object_state_live(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Hydrate strict connections, commit once, and suppress exact replay."""

    with production_hydration_live_fixture(tmp_path) as live:
        for name, value in live.runtime_environment.items():
            monkeypatch.setenv(name, value)

        expected_state_objects = tuple(
            {"schema_name": STATE_SCHEMA, "table_name": table} for table in sorted(GENERIC_TRANSACTION_TABLES)
        )
        assert state_user_tables(live.state) == expected_state_objects
        assert generic_state_row_counts(live.state) == {table: 0 for table in GENERIC_TRANSACTION_TABLES}

        target_database = database_identity(live.target, live.target_database)
        state_database = database_identity(live.state, live.state_database)
        assert target_database["database_name"] == live.target_database
        assert state_database["database_name"] == live.state_database
        assert target_database["database_id"] != state_database["database_id"]

        load_config = live.load_config()
        bindings = None
        try:
            bindings = DefaultRuntimeHydrator().build(
                config=live.runtime_config,
                load_config=load_config,
            )
            storage = bindings.sink_obj.state_storage
            assert isinstance(storage, MssqlGenericTransactionStateStorage)
            assert bindings.source_obj.state_storage is storage
            assert storage.database == live.state_database
            assert storage.schema == STATE_SCHEMA
            assert storage.connector.database == live.state_database
            assert storage._transaction_connector is bindings.sink_obj.connector
            assert bindings.run_state_storage is None
            assert bindings.partition_checkpoint_store is None
            assert bindings.load_identity_service.audit_storage is None
            assert {str(receipt["connection_ref"]) for receipt in bindings.credential_resolution_receipts} == {
                "integration-postgres-source",
                "integration-mssql-target",
                "integration-mssql-state",
            }
            assert {str(receipt["resolver"]) for receipt in bindings.credential_resolution_receipts} == {"env_var"}

            # Runtime admission will execute the same complete contract.  This
            # explicit assertion also proves the externally installed CLI DDL
            # before the first business row can be read.
            require_generic_transaction_catalog(
                storage.connector,
                database=live.state_database,
                schema=STATE_SCHEMA,
            )
            before_catalog = read_schema_catalog_snapshot(bindings.sink_obj, load_config)
            assert before_catalog.exists is False

            target_identity = resolve_mssql_physical_target_identity(
                bindings.sink_obj.connector,
                session=MssqlSessionIdentity.read(bindings.sink_obj.connector),
                database=live.target_database,
                schema=TARGET_SCHEMA,
                table=live.target_table,
            )
            assert target_identity.object_id is None

            before_image = {
                "target_database": target_database,
                "state_database": state_database,
                "target_catalog": before_catalog.to_schema_dict(),
                "target_rows": [],
                "state_objects": expected_state_objects,
                "state_row_counts": generic_state_row_counts(live.state),
                "target_identity": target_identity.digest.hex(),
            }
            processor = ETLProcessor(
                source=bindings.source_obj,
                sink=bindings.sink_obj,
                etl_logger=bindings.etl_logger,
                run_state_storage=bindings.run_state_storage,
                load_identity_service=bindings.load_identity_service,
            )
            run_context = RunContext(
                run_id="production-hydration-live-invocation",
                config={
                    "pipeline_id": "production-hydration-live",
                    "task_id": "full-refresh",
                },
            )
            dag_id = "DAG__integration__postgres_mssql__production_hydration"

            committed = processor.run(
                load_config,
                run_context=run_context,
                dag_id=dag_id,
            )
            assert committed["status"] == "success"
            assert committed["commit_outcome"] == AtomicCommitOutcome.COMMITTED
            assert committed["loaded_rows"] == 3
            assert committed["final_rows"] == 3
            assert committed["commit_receipt_id"]
            assert committed["artifact_terminal"]["cleanup_succeeded"] is True

            target_rows = bindings.sink_obj.connector.get_records(
                f"SELECT [id], [metric_code], [metric_value], [note] "
                f"FROM [{live.target_database}].[{TARGET_SCHEMA}].[{live.target_table}] "
                "ORDER BY [id]",
                as_dict=True,
            )
            assert target_rows == [
                {"id": 1, "metric_code": "requests.total", "metric_value": 12.5, "note": "first"},
                {"id": 2, "metric_code": "errors.total", "metric_value": 0.0, "note": None},
                {"id": 3, "metric_code": "latency.p99", "metric_value": 0.125, "note": "Привет Ω"},
            ]
            assert not live.transfer_root.exists() or not tuple(live.transfer_root.iterdir())

            after_catalog = read_schema_catalog_snapshot(bindings.sink_obj, load_config)
            assert after_catalog.exists is True
            assert tuple(column.name for column in after_catalog.columns) == (
                "id",
                "metric_code",
                "metric_value",
                "note",
            )
            assert after_catalog.checks == ()
            assert after_catalog.indexes == ()
            assert after_catalog.foreign_keys == ()
            assert after_catalog.triggers == ()
            assert after_catalog.behavior == MssqlTableBehaviorState.ordinary_disk_table()

            receipt = generic_receipt_readback(live.state)
            assert receipt["receipt_id"] == committed["commit_receipt_id"]
            assert receipt["target_database"] == live.target_database
            assert receipt["target_schema"] == TARGET_SCHEMA
            assert receipt["target_table"] == live.target_table
            assert receipt["strategy"] == "full_refresh"
            assert bytes(receipt["target_identity"]) == target_identity.digest
            assert receipt["declared_rows"] == 3
            assert receipt["actual_raw_rows"] == 3
            assert receipt["actual_native_rows"] == 3
            assert receipt["staging_rows"] == 3
            assert receipt["total_rows"] == 3
            assert receipt["current_generation"] == receipt["generation"] == 1
            assert bytes(receipt["current_attempt_key"]) == bytes(receipt["attempt_key"])
            assert bytes(receipt["current_route_fingerprint"]) == bytes(receipt["route_fingerprint"])

            schema_expectation = exact_schema_catalog_transition(
                before_catalog,
                after_catalog,
            )
            assert schema_expectation.representation == "exact_sys_catalog_v5"
            physical_after = MssqlPhysicalIntrospector(bindings.sink_obj.connector).inspect(load_config)
            physical_expectation = physical_catalog_expectation(
                PhysicalTableState(
                    sink_type="mssql",
                    table=physical_after.table,
                ),
                expected_after=physical_after,
            )
            expectations = (schema_expectation, physical_expectation)
            assert bytes(receipt["target_before_sha256"]) == aggregate_expectations(
                expectations,
                boundary="before",
            )
            assert bytes(receipt["target_after_sha256"]) == aggregate_expectations(
                expectations,
                boundary="after",
            )
            assert bytes(receipt["target_before_sha256"]) != bytes(receipt["target_after_sha256"])

            state_counts_after_commit = generic_state_row_counts(live.state)
            assert state_counts_after_commit == {table: 1 for table in GENERIC_TRANSACTION_TABLES}
            assert state_user_tables(live.state) == expected_state_objects

            replay = processor.run(
                load_config,
                run_context=run_context,
                dag_id=dag_id,
            )
            assert replay["status"] == "success"
            assert replay["commit_outcome"] == AtomicCommitOutcome.REPLAY_SUPPRESSED
            assert replay["commit_receipt_id"] == committed["commit_receipt_id"]
            assert replay["final_rows"] == 3
            assert generic_state_row_counts(live.state) == state_counts_after_commit
            assert state_user_tables(live.state) == expected_state_objects
            require_generic_transaction_catalog(
                storage.connector,
                database=live.state_database,
                schema=STATE_SCHEMA,
            )

            after_image = {
                "target_database": database_identity(
                    bindings.sink_obj.connector,
                    live.target_database,
                ),
                "state_database": database_identity(storage.connector, live.state_database),
                "target_catalog": after_catalog.to_schema_dict(),
                "target_physical": physical_after.to_dict(),
                "target_rows": target_rows,
                "state_objects": state_user_tables(live.state),
                "state_row_counts": state_counts_after_commit,
                "receipt": _jsonable_receipt(receipt),
                "replay": {
                    "commit_outcome": str(replay["commit_outcome"]),
                    "commit_receipt_id": replay["commit_receipt_id"],
                    "final_rows": replay["final_rows"],
                },
            }
            route_live_recorder.observe_case(
                "transaction_governance",
                "successful_atomic_commit",
                before_image=before_image,
                after_image=after_image,
                observations={
                    "runtime_hydrator": "DefaultRuntimeHydrator",
                    "strict_runtime_context": True,
                    "source_vendor": "PostgreSQL 16",
                    "target_vendor": "SQL Server 2022",
                    "target_and_state_distinct_database_ids": [
                        int(target_database["database_id"]),
                        int(state_database["database_id"]),
                    ],
                    "state_catalog_tables": list(GENERIC_TRANSACTION_TABLES),
                    "state_catalog_table_count": len(expected_state_objects),
                    "secondary_xmin_run_audit_objects": False,
                    "target_identity_sha256": target_identity.digest.hex(),
                    "catalog_representation": schema_expectation.representation,
                    "rendered_state_ddl_sha256": live.rendered_state_ddl_sha256,
                    "commit_receipt_id": committed["commit_receipt_id"],
                    "replay_outcome": str(replay["commit_outcome"]),
                },
            )
        finally:
            close_runtime_bindings(bindings)


def _jsonable_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    """Normalize the queried receipt for deterministic route evidence."""

    return {
        key: (bytes(value).hex() if isinstance(value, bytes | bytearray) else value) for key, value in receipt.items()
    }
