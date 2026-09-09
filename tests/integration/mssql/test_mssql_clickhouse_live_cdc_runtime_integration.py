from __future__ import annotations

import json
import os
import time
import uuid
from decimal import Decimal
from pathlib import Path

import pytest

from dpone.ops.cdc.schema_apply import CdcSchemaEvolutionApplyService
from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc.base import CDCBatch, CDCChange, CDCOperation
from dpone.runtime.cdc.compare import CdcCompareRepairService
from dpone.runtime.cdc.compare_readers import ClickHouseCdcLogCompareReader, MssqlCdcCompareReader
from dpone.runtime.cdc.live_adapters import cdc_event_row
from dpone.runtime.cdc.live_factory import CdcRuntimeLiveAdapterFactory
from dpone.runtime.cdc.materialization import (
    ClickHouseCdcMaterializationPlan,
    ClickHouseCdcMaterializationPolicy,
    ClickHouseCdcMaterializationService,
)
from dpone.runtime.cdc.repair import CdcRepairExecutionService
from dpone.runtime.cdc.resync import CdcResyncExecutionService
from dpone.runtime.cdc.retention import CdcResyncPlanner, CdcRetentionGapService, CdcRetentionPolicy
from dpone.runtime.cdc.retention_probes import MssqlChangeTrackingRetentionProbe
from dpone.runtime.cdc.runtime_models import CdcRuntimePolicy, CdcRuntimeStream
from dpone.runtime.cdc.runtime_orchestrator import CdcRuntimeOrchestrator
from dpone.runtime.cdc.typed_materialization import (
    ClickHouseCdcTypedColumn,
    ClickHouseCdcTypedMaterializationPlan,
    ClickHouseCdcTypedMaterializationPolicy,
    ClickHouseCdcTypedMaterializationService,
    ClickHouseCdcTypedQualityPolicy,
)
from dpone.runtime.connectors.mssql import MSSQLConnector

pytestmark = [pytest.mark.integration, pytest.mark.integration_mssql, pytest.mark.integration_clickhouse]

if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled. Set DPONE_RUN_INTEGRATION=1 to enable them.", allow_module_level=True)

pytest.importorskip("pyodbc")
pytest.importorskip("clickhouse_driver")


def test_mssql_change_tracking_runtime_applies_to_clickhouse_and_commits_offsets(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    database = f"dpone_ct_{uuid.uuid4().hex[:8]}"
    schema = f"dbo_{uuid.uuid4().hex[:8]}"
    source_table = "orders"
    target_table = f"orders_cdc_{uuid.uuid4().hex[:8]}"
    state_schema = f"state_{uuid.uuid4().hex[:8]}"
    stream = CdcRuntimeStream(
        pipeline_name=f"orders-live-{uuid.uuid4().hex[:8]}",
        source="mssql",
        sink="clickhouse",
        backend=CDCBackend.MSSQL_CHANGE_TRACKING,
        source_schema=schema,
        source_table=source_table,
        target_dataset=f"{clickhouse_settings.database}.{target_table}",
        unique_key=("order_id",),
    )

    _create_mssql_database(database)
    mssql = _mssql_connector(database)
    factory = CdcRuntimeLiveAdapterFactory()
    reader = factory.mssql_reader(connector=mssql, stream=stream)
    offset_store = factory.mssql_offset_store(connector=mssql, schema=state_schema, table="cdc_offset")
    sink_applier = factory.clickhouse_sink_applier(connector=clickhouse_connector)
    orchestrator = CdcRuntimeOrchestrator()

    try:
        mssql.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        mssql.execute_query(
            f"""
            CREATE TABLE [{schema}].[{source_table}] (
                [order_id] int NOT NULL PRIMARY KEY,
                [status] nvarchar(50) NULL,
                [amount] decimal(18, 2) NULL
            )
            """
        )
        reader.setup()

        mssql.execute_query(
            f"INSERT INTO [{schema}].[{source_table}] ([order_id], [status], [amount]) VALUES (1, N'new', 10.50)"
        )
        insert_report = orchestrator.run_once(
            stream=stream,
            reader=reader,
            offset_store=offset_store,
            sink_applier=sink_applier,
            output_dir=tmp_path / "insert",
            policy=CdcRuntimePolicy(max_changes=100),
        )

        mssql.execute_query(f"UPDATE [{schema}].[{source_table}] SET [status] = N'paid' WHERE [order_id] = 1")
        update_report = orchestrator.run_once(
            stream=stream,
            reader=reader,
            offset_store=offset_store,
            sink_applier=sink_applier,
            output_dir=tmp_path / "update",
            policy=CdcRuntimePolicy(max_changes=100),
        )

        mssql.execute_query(f"DELETE FROM [{schema}].[{source_table}] WHERE [order_id] = 1")
        delete_report = orchestrator.run_once(
            stream=stream,
            reader=reader,
            offset_store=offset_store,
            sink_applier=sink_applier,
            output_dir=tmp_path / "delete",
            policy=CdcRuntimePolicy(max_changes=100),
        )

        assert insert_report.passed is True
        assert update_report.passed is True
        assert delete_report.passed is True
        assert insert_report.committed is True
        assert update_report.committed is True
        assert delete_report.committed is True
        assert delete_report.next_offset is not None
        assert offset_store.load_offset(stream) == delete_report.next_offset

        rows = clickhouse_connector.get_records(
            f"""
            SELECT
                dpone_cdc_operation,
                dpone_cdc_payload_json,
                dpone_cdc_deleted
            FROM `{clickhouse_settings.database}`.`{target_table}`
            ORDER BY dpone_cdc_position, dpone_cdc_event_hash
            """,
            as_dict=True,
        )
        assert [row["dpone_cdc_operation"] for row in rows] == ["insert", "update", "delete"]
        assert [row["dpone_cdc_deleted"] for row in rows] == [0, 0, 1]
        assert '"order_id":1' in rows[0]["dpone_cdc_payload_json"]
        assert '"status":"paid"' in rows[1]["dpone_cdc_payload_json"]

        replay_source = clickhouse_connector.get_records(
            f"""
            SELECT
                dpone_cdc_operation,
                dpone_cdc_position,
                dpone_cdc_sequence,
                dpone_cdc_payload_json,
                dpone_cdc_before_json
            FROM `{clickhouse_settings.database}`.`{target_table}`
            ORDER BY dpone_cdc_position, dpone_cdc_event_hash
            LIMIT 1
            """,
            as_dict=True,
        )[0]
        rows_before_replay = _clickhouse_table_row_count(
            clickhouse_connector,
            database=clickhouse_settings.database,
            table=target_table,
        )
        replay_change = CDCChange(
            operation=CDCOperation(str(replay_source["dpone_cdc_operation"])),
            data=json.loads(str(replay_source["dpone_cdc_payload_json"])),
            position=str(replay_source["dpone_cdc_position"]),
            source_schema=schema,
            source_table=source_table,
            sequence=None if not replay_source.get("dpone_cdc_sequence") else str(replay_source["dpone_cdc_sequence"]),
            before=json.loads(str(replay_source.get("dpone_cdc_before_json") or "{}")),
            metadata={"backend": "mssql_change_tracking"},
        )
        duplicate_replay_receipt = sink_applier.apply(
            stream=stream,
            batch=CDCBatch(changes=(replay_change,), next_offset=None, high_watermark=replay_change.position),
        )
        rows_after_replay = _clickhouse_table_row_count(
            clickhouse_connector,
            database=clickhouse_settings.database,
            table=target_table,
        )

        assert duplicate_replay_receipt.passed is True
        assert duplicate_replay_receipt.durable is True
        assert duplicate_replay_receipt.rows_applied == 0
        assert duplicate_replay_receipt.metrics["duplicate_events_skipped"] == 1
        assert rows_after_replay == rows_before_replay

        compare_service = CdcCompareRepairService()
        compare_columns = ("order_id", "status", "amount")
        post_delete_compare = compare_service.compare(
            stream=stream,
            source_reader=MssqlCdcCompareReader(
                connector=mssql,
                source_schema=schema,
                source_table=source_table,
                columns=compare_columns,
                unique_key=stream.unique_key,
                max_rows=100,
            ),
            target_reader=ClickHouseCdcLogCompareReader(
                connector=clickhouse_connector,
                cdc_dataset=stream.target_dataset,
                stream_id=stream.stream_id,
                unique_key=stream.unique_key,
                max_rows=100,
            ),
            output_dir=tmp_path / "compare_post_delete",
        )
        assert post_delete_compare.passed is True
        assert post_delete_compare.rows_source == 0
        assert post_delete_compare.rows_target == 0

        committed_before_failure = offset_store.load_offset(stream)
        assert committed_before_failure is not None
        mssql.execute_query(
            f"INSERT INTO [{schema}].[{source_table}] ([order_id], [status], [amount]) VALUES (2, N'blocked', 22.20)"
        )
        missing_compare = compare_service.compare(
            stream=stream,
            source_reader=MssqlCdcCompareReader(
                connector=mssql,
                source_schema=schema,
                source_table=source_table,
                columns=compare_columns,
                unique_key=stream.unique_key,
                max_rows=100,
            ),
            target_reader=ClickHouseCdcLogCompareReader(
                connector=clickhouse_connector,
                cdc_dataset=stream.target_dataset,
                stream_id=stream.stream_id,
                unique_key=stream.unique_key,
                max_rows=100,
            ),
            output_dir=tmp_path / "compare_missing",
        )
        repair_report = CdcRepairExecutionService(sink_applier=sink_applier).execute(
            stream=stream,
            repair_plan_json=missing_compare.repair_plan.output_path or "",
            output_dir=tmp_path / "compare_repair",
        )
        repaired_compare = compare_service.compare(
            stream=stream,
            source_reader=MssqlCdcCompareReader(
                connector=mssql,
                source_schema=schema,
                source_table=source_table,
                columns=compare_columns,
                unique_key=stream.unique_key,
                max_rows=100,
            ),
            target_reader=ClickHouseCdcLogCompareReader(
                connector=clickhouse_connector,
                cdc_dataset=stream.target_dataset,
                stream_id=stream.stream_id,
                unique_key=stream.unique_key,
                max_rows=100,
            ),
            output_dir=tmp_path / "compare_repaired",
        )

        assert missing_compare.passed is False
        assert [diff.kind for diff in missing_compare.diffs] == ["missing_in_target"]
        assert repair_report.passed is True
        assert repair_report.committed is False
        assert repair_report.repair_actions == 1
        assert repaired_compare.passed is True
        assert repaired_compare.rows_source == 1
        assert repaired_compare.rows_target == 1
        assert offset_store.load_offset(stream) == committed_before_failure

        failing_stream = CdcRuntimeStream(
            pipeline_name=stream.pipeline_name,
            source=stream.source,
            sink=stream.sink,
            backend=stream.backend,
            source_schema=stream.source_schema,
            source_table=stream.source_table,
            target_dataset=f"missing_{uuid.uuid4().hex[:8]}.blocked_cdc",
            unique_key=stream.unique_key,
        )
        failure_report = orchestrator.run_once(
            stream=failing_stream,
            reader=reader,
            offset_store=offset_store,
            sink_applier=sink_applier,
            output_dir=tmp_path / "failure",
            policy=CdcRuntimePolicy(max_changes=100),
        )

        assert failure_report.passed is False
        assert failure_report.committed is False
        assert "clickhouse_cdc_apply.failed" in failure_report.blockers
        assert offset_store.load_offset(stream) == committed_before_failure
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{target_table}`")
        mssql.close()
        _drop_mssql_database(database)


def test_mssql_change_tracking_runtime_materializes_clickhouse_current_state(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    database = f"dpone_mat_{uuid.uuid4().hex[:8]}"
    schema = f"dbo_{uuid.uuid4().hex[:8]}"
    source_table = "orders"
    cdc_table = f"orders_cdc_{uuid.uuid4().hex[:8]}"
    active_table = f"orders_current_{uuid.uuid4().hex[:8]}"
    tombstone_table = f"orders_tombstone_{uuid.uuid4().hex[:8]}"
    active_typed_table = f"orders_current_typed_{uuid.uuid4().hex[:8]}"
    tombstone_typed_table = f"orders_tombstone_typed_{uuid.uuid4().hex[:8]}"
    quarantine_typed_table = f"orders_typed_quarantine_{uuid.uuid4().hex[:8]}"
    broken_typed_table = f"orders_current_typed_broken_{uuid.uuid4().hex[:8]}"
    state_schema = f"state_{uuid.uuid4().hex[:8]}"
    stream = CdcRuntimeStream(
        pipeline_name=f"orders-materialize-{uuid.uuid4().hex[:8]}",
        source="mssql",
        sink="clickhouse",
        backend=CDCBackend.MSSQL_CHANGE_TRACKING,
        source_schema=schema,
        source_table=source_table,
        target_dataset=f"{clickhouse_settings.database}.{cdc_table}",
        unique_key=("order_id",),
    )

    _create_mssql_database(database)
    mssql = _mssql_connector(database)
    factory = CdcRuntimeLiveAdapterFactory()
    reader = factory.mssql_reader(connector=mssql, stream=stream)
    offset_store = factory.mssql_offset_store(connector=mssql, schema=state_schema, table="cdc_offset")
    sink_applier = factory.clickhouse_sink_applier(connector=clickhouse_connector)
    orchestrator = CdcRuntimeOrchestrator()
    materializer = ClickHouseCdcMaterializationService(clickhouse_connector)
    typed_materializer = ClickHouseCdcTypedMaterializationService(clickhouse_connector)
    typed_columns = (
        ClickHouseCdcTypedColumn(name="order_id", clickhouse_type="Int32", required=True),
        ClickHouseCdcTypedColumn(name="status", clickhouse_type="Nullable(String)"),
        ClickHouseCdcTypedColumn(name="amount", clickhouse_type="Nullable(Decimal(18,2))"),
    )

    try:
        mssql.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        mssql.execute_query(
            f"""
            CREATE TABLE [{schema}].[{source_table}] (
                [order_id] int NOT NULL PRIMARY KEY,
                [status] nvarchar(50) NULL,
                [amount] decimal(18, 2) NULL
            )
            """
        )
        reader.setup()

        mssql.execute_query(
            f"INSERT INTO [{schema}].[{source_table}] ([order_id], [status], [amount]) VALUES (1, N'new', 10.50)"
        )
        insert_report = orchestrator.run_once(
            stream=stream,
            reader=reader,
            offset_store=offset_store,
            sink_applier=sink_applier,
            output_dir=tmp_path / "insert",
            policy=CdcRuntimePolicy(max_changes=100),
        )

        mssql.execute_query(f"UPDATE [{schema}].[{source_table}] SET [status] = N'paid' WHERE [order_id] = 1")
        mssql.execute_query(
            f"INSERT INTO [{schema}].[{source_table}] ([order_id], [status], [amount]) VALUES (2, N'open', 20.00)"
        )
        update_report = orchestrator.run_once(
            stream=stream,
            reader=reader,
            offset_store=offset_store,
            sink_applier=sink_applier,
            output_dir=tmp_path / "update",
            policy=CdcRuntimePolicy(max_changes=100),
        )

        mssql.execute_query(f"DELETE FROM [{schema}].[{source_table}] WHERE [order_id] = 1")
        delete_report = orchestrator.run_once(
            stream=stream,
            reader=reader,
            offset_store=offset_store,
            sink_applier=sink_applier,
            output_dir=tmp_path / "delete",
            policy=CdcRuntimePolicy(max_changes=100),
        )

        assert insert_report.passed is True
        assert update_report.passed is True
        assert delete_report.passed is True

        active_report = materializer.materialize(
            plan=ClickHouseCdcMaterializationPlan.from_datasets(
                cdc_dataset=f"{clickhouse_settings.database}.{cdc_table}",
                target_dataset=f"{clickhouse_settings.database}.{active_table}",
                unique_key=("order_id",),
                default_database=clickhouse_settings.database,
            ),
            policy=ClickHouseCdcMaterializationPolicy(delete_mode="exclude_deleted"),
            output_dir=tmp_path / "active_materialization",
        )
        tombstone_report = materializer.materialize(
            plan=ClickHouseCdcMaterializationPlan.from_datasets(
                cdc_dataset=f"{clickhouse_settings.database}.{cdc_table}",
                target_dataset=f"{clickhouse_settings.database}.{tombstone_table}",
                unique_key=("order_id",),
                default_database=clickhouse_settings.database,
            ),
            policy=ClickHouseCdcMaterializationPolicy(delete_mode="tombstone"),
            output_dir=tmp_path / "tombstone_materialization",
        )
        active_typed_report = typed_materializer.materialize(
            plan=ClickHouseCdcTypedMaterializationPlan.from_datasets(
                cdc_dataset=f"{clickhouse_settings.database}.{cdc_table}",
                target_dataset=f"{clickhouse_settings.database}.{active_typed_table}",
                unique_key=("order_id",),
                columns=typed_columns,
                default_database=clickhouse_settings.database,
            ),
            policy=ClickHouseCdcTypedMaterializationPolicy(delete_mode="exclude_deleted"),
            output_dir=tmp_path / "active_typed_materialization",
        )
        tombstone_typed_report = typed_materializer.materialize(
            plan=ClickHouseCdcTypedMaterializationPlan.from_datasets(
                cdc_dataset=f"{clickhouse_settings.database}.{cdc_table}",
                target_dataset=f"{clickhouse_settings.database}.{tombstone_typed_table}",
                unique_key=("order_id",),
                columns=typed_columns,
                default_database=clickhouse_settings.database,
            ),
            policy=ClickHouseCdcTypedMaterializationPolicy(delete_mode="tombstone"),
            output_dir=tmp_path / "tombstone_typed_materialization",
        )

        active_rows = clickhouse_connector.get_records(
            f"""
            SELECT
                dpone_cdc_unique_key_json,
                dpone_cdc_payload_json,
                dpone_cdc_deleted,
                dpone_cdc_operation
            FROM `{clickhouse_settings.database}`.`{active_table}`
            ORDER BY dpone_cdc_unique_key_json
            """,
            as_dict=True,
        )
        tombstone_rows = clickhouse_connector.get_records(
            f"""
            SELECT
                dpone_cdc_unique_key_json,
                dpone_cdc_payload_json,
                dpone_cdc_deleted,
                dpone_cdc_operation
            FROM `{clickhouse_settings.database}`.`{tombstone_table}`
            ORDER BY dpone_cdc_unique_key_json
            """,
            as_dict=True,
        )
        active_typed_rows = clickhouse_connector.get_records(
            f"""
            SELECT
                order_id,
                status,
                amount,
                dpone_cdc_deleted,
                dpone_cdc_operation
            FROM `{clickhouse_settings.database}`.`{active_typed_table}`
            ORDER BY order_id
            """,
            as_dict=True,
        )
        tombstone_typed_rows = clickhouse_connector.get_records(
            f"""
            SELECT
                order_id,
                dpone_cdc_deleted,
                dpone_cdc_operation
            FROM `{clickhouse_settings.database}`.`{tombstone_typed_table}`
            ORDER BY order_id
            """,
            as_dict=True,
        )

        assert active_report.passed is True
        assert active_report.rows_source_events == 4
        assert active_report.rows_materialized == 1
        assert active_report.rows_deleted == 1
        assert tombstone_report.passed is True
        assert tombstone_report.rows_materialized == 2
        assert active_typed_report.passed is True
        assert active_typed_report.rows_source_events == 4
        assert active_typed_report.rows_materialized == 1
        assert active_typed_report.rows_deleted == 1
        assert active_typed_report.typed_columns == ("order_id", "status", "amount")
        assert tombstone_typed_report.passed is True
        assert tombstone_typed_report.rows_materialized == 2
        assert (tmp_path / "active_materialization" / "cdc_materialization.json").exists()
        assert (tmp_path / "tombstone_materialization" / "cdc_materialization.md").exists()
        assert (tmp_path / "active_typed_materialization" / "cdc_typed_materialization.json").exists()
        assert (tmp_path / "tombstone_typed_materialization" / "cdc_typed_materialization.md").exists()
        assert active_rows == [
            {
                "dpone_cdc_unique_key_json": '{"order_id":2}',
                "dpone_cdc_payload_json": '{"amount":"20.00","order_id":2,"status":"open"}',
                "dpone_cdc_deleted": 0,
                "dpone_cdc_operation": "insert",
            }
        ]
        assert {row["dpone_cdc_unique_key_json"]: row["dpone_cdc_deleted"] for row in tombstone_rows} == {
            '{"order_id":1}': 1,
            '{"order_id":2}': 0,
        }
        assert active_typed_rows == [
            {
                "order_id": 2,
                "status": "open",
                "amount": Decimal("20.00"),
                "dpone_cdc_deleted": 0,
                "dpone_cdc_operation": "insert",
            }
        ]
        assert {row["order_id"]: row["dpone_cdc_deleted"] for row in tombstone_typed_rows} == {1: 1, 2: 0}

        mssql.execute_query(f"ALTER TABLE [{schema}].[{source_table}] ADD [status_reason] nvarchar(100) NULL")
        mssql.execute_query(
            f"""
            UPDATE [{schema}].[{source_table}]
            SET [status_reason] = N'customer requested hold'
            WHERE [order_id] = 2
            """
        )
        schema_update_report = orchestrator.run_once(
            stream=stream,
            reader=reader,
            offset_store=offset_store,
            sink_applier=sink_applier,
            output_dir=tmp_path / "schema_update",
            policy=CdcRuntimePolicy(max_changes=100),
        )
        schema_change_json = _write_live_schema_apply_change(
            tmp_path / "schema_change.json",
            source_schema=schema,
            source_table=source_table,
            target_dataset=f"{clickhouse_settings.database}.{active_typed_table}",
            source_offset=str(schema_update_report.next_offset.token if schema_update_report.next_offset else ""),
        )
        schema_apply_report = CdcSchemaEvolutionApplyService(connector=clickhouse_connector).apply(
            output_dir=tmp_path / "schema_apply",
            schema_change_json=schema_change_json,
            sink="clickhouse",
            target_dataset=f"{clickhouse_settings.database}.{active_typed_table}",
            mode="apply",
            cdc_dataset=f"{clickhouse_settings.database}.{cdc_table}",
            unique_key=("order_id",),
            columns=(
                "order_id=Int32",
                "status=Nullable(String)",
                "amount=Nullable(Decimal(18,2))",
                "status_reason=Nullable(String)",
            ),
            typed_refresh=True,
            fail_on_parse_errors=True,
            schema_drift_mode="strict",
            quarantine_dataset=f"{clickhouse_settings.database}.{quarantine_typed_table}",
            require_approval=True,
        )
        schema_apply_rows = clickhouse_connector.get_records(
            f"""
            SELECT
                order_id,
                status_reason,
                dpone_cdc_operation
            FROM `{clickhouse_settings.database}`.`{active_typed_table}`
            ORDER BY order_id
            """,
            as_dict=True,
        )

        assert schema_update_report.passed is True
        assert schema_apply_report.passed is True
        assert schema_apply_report.result.applied is True
        assert schema_apply_report.result.typed_refresh["ran"] is True
        assert (tmp_path / "schema_apply" / "cdc_schema_apply_result.json").exists()
        assert (tmp_path / "schema_apply" / "typed_refresh" / "cdc_typed_materialization.json").exists()
        assert schema_apply_rows == [
            {
                "order_id": 2,
                "status_reason": "customer requested hold",
                "dpone_cdc_operation": "update",
            }
        ]

        _insert_malformed_clickhouse_cdc_event(
            clickhouse_connector,
            stream=stream,
            database=clickhouse_settings.database,
            table=cdc_table,
            source_schema=schema,
            source_table=source_table,
            order_id=3,
        )
        broken_typed_report = typed_materializer.materialize(
            plan=ClickHouseCdcTypedMaterializationPlan.from_datasets(
                cdc_dataset=f"{clickhouse_settings.database}.{cdc_table}",
                target_dataset=f"{clickhouse_settings.database}.{broken_typed_table}",
                unique_key=("order_id",),
                columns=typed_columns,
                default_database=clickhouse_settings.database,
            ),
            policy=ClickHouseCdcTypedMaterializationPolicy(
                delete_mode="exclude_deleted",
                quality=ClickHouseCdcTypedQualityPolicy(
                    fail_on_parse_errors=True,
                    max_parse_error_ratio=0.0,
                    quarantine_dataset=f"{clickhouse_settings.database}.{quarantine_typed_table}",
                    schema_drift_mode="strict",
                ),
            ),
            output_dir=tmp_path / "broken_typed_materialization",
        )
        quarantine_rows = clickhouse_connector.get_records(
            f"""
            SELECT
                column_name,
                clickhouse_type,
                dpone_cdc_unique_key_json,
                reason
            FROM `{clickhouse_settings.database}`.`{quarantine_typed_table}`
            ORDER BY column_name, dpone_cdc_unique_key_json
            """,
            as_dict=True,
        )
        broken_table_exists = clickhouse_connector.get_records(
            f"""
            SELECT count() AS exists
            FROM system.tables
            WHERE database = '{clickhouse_settings.database}' AND name = '{broken_typed_table}'
            """,
            as_dict=True,
        )

        assert broken_typed_report.passed is False
        assert "clickhouse_cdc_typed_materialization.parse_quarantine" in broken_typed_report.blockers
        assert broken_typed_report.quality_evidence is not None
        assert broken_typed_report.quality_evidence.parse_quarantine.failed_rows == 1
        assert (tmp_path / "broken_typed_materialization" / "cdc_typed_parse_quarantine.json").exists()
        assert quarantine_rows == [
            {
                "column_name": "amount",
                "clickhouse_type": "Nullable(Decimal(18,2))",
                "dpone_cdc_unique_key_json": '{"order_id":3}',
                "reason": "typed_projection_parse_failed",
            }
        ]
        assert int(broken_table_exists[0]["exists"]) == 0
    finally:
        for table in (
            cdc_table,
            active_table,
            tombstone_table,
            active_typed_table,
            tombstone_typed_table,
            quarantine_typed_table,
            broken_typed_table,
        ):
            clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{table}`")
        mssql.close()
        _drop_mssql_database(database)


def test_mssql_change_tracking_retention_gap_resyncs_clickhouse_without_committing_offsets(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    database = f"dpone_resync_{uuid.uuid4().hex[:8]}"
    schema = f"dbo_{uuid.uuid4().hex[:8]}"
    source_table = "orders"
    cdc_table = f"orders_cdc_{uuid.uuid4().hex[:8]}"
    state_schema = f"state_{uuid.uuid4().hex[:8]}"
    stream = CdcRuntimeStream(
        pipeline_name=f"orders-resync-{uuid.uuid4().hex[:8]}",
        source="mssql",
        sink="clickhouse",
        backend=CDCBackend.MSSQL_CHANGE_TRACKING,
        source_schema=schema,
        source_table=source_table,
        target_dataset=f"{clickhouse_settings.database}.{cdc_table}",
        unique_key=("order_id",),
    )

    _create_mssql_database(database)
    mssql = _mssql_connector(database)
    factory = CdcRuntimeLiveAdapterFactory()
    reader = factory.mssql_reader(connector=mssql, stream=stream)
    offset_store = factory.mssql_offset_store(connector=mssql, schema=state_schema, table="cdc_offset")
    sink_applier = factory.clickhouse_sink_applier(connector=clickhouse_connector)

    try:
        mssql.execute_query(f"EXEC('CREATE SCHEMA [{schema}]')")
        mssql.execute_query(
            f"""
            CREATE TABLE [{schema}].[{source_table}] (
                [order_id] int NOT NULL PRIMARY KEY,
                [status] nvarchar(50) NULL
            )
            """
        )
        reader.setup()
        mssql.execute_query(f"INSERT INTO [{schema}].[{source_table}] ([order_id], [status]) VALUES (10, N'paid')")

        offset_store.save_offset(stream, offset=reader.current_offset())
        saved_offset = offset_store.load_offset(stream)
        assert saved_offset is not None

        retention_report = CdcRetentionGapService(policy=CdcRetentionPolicy(at_risk_margin=5)).evaluate(
            output_dir=tmp_path / "retention",
            stream=stream,
            committed_offset=CDCOffset(
                backend=CDCBackend.MSSQL_CHANGE_TRACKING,
                token="-1",
                snapshot_complete=True,
            ),
            probe=MssqlChangeTrackingRetentionProbe(
                connector=mssql,
                source_schema=schema,
                source_table=source_table,
            ),
        )
        rows_json = _write_rows_json(
            tmp_path / "source_snapshot.json",
            rows=[{"order_id": 10, "status": "paid"}],
        )
        plan_report = CdcResyncPlanner().plan(
            output_dir=tmp_path / "resync_plan",
            retention_report=retention_report,
            rows_json=rows_json,
        )
        execution_report = CdcResyncExecutionService(sink_applier=sink_applier).execute(
            output_dir=tmp_path / "resync_execute",
            resync_plan_json=plan_report.plan.output_path or "",
            stream=stream,
        )
        compare_report = CdcCompareRepairService().compare(
            stream=stream,
            source_reader=MssqlCdcCompareReader(
                connector=mssql,
                source_schema=schema,
                source_table=source_table,
                columns=("order_id", "status"),
                unique_key=stream.unique_key,
                max_rows=100,
            ),
            target_reader=ClickHouseCdcLogCompareReader(
                connector=clickhouse_connector,
                cdc_dataset=stream.target_dataset,
                stream_id=stream.stream_id,
                unique_key=stream.unique_key,
                max_rows=100,
            ),
            output_dir=tmp_path / "compare_after_resync",
        )

        assert retention_report.passed is False
        assert retention_report.decision.level == "gap_detected"
        assert plan_report.passed is True
        assert plan_report.plan.action_count == 1
        assert execution_report.passed is True
        assert execution_report.committed is False
        assert compare_report.passed is True
        assert compare_report.rows_source == 1
        assert compare_report.rows_target == 1
        assert offset_store.load_offset(stream) == saved_offset
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{clickhouse_settings.database}`.`{cdc_table}`")
        mssql.close()
        _drop_mssql_database(database)


def _mssql_connector(database: str) -> MSSQLConnector:
    host = os.getenv("DPONE_IT_MSSQL_HOST", "127.0.0.1")
    port = int(os.getenv("DPONE_IT_MSSQL_PORT", "51433"))
    password = os.getenv("DPONE_IT_MSSQL_PASSWORD", "Dp0ne.Strong.Pw.2026!")
    connector = MSSQLConnector(
        host=host,
        port=port,
        database=database,
        user=os.getenv("DPONE_IT_MSSQL_USER", "sa"),
        password=password,
        driver=os.getenv("DPONE_IT_MSSQL_DRIVER", "ODBC Driver 18 for SQL Server"),
        trust_server_certificate=os.getenv("DPONE_IT_MSSQL_TRUST_SERVER_CERTIFICATE", "yes"),
        query_timeout=60,
    )
    _wait_for_mssql(connector)
    return connector


def _create_mssql_database(database: str) -> None:
    connector = _mssql_connector("master")
    try:
        connector.execute_query(f"IF DB_ID(N'{database}') IS NULL CREATE DATABASE [{database}]")
    finally:
        connector.close()


def _drop_mssql_database(database: str) -> None:
    connector = _mssql_connector("master")
    try:
        connector.execute_query(
            f"""
            IF DB_ID(N'{database}') IS NOT NULL
            BEGIN
                ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
                DROP DATABASE [{database}];
            END
            """
        )
    finally:
        connector.close()


def _wait_for_mssql(connector: MSSQLConnector) -> None:
    deadline = time.monotonic() + float(os.getenv("DPONE_IT_MSSQL_CONNECT_TIMEOUT", "120"))
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            connector.execute_query("SELECT 1")
            return
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    raise RuntimeError(f"MSSQL integration service did not become ready: {last_error}")


def _insert_malformed_clickhouse_cdc_event(
    connector: object,
    *,
    stream: CdcRuntimeStream,
    database: str,
    table: str,
    source_schema: str,
    source_table: str,
    order_id: int,
) -> None:
    change = CDCChange(
        operation=CDCOperation.INSERT,
        data={"order_id": order_id, "status": "bad-decimal", "amount": "not-a-decimal"},
        position=f"manual-malformed-{uuid.uuid4().hex}",
        source_schema=source_schema,
        source_table=source_table,
        sequence=999_999,
    )
    row = cdc_event_row(stream, change)
    columns = tuple(row.keys())
    columns_sql = ", ".join(f"`{column}`" for column in columns)
    values = [{column: row[column] for column in columns}]
    connector.connection.execute(
        f"INSERT INTO `{database}`.`{table}` ({columns_sql}) VALUES",
        values,
    )


def _clickhouse_table_row_count(connector: object, *, database: str, table: str) -> int:
    rows = connector.get_records(
        f"SELECT count() AS rows FROM `{database}`.`{table}`",
        as_dict=True,
    )
    return int(rows[0]["rows"])


def _write_live_schema_apply_change(
    path: Path,
    *,
    source_schema: str,
    source_table: str,
    target_dataset: str,
    source_offset: str,
) -> Path:
    path.write_text(
        json.dumps(
            {
                "change": {
                    "change_id": f"{source_schema}-{source_table}-add-status-reason",
                    "kind": "add_column",
                    "captured_at": "2026-06-12T00:00:00Z",
                    "source_table": f"{source_schema}.{source_table}",
                    "source_column": "status_reason",
                    "target_table": target_dataset,
                    "target_column": "status_reason",
                    "old_type": "",
                    "new_type": "nvarchar(100)",
                    "old_nullable": True,
                    "new_nullable": True,
                    "source_offset": source_offset,
                    "breaking": False,
                }
            },
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return path


def _write_rows_json(path: Path, *, rows: list[dict[str, object]]) -> Path:
    path.write_text(
        json.dumps({"rows": rows}, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path
