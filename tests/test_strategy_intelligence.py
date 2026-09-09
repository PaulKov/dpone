from __future__ import annotations

from dpone.strategy_intelligence.advisor import StrategyAdvisor, StrategyContext
from dpone.strategy_intelligence.certification import StrategyCertificationMatrixService
from dpone.strategy_intelligence.native_paths import NativeFastPathCatalog
from dpone.strategy_intelligence.repair import RepairPlanService, RepairRequest


def test_strategy_advisor_selects_partition_replace_for_large_partitioned_db_load() -> None:
    decision = StrategyAdvisor().advise(
        StrategyContext(
            source_type="postgres",
            sink_type="mssql",
            requested_mode="auto",
            unique_key=("order_id",),
            estimated_rows=50_000_000,
            changed_percent=0.20,
            delete_percent=0.05,
            partition_column="business_date",
            cdc_available=False,
        )
    )

    assert decision.strategy_mode == "partition_replace"
    assert decision.merge_policy == "delete_insert"
    assert decision.native_fast_path == "postgres_copy_to_mssql_bcp"
    assert decision.adaptive_batching.enabled is True
    assert decision.adaptive_batching.initial_batch_size == 100_000
    assert any(item.code == "partition_replace_large_delta" for item in decision.reasons)
    assert any(item.code == "staging_first_required" for item in decision.safety_gates)


def test_strategy_advisor_preserves_explicit_merge_policy() -> None:
    decision = StrategyAdvisor().advise(
        StrategyContext(
            source_type="postgres",
            sink_type="mssql",
            requested_mode="incremental_merge",
            requested_merge_policy="update_insert",
            unique_key=("metric_code",),
        )
    )

    assert decision.merge_policy == "update_insert"


def test_strategy_advisor_selects_cdc_apply_when_cdc_is_available_and_deletes_are_expected() -> None:
    decision = StrategyAdvisor().advise(
        StrategyContext(
            source_type="postgres",
            sink_type="clickhouse",
            requested_mode="auto",
            unique_key=("id",),
            estimated_rows=5_000_000,
            changed_percent=0.10,
            delete_percent=0.02,
            cdc_available=True,
        )
    )

    assert decision.strategy_mode == "cdc_apply"
    assert decision.native_fast_path == "postgres_copy_to_clickhouse_http_tsv"
    assert any(item.code == "cdc_available" for item in decision.reasons)
    assert any(item.code == "delete_semantics" for item in decision.safety_gates)


def test_strategy_advisor_never_recommends_unsafe_clickhouse_mssql_target_cursor() -> None:
    decision = StrategyAdvisor().advise(
        StrategyContext(
            source_type="clickhouse",
            sink_type="mssql",
            requested_mode="auto",
            source_cursor="event_at",
            estimated_rows=25_000_000,
            source_options={"table_kind": "event_log"},
        )
    )

    assert decision.strategy_mode == "full_refresh"
    assert decision.native_fast_path == "clickhouse_streaming_to_mssql_bcp_staging"
    assert any(item.code == "clickhouse_mssql_cursor_unsupported" for item in decision.reasons)
    assert any(item.code == "clickhouse_mssql_complete_boundary_required" for item in decision.warnings)


def test_strategy_advisor_does_not_treat_unique_key_as_clickhouse_source_boundary() -> None:
    decision = StrategyAdvisor().advise(
        StrategyContext(
            source_type="clickhouse",
            sink_type="mssql",
            requested_mode="auto",
            unique_key=("event_id",),
            estimated_rows=25_000_000,
        )
    )

    assert decision.strategy_mode == "full_refresh"
    assert any(item.code == "clickhouse_mssql_key_without_boundary" for item in decision.reasons)


def test_strategy_advisor_does_not_infer_complete_clickhouse_mssql_window_from_predicate_hints() -> None:
    hints = (
        {"source_custom_predicate": "is_deleted = 0"},
        {"custom_predicate": "ptnDate >= today() - 10"},
        {"date_from": "2026-08-01"},
        {"date_from": "2026-08-01", "date_to": "2026-08-10"},
    )

    for source_options in hints:
        decision = StrategyAdvisor().advise(
            StrategyContext(
                source_type="clickhouse",
                sink_type="mssql",
                requested_mode="auto",
                partition_column="ptnDate",
                source_options=source_options,
            )
        )

        assert decision.strategy_mode == "full_refresh", source_options
        assert any(item.code == "clickhouse_mssql_partition_window_unverified" for item in decision.reasons)
        assert any(
            item.code == "clickhouse_mssql_complete_boundary_required" and item.severity == "error"
            for item in decision.warnings
        )


def test_strategy_advisor_warns_when_clickhouse_mssql_event_boundary_is_missing() -> None:
    decision = StrategyAdvisor().advise(
        StrategyContext(
            source_type="clickhouse",
            sink_type="mssql",
            requested_mode="auto",
            estimated_rows=25_000_000,
            source_options={"table_kind": "event_log"},
        )
    )

    assert decision.strategy_mode == "full_refresh"
    assert any(item.code == "event_boundary_required" and item.severity == "error" for item in decision.warnings)


def test_strategy_advisor_keeps_kafka_partition_replace_unsupported() -> None:
    decision = StrategyAdvisor().advise(
        StrategyContext(
            source_type="postgres",
            sink_type="kafka",
            requested_mode="partition_replace",
            unique_key=("id",),
            estimated_rows=100_000,
        )
    )

    assert decision.strategy_mode == "incremental_merge"
    assert decision.merge_policy == "event_upsert"
    assert any(
        item.severity == "error" and item.code == "partition_replace_not_supported" for item in decision.warnings
    )


def test_native_fast_path_catalog_explains_target_specific_commands() -> None:
    catalog = NativeFastPathCatalog()

    pg_to_mssql = catalog.resolve("postgres", "mssql")
    mssql_to_clickhouse = catalog.resolve("mssql", "clickhouse")
    clickhouse_to_mssql = catalog.resolve("clickhouse", "mssql")

    assert pg_to_mssql.path_id == "postgres_copy_to_mssql_bcp"
    assert "COPY" in pg_to_mssql.commands[0]
    assert any("bcp" in command for command in pg_to_mssql.commands)
    assert mssql_to_clickhouse.path_id == "mssql_bcp_queryout_to_clickhouse_typed_wire"
    assert "CustomSeparated" in "\n".join(mssql_to_clickhouse.commands)
    assert "clickhouse-client" in "\n".join(mssql_to_clickhouse.commands)
    assert clickhouse_to_mssql.path_id == "clickhouse_streaming_to_mssql_bcp_staging"
    assert "system.columns" in clickhouse_to_mssql.commands[0]
    assert "bcp target_staging in data.bcp" in clickhouse_to_mssql.commands[1]
    assert "one immutable" in clickhouse_to_mssql.summary
    assert "UNVERIFIED" in clickhouse_to_mssql.summary
    assert "each batch" not in clickhouse_to_mssql.summary
    assert "source fetch batches independent" in clickhouse_to_mssql.commands[2]
    assert "UNVERIFIED" in clickhouse_to_mssql.expected_impact


def test_postgres_mssql_native_path_documents_lossless_bulk_text_codec() -> None:
    path = NativeFastPathCatalog().resolve("postgres", "mssql")

    command_text = "\n".join(path.commands)
    assert path.path_id == "postgres_copy_to_mssql_bcp"
    assert "BulkTextCodec" in path.summary
    assert "length-prefixed" in path.summary
    assert "NULL '__DPONE_NULL__'" not in command_text
    assert "NULL ''" in command_text
    assert "QUOTE E'\\x1f'" in command_text
    assert "bcp target_staging in data.typed.bin -f data.fmt" in command_text
    assert path.fallback_path == "fail_closed_for_certified_key_snapshot"


def test_postgres_mssql_snapshot_plan_explains_lossless_enforcement() -> None:
    from dpone.readiness.managed_planning_snapshot import bulk_wire_override

    plan = bulk_wire_override(
        {"reconciliation": {"enabled": True, "mode": "key_snapshot"}},
        "postgres",
        "mssql",
    )

    assert plan is not None
    assert plan["lossless"] is True
    assert plan["lossless_enforcement"] == {
        "declared_types": "reject_structural_narrowing_before_source_row_export",
        "wire": "immutable_sha256_and_row_count_receipt",
        "native_staging": "source_target_source_roundtrip_before_target_transaction",
    }


def test_repair_plan_service_builds_resume_replay_and_partition_resync_steps() -> None:
    plan = RepairPlanService().plan(
        RepairRequest(
            run_id="01HY0000000000000000000000",
            source_type="postgres",
            sink_type="mssql",
            strategy_mode="partition_replace",
            failed_stage="finalize",
            partition_values=("2026-01-01", "2026-01-02"),
        )
    )

    assert plan.safe_to_auto_resume is False
    assert [step.action for step in plan.steps] == [
        "inspect_run_artifacts",
        "validate_staging_artifacts",
        "replay_partition_replace",
        "quality_reconcile",
        "commit_state_after_success",
    ]
    assert "dpone resync" in plan.commands[0]


def test_strategy_certification_matrix_marks_supported_and_manual_gates() -> None:
    matrix = StrategyCertificationMatrixService().build()

    postgres_mssql = matrix.lookup("postgres", "mssql", "partition_replace")
    postgres_kafka = matrix.lookup("postgres", "kafka", "partition_replace")

    assert postgres_mssql.status == "manual_live_gate"
    assert postgres_mssql.native_fast_path == "postgres_copy_to_mssql_bcp"
    assert postgres_kafka.status == "not_supported"


def test_strategy_certification_matrix_is_truthful_for_clickhouse_mssql_source_capabilities() -> None:
    matrix = StrategyCertificationMatrixService().build()

    assert matrix.lookup("clickhouse", "mssql", "full_refresh").status == "manual_live_gate"
    assert matrix.lookup("clickhouse", "mssql", "replace").status == "manual_live_gate"
    assert matrix.lookup("clickhouse", "mssql", "partition_replace").status == "manual_live_gate"
    assert matrix.lookup("clickhouse", "mssql", "backfill").status == "manual_live_gate"
    for unsupported in ("incremental_append", "incremental_merge", "snapshot_diff", "scd2", "cdc_apply"):
        entry = matrix.lookup("clickhouse", "mssql", unsupported)
        assert entry.status == "not_supported", unsupported
        assert entry.required_evidence == (), unsupported
