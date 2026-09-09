from __future__ import annotations

from dpone.commands.plan_cmd import _render_md, _render_text
from dpone.strategy_intelligence.native_transfer import (
    NativeTransferPlanBuilder,
    NativeTransferRequest,
)


def test_postgres_mssql_cdc_plan_exposes_partitioned_evidence_contract() -> None:
    request = NativeTransferRequest(
        source_type="postgres",
        sink_type="mssql",
        source_table="public.events",
        target_table="dbo.events",
        strategy="cdc_apply",
        unique_key=("event_id",),
        source_options={
            "cdc": {
                "mode": "logical_replication",
                "state_boundary": "logical_lsn",
            },
            "partitioning": {
                "column": "event_id",
                "bounds": "auto",
                "max_partitions": 8,
                "export_workers": 4,
                "load_workers": 4,
            },
        },
        sink_options={"deletes": {"mode": "soft_delete"}},
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.evidence_contract == {
        "route": "postgres_to_mssql",
        "strategy": "cdc_apply",
        "state_commit_gate": "after_target_finalize_and_quality",
        "required_artifacts": [
            "native_transfer_plan.json",
            "transfer_diagnostics.json",
            "cdc_replay_plan.json",
            "partition_checkpoints.json",
            "partition_retry_plan.json",
            "typed_reconciliation.json",
            "quality_results.json",
            "state_transition.json",
        ],
        "required_checks": [
            "partition_checkpoint_consistency",
            "typed_reconciliation",
            "quality_gate",
            "state_transition_after_commit",
        ],
        "partition_retry": {
            "enabled": True,
            "checkpoint_key": "transfer_partition_id",
            "skip_rule": "matching_source_target_strategy_query_schema_and_bounds_hash",
            "state_scope": "per_partition",
        },
        "typed_reconciliation": {
            "required": True,
            "scope": "source_target_type_aware",
        },
    }
    assert plan.to_dict()["evidence_contract"]["partition_retry"]["enabled"] is True


def test_native_transfer_evidence_contract_disables_partition_retry_for_single_partition() -> None:
    plan = NativeTransferPlanBuilder().build(
        NativeTransferRequest(
            source_type="postgres",
            sink_type="mssql",
            source_table="public.events",
            target_table="dbo.events",
            strategy="incremental_append",
            source_options={},
            sink_options={},
        )
    )

    assert plan.evidence_contract["partition_retry"] == {
        "enabled": False,
        "checkpoint_key": "transfer_partition_id",
        "skip_rule": "matching_source_target_strategy_query_schema_and_bounds_hash",
        "state_scope": "single_partition",
    }
    assert "partition_checkpoints.json" not in plan.evidence_contract["required_artifacts"]


def test_plan_renderer_includes_native_transfer_evidence_contract() -> None:
    plan = NativeTransferPlanBuilder().build(
        NativeTransferRequest(
            source_type="postgres",
            sink_type="mssql",
            source_table="public.events",
            target_table="dbo.events",
            strategy="cdc_apply",
            unique_key=("event_id",),
            source_options={
                "cdc": {"state_boundary": "logical_lsn"},
                "partitioning": {"column": "event_id", "max_partitions": 2},
            },
            sink_options={"deletes": {"mode": "soft_delete"}},
        )
    )
    payload = _payload(plan.to_dict())

    text = _render_text(payload)
    markdown = _render_md(payload)

    assert "native_transfer_evidence: artifacts=8 checks=4" in text
    assert "native_transfer_partition_retry: True" in text
    assert "## Native transfer evidence contract" in markdown
    assert "- state_commit_gate: `after_target_finalize_and_quality`" in markdown


def _payload(native_transfer_plan: dict) -> dict:
    return {
        "process": "events",
        "source": {"type": "postgres", "table": "public.events"},
        "sink": {"type": "mssql", "table": "dbo.events"},
        "strategy": {"mode": "cdc_apply"},
        "bulk_path": "native",
        "staging": {"staging_first": True},
        "schema_evolution": {"enabled": True},
        "type_inference": {"options": {"enabled": True}},
        "physical_design": {"options": {"enabled": True}},
        "strategy_intelligence": {
            "decision": {
                "strategy_mode": "cdc_apply",
                "native_fast_path": "postgres_copy_to_mssql_bcp",
                "native_transfer_plan": native_transfer_plan,
            }
        },
        "warnings": [],
    }
