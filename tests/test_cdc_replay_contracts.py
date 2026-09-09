from __future__ import annotations

from dpone.commands.plan_cmd import _render_md, _render_text
from dpone.strategy_intelligence.native_transfer import (
    NativeTransferPlanBuilder,
    NativeTransferRequest,
)


def test_postgres_mssql_cdc_apply_plan_exposes_lossless_replay_contract() -> None:
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
            }
        },
        sink_options={
            "deletes": {
                "mode": "soft_delete",
            }
        },
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.replay_contract == {
        "route": "postgres_to_mssql",
        "strategy": "cdc_apply",
        "state_boundary": "logical_lsn",
        "state_commit": "after_target_finalize_and_quality",
        "delete_mode": "soft_delete",
        "idempotency": "unique_key",
        "lossless_replay": True,
        "artifacts": [
            "cdc_replay_plan.json",
            "state_transition.json",
            "typed_reconciliation.json",
        ],
        "warnings": [],
    }
    assert plan.to_dict()["replay_contract"]["lossless_replay"] is True


def test_postgres_mssql_cdc_apply_plan_warns_when_replay_is_ambiguous() -> None:
    request = NativeTransferRequest(
        source_type="postgres",
        sink_type="mssql",
        source_table="public.events",
        target_table="dbo.events",
        strategy="cdc_apply",
        source_options={"cdc": {"mode": "updated_at"}},
        sink_options={},
    )

    plan = NativeTransferPlanBuilder().build(request)

    assert plan.replay_contract["lossless_replay"] is False
    assert plan.replay_contract["state_boundary"] == "unknown"
    assert "unique_key is required for idempotent cdc_apply replay" in plan.warnings
    assert "source.options.cdc.state_boundary is required for durable replay" in plan.warnings
    assert "sink.options.deletes.mode is not set; defaulting to ignore" in plan.warnings


def test_plan_renderer_includes_cdc_replay_contract() -> None:
    plan = NativeTransferPlanBuilder().build(
        NativeTransferRequest(
            source_type="postgres",
            sink_type="mssql",
            source_table="public.events",
            target_table="dbo.events",
            strategy="cdc_apply",
            unique_key=("event_id",),
            source_options={"cdc": {"state_boundary": "logical_lsn"}},
            sink_options={"deletes": {"mode": "soft_delete"}},
        )
    )
    payload = _payload(plan.to_dict())

    text = _render_text(payload)
    markdown = _render_md(payload)

    assert "native_transfer_replay: postgres_to_mssql lossless=True" in text
    assert "native_transfer_state_commit: after_target_finalize_and_quality" in text
    assert "## Native transfer replay contract" in markdown
    assert "- state_boundary: `logical_lsn`" in markdown


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
