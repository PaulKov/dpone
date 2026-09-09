from __future__ import annotations

from dpone.readiness.cdc import CDCBackend, CDCOffset
from dpone.runtime.cdc import CDCChange, CDCOperation
from dpone.runtime.cdc.identity import CDCEventIdentityService, CDCIdempotencyService
from dpone.runtime.cdc.replay import CDCCommitGate, CDCReplayPlanner


def _change(position: str, *, sequence: int | None = None) -> CDCChange:
    return CDCChange(
        operation=CDCOperation.UPDATE,
        data={"id": 10, "amount": 99.5},
        position=position,
        source_schema="public",
        source_table="orders",
        transaction_id="900",
        sequence=sequence,
    )


def test_cdc_event_identity_is_deterministic_and_changes_with_position() -> None:
    service = CDCEventIdentityService()

    first = service.event_id(_change("0/16B6C50"), unique_key=["id"])
    retry = service.event_id(_change("0/16B6C50"), unique_key=["id"])
    next_position = service.event_id(_change("0/16B6C51"), unique_key=["id"])

    assert first == retry
    assert len(first) == 64
    assert first != next_position


def test_cdc_event_identity_changes_with_payload_even_when_position_matches() -> None:
    service = CDCEventIdentityService()
    original = _change("0/16B6C50")
    corrected = CDCChange(
        operation=CDCOperation.UPDATE,
        data={"id": 10, "amount": 101.0},
        position="0/16B6C50",
        source_schema="public",
        source_table="orders",
        transaction_id="900",
    )

    assert service.event_id(original, unique_key=["id"]) != service.event_id(corrected, unique_key=["id"])


def test_cdc_idempotency_service_detects_duplicate_events() -> None:
    service = CDCIdempotencyService(identity_service=CDCEventIdentityService())

    report = service.evaluate([_change("0/1"), _change("0/2"), _change("0/1")], unique_key=["id"])

    assert report.passed is False
    assert report.total_events == 3
    assert report.unique_events == 2
    assert report.duplicate_event_ids
    assert report.to_dict()["duplicate_count"] == 1


def test_cdc_replay_planner_blocks_rewind_without_allow_rewind() -> None:
    planner = CDCReplayPlanner()

    plan = planner.plan(
        backend=CDCBackend.POSTGRES_LOGICAL,
        pipeline_name="orders-cdc",
        source_schema="public",
        source_table="orders",
        stored_offset=CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token="0/20", snapshot_complete=True),
        replay_from=CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token="0/10", snapshot_complete=True),
        retention_min=CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token="0/05", snapshot_complete=True),
        high_watermark=CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token="0/30", snapshot_complete=True),
    )

    assert plan.safe_to_execute is False
    assert "replay.requires_allow_rewind" in plan.blockers
    assert "replay.artifact_required_for_consumed_postgres_slot" in plan.blockers
    assert plan.steps[0].name == "pause_pipeline"


def test_cdc_replay_planner_allows_artifact_backed_rewind() -> None:
    planner = CDCReplayPlanner()

    plan = planner.plan(
        backend=CDCBackend.POSTGRES_LOGICAL,
        pipeline_name="orders-cdc",
        source_schema="public",
        source_table="orders",
        stored_offset=CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token="0/20", snapshot_complete=True),
        replay_from=CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token="0/10", snapshot_complete=True),
        replay_to=CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token="0/20", snapshot_complete=True),
        retention_min=CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token="0/05", snapshot_complete=True),
        high_watermark=CDCOffset(backend=CDCBackend.POSTGRES_LOGICAL, token="0/30", snapshot_complete=True),
        artifact_uri="s3://dpone-artifacts/orders/0-10-0-20.jsonl",
        allow_rewind=True,
    )

    assert plan.safe_to_execute is True
    assert not plan.blockers
    assert plan.to_dict()["artifact_uri"] == "s3://dpone-artifacts/orders/0-10-0-20.jsonl"
    assert "commit_offset_after_sink_commit" in [step.name for step in plan.steps]


def test_cdc_commit_gate_allows_offset_commit_only_after_sink_commit_and_idempotency() -> None:
    gate = CDCCommitGate()
    offset = CDCOffset(backend=CDCBackend.MSSQL_CDC, token="0x0002", snapshot_complete=True)

    denied = gate.evaluate(
        next_offset=offset,
        expected_backend=CDCBackend.MSSQL_CDC,
        sink_status="success",
        load_status="staged",
        idempotency_passed=True,
    )
    allowed = gate.evaluate(
        next_offset=offset,
        expected_backend=CDCBackend.MSSQL_CDC,
        sink_status="success",
        load_status="committed",
        idempotency_passed=True,
    )

    assert denied.can_commit is False
    assert "load_not_committed" in denied.blockers
    assert allowed.can_commit is True
    assert allowed.next_offset == offset
