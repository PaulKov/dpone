from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.cdc.apply import CdcApplyCertificationService, CdcApplyFixture, InMemoryCdcApplyStrategy
from dpone.ops.cdc.apply_models import CdcApplyEvent


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _fixture_payload() -> dict[str, object]:
    return {
        "unique_key": ["order_id"],
        "snapshot_boundary": "0x00000010",
        "window_start": "0x00000011",
        "window_end": "0x00000014",
        "retention_min": "0x00000001",
        "initial_rows": [
            {"order_id": 1, "status": "new", "amount": "10.00"},
            {"order_id": 2, "status": "new", "amount": "20.00"},
        ],
        "events": [
            {
                "operation": "insert",
                "position": "0x00000011",
                "sequence": 1,
                "key": {"order_id": 3},
                "after": {"order_id": 3, "status": "new", "amount": "30.00"},
            },
            {
                "operation": "update",
                "position": "0x00000012",
                "sequence": 2,
                "key": {"order_id": 1},
                "after": {"order_id": 1, "status": "paid", "amount": "11.00"},
            },
            {
                "operation": "delete",
                "position": "0x00000013",
                "sequence": 3,
                "key": {"order_id": 2},
                "before": {"order_id": 2, "status": "new", "amount": "20.00"},
            },
            {
                "operation": "update",
                "position": "0x00000012",
                "sequence": 2,
                "key": {"order_id": 1},
                "after": {"order_id": 1, "status": "paid", "amount": "11.00"},
            },
        ],
        "expected_rows": [
            {"order_id": 1, "status": "paid", "amount": "11.00"},
            {"order_id": 3, "status": "new", "amount": "30.00"},
        ],
    }


def test_cdc_apply_event_identity_is_deterministic_for_replayed_events() -> None:
    event = CdcApplyEvent.from_dict(
        {
            "operation": "update",
            "position": "0x00000012",
            "sequence": 2,
            "key": {"order_id": 1},
            "after": {"order_id": 1, "status": "paid"},
        }
    )

    assert event.event_id == event.event_id
    assert len(event.event_id) == 64
    assert event.to_dict()["operation"] == "update"


def test_cdc_apply_fixture_loads_rows_and_events_from_json(tmp_path: Path) -> None:
    path = _write_json(tmp_path / "fixture.json", _fixture_payload())

    fixture = CdcApplyFixture.from_path(path)

    assert fixture.unique_key == ("order_id",)
    assert fixture.snapshot_boundary == "0x00000010"
    assert fixture.window_start == "0x00000011"
    assert fixture.window_end == "0x00000014"
    assert len(fixture.events) == 4
    assert fixture.events[0].operation == "insert"


def test_cdc_apply_certification_writes_handoff_ready_evidence(tmp_path: Path) -> None:
    fixture = _write_json(tmp_path / "fixture.json", _fixture_payload())

    report = CdcApplyCertificationService().certify(
        output_dir=tmp_path / "cert",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        source_dataset="dbo.orders",
        target_dataset="analytics.orders",
        fixture_json=fixture,
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    handoff = json.loads(Path(report.handoff_json_path).read_text(encoding="utf-8"))

    assert report.passed is True
    assert payload["schema_version"] == "dpone.cdc_apply_certification.v1"
    assert payload["evidence_status"] == "UNVERIFIED"
    assert payload["stream"]["stream_id"] == "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders"
    assert payload["metrics"]["duplicate_events"] == 1
    assert payload["metrics"]["delete_events"] == 1
    assert payload["evidence_artifacts"]["cdc_apply_correctness"].endswith("cdc_apply_correctness.json")
    assert Path(payload["evidence_artifacts"]["typed_cdc_hash"]).exists()
    assert handoff["passed"] is True
    assert handoff["level"] == "handoff_ready"


def test_cdc_apply_certification_blocks_mismatched_expected_rows(tmp_path: Path) -> None:
    payload = _fixture_payload()
    payload["expected_rows"] = [{"order_id": 1, "status": "wrong", "amount": "11.00"}]
    fixture = _write_json(tmp_path / "fixture.json", payload)

    report = CdcApplyCertificationService().certify(
        output_dir=tmp_path / "cert",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        source_dataset="dbo.orders",
        target_dataset="analytics.orders",
        fixture_json=fixture,
    )

    evidence = json.loads(Path(report.evidence_artifacts["cdc_apply_correctness"]).read_text(encoding="utf-8"))

    assert report.passed is False
    assert "cdc_apply_correctness.mismatch" in report.blockers
    assert evidence["passed"] is False
    assert evidence["actual_row_count"] == 2
    assert evidence["expected_row_count"] == 1


class _RecordingStrategy:
    def __init__(self) -> None:
        self.fixture: CdcApplyFixture | None = None

    def apply(self, fixture: CdcApplyFixture):
        self.fixture = fixture
        return InMemoryCdcApplyStrategy().apply(fixture)


def test_cdc_apply_certification_uses_injected_strategy_by_sink_apply_mode(tmp_path: Path) -> None:
    fixture = _write_json(tmp_path / "fixture.json", _fixture_payload())
    strategy = _RecordingStrategy()

    report = CdcApplyCertificationService(strategies={"clickhouse_replacing_merge_tree": strategy}).certify(
        output_dir=tmp_path / "cert",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        source_dataset="dbo.orders",
        target_dataset="analytics.orders",
        fixture_json=fixture,
    )

    assert report.passed is True
    assert strategy.fixture is not None
    assert strategy.fixture.window_end == "0x00000014"
