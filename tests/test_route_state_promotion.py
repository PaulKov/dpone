from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from dpone.ops.route_execution import RouteExecutionService
from dpone.ops.route_state_promotion import RouteStatePromotionService

UTC = timezone.utc  # noqa: UP017


class _Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 6, 13, 13, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value


def _ledger(
    tmp_path: Path,
    clock: _Clock,
    *,
    stage: str = "loaded_to_staging",
    status: str = "succeeded",
    source_boundary: str = "lsn:001",
    sink_boundary: str = "clickhouse:events:10",
    lease: bool = True,
) -> Path:
    report = RouteExecutionService(clock=clock.now).record_step(
        output_dir=tmp_path / "ledger",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        stage=stage,
        status=status,
        runner_id="worker-a",
        source_boundary=source_boundary,
        sink_boundary=sink_boundary,
        idempotency_key="load-window-001",
        lease_ttl_seconds=300 if lease else None,
    )
    return Path(report.json_path)


def _fencing_token(ledger_json: Path) -> str:
    payload = json.loads(ledger_json.read_text(encoding="utf-8"))
    return str(payload["lease"]["fencing_token"])


def test_route_state_promotion_advances_state_after_matching_ledger_and_receipt(tmp_path: Path) -> None:
    clock = _Clock()
    ledger_json = _ledger(tmp_path, clock)

    report = RouteStatePromotionService(clock=clock.now).promote(
        output_dir=tmp_path / "promotion",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        ledger_json=ledger_json,
        proposed_state="lsn:001",
        source_boundary="lsn:001",
        sink_boundary="clickhouse:events:10",
        idempotency_key="promote-lsn-001",
        fencing_token=_fencing_token(ledger_json),
        commit_token="clickhouse-part-0001",
        target="analytics.orders_cdc",
        rows_applied=10,
        events_applied=10,
    )
    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

    assert report.passed is True
    assert payload["schema_version"] == "dpone.route_state_promotion.v1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__cdc"
    assert payload["promoted_state"]["source_state"] == "lsn:001"
    assert payload["receipt"]["commit_token"] == "clickhouse-part-0001"
    assert payload["state_backend"] == "local_json"
    assert Path(payload["state_path"]).exists()


def test_route_state_promotion_is_idempotent_for_same_receipt(tmp_path: Path) -> None:
    clock = _Clock()
    ledger_json = _ledger(tmp_path, clock)
    service = RouteStatePromotionService(clock=clock.now)
    kwargs = {
        "output_dir": tmp_path / "promotion",
        "source": "mssql",
        "sink": "clickhouse",
        "strategy": "cdc",
        "dataset": "dbo.orders",
        "run_id": "run-1",
        "ledger_json": ledger_json,
        "proposed_state": "lsn:001",
        "source_boundary": "lsn:001",
        "sink_boundary": "clickhouse:events:10",
        "idempotency_key": "promote-lsn-001",
        "fencing_token": _fencing_token(ledger_json),
        "commit_token": "clickhouse-part-0001",
        "target": "analytics.orders_cdc",
        "rows_applied": 10,
        "events_applied": 10,
    }

    first = service.promote(**kwargs)
    replay = service.promote(**kwargs)

    assert first.passed is True
    assert replay.passed is True
    assert "state_promotion.idempotent_replay" in replay.warnings
    assert replay.promoted_state is not None
    assert replay.promoted_state.version == 1


def test_route_state_promotion_blocks_without_durable_sink_success(tmp_path: Path) -> None:
    clock = _Clock()
    ledger_json = _ledger(tmp_path, clock, stage="extracting", status="running", lease=False)

    report = RouteStatePromotionService(clock=clock.now).promote(
        output_dir=tmp_path / "promotion",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        ledger_json=ledger_json,
        proposed_state="lsn:001",
        source_boundary="lsn:001",
        sink_boundary="clickhouse:events:10",
        idempotency_key="promote-lsn-001",
        commit_token="clickhouse-part-0001",
        target="analytics.orders_cdc",
    )

    assert report.passed is False
    assert "state_promotion.missing_durable_sink_success" in report.blockers


def test_route_state_promotion_blocks_boundary_and_fencing_mismatch(tmp_path: Path) -> None:
    clock = _Clock()
    ledger_json = _ledger(tmp_path, clock)

    report = RouteStatePromotionService(clock=clock.now).promote(
        output_dir=tmp_path / "promotion",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        ledger_json=ledger_json,
        proposed_state="lsn:002",
        source_boundary="lsn:002",
        sink_boundary="clickhouse:events:11",
        idempotency_key="promote-lsn-002",
        fencing_token="wrong-token",
        commit_token="clickhouse-part-0002",
        target="analytics.orders_cdc",
    )

    assert report.passed is False
    assert "state_promotion.boundary_mismatch" in report.blockers
    assert "state_promotion.fencing_token_mismatch" in report.blockers
