from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dpone.ops.route_execution import RouteExecutionService
from dpone.ops.routes.execution_models import RouteExecutionStage, RouteExecutionStatus, RouteExecutionStep
from dpone.ops.routes.execution_store_sqlite import SqliteRouteExecutionLedgerStore
from dpone.ops.routes.models import RouteKey

UTC = timezone.utc  # noqa: UP017


class _Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 6, 13, 12, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value

    def advance(self, delta: timedelta) -> None:
        self.value += delta


def _step(
    *, idempotency_key: str, stage: RouteExecutionStage = RouteExecutionStage.LOADED_TO_STAGING
) -> RouteExecutionStep:
    return RouteExecutionStep(
        stage=stage,
        status=RouteExecutionStatus.SUCCEEDED,
        runner_id="worker-a",
        created_at="2026-06-13T12:00:00+00:00",
        source_boundary="lsn:001",
        sink_boundary="clickhouse:events:1",
        idempotency_key=idempotency_key,
        artifact_hashes={},
        metadata={},
    )


def test_sqlite_store_rejects_stale_compare_and_swap_append(tmp_path: Path) -> None:
    store = SqliteRouteExecutionLedgerStore(db_path=tmp_path / "shared.sqlite3")
    route = RouteKey.of("mssql", "clickhouse", "cdc")

    first_path, first_appended = store.append_steps_if_version(
        output_dir=tmp_path / "reports",
        route=route,
        dataset="dbo.orders",
        run_id="run-1",
        expected_step_count=0,
        steps=(_step(idempotency_key="window-1"),),
    )
    second_path, second_appended = store.append_steps_if_version(
        output_dir=tmp_path / "reports",
        route=route,
        dataset="dbo.orders",
        run_id="run-1",
        expected_step_count=0,
        steps=(_step(idempotency_key="window-2"),),
    )

    assert first_appended is True
    assert second_appended is False
    assert first_path == second_path == tmp_path / "shared.sqlite3"
    assert [
        step.idempotency_key
        for step in store.read_steps(output_dir=tmp_path, route=route, dataset="dbo.orders", run_id="run-1")
    ] == ["window-1"]


def test_sqlite_route_execution_service_persists_steps_across_service_instances(tmp_path: Path) -> None:
    clock = _Clock()
    db_path = tmp_path / "shared.sqlite3"
    first_service = RouteExecutionService(store=SqliteRouteExecutionLedgerStore(db_path=db_path), clock=clock.now)
    second_service = RouteExecutionService(store=SqliteRouteExecutionLedgerStore(db_path=db_path), clock=clock.now)

    first = first_service.record_step(
        output_dir=tmp_path / "reports",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        stage="loaded_to_staging",
        status="succeeded",
        runner_id="worker-a",
        source_boundary="lsn:001",
        sink_boundary="clickhouse:events:1",
        idempotency_key="window-1",
    )
    second = second_service.record_step(
        output_dir=tmp_path / "reports",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        stage="state_committed",
        status="committed",
        runner_id="worker-b",
        source_boundary="lsn:001",
        sink_boundary="checkpoint:001",
        idempotency_key="checkpoint-1",
    )

    payload = json.loads(Path(second.json_path).read_text(encoding="utf-8"))

    assert first.passed is True
    assert second.passed is True
    assert second.step_count == 2
    assert payload["store_backend"] == "sqlite"
    assert payload["ledger_path"] == str(db_path)
    assert [step["idempotency_key"] for step in payload["steps"]] == ["window-1", "checkpoint-1"]


def test_sqlite_lease_fencing_is_shared_between_service_instances(tmp_path: Path) -> None:
    clock = _Clock()
    db_path = tmp_path / "shared.sqlite3"
    first_service = RouteExecutionService(store=SqliteRouteExecutionLedgerStore(db_path=db_path), clock=clock.now)
    second_service = RouteExecutionService(store=SqliteRouteExecutionLedgerStore(db_path=db_path), clock=clock.now)

    first = first_service.record_step(
        output_dir=tmp_path / "reports",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        stage="extracting",
        status="running",
        runner_id="worker-a",
        lease_ttl_seconds=30,
    )
    blocked = second_service.record_step(
        output_dir=tmp_path / "reports",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-2",
        stage="extracting",
        status="running",
        runner_id="worker-b",
        lease_ttl_seconds=30,
    )
    clock.advance(timedelta(seconds=31))
    takeover = second_service.record_step(
        output_dir=tmp_path / "reports",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-2",
        stage="extracting",
        status="running",
        runner_id="worker-b",
        lease_ttl_seconds=30,
    )

    assert first.passed is True
    assert blocked.passed is False
    assert "route_execution.lease_held_by_another_runner" in blocked.blockers
    assert takeover.passed is True
    assert takeover.lease is not None
    assert takeover.lease.owner == "worker-b"
