from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from dpone.ops.route_execution import RouteExecutionService
from dpone.ops.route_state_promotion import RouteStatePromotionService
from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.state_promotion_models import RouteStateRecord
from dpone.ops.routes.state_store_sqlite import SqliteRouteStateStore

UTC = timezone.utc  # noqa: UP017


class _Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 6, 13, 14, 0, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value


def _record(route: RouteKey, *, source_state: str, version: int = 1) -> RouteStateRecord:
    return RouteStateRecord(
        route=route,
        dataset="dbo.orders",
        source_state=source_state,
        source_boundary=source_state,
        sink_boundary="clickhouse:events:10",
        run_id="run-1",
        idempotency_key=f"promote-{source_state}",
        fencing_token="token-1",
        commit_token="commit-1",
        promoted_at="2026-06-13T14:00:00+00:00",
        version=version,
    )


def test_sqlite_route_state_store_rejects_stale_compare_and_swap(tmp_path: Path) -> None:
    route = RouteKey.of("mssql", "clickhouse", "cdc")
    store = SqliteRouteStateStore(db_path=tmp_path / "state.sqlite3")

    first_path, first_written = store.write_state_if_version(
        output_dir=tmp_path / "reports",
        route=route,
        dataset="dbo.orders",
        expected_version=0,
        record=_record(route, source_state="lsn:001"),
    )
    second_path, second_written = store.write_state_if_version(
        output_dir=tmp_path / "reports",
        route=route,
        dataset="dbo.orders",
        expected_version=0,
        record=_record(route, source_state="lsn:002"),
    )
    current = store.read_state(output_dir=tmp_path / "reports", route=route, dataset="dbo.orders")

    assert first_written is True
    assert second_written is False
    assert first_path == second_path == tmp_path / "state.sqlite3"
    assert current is not None
    assert current.source_state == "lsn:001"


def test_sqlite_route_state_promotion_is_shared_between_service_instances(tmp_path: Path) -> None:
    clock = _Clock()
    ledger = RouteExecutionService(clock=clock.now).record_step(
        output_dir=tmp_path / "ledger",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        stage="loaded_to_staging",
        status="succeeded",
        runner_id="worker-a",
        source_boundary="lsn:001",
        sink_boundary="clickhouse:events:10",
        idempotency_key="load-window-001",
    )
    db_path = tmp_path / "state.sqlite3"
    first_service = RouteStatePromotionService(state_store=SqliteRouteStateStore(db_path=db_path), clock=clock.now)
    second_service = RouteStatePromotionService(state_store=SqliteRouteStateStore(db_path=db_path), clock=clock.now)

    first = first_service.promote(
        output_dir=tmp_path / "promotion",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        ledger_json=ledger.json_path,
        proposed_state="lsn:001",
        source_boundary="lsn:001",
        sink_boundary="clickhouse:events:10",
        idempotency_key="promote-lsn-001",
        commit_token="clickhouse-part-0001",
        target="analytics.orders_cdc",
    )
    replay = second_service.promote(
        output_dir=tmp_path / "promotion",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        ledger_json=ledger.json_path,
        proposed_state="lsn:001",
        source_boundary="lsn:001",
        sink_boundary="clickhouse:events:10",
        idempotency_key="promote-lsn-001",
        commit_token="clickhouse-part-0001",
        target="analytics.orders_cdc",
    )

    assert first.passed is True
    assert replay.passed is True
    assert "state_promotion.idempotent_replay" in replay.warnings
    assert replay.state_backend == "sqlite"
    assert replay.promoted_state is not None
    assert replay.promoted_state.version == 1
