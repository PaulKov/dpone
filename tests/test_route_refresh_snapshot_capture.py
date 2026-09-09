from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.refresh_verification_models import typed_hash


class _RowsReader:
    def __init__(self, rows_by_ordinal: dict[int, list[dict[str, object]]]) -> None:
        self.rows_by_ordinal = rows_by_ordinal
        self.requests: list[object] = []

    def read_rows(self, request: object) -> list[dict[str, object]]:
        self.requests.append(request)
        return self.rows_by_ordinal[int(getattr(request, "ordinal"))]


class _ExplodingRowsReader:
    def read_rows(self, request: object) -> list[dict[str, object]]:
        del request
        raise AssertionError("reader should not be called for blocked execution")


def test_route_refresh_snapshot_capture_writes_snapshots_for_verification(tmp_path: Path) -> None:
    from dpone.ops.route_refresh_snapshot_capture import RouteRefreshSnapshotCaptureService

    execution_json = _execution_json(tmp_path)
    source_rows = {
        1: [
            {"order_id": 1, "amount": Decimal("10.5000"), "status": "new"},
            {"order_id": 2, "amount": Decimal("20.0000"), "status": "paid"},
        ],
        2: [{"order_id": 3, "amount": Decimal("30.2500"), "status": "packed"}],
    }
    sink_rows = {
        1: [
            {"order_id": "1", "amount": "10.50", "status": "new"},
            {"order_id": "2", "amount": "20.00", "status": "paid"},
        ],
        2: [{"order_id": "3", "amount": "30.25", "status": "packed"}],
    }

    report = RouteRefreshSnapshotCaptureService(
        source_reader=_RowsReader(source_rows),
        sink_reader=_RowsReader(sink_rows),
    ).capture(
        route_refresh_execution_json=execution_json,
        output_dir=tmp_path / "capture",
        runner_id="operator-a",
        key_columns=("order_id",),
        boundary_column="order_id",
        columns=("order_id", "amount", "status"),
        type_hints={"order_id": "int", "amount": "decimal(18,2)", "status": "string"},
    )

    assert report.passed is True
    assert report.status == "captured"
    assert report.summary == {
        "chunks_total": 2,
        "chunks_captured": 2,
        "chunks_failed": 0,
        "source_rows": 3,
        "sink_rows": 3,
        "column_count": 3,
    }
    assert Path(report.source_snapshot_json).is_file()
    assert Path(report.sink_snapshot_json).is_file()
    assert Path(report.json_path).is_file()
    assert Path(report.markdown_path).is_file()

    source_snapshot = json.loads(Path(report.source_snapshot_json).read_text(encoding="utf-8"))
    sink_snapshot = json.loads(Path(report.sink_snapshot_json).read_text(encoding="utf-8"))
    assert source_snapshot["schema_version"] == "dpone.route_refresh_snapshot.v1"
    assert sink_snapshot["schema_version"] == "dpone.route_refresh_snapshot.v1"
    assert source_snapshot["side"] == "source"
    assert sink_snapshot["side"] == "sink"
    assert source_snapshot["columns"] == ["order_id", "amount", "status"]
    assert source_snapshot["chunks"][0]["row_count"] == 2
    assert source_snapshot["chunks"][0]["min_boundary"] == "1"
    assert source_snapshot["chunks"][0]["max_boundary"] == "2"
    assert source_snapshot["chunks"][0]["typed_hash"] == sink_snapshot["chunks"][0]["typed_hash"]
    assert source_snapshot["chunks"][0]["typed_hash"] == typed_hash(
        source_rows[1],
        columns=("order_id", "amount", "status"),
        key_columns=("order_id",),
        type_hints={"order_id": "int", "amount": "decimal(18,2)", "status": "string"},
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    assert payload["schema_version"] == "dpone.route_refresh_snapshot_capture.v1"
    assert payload["artifacts"][0]["name"] == "source_route_refresh_snapshot"
    assert payload["artifacts"][1]["name"] == "sink_route_refresh_snapshot"


def test_route_refresh_snapshot_capture_recovers_bounds_from_legacy_execution_receipts(tmp_path: Path) -> None:
    from dpone.ops.route_refresh_snapshot_capture import RouteRefreshSnapshotCaptureService

    execution_json = _execution_json(tmp_path, include_chunk_bounds=False)
    source_reader = _RowsReader({1: [{"order_id": 1}], 2: [{"order_id": 3}]})
    sink_reader = _RowsReader({1: [{"order_id": 1}], 2: [{"order_id": 3}]})

    report = RouteRefreshSnapshotCaptureService(source_reader=source_reader, sink_reader=sink_reader).capture(
        route_refresh_execution_json=execution_json,
        output_dir=tmp_path / "capture",
        runner_id="operator-a",
        key_columns=("order_id",),
        boundary_column="order_id",
        columns=("order_id",),
        type_hints={"order_id": "int"},
    )

    assert report.passed is True
    assert [(getattr(item, "start"), getattr(item, "end")) for item in source_reader.requests] == [
        ("1", "2"),
        ("3", "3"),
    ]


def test_route_refresh_snapshot_capture_records_duplicate_and_null_keys(tmp_path: Path) -> None:
    from dpone.ops.routes.refresh_snapshot_capture_reader import snapshot_from_rows

    snapshot = snapshot_from_rows(
        [
            {"order_id": 1, "amount": "10.00"},
            {"order_id": 1, "amount": "10.00"},
            {"order_id": None, "amount": "11.00"},
        ],
        columns=("order_id", "amount"),
        key_columns=("order_id",),
        boundary_column="order_id",
        type_hints={"order_id": "int", "amount": "decimal(18,2)"},
    )

    assert snapshot.row_count == 3
    assert snapshot.duplicate_keys == 1
    assert snapshot.null_keys == 1
    assert snapshot.sample_rows == 0


def test_route_refresh_snapshot_capture_fails_closed_for_non_executed_receipt(tmp_path: Path) -> None:
    from dpone.ops.route_refresh_snapshot_capture import RouteRefreshSnapshotCaptureService

    execution_json = _execution_json(tmp_path, status="dry_run", passed=True, executed=False)

    report = RouteRefreshSnapshotCaptureService(
        source_reader=_ExplodingRowsReader(),
        sink_reader=_ExplodingRowsReader(),
    ).capture(
        route_refresh_execution_json=execution_json,
        output_dir=tmp_path / "capture",
        runner_id="operator-a",
        key_columns=("order_id",),
        boundary_column="order_id",
        columns=("order_id",),
    )

    assert report.passed is False
    assert report.status == "blocked"
    assert "route_refresh_snapshot_capture.execution_not_succeeded" in report.blockers
    assert "route_refresh_snapshot_capture.execution_not_run" in report.blockers
    assert report.summary["chunks_captured"] == 0
    assert Path(report.json_path).is_file()


def test_route_refresh_snapshot_capture_service_stays_backend_agnostic() -> None:
    service_path = Path("src/dpone/ops/route_refresh_snapshot_capture.py")
    if not service_path.exists():
        pytest.fail("route refresh snapshot capture service is missing")
    service_text = service_path.read_text(encoding="utf-8")

    forbidden_fragments = {
        "Mssql",
        "ClickHouse",
        "Postgres",
        "refresh_executors",
        "BcpRunner",
        "ClickHouseClientRunner",
    }

    assert [fragment for fragment in forbidden_fragments if fragment in service_text] == []


def _execution_json(
    tmp_path: Path,
    *,
    route: RouteKey | None = None,
    status: str = "succeeded",
    passed: bool = True,
    executed: bool = True,
    include_chunk_bounds: bool = True,
) -> Path:
    path = tmp_path / "route_refresh_execution.json"
    chunk_1 = {
        "ordinal": 1,
        "idempotency_key": "route:analytics.orders:1:1..2",
        "status": "succeeded",
        "passed": True,
        "artifact_path": str(tmp_path / "chunk_1.json"),
    }
    chunk_2 = {
        "ordinal": 2,
        "idempotency_key": "route:analytics.orders:2:3..3",
        "status": "succeeded",
        "passed": True,
        "artifact_path": str(tmp_path / "chunk_2.json"),
    }
    if include_chunk_bounds:
        chunk_1.update(
            {
                "start": "1",
                "end": "2",
                "source_boundary": "1..2",
                "sink_boundary": "1..2",
            }
        )
        chunk_2.update(
            {
                "start": "3",
                "end": "3",
                "source_boundary": "3..3",
                "sink_boundary": "3..3",
            }
        )
    payload: dict[str, Any] = {
        "schema_version": "dpone.route_refresh_execution.v1",
        "route": (route or RouteKey.of("mssql", "clickhouse", "incremental_merge")).to_dict(),
        "dataset": "analytics.orders",
        "runner_id": "refresh-runner",
        "mode": "execute" if executed else "dry_run",
        "executed": executed,
        "status": status,
        "passed": passed,
        "chunks": [chunk_1, chunk_2],
        "blockers": [],
        "warnings": [],
    }
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path
