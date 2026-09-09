from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from uuid import UUID

from dpone.ops.route_refresh_verify import RouteRefreshVerificationService
from dpone.ops.routes.models import RouteKey
from dpone.ops.routes.refresh_verification_models import (
    RouteRefreshSideSnapshot,
    typed_hash,
)
from dpone.ops.routes.refresh_verification_policy import RouteRefreshVerificationPolicy


@dataclass
class _RecordingSnapshotReader:
    side: str
    snapshots: dict[int, RouteRefreshSideSnapshot]
    calls: list[tuple[int, str]]

    def read_chunk(self, request: object) -> RouteRefreshSideSnapshot:
        ordinal = int(getattr(request, "ordinal"))
        self.calls.append((ordinal, self.side))
        return self.snapshots[ordinal]


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _execution_json(tmp_path: Path, *, passed: bool = True, status: str = "succeeded") -> Path:
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")
    chunks = [
        {
            "ordinal": 1,
            "idempotency_key": "mssql_to_clickhouse__incremental_merge:analytics.orders:1:1..2",
            "status": "succeeded" if passed else "failed",
            "passed": passed,
            "rows_read": 2,
            "rows_written": 2,
            "artifact_path": str(tmp_path / "chunks" / "chunk_1.json"),
            "summary": "chunk applied",
            "blockers": [] if passed else ["sink_apply_failed"],
            "warnings": [],
            "duration_seconds": 0.0,
        },
        {
            "ordinal": 2,
            "idempotency_key": "mssql_to_clickhouse__incremental_merge:analytics.orders:2:3..4",
            "status": "succeeded",
            "passed": True,
            "rows_read": 2,
            "rows_written": 2,
            "artifact_path": str(tmp_path / "chunks" / "chunk_2.json"),
            "summary": "chunk applied",
            "blockers": [],
            "warnings": [],
            "duration_seconds": 0.0,
        },
    ]
    return _write_json(
        tmp_path / "execute" / "route_refresh_execution.json",
        {
            "schema_version": "dpone.route_refresh_execution.v1",
            "route": route.to_dict(),
            "dataset": "analytics.orders",
            "runner_id": "operator-a",
            "mode": "execute",
            "executed": True,
            "status": status,
            "passed": passed,
            "ready_for_state_promotion": passed and status == "succeeded",
            "summary": {
                "chunks_total": 2,
                "chunks_succeeded": 2 if passed else 1,
                "chunks_failed": 0 if passed else 1,
                "chunks_skipped": 0,
                "rows_read": 4,
                "rows_written": 4,
            },
            "chunks": chunks,
            "artifacts": [],
            "blockers": [] if passed else ["route_refresh_execution.chunk_failed:1"],
            "warnings": [],
            "next_actions": [],
            "output_dir": str(tmp_path / "execute"),
            "json_path": str(tmp_path / "execute" / "route_refresh_execution.json"),
            "markdown_path": str(tmp_path / "execute" / "route_refresh_execution.md"),
        },
    )


def _snapshot(*, row_count: int, min_boundary: str, max_boundary: str, digest: str) -> RouteRefreshSideSnapshot:
    return RouteRefreshSideSnapshot(
        row_count=row_count,
        min_boundary=min_boundary,
        max_boundary=max_boundary,
        typed_hash=digest,
        duplicate_keys=0,
        null_keys=0,
    )


def test_typed_hash_canonicalizes_rows_by_declared_types() -> None:
    left = [
        {"order_id": 2, "status": "paid", "amount": Decimal("20.00")},
        {"order_id": 1, "status": "new", "amount": Decimal("10.50")},
    ]
    right = [
        {"amount": "10.5", "status": "new", "order_id": "1"},
        {"amount": "20.000", "status": "paid", "order_id": "2"},
    ]

    assert typed_hash(
        left,
        columns=("order_id", "status", "amount"),
        key_columns=("order_id",),
        type_hints={"order_id": "int", "status": "string", "amount": "decimal(18,2)"},
    ) == typed_hash(
        right,
        columns=("order_id", "status", "amount"),
        key_columns=("order_id",),
        type_hints={"order_id": "int", "status": "string", "amount": "decimal(18,2)"},
    )


def test_typed_hash_canonicalizes_physical_contract_types() -> None:
    left = [
        {
            "order_id": 1,
            "payload": bytes.fromhex("00ff10"),
            "business_time": time(1, 2, 3),
            "trace_id": UUID("00000000-0000-0000-0000-000000000123"),
            "business_date": date(2026, 1, 2),
            "created_at": datetime(2026, 1, 2, 3, 4, 5, 120000, tzinfo=UTC),
        }
    ]
    right = [
        {
            "order_id": "1",
            "payload": "00FF10",
            "business_time": "3723",
            "trace_id": "00000000-0000-0000-0000-000000000123",
            "business_date": "2026-01-02",
            "created_at": "2026-01-02 03:04:05.120",
        }
    ]
    hints = {
        "order_id": "int",
        "payload": "varbinary(max)",
        "business_time": "time(7)",
        "trace_id": "uniqueidentifier",
        "business_date": "date",
        "created_at": "datetime2(7)",
    }

    assert typed_hash(left, columns=tuple(hints), key_columns=("order_id",), type_hints=hints) == typed_hash(
        right,
        columns=tuple(hints),
        key_columns=("order_id",),
        type_hints=hints,
    )


def test_route_refresh_verification_policy_blocks_mismatched_snapshots() -> None:
    policy = RouteRefreshVerificationPolicy()

    passed = policy.evaluate_chunk(
        ordinal=1,
        idempotency_key="chunk-1",
        source=_snapshot(row_count=2, min_boundary="1", max_boundary="2", digest="abc"),
        sink=_snapshot(row_count=2, min_boundary="1", max_boundary="2", digest="abc"),
    )
    failed = policy.evaluate_chunk(
        ordinal=2,
        idempotency_key="chunk-2",
        source=_snapshot(row_count=2, min_boundary="1", max_boundary="2", digest="abc"),
        sink=RouteRefreshSideSnapshot(
            row_count=1,
            min_boundary="1",
            max_boundary="3",
            typed_hash="def",
            duplicate_keys=1,
            null_keys=1,
        ),
    )

    assert passed.passed is True
    assert passed.status == "verified"
    assert failed.passed is False
    assert failed.status == "failed"
    assert "route_refresh_verification.row_count_mismatch:2" in failed.blockers
    assert "route_refresh_verification.max_boundary_mismatch:2" in failed.blockers
    assert "route_refresh_verification.typed_hash_mismatch:2" in failed.blockers
    assert "route_refresh_verification.duplicate_keys:2" in failed.blockers
    assert "route_refresh_verification.null_keys:2" in failed.blockers


def test_route_refresh_verify_reads_execution_and_writes_reconciliation_report(tmp_path: Path) -> None:
    digest_1 = "a" * 64
    digest_2 = "b" * 64
    source_reader = _RecordingSnapshotReader(
        side="source",
        snapshots={
            1: _snapshot(row_count=2, min_boundary="1", max_boundary="2", digest=digest_1),
            2: _snapshot(row_count=2, min_boundary="3", max_boundary="4", digest=digest_2),
        },
        calls=[],
    )
    sink_reader = _RecordingSnapshotReader(
        side="sink",
        snapshots={
            1: _snapshot(row_count=2, min_boundary="1", max_boundary="2", digest=digest_1),
            2: _snapshot(row_count=2, min_boundary="3", max_boundary="4", digest=digest_2),
        },
        calls=[],
    )

    report = RouteRefreshVerificationService(source_reader=source_reader, sink_reader=sink_reader).verify(
        route_refresh_execution_json=_execution_json(tmp_path),
        output_dir=tmp_path / "verify",
        runner_id="verifier-a",
        key_columns=("order_id",),
        boundary_column="order_id",
        columns=("order_id", "status", "amount"),
        type_hints={"order_id": "int", "status": "string", "amount": "decimal(18,2)"},
    )
    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")

    assert report.passed is True
    assert report.status == "verified"
    assert report.ready_for_state_promotion is True
    assert source_reader.calls == [(1, "source"), (2, "source")]
    assert sink_reader.calls == [(1, "sink"), (2, "sink")]
    assert payload["schema_version"] == "dpone.route_refresh_verification.v1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert payload["summary"]["chunks_verified"] == 2
    assert payload["artifact_index"]["route_refresh_execution"]["sha256"] != "0" * 64
    assert "Route refresh verification" in markdown


def test_route_refresh_verify_blocks_non_succeeded_execution_before_reading_snapshots(tmp_path: Path) -> None:
    source_reader = _RecordingSnapshotReader(side="source", snapshots={}, calls=[])
    sink_reader = _RecordingSnapshotReader(side="sink", snapshots={}, calls=[])

    report = RouteRefreshVerificationService(source_reader=source_reader, sink_reader=sink_reader).verify(
        route_refresh_execution_json=_execution_json(tmp_path, passed=False, status="partial_failure"),
        output_dir=tmp_path / "verify",
        runner_id="verifier-a",
        key_columns=("order_id",),
        boundary_column="order_id",
        columns=("order_id", "status", "amount"),
    )

    assert report.passed is False
    assert report.status == "blocked"
    assert "route_refresh_verification.execution_not_succeeded" in report.blockers
    assert source_reader.calls == []
    assert sink_reader.calls == []


def test_route_refresh_verify_service_stays_backend_agnostic() -> None:
    service_text = Path("src/dpone/ops/route_refresh_verify.py").read_text(encoding="utf-8")

    forbidden_fragments = {
        "Mssql",
        "ClickHouse",
        "Postgres",
        "refresh_executors",
        "BcpRunner",
    }

    assert [fragment for fragment in forbidden_fragments if fragment in service_text] == []
