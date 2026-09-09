from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.route_refresh_plan import RouteRefreshPlanService


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _route(source: str = "mssql", sink: str = "clickhouse", strategy: str = "incremental_merge") -> dict[str, str]:
    return {"source": source, "sink": sink, "strategy": strategy}


def _artifact(
    path: Path,
    *,
    passed: bool = True,
    route: dict[str, str] | None = None,
    blockers: list[str] | None = None,
) -> Path:
    payload: dict[str, object] = {
        "schema_version": "dpone.test_refresh_evidence.v1",
        "route": route or _route(),
        "passed": passed,
        "summary": "refresh evidence",
        "blockers": blockers if blockers is not None else ([] if passed else ["upstream.failed"]),
    }
    return _write_json(path, payload)


def test_route_refresh_plan_builds_ready_integer_chunks(tmp_path: Path) -> None:
    report = RouteRefreshPlanService().plan(
        output_dir=tmp_path / "refresh",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        dataset="analytics.orders",
        reason="dq_repair",
        window_kind="integer",
        start="1",
        end="250",
        chunk_size=100,
        artifacts={"route_data_quality": _artifact(tmp_path / "route_data_quality.json")},
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")

    assert payload["schema_version"] == "dpone.route_refresh_plan.v1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert payload["dataset"] == "analytics.orders"
    assert payload["reason"] == "dq_repair"
    assert payload["status"] == "ready"
    assert payload["passed"] is True
    assert [chunk["ordinal"] for chunk in payload["chunks"]] == [1, 2, 3]
    assert payload["chunks"][0]["start"] == "1"
    assert payload["chunks"][0]["end"] == "100"
    assert payload["chunks"][2]["start"] == "201"
    assert payload["chunks"][2]["end"] == "250"
    assert payload["chunks"][0]["idempotency_key"].startswith("mssql_to_clickhouse__incremental_merge:")
    assert payload["approval"]["required"] is False
    assert payload["state_rewind"]["safe_to_rewind"] is True
    assert "Route refresh plan" in markdown


def test_route_refresh_plan_builds_multi_chunk_timestamp_windows(tmp_path: Path) -> None:
    report = RouteRefreshPlanService().plan(
        output_dir=tmp_path / "refresh",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        dataset="analytics.orders",
        reason="range_replay",
        window_kind="timestamp",
        start="2025-01-01",
        end="2025-01-10",
        chunk_size=3,  # days per chunk, same contract as backfill step "3d"
        artifacts={"route_data_quality": _artifact(tmp_path / "route_data_quality.json")},
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

    assert payload["status"] == "ready"
    assert [chunk["ordinal"] for chunk in payload["chunks"]] == [1, 2, 3, 4]
    assert payload["chunks"][0]["start"] == "2025-01-01"
    assert payload["chunks"][0]["end"] == "2025-01-04"
    assert payload["chunks"][3]["end"] == "2025-01-11"


def test_route_refresh_plan_timestamp_window_without_chunk_size_stays_single_chunk(tmp_path: Path) -> None:
    report = RouteRefreshPlanService().plan(
        output_dir=tmp_path / "refresh",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        dataset="analytics.orders",
        reason="range_replay",
        window_kind="timestamp",
        start="2025-01-01",
        end="2025-01-10",
        artifacts={"route_data_quality": _artifact(tmp_path / "route_data_quality.json")},
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))

    assert [chunk["ordinal"] for chunk in payload["chunks"]] == [1]


def test_route_refresh_plan_requires_approval_for_state_rewind_and_destructive_refresh(tmp_path: Path) -> None:
    report = RouteRefreshPlanService().plan(
        output_dir=tmp_path / "refresh",
        source="postgres",
        sink="mssql",
        strategy="incremental_merge",
        dataset="dbo.orders",
        reason="manual_resync",
        window_kind="integer",
        start="10",
        end="30",
        chunk_size=10,
        current_state="lsn:200",
        target_state="lsn:100",
        destructive=True,
    )

    assert report.passed is False
    assert report.status == "approval_required"
    assert report.approval.required is True
    assert report.approval.reasons == ("destructive_refresh", "state_rewind")
    assert report.state_rewind.safe_to_rewind is False
    assert "route_refresh.approval_required" in report.blockers


def test_route_refresh_plan_blocks_invalid_window_and_chunk_cap(tmp_path: Path) -> None:
    invalid = RouteRefreshPlanService().plan(
        output_dir=tmp_path / "invalid",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        dataset="analytics.orders",
        reason="range_replay",
        window_kind="integer",
        start="50",
        end="10",
        chunk_size=10,
    )
    too_many = RouteRefreshPlanService().plan(
        output_dir=tmp_path / "too-many",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        dataset="analytics.orders",
        reason="range_replay",
        window_kind="integer",
        start="1",
        end="1000",
        chunk_size=10,
        max_chunks=10,
    )

    assert invalid.passed is False
    assert invalid.status == "blocked"
    assert "route_refresh.window_invalid" in invalid.blockers
    assert too_many.passed is False
    assert too_many.status == "blocked"
    assert "route_refresh.chunk_count_exceeded" in too_many.blockers


def test_route_refresh_plan_blocks_required_evidence_route_mismatch_and_malformed_json(tmp_path: Path) -> None:
    malformed = tmp_path / "route_run_supervisor.json"
    malformed.write_text("{not-json", encoding="utf-8")
    report = RouteRefreshPlanService().plan(
        output_dir=tmp_path / "refresh",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        dataset="analytics.orders",
        reason="retention_gap",
        window_kind="integer",
        start="1",
        end="20",
        chunk_size=10,
        artifacts={
            "route_data_quality": _artifact(
                tmp_path / "route_data_quality.json",
                route=_route(source="postgres", sink="mssql"),
            ),
            "route_run_supervisor": malformed,
        },
        required_evidence=("route_data_quality", "route_run_supervisor", "cdc_retention_check"),
    )

    assert report.passed is False
    assert report.status == "blocked"
    assert "route_data_quality.route_mismatch" in report.blockers
    assert "route_run_supervisor.invalid_json" in report.blockers
    assert "cdc_retention_check.missing" in report.blockers


def test_route_refresh_plan_supports_partition_windows_without_numeric_chunking(tmp_path: Path) -> None:
    report = RouteRefreshPlanService().plan(
        output_dir=tmp_path / "refresh",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        dataset="analytics.orders",
        reason="schema_backfill",
        window_kind="partition",
        start="2026-06-01",
        end="2026-06-03",
        partition="dt=2026-06-01",
        require_approval=True,
    )

    assert report.status == "approval_required"
    assert len(report.chunks) == 1
    assert report.chunks[0].partition == "dt=2026-06-01"
    assert report.approval.reasons == ("policy_required",)


def test_route_refresh_plan_supports_first_routes_from_matrix(tmp_path: Path) -> None:
    for source, sink in (("postgres", "mssql"), ("mssql", "clickhouse")):
        report = RouteRefreshPlanService().plan(
            output_dir=tmp_path / f"{source}-to-{sink}",
            source=source,
            sink=sink,
            strategy="incremental_merge",
            dataset="public.orders",
            reason="initial_backfill",
            window_kind="integer",
            start="1",
            end="5",
            chunk_size=5,
        )

        assert report.profile is not None
        assert report.passed is True
        assert report.route.case_id == f"{source}_to_{sink}__incremental_merge"
