from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.cdc.catalog import CdcHandoffCatalog
from dpone.ops.cdc.evidence import CdcEvidenceReader
from dpone.ops.cdc.handoff import SnapshotCdcHandoffService
from dpone.ops.cdc.models import CdcStreamKey
from dpone.ops.cdc.policy import CdcHandoffPolicy
from dpone.ops.routes.models import RouteKey


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_cdc_stream_key_normalizes_route_and_dataset_identity() -> None:
    key = CdcStreamKey.of(
        source=" SQLServer ",
        sink=" ClickHouse ",
        strategy=" CDC ",
        source_dataset="dbo.Orders",
        target_dataset="analytics.orders",
    )

    assert key.route.case_id == "mssql_to_clickhouse__cdc"
    assert key.stream_id == "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders"
    assert key.colon_id == "mssql:clickhouse:cdc:dbo.Orders:analytics.orders"
    assert key.to_dict()["source_dataset"] == "dbo.Orders"


def test_cdc_catalog_builds_mssql_clickhouse_profile_from_route_matrix() -> None:
    profile = CdcHandoffCatalog.default().get(RouteKey.of("mssql", "clickhouse", "cdc"))

    assert profile is not None
    assert profile.route.case_id == "mssql_to_clickhouse__cdc"
    assert profile.source_backend == "mssql_cdc"
    assert profile.sink_apply_mode == "clickhouse_replacing_merge_tree"
    assert profile.snapshot_boundary_kind == "mssql_lsn"
    assert profile.native_apply_path == "mssql_cdc_to_clickhouse_typed_staging_apply"
    assert "cdc_snapshot_boundary" in profile.required_evidence
    assert "cdc_apply_correctness" in profile.required_evidence
    assert "typed_cdc_hash" in profile.required_evidence


def test_cdc_policy_blocks_missing_required_evidence(tmp_path: Path) -> None:
    profile = CdcHandoffCatalog.default().get(RouteKey.of("mssql", "clickhouse", "cdc"))
    assert profile is not None
    boundary = _write_json(
        tmp_path / "boundary.json",
        {"passed": True, "summary": "snapshot boundary captured", "snapshot_lsn": "0x0000001"},
    )

    decision = CdcHandoffPolicy().evaluate(
        profile=profile,
        evidence=CdcEvidenceReader().read(profile=profile, artifacts={"cdc_snapshot_boundary": boundary}),
    )

    assert decision.passed is False
    assert decision.level == "blocked"
    assert "cdc_window.missing" in decision.blockers
    assert "cdc_apply_correctness.missing" in decision.blockers
    assert decision.score < 50.0


def test_snapshot_cdc_handoff_service_writes_report_for_mssql_clickhouse(tmp_path: Path) -> None:
    profile = CdcHandoffCatalog.default().get(RouteKey.of("mssql", "clickhouse", "cdc"))
    assert profile is not None
    artifacts = {
        name: _write_json(tmp_path / "input" / f"{name}.json", {"passed": True, "summary": f"{name} ok"})
        for name in profile.required_evidence
    }

    report = SnapshotCdcHandoffService(catalog=CdcHandoffCatalog.default()).evaluate(
        output_dir=tmp_path / "handoff",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        source_dataset="dbo.orders",
        target_dataset="analytics.orders",
        artifacts=artifacts,
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")

    assert report.passed is True
    assert payload["schema_version"] == "dpone.cdc_handoff.v1"
    assert payload["stream"]["stream_id"] == "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders"
    assert payload["profile"]["source_backend"] == "mssql_cdc"
    assert payload["passed"] is True
    assert payload["level"] == "handoff_ready"
    assert "# dpone CDC snapshot handoff" in markdown
    assert "mssql_to_clickhouse__cdc" in markdown


def test_snapshot_cdc_handoff_service_reports_unsupported_routes_without_live_execution(tmp_path: Path) -> None:
    report = SnapshotCdcHandoffService().evaluate(
        output_dir=tmp_path / "handoff",
        source="postgres",
        sink="mssql",
        strategy="cdc",
        source_dataset="public.orders",
        target_dataset="dbo.orders",
        artifacts={},
    )

    assert report.passed is False
    assert report.level == "unknown"
    assert report.blockers == ("cdc.route_unsupported:postgres:mssql:cdc",)
