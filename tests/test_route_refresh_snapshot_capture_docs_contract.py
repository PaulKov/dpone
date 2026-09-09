from __future__ import annotations

from pathlib import Path


def test_route_refresh_snapshot_capture_docs_cover_user_developer_ci_and_architecture() -> None:
    requirements = {
        Path("docs/route-refresh-execute.md"): (
            "Route refresh snapshot capture",
            "route-refresh-capture-snapshots",
            "route_refresh_snapshot_capture.json",
            "source_route_refresh_snapshot.json",
            "sink_route_refresh_snapshot.json",
        ),
        Path("docs/developer-route-refresh-execute.md"): (
            "Route refresh snapshot capture architecture",
            "RouteRefreshSnapshotCaptureService",
            "RouteRefreshRowsReader",
            "refresh_snapshot_capture_reader",
            "refresh_snapshot_capture_registry",
        ),
        Path("docs/ops-cli.md"): (
            "Route refresh capture snapshots",
            "route-refresh-capture-snapshots",
        ),
        Path("docs/ci-cd.md"): (
            "route_refresh_snapshot_capture.json",
            "test_route_refresh_snapshot_capture.py",
            "test_cli_route_refresh_snapshot_capture_command.py",
            "10,000 rows",
            "200 columns",
        ),
        Path("docs/architecture.md"): (
            "RouteRefreshSnapshotCaptureService",
            "route_refresh_snapshot_capture.json",
        ),
        Path("docs/source-sink/mssql-to-clickhouse.md"): (
            "route-refresh-capture-snapshots",
            "source_route_refresh_snapshot.json",
            "sink_route_refresh_snapshot.json",
            "10,000 rows",
            "200 columns",
            "schema evolution",
        ),
        Path("docs/source-sink/postgres-to-mssql.md"): (
            "route-refresh-capture-snapshots",
            "source_route_refresh_snapshot.json",
            "sink_route_refresh_snapshot.json",
            "10,000 rows",
            "200 columns",
            "schema evolution",
        ),
    }

    missing: list[str] = []
    for path, fragments in requirements.items():
        text = path.read_text(encoding="utf-8")
        missing.extend(f"{path}:{fragment}" for fragment in fragments if fragment not in text)

    assert missing == []


def test_live_certification_workflow_attaches_snapshot_capture_artifacts() -> None:
    text = Path(".github/workflows/live-certification.yml").read_text(encoding="utf-8")

    assert "route_refresh_snapshot_capture.json" in text
    assert "source_route_refresh_snapshot.json" in text
    assert "sink_route_refresh_snapshot.json" in text
    assert "route_refresh_snapshot_capture" in text
