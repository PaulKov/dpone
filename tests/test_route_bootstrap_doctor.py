from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.connection_doctor import ConnectionDoctorService
from dpone.ops.route_bootstrap import RouteBootstrapService
from dpone.ops.route_doctor import RouteDoctorService
from dpone.ops.source_discovery import SourceDiscoveryService


def test_route_bootstrap_doctor_public_exports_are_available() -> None:
    from dpone.ops import ConnectionDoctorService as ExportedConnectionDoctorService
    from dpone.ops import RouteBootstrapService as ExportedRouteBootstrapService
    from dpone.ops import RouteDoctorService as ExportedRouteDoctorService
    from dpone.ops import SourceDiscoveryService as ExportedSourceDiscoveryService
    from dpone.ops.routes import OnboardingCheck

    assert ExportedConnectionDoctorService is ConnectionDoctorService
    assert ExportedSourceDiscoveryService is SourceDiscoveryService
    assert ExportedRouteBootstrapService is RouteBootstrapService
    assert ExportedRouteDoctorService is RouteDoctorService
    assert OnboardingCheck.__name__ == "OnboardingCheck"


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _schema(path: Path) -> Path:
    return _write_json(
        path,
        {
            "tables": [
                {
                    "schema": "dbo",
                    "name": "orders",
                    "row_count": 10000,
                    "columns": [
                        {"name": "order_id", "type": "int", "nullable": False},
                        {"name": "updated_at", "type": "datetime2", "nullable": False},
                        {"name": "payload", "type": "nvarchar(max)", "nullable": True},
                    ],
                }
            ]
        },
    )


def test_connection_doctor_reports_tools_env_extras_and_blocks_missing_required_tool(
    monkeypatch,
    tmp_path: Path,
) -> None:
    monkeypatch.setenv("DPONE_TEST_MSSQL_DSN", "Driver={ODBC Driver 18 for SQL Server};Server=localhost")

    report = ConnectionDoctorService(tool_resolver=lambda name: "/usr/bin/python" if name == "python" else None).check(
        output_dir=tmp_path / "connection-doctor",
        source="postgres",
        sink="mssql",
        strategy="incremental_merge",
        required_tools=("python", "definitely_missing_dpone_tool"),
        required_env=("DPONE_TEST_MSSQL_DSN", "DPONE_TEST_CLICKHOUSE_DSN"),
        optional_env=("DPONE_TEST_OPTIONAL",),
        python_imports=("json", "definitely_missing_dpone_module"),
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    markdown = Path(report.markdown_path).read_text(encoding="utf-8")

    assert payload["schema_version"] == "dpone.connection_doctor.v1"
    assert payload["route"]["case_id"] == "postgres_to_mssql__incremental_merge"
    assert payload["passed"] is False
    assert payload["status"] == "blocked"
    assert "tool.definitely_missing_dpone_tool.missing" in payload["blockers"]
    assert "env.DPONE_TEST_CLICKHOUSE_DSN.missing" in payload["blockers"]
    assert "python_import.definitely_missing_dpone_module.missing" in payload["blockers"]
    assert payload["profile"]["install_extras"] == ["postgres", "mssql"]
    assert any(check["name"] == "tool.python" and check["passed"] is True for check in payload["checks"])
    assert any(check["name"] == "env.DPONE_TEST_OPTIONAL" and check["required"] is False for check in payload["checks"])
    assert "Connection doctor" in markdown


def test_source_discovery_reads_schema_json_and_marks_key_cursor_candidates(tmp_path: Path) -> None:
    report = SourceDiscoveryService().discover(
        output_dir=tmp_path / "source-discover",
        source="mssql",
        schema_json=_schema(tmp_path / "schema.json"),
        dataset="dbo.orders",
    )

    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    table = payload["tables"][0]

    assert payload["schema_version"] == "dpone.source_discovery.v1"
    assert payload["passed"] is True
    assert payload["source"] == "mssql"
    assert table["qualified_name"] == "dbo.orders"
    assert table["row_count"] == 10000
    assert table["primary_key_candidates"] == ["order_id"]
    assert table["cursor_candidates"] == ["updated_at"]
    assert table["columns"][0]["kind"] == "integer"
    assert table["columns"][2]["risk_level"] == "warning"
    assert Path(report.markdown_path).exists()


def test_route_bootstrap_generates_manifest_commands_and_type_risk_summary(tmp_path: Path) -> None:
    discovery = SourceDiscoveryService().discover(
        output_dir=tmp_path / "source-discover",
        source="mssql",
        schema_json=_schema(tmp_path / "schema.json"),
        dataset="dbo.orders",
    )

    report = RouteBootstrapService().bootstrap(
        output_dir=tmp_path / "route-bootstrap",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        dataset="dbo.orders",
        source_discovery_json=discovery.json_path,
        manifest_id="orders_mssql_to_clickhouse",
    )
    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    manifest = json.loads(Path(payload["manifest_path"]).read_text(encoding="utf-8"))

    assert payload["schema_version"] == "dpone.route_bootstrap.v1"
    assert payload["passed"] is True
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert payload["manifest"]["id"] == "orders_mssql_to_clickhouse"
    assert manifest["processes"][0]["source"]["type"] == "mssql"
    assert manifest["processes"][0]["sink"]["type"] == "clickhouse"
    assert "payload" in payload["type_risks"]["warning_columns"]
    assert any("route-readiness" in command for command in payload["next_commands"])
    assert any("connection-doctor" in command for command in payload["next_commands"])


def test_route_doctor_aggregates_artifacts_and_blocks_until_bootstrap_exists(tmp_path: Path) -> None:
    connection = ConnectionDoctorService(tool_resolver=lambda name: "/usr/bin/python").check(
        output_dir=tmp_path / "connection-doctor",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        required_tools=("python",),
    )
    discovery = SourceDiscoveryService().discover(
        output_dir=tmp_path / "source-discover",
        source="mssql",
        schema_json=_schema(tmp_path / "schema.json"),
        dataset="dbo.orders",
    )

    blocked = RouteDoctorService().diagnose(
        output_dir=tmp_path / "route-doctor-blocked",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={
            "connection_doctor": connection.json_path,
            "source_discovery": discovery.json_path,
        },
        required_artifacts=("connection_doctor", "source_discovery", "route_bootstrap"),
    )

    bootstrap = RouteBootstrapService().bootstrap(
        output_dir=tmp_path / "route-bootstrap",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        dataset="dbo.orders",
        source_discovery_json=discovery.json_path,
    )
    ready = RouteDoctorService().diagnose(
        output_dir=tmp_path / "route-doctor-ready",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        artifacts={
            "connection_doctor": connection.json_path,
            "source_discovery": discovery.json_path,
            "route_bootstrap": bootstrap.json_path,
        },
    )

    assert blocked.passed is False
    assert "route_bootstrap.missing" in blocked.blockers
    assert ready.passed is True
    assert ready.status == "ready"
    assert ready.route.case_id == "mssql_to_clickhouse__incremental_merge"
    assert json.loads(Path(ready.json_path).read_text(encoding="utf-8"))["score"] == 100.0
