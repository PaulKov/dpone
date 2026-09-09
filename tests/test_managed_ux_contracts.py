from __future__ import annotations

import json
import threading
from http.server import ThreadingHTTPServer
from pathlib import Path
from types import SimpleNamespace
from urllib.error import HTTPError
from urllib.request import urlopen

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.commands.plan_cmd import _render_text as render_plan_text
from dpone.commands.registry import get_commands
from dpone.commands.studio_cmd import build_studio_handler
from dpone.readiness.managed import (
    ConnectorScaffoldService,
    ExecutionPlanService,
    PerformanceAdvisor,
    QualityService,
    RunArtifactWriter,
    StateInspectorService,
)
from dpone.readiness.studio_entrypoint import studio_metadata
from dpone.readiness.studio_http_models import StudioHttpConfig


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_managed_cli_commands_are_registered() -> None:
    names = {command.name for command in get_commands()}

    assert {"doctor", "init", "plan", "run-report", "state", "connectors", "perf", "studio"}.issubset(names)


def test_doctor_supports_profile_and_markdown_without_secret_leak(monkeypatch: pytest.MonkeyPatch, capsys) -> None:
    _patch_context(monkeypatch)
    monkeypatch.setenv("DPONE_TEST_SECRET_PASSWORD", "super-secret-value")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["doctor", "--profile", "ci", "--format", "md"])

    assert exc.value.code in {0, 1}
    out = capsys.readouterr().out
    assert "# dpone doctor" in out
    assert "profile" in out
    assert "super-secret-value" not in out


def test_init_generates_manifest_env_and_smoke_command(tmp_path: Path) -> None:
    output = tmp_path / "orders.batch.yaml"

    result = ConnectorScaffoldService().generate_init_bundle(
        output_path=output,
        source_type="postgres",
        sink_type="mssql",
        source_connection="pg_src",
        sink_connection="mssql_dwh",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        strategy="incremental_merge",
        unique_key="id",
    )

    assert output.exists()
    assert result.manifest_path == output
    assert result.env_example_path.exists()
    assert result.smoke_command.startswith("dpone plan ")
    assert "schema_evolution" in output.read_text(encoding="utf-8")
    manifest = yaml.safe_load(output.read_text(encoding="utf-8"))
    assert "checks" not in manifest["quality"]
    assert manifest["quality"]["gates"] == [
        {
            "id": "target_min_rows",
            "type": "min_rows",
            "side": "target",
            "threshold": 1,
            "severity": "warning",
        }
    ]


def test_init_accepts_output_directory(tmp_path: Path) -> None:
    output_dir = tmp_path / "bundle"
    output_dir.mkdir()

    result = ConnectorScaffoldService().generate_init_bundle(
        output_path=output_dir,
        source_type="mssql",
        sink_type="clickhouse",
        source_connection="mssql_src",
        sink_connection="ch_dwh",
        source_schema="dbo",
        source_table="orders",
        target_schema="dwh",
        target_table="orders",
        strategy="incremental_merge",
        unique_key="id",
    )

    assert result.manifest_path == output_dir / "manifest.yaml"
    assert result.manifest_path.exists()
    assert result.env_example_path == output_dir / "manifest.env.example"
    assert result.env_example_path.exists()
    assert "dpone plan" in result.smoke_command


def test_execution_plan_renders_bulk_state_schema_and_quality_sections(tmp_path: Path) -> None:
    manifest = tmp_path / "orders.batch.yaml"
    ConnectorScaffoldService().generate_init_bundle(
        output_path=manifest,
        source_type="postgres",
        sink_type="mssql",
        source_connection="pg_src",
        sink_connection="mssql_dwh",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        strategy="incremental_merge",
        unique_key="id",
    )

    plan = ExecutionPlanService().plan_manifest(manifest, selector="public.orders", apply_safe_schema=False)

    assert plan["bulk_path"] == "postgres_copy_to_mssql_bcp"
    assert plan["staging"]["staging_first"] is True
    assert plan["schema_evolution"]["enabled"] is True
    assert plan["type_matrix"]["available"] is True
    assert plan["type_matrix"]["profile"] == "postgres_to_mssql_native_v2"
    assert plan["type_matrix"]["explain_command"].startswith("dpone schema type-matrix")
    assert "--source postgres" in plan["type_matrix"]["explain_command"]
    assert "--sink mssql" in plan["type_matrix"]["explain_command"]
    assert plan["state"]["backend"] == "mssql"
    assert plan["state"]["connection_id"] == "sink"
    assert plan["state"]["atomicity"] == "target_atomic"
    assert plan["state"]["provisioning"] == "external"
    assert "quality" in plan
    assert "ALTER" not in json.dumps(plan)


def test_execution_plan_uses_canonical_empty_quality_gates(tmp_path: Path) -> None:
    manifest = tmp_path / "orders.batch.yaml"
    ConnectorScaffoldService().generate_init_bundle(
        output_path=manifest,
        source_type="postgres",
        sink_type="mssql",
        source_connection="pg_src",
        sink_connection="mssql_dwh",
        source_schema="public",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        strategy="incremental_merge",
        unique_key="id",
    )
    payload = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    payload.pop("quality", None)
    manifest.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    plan = ExecutionPlanService().plan_manifest(manifest, selector="public.orders")

    assert plan["quality"] == {"gates": []}


def test_execution_plan_exposes_required_parquet_object_storage_pull_before_live_preflight(tmp_path: Path) -> None:
    manifest = tmp_path / "orders.batch.yaml"
    ConnectorScaffoldService().generate_init_bundle(
        output_path=manifest,
        source_type="mssql",
        sink_type="clickhouse",
        source_connection="mssql_src",
        sink_connection="clickhouse_dwh",
        source_schema="dbo",
        source_table="orders",
        target_schema="analytics",
        target_table="orders",
        strategy="full_refresh",
        unique_key=None,
    )
    payload = yaml.safe_load(manifest.read_text(encoding="utf-8"))
    source_options = payload["defaults"]["source"]["options"]
    source_options["columns"] = [{"name": "id", "type": "int"}, {"name": "created_at", "type": "datetime2(7)"}]
    source_options["native_transfer"] = {
        "snapshot": {
            "columnar_fast_path": {
                "mode": "required",
                "provider": "object_storage_pull",
                "object_storage": {
                    "uri_prefix": "s3://dpone-stage/orders/{run_id}/",
                    "format": "parquet",
                },
            }
        }
    }
    manifest.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")

    plan = ExecutionPlanService().plan_manifest(manifest, selector="dbo.orders")

    assert plan["bulk_path"] == "mssql_parquet_object_storage_to_clickhouse_pull"
    assert plan["columnar_fast_path"]["requested_mode"] == "required"
    assert plan["columnar_fast_path"]["requested_provider"] == "object_storage_pull"
    assert plan["columnar_fast_path"]["selected_provider"] == "blocked"
    assert plan["columnar_fast_path"]["should_start_source_io"] is False
    assert "object_storage_preflight_missing" in plan["columnar_fast_path"]["blockers"]
    assert "columnar_fast_path:object_storage_preflight_missing" in plan["warnings"]
    rendered = render_plan_text(plan)
    assert "bulk_path: mssql_parquet_object_storage_to_clickhouse_pull" in rendered
    assert "columnar_fast_path: requested=object_storage_pull mode=required selected=blocked" in rendered
    assert "columnar_fast_path_blocker: object_storage_preflight_missing" in rendered


def test_quality_service_checks_rows_and_modes() -> None:
    rows = [{"id": 1, "name": "Ada"}, {"id": 2, "name": None}, {"id": 2, "name": "Bob"}]
    checks = [
        {"type": "not_null", "column": "name", "mode": "warn"},
        {"type": "unique", "columns": ["id"], "mode": "fail"},
        {"type": "min_rows", "value": 2, "mode": "fail"},
    ]

    result = QualityService().run_checks(rows, checks)

    assert result.passed is False
    assert [item.status for item in result.checks] == ["failed", "failed", "passed"]
    assert result.to_dict()["failed"] == 1
    assert result.to_dict()["warned"] == 1
    assert result.to_dict()["status"] == "failed"


def test_quality_warning_mode_keeps_four_state_contract_without_blocking() -> None:
    result = QualityService().run_checks(
        [{"id": 1, "name": None}],
        [{"type": "not_null", "column": "name", "mode": "warn"}],
    )

    assert result.passed is True
    assert result.status == "passed"
    assert result.checks[0].status == "failed"
    assert result.warned == 1
    assert result.failed == 0


@pytest.mark.parametrize("check_type", ["freshness", "row_count_delta", "source_target_count"])
def test_quality_service_plan_only_checks_are_unverified(check_type: str) -> None:
    result = QualityService().run_checks([{"id": 1}], [{"type": check_type, "mode": "fail"}])

    assert result.passed is False
    assert result.status == "unverified"
    assert result.unverified == 1
    assert result.checks[0].status == "unverified"
    assert result.checks[0].details == {
        "evidence_profile": "plan_only",
        "executed": False,
    }


def test_quality_service_empty_and_all_skipped_runs_never_pass() -> None:
    empty = QualityService().run_checks([], [])
    skipped = QualityService().run_checks(
        [{"id": 1}],
        [{"type": "min_rows", "value": 1, "mode": "skip"}],
    )

    assert empty.passed is False
    assert empty.status == "unverified"
    assert empty.to_dict()["configured"] == 0
    assert empty.to_dict()["executed"] == 0
    assert skipped.passed is False
    assert skipped.status == "skipped"
    assert skipped.skipped == 1
    assert skipped.to_dict()["executed"] == 0


def test_quality_service_partial_skip_never_claims_complete_success() -> None:
    result = QualityService().run_checks(
        [{"id": 1}],
        [
            {"type": "not_null", "column": "id", "mode": "fail"},
            {"type": "unique", "columns": ["id"], "mode": "skip"},
        ],
    )

    assert result.executed == 1
    assert result.skipped == 1
    assert result.passed is False
    assert result.status == "unverified"


def test_quality_service_empty_materialized_run_is_unverified() -> None:
    result = QualityService().run_checks(
        [],
        [{"type": "not_null", "column": "id", "mode": "fail"}],
    )

    assert result.passed is False
    assert result.status == "unverified"
    assert result.unverified == 1
    assert result.checks[0].details == {
        "evidence_profile": "empty_run",
        "executed": False,
    }


def test_run_artifact_writer_emits_json_markdown_and_html(tmp_path: Path) -> None:
    writer = RunArtifactWriter(base_dir=tmp_path)
    artifact = writer.write(
        run_id="run-1",
        pipeline="orders",
        timeline=[{"stage": "extract", "status": "success", "rows": 10}],
        state={"before": None, "after": {"xmin": 42}},
        quality={"passed": True},
    )

    assert artifact.json_path.exists()
    assert artifact.markdown_path.exists()
    assert artifact.html_path.exists()
    assert "orders" in artifact.markdown_path.read_text(encoding="utf-8")


def test_state_inspector_requires_yes_for_destructive_operations() -> None:
    service = StateInspectorService()

    preview = service.reset("postgres", "xmin", "public.orders", yes=False)
    exported = service.export("mssql", "kafka_offsets", "orders")

    assert preview["mode"] == "preview"
    assert preview["requires_yes"] is True
    assert exported["backend"] == "mssql"


def test_performance_advisor_recommends_native_bulk_paths() -> None:
    recommendations = PerformanceAdvisor().advise(
        source_type="postgres",
        sink_type="mssql",
        strategy="incremental_merge",
        options={"estimated_rows": 5_000_000},
    )

    assert any(item.code == "postgres_to_mssql_bcp" for item in recommendations)
    assert any(item.code == "mssql_bulk_target_finalize_tuning" for item in recommendations)
    assert any(item.severity == "high" for item in recommendations)


def test_connector_scaffold_generates_sdk_files(tmp_path: Path) -> None:
    result = ConnectorScaffoldService().scaffold_connector("demo_api", root=tmp_path)

    assert (tmp_path / "src" / "dpone_ext" / "demo_api" / "connector.py").exists()
    assert (tmp_path / "tests" / "test_demo_api_contracts.py").exists()
    assert result["connector"] == "demo_api"


def test_studio_smoke_payload_is_local_and_service_backed() -> None:
    payload = ConnectorScaffoldService().studio_payload(host="127.0.0.1", port=8765)

    assert payload["url"] == "http://127.0.0.1:8765"
    assert payload["mode"] == "local_development_adapter"
    assert payload["ui_status"] == "not_installed"
    assert payload["usability_status"] == "UNVERIFIED"
    assert payload["release_verdict"] == "NO-GO"
    assert payload["rbac"] == "not_implemented"
    assert payload["auth"] == "local_only"
    assert "/api/v1/capabilities" in payload["endpoints"]
    assert "studio_repo" not in payload


def test_studio_metadata_reports_remote_shared_token_without_exposing_token() -> None:
    config = StudioHttpConfig(
        host="0.0.0.0",
        token="secret-token",
        allow_remote=True,
        cors_origins=("https://studio.example",),
    )

    payload = studio_metadata(host=config.host, port=8765, config=config)

    assert payload["auth"] == "shared_token"
    assert payload["remote_enabled"] is True
    assert "secret-token" not in json.dumps(payload)


def test_studio_http_server_exposes_core_endpoints() -> None:
    payload = ConnectorScaffoldService().studio_payload(host="127.0.0.1", port=0)
    server = ThreadingHTTPServer(("127.0.0.1", 0), build_studio_handler(payload))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    base_url = f"http://{host}:{port}"

    try:
        with pytest.raises(HTTPError) as landing:
            urlopen(f"{base_url}/", timeout=5)  # noqa: S310 - local test server
        doctor = json.loads(urlopen(f"{base_url}/api/doctor", timeout=5).read())  # noqa: S310 - local test server
        connectors = json.loads(
            urlopen(f"{base_url}/api/v1/capabilities", timeout=5).read()  # noqa: S310 - local test server
        )
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)

    assert landing.value.code == 404
    assert doctor["profile"] == "local"
    assert connectors["schema"] == "dpone.capability-discovery.v1"
    assert any(item["id"] == "mssql" for item in connectors["connectors"])


def test_managed_cli_json_outputs(monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys) -> None:
    _patch_context(monkeypatch)
    manifest = tmp_path / "orders.batch.yaml"

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "init",
                "--source-type",
                "postgres",
                "--sink-type",
                "mssql",
                "--source-connection",
                "pg_src",
                "--sink-connection",
                "mssql_dwh",
                "--source-schema",
                "public",
                "--source-table",
                "orders",
                "--target-schema",
                "landing",
                "--target-table",
                "orders",
                "--strategy",
                "incremental_merge",
                "--unique-key",
                "id",
                "--out",
                str(manifest),
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0
    init_payload = json.loads(capsys.readouterr().out)
    assert init_payload["manifest_path"] == str(manifest)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["plan", str(manifest), "--selector", "public.orders", "--format", "json"])
    assert exc.value.code == 0
    plan_payload = json.loads(capsys.readouterr().out)
    assert plan_payload["source"]["type"] == "postgres"

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["perf", "advise", str(manifest), "--selector", "public.orders", "--format", "json"])
    assert exc.value.code == 0
    perf_payload = json.loads(capsys.readouterr().out)
    assert perf_payload["recommendations"]
