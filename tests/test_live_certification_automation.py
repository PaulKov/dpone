from __future__ import annotations

import json
import shlex
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.commands.registry import get_commands
from dpone.ops.benchmark_slo_gate import BenchmarkSloGateService
from dpone.ops.live_certification import LiveCertificationAutomationService
from dpone.ops.live_certification_local import (
    LOCAL_SERVICE_MARKER_MIN_PASSED,
    LOCAL_SERVICE_MARKER_TESTS,
)
from dpone.ops.live_state_reconciliation import LiveStateReconciliationCertificationService
from dpone.ops.performance_certification import PerformanceCertificationService
from dpone.ops.pre_release_checklist import (
    REQUIRED_MINOR_MAJOR_CHECKS,
    REQUIRED_PATCH_CHECKS,
    PreReleaseChecklistService,
)
from dpone.ops.release_evidence_pack import ReleaseEvidencePackService

ROOT = Path(__file__).resolve().parents[1]
MYSQL_LOCAL_ROUTE_NODE_IDS = (
    "tests/integration/mysql/test_mysql_to_postgres_native_transfer_integration.py"
    "::test_mysql_to_postgres_full_refresh_csv_copy",
    "tests/integration/mysql/test_mysql_to_postgres_native_transfer_integration.py"
    "::test_mysql_to_postgres_incremental_merge_watermark",
    "tests/integration/mysql/test_mysql_to_clickhouse_native_transfer_integration.py"
    "::test_mysql_to_clickhouse_full_refresh_tsv",
    "tests/integration/mysql/test_mysql_to_clickhouse_native_transfer_integration.py"
    "::test_mysql_to_clickhouse_incremental_merge_watermark",
    "tests/integration/mysql/test_mysql_to_kafka_native_transfer_integration.py"
    "::test_mysql_to_kafka_full_refresh_csv_produce",
)


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_live_certification_automation_plan_covers_local_services_and_evidence(tmp_path: Path) -> None:
    report = LiveCertificationAutomationService().build(
        output_dir=tmp_path / "live-certification",
        profile="local_live",
        row_count=25000,
        include_vendor_live=False,
    )

    assert report.passed is True
    assert report.profile == "local_live"
    assert report.row_count == 25000
    assert report.credentials_required is False
    assert report.services == ("postgres", "mysql", "mssql", "clickhouse", "kafka", "schema-registry", "minio")
    assert [step.name for step in report.steps] == [
        "install_native_tooling",
        "start_local_services",
        "prepare_local_mssql_database",
        "run_local_service_markers",
        "run_mysql_local_route_cells",
        "run_source_sink_matrix",
        "build_matrix_report",
        "stop_local_services",
    ]
    assert "service_markers.json" in report.required_artifacts
    assert "mysql_local_route_cells.json" in report.required_artifacts
    assert "benchmark_slo_gate.json" not in report.required_artifacts
    assert "connector_certification_pack.json" not in report.required_artifacts
    assert any("docker compose -f docker/docker-compose.integration.yml up -d" in step.command for step in report.steps)
    prepare_step = next(step for step in report.steps if step.name == "prepare_local_mssql_database")
    assert "DB_ID(N'dpone')" in prepare_step.command
    assert "$MSSQL_SA_PASSWORD" in prepare_step.command
    assert "Dp0ne.Strong.Pw.2026!" not in prepare_step.command
    marker_step = next(step for step in report.steps if step.name == "run_local_service_markers")
    assert all(node_id in marker_step.command for node_id in LOCAL_SERVICE_MARKER_TESTS)
    assert "DPONE_IT_MSSQL_HOST=127.0.0.1" in marker_step.command
    assert "DPONE_KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:59092" in marker_step.command
    assert "DPONE_SCHEMA_REGISTRY_URL=http://127.0.0.1:58081" in marker_step.command
    assert f"--min-passed {LOCAL_SERVICE_MARKER_MIN_PASSED}" in marker_step.command
    assert "--max-skipped 0" in marker_step.command
    assert "--evidence-json test_artifacts/live_certification/service_markers.json" in marker_step.command
    assert '--commit-sha "$(git rev-parse HEAD)"' in marker_step.command
    assert Path(report.json_path).exists()
    assert Path(report.markdown_path).exists()


def test_live_certification_vendor_plan_marks_credentials_required(tmp_path: Path) -> None:
    report = LiveCertificationAutomationService().build(
        output_dir=tmp_path / "vendor-certification",
        profile="vendor_live",
        row_count=10000,
        include_vendor_live=True,
    )

    assert report.passed is True
    assert report.credentials_required is True
    assert "vendor_live_secrets" in report.required_environment
    assert any("integration_live" in step.command for step in report.steps)


def test_real_local_certification_plan_requires_observed_matrix_and_mysql_evidence(tmp_path: Path) -> None:
    report = LiveCertificationAutomationService().build(
        output_dir=tmp_path / "real-local-certification",
        profile="real_local",
        row_count=50000,
        include_vendor_live=False,
    )

    assert report.passed is True
    assert report.profile == "real_local"
    assert report.credentials_required is False
    assert report.services == ("postgres", "mysql", "mssql", "clickhouse", "kafka", "schema-registry", "minio")
    assert "performance_certification.json" not in report.required_artifacts
    assert "live_state_reconciliation.json" not in report.required_artifacts
    assert "release_evidence_pack.json" not in report.required_artifacts
    assert "mysql_local_route_cells_junit.xml" in report.required_artifacts
    assert "mysql_local_route_cells.json" in report.required_artifacts
    matrix_step = next(step for step in report.steps if step.name == "run_source_sink_matrix")
    assert "DPONE_MATRIX_RUN_MODE=mock_contract" in matrix_step.command
    assert "--junitxml=test_artifacts/live_certification/matrix/junit.xml" in matrix_step.command
    assert matrix_step.artifacts == ("junit.xml",)
    assert all("DPONE_MATRIX_RUN_MODE=real_local" not in step.command for step in report.steps)
    assert all(step.name != "build_release_evidence_pack" for step in report.steps)
    mysql_step = next(step for step in report.steps if step.name == "run_mysql_local_route_cells")
    selected_node_ids = tuple(
        token for token in shlex.split(mysql_step.command) if token.startswith("tests/integration/mysql/")
    )
    assert selected_node_ids == MYSQL_LOCAL_ROUTE_NODE_IDS
    assert mysql_step.required is True
    assert mysql_step.artifacts == ("mysql_local_route_cells_junit.xml", "mysql_local_route_cells.json")
    assert "tools/ci/assert_junit_executed.py" in mysql_step.command
    assert "--junit test_artifacts/live_certification/mysql/mysql_local_route_cells_junit.xml" in mysql_step.command
    assert "--min-passed 5" in mysql_step.command
    assert "--max-skipped 0" in mysql_step.command
    assert "--evidence-json test_artifacts/live_certification/mysql/mysql_local_route_cells.json" in mysql_step.command
    assert "--profile real_local" in mysql_step.command
    assert '--commit-sha "$(git rev-parse HEAD)"' in mysql_step.command


def test_type_matrix_certification_plan_is_first_class_local_profile(tmp_path: Path) -> None:
    report = LiveCertificationAutomationService().build(
        output_dir=tmp_path / "type-matrix-certification",
        profile="type_matrix_certification",
        row_count=10000,
        include_vendor_live=False,
    )

    assert report.passed is True
    assert report.profile == "type_matrix_certification"
    assert report.credentials_required is False
    assert report.services == ("postgres", "mysql", "mssql", "clickhouse", "kafka", "schema-registry", "minio")
    assert "type_matrix_decisions.json" in report.required_artifacts
    assert "schema_evolution_rerun.json" in report.required_artifacts
    assert any(step.name == "run_type_matrix_live_fixtures" for step in report.steps)


def test_native_transfer_plan_retains_only_executed_fixture_evidence(tmp_path: Path) -> None:
    report = LiveCertificationAutomationService().build(
        output_dir=tmp_path / "native-transfer-certification",
        profile="native_transfer",
        row_count=10000,
        include_vendor_live=False,
    )

    assert report.passed is True
    assert report.profile == "native_transfer"
    assert report.credentials_required is False
    assert report.services == ("postgres", "mysql", "mssql", "clickhouse", "kafka", "schema-registry", "minio")
    step_names = [step.name for step in report.steps]
    assert step_names == [
        "install_native_tooling",
        "start_local_services",
        "prepare_local_mssql_database",
        "run_local_service_markers",
        "run_native_transfer_live_fixtures",
        "stop_local_services",
    ]
    assert "native_transfer_live_junit.xml" in report.required_artifacts
    assert "native_transfer_live_fixtures.json" in report.required_artifacts
    assert "strategy_certification_bundle.json" not in report.required_artifacts
    fixture_step = next(step for step in report.steps if step.name == "run_native_transfer_live_fixtures")
    assert "--max-skipped 0" in fixture_step.command
    assert "--evidence-json" in fixture_step.command
    assert "<commit_sha>" not in fixture_step.command
    assert '--commit-sha "$(git rev-parse HEAD)"' in fixture_step.command


def test_benchmark_slo_gate_combines_performance_and_operational_objectives(tmp_path: Path) -> None:
    report = BenchmarkSloGateService().evaluate(
        output_dir=tmp_path / "benchmark-slo",
        metrics={
            "throughput_rows_per_second": 120000,
            "duration_seconds": 40,
            "freshness_lag_seconds": 120,
            "failure_rate": 0,
        },
        baseline={
            "throughput_rows_per_second": {"value": 100000, "direction": "higher"},
            "duration_seconds": {"value": 60, "direction": "lower"},
        },
        objectives={
            "throughput_rows_per_second": {"min": 100000},
            "freshness_lag_seconds": {"max": 300},
            "failure_rate": {"max": 0},
        },
        allowed_regression_ratio=0.10,
    )

    assert report.passed is True
    assert report.benchmark_passed is True
    assert report.slo_passed is True
    assert report.metric_count == 4
    assert report.json_path.endswith("benchmark_slo_gate.json")
    assert Path(report.json_path).exists()
    assert Path(report.markdown_path).exists()
    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    assert payload["benchmark"]["passed"] is True
    assert payload["slo"]["passed"] is True


def test_benchmark_slo_gate_fails_on_regression_or_slo_breach(tmp_path: Path) -> None:
    report = BenchmarkSloGateService().evaluate(
        output_dir=tmp_path / "benchmark-slo",
        metrics={"throughput_rows_per_second": 50000, "freshness_lag_seconds": 900},
        baseline={"throughput_rows_per_second": {"value": 100000, "direction": "higher"}},
        objectives={"freshness_lag_seconds": {"max": 300}},
    )

    assert report.passed is False
    assert report.benchmark_passed is False
    assert report.slo_passed is False
    assert "benchmark_baseline.not_passed" in report.blockers
    assert "slo.not_passed" in report.blockers


def test_performance_certification_builds_release_grade_artifact(tmp_path: Path) -> None:
    report = PerformanceCertificationService().certify(
        output_dir=tmp_path / "performance",
        profile="real_local",
        row_count=50000,
        metrics={
            "throughput_rows_per_second": 125000,
            "duration_seconds": 40,
            "memory_peak_mb": 512,
            "failure_rate": 0,
        },
        minimums={
            "throughput_rows_per_second": 100000,
            "failure_rate": 0,
        },
        maximums={
            "duration_seconds": 60,
            "memory_peak_mb": 1024,
        },
    )

    assert report.passed is True
    assert report.profile == "real_local"
    assert report.row_count == 50000
    assert json.loads(Path(report.json_path).read_text(encoding="utf-8"))["evidence_status"] == "PASS"
    assert report.blockers == tuple()
    assert Path(report.json_path).exists()
    payload = json.loads(Path(report.json_path).read_text(encoding="utf-8"))
    assert payload["passed"] is True
    assert payload["results"][0]["metric"] == "throughput_rows_per_second"


def test_live_state_reconciliation_certification_requires_state_and_delete_evidence(tmp_path: Path) -> None:
    matrix = _write_json(tmp_path / "matrix.json", {"passed": True, "total_cases": 157})
    state = _write_json(
        tmp_path / "state.json",
        {
            "passed": True,
            "state_backends": ["postgres", "mssql"],
            "checks": ["xmin_state", "kafka_offsets"],
        },
    )
    reconciliation = _write_json(
        tmp_path / "reconciliation.json",
        {
            "passed": True,
            "delete_reconciliation": {"passed": True, "physical_deletes_checked": 500},
        },
    )

    report = LiveStateReconciliationCertificationService().certify(
        output_dir=tmp_path / "state-reconciliation",
        profile="real_local",
        artifacts={
            "matrix": matrix,
            "state": state,
            "reconciliation": reconciliation,
        },
        required=("matrix", "state", "reconciliation"),
    )

    assert report.passed is True
    assert report.blockers == tuple()
    assert report.state_backends == ("mssql", "postgres")
    assert report.delete_reconciliation_checked is True
    assert Path(report.json_path).exists()
    assert json.loads(Path(report.json_path).read_text(encoding="utf-8"))["evidence_status"] == "PASS"


def test_release_evidence_pack_is_red_when_required_artifact_missing(tmp_path: Path) -> None:
    performance = _write_json(tmp_path / "performance.json", {"passed": True})

    report = ReleaseEvidencePackService().build(
        output_dir=tmp_path / "release-pack",
        release="v0.5.1",
        profile="real_local",
        artifacts={"performance_certification": performance},
        required=("performance_certification", "live_state_reconciliation"),
    )

    assert report.passed is False
    assert "live_state_reconciliation.missing" in report.blockers
    assert Path(report.json_path).exists()
    assert Path(report.markdown_path).exists()


def test_pre_release_checklist_requires_all_minor_release_domains(tmp_path: Path) -> None:
    report = PreReleaseChecklistService().build(
        output_dir=tmp_path / "pre-release",
        release="v0.5.1",
        release_type="minor",
        checks={name: True for name in REQUIRED_MINOR_MAJOR_CHECKS},
    )

    assert report.passed is True
    assert report.release_type == "minor"
    assert report.blockers == tuple()
    assert Path(report.json_path).exists()
    assert tuple(item.name for item in report.checks if item.required) == REQUIRED_MINOR_MAJOR_CHECKS


def test_pre_release_checklist_fails_when_docs_or_run_api_missing(tmp_path: Path) -> None:
    report = PreReleaseChecklistService().build(
        output_dir=tmp_path / "pre-release",
        release="v0.5.1",
        release_type="minor",
        checks={"cli_help_surface": True, "documentation_mkdocs": False},
    )

    assert report.passed is False
    assert "run_cli_manifest.missing" in report.blockers
    assert "run_python_api_manifest.missing" in report.blockers
    assert "documentation_mkdocs.not_passed" in report.blockers
    assert "docker_live_routes.missing" in report.blockers


def test_pre_release_checklist_patch_release_keeps_fast_but_real_gate(tmp_path: Path) -> None:
    report = PreReleaseChecklistService().build(
        output_dir=tmp_path / "pre-release",
        release="v0.5.2",
        release_type="patch",
        checks={name: True for name in REQUIRED_PATCH_CHECKS},
    )

    assert report.passed is True
    assert tuple(item.name for item in report.checks if item.required) == REQUIRED_PATCH_CHECKS


def test_live_certification_cli_commands_output_json(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path) -> None:
    _patch_cli(monkeypatch)
    ops_group = next(command for command in get_commands() if command.name == "ops")
    assert {
        "live-certification-plan",
        "benchmark-slo-gate",
        "performance-certification",
        "live-state-reconciliation",
        "release-evidence-pack",
        "pre-release-checklist",
    } <= {command.name for command in ops_group.subcommands}

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "live-certification-plan",
                "--output-dir",
                str(tmp_path / "live-certification"),
                "--profile",
                "local_live",
                "--row-count",
                "25000",
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["profile"] == "local_live"
    assert payload["credentials_required"] is False

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "live-certification-plan",
                "--output-dir",
                str(tmp_path / "real-local-certification"),
                "--profile",
                "real_local",
                "--row-count",
                "25000",
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["profile"] == "real_local"
    matrix_step = next(step for step in payload["steps"] if step["name"] == "run_source_sink_matrix")
    assert "DPONE_MATRIX_RUN_MODE=mock_contract" in matrix_step["command"]
    assert "--junitxml=test_artifacts/live_certification/matrix/junit.xml" in matrix_step["command"]
    assert all("DPONE_MATRIX_RUN_MODE=real_local" not in step["command"] for step in payload["steps"])

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "benchmark-slo-gate",
                "--output-dir",
                str(tmp_path / "benchmark-slo"),
                "--metrics-json",
                '{"throughput_rows_per_second":120000,"freshness_lag_seconds":120}',
                "--baseline-json",
                '{"throughput_rows_per_second":{"value":100000,"direction":"higher"}}',
                "--objectives-json",
                '{"freshness_lag_seconds":{"max":300}}',
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "performance-certification",
                "--output-dir",
                str(tmp_path / "performance-cli"),
                "--profile",
                "real_local",
                "--row-count",
                "25000",
                "--metrics-json",
                '{"throughput_rows_per_second":120000,"duration_seconds":30}',
                "--minimum-json",
                '{"throughput_rows_per_second":100000}',
                "--maximum-json",
                '{"duration_seconds":60}',
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True

    state_artifact = _write_json(tmp_path / "state-cli.json", {"passed": True, "state_backends": ["postgres"]})
    reconciliation_artifact = _write_json(
        tmp_path / "reconciliation-cli.json",
        {"passed": True, "delete_reconciliation": {"passed": True}},
    )
    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "live-state-reconciliation",
                "--output-dir",
                str(tmp_path / "state-reconciliation-cli"),
                "--profile",
                "real_local",
                "--artifact",
                f"state={state_artifact}",
                "--artifact",
                f"reconciliation={reconciliation_artifact}",
                "--require",
                "state",
                "--require",
                "reconciliation",
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "release-evidence-pack",
                "--output-dir",
                str(tmp_path / "release-pack-cli"),
                "--release",
                "v0.5.1",
                "--profile",
                "real_local",
                "--artifact",
                f"performance_certification={tmp_path / 'performance-cli' / 'performance_certification.json'}",
                "--require",
                "performance_certification",
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["release"] == "v0.5.1"
    assert payload["passed"] is True
    assert payload["blockers"] == []

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "pre-release-checklist",
                "--output-dir",
                str(tmp_path / "pre-release-cli"),
                "--release",
                "v0.5.1",
                "--release-type",
                "minor",
                "--check",
                "cli_help_surface=true",
                "--check",
                "cli_output_contracts=true",
                "--check",
                "run_cli_manifest=true",
                "--check",
                "run_python_api_manifest=true",
                "--check",
                "nested_hierarchical_identity=true",
                "--check",
                "nested_parent_child_integrity=true",
                "--check",
                "source_sink_strategy_matrix=true",
                "--check",
                "source_sink_artifacts=true",
                "--check",
                "docker_live_routes=true",
                "--check",
                "contracts_guardrails=true",
                "--check",
                "documentation_yaml_examples=true",
                "--check",
                "documentation_links=true",
                "--check",
                "documentation_mkdocs=true",
                "--check",
                "ci_cd_quality=true",
                "--check",
                "package=true",
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True


def test_github_actions_has_live_certification_workflow() -> None:
    workflow_path = ROOT / ".github" / "workflows" / "live-certification.yml"
    workflow = yaml.safe_load(workflow_path.read_text(encoding="utf-8"))

    triggers = workflow.get("on") or workflow.get(True)
    assert "workflow_dispatch" in triggers
    profile_options = triggers["workflow_dispatch"]["inputs"]["profile"]["options"]
    assert "real_local" in profile_options
    assert "type_matrix_certification" in profile_options
    assert "native_transfer" in profile_options
    assert "pull_request" not in triggers
    job = workflow["jobs"]["local-live-certification"]
    steps_by_name = {step["name"]: step for step in job["steps"]}
    step_names = list(steps_by_name)
    script = "\n".join(step.get("run", "") for step in job["steps"])
    assert "dpone ops live-certification-plan" in script
    assert (
        "docker compose -f docker/docker-compose.integration.yml up -d "
        "postgres mysql mssql clickhouse kafka schema-registry minio"
    ) in script
    marker_script = steps_by_name["Run local service markers"]["run"]
    assert all(node_id in marker_script for node_id in LOCAL_SERVICE_MARKER_TESTS)
    assert f"--min-passed {LOCAL_SERVICE_MARKER_MIN_PASSED}" in marker_script
    assert "--max-skipped 0" in marker_script
    assert '--evidence-json "$CERT_ROOT/service_markers.json"' in marker_script
    mysql_script = steps_by_name["Run MySQL local route cells"]["run"]
    assert all(node_id in mysql_script for node_id in MYSQL_LOCAL_ROUTE_NODE_IDS)
    assert "--min-passed 5" in mysql_script
    assert "--max-skipped 0" in mysql_script
    assert '--evidence-json "$CERT_ROOT/mysql/mysql_local_route_cells.json"' in mysql_script
    assert '--summary-md "$GITHUB_STEP_SUMMARY"' in mysql_script
    assert job["env"]["DPONE_IT_MYSQL_HOST"] == "127.0.0.1"
    assert job["env"]["DPONE_IT_MYSQL_PORT_FORWARD"] == "53306"
    assert "mysql/mysql_local_route_cells.json" in script
    assert '"passed": true' not in script
    assert "--check docker_live_routes=true" not in script
    assert "--check documentation_mkdocs=true" not in script
    assert "|| true" not in script
    disabled_steps = [step for step in job["steps"] if str(step["name"]).startswith("Disabled (UNVERIFIED):")]
    assert disabled_steps
    assert all(step["if"] == "${{ false }}" for step in disabled_steps)
    assert "DPONE_MATRIX_RUN_MODE=mock_contract" in script
    assert "DPONE_MATRIX_RUN_MODE=real_local" not in script
    matrix_script = steps_by_name["Run source/sink matrix"]["run"]
    assert '--junitxml="$DPONE_MATRIX_ARTIFACT_DIR/junit.xml"' in matrix_script
    assert '--evidence-json "$CERT_ROOT/matrix_execution.json"' in matrix_script
    assert '--evidence-json "$DPONE_MATRIX_ARTIFACT_DIR/' not in matrix_script
    assert "type_matrix_certification" in script
    assert job["env"]["CERT_PROFILE"] == "${{ inputs.profile }}"
    assert '--profile "${CERT_PROFILE}"' in script
    assert 'typed_reconciliation.json").write_text' not in script
    assert 'cat > "$CERT_ROOT/mysql/mysql_local_route_cells.json"' not in script
    for step_name in (
        "Run source/sink matrix",
        "Run route-refresh-execute MSSQL ClickHouse live certification",
        "Run route-refresh-execute Postgres MSSQL live certification",
    ):
        assert "--max-skipped 0" in steps_by_name[step_name]["run"]
    native_transfer_script = steps_by_name["Run native transfer live fixtures"]["run"]
    assert "--min-passed 3" in native_transfer_script
    assert "--max-skipped 0" in native_transfer_script
    assert '--evidence-json "$CERT_ROOT/native_transfer_live_fixtures.json"' in native_transfer_script
    assert '--commit-sha "$GITHUB_SHA"' in native_transfer_script
    mssql_prepare = steps_by_name["Prepare local MSSQL benchmark database"]
    assert mssql_prepare["env"]["DPONE_IT_MSSQL_DATABASE"] == "dpone"
    assert "CREATE DATABASE" in mssql_prepare["run"]
    assert step_names.index("Prepare local MSSQL benchmark database") < step_names.index(
        "Run Postgres to MSSQL native fast-path smoke"
    )
    assert (
        steps_by_name["Run Postgres to MSSQL native fast-path smoke"]["if"]
        == "${{ inputs.profile != 'type_matrix_certification' }}"
    )
    postgres_mssql_smoke = steps_by_name["Run Postgres to MSSQL native fast-path smoke"]["run"]
    assert "--num-partitions" not in postgres_mssql_smoke
    assert "--export-workers" not in postgres_mssql_smoke
    assert "--load-workers" not in postgres_mssql_smoke
    assert triggers["workflow_call"]["inputs"]["native_benchmark_partitions"]["default"] == "1"
    assert triggers["workflow_dispatch"]["inputs"]["native_benchmark_partitions"]["default"] == "1"


def test_live_certification_docs_are_self_service() -> None:
    docs = [
        ROOT / "docs" / "live-certification.md",
        ROOT / "docs" / "testing" / "integration-matrix.md",
        ROOT / "docs" / "operational-control-plane.md",
        ROOT / "docs" / "release-evidence.md",
    ]
    required = (
        "local_live",
        "real_local",
        "vendor_live",
        "benchmark-slo-gate",
        "performance-certification",
        "live-state-reconciliation",
        "release-evidence-pack",
        "pre-release-checklist",
        "docker_live_routes",
        "documentation_mkdocs",
        "live-certification-plan",
        "Runbook",
        "docker/docker-compose.integration.yml",
    )
    for path in docs:
        text = path.read_text(encoding="utf-8")
        for item in required:
            assert item in text, f"{path.relative_to(ROOT)} missing {item}"


def _write_json(path: Path, payload: dict) -> str:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return str(path)
