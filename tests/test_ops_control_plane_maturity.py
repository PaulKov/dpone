from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.commands.registry import get_commands
from dpone.ops.catalog_publish import CatalogPublicationService
from dpone.ops.certification_pack import ConnectorCertificationPackService
from dpone.ops.deploy_profiles import DeploymentProfileRenderer
from dpone.ops.observability_pack import ObservabilityPackService
from dpone.ops.reconciliation import ReconciliationService
from dpone.ops.recovery import RuntimeRecoveryPlanner
from dpone.ops.staging_evidence import StagingEvidenceService


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_ops_group_exposes_control_plane_commands() -> None:
    ops_group = next(command for command in get_commands() if command.name == "ops")
    command_names = {command.name for command in ops_group.subcommands}

    assert command_names >= {
        "certification-pack",
        "recovery-plan",
        "reconcile",
        "observability-pack",
        "deploy-render",
        "staging-evidence",
        "catalog-publish",
    }


def test_connector_certification_pack_aggregates_required_evidence(tmp_path: Path) -> None:
    certification = tmp_path / "certification_report.json"
    observability = tmp_path / "runtime_metrics.json"
    lineage = tmp_path / "openlineage_event.json"
    certification.write_text(
        json.dumps(
            {
                "passed": True,
                "evidence_status": "PASS",
                "results": [
                    {"case_id": "postgres_to_mssql__incremental_merge", "passed": True},
                    {"case_id": "mssql_to_clickhouse__replace", "passed": True},
                ],
            }
        ),
        encoding="utf-8",
    )
    observability.write_text(json.dumps({"passed": True, "metric_count": 12}), encoding="utf-8")
    lineage.write_text(json.dumps({"passed": True, "eventType": "COMPLETE"}), encoding="utf-8")

    report = ConnectorCertificationPackService().build(
        output_dir=tmp_path / "pack",
        pack_id="dpone-local-cert",
        artifacts={
            "certification_report": certification,
            "observability": observability,
            "lineage": lineage,
        },
        required=("certification_report", "observability", "lineage"),
    )

    assert report.passed is True
    assert report.coverage.case_count == 2
    assert report.coverage.sources == ("mssql", "postgres")
    assert report.coverage.sinks == ("clickhouse", "mssql")
    assert {item.name for item in report.items} == {"certification_report", "observability", "lineage"}
    assert all(len(item.sha256) == 64 for item in report.items)
    assert Path(report.json_path).exists()
    assert Path(report.markdown_path).exists()


def test_connector_certification_pack_rejects_unverified_mock_report(tmp_path: Path) -> None:
    certification = tmp_path / "certification_report.json"
    certification.write_text(
        json.dumps(
            {
                "passed": True,
                "evidence_status": "UNVERIFIED",
                "profile": "mock_contract",
                "results": [{"case_id": "postgres_to_mssql__snapshot_diff", "passed": True}],
            }
        ),
        encoding="utf-8",
    )

    report = ConnectorCertificationPackService().build(
        output_dir=tmp_path / "pack",
        pack_id="mock-must-not-certify",
        artifacts={"certification_report": certification},
    )

    assert report.passed is False
    assert report.evidence_status == "UNVERIFIED"
    assert report.blockers == ("certification_report.not_passed",)


def test_runtime_recovery_planner_detects_failed_jobs_locks_and_uncommitted_loads(tmp_path: Path) -> None:
    state_dir = tmp_path / "state"
    lock_dir = tmp_path / "locks"
    package_dir = tmp_path / "packages"
    state_dir.mkdir()
    lock_dir.mkdir()
    package_dir.mkdir()
    (state_dir / "run_01.job_state.json").write_text(
        json.dumps(
            {
                "run_id": "run_01",
                "status": "failed",
                "resumable": True,
                "blockers": ["target.timeout"],
                "manifest_path": "manifests/orders.yml",
                "selector": "orders",
            }
        ),
        encoding="utf-8",
    )
    (lock_dir / "orders.lock.json").write_text(json.dumps({"key": "orders", "token": "abc"}), encoding="utf-8")
    (package_dir / "01JLOAD0000000000000000000.json").write_text(
        json.dumps(
            {
                "run_id": "run_01",
                "load_id": "01JLOAD0000000000000000000",
                "target": "landing.orders",
                "chunk_id": None,
                "status": "staged",
                "started_at": "2026-06-06T00:00:00+00:00",
                "updated_at": "2026-06-06T00:00:01+00:00",
                "rows_staged": 100,
                "rows_loaded": 0,
                "artifact_uri": "s3://stage/run_01/orders.tsv",
                "state_after": {},
                "error": None,
            }
        ),
        encoding="utf-8",
    )

    report = RuntimeRecoveryPlanner().plan(
        state_dir=state_dir,
        lock_dir=lock_dir,
        load_package_dir=package_dir,
        output_dir=tmp_path / "recovery",
    )

    assert report.passed is False
    assert report.failed_runs == 1
    assert report.active_locks == 1
    assert report.uncommitted_loads == 1
    assert {action.action for action in report.actions} >= {
        "dpone orchestrate run --resume-policy resume",
        "inspect_or_cleanup_lock",
        "rollback_or_commit_load_package",
    }
    assert Path(report.json_path).exists()
    assert Path(report.markdown_path).exists()


def test_reconciliation_service_returns_counts_checksums_and_repair_actions(tmp_path: Path) -> None:
    report = ReconciliationService().reconcile(
        output_dir=tmp_path / "reconcile",
        source_rows=[
            {"id": 1, "amount": 100, "__dpone__deleted_at": None},
            {"id": 2, "amount": 200, "__dpone__deleted_at": None},
            {"id": 3, "amount": 300, "__dpone__deleted_at": "2026-06-06T00:00:00+00:00"},
        ],
        target_rows=[
            {"id": 1, "amount": 100},
            {"id": 2, "amount": 250},
            {"id": 4, "amount": 400},
        ],
        key_columns=("id",),
        compare_columns=("amount",),
        delete_column="__dpone__deleted_at",
    )

    assert report.passed is False
    assert report.source_count == 3
    assert report.target_count == 3
    assert report.missing_count == 0
    assert report.extra_count == 1
    assert report.mismatch_count == 1
    assert report.delete_count == 1
    assert {action.action for action in report.repair_actions} == {
        "delete_target_row",
        "update_target_row",
    }
    assert Path(report.json_path).exists()
    assert Path(report.markdown_path).exists()


def test_observability_pack_writes_grafana_dashboard_and_alert_rules(tmp_path: Path) -> None:
    report = ObservabilityPackService().build(
        output_dir=tmp_path / "obs-pack",
        service_name="dpone",
        dashboard_title="dpone production",
        alert_thresholds={
            "dpone_run_passed": {"min": 1},
            "dpone_error_count": {"max": 0},
        },
    )

    assert report.passed is True
    assert Path(report.grafana_dashboard_path).exists()
    assert Path(report.prometheus_alerts_path).exists()
    dashboard = json.loads(Path(report.grafana_dashboard_path).read_text(encoding="utf-8"))
    assert dashboard["title"] == "dpone production"
    assert {panel["title"] for panel in dashboard["panels"]} >= {
        "Rows processed",
        "Runtime seconds",
        "Errors and warnings",
    }


@pytest.mark.parametrize(
    ("target", "expected"),
    [
        ("docker-compose", "services:"),
        ("k8s-cronjob", "kind: CronJob"),
        ("airflow", "BashOperator"),
        ("dagster", "@asset"),
    ],
)
def test_deployment_profile_renderer_outputs_operator_templates(tmp_path: Path, target: str, expected: str) -> None:
    report = DeploymentProfileRenderer().render(
        output_dir=tmp_path / target,
        target=target,
        manifest_path="manifests/orders.yml",
        selector="orders",
        image="ghcr.io/paulkov/dpone:0.2.8",
        schedule="0 2 * * *",
    )

    assert report.passed is True
    assert Path(report.artifact_path).read_text(encoding="utf-8").count(expected) >= 1
    assert Path(report.json_path).exists()
    assert Path(report.markdown_path).exists()


def test_staging_evidence_validates_manifest_and_native_load_hints(tmp_path: Path) -> None:
    manifest = tmp_path / "object_storage_manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "provider": "s3",
                "base_uri": "s3://dpone-stage/orders",
                "run_id": "run_01",
                "dataset": "landing",
                "table": "orders",
                "file_format": "tsv",
                "compression": "zstd",
                "cleanup_policy": "retain",
                "object_count": 2,
                "total_size_bytes": 1024,
                "objects": [
                    {
                        "uri": "s3://dpone-stage/orders/run_01/part-000.tsv",
                        "file_name": "part-000.tsv",
                        "size_bytes": 512,
                        "sha256": "a" * 64,
                    },
                    {
                        "uri": "s3://dpone-stage/orders/run_01/part-001.tsv",
                        "file_name": "part-001.tsv",
                        "size_bytes": 512,
                        "sha256": "b" * 64,
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    report = StagingEvidenceService().build(
        output_dir=tmp_path / "staging-evidence",
        manifest_path=manifest,
        sink="clickhouse",
        target_table="landing.orders",
    )

    assert report.passed is True
    assert report.object_count == 2
    assert "INSERT INTO landing.orders" in report.native_load_hint
    assert Path(report.json_path).exists()
    assert Path(report.markdown_path).exists()


def test_catalog_publication_service_builds_dbt_openlineage_and_datahub_payloads(tmp_path: Path) -> None:
    run_registry = tmp_path / "run_registry.json"
    run_registry.write_text(
        json.dumps(
            {
                "run_id": "run_01",
                "process": "orders",
                "manifest": "manifests/orders.yml",
                "passed": True,
                "artifacts": [],
            }
        ),
        encoding="utf-8",
    )

    report = CatalogPublicationService().publish(
        output_dir=tmp_path / "catalog",
        run_registry_entry_path=run_registry,
        input_datasets={"postgres": "public.orders"},
        output_datasets={"mssql": "landing.orders"},
        namespace="dpone.local",
    )

    assert report.passed is True
    assert Path(report.openlineage_path).exists()
    assert Path(report.dbt_sources_path).exists()
    assert Path(report.datahub_path).exists()
    datahub = json.loads(Path(report.datahub_path).read_text(encoding="utf-8"))
    assert datahub["entities"][0]["urn"].startswith("urn:li:dataset")


def test_ops_deploy_render_cli_outputs_json(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "deploy-render",
                "--target",
                "docker-compose",
                "--manifest",
                "manifests/orders.yml",
                "--selector",
                "orders",
                "--output-dir",
                str(tmp_path / "deploy"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["target"] == "docker-compose"
    assert Path(payload["artifact_path"]).exists()
