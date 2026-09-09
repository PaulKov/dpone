from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.readiness.migration_control import MigrationPack, MigrationTarget


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_data_product_slo_cli_plan_evaluate_gate_and_incident_report(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest_path = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    contract_gate_path = _write_json(tmp_path / "contract-gate.json", _contract_gate())
    consumer_gate_path = _write_json(tmp_path / "consumer-gate.json", _consumer_gate())

    for args in (
        ["data", "--help"],
        ["data", "product", "--help"],
        ["data", "product", "slo", "--help"],
        ["data", "product", "slo", "plan", "--help"],
        ["data", "product", "slo", "evaluate", "--help"],
        ["data", "product", "slo", "gate", "--help"],
        ["data", "product", "incident", "--help"],
        ["data", "product", "incident", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    plan_path = tmp_path / "slo-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "data",
                "product",
                "slo",
                "plan",
                "--manifest",
                str(manifest_path),
                "--contract-gate",
                str(contract_gate_path),
                "--consumer-gate",
                str(consumer_gate_path),
                "--format",
                "json",
                "--output",
                str(plan_path),
            ]
        )
    assert plan_exit.value.code == 0
    plan_console = json.loads(capsys.readouterr().out)
    plan_file = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan_console["slo_plan_id"] == plan_file["slo_plan_id"]
    assert plan_file["schema_version"] == "dpone.data_product_slo_plan.v1"
    assert plan_file["status"] == "ready"

    runtime_path = _write_json(tmp_path / "latest-run.json", _runtime_artifact(freshness_lag_seconds=1800))
    registry_path = _write_json(
        tmp_path / "registry.json",
        {
            "schema_version": "dpone.schema_migration_evidence_registry_query.v1",
            "records": [{"stage": "watched", "status": "ready"}],
        },
    )
    evaluation_path = tmp_path / "slo-evaluation.json"
    with pytest.raises(SystemExit) as eval_exit:
        cli_main.main(
            [
                "data",
                "product",
                "slo",
                "evaluate",
                "--plan",
                str(plan_path),
                "--registry",
                str(registry_path),
                "--runtime-artifact",
                str(runtime_path),
                "--format",
                "json",
                "--output",
                str(evaluation_path),
            ]
        )
    assert eval_exit.value.code == 2
    evaluation = json.loads(capsys.readouterr().out)
    assert evaluation["status"] == "blocked"
    assert (
        json.loads(evaluation_path.read_text(encoding="utf-8"))["slo_evaluation_id"] == evaluation["slo_evaluation_id"]
    )

    gate_path = tmp_path / "slo-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "slo",
                "gate",
                "--evaluation",
                str(evaluation_path),
                "--profile",
                "prod_strict",
                "--format",
                "json",
                "--output",
                str(gate_path),
            ]
        )
    assert gate_exit.value.code == 2
    gate = json.loads(capsys.readouterr().out)
    assert gate["schema_version"] == "dpone.data_product_slo_gate.v1"
    assert gate["status"] == "blocked"
    assert json.loads(gate_path.read_text(encoding="utf-8"))["slo_gate_id"] == gate["slo_gate_id"]

    incident_path = tmp_path / "incident.md"
    with pytest.raises(SystemExit) as incident_exit:
        cli_main.main(
            [
                "data",
                "product",
                "incident",
                "report",
                "--evaluation",
                str(evaluation_path),
                "--slo-gate",
                str(gate_path),
                "--format",
                "md",
                "--output",
                str(incident_path),
            ]
        )
    assert incident_exit.value.code == 0
    markdown = capsys.readouterr().out
    assert "# Data Product Incident Report" in markdown
    assert "analytics.orders" in markdown
    assert incident_path.read_text(encoding="utf-8") == markdown


def test_data_product_slo_bundle_and_registry_cli_hooks(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    pack = MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders"},
        actual={"sink_type": "clickhouse", "table": "analytics.orders"},
        changes=({"change_type": "schema_contract_major", "path": "analytics.orders"},),
        strategy="expand_contract",
    )
    pack_path = _write_json(tmp_path / "orders.pack.json", pack.to_dict(command="plan"))
    slo_gate_path = _write_json(
        tmp_path / "slo-gate.json",
        {
            "schema_version": "dpone.data_product_slo_gate.v1",
            "slo_gate_id": "sha256:" + "6" * 64,
            "status": "allowed",
            "profile": "prod_strict",
            "product_id": "analytics.orders",
            "pack_id": pack.pack_id,
            "blockers": [],
            "warnings": [],
        },
    )
    incident_path = _write_json(
        tmp_path / "incident.json",
        {
            "schema_version": "dpone.data_product_incident_report.v1",
            "incident_report_id": "sha256:" + "7" * 64,
            "status": "healthy",
            "severity": "none",
            "product_id": "analytics.orders",
            "pack_id": pack.pack_id,
            "blockers": [],
            "warnings": [],
        },
    )
    bundle_dir = tmp_path / "bundle"

    with pytest.raises(SystemExit) as build_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "bundle",
                "build",
                "--pack",
                str(pack_path),
                "--data-product-slo-gate",
                str(slo_gate_path),
                "--data-product-incident-report",
                str(incident_path),
                "--output-dir",
                str(bundle_dir),
                "--format",
                "json",
            ]
        )
    assert build_exit.value.code == 0
    bundle = json.loads(capsys.readouterr().out)
    assert bundle["summary"]["data_product_slo_gate_id"] == "sha256:" + "6" * 64
    assert bundle["summary"]["data_product_incident_report_id"] == "sha256:" + "7" * 64

    record_path = tmp_path / "registry-record.json"
    with pytest.raises(SystemExit) as registry_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "registry",
                "record",
                "--bundle",
                str(bundle_dir / "bundle.json"),
                "--data-product-slo-gate",
                str(slo_gate_path),
                "--data-product-incident-report",
                str(incident_path),
                "--environment",
                "prod",
                "--stage",
                "slo_gate_passed",
                "--store-uri",
                str(tmp_path / "registry.json"),
                "--format",
                "json",
                "--output",
                str(record_path),
            ]
        )
    assert registry_exit.value.code == 0
    record = json.loads(record_path.read_text(encoding="utf-8"))
    assert record["stage"] == "slo_gate_passed"
    assert {"data_product_slo_gate", "data_product_incident_report"} <= {
        item["kind"] for item in record["artifact_refs"]
    }


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _manifest() -> dict[str, object]:
    return {
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {
                "schema_contract": {"id": "analytics.orders", "version": "2.0.0"},
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "slo": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "objectives": {
                            "freshness": {"max_lag_seconds": 900},
                            "volume": {"min_rows": 1, "max_relative_delta": 0.001},
                            "latency": {"max_run_duration_ms": 300000},
                            "quality": {"max_duplicate_keys": 0, "max_null_keys": 0, "typed_hash": "warning"},
                            "availability": {"max_failed_runs": 0},
                        },
                        "consumers": {
                            "require_critical_consumer_green": True,
                            "unknown_consumer": "warn",
                        },
                        "incident": {
                            "enabled": True,
                            "severity_map": {
                                "critical_consumer_failed": "sev1",
                                "freshness_breach": "sev2",
                                "volume_breach": "sev2",
                                "warning_only": "sev3",
                            },
                        },
                    },
                },
            },
        }
    }


def _contract_gate() -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_contract_gate.v1",
        "contract_gate_id": "sha256:" + "1" * 64,
        "contract_id": "analytics.orders",
        "contract_version_id": "sha256:" + "2" * 64,
        "status": "allowed",
        "blockers": [],
        "warnings": [],
    }


def _consumer_gate() -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_contract_consumer_gate.v1",
        "consumer_gate_id": "sha256:" + "3" * 64,
        "contract_id": "analytics.orders",
        "status": "allowed",
        "summary": {"critical_consumers_failed": 0, "unknown_consumers": 0},
        "blockers": [],
        "warnings": [],
    }


def _runtime_artifact(*, freshness_lag_seconds: int) -> dict[str, object]:
    return {
        "schema_version": "dpone.runtime_run.v1",
        "product_id": "analytics.orders",
        "status": "succeeded",
        "freshness_lag_seconds": freshness_lag_seconds,
        "rows_loaded": 1000,
        "duration_ms": 120_000,
        "failed_runs": 0,
        "quality": {"duplicate_keys": 0, "null_keys": 0, "typed_hash_status": "passed"},
    }


def _write_yaml(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
