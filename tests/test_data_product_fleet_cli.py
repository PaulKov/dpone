from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_data_product_fleet_cli_evaluate_gate_report_export_and_route_dry_run(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest_path = _write_yaml(tmp_path / "orders.yaml", _manifest())
    registry_path = _write_json(
        tmp_path / "registry.json",
        {
            "records": [
                _record(
                    product_id="analytics.orders",
                    status="blocked",
                    blockers=("data_product_error_budget.fast_burn_exceeded:fast_burn",),
                )
            ]
        },
    )

    for args in (
        ["data", "product", "fleet", "--help"],
        ["data", "product", "fleet", "evaluate", "--help"],
        ["data", "product", "fleet", "gate", "--help"],
        ["data", "product", "fleet", "report", "--help"],
        ["data", "product", "fleet", "export", "--help"],
        ["data", "product", "incident", "route", "dry-run", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    evaluation_path = tmp_path / "fleet.evaluation.json"
    with pytest.raises(SystemExit) as evaluate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "fleet",
                "evaluate",
                "--manifests",
                str(manifest_path),
                "--registry",
                str(registry_path),
                "--observed-at",
                "2026-06-28T12:00:00Z",
                "--format",
                "json",
                "--output",
                str(evaluation_path),
            ]
        )
    assert evaluate_exit.value.code == 2
    evaluation = json.loads(capsys.readouterr().out)
    assert evaluation["schema_version"] == "dpone.data_product_fleet_evaluation.v1"
    assert evaluation["status"] == "frozen"
    assert (
        json.loads(evaluation_path.read_text(encoding="utf-8"))["fleet_evaluation_id"]
        == evaluation["fleet_evaluation_id"]
    )

    gate_path = tmp_path / "fleet.gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "fleet",
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
    assert gate["schema_version"] == "dpone.data_product_fleet_gate.v1"
    assert gate["status"] == "frozen"

    report_path = tmp_path / "fleet.report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "data",
                "product",
                "fleet",
                "report",
                "--evaluation",
                str(evaluation_path),
                "--format",
                "md",
                "--output",
                str(report_path),
            ]
        )
    assert report_exit.value.code == 0
    report = capsys.readouterr().out
    assert "# Data Product Fleet Reliability" in report
    assert report_path.read_text(encoding="utf-8") == report

    export_path = tmp_path / "fleet.prom"
    with pytest.raises(SystemExit) as export_exit:
        cli_main.main(
            [
                "data",
                "product",
                "fleet",
                "export",
                "--evaluation",
                str(evaluation_path),
                "--target",
                "prometheus",
                "--format",
                "text",
                "--output",
                str(export_path),
            ]
        )
    assert export_exit.value.code == 0
    exported = capsys.readouterr().out
    assert "dpone_data_product_release_frozen" in exported
    assert export_path.read_text(encoding="utf-8") == exported

    route_payload_path = _write_json(
        tmp_path / "route-payload.json",
        {
            "schema_version": "dpone.data_product_incident_route_payload.v1",
            "route_payload_id": "sha256:" + "6" * 64,
            "incident_id": "sha256:" + "7" * 64,
            "provider": "slack",
            "payload": {"text": "sev1 data product incident"},
            "network_writes": [],
        },
    )
    receipt_path = tmp_path / "route-receipt.json"
    with pytest.raises(SystemExit) as dry_run_exit:
        cli_main.main(
            [
                "data",
                "product",
                "incident",
                "route",
                "dry-run",
                "--payload",
                str(route_payload_path),
                "--provider",
                "slack",
                "--format",
                "json",
                "--output",
                str(receipt_path),
            ]
        )
    assert dry_run_exit.value.code == 0
    receipt = json.loads(capsys.readouterr().out)
    assert receipt["schema_version"] == "dpone.data_product_route_delivery_receipt.v1"
    assert receipt["status"] == "dry_run"
    assert receipt["network_writes"] == []
    assert (
        json.loads(receipt_path.read_text(encoding="utf-8"))["route_delivery_receipt_id"]
        == receipt["route_delivery_receipt_id"]
    )


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_yaml(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


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
                    "criticality": "critical",
                    "reliability_control_tower": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "fleet": {
                            "unknown_product": "warn",
                            "stale_evidence_policy": "block",
                            "stale_after_seconds": 86400,
                            "freeze_on": ["fast_burn", "budget_exhausted", "sev1_open"],
                        },
                        "export": {"enabled": True, "targets": ["prometheus", "opentelemetry"]},
                        "routing": {"enabled": True, "mode": "dry_run", "providers": ["slack", "jira"]},
                    },
                },
            },
        }
    }


def _record(*, product_id: str, status: str, blockers: tuple[str, ...]) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_evidence_registry_record.v1",
        "record_id": "sha256:" + "1" * 64,
        "recorded_at": "2026-06-28T12:00:00Z",
        "target": {"sink_type": "clickhouse", "table": product_id},
        "environment": "prod",
        "stage": "error_budget_gate_passed",
        "status": status,
        "artifact_refs": [
            {
                "kind": "data_product_error_budget_gate",
                "schema_version": "dpone.data_product_error_budget_gate.v1",
                "evidence_id": "sha256:" + "2" * 64,
            }
        ],
        "blockers": list(blockers),
        "warnings": [],
    }
