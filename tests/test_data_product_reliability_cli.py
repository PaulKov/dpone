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


def test_data_product_reliability_cli_budget_incident_and_closeout(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest_path = _write_yaml(tmp_path / "manifest.yaml", _manifest())
    slo_evaluation_path = _write_json(
        tmp_path / "slo-evaluation.json",
        _slo_evaluation(status="blocked", recorded_at="2026-06-28T11:55:00Z"),
    )
    slo_gate_path = _write_json(tmp_path / "slo-gate.json", _slo_gate(status="blocked"))

    for args in (
        ["data", "product", "slo", "budget", "--help"],
        ["data", "product", "slo", "budget", "plan", "--help"],
        ["data", "product", "slo", "budget", "evaluate", "--help"],
        ["data", "product", "slo", "budget", "gate", "--help"],
        ["data", "product", "incident", "lifecycle", "--help"],
        ["data", "product", "incident", "route", "render", "--help"],
        ["data", "product", "release", "closeout", "gate", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    plan_path = tmp_path / "budget-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "data",
                "product",
                "slo",
                "budget",
                "plan",
                "--manifest",
                str(manifest_path),
                "--slo-evaluation",
                str(slo_evaluation_path),
                "--format",
                "json",
                "--output",
                str(plan_path),
            ]
        )
    assert plan_exit.value.code == 0
    plan = json.loads(capsys.readouterr().out)
    assert plan["schema_version"] == "dpone.data_product_error_budget_plan.v1"
    assert json.loads(plan_path.read_text(encoding="utf-8"))["error_budget_plan_id"] == plan["error_budget_plan_id"]

    history_path = _write_json(
        tmp_path / "slo-history.json",
        {
            "evaluations": [
                _slo_evaluation(status="passed", recorded_at="2026-06-28T11:50:00Z"),
                _slo_evaluation(status="blocked", recorded_at="2026-06-28T11:55:00Z"),
            ]
        },
    )
    evaluation_path = tmp_path / "budget-evaluation.json"
    with pytest.raises(SystemExit) as evaluate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "slo",
                "budget",
                "evaluate",
                "--plan",
                str(plan_path),
                "--history",
                str(history_path),
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
    assert evaluation["status"] == "blocked"
    assert (
        json.loads(evaluation_path.read_text(encoding="utf-8"))["error_budget_evaluation_id"]
        == evaluation["error_budget_evaluation_id"]
    )

    budget_gate_path = tmp_path / "budget-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "slo",
                "budget",
                "gate",
                "--evaluation",
                str(evaluation_path),
                "--profile",
                "prod_strict",
                "--format",
                "json",
                "--output",
                str(budget_gate_path),
            ]
        )
    assert gate_exit.value.code == 2
    budget_gate = json.loads(capsys.readouterr().out)
    assert budget_gate["schema_version"] == "dpone.data_product_error_budget_gate.v1"
    assert budget_gate["status"] == "blocked"

    incident_path = tmp_path / "incident-lifecycle.json"
    with pytest.raises(SystemExit) as incident_exit:
        cli_main.main(
            [
                "data",
                "product",
                "incident",
                "lifecycle",
                "open",
                "--slo-evaluation",
                str(slo_evaluation_path),
                "--slo-gate",
                str(slo_gate_path),
                "--budget-gate",
                str(budget_gate_path),
                "--format",
                "json",
                "--output",
                str(incident_path),
            ]
        )
    assert incident_exit.value.code == 2
    incident = json.loads(capsys.readouterr().out)
    assert incident["status"] == "open"

    acknowledged_path = tmp_path / "incident-ack.json"
    with pytest.raises(SystemExit) as ack_exit:
        cli_main.main(
            [
                "data",
                "product",
                "incident",
                "lifecycle",
                "ack",
                "--incident",
                str(incident_path),
                "--actor",
                "data-platform",
                "--format",
                "json",
                "--output",
                str(acknowledged_path),
            ]
        )
    assert ack_exit.value.code == 0
    acknowledged = json.loads(capsys.readouterr().out)
    assert acknowledged["status"] == "acknowledged"

    resolved_path = tmp_path / "incident-resolved.json"
    allowed_slo_gate_path = _write_json(tmp_path / "slo-gate-allowed.json", _slo_gate(status="allowed"))
    with pytest.raises(SystemExit) as resolve_exit:
        cli_main.main(
            [
                "data",
                "product",
                "incident",
                "lifecycle",
                "resolve",
                "--incident",
                str(acknowledged_path),
                "--evidence",
                str(allowed_slo_gate_path),
                "--format",
                "json",
                "--output",
                str(resolved_path),
            ]
        )
    assert resolve_exit.value.code == 0
    assert json.loads(capsys.readouterr().out)["status"] == "resolved"

    route_path = tmp_path / "incident-slack.json"
    with pytest.raises(SystemExit) as route_exit:
        cli_main.main(
            [
                "data",
                "product",
                "incident",
                "route",
                "render",
                "--incident",
                str(resolved_path),
                "--provider",
                "slack",
                "--format",
                "json",
                "--output",
                str(route_path),
            ]
        )
    assert route_exit.value.code == 0
    route = json.loads(capsys.readouterr().out)
    assert route["provider"] == "slack"
    assert route["network_writes"] == []

    closeout_path = tmp_path / "closeout.json"
    allowed_budget_gate_path = _write_json(tmp_path / "budget-gate-allowed.json", _budget_gate(status="allowed"))
    with pytest.raises(SystemExit) as closeout_exit:
        cli_main.main(
            [
                "data",
                "product",
                "release",
                "closeout",
                "gate",
                "--slo-gate",
                str(allowed_slo_gate_path),
                "--budget-gate",
                str(allowed_budget_gate_path),
                "--incident",
                str(resolved_path),
                "--watch-certificate",
                str(_write_json(tmp_path / "watch.json", _watch_certificate())),
                "--policy-gate",
                str(
                    _write_json(
                        tmp_path / "policy-gate.json",
                        {
                            "schema_version": "dpone.data_product_policy_gate.v1",
                            "policy_gate_id": "sha256:" + "9" * 64,
                            "status": "waived",
                            "blockers": [],
                            "warnings": [],
                        },
                    )
                ),
                "--format",
                "json",
                "--output",
                str(closeout_path),
            ]
        )
    assert closeout_exit.value.code == 0
    closeout = json.loads(capsys.readouterr().out)
    assert closeout["schema_version"] == "dpone.data_product_release_closeout_gate.v1"
    assert closeout["status"] == "allowed"
    assert closeout["policy_gate_id"] == "sha256:" + "9" * 64
    assert (
        json.loads(closeout_path.read_text(encoding="utf-8"))["release_closeout_gate_id"]
        == closeout["release_closeout_gate_id"]
    )


def test_data_product_reliability_bundle_and_registry_cli_hooks(
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
    budget_gate_path = _write_json(tmp_path / "budget-gate.json", _budget_gate(status="allowed", pack_id=pack.pack_id))
    incident_path = _write_json(tmp_path / "incident.json", _incident_lifecycle(pack_id=pack.pack_id))
    closeout_path = _write_json(tmp_path / "closeout.json", _closeout_gate(pack_id=pack.pack_id))
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
                "--data-product-error-budget-gate",
                str(budget_gate_path),
                "--data-product-incident-lifecycle",
                str(incident_path),
                "--data-product-release-closeout-gate",
                str(closeout_path),
                "--output-dir",
                str(bundle_dir),
                "--format",
                "json",
            ]
        )
    assert build_exit.value.code == 0
    bundle = json.loads(capsys.readouterr().out)
    assert bundle["summary"]["data_product_error_budget_gate_id"] == "sha256:" + "2" * 64
    assert bundle["summary"]["data_product_incident_lifecycle_id"] == "sha256:" + "4" * 64
    assert bundle["summary"]["data_product_release_closeout_gate_id"] == "sha256:" + "5" * 64

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
                "--data-product-error-budget-gate",
                str(budget_gate_path),
                "--data-product-incident-lifecycle",
                str(incident_path),
                "--data-product-release-closeout-gate",
                str(closeout_path),
                "--environment",
                "prod",
                "--stage",
                "release_closeout_passed",
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
    assert record["stage"] == "release_closeout_passed"
    assert {
        "data_product_error_budget_gate",
        "data_product_incident_lifecycle",
        "data_product_release_closeout_gate",
    } <= {item["kind"] for item in record["artifact_refs"]}


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _manifest() -> dict[str, object]:
    return {
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {
                "data_product": {
                    "id": "analytics.orders",
                    "owner": "data-platform",
                    "tier": "gold",
                    "criticality": "high",
                    "slo": {
                        "enabled": True,
                        "error_budget": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "objective_window": "30d",
                            "windows": [
                                {"name": "fast_burn", "duration": "1h", "max_burn_rate": 14.4},
                                {"name": "monthly", "duration": "30d", "min_budget_remaining": 0.10},
                            ],
                            "release_policy": {"freeze_on_budget_exhausted": True},
                        },
                        "incident_lifecycle": {
                            "enabled": True,
                            "mode": "gate",
                            "require_ack_for_sev1": True,
                            "require_resolution_evidence": True,
                            "routing": {"enabled": True, "mode": "render", "providers": ["slack"]},
                        },
                    },
                }
            },
        }
    }


def _slo_evaluation(*, status: str, recorded_at: str = "2026-06-28T12:00:00Z") -> dict[str, object]:
    blockers = ["data_product_slo.freshness_breach"] if status == "blocked" else []
    return {
        "schema_version": "dpone.data_product_slo_evaluation.v1",
        "status": status,
        "product_id": "analytics.orders",
        "slo_evaluation_id": "sha256:" + ("8" if status == "blocked" else "9") * 64,
        "recorded_at": recorded_at,
        "blockers": blockers,
        "warnings": [],
        "checks": [{"name": "freshness", "status": "failed" if blockers else "passed", "details": blockers}],
    }


def _slo_gate(*, status: str) -> dict[str, object]:
    return {
        "schema_version": "dpone.data_product_slo_gate.v1",
        "status": status,
        "product_id": "analytics.orders",
        "slo_gate_id": "sha256:" + "1" * 64,
        "blockers": ["data_product_slo.freshness_breach"] if status == "blocked" else [],
        "warnings": [],
    }


def _budget_gate(*, status: str, pack_id: str | None = None) -> dict[str, object]:
    return {
        "schema_version": "dpone.data_product_error_budget_gate.v1",
        "status": status,
        "product_id": "analytics.orders",
        "error_budget_gate_id": "sha256:" + "2" * 64,
        "pack_id": pack_id,
        "blockers": ["data_product_error_budget.fast_burn_exceeded:fast_burn"] if status == "blocked" else [],
        "warnings": [],
    }


def _watch_certificate() -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_watch_certificate.v1",
        "status": "stable",
        "certificate_id": "sha256:" + "3" * 64,
        "blockers": [],
        "warnings": [],
    }


def _incident_lifecycle(*, pack_id: str) -> dict[str, object]:
    return {
        "schema_version": "dpone.data_product_incident_lifecycle.v1",
        "status": "resolved",
        "incident_id": "sha256:" + "4" * 64,
        "product_id": "analytics.orders",
        "severity": "sev2",
        "signals": ["freshness_breach"],
        "pack_id": pack_id,
        "events": [{"kind": "opened"}, {"kind": "acknowledged"}, {"kind": "resolved"}],
        "blockers": [],
        "warnings": [],
    }


def _closeout_gate(*, pack_id: str) -> dict[str, object]:
    return {
        "schema_version": "dpone.data_product_release_closeout_gate.v1",
        "status": "allowed",
        "release_closeout_gate_id": "sha256:" + "5" * 64,
        "product_id": "analytics.orders",
        "pack_id": pack_id,
        "blockers": [],
        "warnings": [],
    }


def _write_yaml(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path
