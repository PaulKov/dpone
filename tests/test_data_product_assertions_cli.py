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


def test_data_product_assertions_cli_plan_evaluate_gate_and_report(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest_path = _write_yaml(tmp_path / "orders.yaml", _manifest(tmp_path))

    for args in (
        ["data", "product", "assertions", "--help"],
        ["data", "product", "assertions", "plan", "--help"],
        ["data", "product", "assertions", "evaluate", "--help"],
        ["data", "product", "assertions", "gate", "--help"],
        ["data", "product", "assertions", "report", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    plan_path = tmp_path / "assertion-plan.json"
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "data",
                "product",
                "assertions",
                "plan",
                "--manifest",
                str(manifest_path),
                "--format",
                "json",
                "--output",
                str(plan_path),
            ]
        )
    assert plan_exit.value.code == 0
    plan_console = json.loads(capsys.readouterr().out)
    plan_file = json.loads(plan_path.read_text(encoding="utf-8"))
    assert plan_console["assertion_plan_id"] == plan_file["assertion_plan_id"]
    assert plan_file["schema_version"] == "dpone.data_product_assertion_plan.v1"
    assert plan_file["status"] == "ready"

    runtime_path = _write_json(tmp_path / "latest-run.json", _runtime(freshness_lag_seconds=1800, rows_loaded=0))
    target_evidence_path = _write_json(
        tmp_path / "target-evidence.json",
        {
            "row_count": 0,
            "null_key_failures": {"order_id": 1},
            "duplicate_key_failures": {"order_id": 1},
            "sql_results": {"amount_non_negative": {"failures": 2}},
        },
    )
    evaluation_path = tmp_path / "assertion-evaluation.json"
    with pytest.raises(SystemExit) as eval_exit:
        cli_main.main(
            [
                "data",
                "product",
                "assertions",
                "evaluate",
                "--plan",
                str(plan_path),
                "--runtime-artifact",
                str(runtime_path),
                "--target-evidence",
                str(target_evidence_path),
                "--format",
                "json",
                "--output",
                str(evaluation_path),
            ]
        )
    assert eval_exit.value.code == 2
    evaluation = json.loads(capsys.readouterr().out)
    assert evaluation["schema_version"] == "dpone.data_product_assertion_evaluation.v1"
    assert evaluation["status"] == "blocked"
    assert (
        json.loads(evaluation_path.read_text(encoding="utf-8"))["assertion_evaluation_id"]
        == evaluation["assertion_evaluation_id"]
    )

    gate_path = tmp_path / "assertion-gate.json"
    with pytest.raises(SystemExit) as gate_exit:
        cli_main.main(
            [
                "data",
                "product",
                "assertions",
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
    assert gate["schema_version"] == "dpone.data_product_assertion_gate.v1"
    assert gate["status"] == "blocked"
    assert json.loads(gate_path.read_text(encoding="utf-8"))["assertion_gate_id"] == gate["assertion_gate_id"]

    report_path = tmp_path / "assertion-report.md"
    with pytest.raises(SystemExit) as report_exit:
        cli_main.main(
            [
                "data",
                "product",
                "assertions",
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
    markdown = capsys.readouterr().out
    assert "# Data Product Assertion Report" in markdown
    assert report_path.read_text(encoding="utf-8") == markdown


def test_data_product_assertions_bundle_and_registry_cli_hooks(
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
    assertion_gate_path = _write_json(
        tmp_path / "assertion-gate.json",
        {
            "schema_version": "dpone.data_product_assertion_gate.v1",
            "assertion_gate_id": "sha256:" + "3" * 64,
            "status": "allowed",
            "profile": "prod_strict",
            "product_id": "analytics.orders",
            "pack_id": pack.pack_id,
            "blockers": [],
            "warnings": [],
        },
    )
    assertion_report_path = _write_json(
        tmp_path / "assertion-report.json",
        {
            "schema_version": "dpone.data_product_assertion_report.v1",
            "assertion_report_id": "sha256:" + "4" * 64,
            "status": "passed",
            "product_id": "analytics.orders",
            "pack_id": pack.pack_id,
            "markdown": "# Data Product Assertion Report",
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
                "--data-product-assertion-gate",
                str(assertion_gate_path),
                "--data-product-assertion-report",
                str(assertion_report_path),
                "--output-dir",
                str(bundle_dir),
                "--format",
                "json",
            ]
        )
    assert build_exit.value.code == 0
    bundle = json.loads(capsys.readouterr().out)
    assert bundle["summary"]["data_product_assertion_gate_id"] == "sha256:" + "3" * 64
    assert bundle["summary"]["data_product_assertion_report_id"] == "sha256:" + "4" * 64

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
                "--data-product-assertion-gate",
                str(assertion_gate_path),
                "--data-product-assertion-report",
                str(assertion_report_path),
                "--environment",
                "prod",
                "--stage",
                "assertion_gate_passed",
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
    assert record["stage"] == "assertion_gate_passed"
    assert {"data_product_assertion_gate", "data_product_assertion_report"} <= {
        item["kind"] for item in record["artifact_refs"]
    }


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _write_yaml(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return path


def _manifest(tmp_path: Path) -> dict[str, object]:
    _write_json(tmp_path / "run_results.json", {"results": [{"unique_id": "test.orders.not_null", "status": "pass"}]})
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
                    "assertions": {
                        "enabled": True,
                        "mode": "gate",
                        "profile": "prod_strict",
                        "unknown_assertion_source": "warn",
                        "suites": [
                            {
                                "id": "orders_contract_quality",
                                "owner": "data-platform",
                                "severity": "critical",
                                "assertions": [
                                    {"id": "order_id_not_null", "type": "null_key", "columns": ["order_id"]},
                                    {"id": "order_id_unique", "type": "duplicate_key", "columns": ["order_id"]},
                                    {"id": "row_count_positive", "type": "row_count", "min_rows": 1},
                                    {
                                        "id": "amount_non_negative",
                                        "type": "sql",
                                        "query": ("SELECT count() AS failures FROM analytics.orders WHERE amount < 0"),
                                        "expect": {"column": "failures", "equals": 0},
                                    },
                                ],
                            }
                        ],
                        "imports": {"dbt_run_results": str(tmp_path / "run_results.json")},
                    },
                },
            },
        }
    }


def _runtime(*, freshness_lag_seconds: int, rows_loaded: int) -> dict[str, object]:
    return {
        "schema_version": "dpone.run.latest.v1",
        "status": "success",
        "freshness_lag_seconds": freshness_lag_seconds,
        "rows_loaded": rows_loaded,
    }
