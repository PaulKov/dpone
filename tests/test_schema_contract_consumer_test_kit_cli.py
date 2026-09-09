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


def test_schema_contract_consumer_test_kit_cli_plan_render_certify(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)
    manifest = tmp_path / "manifest.yaml"
    matrix = tmp_path / "matrix.json"
    kit_output = tmp_path / "consumer-test-kit.json"
    pytest_output = tmp_path / "test_consumers.py"
    certification_output = tmp_path / "consumer-certification.json"
    manifest.write_text(yaml.safe_dump(_manifest(), sort_keys=False), encoding="utf-8")
    matrix.write_text(json.dumps(_matrix(), ensure_ascii=False), encoding="utf-8")

    for args in (
        ["schema", "contract", "consumers", "test-kit", "--help"],
        ["schema", "contract", "consumers", "test-kit", "plan", "--help"],
        ["schema", "contract", "consumers", "test-kit", "render", "--help"],
        ["schema", "contract", "consumers", "test-kit", "certify", "--help"],
    ):
        with pytest.raises(SystemExit) as help_exit:
            cli_main.main(args)
        assert help_exit.value.code == 0
    capsys.readouterr()

    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "consumers",
                "test-kit",
                "plan",
                "--manifest",
                str(manifest),
                "--matrix",
                str(matrix),
                "--format",
                "json",
                "--output",
                str(kit_output),
            ]
        )
    assert plan_exit.value.code == 0
    kit = json.loads(kit_output.read_text(encoding="utf-8"))
    assert kit["schema_version"] == "dpone.schema_contract_consumer_test_kit.v1"
    assert json.loads(capsys.readouterr().out)["test_kit_id"] == kit["test_kit_id"]

    with pytest.raises(SystemExit) as render_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "consumers",
                "test-kit",
                "render",
                "--kit",
                str(kit_output),
                "--format",
                "pytest",
                "--output",
                str(pytest_output),
            ]
        )
    assert render_exit.value.code == 0
    assert "def test_consumer_contract_finance_daily_margin" in pytest_output.read_text(encoding="utf-8")

    with pytest.raises(SystemExit) as certify_exit:
        cli_main.main(
            [
                "schema",
                "contract",
                "consumers",
                "test-kit",
                "certify",
                "--kit",
                str(kit_output),
                "--result",
                "passed",
                "--format",
                "json",
                "--output",
                str(certification_output),
            ]
        )
    assert certify_exit.value.code == 0
    certification = json.loads(certification_output.read_text(encoding="utf-8"))
    assert certification["schema_version"] == "dpone.schema_contract_consumer_certification.v1"
    assert certification["status"] == "certified"


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _manifest() -> dict[str, object]:
    return {
        "sink": {
            "type": "clickhouse",
            "table": {"schema": "analytics", "name": "orders"},
            "options": {
                "schema_contract": {
                    "id": "analytics.orders",
                    "version": "2.0.0",
                    "owner": "data-platform",
                    "compatibility": "backward",
                    "registry": {"enabled": True, "mode": "gate"},
                    "columns": {
                        "order_id": {"type": "integer", "nullable": False},
                        "customer_id": {"type": "integer", "nullable": True},
                    },
                }
            },
        }
    }


def _matrix() -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_contract_consumer_matrix.v1",
        "status": "blocked",
        "contract_id": "analytics.orders",
        "base_version": "1.5.0",
        "head_version": "2.0.0",
        "consumer_matrix_id": "sha256:" + "3" * 64,
        "required_bump": "major",
        "consumers": [
            {
                "id": "finance.daily_margin",
                "type": "dashboard",
                "owner": "finance-analytics",
                "version_constraint": "1.x",
                "reads": {"columns": ["amount", "customer_id"]},
                "status": "blocked",
                "blockers": ["schema_contract.consumer_version_incompatible:finance.daily_margin"],
                "confidence": "explicit",
                "source": "manual",
            }
        ],
        "summary": {"consumers_count": 1, "blocked_consumers": 1},
        "blockers": ["schema_contract.consumer_version_incompatible:finance.daily_margin"],
        "warnings": [],
        "reviewer_actions": ["Run generated consumer contract tests before promotion."],
    }
