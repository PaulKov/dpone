from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.services.ops import command_handlers_cdc


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: object) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_ops_cdc_compare_repair_cli_delegates_local_options(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    source_rows = _write_json(tmp_path / "source_rows.json", [{"order_id": 1, "status": "paid"}])
    target_rows = _write_json(tmp_path / "target_rows.json", [{"order_id": 1, "status": "stale"}])
    captured: dict[str, object] = {}

    class _Report:
        passed = False

        def to_dict(self) -> dict[str, object]:
            return {"schema_version": "dpone.cdc_compare_repair.v1", "passed": False}

        def to_markdown(self) -> str:
            return "# fake\n"

    class _Compare:
        def compare(self, **kwargs: object) -> _Report:
            captured.update(kwargs)
            return _Report()

    class _Cdc:
        def compare_repair(self) -> _Compare:
            return _Compare()

    class _Release:
        def cdc(self) -> _Cdc:
            return _Cdc()

    monkeypatch.setattr(
        command_handlers_cdc.OpsServiceCatalog,
        "default",
        staticmethod(lambda: SimpleNamespace(release=_Release())),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-compare-repair",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--backend",
                "mssql_change_tracking",
                "--pipeline-name",
                "orders-cdc",
                "--source-schema",
                "dbo",
                "--source-table",
                "orders",
                "--target-dataset",
                "analytics.orders_cdc",
                "--unique-key",
                "order_id",
                "--column",
                "order_id",
                "--column",
                "status",
                "--source-rows-json",
                str(source_rows),
                "--target-rows-json",
                str(target_rows),
                "--output-dir",
                str(tmp_path / "compare"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 1
    assert payload["passed"] is False
    assert captured["mode"] == "local"
    assert captured["source_rows_json"] == str(source_rows)
    assert captured["target_rows_json"] == str(target_rows)
    assert captured["columns"] == ("order_id", "status")
    assert captured["unique_key"] == ("order_id",)


def test_ops_cdc_repair_execute_cli_delegates_live_options(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    repair_plan = _write_json(tmp_path / "cdc_repair_plan.json", {"schema_version": "dpone.cdc_repair_plan.v1"})
    captured: dict[str, object] = {}

    class _Report:
        passed = True

        def to_dict(self) -> dict[str, object]:
            return {"schema_version": "dpone.cdc_repair_execution.v1", "passed": True}

        def to_markdown(self) -> str:
            return "# fake\n"

    class _Repair:
        def execute(self, **kwargs: object) -> _Report:
            captured.update(kwargs)
            return _Report()

    class _Cdc:
        def repair_execution(self) -> _Repair:
            return _Repair()

    class _Release:
        def cdc(self) -> _Cdc:
            return _Cdc()

    monkeypatch.setattr(
        command_handlers_cdc.OpsServiceCatalog,
        "default",
        staticmethod(lambda: SimpleNamespace(release=_Release())),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-repair-execute",
                "--mode",
                "live",
                "--repair-plan-json",
                str(repair_plan),
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--backend",
                "mssql_change_tracking",
                "--pipeline-name",
                "orders-cdc",
                "--source-schema",
                "dbo",
                "--source-table",
                "orders",
                "--target-dataset",
                "analytics.orders_cdc",
                "--unique-key",
                "order_id",
                "--sink-connection-id",
                "clickhouse-prod",
                "--credentials-source",
                "env",
                "--max-actions",
                "25",
                "--output-dir",
                str(tmp_path / "repair"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["passed"] is True
    assert captured["mode"] == "live"
    assert captured["repair_plan_json"] == str(repair_plan)
    assert captured["backend"] == "mssql_change_tracking"
    assert captured["unique_key"] == ("order_id",)
    assert captured["sink_connection_id"] == "clickhouse-prod"
    assert captured["max_actions"] == 25
