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


class _Report:
    def __init__(self, *, schema_version: str, passed: bool) -> None:
        self.passed = passed
        self._schema_version = schema_version

    def to_dict(self) -> dict[str, object]:
        return {"schema_version": self._schema_version, "passed": self.passed}

    def to_markdown(self) -> str:
        return "# fake\n"


def _install_catalog(monkeypatch: pytest.MonkeyPatch, cdc: object) -> None:
    class _Release:
        def cdc(self) -> object:
            return cdc

    monkeypatch.setattr(
        command_handlers_cdc.OpsServiceCatalog,
        "default",
        staticmethod(lambda: SimpleNamespace(release=_Release())),
    )


def test_ops_cdc_retention_check_cli_delegates_local_options(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    captured: dict[str, object] = {}

    class _Retention:
        def evaluate(self, **kwargs: object) -> _Report:
            captured.update(kwargs)
            return _Report(schema_version="dpone.cdc_retention_check.v1", passed=False)

    class _Cdc:
        def retention_check(self) -> _Retention:
            return _Retention()

    _install_catalog(monkeypatch, _Cdc())

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-retention-check",
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
                "--mode",
                "local",
                "--committed-offset",
                "99",
                "--min-available-offset",
                "100",
                "--high-watermark",
                "150",
                "--current-offset",
                "150",
                "--retention-seconds",
                "172800",
                "--output-dir",
                str(tmp_path / "retention"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 1
    assert payload["passed"] is False
    assert captured["mode"] == "local"
    assert captured["committed_offset"] == "99"
    assert captured["min_available_offset"] == "100"
    assert captured["retention_seconds"] == 172800
    assert captured["unique_key"] == ("order_id",)


def test_ops_cdc_resync_plan_cli_delegates_rows_and_retention_report(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    retention_report = _write_json(tmp_path / "cdc_retention_check.json", {"schema_version": "x"})
    rows_json = _write_json(tmp_path / "rows.json", {"rows": [{"order_id": 1}]})
    captured: dict[str, object] = {}

    class _Planner:
        def plan(self, **kwargs: object) -> _Report:
            captured.update(kwargs)
            return _Report(schema_version="dpone.cdc_resync_plan_report.v1", passed=True)

    class _Cdc:
        def resync_plan(self) -> _Planner:
            return _Planner()

    _install_catalog(monkeypatch, _Cdc())

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-resync-plan",
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
                "--retention-report-json",
                str(retention_report),
                "--rows-json",
                str(rows_json),
                "--max-rows",
                "25",
                "--output-dir",
                str(tmp_path / "plan"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["schema_version"] == "dpone.cdc_resync_plan_report.v1"
    assert captured["retention_report_json"] == str(retention_report)
    assert captured["rows_json"] == str(rows_json)
    assert captured["max_rows"] == 25


def test_ops_cdc_resync_execute_cli_delegates_live_options(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    plan_json = _write_json(tmp_path / "cdc_resync_plan.json", {"schema_version": "x"})
    captured: dict[str, object] = {}

    class _Execution:
        def execute(self, **kwargs: object) -> _Report:
            captured.update(kwargs)
            return _Report(schema_version="dpone.cdc_resync_execution.v1", passed=True)

    class _Cdc:
        def resync_execution(self) -> _Execution:
            return _Execution()

    _install_catalog(monkeypatch, _Cdc())

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-resync-execute",
                "--mode",
                "live",
                "--resync-plan-json",
                str(plan_json),
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
                "10",
                "--output-dir",
                str(tmp_path / "execute"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["passed"] is True
    assert captured["mode"] == "live"
    assert captured["resync_plan_json"] == str(plan_json)
    assert captured["sink_connection_id"] == "clickhouse-prod"
    assert captured["max_actions"] == 10
