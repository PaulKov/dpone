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


def _write_quarantine(path: Path) -> Path:
    path.write_text('{"schema_version":"dpone.cdc_poison_quarantine.v1","records":[]}', encoding="utf-8")
    return path


def test_ops_cdc_quarantine_inspect_cli_delegates_to_service(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    quarantine_json = _write_quarantine(tmp_path / "cdc_poison_quarantine.json")
    captured: dict[str, object] = {}

    class _Report:
        passed = True

        def to_dict(self) -> dict[str, object]:
            return {"schema_version": "dpone.cdc_quarantine_inspection.v1", "passed": True}

        def to_markdown(self) -> str:
            return "# fake\n"

    class _Inspect:
        def inspect(self, **kwargs: object) -> _Report:
            captured.update(kwargs)
            return _Report()

    class _Cdc:
        def quarantine_inspection(self) -> _Inspect:
            return _Inspect()

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
                "cdc-quarantine-inspect",
                "--quarantine-json",
                str(quarantine_json),
                "--output-dir",
                str(tmp_path / "inspect"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["passed"] is True
    assert captured["quarantine_json"] == str(quarantine_json)
    assert captured["output_dir"] == str(tmp_path / "inspect")


def test_ops_cdc_replay_execute_cli_delegates_live_options(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    quarantine_json = _write_quarantine(tmp_path / "cdc_poison_quarantine.json")
    captured: dict[str, object] = {}

    class _Report:
        passed = True

        def to_dict(self) -> dict[str, object]:
            return {"schema_version": "dpone.cdc_replay_execution.v1", "passed": True}

        def to_markdown(self) -> str:
            return "# fake\n"

    class _Replay:
        def execute(self, **kwargs: object) -> _Report:
            captured.update(kwargs)
            return _Report()

    class _Cdc:
        def replay_execution(self) -> _Replay:
            return _Replay()

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
                "cdc-replay-execute",
                "--mode",
                "live",
                "--quarantine-json",
                str(quarantine_json),
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
                "--max-events",
                "25",
                "--output-dir",
                str(tmp_path / "replay"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert exc.value.code == 0
    assert payload["passed"] is True
    assert captured["mode"] == "live"
    assert captured["quarantine_json"] == str(quarantine_json)
    assert captured["backend"] == "mssql_change_tracking"
    assert captured["unique_key"] == ("order_id",)
    assert captured["sink_connection_id"] == "clickhouse-prod"
    assert captured["max_events"] == 25
