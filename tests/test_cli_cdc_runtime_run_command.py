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


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _events_payload(*, duplicate: bool = False) -> dict[str, object]:
    event = {
        "operation": "update",
        "position": "0x11",
        "sequence": 1,
        "source_schema": "dbo",
        "source_table": "orders",
        "data": {"order_id": 1, "status": "paid"},
        "metadata": {"backend": "mssql_cdc"},
    }
    return {
        "high_watermark": "0x12",
        "next_offset": {"backend": "mssql_cdc", "token": "0x12", "snapshot_complete": True},
        "changes": [event, event] if duplicate else [event],
    }


def test_ops_cdc_runtime_run_cli_outputs_json_and_commits_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    events = _write_json(tmp_path / "events.json", _events_payload())
    checkpoint = tmp_path / "checkpoint.json"

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-runtime-run",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--backend",
                "mssql_cdc",
                "--pipeline-name",
                "orders-cdc",
                "--source-schema",
                "dbo",
                "--source-table",
                "orders",
                "--target-dataset",
                "analytics.orders",
                "--unique-key",
                "order_id",
                "--events-json",
                str(events),
                "--checkpoint-json",
                str(checkpoint),
                "--output-dir",
                str(tmp_path / "runtime"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    checkpoint_payload = json.loads(checkpoint.read_text(encoding="utf-8"))
    assert payload["passed"] is True
    assert payload["committed"] is True
    assert payload["stream"]["route_id"] == "mssql_to_clickhouse__cdc"
    assert payload["sink_receipt"]["durable"] is True
    assert checkpoint_payload["offset"]["token"] == "0x12"
    assert Path(payload["sink_receipt"]["artifact_uri"]).exists()


def test_ops_cdc_runtime_run_cli_returns_nonzero_and_does_not_commit_duplicate_events(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    events = _write_json(tmp_path / "events.json", _events_payload(duplicate=True))
    checkpoint = tmp_path / "checkpoint.json"

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-runtime-run",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--backend",
                "mssql_cdc",
                "--pipeline-name",
                "orders-cdc",
                "--source-schema",
                "dbo",
                "--source-table",
                "orders",
                "--target-dataset",
                "analytics.orders",
                "--unique-key",
                "order_id",
                "--events-json",
                str(events),
                "--checkpoint-json",
                str(checkpoint),
                "--output-dir",
                str(tmp_path / "runtime"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert payload["committed"] is False
    assert "cdc_runtime.duplicate_events" in payload["blockers"]
    assert not checkpoint.exists()


def test_ops_cdc_runtime_run_cli_passes_live_mode_connection_options(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    captured: dict[str, object] = {}

    class _Report:
        passed = True

        def to_dict(self) -> dict[str, object]:
            return {"passed": True, "committed": True, "mode": "live"}

        def to_markdown(self) -> str:
            return "# fake\n"

    class _RuntimeRun:
        def run(self, **kwargs: object) -> _Report:
            captured.update(kwargs)
            return _Report()

    class _Cdc:
        def runtime_run(self) -> _RuntimeRun:
            return _RuntimeRun()

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
                "cdc-runtime-run",
                "--mode",
                "live",
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
                "--source-connection-id",
                "mssql-prod",
                "--sink-connection-id",
                "clickhouse-prod",
                "--credentials-source",
                "env",
                "--state-schema",
                "etl_state",
                "--state-table",
                "etl_cdc_offset",
                "--output-dir",
                str(tmp_path / "runtime"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    assert json.loads(capsys.readouterr().out)["mode"] == "live"
    assert captured["mode"] == "live"
    assert captured["source_connection_id"] == "mssql-prod"
    assert captured["sink_connection_id"] == "clickhouse-prod"
    assert captured["credentials_source"] == "env"
    assert captured["state_schema"] == "etl_state"
    assert captured["state_table"] == "etl_cdc_offset"
