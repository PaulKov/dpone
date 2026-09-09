from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.ops.route_execution import RouteExecutionService


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _ledger(tmp_path: Path) -> Path:
    report = RouteExecutionService().record_step(
        output_dir=tmp_path / "ledger",
        source="mssql",
        sink="clickhouse",
        strategy="cdc",
        dataset="dbo.orders",
        run_id="run-1",
        stage="loaded_to_staging",
        status="succeeded",
        runner_id="worker-a",
        source_boundary="lsn:001",
        sink_boundary="clickhouse:events:10",
        idempotency_key="load-window-001",
    )
    return Path(report.json_path)


def test_ops_route_state_promote_cli_outputs_json_and_writes_state(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    ledger_json = _ledger(tmp_path)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-state-promote",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "cdc",
                "--dataset",
                "dbo.orders",
                "--run-id",
                "run-1",
                "--ledger-json",
                str(ledger_json),
                "--proposed-state",
                "lsn:001",
                "--source-boundary",
                "lsn:001",
                "--sink-boundary",
                "clickhouse:events:10",
                "--idempotency-key",
                "promote-lsn-001",
                "--commit-token",
                "clickhouse-part-0001",
                "--target",
                "analytics.orders_cdc",
                "--state-backend",
                "sqlite",
                "--state-uri",
                str(tmp_path / "state.sqlite3"),
                "--output-dir",
                str(tmp_path / "promotion"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)

    assert exc.value.code == 0
    assert payload["schema_version"] == "dpone.route_state_promotion.v1"
    assert payload["passed"] is True
    assert payload["state_backend"] == "sqlite"
    assert payload["promoted_state"]["source_state"] == "lsn:001"
    assert Path(payload["json_path"]).exists()
    assert Path(payload["state_path"]).exists()


def test_ops_route_state_promote_cli_returns_nonzero_for_blockers(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    ledger_json = _ledger(tmp_path)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-state-promote",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "cdc",
                "--dataset",
                "dbo.orders",
                "--run-id",
                "run-1",
                "--ledger-json",
                str(ledger_json),
                "--proposed-state",
                "lsn:002",
                "--source-boundary",
                "lsn:002",
                "--sink-boundary",
                "clickhouse:events:10",
                "--idempotency-key",
                "promote-lsn-002",
                "--commit-token",
                "clickhouse-part-0002",
                "--target",
                "analytics.orders_cdc",
                "--output-dir",
                str(tmp_path / "promotion"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)

    assert exc.value.code == 1
    assert payload["passed"] is False
    assert "state_promotion.boundary_mismatch" in payload["blockers"]
