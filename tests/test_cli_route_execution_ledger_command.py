from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def test_ops_route_execution_ledger_cli_outputs_json_and_writes_report(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    artifact = _write_json(tmp_path / "cdc_apply.json", {"passed": True})

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-execution-ledger",
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
                "--stage",
                "loaded_to_staging",
                "--status",
                "succeeded",
                "--runner-id",
                "worker-a",
                "--source-boundary",
                "lsn:001",
                "--sink-boundary",
                "clickhouse:events:10",
                "--idempotency-key",
                "load-window-001",
                "--artifact",
                f"cdc_apply={artifact}",
                "--output-dir",
                str(tmp_path / "ledger"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)

    assert exc.value.code == 0
    assert payload["schema_version"] == "dpone.route_execution_ledger.v1"
    assert payload["passed"] is True
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__cdc"
    assert payload["steps"][0]["artifact_hashes"]["cdc_apply"]["sha256"] != "0" * 64
    assert Path(payload["json_path"]).exists()
    assert Path(payload["markdown_path"]).exists()


def test_ops_route_execution_ledger_cli_returns_nonzero_for_protocol_blockers(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-execution-ledger",
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
                "--stage",
                "state_committed",
                "--status",
                "committed",
                "--runner-id",
                "worker-a",
                "--output-dir",
                str(tmp_path / "ledger"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)

    assert exc.value.code == 1
    assert payload["passed"] is False
    assert "route_execution.state_commit_before_sink_success" in payload["blockers"]


def test_ops_route_execution_ledger_cli_supports_sqlite_store_backend(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    db_path = tmp_path / "shared.sqlite3"

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-execution-ledger",
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
                "--stage",
                "loaded_to_staging",
                "--status",
                "succeeded",
                "--runner-id",
                "worker-a",
                "--source-boundary",
                "lsn:001",
                "--sink-boundary",
                "clickhouse:events:1",
                "--idempotency-key",
                "window-1",
                "--store-backend",
                "sqlite",
                "--store-uri",
                str(db_path),
                "--output-dir",
                str(tmp_path / "ledger"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)

    assert exc.value.code == 0
    assert payload["passed"] is True
    assert payload["store_backend"] == "sqlite"
    assert payload["ledger_path"] == str(db_path)
    assert db_path.exists()
