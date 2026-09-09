from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.ops.routes.models import RouteKey


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_ops_route_refresh_capture_snapshots_cli_outputs_verify_ready_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as capture_exc:
        cli_main.main(
            [
                "ops",
                "route-refresh-capture-snapshots",
                "--route-refresh-execution-json",
                str(_execution_json(tmp_path)),
                "--source-rows-json",
                str(_rows_json(tmp_path, "source")),
                "--sink-rows-json",
                str(_rows_json(tmp_path, "sink")),
                "--runner-id",
                "capture-a",
                "--key",
                "order_id",
                "--boundary-column",
                "order_id",
                "--column",
                "order_id",
                "--column",
                "status",
                "--column",
                "amount",
                "--type",
                "order_id=int",
                "--type",
                "amount=decimal(18,2)",
                "--output-dir",
                str(tmp_path / "capture"),
                "--format",
                "json",
            ]
        )

    assert capture_exc.value.code == 0
    capture_payload = json.loads(capsys.readouterr().out)
    assert capture_payload["schema_version"] == "dpone.route_refresh_snapshot_capture.v1"
    assert capture_payload["status"] == "captured"
    assert capture_payload["passed"] is True
    assert capture_payload["summary"]["source_rows"] == 3
    assert capture_payload["summary"]["sink_rows"] == 3
    assert Path(capture_payload["source_snapshot_json"]).is_file()
    assert Path(capture_payload["sink_snapshot_json"]).is_file()

    with pytest.raises(SystemExit) as verify_exc:
        cli_main.main(
            [
                "ops",
                "route-refresh-verify",
                "--route-refresh-execution-json",
                str(_execution_json(tmp_path)),
                "--source-snapshot-json",
                capture_payload["source_snapshot_json"],
                "--sink-snapshot-json",
                capture_payload["sink_snapshot_json"],
                "--runner-id",
                "verify-a",
                "--key",
                "order_id",
                "--boundary-column",
                "order_id",
                "--column",
                "order_id",
                "--column",
                "status",
                "--column",
                "amount",
                "--type",
                "order_id=int",
                "--type",
                "amount=decimal(18,2)",
                "--output-dir",
                str(tmp_path / "verify"),
                "--format",
                "json",
            ]
        )

    assert verify_exc.value.code == 0
    verify_payload = json.loads(capsys.readouterr().out)
    assert verify_payload["status"] == "verified"
    assert verify_payload["summary"]["chunks_verified"] == 2


def test_ops_route_refresh_capture_snapshots_cli_returns_nonzero_for_dry_run_execution(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-refresh-capture-snapshots",
                "--route-refresh-execution-json",
                str(_execution_json(tmp_path, status="dry_run", executed=False)),
                "--source-rows-json",
                str(_rows_json(tmp_path, "source")),
                "--sink-rows-json",
                str(_rows_json(tmp_path, "sink")),
                "--runner-id",
                "capture-a",
                "--key",
                "order_id",
                "--boundary-column",
                "order_id",
                "--column",
                "order_id",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "route_refresh_snapshot_capture.execution_not_succeeded" in payload["blockers"]
    assert "route_refresh_snapshot_capture.execution_not_run" in payload["blockers"]


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _execution_json(tmp_path: Path, *, status: str = "succeeded", executed: bool = True) -> Path:
    return _write_json(
        tmp_path / "execute" / f"route_refresh_execution_{status}_{executed}.json",
        {
            "schema_version": "dpone.route_refresh_execution.v1",
            "route": RouteKey.of("postgres", "mssql", "incremental_merge").to_dict(),
            "dataset": "dbo.orders",
            "runner_id": "operator-a",
            "mode": "execute" if executed else "dry_run",
            "executed": executed,
            "status": status,
            "passed": True,
            "chunks": [
                {
                    "ordinal": 1,
                    "idempotency_key": "postgres_to_mssql__incremental_merge:dbo.orders:1:1..2",
                    "status": "succeeded",
                    "passed": True,
                    "rows_read": 2,
                    "rows_written": 2,
                    "start": "1",
                    "end": "2",
                    "artifact_path": "",
                },
                {
                    "ordinal": 2,
                    "idempotency_key": "postgres_to_mssql__incremental_merge:dbo.orders:2:3..3",
                    "status": "succeeded",
                    "passed": True,
                    "rows_read": 1,
                    "rows_written": 1,
                    "start": "3",
                    "end": "3",
                    "artifact_path": "",
                },
            ],
            "blockers": [],
            "warnings": [],
        },
    )


def _rows_json(tmp_path: Path, side: str) -> Path:
    return _write_json(
        tmp_path / f"{side}_rows.json",
        {
            "chunks": [
                {
                    "ordinal": 1,
                    "rows": [
                        {"order_id": 1, "status": "new", "amount": "10.50"},
                        {"order_id": 2, "status": "paid", "amount": "20.00"},
                    ],
                },
                {
                    "ordinal": 2,
                    "rows": [
                        {"order_id": 3, "status": "packed", "amount": "30.25"},
                    ],
                },
            ]
        },
    )
