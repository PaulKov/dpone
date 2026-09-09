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


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _execution_json(tmp_path: Path) -> Path:
    route = RouteKey.of("postgres", "mssql", "incremental_merge")
    return _write_json(
        tmp_path / "execute" / "route_refresh_execution.json",
        {
            "schema_version": "dpone.route_refresh_execution.v1",
            "route": route.to_dict(),
            "dataset": "dbo.orders",
            "runner_id": "operator-a",
            "mode": "execute",
            "executed": True,
            "status": "succeeded",
            "passed": True,
            "ready_for_state_promotion": True,
            "summary": {"chunks_total": 1, "chunks_succeeded": 1, "rows_read": 2, "rows_written": 2},
            "chunks": [
                {
                    "ordinal": 1,
                    "idempotency_key": "postgres_to_mssql__incremental_merge:dbo.orders:1:1..2",
                    "status": "succeeded",
                    "passed": True,
                    "rows_read": 2,
                    "rows_written": 2,
                    "artifact_path": "",
                    "summary": "chunk applied",
                    "blockers": [],
                    "warnings": [],
                }
            ],
            "artifacts": [],
            "blockers": [],
            "warnings": [],
            "next_actions": [],
        },
    )


def _snapshot_json(tmp_path: Path, side: str, *, typed_hash: str) -> Path:
    return _write_json(
        tmp_path / f"{side}_snapshot.json",
        {
            "schema_version": "dpone.route_refresh_snapshot.v1",
            "side": side,
            "route": RouteKey.of("postgres", "mssql", "incremental_merge").to_dict(),
            "dataset": "dbo.orders",
            "chunks": [
                {
                    "ordinal": 1,
                    "row_count": 2,
                    "min_boundary": "1",
                    "max_boundary": "2",
                    "typed_hash": typed_hash,
                    "duplicate_keys": 0,
                    "null_keys": 0,
                }
            ],
        },
    )


def test_ops_route_refresh_verify_cli_outputs_json_for_matching_snapshots(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    digest = "a" * 64

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-refresh-verify",
                "--route-refresh-execution-json",
                str(_execution_json(tmp_path)),
                "--source-snapshot-json",
                str(_snapshot_json(tmp_path, "source", typed_hash=digest)),
                "--sink-snapshot-json",
                str(_snapshot_json(tmp_path, "sink", typed_hash=digest)),
                "--runner-id",
                "verifier-a",
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

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.route_refresh_verification.v1"
    assert payload["status"] == "verified"
    assert payload["passed"] is True
    assert payload["ready_for_state_promotion"] is True
    assert payload["summary"]["chunks_verified"] == 1
    assert Path(payload["json_path"]).exists()


def test_ops_route_refresh_verify_cli_returns_nonzero_for_typed_hash_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-refresh-verify",
                "--route-refresh-execution-json",
                str(_execution_json(tmp_path)),
                "--source-snapshot-json",
                str(_snapshot_json(tmp_path, "source", typed_hash="a" * 64)),
                "--sink-snapshot-json",
                str(_snapshot_json(tmp_path, "sink", typed_hash="b" * 64)),
                "--runner-id",
                "verifier-a",
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
    assert payload["status"] == "failed"
    assert "route_refresh_verification.typed_hash_mismatch:1" in payload["blockers"]
