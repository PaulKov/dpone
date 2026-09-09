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
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _fixture_payload() -> dict[str, object]:
    return {
        "unique_key": ["id"],
        "snapshot_boundary": "0x10",
        "window_start": "0x11",
        "window_end": "0x13",
        "retention_min": "0x01",
        "initial_rows": [{"id": 1, "status": "new"}, {"id": 2, "status": "new"}],
        "events": [
            {
                "operation": "update",
                "position": "0x11",
                "sequence": 1,
                "key": {"id": 1},
                "after": {"id": 1, "status": "paid"},
            },
            {"operation": "delete", "position": "0x12", "sequence": 2, "key": {"id": 2}},
        ],
        "expected_rows": [{"id": 1, "status": "paid"}],
    }


def test_ops_cdc_apply_certification_cli_outputs_json_and_handoff_paths(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    fixture = _write_json(tmp_path / "fixture.json", _fixture_payload())

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-apply-certification",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--source-dataset",
                "dbo.orders",
                "--target-dataset",
                "analytics.orders",
                "--fixture-json",
                str(fixture),
                "--output-dir",
                str(tmp_path / "cert"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__cdc"
    assert Path(payload["handoff_json_path"]).exists()
    assert Path(payload["evidence_artifacts"]["cdc_apply_correctness"]).exists()


def test_ops_cdc_apply_certification_cli_returns_nonzero_for_mismatch(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    payload = _fixture_payload()
    payload["expected_rows"] = [{"id": 1, "status": "wrong"}]
    fixture = _write_json(tmp_path / "fixture.json", payload)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-apply-certification",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--source-dataset",
                "dbo.orders",
                "--target-dataset",
                "analytics.orders",
                "--fixture-json",
                str(fixture),
                "--output-dir",
                str(tmp_path / "cert"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    result = json.loads(capsys.readouterr().out)
    assert result["passed"] is False
    assert "cdc_apply_correctness.mismatch" in result["blockers"]
