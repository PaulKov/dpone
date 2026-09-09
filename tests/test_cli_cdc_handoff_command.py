from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.ops.cdc.catalog import CdcHandoffCatalog
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
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def _cdc_artifacts(tmp_path: Path) -> list[str]:
    profile = CdcHandoffCatalog.default().get(RouteKey.of("mssql", "clickhouse", "cdc"))
    assert profile is not None
    values: list[str] = []
    for name in profile.required_evidence:
        path = _write_json(tmp_path / f"{name}.json", {"passed": True, "summary": f"{name} ok"})
        values.extend(["--artifact", f"{name}={path}"])
    return values


def test_ops_cdc_handoff_cli_outputs_json_report(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-handoff",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "cdc",
                "--source-dataset",
                "dbo.orders",
                "--target-dataset",
                "analytics.orders",
                "--output-dir",
                str(tmp_path / "handoff"),
                *_cdc_artifacts(tmp_path),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["stream"]["stream_id"] == "mssql_to_clickhouse__cdc__dbo_orders__analytics_orders"
    assert Path(payload["json_path"]).exists()


def test_ops_cdc_handoff_cli_returns_nonzero_for_missing_apply_evidence(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "cdc-handoff",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "cdc",
                "--source-dataset",
                "dbo.orders",
                "--target-dataset",
                "analytics.orders",
                "--output-dir",
                str(tmp_path / "handoff"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert "cdc_apply_correctness.missing" in payload["blockers"]
