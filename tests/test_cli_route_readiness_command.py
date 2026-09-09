from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.ops.routes.catalog import RouteProfileCatalog
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


def _green_route_artifacts(tmp_path: Path, source: str, sink: str, strategy: str) -> list[str]:
    profile = RouteProfileCatalog.default().get(RouteKey.of(source, sink, strategy))
    assert profile is not None
    values: list[str] = []
    for name in profile.required_evidence:
        path = _write_json(tmp_path / f"{name}.json", {"passed": True, "summary": f"{name} ok"})
        values.extend(["--artifact", f"{name}={path}"])
    return values


def test_ops_route_readiness_cli_outputs_json_and_writes_artifacts(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-readiness",
                "--source",
                "postgres",
                "--sink",
                "mssql",
                "--strategy",
                "incremental_merge",
                "--output-dir",
                str(tmp_path / "route-readiness"),
                *_green_route_artifacts(tmp_path, "postgres", "mssql", "incremental_merge"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["level"] == "certified"
    assert payload["route"]["case_id"] == "postgres_to_mssql__incremental_merge"
    assert Path(payload["json_path"]).exists()
    assert Path(payload["markdown_path"]).exists()


def test_ops_route_readiness_cli_returns_nonzero_for_missing_required_evidence(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-readiness",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--output-dir",
                str(tmp_path / "route-readiness"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert payload["level"] == "blocked"
    assert "matrix_case.missing" in payload["blockers"]
