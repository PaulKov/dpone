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


def _route_artifacts(tmp_path: Path, source: str, sink: str, strategy: str) -> list[str]:
    profile = RouteProfileCatalog.default().get(RouteKey.of(source, sink, strategy))
    assert profile is not None
    auto = {"matrix_case", "docs_runbook", "manifest_example"}
    values: list[str] = []
    for name in profile.required_evidence:
        if name in auto:
            continue
        path = _write_json(tmp_path / f"{name}.json", {"passed": True, "summary": f"{name} ok"})
        values.extend(["--artifact", f"{name}={path}"])
    return values


def test_ops_route_certification_pack_cli_outputs_json_and_readiness_paths(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-certification-pack",
                "--source",
                "postgres",
                "--sink",
                "mssql",
                "--strategy",
                "incremental_merge",
                "--output-dir",
                str(tmp_path / "pack"),
                *_route_artifacts(tmp_path, "postgres", "mssql", "incremental_merge"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["route"]["case_id"] == "postgres_to_mssql__incremental_merge"
    assert Path(payload["readiness_json_path"]).exists()
    assert Path(payload["artifacts"]["matrix_case"]).exists()


def test_ops_route_certification_pack_cli_returns_nonzero_for_missing_heavy_evidence(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-certification-pack",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--output-dir",
                str(tmp_path / "pack"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert "type_fidelity.missing" in payload["blockers"]
