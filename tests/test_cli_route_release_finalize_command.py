from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.ops.checksums import sha256_file
from dpone.ops.routes.models import RouteKey


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_bundle(path: Path, route: RouteKey, *, release: str = "v0.9.0-rc1") -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    stage = path.parent / "certification_report.json"
    stage.write_text(
        json.dumps(
            {
                "passed": True,
                "evidence_status": "PASS",
                "blockers": [],
            },
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    path.write_text(
        json.dumps(
            {
                "schema_version": "dpone.route_certification_bundle.v1",
                "release": release,
                "profile": "oss_ci",
                "route": route.to_dict(),
                "passed": True,
                "evidence_status": "PASS",
                "level": "certified",
                "score": 100.0,
                "blockers": [],
                "stages": [
                    {
                        "name": "certification_report",
                        "required": True,
                        "passed": True,
                        "blockers": [],
                        "path": stage.name,
                        "sha256": sha256_file(stage),
                    }
                ],
                "artifact_index": {"certification_report": stage.name},
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _bundle_root(tmp_path: Path, *, release: str = "v0.9.0-rc1") -> Path:
    root = tmp_path / "bundles"
    for route in (
        RouteKey.of("postgres", "mssql", "incremental_merge"),
        RouteKey.of("mssql", "clickhouse", "incremental_merge"),
    ):
        _write_bundle(root / route.case_id / "route_certification_bundle.json", route, release=release)
    return root


def test_ops_route_release_finalize_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-release-finalize",
                "--release",
                "v0.9.0-rc1",
                "--bundle-root",
                str(_bundle_root(tmp_path)),
                "--history-dir",
                str(tmp_path / "history"),
                "--output-dir",
                str(tmp_path / "final"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["level"] == "final_ready"
    assert Path(payload["json_path"]).exists()
    assert Path(payload["history_index_path"]).exists()


def test_ops_route_release_finalize_returns_nonzero_for_missing_route(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    root = tmp_path / "bundles"
    route = RouteKey.of("postgres", "mssql", "incremental_merge")
    _write_bundle(root / route.case_id / "route_certification_bundle.json", route)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-release-finalize",
                "--release",
                "v0.9.0-rc1",
                "--bundle-root",
                str(root),
                "--history-dir",
                str(tmp_path / "history"),
                "--output-dir",
                str(tmp_path / "final"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert "mssql_to_clickhouse__incremental_merge.missing" in payload["blockers"]
