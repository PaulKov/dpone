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


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _bundle(path: Path, route: RouteKey) -> Path:
    stage = _write_json(
        path.parent / "certification_report.json",
        {
            "passed": True,
            "evidence_status": "PASS",
            "blockers": [],
        },
    )
    return _write_json(
        path,
        {
            "schema_version": "dpone.route_certification_bundle.v1",
            "release": "v0.9.0-rc1",
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
    )


def _bundle_args(tmp_path: Path) -> list[str]:
    routes = (
        RouteKey.of("postgres", "mssql", "incremental_merge"),
        RouteKey.of("mssql", "clickhouse", "incremental_merge"),
    )
    values: list[str] = []
    for route in routes:
        path = _bundle(tmp_path / route.case_id / "route_certification_bundle.json", route)
        values.extend(["--route-bundle", f"{route.case_id}={path}"])
    return values


def test_ops_route_certify_release_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-certify-release",
                "--release",
                "v0.9.0-rc1",
                "--profile",
                "oss_ci",
                "--output-dir",
                str(tmp_path / "release"),
                *_bundle_args(tmp_path),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["level"] == "release_ready"
    assert payload["route_index"]["postgres_to_mssql__incremental_merge"]["passed"] is True
    assert Path(payload["json_path"]).exists()


def test_ops_route_certify_release_cli_returns_nonzero_for_missing_route(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-certify-release",
                "--release",
                "v0.9.0-rc1",
                "--profile",
                "vendor_live",
                "--output-dir",
                str(tmp_path / "release"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert "postgres_to_mssql__incremental_merge.missing" in payload["blockers"]
    assert "mssql_to_clickhouse__incremental_merge.missing" in payload["blockers"]
