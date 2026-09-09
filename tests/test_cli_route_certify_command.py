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
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _artifact_args(tmp_path: Path, route: RouteKey) -> list[str]:
    profile = RouteProfileCatalog.default().get(route)
    assert profile is not None
    names = tuple(
        dict.fromkeys(
            (
                "route_refresh_execution",
                "route_refresh_snapshot_capture",
                "route_refresh_verification",
                "route_execution_ledger",
                "state_promotion",
                "benchmark_slo",
                "pre_release_checklist",
                "evidence_chain",
                *profile.required_evidence,
            )
        )
    )
    values: list[str] = []
    for name in names:
        path = _write_json(
            tmp_path / f"{name}.json",
            {
                "passed": True,
                "summary": f"{name} ok",
                "route": route.to_dict(),
                "blockers": [],
            },
        )
        values.extend(["--artifact", f"{name}={path}"])
    return values


def test_ops_route_certify_cli_outputs_json_bundle(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    route = RouteKey.of("postgres", "mssql", "incremental_merge")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-certify",
                "--release",
                "v0.9.0-rc1",
                "--source",
                route.source,
                "--sink",
                route.sink,
                "--strategy",
                route.strategy,
                "--profile",
                "oss_ci",
                "--output-dir",
                str(tmp_path / "certify"),
                *_artifact_args(tmp_path, route),
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
    assert Path(payload["artifact_index"]["route_promotion_gate"]).exists()


def test_ops_route_certify_cli_returns_nonzero_when_required_evidence_missing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-certify",
                "--release",
                "v0.9.0-rc1",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--profile",
                "vendor_live",
                "--output-dir",
                str(tmp_path / "certify"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert "route_live_evidence_bundle.missing" in payload["blockers"]


def test_ops_route_certify_cli_reports_invalid_matrix_release_without_traceback(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-certify",
                "--release",
                "v0.72.3",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--release-set",
                str(tmp_path / "missing-release-set.json"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "dpone.error.v1"
    assert payload["code"] == "DPONE_ROUTE_MATRIX_CLAIM_RELEASE_UNSAFE"
