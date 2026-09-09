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


def _artifact_args(tmp_path: Path, route: RouteKey, *, passed: bool = True) -> list[str]:
    profile = RouteProfileCatalog.default().get(route)
    assert profile is not None
    names = tuple(
        dict.fromkeys(
            (
                "service_markers",
                "route_execution_ledger",
                "route_refresh_verification",
                "state_promotion",
                "benchmark_slo",
                "performance_certification",
                "live_state_reconciliation",
                "pre_release_checklist",
                "evidence_chain",
                *profile.required_evidence,
                "cdc_handoff",
                "cdc_apply_certification",
                "cdc_observability_evidence",
                "cdc_recovery_evidence",
                "cdc_schema_evolution_evidence",
                "cdc_promotion_gate",
                "native_transfer_evidence",
            )
        )
    )
    values: list[str] = []
    for name in names:
        path = _write_json(
            tmp_path / f"{name}.json",
            {
                "passed": passed,
                "evidence_status": "PASS" if passed else "FAIL",
                "summary": f"{name} {'ok' if passed else 'failed'}",
                "route": route.to_dict(),
                "blockers": [] if passed else [f"{name}.failed"],
            },
        )
        values.extend(["--artifact", f"{name}={path}"])
    return values


def test_ops_route_rc_orchestrator_cli_outputs_json_release_train(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    route = RouteKey.of("mssql", "clickhouse", "incremental_merge")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-rc-orchestrator",
                "--release",
                "0.8.0-rc1",
                "--source",
                route.source,
                "--sink",
                route.sink,
                "--strategy",
                route.strategy,
                "--profile",
                "vendor_live",
                "--row-count",
                "25000",
                "--output-dir",
                str(tmp_path / "rc"),
                *_artifact_args(tmp_path, route),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["level"] == "release_ready"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert Path(payload["json_path"]).exists()
    assert Path(payload["artifact_index"]["release_evidence_pack"]).exists()


def test_ops_route_rc_orchestrator_cli_returns_nonzero_for_failed_train(
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
                "route-rc-orchestrator",
                "--release",
                "0.8.0-rc1",
                "--source",
                route.source,
                "--sink",
                route.sink,
                "--strategy",
                route.strategy,
                "--profile",
                "real_local",
                "--output-dir",
                str(tmp_path / "rc"),
                *_artifact_args(tmp_path, route, passed=False),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert "route_certification_pack.not_passed" in payload["blockers"]
