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


def _route_args(tmp_path: Path) -> list[str]:
    return [
        "ops",
        "route-conformance",
        "run",
        "--source",
        "mssql",
        "--sink",
        "clickhouse",
        "--strategy",
        "incremental_merge",
        "--dataset-profile",
        "wide_10k_contract",
        "--rows",
        "120",
        "--columns",
        "24",
        "--chunk-size",
        "30",
        "--include-nested",
        "--require-schema-evolution",
        "--output-dir",
        str(tmp_path / "conformance"),
        "--format",
        "json",
    ]


def test_ops_route_conformance_run_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(_route_args(tmp_path))

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.route_conformance.v1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert payload["passed"] is True
    assert payload["dataset"]["column_count"] == 24
    assert Path(payload["json_path"]).exists()


def test_ops_route_conformance_summarize_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    with pytest.raises(SystemExit):
        cli_main.main(_route_args(tmp_path))
    run_payload = json.loads(capsys.readouterr().out)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-conformance",
                "summarize",
                "--artifact",
                f"mssql_to_clickhouse={run_payload['json_path']}",
                "--output-dir",
                str(tmp_path / "summary"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.route_conformance_summary.v1"
    assert payload["passed"] is True
    assert payload["routes"][0]["name"] == "mssql_to_clickhouse"


def test_ops_route_conformance_release_gate_cli_blocks_failed_artifact(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    with pytest.raises(SystemExit) as run_exc:
        cli_main.main(
            [
                "ops",
                "route-conformance",
                "run",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--dataset-profile",
                "too_small",
                "--rows",
                "8",
                "--columns",
                "8",
                "--min-rows",
                "100",
                "--output-dir",
                str(tmp_path / "failed"),
                "--format",
                "json",
            ]
        )
    failed_payload = json.loads(capsys.readouterr().out)
    assert run_exc.value.code == 1

    with pytest.raises(SystemExit) as gate_exc:
        cli_main.main(
            [
                "ops",
                "route-conformance",
                "release-gate",
                "--release",
                "v0.10.0-rc1",
                "--artifact",
                f"mssql_to_clickhouse={failed_payload['json_path']}",
                "--output-dir",
                str(tmp_path / "gate"),
                "--format",
                "json",
            ]
        )

    payload = json.loads(capsys.readouterr().out)
    assert gate_exc.value.code == 1
    assert payload["schema_version"] == "dpone.route_conformance_release_gate.v1"
    assert payload["passed"] is False
    assert "mssql_to_clickhouse.not_passed" in payload["blockers"]


def test_ops_route_conformance_live_run_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-conformance",
                "live-run",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--adapter",
                "in_memory",
                "--dataset-profile",
                "wide_live_contract",
                "--rows",
                "120",
                "--columns",
                "24",
                "--chunk-size",
                "30",
                "--include-nested",
                "--require-schema-evolution",
                "--min-rows",
                "100",
                "--min-columns",
                "20",
                "--output-dir",
                str(tmp_path / "live"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.route_conformance_live.v1"
    assert payload["passed"] is True
    assert payload["adapter"] == "in_memory"
    assert payload["verification"]["source_rows"] == 120


def test_ops_route_conformance_live_run_cli_blocks_drift(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-conformance",
                "live-run",
                "--source",
                "postgres",
                "--sink",
                "mssql",
                "--strategy",
                "incremental_merge",
                "--adapter",
                "in_memory",
                "--drift",
                "value",
                "--rows",
                "16",
                "--columns",
                "10",
                "--chunk-size",
                "4",
                "--min-rows",
                "10",
                "--output-dir",
                str(tmp_path / "live-drift"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert "typed_hash.mismatch" in payload["blockers"]


def test_ops_route_conformance_live_run_cli_blocks_vendor_live_without_opt_in(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    monkeypatch.delenv("DPONE_VENDOR_LIVE", raising=False)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-conformance",
                "live-run",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--adapter",
                "vendor_live",
                "--rows",
                "16",
                "--columns",
                "10",
                "--chunk-size",
                "4",
                "--output-dir",
                str(tmp_path / "vendor-live-missing-opt-in"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is False
    assert payload["conformance"] is None
    assert payload["live_steps"][0]["name"] == "seed_source"
    assert "vendor_live.opt_in_missing:DPONE_VENDOR_LIVE" in payload["blockers"]
