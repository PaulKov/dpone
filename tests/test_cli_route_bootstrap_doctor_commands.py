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
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _schema(path: Path) -> Path:
    return _write_json(
        path,
        {
            "tables": [
                {
                    "schema": "dbo",
                    "name": "orders",
                    "row_count": 10000,
                    "columns": [
                        {"name": "order_id", "type": "int", "nullable": False},
                        {"name": "updated_at", "type": "datetime2", "nullable": False},
                    ],
                }
            ]
        },
    )


def test_ops_connection_doctor_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    replayable_doctor_import_surface: None,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "connection-doctor",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--tool",
                "python",
                "--python-import",
                "json",
                "--output-dir",
                str(tmp_path / "connection-doctor"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.connection_doctor.v1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert payload["passed"] is True
    assert Path(payload["json_path"]).exists()


def test_ops_source_discover_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "source-discover",
                "--source",
                "mssql",
                "--dataset",
                "dbo.orders",
                "--schema-json",
                str(_schema(tmp_path / "schema.json")),
                "--output-dir",
                str(tmp_path / "source-discover"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.source_discovery.v1"
    assert payload["tables"][0]["qualified_name"] == "dbo.orders"
    assert payload["tables"][0]["primary_key_candidates"] == ["order_id"]


def test_ops_route_bootstrap_cli_outputs_manifest(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    discovery = _write_json(
        tmp_path / "source_discovery.json",
        {
            "schema_version": "dpone.source_discovery.v1",
            "source": "mssql",
            "dataset": "dbo.orders",
            "passed": True,
            "tables": [
                {
                    "qualified_name": "dbo.orders",
                    "columns": [{"name": "order_id", "type": "int", "nullable": False, "risk_level": "ready"}],
                }
            ],
            "warnings": [],
            "blockers": [],
        },
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-bootstrap",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--dataset",
                "dbo.orders",
                "--source-discovery-json",
                str(discovery),
                "--output-dir",
                str(tmp_path / "route-bootstrap"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.route_bootstrap.v1"
    assert payload["passed"] is True
    assert Path(payload["manifest_path"]).exists()
    assert any("route-readiness" in command for command in payload["next_commands"])


def test_ops_route_doctor_cli_blocks_missing_required_artifact(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-doctor",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--require",
                "connection_doctor",
                "--output-dir",
                str(tmp_path / "route-doctor"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.route_doctor.v1"
    assert payload["passed"] is False
    assert "connection_doctor.missing" in payload["blockers"]
