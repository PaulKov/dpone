from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_schema_identity_plan_cli_outputs_json_md_and_file(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path)
    source = _write_columns(tmp_path / "source.json", [{"name": "client_id", "dtype": "bigint"}])
    actual = tmp_path / "actual.json"
    actual.write_text(
        json.dumps(
            {
                "sink_type": "clickhouse",
                "table": "landing.orders",
                "columns": {"customer_id": {"type": "Int64"}},
            }
        ),
        encoding="utf-8",
    )
    output = tmp_path / "identity.json"

    with pytest.raises(SystemExit) as json_exit:
        cli_main.main(
            [
                "schema",
                "identity",
                "plan",
                "--manifest",
                str(manifest),
                "--source",
                str(source),
                "--actual",
                str(actual),
                "--output",
                str(output),
                "--format",
                "json",
            ]
        )

    assert json_exit.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.schema_identity_plan.v1"
    assert payload["identity_decisions"][0]["action"] == "rename_alias"
    assert payload["canonical_source_schema"] == [{"name": "customer_id", "dtype": "bigint", "nullable": True}]
    assert json.loads(output.read_text(encoding="utf-8"))["identity_decisions"][0]["observed_name"] == "client_id"

    with pytest.raises(SystemExit) as md_exit:
        cli_main.main(
            ["schema", "identity", "plan", "--manifest", str(manifest), "--source", str(source), "--format", "md"]
        )

    assert md_exit.value.code == 0
    assert "rename_alias" in capsys.readouterr().out


def test_schema_identity_plan_cli_returns_actionable_config_error(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(
        tmp_path,
        columns={
            "customer_id": {"id": "orders.party"},
            "buyer_id": {"id": "orders.party"},
        },
    )
    source = _write_columns(tmp_path / "source.json", [{"name": "client_id", "dtype": "bigint"}])

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            ["schema", "identity", "plan", "--manifest", str(manifest), "--source", str(source), "--format", "json"]
        )

    assert exc.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert payload["blockers"][0].startswith("schema_identity.configuration_error")


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_manifest(
    tmp_path: Path,
    *,
    columns: dict[str, object] | None = None,
) -> Path:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "source": {
                    "type": "mssql",
                    "table": {"schema": "dbo", "name": "orders"},
                },
                "sink": {
                    "type": "clickhouse",
                    "table": {"schema": "landing", "name": "orders"},
                    "options": {
                        "schema_identity": {
                            "enabled": True,
                            "columns": columns
                            or {
                                "customer_id": {
                                    "id": "orders.customer_id",
                                    "aliases": [{"name": "client_id"}],
                                }
                            },
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return manifest


def _write_columns(path: Path, columns: list[dict[str, object]]) -> Path:
    path.write_text(json.dumps(columns), encoding="utf-8")
    return path
