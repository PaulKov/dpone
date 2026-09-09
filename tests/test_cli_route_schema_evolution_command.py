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
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_ops_route_schema_evolution_cli_outputs_json(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    schema_evolution = _write_json(
        tmp_path / "schema_evolution.json",
        {
            "schema_version": "dpone.cdc_schema_evolution_evidence.v1",
            "route": {"source": "mssql", "sink": "clickhouse", "strategy": "incremental_merge"},
            "passed": True,
            "blockers": [],
            "warnings": [],
            "change": {"kind": "add_column", "source_table": "dbo.orders", "source_column": "status"},
            "plan": {
                "compatibility_level": "compatible",
                "target_ddl_preview": "ALTER TABLE analytics.orders ADD COLUMN status Nullable(String)",
                "ddl_dry_run_passed": True,
                "type_widening_safe": True,
                "offset_schema_ordering_safe": True,
                "backfill_required": False,
            },
        },
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "ops",
                "route-schema-evolution",
                "--source",
                "mssql",
                "--sink",
                "clickhouse",
                "--strategy",
                "incremental_merge",
                "--schema-evolution-json",
                str(schema_evolution),
                "--output-dir",
                str(tmp_path / "route-schema"),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["passed"] is True
    assert payload["apply_decision"]["mode"] == "auto_apply"
    assert Path(payload["json_path"]).exists()
