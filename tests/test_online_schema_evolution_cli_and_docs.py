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


def _write_columns(path: Path, columns: list[dict[str, object]]) -> Path:
    path.write_text(json.dumps(columns), encoding="utf-8")
    return path


def test_schema_plan_cli_renders_online_governance_metadata(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    source = _write_columns(tmp_path / "source.json", [{"name": "id", "dtype": "bigint", "nullable": False}])
    target = _write_columns(tmp_path / "target.json", [{"name": "id", "dtype": "int", "nullable": False}])

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "schema",
                "plan",
                "--source",
                str(source),
                "--target",
                str(target),
                "--table",
                "public.orders",
                "--dialect",
                "postgres",
                "--ddl-mode",
                "online",
                "--lock-timeout-seconds",
                "5",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["online_schema_evolution"]["online_eligible"] is False
    assert payload["online_schema_evolution"]["actions"][0]["risk_level"] == "blocking"
    assert payload["online_schema_evolution"]["blockers"] == ["schema_evolution.blocking:type_widen:id"]


def test_manifest_schemas_and_docs_expose_online_schema_evolution() -> None:
    config_schema = json.loads(Path("src/dpone/schema/etl-config.schema.json").read_text(encoding="utf-8"))
    batch_schema = json.loads(Path("src/dpone/schema/etl-batch-manifest.schema.json").read_text(encoding="utf-8"))
    docs = Path("docs/online-schema-evolution.md").read_text(encoding="utf-8")
    mkdocs = Path("mkdocs.yml").read_text(encoding="utf-8")

    for schema in (config_schema, batch_schema):
        sink_options = (
            schema["properties"]["sink"]["properties"]["options"]["properties"]
            if "sink" in schema.get("properties", {})
            else schema["definitions"]["process_fragment"]["properties"]["sink"]["properties"]["options"]["properties"]
        )
        evolution = sink_options["schema_evolution"]["properties"]
        assert evolution["ddl_mode"]["enum"] == ["online", "safe_window", "plan_only", "manual_approval"]
        assert evolution["columns"]["enum"] == ["evolve", "freeze", "ignore", "quarantine"]
        assert evolution["data_type"]["enum"] == ["widen", "variant_column", "freeze", "quarantine"]

    assert "online schema evolution" in docs.lower()
    assert "expand-contract" in docs
    assert "online-schema-evolution.md" in mkdocs
