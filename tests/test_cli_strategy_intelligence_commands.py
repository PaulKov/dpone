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


def _patch_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _manifest(path: Path, *, source: str = "postgres", sink: str = "mssql") -> None:
    path.write_text(
        f"""
source:
  type: {source}
  connection_id: src
  connection_type: env
  table: {{schema: public, name: orders}}
  options:
    partition_column: business_date
sink:
  type: {sink}
  connection_id: dst
  connection_type: env
  table: {{schema: landing, name: orders}}
  strategy:
    mode: auto
    unique_key: [order_id]
""".strip()
        + "\n",
        encoding="utf-8",
    )


def test_strategy_advise_cli_returns_explainable_json(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    manifest = tmp_path / "orders.yaml"
    _manifest(manifest)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "strategy",
                "advise",
                str(manifest),
                "--estimated-rows",
                "50000000",
                "--changed-percent",
                "0.2",
                "--delete-percent",
                "0.05",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["decision"]["strategy_mode"] == "partition_replace"
    assert payload["decision"]["native_fast_path"] == "postgres_copy_to_mssql_bcp"
    assert payload["repair_commands"][0].startswith("dpone resync")


def test_strategy_advise_compiles_flow_authoring_before_reading_route(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    manifest = tmp_path / "marketing_wau.yaml"
    manifest.write_text(
        f"""
kind: dpone.flow.v1
authoring:
  mode: flow
  source: {manifest.name}
metadata:
  id: marketing_wau
  domain: marketing
processes:
  - name: marketing_wau
    source:
      type: clickhouse
      connection_ref: clickhouse_marketing
      table: {{schema: marketing_datamarts, name: wau_for_da}}
    sink:
      type: mssql
      connection_ref: mssql_marketing
      table: {{schema: ch, name: marketing__wau_for_da}}
      strategy: {{mode: full_refresh}}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["strategy", "advise", str(manifest), "--format", "json"])

    assert exc.value.code == 0
    decision = json.loads(capsys.readouterr().out)["decision"]
    assert decision["source_type"] == "clickhouse"
    assert decision["sink_type"] == "mssql"
    assert decision["strategy_mode"] == "full_refresh"


def test_perf_advise_includes_strategy_intelligence_recommendations(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    manifest = tmp_path / "orders.yaml"
    _manifest(manifest)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["perf", "advise", str(manifest), "--format", "json"])

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert "strategy_intelligence" in payload
    assert payload["strategy_intelligence"]["decision"]["native_fast_path"] == "postgres_copy_to_mssql_bcp"
