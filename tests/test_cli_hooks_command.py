from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.commands.registry import get_commands


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_cli(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    settings = SimpleNamespace(repo_root=tmp_path, project_dir=tmp_path, manifest_dir=tmp_path)
    monkeypatch.setattr(
        cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger, settings=settings))
    )


def test_hooks_command_group_is_registered() -> None:
    assert "hooks" in {command.name for command in get_commands()}


def test_hooks_execute_dry_run_outputs_selected_hook(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path) -> None:
    _patch_cli(monkeypatch, tmp_path)
    manifest = tmp_path / "orders.yaml"
    manifest.write_text(
        """
name: orders
source:
  type: mssql
  connection_id: source
  table:
    schema: dbo
    name: orders
  options:
    hooks:
      pre_hook:
        - id: refresh_orders
          kind: source_refresh
          type: sql
          sql: "EXEC [dbo].[p_refresh_orders]"
          mutates_source: true
sink:
  type: clickhouse
  connection_id: sink
  table:
    schema: raw
    name: orders
""",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "hooks",
                "execute",
                str(manifest),
                "--phase",
                "pre_hook",
                "--hook-id",
                "refresh_orders",
                "--dry-run",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["phase"] == "pre_hook"
    assert payload["status"] == "planned"
    assert payload["steps"][0]["id"] == "refresh_orders"
    assert payload["steps"][0]["kind"] == "source_refresh"
    assert "p_refresh_orders" not in json.dumps(payload)


def test_hooks_execute_can_write_json_report_to_file(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path) -> None:
    _patch_cli(monkeypatch, tmp_path)
    manifest = tmp_path / "orders.yaml"
    output = tmp_path / "hook-report.json"
    manifest.write_text(
        """
name: orders
source:
  type: mssql
  connection_id: source
  table:
    schema: dbo
    name: orders
  options:
    hooks:
      pre_hook:
        - id: refresh_orders
          kind: source_refresh
          type: sql
          sql: "EXEC [dbo].[p_refresh_orders]"
          mutates_source: true
sink:
  type: clickhouse
  connection_id: sink
  table:
    schema: raw
    name: orders
""",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "hooks",
                "execute",
                str(manifest),
                "--phase",
                "pre_hook",
                "--dry-run",
                "--format",
                "json",
                "--output",
                str(output),
            ]
        )

    assert exc.value.code == 0
    assert capsys.readouterr().out == ""
    payload = json.loads(output.read_text(encoding="utf-8"))
    assert payload["status"] == "planned"


def test_hooks_execute_dry_run_resolves_sql_file(monkeypatch: pytest.MonkeyPatch, capsys, tmp_path: Path) -> None:
    _patch_cli(monkeypatch, tmp_path)
    manifest_dir = tmp_path / "manifests"
    sql_dir = tmp_path / "sql"
    manifest_dir.mkdir()
    sql_dir.mkdir()
    (sql_dir / "refresh_orders.sql").write_text("EXEC [dbo].[p_refresh_orders]\n", encoding="utf-8")
    manifest = manifest_dir / "orders.yaml"
    manifest.write_text(
        """
name: orders
source:
  type: mssql
  connection_id: source
  table:
    schema: dbo
    name: orders
  options:
    hooks:
      pre_hook:
        - id: refresh_orders
          kind: source_refresh
          type: sql
          sql_file: ../sql/refresh_orders.sql
          mutates_source: true
sink:
  type: clickhouse
  connection_id: sink
  table:
    schema: raw
    name: orders
""",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "hooks",
                "execute",
                str(manifest),
                "--phase",
                "pre_hook",
                "--hook-id",
                "refresh_orders",
                "--dry-run",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "planned"
    assert payload["steps"][0]["id"] == "refresh_orders"
    assert "p_refresh_orders" not in json.dumps(payload)


def test_hooks_execute_unknown_hook_id_fails_loudly(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    _patch_cli(monkeypatch, tmp_path)
    manifest = tmp_path / "orders.yaml"
    manifest.write_text(
        """
name: orders
source:
  type: mssql
  connection_id: source
  table:
    schema: dbo
    name: orders
  options:
    hooks:
      pre_hook:
        - id: refresh_orders
          kind: source_refresh
          type: sql
          sql: "EXEC [dbo].[p_refresh_orders]"
          mutates_source: true
sink:
  type: clickhouse
  connection_id: sink
  table:
    schema: raw
    name: orders
""",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "hooks",
                "execute",
                str(manifest),
                "--phase",
                "pre_hook",
                "--hook-id",
                "missing",
                "--dry-run",
            ]
        )

    assert exc.value.code == 2
