from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.readiness.migration_control import MigrationPack, MigrationTarget


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def test_rehearse_fixture_help_lists_nested_commands(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)
    with pytest.raises(SystemExit) as exc_info:
        cli_main.main(["schema", "migration", "rehearse", "fixture", "--help"])

    assert exc_info.value.code == 0
    assert "{plan,build,profile}" in capsys.readouterr().out


def test_rehearse_fixture_plan_build_profile_console_and_output(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_cli(monkeypatch)
    pack_path = tmp_path / "pack.json"
    manifest_path = tmp_path / "manifest.yaml"
    plan_path = tmp_path / "fixture-plan.json"
    build_path = tmp_path / "fixture-build.json"
    profile_path = tmp_path / "profile-after.json"
    connection_path = tmp_path / "clickhouse-stage.json"
    pack_path.write_text(json.dumps(_pack().to_dict(command="plan")), encoding="utf-8")
    manifest_path.write_text(
        """
sink:
  options:
    physical_design:
      migration:
        rehearsal:
          data_fixture:
            mode: synthetic
            min_rows: 12
            min_columns: 4
            include_edge_cases: true
            preserve_hierarchy: true
            seed: cli-fixture
          quality_profile:
            row_count: true
            typed_hash: true
            duplicate_key: true
            null_key: true
schema:
  columns:
    - name: id
      type: Int64
      key: true
    - name: parent_id
      type: Nullable(Int64)
      parent: id
    - name: amount
      type: Decimal(18, 2)
    - name: comment
      type: Nullable(String)
""",
        encoding="utf-8",
    )
    connection_path.write_text(json.dumps({"type": "clickhouse", "environment": "stage"}), encoding="utf-8")

    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "rehearse",
                "fixture",
                "plan",
                "--pack",
                str(pack_path),
                "--manifest",
                str(manifest_path),
                "--output",
                str(plan_path),
                "--format",
                "json",
            ]
        )

    assert plan_exit.value.code == 0
    assert plan_path.exists()
    assert json.loads(capsys.readouterr().out)["schema_version"] == "dpone.schema_migration_fixture_plan.v1"

    with pytest.raises(SystemExit) as build_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "rehearse",
                "fixture",
                "build",
                "--plan",
                str(plan_path),
                "--target-connection",
                str(connection_path),
                "--output",
                str(build_path),
                "--format",
                "json",
            ]
        )

    assert build_exit.value.code == 0
    build = json.loads(build_path.read_text(encoding="utf-8"))
    assert build["schema_version"] == "dpone.schema_migration_fixture_build.v1"
    assert build["status"] == "dry_run"
    assert build["metrics"]["row_count"] == 12

    with pytest.raises(SystemExit) as profile_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "rehearse",
                "fixture",
                "profile",
                "--fixture-build",
                str(build_path),
                "--target-connection",
                str(connection_path),
                "--stage",
                "after",
                "--output",
                str(profile_path),
                "--format",
                "json",
            ]
        )

    assert profile_exit.value.code == 0
    profile = json.loads(profile_path.read_text(encoding="utf-8"))
    assert profile["schema_version"] == "dpone.schema_migration_data_profile.v1"
    assert profile["stage"] == "after"


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _pack() -> MigrationPack:
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table="analytics.orders"),
        desired={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": "analytics.orders", "order_by": ["id"]},
        changes=({"change_type": "table_setting", "path": "table_settings.index_granularity"},),
        ddl=("ALTER TABLE analytics.orders MODIFY SETTING index_granularity = 8192",),
        strategy="online_safe",
    )
