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


class _CapturingLogger(_LoggerStub):
    def __init__(self) -> None:
        self.errors: list[str] = []

    def error(self, message: str) -> None:
        self.errors.append(message)


def test_schema_migration_group_help_surfaces(monkeypatch: pytest.MonkeyPatch) -> None:
    _patch_cli(monkeypatch)

    with pytest.raises(SystemExit) as group_exit:
        cli_main.main(["schema", "migration", "--help"])
    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(["schema", "migration", "plan", "--help"])

    assert group_exit.value.code == 0
    assert plan_exit.value.code == 0


def test_schema_migration_plan_outputs_stable_pack(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["schema", "migration", "plan", "--manifest", str(manifest), "--format", "json"])

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.schema_migration_pack.v1"
    assert payload["command"] == "plan"
    assert payload["target"]["sink_type"] == "clickhouse"
    assert payload["target"]["table"] == "landing.orders"
    assert payload["pack_id"].startswith("sha256:")
    assert payload["desired_fingerprint"].startswith("sha256:")
    assert payload["actual_fingerprint"] is None
    assert payload["blockers"] == []
    assert payload["ddl"][0].startswith("CREATE TABLE")


def test_schema_migration_baseline_writes_artifact_ledger(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path)
    actual = _write_actual(tmp_path, table_settings={"min_rows_for_wide_part": 8192})
    ledger = tmp_path / "ledger.json"

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "schema",
                "migration",
                "baseline",
                "--manifest",
                str(manifest),
                "--actual",
                str(actual),
                "--ledger",
                str(ledger),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.schema_migration_ledger_record.v1"
    assert payload["status"] == "baseline"
    assert payload["actual_fingerprint"].startswith("sha256:")
    saved = json.loads(ledger.read_text(encoding="utf-8"))
    assert saved["records"][0]["status"] == "baseline"


def test_schema_migration_apply_refuses_stale_actual_fingerprint(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path)
    planned_actual = _write_actual(tmp_path, table_settings={"min_rows_for_wide_part": 8192})
    stale_actual = _write_actual(tmp_path, "stale_actual.json", table_settings={"min_rows_for_wide_part": 0})
    plan_path = tmp_path / "pack.json"
    ledger = tmp_path / "ledger.json"

    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "plan",
                "--manifest",
                str(manifest),
                "--actual",
                str(planned_actual),
                "--output",
                str(plan_path),
                "--format",
                "json",
            ]
        )
    assert plan_exit.value.code == 0
    capsys.readouterr()

    with pytest.raises(SystemExit) as apply_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "apply",
                "--plan",
                str(plan_path),
                "--actual",
                str(stale_actual),
                "--ledger",
                str(ledger),
                "--format",
                "json",
            ]
        )

    assert apply_exit.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "migration.actual_fingerprint_mismatch" in payload["blockers"]
    assert not ledger.exists()


def test_schema_migration_history_renders_artifact_ledger(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path)
    actual = _write_actual(tmp_path)
    ledger = tmp_path / "ledger.json"
    _run_baseline(manifest, actual, ledger)

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["schema", "migration", "history", "--ledger", str(ledger), "--format", "table"])

    assert exc.value.code == 0
    output = capsys.readouterr().out
    assert "schema migration history" in output
    assert "baseline" in output
    assert "landing.orders" in output


def test_schema_migration_rollback_blocks_non_reversible_pack(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path)
    plan_path = tmp_path / "pack.json"

    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "plan",
                "--manifest",
                str(manifest),
                "--output",
                str(plan_path),
                "--format",
                "json",
            ]
        )
    assert plan_exit.value.code == 0
    capsys.readouterr()

    with pytest.raises(SystemExit) as rollback_exit:
        cli_main.main(["schema", "migration", "rollback", "--plan", str(plan_path), "--format", "json"])

    assert rollback_exit.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "migration.rollback_not_supported" in payload["blockers"]


def test_schema_migration_apply_reports_invalid_pack_as_configuration_error(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    logger = _CapturingLogger()
    monkeypatch.setattr(cli_main, "setup_logging", lambda: logger)
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))
    bad_plan = tmp_path / "bad-plan.json"
    bad_plan.write_text("[]", encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["schema", "migration", "apply", "--plan", str(bad_plan), "--format", "json"])

    assert exc.value.code == 2
    assert logger.errors
    assert "must contain a JSON object" in logger.errors[0]


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_manifest(tmp_path: Path) -> Path:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "source": {"options": {"columns": [{"name": "id", "type": "bigint"}]}},
                "sink": {
                    "type": "clickhouse",
                    "table": {"schema": "landing", "name": "orders"},
                    "options": {
                        "physical_design": {
                            "storage": {
                                "clickhouse": {
                                    "order_by": ["id"],
                                    "table_settings": {"min_rows_for_wide_part": 8192},
                                }
                            }
                        }
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return manifest


def _write_actual(
    tmp_path: Path,
    name: str = "actual.json",
    *,
    table_settings: dict[str, object] | None = None,
) -> Path:
    actual = tmp_path / name
    actual.write_text(
        json.dumps(
            {
                "sink_type": "clickhouse",
                "table": "landing.orders",
                "engine": "MergeTree",
                "order_by": ["id"],
                "columns": {"id": {"type": "Nullable(Int64)", "nullable": True}},
                "table_settings": table_settings or {"min_rows_for_wide_part": 8192},
            }
        ),
        encoding="utf-8",
    )
    return actual


def _run_baseline(manifest: Path, actual: Path, ledger: Path) -> None:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "schema",
                "migration",
                "baseline",
                "--manifest",
                str(manifest),
                "--actual",
                str(actual),
                "--ledger",
                str(ledger),
                "--format",
                "json",
            ]
        )
    assert exc.value.code == 0
