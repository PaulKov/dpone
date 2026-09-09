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


def test_schema_migration_plan_strategy_shadow_outputs_phases(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path)
    actual = _write_actual(tmp_path, order_by=[])

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "schema",
                "migration",
                "plan",
                "--manifest",
                str(manifest),
                "--actual",
                str(actual),
                "--strategy",
                "shadow",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["strategy"] == "shadow"
    assert payload["blockers"] == []
    assert [phase["name"] for phase in payload["phases"]] == [
        "prepare",
        "create_shadow",
        "backfill",
        "validate",
        "cutover",
        "contract",
    ]
    assert any("EXCHANGE TABLES" in op["sql"] for phase in payload["phases"] for op in phase.get("operations", []))


def test_schema_migration_plan_reads_shadow_strategy_from_manifest(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path, migration_strategy="shadow")
    actual = _write_actual(tmp_path, order_by=[])

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "schema",
                "migration",
                "plan",
                "--manifest",
                str(manifest),
                "--actual",
                str(actual),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["strategy"] == "shadow"
    assert payload["phases"]


def test_schema_migration_plan_strategy_online_safe_emits_safe_setting_ddl(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path)
    actual = _write_actual(tmp_path, order_by=["id"], table_settings={"min_rows_for_wide_part": 0})

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "schema",
                "migration",
                "plan",
                "--manifest",
                str(manifest),
                "--actual",
                str(actual),
                "--strategy",
                "online_safe",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["strategy"] == "online_safe"
    assert payload["blockers"] == []
    assert payload["ddl"] == ["ALTER TABLE `landing`.`orders` MODIFY SETTING min_rows_for_wide_part = 8192"]


def test_schema_migration_apply_phase_enforces_phase_order_and_writes_ledger(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path)
    actual = _write_actual(tmp_path, order_by=[])
    pack_path = tmp_path / "shadow-pack.json"
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
                str(actual),
                "--strategy",
                "shadow",
                "--output",
                str(pack_path),
                "--format",
                "json",
            ]
        )
    assert plan_exit.value.code == 0
    capsys.readouterr()

    with pytest.raises(SystemExit) as blocked_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "apply",
                "--plan",
                str(pack_path),
                "--phase",
                "backfill",
                "--ledger",
                str(ledger),
                "--format",
                "json",
            ]
        )

    assert blocked_exit.value.code == 2
    blocked = json.loads(capsys.readouterr().out)
    assert "migration.phase_order_violation:prepare" in blocked["blockers"]

    with pytest.raises(SystemExit) as prepare_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "apply",
                "--plan",
                str(pack_path),
                "--phase",
                "prepare",
                "--actual",
                str(actual),
                "--ledger",
                str(ledger),
                "--format",
                "json",
            ]
        )

    assert prepare_exit.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "phase_applied"
    assert payload["phase"] == "prepare"
    saved = json.loads(ledger.read_text(encoding="utf-8"))
    assert saved["records"][0]["phase"] == "prepare"
    assert saved["records"][0]["status"] == "phase_applied"


def test_schema_migration_apply_phase_replay_is_idempotent_noop(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path)
    actual = _write_actual(tmp_path, order_by=[])
    pack_path = tmp_path / "shadow-pack.json"
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
                str(actual),
                "--strategy",
                "shadow",
                "--output",
                str(pack_path),
                "--format",
                "json",
            ]
        )
    assert plan_exit.value.code == 0
    capsys.readouterr()

    with pytest.raises(SystemExit) as first_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "apply",
                "--plan",
                str(pack_path),
                "--phase",
                "prepare",
                "--ledger",
                str(ledger),
                "--format",
                "json",
            ]
        )
    assert first_exit.value.code == 0
    capsys.readouterr()
    before = json.loads(ledger.read_text(encoding="utf-8"))

    with pytest.raises(SystemExit) as replay_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "apply",
                "--plan",
                str(pack_path),
                "--phase",
                "prepare",
                "--ledger",
                str(ledger),
                "--format",
                "json",
            ]
        )

    assert replay_exit.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "phase_already_applied"
    assert payload["phase"] == "prepare"
    assert json.loads(ledger.read_text(encoding="utf-8")) == before


def test_schema_migration_apply_execute_requires_target_connection(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path)
    actual = _write_actual(tmp_path, order_by=[])
    pack_path = tmp_path / "shadow-pack.json"

    with pytest.raises(SystemExit) as plan_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "plan",
                "--manifest",
                str(manifest),
                "--actual",
                str(actual),
                "--strategy",
                "shadow",
                "--output",
                str(pack_path),
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
                str(pack_path),
                "--phase",
                "create_shadow",
                "--execute",
                "--format",
                "json",
            ]
        )

    assert apply_exit.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "blocked"
    assert "migration.target_connection_required" in payload["blockers"]


def test_schema_migration_rollback_shadow_pack_blocks_after_contract(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
) -> None:
    _patch_cli(monkeypatch)
    manifest = _write_manifest(tmp_path)
    actual = _write_actual(tmp_path, order_by=[])
    pack_path = tmp_path / "shadow-pack.json"
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
                str(actual),
                "--strategy",
                "shadow",
                "--output",
                str(pack_path),
                "--format",
                "json",
            ]
        )
    assert plan_exit.value.code == 0
    capsys.readouterr()
    _append_phase_ledger(ledger, json.loads(pack_path.read_text(encoding="utf-8"))["pack_id"], "contract")

    with pytest.raises(SystemExit) as rollback_exit:
        cli_main.main(
            [
                "schema",
                "migration",
                "rollback",
                "--plan",
                str(pack_path),
                "--ledger",
                str(ledger),
                "--format",
                "json",
            ]
        )

    assert rollback_exit.value.code == 2
    payload = json.loads(capsys.readouterr().out)
    assert "migration.rollback_not_supported_after_contract" in payload["blockers"]


def _patch_cli(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def _write_manifest(tmp_path: Path, *, migration_strategy: str | None = None) -> Path:
    physical_design: dict[str, object] = {
        "storage": {
            "clickhouse": {
                "engine": "MergeTree",
                "order_by": ["id"],
                "table_settings": {"min_rows_for_wide_part": 8192},
            }
        }
    }
    if migration_strategy:
        physical_design["migration"] = {"strategy": migration_strategy}
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "source": {"options": {"columns": [{"name": "id", "type": "bigint"}]}},
                "sink": {
                    "type": "clickhouse",
                    "table": {"schema": "landing", "name": "orders"},
                    "options": {"physical_design": physical_design},
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return manifest


def _write_actual(
    tmp_path: Path,
    *,
    order_by: list[str],
    table_settings: dict[str, int] | None = None,
) -> Path:
    actual = tmp_path / "actual.json"
    actual.write_text(
        json.dumps(
            {
                "sink_type": "clickhouse",
                "table": "landing.orders",
                "engine": "MergeTree",
                "order_by": order_by,
                "columns": {"id": {"type": "Int64", "nullable": False}},
                "table_settings": table_settings or {"min_rows_for_wide_part": 8192},
            }
        ),
        encoding="utf-8",
    )
    return actual


def _append_phase_ledger(ledger: Path, pack_id: str, phase: str) -> None:
    ledger.write_text(
        json.dumps(
            {
                "schema_version": "dpone.schema_migration_ledger.v1",
                "records": [
                    {
                        "schema_version": "dpone.schema_migration_ledger_record.v1",
                        "pack_id": pack_id,
                        "status": "phase_applied",
                        "phase": phase,
                        "target": {"sink_type": "clickhouse", "table": "landing.orders"},
                        "warnings": [],
                        "blockers": [],
                        "applied_at": "2026-06-20T00:00:00+00:00",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
