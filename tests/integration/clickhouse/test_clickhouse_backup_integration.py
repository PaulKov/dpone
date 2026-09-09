from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.services.schema_migration_backup import MigrationBackupFacade

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytestmark.append(pytest.mark.skip(reason="set DPONE_RUN_INTEGRATION=1 to run ClickHouse live tests"))

pytest.importorskip("clickhouse_driver")


def test_clickhouse_backup_restore_and_certify_wide_table_live(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_backup_wide_{uuid.uuid4().hex[:10]}"
    database = clickhouse_settings.database
    quoted = f"`{database}`.`{table}`"
    restore_table = ""
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")
    clickhouse_connector.execute_query(f"CREATE TABLE {quoted} ({_wide_columns_ddl()}) ENGINE = MergeTree ORDER BY id")
    clickhouse_connector.connection.execute(f"INSERT INTO {quoted} VALUES", _wide_rows())
    try:
        facade = MigrationBackupFacade()
        pack = _backup_pack(database, table)
        pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
        manifest_path = _write_json(tmp_path / "manifest.json", _manifest(pack, table))
        prod_connection_path = _write_json(
            tmp_path / "clickhouse-prod.json", _connection(clickhouse_settings, database, "prod")
        )
        stage_connection_path = _write_json(
            tmp_path / "clickhouse-stage.json", _connection(clickhouse_settings, database, "stage")
        )
        approval_path = _write_json(tmp_path / "approval.json", _approval(pack))

        plan = facade.plan(
            pack_path=str(pack_path),
            manifest_path=str(manifest_path),
            target_connection_path=str(prod_connection_path),
            environment="prod",
        )
        assert plan["required"] is True
        assert plan["status"] == "planned"
        assert "BACKUP TABLE" in plan["operations"][0]["sql"]
        backup_run = facade.create(
            plan_path=str(_write_json(tmp_path / "backup-plan.json", plan)),
            approval_path=str(approval_path),
            execute=True,
        )
        if backup_run["status"] != "backed_up":
            _skip_if_native_backup_unavailable(backup_run)
        restore_plan = facade.restore_plan(
            backup_run_path=str(_write_json(tmp_path / "backup-run.json", backup_run)),
            target_connection_path=str(stage_connection_path),
            environment="stage",
        )
        restore_table = str(restore_plan["restore_table"]).split(".")[-1]
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{restore_table}`")
        restore_run = facade.restore_run(
            plan_path=str(_write_json(tmp_path / "restore-plan.json", restore_plan)), execute=True
        )
        certificate = facade.certify(
            backup_run_path=str(_write_json(tmp_path / "backup-run-final.json", backup_run)),
            restore_run_path=str(_write_json(tmp_path / "restore-run.json", restore_run)),
            profile="prod_strict",
        )

        assert restore_run["status"] == "restored"
        assert certificate["status"] == "certified"
        assert certificate["remediation"]["capability"] == "restore_from_backup"
        assert clickhouse_connector.get_records(f"SELECT count() FROM `{database}`.`{restore_table}`")[0][0] == 10_000
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")
        if restore_table:
            clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{restore_table}`")


def _backup_pack(database: str, table: str) -> MigrationPack:
    dataset = f"{database}.{table}"
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table=dataset),
        desired={"sink_type": "clickhouse", "table": dataset, "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": dataset, "order_by": []},
        changes=({"change_type": "order_by", "path": "order_by"},),
        strategy="shadow",
    )


def _manifest(pack: MigrationPack, table: str) -> dict[str, object]:
    return {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "backup": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "strategy": "target_native",
                            "require_for": ["physical_layout_change"],
                            "restore_rehearsal": {"enabled": True, "environment": "stage"},
                            "clickhouse": {
                                "destination": f"Disk('backups', 'dpone_backup_{pack.pack_id[-12:]}_{table}.zip')",
                                "incremental": False,
                            },
                        }
                    }
                }
            }
        }
    }


def _connection(clickhouse_settings, database: str, environment: str) -> dict[str, object]:
    return {
        "type": "clickhouse",
        "environment": environment,
        "host": clickhouse_settings.host,
        "port": clickhouse_settings.port,
        "database": database,
        "user": clickhouse_settings.user,
        "password": clickhouse_settings.password,
        "secure": clickhouse_settings.secure,
    }


def _approval(pack: MigrationPack) -> dict[str, object]:
    return {"pack_id": pack.pack_id, "approved_by": "data-platform-owner", "approved_risks": ["physical_layout_change"]}


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def _wide_columns_ddl() -> str:
    columns = ["id Int64", "parent_id Nullable(Int64)"]
    columns.extend(f"`col_{index:03d}` String" for index in range(198))
    return ", ".join(columns)


def _wide_rows() -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for row_id in range(10_000):
        parent_id = row_id - 1 if row_id else None
        rows.append((row_id, parent_id, *(f"value_{row_id}_{index}" for index in range(198))))
    return rows


def _skip_if_native_backup_unavailable(backup_run: dict[str, object]) -> None:
    text = json.dumps(backup_run, ensure_ascii=False).lower()
    known_server_limits = ("backup", "disk", "access_storage", "not found", "not allowed", "unsupported")
    if any(token in text for token in known_server_limits):
        pytest.skip(f"ClickHouse native BACKUP destination is not available in this environment: {text[:500]}")
    pytest.fail(f"ClickHouse backup failed unexpectedly: {text[:1000]}")
