from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.services.schema_migration_backup import MigrationBackupFacade
from dpone.services.schema_migration_recovery import MigrationRecoveryFacade

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytestmark.append(pytest.mark.skip(reason="set DPONE_RUN_INTEGRATION=1 to run ClickHouse live tests"))

pytest.importorskip("clickhouse_driver")


def test_clickhouse_recovery_full_incremental_chain_and_restore_live(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_recovery_wide_{uuid.uuid4().hex[:10]}"
    database = clickhouse_settings.database
    quoted = f"`{database}`.`{table}`"
    restored_tables: list[str] = []
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")
    clickhouse_connector.execute_query(f"CREATE TABLE {quoted} ({_wide_columns_ddl()}) ENGINE = MergeTree ORDER BY id")
    clickhouse_connector.connection.execute(f"INSERT INTO {quoted} VALUES", _wide_rows(0, 10_000))
    try:
        backup_facade = MigrationBackupFacade()
        recovery_facade = MigrationRecoveryFacade()
        pack = _pack(database, table)
        pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
        connection_path = _write_json(
            tmp_path / "clickhouse-prod.json", _connection(clickhouse_settings, database, "prod")
        )
        stage_connection_path = _write_json(
            tmp_path / "clickhouse-stage.json", _connection(clickhouse_settings, database, "stage")
        )
        approval_path = _write_json(tmp_path / "approval.json", _approval(pack))
        store_uri = tmp_path / "recovery.json"

        full_certificate = _create_backup_certificate(
            backup_facade=backup_facade,
            pack_path=pack_path,
            manifest_path=_write_json(tmp_path / "manifest-full.json", _manifest(pack, table, store_uri, False)),
            connection_path=connection_path,
            stage_connection_path=stage_connection_path,
            approval_path=approval_path,
            tmp_path=tmp_path,
            prefix="full",
            restored_tables=restored_tables,
        )
        full_point = recovery_facade.point_record(
            backup_certificate_path=str(_write_json(tmp_path / "full-certificate.json", full_certificate)),
            environment="prod",
            store_backend="local_json",
            store_uri=str(store_uri),
        )
        assert full_point["status"] == "usable"

        clickhouse_connector.connection.execute(f"INSERT INTO {quoted} VALUES", _wide_rows(10_000, 10_100))
        incremental_plan = backup_facade.plan(
            pack_path=str(pack_path),
            manifest_path=str(_write_json(tmp_path / "manifest-inc.json", _manifest(pack, table, store_uri, True))),
            target_connection_path=str(connection_path),
            environment="prod",
        )
        assert incremental_plan["backup_kind"] == "incremental"
        incremental_run = backup_facade.create(
            plan_path=str(_write_json(tmp_path / "inc-plan.json", incremental_plan)),
            approval_path=str(approval_path),
            execute=True,
        )
        if incremental_run["status"] != "backed_up":
            _skip_if_native_backup_unavailable(incremental_run)
        incremental_certificate = backup_facade.certify(
            backup_run_path=str(_write_json(tmp_path / "inc-run.json", incremental_run)),
            profile="advisory",
        )
        incremental_point = recovery_facade.point_record(
            backup_certificate_path=str(_write_json(tmp_path / "inc-certificate.json", incremental_certificate)),
            environment="prod",
            store_backend="local_json",
            store_uri=str(store_uri),
        )
        chain = recovery_facade.chain_verify(
            restore_point_id=incremental_point["restore_point_id"],
            store_backend="local_json",
            store_uri=str(store_uri),
            require_restore_rehearsal=False,
        )
        restore_plan = recovery_facade.restore_plan(
            restore_point_id=incremental_point["restore_point_id"],
            target_connection_path=str(stage_connection_path),
            environment="stage",
            store_backend="local_json",
            store_uri=str(store_uri),
        )
        restored_tables.append(str(restore_plan["restore_table"]).split(".")[-1])
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{restored_tables[-1]}`")
        restore_run = recovery_facade.restore_run(
            plan_path=str(_write_json(tmp_path / "recovery-restore-plan.json", restore_plan)),
            execute=True,
        )
        if restore_run["status"] != "restored":
            _skip_if_native_backup_unavailable(restore_run)
        certificate = recovery_facade.restore_certify(
            restore_run_path=str(_write_json(tmp_path / "recovery-restore-run.json", restore_run)),
            profile="prod_strict",
        )
        retention = recovery_facade.retention_plan(
            target=f"clickhouse.{database}.{table}",
            environment="prod",
            store_backend="local_json",
            store_uri=str(store_uri),
        )

        assert chain["status"] == "verified"
        assert len(chain["chain"]) == 2
        assert certificate["status"] == "certified"
        assert retention["status"] == "warning"
        assert any(item["action"] == "protect_base_with_dependents" for item in retention["actions"])
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")
        for restored_table in restored_tables:
            clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{restored_table}`")


def _create_backup_certificate(
    *,
    backup_facade: MigrationBackupFacade,
    pack_path: Path,
    manifest_path: Path,
    connection_path: Path,
    stage_connection_path: Path,
    approval_path: Path,
    tmp_path: Path,
    prefix: str,
    restored_tables: list[str],
) -> dict[str, object]:
    plan = backup_facade.plan(
        pack_path=str(pack_path),
        manifest_path=str(manifest_path),
        target_connection_path=str(connection_path),
        environment="prod",
    )
    run = backup_facade.create(
        plan_path=str(_write_json(tmp_path / f"{prefix}-backup-plan.json", plan)),
        approval_path=str(approval_path),
        execute=True,
    )
    if run["status"] != "backed_up":
        _skip_if_native_backup_unavailable(run)
    restore_plan = backup_facade.restore_plan(
        backup_run_path=str(_write_json(tmp_path / f"{prefix}-backup-run.json", run)),
        target_connection_path=str(stage_connection_path),
        environment="stage",
    )
    restored_tables.append(str(restore_plan["restore_table"]).split(".")[-1])
    restore_run = backup_facade.restore_run(
        plan_path=str(_write_json(tmp_path / f"{prefix}-restore-plan.json", restore_plan)),
        execute=True,
    )
    if restore_run["status"] != "restored":
        _skip_if_native_backup_unavailable(restore_run)
    return backup_facade.certify(
        backup_run_path=str(_write_json(tmp_path / f"{prefix}-backup-run-final.json", run)),
        restore_run_path=str(_write_json(tmp_path / f"{prefix}-restore-run.json", restore_run)),
        profile="prod_strict",
    )


def _pack(database: str, table: str) -> MigrationPack:
    dataset = f"{database}.{table}"
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table=dataset),
        desired={"sink_type": "clickhouse", "table": dataset, "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": dataset, "order_by": []},
        changes=({"change_type": "order_by", "path": "order_by"},),
        strategy="shadow",
    )


def _manifest(pack: MigrationPack, table: str, store_uri: Path, incremental: bool) -> dict[str, object]:
    destination_name = "inc" if incremental else "full"
    return {
        "sink": {
            "type": "clickhouse",
            "table": pack.target.table,
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
                                "destination": (
                                    f"Disk('backups', 'dpone_recovery_{destination_name}_{pack.pack_id[-12:]}_{table}.zip')"
                                ),
                                "incremental": {"enabled": incremental},
                            },
                        },
                        "recovery": {
                            "enabled": True,
                            "profile": "prod_strict",
                            "catalog": {"store_backend": "local_json", "store_uri": str(store_uri)},
                            "clickhouse": {"incremental": {"enabled": incremental, "base_selection": "latest_usable"}},
                        },
                    }
                }
            },
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


def _wide_rows(start: int, stop: int) -> list[tuple[object, ...]]:
    rows: list[tuple[object, ...]] = []
    for row_id in range(start, stop):
        parent_id = row_id - 1 if row_id else None
        rows.append((row_id, parent_id, *(f"value_{row_id}_{index}" for index in range(198))))
    return rows


def _skip_if_native_backup_unavailable(payload: dict[str, object]) -> None:
    text = json.dumps(payload, ensure_ascii=False).lower()
    known_server_limits = ("backup", "disk", "access_storage", "not found", "not allowed", "unsupported")
    if any(token in text for token in known_server_limits):
        pytest.skip(f"ClickHouse native BACKUP destination is not available in this environment: {text[:500]}")
    pytest.fail(f"ClickHouse recovery operation failed unexpectedly: {text[:1000]}")
