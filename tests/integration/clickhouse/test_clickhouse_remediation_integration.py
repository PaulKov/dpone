from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest

from dpone.readiness.migration_control import MigrationPack, MigrationTarget
from dpone.services.schema_migration_remediation import MigrationRemediationFacade

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytestmark.append(pytest.mark.skip(reason="set DPONE_RUN_INTEGRATION=1 to run ClickHouse live tests"))

pytest.importorskip("clickhouse_driver")


def test_clickhouse_remediation_executes_table_setting_rollback_live(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_remediation_setting_{uuid.uuid4().hex[:10]}"
    database = clickhouse_settings.database
    quoted = f"`{database}`.`{table}`"
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")
    clickhouse_connector.execute_query(
        f"CREATE TABLE {quoted} (id Int64) ENGINE = MergeTree ORDER BY id SETTINGS min_rows_for_wide_part = 10"
    )
    try:
        pack = _setting_pack(database, table)
        plan = _plan_apply_certify(
            tmp_path,
            pack=pack,
            ledger=_ledger(pack, status="applied"),
            manifest=_manifest(),
            connection=_connection(clickhouse_settings, database),
        )
        assert plan["capability"] == "online_safe"

        create_query = _create_table_query(clickhouse_connector, database, table)
        assert "min_rows_for_wide_part = 10" not in create_query
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {quoted}")


def test_clickhouse_remediation_executes_shadow_exchange_rollback_live(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_remediation_shadow_{uuid.uuid4().hex[:10]}"
    backup = f"__dpone_shadow_{table}_pack"
    database = clickhouse_settings.database
    actual_quoted = f"`{database}`.`{table}`"
    backup_quoted = f"`{database}`.`{backup}`"
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {actual_quoted}")
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {backup_quoted}")
    clickhouse_connector.execute_query(
        f"CREATE TABLE {actual_quoted} (id Int64, value String) ENGINE = MergeTree ORDER BY id"
    )
    clickhouse_connector.execute_query(
        f"CREATE TABLE {backup_quoted} (id Int64, value String) ENGINE = MergeTree ORDER BY tuple()"
    )
    try:
        clickhouse_connector.execute_query(f"INSERT INTO {actual_quoted} VALUES (2, 'after')")
        clickhouse_connector.execute_query(f"INSERT INTO {backup_quoted} VALUES (1, 'before')")
        pack = _shadow_pack(database, table, backup)
        plan = _plan_apply_certify(
            tmp_path,
            pack=pack,
            ledger=_ledger(pack, status="phase_applied", phase="cutover"),
            manifest=_manifest(),
            connection=_connection(clickhouse_settings, database),
        )
        assert plan["capability"] == "shadow_exchange"
        assert clickhouse_connector.get_records(f"SELECT value FROM {actual_quoted} ORDER BY id")[0][0] == "before"
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {actual_quoted}")
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS {backup_quoted}")


def _plan_apply_certify(
    tmp_path: Path,
    *,
    pack: MigrationPack,
    ledger: dict[str, object],
    manifest: dict[str, object],
    connection: dict[str, object],
) -> dict[str, object]:
    facade = MigrationRemediationFacade()
    pack_path = _write_json(tmp_path / "pack.json", pack.to_dict(command="plan"))
    watch_path = _write_json(tmp_path / "watch.json", _watch_certificate(pack))
    ledger_path = _write_json(tmp_path / "ledger.json", ledger)
    manifest_path = _write_json(tmp_path / "manifest.json", manifest)
    connection_path = _write_json(tmp_path / "clickhouse-prod.json", connection)
    approval_path = _write_json(tmp_path / "approval.json", _approval(pack))
    plan = facade.plan(
        pack_path=str(pack_path),
        watch_certificate_path=str(watch_path),
        ledger_path=str(ledger_path),
        manifest_path=str(manifest_path),
        target_connection_path=str(connection_path),
        environment="prod",
    )
    plan_path = _write_json(tmp_path / f"{pack.pack_id[-8:]}-remediation-plan.json", plan)
    run = facade.apply(plan_path=str(plan_path), approval_path=str(approval_path), execute=True)
    certificate = facade.certify(
        run_path=str(_write_json(tmp_path / f"{pack.pack_id[-8:]}-remediation-run.json", run)),
        target_connection_path=str(connection_path),
        profile="prod_strict",
    )
    assert run["status"] == "remediated"
    assert certificate["status"] == "certified"
    return plan


def _setting_pack(database: str, table: str) -> MigrationPack:
    dataset = f"{database}.{table}"
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table=dataset),
        desired={"sink_type": "clickhouse", "table": dataset, "table_settings": {}},
        actual={"sink_type": "clickhouse", "table": dataset, "table_settings": {"min_rows_for_wide_part": 10}},
        changes=({"change_type": "table_setting", "path": "table_settings.min_rows_for_wide_part"},),
        strategy="online_safe",
        rollback={
            "supported": True,
            "ddl": [f"ALTER TABLE `{database}`.`{table}` RESET SETTING min_rows_for_wide_part"],
        },
    )


def _shadow_pack(database: str, table: str, backup: str) -> MigrationPack:
    dataset = f"{database}.{table}"
    backup_dataset = f"{database}.{backup}"
    return MigrationPack.build(
        target=MigrationTarget(sink_type="clickhouse", table=dataset),
        desired={"sink_type": "clickhouse", "table": dataset, "order_by": ["id"]},
        actual={"sink_type": "clickhouse", "table": dataset, "order_by": []},
        changes=({"change_type": "order_by", "path": "order_by"},),
        strategy="shadow",
        rollback={
            "supported": True,
            "supported_until_phase": "contract",
            "ddl": [f"EXCHANGE TABLES `{database}`.`{table}` AND `{database}`.`{backup}`"],
            "shadow_table": backup_dataset,
        },
    )


def _manifest() -> dict[str, object]:
    return {
        "sink": {
            "options": {
                "physical_design": {
                    "migration": {
                        "remediation": {
                            "enabled": True,
                            "mode": "gate",
                            "profile": "prod_strict",
                            "execution": {"require_approval": True, "require_watch_certificate": True},
                            "rollback": {"strategy": "controlled", "allow_after_contract": False},
                            "certify": {"physical_design": True, "canary_queries": True},
                        }
                    }
                }
            }
        }
    }


def _watch_certificate(pack: MigrationPack) -> dict[str, object]:
    return {
        "schema_version": "dpone.schema_migration_watch_certificate.v1",
        "certificate_id": "sha256:" + "9" * 64,
        "pack_id": pack.pack_id,
        "environment": "prod",
        "target": pack.target.to_dict(),
        "status": "blocked",
        "profile": "prod_strict",
        "samples": {"planned": 2, "executed": 2, "passed": 0, "failed": 1},
        "remediation": {"decision": "rollback_required", "commands": []},
        "checks": [],
        "blockers": ["schema_migration_watch.physical_drift"],
        "warnings": [],
        "metrics": {},
    }


def _ledger(pack: MigrationPack, *, status: str, phase: str | None = None) -> dict[str, object]:
    record: dict[str, object] = {
        "schema_version": "dpone.schema_migration_ledger_record.v1",
        "pack_id": pack.pack_id,
        "status": status,
        "target": pack.target.to_dict(),
        "desired_fingerprint": pack.desired_fingerprint,
        "actual_fingerprint": pack.actual_fingerprint,
        "environment": "prod",
        "blockers": [],
        "warnings": [],
    }
    if phase:
        record["phase"] = phase
    return {"schema_version": "dpone.schema_migration_ledger.v1", "records": [record]}


def _approval(pack: MigrationPack) -> dict[str, object]:
    return {"pack_id": pack.pack_id, "approved_by": "data-platform-owner", "approved_risks": ["controlled_rollback"]}


def _connection(clickhouse_settings, database: str) -> dict[str, object]:
    return {
        "type": "clickhouse",
        "environment": "prod",
        "host": clickhouse_settings.host,
        "port": clickhouse_settings.port,
        "database": database,
        "user": clickhouse_settings.user,
        "password": clickhouse_settings.password,
        "secure": clickhouse_settings.secure,
    }


def _create_table_query(clickhouse_connector, database: str, table: str) -> str:
    rows = clickhouse_connector.get_records(
        "SELECT create_table_query FROM system.tables WHERE database = %(database)s AND name = %(table)s",
        {"database": database, "table": table},
    )
    return str(rows[0][0])


def _write_json(path: Path, payload: dict[str, object]) -> Path:
    path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    return path
