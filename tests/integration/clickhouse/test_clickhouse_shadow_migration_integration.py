from __future__ import annotations

import json
import os
import uuid
from pathlib import Path

import pytest
import yaml

from dpone.services.schema_migration import MigrationControlFacade

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

pytest.importorskip("clickhouse_driver")


def test_clickhouse_shadow_migration_pack_sql_cutover_and_rollback_live(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_shadow_{uuid.uuid4().hex[:10]}"
    database = clickhouse_settings.database
    shadow_table = ""
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{table}`")
    clickhouse_connector.execute_query(
        f"CREATE TABLE `{database}`.`{table}` ("
        "id Int64, name String, amount Decimal(18, 2)"
        ") ENGINE = MergeTree ORDER BY tuple()"
    )
    try:
        clickhouse_connector.connection.execute(
            f"INSERT INTO `{database}`.`{table}` (id, name, amount) VALUES",
            [(index, f"name_{index % 17}", f"{index % 1000}.{index % 100:02d}") for index in range(10_000)],
        )
        manifest = _write_manifest(tmp_path, database=database, table=table)
        actual = _write_actual(tmp_path, database=database, table=table)

        pack = MigrationControlFacade().plan(
            manifest_path=str(manifest),
            actual_path=str(actual),
            strategy="shadow",
        )

        assert pack["blockers"] == []
        phase_map = {phase["name"]: phase for phase in pack["phases"]}
        shadow_table = str(pack["shadow"]["shadow_table"]).split(".", 1)[1]
        _execute_operations(clickhouse_connector, phase_map["create_shadow"])
        _execute_operations(clickhouse_connector, phase_map["backfill"])
        _assert_validation_queries_pass(clickhouse_connector, phase_map["validate"])
        _execute_operations(clickhouse_connector, phase_map["cutover"])
        assert _sorting_key(clickhouse_connector, database, table) == "id"

        clickhouse_connector.execute_query(pack["rollback"]["ddl"][0])
        assert _sorting_key(clickhouse_connector, database, table) == ""

        clickhouse_connector.execute_query(pack["rollback"]["ddl"][0])
        _execute_operations(clickhouse_connector, phase_map["contract"])
        assert _table_exists(clickhouse_connector, database, shadow_table) == 0
        assert clickhouse_connector.get_records(f"SELECT count() FROM `{database}`.`{table}`")[0][0] == 10_000
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{table}`")
        if shadow_table:
            clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{shadow_table}`")


def test_clickhouse_shadow_migration_apply_executes_phases_live(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_shadow_exec_{uuid.uuid4().hex[:10]}"
    database = clickhouse_settings.database
    ledger = tmp_path / "ledger.json"
    shadow_table = ""
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{table}`")
    clickhouse_connector.execute_query(
        f"CREATE TABLE `{database}`.`{table}` ("
        "id Int64, name String, amount Decimal(18, 2)"
        ") ENGINE = MergeTree ORDER BY tuple()"
    )
    try:
        clickhouse_connector.connection.execute(
            f"INSERT INTO `{database}`.`{table}` (id, name, amount) VALUES",
            [(index, f"name_{index % 17}", f"{index % 1000}.{index % 100:02d}") for index in range(1_000)],
        )
        manifest = _write_manifest(tmp_path, database=database, table=table)
        actual = _write_actual(tmp_path, database=database, table=table)
        pack_path = tmp_path / "shadow-pack.json"
        connection = _write_clickhouse_connection(tmp_path, clickhouse_settings)

        pack = MigrationControlFacade().plan(
            manifest_path=str(manifest),
            actual_path=str(actual),
            strategy="shadow",
        )
        pack_path.write_text(json.dumps(pack), encoding="utf-8")
        shadow_table = str(pack["shadow"]["shadow_table"]).split(".", 1)[1]

        for phase in ("prepare", "create_shadow", "backfill", "validate", "cutover", "contract"):
            code, payload = MigrationControlFacade().apply(
                plan_path=str(pack_path),
                ledger_path=str(ledger),
                phase=phase,
                execute=True,
                target_connection_path=str(connection),
            )
            assert code == 0
            assert payload["status"] == "phase_applied"
            assert payload["phase"] == phase

        assert _sorting_key(clickhouse_connector, database, table) == "id"
        assert _table_exists(clickhouse_connector, database, shadow_table) == 0
        assert clickhouse_connector.get_records(f"SELECT count() FROM `{database}`.`{table}`")[0][0] == 1_000
        records = json.loads(ledger.read_text(encoding="utf-8"))["records"]
        assert [record["phase"] for record in records] == [
            "prepare",
            "create_shadow",
            "backfill",
            "validate",
            "cutover",
            "contract",
        ]
        assert any(record.get("operations") for record in records)
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{table}`")
        if shadow_table:
            clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{shadow_table}`")


def _write_manifest(tmp_path: Path, *, database: str, table: str) -> Path:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "source": {
                    "options": {
                        "columns": [
                            {"name": "id", "type": "bigint"},
                            {"name": "name", "type": "varchar"},
                            {"name": "amount", "type": "decimal"},
                        ]
                    }
                },
                "sink": {
                    "type": "clickhouse",
                    "table": {"schema": database, "name": table},
                    "options": {
                        "physical_design": {
                            "columns": {
                                "id": {"target_type": {"clickhouse": "Int64"}},
                                "name": {"target_type": {"clickhouse": "String"}},
                                "amount": {"target_type": {"clickhouse": "Decimal(18, 2)"}},
                            },
                            "storage": {"clickhouse": {"engine": "MergeTree", "order_by": ["id"]}},
                        }
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return manifest


def _write_actual(tmp_path: Path, *, database: str, table: str) -> Path:
    actual = tmp_path / "actual.json"
    actual.write_text(
        json.dumps(
            {
                "sink_type": "clickhouse",
                "table": f"{database}.{table}",
                "engine": "MergeTree",
                "order_by": [],
                "columns": {
                    "id": {"type": "Int64", "nullable": False, "position": 1},
                    "name": {"type": "String", "nullable": False, "position": 2},
                    "amount": {"type": "Decimal(18, 2)", "nullable": False, "position": 3},
                },
                "table_settings": {},
            }
        ),
        encoding="utf-8",
    )
    return actual


def _write_clickhouse_connection(tmp_path: Path, clickhouse_settings) -> Path:
    connection = tmp_path / "clickhouse-connection.json"
    connection.write_text(
        json.dumps(
            {
                "type": "clickhouse",
                "host": clickhouse_settings.host,
                "port": clickhouse_settings.port,
                "database": clickhouse_settings.database,
                "user": clickhouse_settings.user,
                "password": clickhouse_settings.password,
                "secure": False,
            }
        ),
        encoding="utf-8",
    )
    return connection


def _execute_operations(clickhouse_connector, phase: dict[str, object]) -> None:
    for operation in phase.get("operations", []):
        if isinstance(operation, dict):
            clickhouse_connector.execute_query(str(operation["sql"]))


def _assert_validation_queries_pass(clickhouse_connector, phase: dict[str, object]) -> None:
    results = [
        clickhouse_connector.get_records(str(validation["sql"]))
        for validation in phase.get("validations", [])
        if isinstance(validation, dict)
    ]
    assert results[0] == results[1]
    assert results[2] == results[3]
    assert results[4] == []


def _sorting_key(clickhouse_connector, database: str, table: str) -> str:
    return str(
        clickhouse_connector.get_records(
            "SELECT sorting_key FROM system.tables WHERE database = %(database)s AND name = %(table)s",
            {"database": database, "table": table},
        )[0][0]
    )


def _table_exists(clickhouse_connector, database: str, table: str) -> int:
    return int(clickhouse_connector.get_records(f"EXISTS TABLE `{database}`.`{table}`")[0][0])
