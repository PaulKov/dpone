from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

import pytest
import yaml

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifacts import InMemoryRowsArtifact
from dpone.runtime.schema_identity import SchemaIdentityProjectionService
from dpone.runtime.sinks.clickhouse import ClickHouseSink
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.services.schema_migration import MigrationControlFacade

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

pytest.importorskip("clickhouse_driver")


def test_clickhouse_schema_identity_expand_contract_wide_table_live(
    clickhouse_connector,
    clickhouse_settings,
    tmp_path: Path,
) -> None:
    table = f"it_identity_{uuid.uuid4().hex[:10]}"
    database = clickhouse_settings.database
    ledger = tmp_path / "ledger.json"
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{table}`")
    clickhouse_connector.execute_query(_wide_create_sql(database, table))
    try:
        _insert_wide_rows(clickhouse_connector, database, table, rows=10_000)
        manifest = _write_identity_manifest(tmp_path, database=database, table=table, strategy="expand_contract")
        source = _write_wide_source(tmp_path / "source.json")
        actual = _write_wide_actual(tmp_path / "actual.json", database=database, table=table)
        pack = MigrationControlFacade().plan(
            manifest_path=str(manifest),
            source_path=str(source),
            actual_path=str(actual),
        )
        pack_path = tmp_path / "identity-pack.json"
        pack_path.write_text(json.dumps(pack), encoding="utf-8")
        connection = _write_clickhouse_connection(tmp_path, clickhouse_settings)

        for phase in ("expand", "backfill"):
            code, payload = MigrationControlFacade().apply(
                plan_path=str(pack_path),
                ledger_path=str(ledger),
                phase=phase,
                execute=True,
                target_connection_path=str(connection),
            )
            assert code == 0, payload
            assert payload["phase"] == phase

        _wait_for_mutations(clickhouse_connector, database, table)
        for phase in ("validate", "contract"):
            code, payload = MigrationControlFacade().apply(
                plan_path=str(pack_path),
                ledger_path=str(ledger),
                phase=phase,
                execute=True,
                target_connection_path=str(connection),
            )
            assert code == 0, payload
            assert payload["phase"] == phase

        columns = _columns(clickhouse_connector, database, table)
        assert "customer_id" in columns
        assert "client_id" not in columns
        assert clickhouse_connector.get_records(f"SELECT count() FROM `{database}`.`{table}`")[0][0] == 10_000
        assert (
            clickhouse_connector.get_records(f"SELECT sum(customer_id) FROM `{database}`.`{table}`")[0][0] == 49_995_000
        )
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{table}`")


def test_clickhouse_schema_identity_runtime_alias_projection_loads_live(
    clickhouse_connector,
    clickhouse_settings,
) -> None:
    table = f"it_identity_runtime_{uuid.uuid4().hex[:10]}"
    database = clickhouse_settings.database
    clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{table}`")
    try:
        load_config = _load_config(database, table)
        payload = LoadPayload(
            artifact=InMemoryRowsArtifact(_wide_rows(10_000)),
            schema=[("client_id", "bigint"), *[(name, "int") for name in _wide_column_names()]],
        )
        projected = SchemaIdentityProjectionService().prepare_payload(load_config, payload)
        result = ClickHouseSink(clickhouse_connector).load(load_config, projected)

        columns = _columns(clickhouse_connector, database, table)
        assert result.total_rows == 10_000
        assert "customer_id" in columns
        assert "client_id" not in columns
        assert (
            clickhouse_connector.get_records(f"SELECT sum(customer_id) FROM `{database}`.`{table}`")[0][0] == 49_995_000
        )
    finally:
        clickhouse_connector.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{table}`")


def _wide_column_names() -> list[str]:
    return [f"c{index:03d}" for index in range(1, 200)]


def _wide_create_sql(database: str, table: str) -> str:
    columns = ["`client_id` Int64", *[f"`{name}` Int64" for name in _wide_column_names()]]
    return f"CREATE TABLE `{database}`.`{table}` ({', '.join(columns)}) ENGINE = MergeTree ORDER BY tuple()"


def _insert_wide_rows(clickhouse_connector, database: str, table: str, *, rows: int) -> None:
    columns = ["client_id", *_wide_column_names()]
    quoted = ", ".join(f"`{name}`" for name in columns)
    for start in range(0, rows, 1_000):
        batch = [_wide_row(index, as_tuple=True) for index in range(start, min(start + 1_000, rows))]
        clickhouse_connector.connection.execute(f"INSERT INTO `{database}`.`{table}` ({quoted}) VALUES", batch)


def _wide_rows(rows: int) -> list[dict[str, int]]:
    return [_wide_row(index, as_tuple=False) for index in range(rows)]


def _wide_row(index: int, *, as_tuple: bool) -> tuple[int, ...] | dict[str, int]:
    values = [index, *[((index + offset) % 10_000) for offset, _ in enumerate(_wide_column_names(), start=1)]]
    if as_tuple:
        return tuple(values)
    return {"client_id": values[0], **{name: values[pos] for pos, name in enumerate(_wide_column_names(), start=1)}}


def _write_identity_manifest(tmp_path: Path, *, database: str, table: str, strategy: str) -> Path:
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(
        yaml.safe_dump(
            {
                "source": {"type": "mssql", "table": {"schema": "dbo", "name": "orders"}},
                "sink": {
                    "type": "clickhouse",
                    "table": {"schema": database, "name": table},
                    "options": {
                        "physical_design": {"storage": {"clickhouse": {"engine": "MergeTree", "order_by": []}}},
                        "schema_identity": {
                            "enabled": True,
                            "rename": {"strategy": strategy},
                            "columns": {
                                "customer_id": {
                                    "id": "orders.customer_id",
                                    "aliases": [{"name": "client_id"}],
                                }
                            },
                        },
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return manifest


def _write_wide_source(path: Path) -> Path:
    columns = [{"name": "client_id", "dtype": "bigint", "nullable": False}]
    columns.extend({"name": name, "dtype": "int", "nullable": False} for name in _wide_column_names())
    path.write_text(json.dumps(columns), encoding="utf-8")
    return path


def _write_wide_actual(path: Path, *, database: str, table: str) -> Path:
    columns = {"client_id": {"type": "Int64", "nullable": False, "position": 1}}
    columns.update(
        {
            name: {"type": "Int64", "nullable": False, "position": index + 1}
            for index, name in enumerate(_wide_column_names(), start=1)
        }
    )
    path.write_text(
        json.dumps(
            {
                "sink_type": "clickhouse",
                "table": f"{database}.{table}",
                "engine": "MergeTree",
                "order_by": [],
                "columns": columns,
                "table_settings": {},
            }
        ),
        encoding="utf-8",
    )
    return path


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


def _load_config(database: str, table: str) -> LoadConfig:
    return LoadConfig(
        source_conn_id="mssql",
        target_conn_id="clickhouse",
        source_schema="dbo",
        source_table="orders",
        target_schema=database,
        target_table=table,
        load_strategy=LoadStrategy.FULL_REFRESH,
        options={
            "schema_identity": {
                "enabled": True,
                "columns": {
                    "customer_id": {
                        "id": "orders.customer_id",
                        "aliases": [{"name": "client_id"}],
                    }
                },
            }
        },
    )


def _wait_for_mutations(clickhouse_connector, database: str, table: str) -> None:
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        pending = clickhouse_connector.get_records(
            "SELECT count() FROM system.mutations WHERE database = %(database)s AND table = %(table)s AND is_done = 0",
            {"database": database, "table": table},
        )[0][0]
        if int(pending) == 0:
            return
        time.sleep(0.5)
    raise AssertionError(f"ClickHouse mutations did not finish for {database}.{table}")


def _columns(clickhouse_connector, database: str, table: str) -> set[str]:
    return {
        str(row[0])
        for row in clickhouse_connector.get_records(
            "SELECT name FROM system.columns WHERE database = %(database)s AND table = %(table)s",
            {"database": database, "table": table},
        )
    }
