from __future__ import annotations

# ruff: noqa: E402
import csv
import os
from pathlib import Path

import pytest

pytestmark = [pytest.mark.integration]
if str(os.getenv("DPONE_RUN_INTEGRATION", "0")).strip().lower() not in {"1", "true", "yes", "on"}:
    pytest.skip("Integration tests are disabled", allow_module_level=True)

pytest.importorskip("psycopg")

from psycopg import sql

from dpone.config import LoadConfig
from dpone.runtime.artifacts import FileExportArtifact
from dpone.runtime.sinks.postgres_staging_manager import PostgresStagingManager


def _load_config(schema: str, table: str = "orders") -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="tgt",
        source_schema="public",
        source_table="orders",
        target_schema=schema,
        target_table=table,
        staging_schema=schema,
        log_sample_rows=0,
    )


def test_postgres_staging_manager_creates_inserts_and_drops_tables(postgres_connector, postgres_schema: str) -> None:
    manager = PostgresStagingManager(postgres_connector)
    load_config = _load_config(postgres_schema)
    schema = [("id", "integer"), ("name", "text")]

    artifact = manager.create(load_config, schema)
    try:
        inserted = manager.insert_rows(
            artifact,
            [
                {"id": 1, "name": "alice"},
                {"id": 2, "name": "bob"},
            ],
        )
        assert inserted == 2

        rows = postgres_connector.get_records(
            sql.SQL("SELECT id, name FROM {}.{} ORDER BY id").format(
                sql.Identifier(artifact.schema),
                sql.Identifier(artifact.table),
            ),
            as_dict=True,
        )
        assert rows == [{"id": 1, "name": "alice"}, {"id": 2, "name": "bob"}]
    finally:
        manager.drop(artifact)

    exists = postgres_connector.get_records(
        """
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
        )
        """,
        (artifact.schema, artifact.table),
    )
    assert exists[0][0] is False


def test_postgres_staging_manager_loads_from_csv_file(
    postgres_connector,
    postgres_schema: str,
    tmp_path: Path,
) -> None:
    manager = PostgresStagingManager(postgres_connector)
    load_config = _load_config(postgres_schema, table="file_orders")
    schema = [("id", "integer"), ("city", "text")]

    artifact = manager.create(load_config, schema)
    csv_path = tmp_path / "orders.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow([1, "Paris"])
        writer.writerow([2, "Berlin"])

    file_artifact = FileExportArtifact(str(csv_path), columns=["id", "city"], compressed=False, format="csv")
    try:
        inserted = manager.load_from_file(artifact, file_artifact)
        assert inserted == 2

        count = postgres_connector.get_records(
            sql.SQL("SELECT COUNT(*) FROM {}.{}").format(
                sql.Identifier(artifact.schema),
                sql.Identifier(artifact.table),
            )
        )
        assert count[0][0] == 2
    finally:
        manager.drop(artifact)
