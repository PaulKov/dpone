from __future__ import annotations

import json
import os
import subprocess
import uuid
from pathlib import Path
from typing import Any

import pytest

from tests.test_semantic_refresh_mssql_live import (
    _connect_factory,
    _drop_named_schema,
    _execute,
)

pytestmark = [pytest.mark.integration_mssql, pytest.mark.integration_live]

_ENABLED = os.getenv("DPONE_RUN_DBT_SQLSERVER_STRATEGY_MATRIX_LIVE") == "1"
_PROJECT = Path("tests/fixtures/dbt-sqlserver-strategy-matrix")


@pytest.mark.skipif(not _ENABLED, reason="Set DPONE_RUN_DBT_SQLSERVER_STRATEGY_MATRIX_LIVE=1")
def test_live_dbt_sqlserver_materialization_and_incremental_strategy_matrix(tmp_path: Path) -> None:
    suffix = uuid.uuid4().hex[:12]
    source_schema = f"dpone_dbt_strategy_src_{suffix}"
    target_schema = f"dpone_dbt_strategy_{suffix}"
    connection = _connect_factory()()
    connection.autocommit = True
    try:
        _drop_strategy_schema(connection, target_schema)
        _drop_named_schema(connection, source_schema)
        _execute(connection, f"CREATE SCHEMA [{source_schema}]")
        _execute(
            connection,
            f"""
CREATE TABLE [{source_schema}].[events] (
    id int NOT NULL PRIMARY KEY,
    payload varchar(32) NULL,
    batch_id int NOT NULL
);
INSERT INTO [{source_schema}].[events] (id, payload, batch_id)
VALUES (1, 'one', 1), (2, 'two-v1', 1);
""",
        )
        _run_dbt(
            source_schema=source_schema,
            target_schema=target_schema,
            batch_id=1,
            target_path=tmp_path / "first",
        )
        _execute(
            connection,
            f"""
UPDATE [{source_schema}].[events] SET payload = 'two-v2', batch_id = 2 WHERE id = 2;
INSERT INTO [{source_schema}].[events] (id, payload, batch_id) VALUES (3, 'three', 2);
""",
        )
        _run_dbt(
            source_schema=source_schema,
            target_schema=target_schema,
            batch_id=2,
            target_path=tmp_path / "second",
        )

        assert _rows(connection, target_schema, "matrix_table") == [
            (1, "one", 1),
            (2, "two-v2", 2),
            (3, "three", 2),
        ]
        assert _rows(connection, target_schema, "matrix_view") == _rows(
            connection,
            target_schema,
            "matrix_table",
        )
        assert _rows(connection, target_schema, "matrix_append") == [
            (1, "one", 1),
            (2, "two-v1", 1),
            (2, "two-v2", 2),
            (3, "three", 2),
        ]
        assert _rows(connection, target_schema, "matrix_merge") == [
            (1, "one", 1),
            (2, "two-v2", 2),
            (3, "three", 2),
        ]
    finally:
        _drop_strategy_schema(connection, target_schema)
        _drop_named_schema(connection, source_schema)
        connection.close()


def _run_dbt(
    *,
    source_schema: str,
    target_schema: str,
    batch_id: int,
    target_path: Path,
) -> None:
    env = {
        **os.environ,
        "DPONE_DBT_STRATEGY_SCHEMA": target_schema,
    }
    result = subprocess.run(
        [
            "dbt",
            "run",
            "--project-dir",
            str(_PROJECT),
            "--profiles-dir",
            str(_PROJECT),
            "--target-path",
            str(target_path),
            "--no-partial-parse",
            "--vars",
            json.dumps(
                {
                    "batch_id": batch_id,
                    "source_schema": source_schema,
                    "source_table": "events",
                },
                sort_keys=True,
            ),
        ],
        check=False,
        capture_output=True,
        env=env,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (result.stdout + result.stderr)[-6000:]


def _rows(connection: Any, schema: str, table: str) -> list[tuple[int, str | None, int]]:
    cursor = connection.cursor()
    try:
        rows = cursor.execute(
            f"SELECT id, payload, batch_id FROM [{schema}].[{table}] ORDER BY id, batch_id, payload"
        ).fetchall()
        return [(int(row[0]), row[1], int(row[2])) for row in rows]
    finally:
        cursor.close()


def _drop_strategy_schema(connection: Any, schema: str) -> None:
    _execute(connection, f"DROP VIEW IF EXISTS [{schema}].[matrix_view]")
    _drop_named_schema(connection, schema)
