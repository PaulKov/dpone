"""Optional Docker MySQL source smoke (SKIP when service unavailable)."""

from __future__ import annotations

import os

import pytest

pytestmark = [pytest.mark.integration, pytest.mark.integration_mysql]


def _mysql_env_ready() -> bool:
    return bool(os.environ.get("DPONE_IT_MYSQL_HOST") or os.environ.get("DPONE_IT_MYSQL_PORT_FORWARD"))


@pytest.mark.skipif(not _mysql_env_ready(), reason="MySQL integration host not configured (DPONE_IT_MYSQL_*)")
def test_mysql_connector_selects_and_exports_mssql_delimited(tmp_path) -> None:
    pymysql = pytest.importorskip("pymysql")
    from dpone.runtime.connectors.mysql import MySQLConnector

    host = os.environ.get("DPONE_IT_MYSQL_HOST", "127.0.0.1")
    port = int(os.environ.get("DPONE_IT_MYSQL_PORT_FORWARD", "53306"))
    database = os.environ.get("DPONE_IT_MYSQL_DATABASE", "dpone_it")
    user = os.environ.get("DPONE_IT_MYSQL_USER", "dpone")
    password = os.environ.get("DPONE_IT_MYSQL_PASSWORD", "dpone")

    admin = pymysql.connect(host=host, port=port, user=user, password=password, database=database, autocommit=True)
    try:
        with admin.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS dpone_mysql_smoke (
                    id INT PRIMARY KEY,
                    name VARCHAR(64) NOT NULL,
                    updated_at DATETIME NOT NULL
                )
                """
            )
            cursor.execute("DELETE FROM dpone_mysql_smoke")
            cursor.execute(
                "INSERT INTO dpone_mysql_smoke (id, name, updated_at) VALUES (1, %s, '2026-07-21 10:00:00')",
                ("hello\tworld",),
            )
    finally:
        admin.close()

    connector = MySQLConnector(host=host, port=port, database=database, user=user, password=password)
    schema = list(connector.get_table_column_types(database, "dpone_mysql_smoke").items())
    out = tmp_path / "smoke.bcp"
    stats = connector.export_mssql_delimited_to_file(
        f"SELECT `id`, `name`, `updated_at` FROM `{database}`.`dpone_mysql_smoke`",
        str(out),
        schema,
    )
    assert stats["row_count"] == 1
    assert out.exists()
    text = out.read_text(encoding="utf-8")
    assert "1\t" in text
