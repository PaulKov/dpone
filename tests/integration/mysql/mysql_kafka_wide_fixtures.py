"""Wide MySQL fixtures for mysql→Kafka live certification.

No dedicated PairTypeMatrix profile (parity with postgres→kafka). The wide
table reuses the shared mysql wide column set; live assertions validate JSON
message payloads from CSV export wire rather than sink DDL.
"""

from __future__ import annotations

from dataclasses import dataclass

WIDE_TABLE = "mysql_to_kafka_wide"

# Shared MySQL DDL/seed with other mysql wide suites.
_SPECS: list[tuple[str, str, str, str]] = [
    ("id", "INT NOT NULL", "int", "1"),
    ("business_date", "DATE NOT NULL", "date", "'2026-07-21'"),
    ("updated_at", "DATETIME NOT NULL", "datetime", "'2026-07-21 10:00:00'"),
    ("c_tinyint", "TINYINT NULL", "tinyint", "7"),
    ("c_smallint", "SMALLINT NULL", "smallint", "70"),
    ("c_bigint", "BIGINT NULL", "bigint", "7000000000"),
    ("c_decimal", "DECIMAL(18,4) NULL", "decimal(18,4)", "12.3456"),
    ("c_bool", "TINYINT(1) NULL", "tinyint(1)", "1"),
    ("c_float", "FLOAT NULL", "float", "1.25"),
    ("c_double", "DOUBLE NULL", "double", "2.5"),
    ("c_date", "DATE NULL", "date", "'2026-01-15'"),
    ("c_datetime", "DATETIME NULL", "datetime", "'2026-01-15 12:30:00'"),
    ("c_timestamp", "TIMESTAMP NULL", "timestamp", "'2026-01-15 12:30:00'"),
    ("c_time", "TIME NULL", "time", "'12:30:00'"),
    ("c_varchar", "VARCHAR(255) NULL", "varchar(255)", "'alpha,comma'"),
    ("c_text", "TEXT NULL", "text", "'long text value'"),
    ("c_json", "JSON NULL", "json", "CAST('{\\\"k\\\":1}' AS JSON)"),
    ("c_blob", "BLOB NULL", "blob", "X'010203'"),
    ("c_name", "VARCHAR(64) NOT NULL", "varchar(64)", "'row-one'"),
]


@dataclass(frozen=True)
class WideColumn:
    name: str
    mysql_ddl: str
    mysql_type: str
    seed_sql: str


def wide_columns() -> list[WideColumn]:
    return [
        WideColumn(name=name, mysql_ddl=f"`{name}` {ddl}", mysql_type=mysql_type, seed_sql=seed)
        for name, ddl, mysql_type, seed in _SPECS
    ]


def create_wide_mysql_table(mysql, *, table: str = WIDE_TABLE) -> list[WideColumn]:
    columns = wide_columns()
    database = mysql.database
    ddl_cols = ",\n            ".join(col.mysql_ddl for col in columns)
    mysql.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{table}`")
    mysql.execute_query(
        f"""
        CREATE TABLE `{database}`.`{table}` (
            {ddl_cols},
            PRIMARY KEY (`id`)
        )
        """
    )
    names = ", ".join(f"`{col.name}`" for col in columns)
    values = ", ".join(col.seed_sql for col in columns)
    mysql.execute_query(f"INSERT INTO `{database}`.`{table}` ({names}) VALUES ({values})")
    values2 = []
    for col in columns:
        if col.name == "id":
            values2.append("2")
        elif col.name == "updated_at":
            values2.append("'2026-07-21 11:00:00'")
        elif col.name == "c_name":
            values2.append("'row-two'")
        elif col.name == "c_varchar":
            values2.append("'beta'")
        elif col.name == "c_bool":
            values2.append("0")
        else:
            values2.append(col.seed_sql)
    mysql.execute_query(f"INSERT INTO `{database}`.`{table}` ({names}) VALUES ({', '.join(values2)})")
    return columns


def insert_wide_watermark_row(mysql, *, table: str = WIDE_TABLE, row_id: int = 3) -> None:
    columns = wide_columns()
    database = mysql.database
    names = ", ".join(f"`{col.name}`" for col in columns)
    values = []
    for col in columns:
        if col.name == "id":
            values.append(str(row_id))
        elif col.name == "updated_at":
            values.append("'2026-07-21 12:00:00'")
        elif col.name == "c_name":
            values.append("'row-three'")
        else:
            values.append(col.seed_sql)
    mysql.execute_query(f"INSERT INTO `{database}`.`{table}` ({names}) VALUES ({', '.join(values)})")


__all__ = [
    "WIDE_TABLE",
    "WideColumn",
    "create_wide_mysql_table",
    "insert_wide_watermark_row",
    "wide_columns",
]
