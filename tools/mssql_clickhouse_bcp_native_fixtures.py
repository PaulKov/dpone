"""BCP-native wide type fixtures for MSSQL -> ClickHouse certification."""

from __future__ import annotations

import sys
from pathlib import Path

_TOOLS_DIR = Path(__file__).resolve().parent
if str(_TOOLS_DIR) not in sys.path:
    sys.path.insert(0, str(_TOOLS_DIR))

import mssql_clickhouse_wide_type_certification as base  # noqa: E402


def build_bcp_native_columns(column_count: int) -> list[base.WideTypeColumn]:
    """Return a wide fixture covered by the v0.26 BCP-native decoder."""

    columns = [
        base.WideTypeColumn("order_id", "int NOT NULL", "CAST(gs AS int)"),
        base.WideTypeColumn("tiny_id", "tinyint", "CAST(gs % 256 AS tinyint)"),
        base.WideTypeColumn("small_id", "smallint", "CAST((gs % 32768) - 16384 AS smallint)"),
        base.WideTypeColumn("customer_id", "bigint", "CAST(gs * 1000003 AS bigint)"),
        base.WideTypeColumn("amount", "decimal(18,4)", "CAST(((gs % 200000) - 100000) / 10000.0 AS decimal(18,4))"),
        base.WideTypeColumn("amount_high", "numeric(38,9)", "CAST((gs % 1000000) / 1000000000.0 AS numeric(38,9))"),
        base.WideTypeColumn("money_amount", "money", "CAST(((gs % 20000) - 10000) / 100.0 AS money)"),
        base.WideTypeColumn("small_money_amount", "smallmoney", "CAST(((gs % 2000) - 1000) / 100.0 AS smallmoney)"),
        base.WideTypeColumn("is_active", "bit", "CAST(gs % 2 AS bit)"),
        base.WideTypeColumn("float_value", "float", "CAST((gs % 100000) / 7.0 AS float)"),
        base.WideTypeColumn("real_value", "real", "CAST((gs % 10000) / 3.0 AS real)"),
        base.WideTypeColumn(
            "row_guid", "uniqueidentifier", "CONVERT(uniqueidentifier, HASHBYTES('MD5', CONVERT(varchar(32), gs)))"
        ),
        base.WideTypeColumn(
            "business_date",
            "date",
            "CASE WHEN gs = 1 THEN CONVERT(date, '1970-01-01') "
            "WHEN gs = 2 THEN CONVERT(date, '2149-06-06') "
            "ELSE DATEADD(day, gs % 365, CONVERT(date, '2026-01-01')) END",
        ),
        base.WideTypeColumn("business_time", "time(7)", "CAST('12:34:56.1234567' AS time(7))"),
        base.WideTypeColumn(
            "created_dt",
            "datetime",
            "CASE WHEN gs = 1 THEN CONVERT(datetime, '1900-01-01T00:00:00') "
            "WHEN gs = 2 THEN CONVERT(datetime, '2299-12-31T23:59:59.997') "
            "ELSE DATEADD(millisecond, gs % 997, CONVERT(datetime, '2026-01-01T00:00:00')) END",
        ),
        base.WideTypeColumn(
            "created_at",
            "datetime2(7)",
            "CASE WHEN gs = 1 THEN CONVERT(datetime2(7), '1900-01-01T00:00:00.0000000') "
            "WHEN gs = 2 THEN CONVERT(datetime2(7), '2299-12-31T23:59:59.9999999') "
            "ELSE CONVERT(datetime2(7), '2026-06-22T12:34:56.1234567') END",
        ),
        base.WideTypeColumn(
            "offset_at",
            "datetimeoffset(7)",
            "CAST('2026-06-22T15:34:56.7654321+03:00' AS datetimeoffset(7))",
        ),
        base.WideTypeColumn(
            "rounded_dt", "smalldatetime", "DATEADD(minute, gs % 1440, CONVERT(smalldatetime, '2026-01-01T00:00:00'))"
        ),
        base.WideTypeColumn("fixed_ascii", "char(8)", "CAST('a' AS char(8))"),
        base.WideTypeColumn("ascii_text", "varchar(200)", "'ascii-' + CONVERT(varchar(40), gs) + CHAR(9) + 'tab'"),
        base.WideTypeColumn(
            "ascii_max", "varchar(max)", "REPLICATE(CONVERT(varchar(max), 'x'), 128) + CONVERT(varchar(40), gs)"
        ),
        base.WideTypeColumn("fixed_unicode", "nchar(8)", "CAST(N'я' AS nchar(8))"),
        base.WideTypeColumn(
            "unicode_text", "nvarchar(200)", "N'Привет-' + CONVERT(nvarchar(40), gs) + NCHAR(10) + N'line'"
        ),
        base.WideTypeColumn(
            "unicode_max", "nvarchar(max)", "REPLICATE(CONVERT(nvarchar(max), N'ж'), 128) + CONVERT(nvarchar(40), gs)"
        ),
        base.WideTypeColumn("empty_string", "nvarchar(20)", "CASE WHEN gs % 7 = 0 THEN N'' ELSE N'not-empty' END"),
        base.WideTypeColumn("nullable_text", "nvarchar(20)", "CASE WHEN gs % 11 = 0 THEN NULL ELSE N'value' END"),
        base.WideTypeColumn(
            "fixed_payload", "binary(4)", "CONVERT(binary(4), HASHBYTES('MD5', CONVERT(varchar(32), gs)))"
        ),
        base.WideTypeColumn("payload_bin", "varbinary(16)", "HASHBYTES('MD5', CONVERT(varchar(32), gs))"),
        base.WideTypeColumn(
            "payload_bin_max",
            "varbinary(max)",
            "CONVERT(varbinary(max), HASHBYTES('SHA2_256', CONVERT(varchar(32), gs)))",
        ),
    ]
    if column_count <= len(columns):
        return columns[:column_count]
    return [*columns, *_extra_columns(column_count - len(columns))]


def _extra_columns(count: int) -> list[base.WideTypeColumn]:
    templates = (
        ("wide_int", "int", "CAST((gs + {i}) % 2147483647 AS int)"),
        ("wide_decimal", "decimal(18,4)", "CAST(((gs + {i}) % 100000) / 10000.0 AS decimal(18,4))"),
        (
            "wide_text",
            "nvarchar(80)",
            "CASE WHEN (gs + {i}) % 17 = 0 THEN NULL ELSE N'wide-{i}-' + CONVERT(nvarchar(40), gs) END",
        ),
        (
            "wide_varchar",
            "varchar(80)",
            "CASE WHEN (gs + {i}) % 19 = 0 THEN '' ELSE 'wide-{i}-' + CONVERT(varchar(40), gs) END",
        ),
        (
            "wide_date",
            "date",
            "CASE WHEN gs = 1 THEN CONVERT(date, '1970-01-01') "
            "WHEN gs = 2 THEN CONVERT(date, '2149-06-06') "
            "ELSE DATEADD(day, (gs + {i}) % 365, CONVERT(date, '2026-01-01')) END",
        ),
        ("wide_time", "time(7)", "CAST('01:02:03.1234567' AS time(7))"),
        ("wide_binary", "varbinary(16)", "HASHBYTES('MD5', CONVERT(varchar(32), gs + {i}))"),
        ("wide_bit", "bit", "CAST((gs + {i}) % 2 AS bit)"),
        (
            "wide_datetime",
            "datetime2(7)",
            "CASE WHEN gs = 1 THEN CONVERT(datetime2(7), '1900-01-01T00:00:00.0000000') "
            "WHEN gs = 2 THEN CONVERT(datetime2(7), '2299-12-31T23:59:59.9999999') "
            "ELSE DATEADD(second, (gs + {i}) % 31536000, CONVERT(datetime2(7), '2026-01-01T00:00:00')) END",
        ),
        (
            "wide_uuid",
            "uniqueidentifier",
            "CONVERT(uniqueidentifier, HASHBYTES('MD5', CONVERT(varchar(32), gs + {i})))",
        ),
    )
    return [
        base.WideTypeColumn(
            name=f"{prefix}_{index + 1:03d}",
            mssql_type=mssql_type,
            insert_expression=expression.format(i=index + 1),
        )
        for index, (prefix, mssql_type, expression) in ((i, templates[i % len(templates)]) for i in range(count))
    ]


__all__ = ["build_bcp_native_columns"]
