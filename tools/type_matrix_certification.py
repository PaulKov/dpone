#!/usr/bin/env python
"""Certify heterogeneous type handling for PostgreSQL, MSSQL, and ClickHouse.

The suite verifies two directions:

1. PostgreSQL -> MSSQL using PostgreSQL COPY TO STDOUT and SQL Server bcp in.
2. MSSQL -> ClickHouse using SQL Server bcp queryout and ClickHouse HTTP TabSeparated insert.

For engine-specific/exotic types, correctness is defined as lossless canonical
serialization. This is the only portable cross-engine contract for values such
as PostgreSQL ranges/composites and SQL Server hierarchyid/geography/sql_variant.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests

from dpone._compat import UTC
from dpone.runtime.connectors import MSSQLConnector, PostgresConnector
from dpone.runtime.connectors.clickhouse import ClickHouseConnector
from dpone.runtime.connectors.mssql_bulk import BcpOptions

ACTOR = "Codex GPT-5"


@dataclass(frozen=True, slots=True)
class TypeCase:
    name: str
    source_type: str
    target_type: str
    category: str
    source_expression: str
    export_expression: str
    insertable: bool = True
    notes: str = ""

    @property
    def source_sql(self) -> str:
        return f"({self.source_expression})::{self.source_type} AS {self.name}"

    @property
    def export_value_sql(self) -> str:
        return self.export_expression.rsplit(" AS ", 1)[0]


@dataclass(frozen=True, slots=True)
class StageMetric:
    name: str
    seconds: float
    rows: int | None = None
    bytes_processed: int | None = None
    rows_per_second: float | None = None
    mib_per_second: float | None = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="dpone exhaustive-ish type matrix certification")
    parser.add_argument("--rows", type=int, default=25)
    parser.add_argument("--json-output", required=True)
    parser.add_argument("--artifact-label", required=True)
    parser.add_argument("--bcp-path", default="/opt/homebrew/bin/bcp")
    parser.add_argument("--pg-host", default="127.0.0.1")
    parser.add_argument("--pg-port", type=int, default=15432)
    parser.add_argument("--pg-database", default="dpone")
    parser.add_argument("--pg-user", default="dpone")
    parser.add_argument("--pg-password", default="dpone_pass")
    parser.add_argument("--mssql-host", default="127.0.0.1")
    parser.add_argument("--mssql-port", type=int, default=15433)
    parser.add_argument("--mssql-database", default="dpone")
    parser.add_argument("--mssql-user", default="sa")
    parser.add_argument("--mssql-password", default="Dpone_Strong_12345!")
    parser.add_argument("--mssql-driver", default="ODBC Driver 18 for SQL Server")
    parser.add_argument("--clickhouse-host", default="127.0.0.1")
    parser.add_argument("--clickhouse-native-port", type=int, default=19000)
    parser.add_argument("--clickhouse-http-port", type=int, default=18123)
    parser.add_argument("--clickhouse-database", default="dpone")
    parser.add_argument("--clickhouse-user", default="dpone")
    parser.add_argument("--clickhouse-password", default="dpone_pass")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    artifact: dict[str, Any] = {
        "artifact_schema": "dpone.test-artifact.v1",
        "artifact_type": "certification.type_matrix",
        "artifact_label": args.artifact_label,
        "created_by": ACTOR,
        "executed_by_os_user": "macbook",
        "started_at": now_utc(),
        "rows": args.rows,
        "definition": "lossless canonical serialization for cross-engine exotic/system-specific types",
    }
    pg = PostgresConnector(
        host=args.pg_host,
        port=args.pg_port,
        database=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
        application_name="dpone-type-cert-pg",
    )
    mssql = MSSQLConnector(
        host=args.mssql_host,
        port=args.mssql_port,
        database=args.mssql_database,
        user=args.mssql_user,
        password=args.mssql_password,
        driver=args.mssql_driver,
        trust_server_certificate=True,
        application_name="dpone-type-cert-mssql",
        bcp_path=args.bcp_path,
        query_timeout=0,
    )
    clickhouse = ClickHouseConnector(
        host=args.clickhouse_host,
        port=args.clickhouse_native_port,
        database=args.clickhouse_database,
        user=args.clickhouse_user,
        password=args.clickhouse_password,
        application_name="dpone-type-cert-clickhouse",
    )
    try:
        pg_cases = postgres_cases()
        mssql_cases = mssql_cases_matrix()
        pg_result = certify_postgres_to_mssql(pg, mssql, args, pg_cases)
        mssql_result = certify_mssql_to_clickhouse(mssql, clickhouse, args, mssql_cases)
        artifact.update(
            {
                "status": "passed" if pg_result["passed"] and mssql_result["passed"] else "failed",
                "result": "passed" if pg_result["passed"] and mssql_result["passed"] else "failed",
                "postgres_to_mssql": pg_result,
                "mssql_to_clickhouse": mssql_result,
                "finished_at": now_utc(),
            }
        )
        write_json(Path(args.json_output), artifact)
        print(json.dumps(artifact, indent=2, ensure_ascii=False, default=str))
        return 0 if artifact["status"] == "passed" else 1
    except Exception as exc:
        artifact.update(
            {
                "status": "failed",
                "result": "failed",
                "error_type": exc.__class__.__name__,
                "error": str(exc),
                "finished_at": now_utc(),
            }
        )
        write_json(Path(args.json_output), artifact)
        print(json.dumps(artifact, indent=2, ensure_ascii=False, default=str))
        return 1
    finally:
        pg.close()
        mssql.close()


def postgres_cases() -> list[TypeCase]:
    return [
        TypeCase("id", "bigint", "nvarchar(max)", "control", "gs", "id::text AS id"),
        TypeCase("pg_boolean", "boolean", "nvarchar(max)", "boolean", "gs % 2 = 0", "pg_boolean::text AS pg_boolean"),
        TypeCase(
            "pg_smallint", "smallint", "nvarchar(max)", "numeric", "gs % 32767", "pg_smallint::text AS pg_smallint"
        ),
        TypeCase("pg_integer", "integer", "nvarchar(max)", "numeric", "gs", "pg_integer::text AS pg_integer"),
        TypeCase("pg_bigint", "bigint", "nvarchar(max)", "numeric", "gs * 100000", "pg_bigint::text AS pg_bigint"),
        TypeCase(
            "pg_numeric",
            "numeric(38,10)",
            "nvarchar(max)",
            "numeric",
            "gs::numeric / 7.0",
            "pg_numeric::text AS pg_numeric",
        ),
        TypeCase("pg_real", "real", "nvarchar(max)", "numeric", "gs::real / 3.0", "pg_real::text AS pg_real"),
        TypeCase(
            "pg_double",
            "double precision",
            "nvarchar(max)",
            "numeric",
            "gs::double precision / 9.0",
            "pg_double::text AS pg_double",
        ),
        TypeCase(
            "pg_money", "money", "nvarchar(max)", "money", "(gs::numeric / 100.0)::money", "pg_money::text AS pg_money"
        ),
        TypeCase("pg_text", "text", "nvarchar(max)", "text", "'text-' || gs::text", "pg_text AS pg_text"),
        TypeCase(
            "pg_varchar", "varchar(128)", "nvarchar(max)", "text", "'varchar-' || gs::text", "pg_varchar AS pg_varchar"
        ),
        TypeCase(
            "pg_char", "char(16)", "nvarchar(max)", "text", "rpad('char-' || gs::text, 16, 'x')", "pg_char AS pg_char"
        ),
        TypeCase(
            "pg_bytea",
            "bytea",
            "nvarchar(max)",
            "binary",
            "decode(md5(gs::text), 'hex')",
            "encode(pg_bytea, 'hex') AS pg_bytea",
        ),
        TypeCase(
            "pg_date",
            "date",
            "nvarchar(max)",
            "date_time",
            "date '2026-01-01' + (gs % 365)::int",
            "pg_date::text AS pg_date",
        ),
        TypeCase(
            "pg_time",
            "time(6)",
            "nvarchar(max)",
            "date_time",
            "time '00:00:00' + (gs % 86400) * interval '1 second'",
            "pg_time::text AS pg_time",
        ),
        TypeCase(
            "pg_timetz",
            "timetz",
            "nvarchar(max)",
            "date_time",
            "time with time zone '12:00:00+03' + (gs % 60) * interval '1 second'",
            "pg_timetz::text AS pg_timetz",
        ),
        TypeCase(
            "pg_timestamp",
            "timestamp(6)",
            "nvarchar(max)",
            "date_time",
            "timestamp '2026-01-01 00:00:00' + gs * interval '1 second'",
            "pg_timestamp::text AS pg_timestamp",
        ),
        TypeCase(
            "pg_timestamptz",
            "timestamptz",
            "nvarchar(max)",
            "date_time",
            "timestamptz '2026-01-01 00:00:00+00' + gs * interval '1 second'",
            "pg_timestamptz::text AS pg_timestamptz",
        ),
        TypeCase(
            "pg_interval",
            "interval",
            "nvarchar(max)",
            "date_time",
            "make_interval(secs => gs::int)",
            "pg_interval::text AS pg_interval",
        ),
        TypeCase(
            "pg_uuid",
            "uuid",
            "nvarchar(max)",
            "uuid",
            "('00000000-0000-0000-0000-' || lpad(gs::text, 12, '0'))::uuid",
            "pg_uuid::text AS pg_uuid",
        ),
        TypeCase(
            "pg_json",
            "json",
            "nvarchar(max)",
            "semi_structured",
            "json_build_object('id', gs, 'kind', 'json')",
            "pg_json::text AS pg_json",
        ),
        TypeCase(
            "pg_jsonb",
            "jsonb",
            "nvarchar(max)",
            "semi_structured",
            "jsonb_build_object('id', gs, 'kind', 'jsonb')",
            "pg_jsonb::text AS pg_jsonb",
        ),
        TypeCase(
            "pg_xml",
            "xml",
            "nvarchar(max)",
            "semi_structured",
            "xmlelement(name row, xmlattributes(gs as id))",
            "xmlserialize(CONTENT pg_xml AS text) AS pg_xml",
        ),
        TypeCase(
            "pg_int_array",
            "integer[]",
            "nvarchar(max)",
            "array",
            "ARRAY[gs::int, (gs + 1)::int]",
            "pg_int_array::text AS pg_int_array",
        ),
        TypeCase(
            "pg_text_array",
            "text[]",
            "nvarchar(max)",
            "array",
            "ARRAY['a' || gs::text, 'b' || gs::text]",
            "pg_text_array::text AS pg_text_array",
        ),
        TypeCase(
            "pg_numrange",
            "numrange",
            "nvarchar(max)",
            "range",
            "numrange(gs::numeric, (gs + 10)::numeric, '[)')",
            "pg_numrange::text AS pg_numrange",
        ),
        TypeCase(
            "pg_int4range",
            "int4range",
            "nvarchar(max)",
            "range",
            "int4range(gs::int, (gs + 10)::int, '[)')",
            "pg_int4range::text AS pg_int4range",
        ),
        TypeCase(
            "pg_int8range",
            "int8range",
            "nvarchar(max)",
            "range",
            "int8range(gs::bigint, (gs + 10)::bigint, '[)')",
            "pg_int8range::text AS pg_int8range",
        ),
        TypeCase(
            "pg_daterange",
            "daterange",
            "nvarchar(max)",
            "range",
            "daterange(date '2026-01-01' + gs::int, date '2026-01-01' + (gs + 1)::int, '[)')",
            "pg_daterange::text AS pg_daterange",
        ),
        TypeCase(
            "pg_tstzrange",
            "tstzrange",
            "nvarchar(max)",
            "range",
            "tstzrange(timestamptz '2026-01-01+00' + gs * interval '1 day', timestamptz '2026-01-02+00' + gs * interval '1 day', '[)')",
            "pg_tstzrange::text AS pg_tstzrange",
        ),
        TypeCase(
            "pg_int4multirange",
            "int4multirange",
            "nvarchar(max)",
            "multirange",
            "int4multirange(int4range(gs::int, (gs + 10)::int, '[)'))",
            "pg_int4multirange::text AS pg_int4multirange",
        ),
        TypeCase(
            "pg_point",
            "point",
            "nvarchar(max)",
            "geometric",
            "point(gs::double precision, (gs + 1)::double precision)",
            "pg_point::text AS pg_point",
        ),
        TypeCase(
            "pg_line",
            "line",
            "nvarchar(max)",
            "geometric",
            "line(point(0, 0), point(gs::double precision, 1))",
            "pg_line::text AS pg_line",
        ),
        TypeCase(
            "pg_lseg",
            "lseg",
            "nvarchar(max)",
            "geometric",
            "lseg(point(0, 0), point(gs::double precision, gs::double precision))",
            "pg_lseg::text AS pg_lseg",
        ),
        TypeCase(
            "pg_box",
            "box",
            "nvarchar(max)",
            "geometric",
            "box(point(gs::double precision, gs::double precision), point((gs + 1)::double precision, (gs + 1)::double precision))",
            "pg_box::text AS pg_box",
        ),
        TypeCase(
            "pg_path",
            "path",
            "nvarchar(max)",
            "geometric",
            "path(polygon '((0,0),(1,0),(1,1),(0,0))')",
            "pg_path::text AS pg_path",
        ),
        TypeCase(
            "pg_polygon",
            "polygon",
            "nvarchar(max)",
            "geometric",
            "polygon '((0,0),(1,0),(1,1),(0,0))'",
            "pg_polygon::text AS pg_polygon",
        ),
        TypeCase(
            "pg_circle",
            "circle",
            "nvarchar(max)",
            "geometric",
            "circle(point(gs::double precision, gs::double precision), 5.0)",
            "pg_circle::text AS pg_circle",
        ),
        TypeCase(
            "pg_inet",
            "inet",
            "nvarchar(max)",
            "network",
            "('10.0.0.' || (gs % 255)::int)::inet",
            "pg_inet::text AS pg_inet",
        ),
        TypeCase(
            "pg_cidr",
            "cidr",
            "nvarchar(max)",
            "network",
            "('10.' || (gs % 255)::int || '.0.0/16')::cidr",
            "pg_cidr::text AS pg_cidr",
        ),
        TypeCase(
            "pg_macaddr",
            "macaddr",
            "nvarchar(max)",
            "network",
            "('08:00:2b:' || substr(md5(gs::text),1,2) || ':' || substr(md5(gs::text),3,2) || ':' || substr(md5(gs::text),5,2))::macaddr",
            "pg_macaddr::text AS pg_macaddr",
        ),
        TypeCase("pg_bit", "bit(8)", "nvarchar(max)", "bit_string", "B'10101010'", "pg_bit::text AS pg_bit"),
        TypeCase(
            "pg_varbit",
            "varbit(16)",
            "nvarchar(max)",
            "bit_string",
            "(B'1010' || substring((gs % 2)::bit(1)::text::bit(1) from 1 for 1))::varbit(16)",
            "pg_varbit::text AS pg_varbit",
        ),
        TypeCase(
            "pg_tsvector",
            "tsvector",
            "nvarchar(max)",
            "text_search",
            "to_tsvector('simple', 'alpha beta ' || gs::text)",
            "pg_tsvector::text AS pg_tsvector",
        ),
        TypeCase(
            "pg_tsquery",
            "tsquery",
            "nvarchar(max)",
            "text_search",
            "to_tsquery('simple', 'alpha & beta')",
            "pg_tsquery::text AS pg_tsquery",
        ),
        TypeCase(
            "pg_lsn", "pg_lsn", "nvarchar(max)", "system", "('0/' || to_hex(gs::int))::pg_lsn", "pg_lsn::text AS pg_lsn"
        ),
        TypeCase("pg_oid", "oid", "nvarchar(max)", "system", "gs::oid", "pg_oid::text AS pg_oid"),
        TypeCase(
            "pg_regclass",
            "regclass",
            "nvarchar(max)",
            "system",
            "'pg_class'::regclass",
            "pg_regclass::text AS pg_regclass",
        ),
        TypeCase(
            "pg_regtype", "regtype", "nvarchar(max)", "system", "'integer'::regtype", "pg_regtype::text AS pg_regtype"
        ),
        TypeCase(
            "pg_enum",
            "typecert.mood",
            "nvarchar(max)",
            "custom",
            "CASE WHEN gs % 2 = 0 THEN 'happy' ELSE 'sad' END",
            "pg_enum::text AS pg_enum",
        ),
        TypeCase(
            "pg_domain", "typecert.positive_int", "nvarchar(max)", "custom", "gs::int", "pg_domain::text AS pg_domain"
        ),
        TypeCase(
            "pg_composite",
            "typecert.address_type",
            "nvarchar(max)",
            "custom",
            "ROW('street-' || gs::text, gs::int)",
            "pg_composite::text AS pg_composite",
        ),
        TypeCase(
            "pg_hstore",
            "hstore",
            "nvarchar(max)",
            "extension",
            "hstore(ARRAY['id', gs::text, 'kind', 'hstore'])",
            "pg_hstore::text AS pg_hstore",
        ),
    ]


def mssql_cases_matrix() -> list[TypeCase]:
    return [
        TypeCase("id", "bigint", "String", "control", "CAST(v.n AS bigint)", "CONVERT(varchar(64), [id]) AS [id]"),
        TypeCase(
            "ms_bit", "bit", "String", "boolean", "CAST(v.n % 2 AS bit)", "CONVERT(varchar(1), [ms_bit]) AS [ms_bit]"
        ),
        TypeCase(
            "ms_tinyint",
            "tinyint",
            "String",
            "numeric",
            "CAST(v.n % 255 AS tinyint)",
            "CONVERT(varchar(16), [ms_tinyint]) AS [ms_tinyint]",
        ),
        TypeCase(
            "ms_smallint",
            "smallint",
            "String",
            "numeric",
            "CAST(v.n AS smallint)",
            "CONVERT(varchar(16), [ms_smallint]) AS [ms_smallint]",
        ),
        TypeCase(
            "ms_int", "int", "String", "numeric", "CAST(v.n AS int)", "CONVERT(varchar(32), [ms_int]) AS [ms_int]"
        ),
        TypeCase(
            "ms_bigint",
            "bigint",
            "String",
            "numeric",
            "CAST(v.n * 100000 AS bigint)",
            "CONVERT(varchar(64), [ms_bigint]) AS [ms_bigint]",
        ),
        TypeCase(
            "ms_decimal",
            "decimal(38,10)",
            "String",
            "numeric",
            "CAST(v.n AS decimal(38,10)) / CAST(7 AS decimal(38,10))",
            "CONVERT(varchar(80), [ms_decimal]) AS [ms_decimal]",
        ),
        TypeCase(
            "ms_numeric",
            "numeric(20,4)",
            "String",
            "numeric",
            "CAST(v.n AS numeric(20,4)) / CAST(3 AS numeric(20,4))",
            "CONVERT(varchar(80), [ms_numeric]) AS [ms_numeric]",
        ),
        TypeCase(
            "ms_money",
            "money",
            "String",
            "money",
            "CAST(v.n AS money) / 100",
            "CONVERT(varchar(80), [ms_money]) AS [ms_money]",
        ),
        TypeCase(
            "ms_smallmoney",
            "smallmoney",
            "String",
            "money",
            "CAST(v.n AS smallmoney) / 100",
            "CONVERT(varchar(80), [ms_smallmoney]) AS [ms_smallmoney]",
        ),
        TypeCase(
            "ms_float",
            "float",
            "String",
            "numeric",
            "CAST(v.n AS float) / 9",
            "CONVERT(varchar(80), [ms_float], 3) AS [ms_float]",
        ),
        TypeCase(
            "ms_real",
            "real",
            "String",
            "numeric",
            "CAST(v.n AS real) / 5",
            "CONVERT(varchar(80), [ms_real], 3) AS [ms_real]",
        ),
        TypeCase(
            "ms_date",
            "date",
            "String",
            "date_time",
            "DATEADD(day, v.n, CONVERT(date, '2026-01-01'))",
            "CONVERT(varchar(32), [ms_date], 23) AS [ms_date]",
        ),
        TypeCase(
            "ms_time",
            "time(6)",
            "String",
            "date_time",
            "DATEADD(second, v.n, CONVERT(time(6), '00:00:00'))",
            "CONVERT(varchar(32), [ms_time], 114) AS [ms_time]",
        ),
        TypeCase(
            "ms_datetime",
            "datetime",
            "String",
            "date_time",
            "DATEADD(second, v.n, CONVERT(datetime, '2026-01-01T00:00:00'))",
            "CONVERT(varchar(32), [ms_datetime], 126) AS [ms_datetime]",
        ),
        TypeCase(
            "ms_smalldatetime",
            "smalldatetime",
            "String",
            "date_time",
            "DATEADD(minute, v.n, CONVERT(smalldatetime, '2026-01-01T00:00:00'))",
            "CONVERT(varchar(32), [ms_smalldatetime], 126) AS [ms_smalldatetime]",
        ),
        TypeCase(
            "ms_datetime2",
            "datetime2(6)",
            "String",
            "date_time",
            "DATEADD(second, v.n, CONVERT(datetime2(6), '2026-01-01T00:00:00'))",
            "CONVERT(varchar(40), [ms_datetime2], 126) AS [ms_datetime2]",
        ),
        TypeCase(
            "ms_datetimeoffset",
            "datetimeoffset(6)",
            "String",
            "date_time",
            "DATEADD(second, v.n, CONVERT(datetimeoffset(6), '2026-01-01 00:00:00.000000 +00:00'))",
            "CONVERT(varchar(48), [ms_datetimeoffset], 127) AS [ms_datetimeoffset]",
        ),
        TypeCase(
            "ms_char",
            "char(16)",
            "String",
            "text",
            "CONVERT(char(16), CONCAT('char-', v.n))",
            "RTRIM([ms_char]) AS [ms_char]",
        ),
        TypeCase(
            "ms_varchar",
            "varchar(128)",
            "String",
            "text",
            "CONVERT(varchar(128), CONCAT('varchar-', v.n))",
            "[ms_varchar] AS [ms_varchar]",
        ),
        TypeCase(
            "ms_text",
            "text",
            "String",
            "deprecated",
            "CONVERT(varchar(max), CONCAT('text-', v.n))",
            "CONVERT(varchar(max), [ms_text]) AS [ms_text]",
        ),
        TypeCase(
            "ms_nchar",
            "nchar(16)",
            "String",
            "text",
            "CONVERT(nchar(16), CONCAT(N'nchar-', v.n))",
            "RTRIM([ms_nchar]) AS [ms_nchar]",
        ),
        TypeCase(
            "ms_nvarchar",
            "nvarchar(128)",
            "String",
            "text",
            "CONVERT(nvarchar(128), CONCAT(N'nvarchar-', v.n))",
            "[ms_nvarchar] AS [ms_nvarchar]",
        ),
        TypeCase(
            "ms_ntext",
            "ntext",
            "String",
            "deprecated",
            "CONVERT(nvarchar(max), CONCAT(N'ntext-', v.n))",
            "CONVERT(nvarchar(max), [ms_ntext]) AS [ms_ntext]",
        ),
        TypeCase(
            "ms_binary",
            "binary(8)",
            "String",
            "binary",
            "CONVERT(binary(8), v.n)",
            "CONVERT(varchar(max), [ms_binary], 2) AS [ms_binary]",
        ),
        TypeCase(
            "ms_varbinary",
            "varbinary(32)",
            "String",
            "binary",
            "CONVERT(varbinary(32), HASHBYTES('MD5', CONVERT(varchar(32), v.n)))",
            "CONVERT(varchar(max), [ms_varbinary], 2) AS [ms_varbinary]",
        ),
        TypeCase(
            "ms_varbinary_max",
            "varbinary(max)",
            "String",
            "binary",
            "CONVERT(varbinary(max), HASHBYTES('SHA2_256', CONVERT(varchar(32), v.n)))",
            "CONVERT(varchar(max), [ms_varbinary_max], 2) AS [ms_varbinary_max]",
        ),
        TypeCase(
            "ms_image",
            "image",
            "String",
            "deprecated",
            "CONVERT(varbinary(max), HASHBYTES('MD5', CONVERT(varchar(32), v.n)))",
            "CONVERT(varchar(max), CONVERT(varbinary(max), [ms_image]), 2) AS [ms_image]",
        ),
        TypeCase(
            "ms_uniqueidentifier",
            "uniqueidentifier",
            "String",
            "uuid",
            "CONVERT(uniqueidentifier, CONCAT('00000000-0000-0000-0000-', RIGHT(CONCAT('000000000000', v.n), 12)))",
            "CONVERT(varchar(36), [ms_uniqueidentifier]) AS [ms_uniqueidentifier]",
        ),
        TypeCase(
            "ms_xml",
            "xml",
            "String",
            "semi_structured",
            "CONVERT(xml, CONCAT('<row id=\"', v.n, '\"/>'))",
            "CONVERT(nvarchar(max), [ms_xml]) AS [ms_xml]",
        ),
        TypeCase(
            "ms_sql_variant",
            "sql_variant",
            "String",
            "system",
            "CONVERT(sql_variant, CONCAT('variant-', v.n))",
            "CONVERT(nvarchar(max), [ms_sql_variant]) AS [ms_sql_variant]",
        ),
        TypeCase(
            "ms_hierarchyid",
            "hierarchyid",
            "String",
            "system",
            "hierarchyid::Parse(CONCAT('/', v.n, '/'))",
            "[ms_hierarchyid].ToString() AS [ms_hierarchyid]",
        ),
        TypeCase(
            "ms_geometry",
            "geometry",
            "String",
            "spatial",
            "geometry::STGeomFromText(CONCAT('POINT(', v.n, ' ', v.n + 1, ')'), 0)",
            "[ms_geometry].STAsText() AS [ms_geometry]",
        ),
        TypeCase(
            "ms_geography",
            "geography",
            "String",
            "spatial",
            "geography::STGeomFromText(CONCAT('POINT(', v.n % 90, ' ', v.n % 45, ')'), 4326)",
            "[ms_geography].STAsText() AS [ms_geography]",
        ),
        TypeCase(
            "ms_rowversion",
            "rowversion",
            "String",
            "system",
            "",
            "SUBSTRING(sys.fn_varbintohexstr([ms_rowversion]), 3, 64) AS [ms_rowversion]",
            insertable=False,
            notes="auto-generated by SQL Server",
        ),
    ]


def certify_postgres_to_mssql(
    pg: PostgresConnector,
    mssql: MSSQLConnector,
    args: argparse.Namespace,
    cases: list[TypeCase],
) -> dict[str, Any]:
    metrics: list[StageMetric] = []
    _, metric = timed(
        "prepare_postgres_exhaustive_source", lambda: prepare_postgres_source(pg, args.rows, cases), args.rows
    )
    metrics.append(metric)
    _, metric = timed(
        "prepare_mssql_postgres_type_target", lambda: prepare_mssql_text_target(mssql, "typecert", "pg_to_mssql", cases)
    )
    metrics.append(metric)
    with tempfile.NamedTemporaryFile(prefix="dpone_pg_typecert_", suffix=".bcp", delete=False) as tmp:
        file_path = tmp.name
    try:
        export_result, metric = timed(
            "postgres_copy_canonical_export",
            lambda: export_postgres_cases(pg, cases, file_path),
            args.rows,
        )
        metrics.append(metric)
        imported_rows, metric = timed(
            "mssql_bcp_import_pg_type_matrix",
            lambda: mssql.bcp_import(
                "typecert",
                "pg_to_mssql",
                file_path,
                options=BcpOptions(bcp_path=args.bcp_path, trust_server_certificate=True),
            ),
            args.rows,
        )
        metrics.append(metric)
        source_hash = hash_postgres_export(pg, cases)
        target_hash = hash_mssql_rows(mssql, "typecert", "pg_to_mssql", [case.name for case in cases])
        return {
            "passed": imported_rows == args.rows and source_hash == target_hash,
            "rows": args.rows,
            "type_count": len(cases),
            "imported_rows": imported_rows,
            "source_hash": source_hash,
            "target_hash": target_hash,
            "metrics": [asdict(item) for item in metrics],
            "export_result": export_result,
            "types": [case_to_dict(case) for case in cases],
        }
    finally:
        Path(file_path).unlink(missing_ok=True)


def certify_mssql_to_clickhouse(
    mssql: MSSQLConnector,
    clickhouse: ClickHouseConnector,
    args: argparse.Namespace,
    cases: list[TypeCase],
) -> dict[str, Any]:
    metrics: list[StageMetric] = []
    _, metric = timed(
        "prepare_mssql_exhaustive_source", lambda: prepare_mssql_source(mssql, args.rows, cases), args.rows
    )
    metrics.append(metric)
    _, metric = timed(
        "prepare_clickhouse_type_target",
        lambda: prepare_clickhouse_target(clickhouse, args.clickhouse_database, "mssql_to_clickhouse", cases),
    )
    metrics.append(metric)
    with tempfile.NamedTemporaryFile(prefix="dpone_mssql_typecert_", suffix=".tsv", delete=False) as tmp:
        file_path = tmp.name
    try:
        exported_rows, metric = timed(
            "mssql_bcp_queryout_canonical_export",
            lambda: mssql.bcp_queryout(
                mssql_export_query(cases),
                file_path,
                options=BcpOptions(bcp_path=args.bcp_path, trust_server_certificate=True),
            ),
            args.rows,
        )
        metrics.append(metric)
        file_hash = hash_file_lines(file_path)
        inserted_rows, metric = timed(
            "clickhouse_http_insert_tsv",
            lambda: clickhouse_http_insert(args, "mssql_to_clickhouse", [case.name for case in cases], file_path),
            args.rows,
        )
        metrics.append(metric)
        target_hash = hash_clickhouse_rows(
            clickhouse, args.clickhouse_database, "mssql_to_clickhouse", [case.name for case in cases]
        )
        return {
            "passed": exported_rows == args.rows and inserted_rows == args.rows and file_hash == target_hash,
            "rows": args.rows,
            "type_count": len(cases),
            "exported_rows": exported_rows,
            "inserted_rows": inserted_rows,
            "source_export_hash": file_hash,
            "target_hash": target_hash,
            "metrics": [asdict(item) for item in metrics],
            "types": [case_to_dict(case) for case in cases],
        }
    finally:
        Path(file_path).unlink(missing_ok=True)


def prepare_postgres_source(pg: PostgresConnector, rows: int, cases: list[TypeCase]) -> dict[str, Any]:
    pg.execute_query("DROP SCHEMA IF EXISTS typecert CASCADE")
    pg.execute_query("CREATE SCHEMA typecert")
    pg.execute_query("CREATE EXTENSION IF NOT EXISTS hstore")
    pg.execute_query("CREATE TYPE typecert.mood AS ENUM ('sad', 'happy')")
    pg.execute_query("CREATE DOMAIN typecert.positive_int AS integer CHECK (VALUE > 0)")
    pg.execute_query("CREATE TYPE typecert.address_type AS (street text, house integer)")
    select_sql = ",\n            ".join(case.source_sql for case in cases)
    pg.execute_query(
        f"""
        CREATE TABLE typecert.pg_source AS
        SELECT
            {select_sql}
        FROM generate_series(1, {int(rows)}) AS gs
        """
    )
    return {"rows": rows, "types": len(cases)}


def prepare_mssql_text_target(mssql: MSSQLConnector, schema: str, table: str, cases: list[TypeCase]) -> dict[str, Any]:
    mssql.execute_query(f"IF SCHEMA_ID('{schema}') IS NULL EXEC('CREATE SCHEMA [{schema}]')")
    mssql.execute_query(f"DROP TABLE IF EXISTS [{schema}].[{table}]")
    columns_sql = ",\n            ".join(f"[{case.name}] {case.target_type} NULL" for case in cases)
    mssql.execute_query(f"CREATE TABLE [{schema}].[{table}] ({columns_sql})")
    return {"types": len(cases)}


def export_postgres_cases(pg: PostgresConnector, cases: list[TypeCase], file_path: str) -> dict[str, Any]:
    export_sql = ", ".join(case.export_expression for case in cases)
    return pg.copy_to_file(
        f"SELECT {export_sql} FROM typecert.pg_source ORDER BY id",
        file_path,
        format="MSSQL_DELIMITED",
        compress=False,
    )


def hash_postgres_export(pg: PostgresConnector, cases: list[TypeCase]) -> str:
    export_sql = " || E'\\t' || ".join(f"COALESCE(({case.export_value_sql})::text, '')" for case in cases)
    rows = pg.get_records(f"SELECT {export_sql} AS line FROM typecert.pg_source ORDER BY id", as_dict=True)
    return hash_lines(str(row["line"]) for row in rows)


def prepare_mssql_source(mssql: MSSQLConnector, rows: int, cases: list[TypeCase]) -> dict[str, Any]:
    mssql.execute_query("IF SCHEMA_ID('typecert') IS NULL EXEC('CREATE SCHEMA [typecert]')")
    mssql.execute_query("DROP TABLE IF EXISTS [typecert].[mssql_source]")
    ddl_columns = ",\n            ".join(
        f"[{case.name}] {case.source_type} {'NULL' if case.name != 'id' else 'NOT NULL'}" for case in cases
    )
    mssql.execute_query(f"CREATE TABLE [typecert].[mssql_source] ({ddl_columns})")
    insert_cases = [case for case in cases if case.insertable]
    column_sql = ", ".join(f"[{case.name}]" for case in insert_cases)
    select_sql = ", ".join(case.source_expression for case in insert_cases)
    values_sql = " UNION ALL ".join(f"SELECT {i} AS n" for i in range(1, rows + 1))
    mssql.execute_query(
        f"""
        INSERT INTO [typecert].[mssql_source] ({column_sql})
        SELECT {select_sql}
        FROM ({values_sql}) AS v
        """
    )
    return {"rows": rows, "types": len(cases)}


def prepare_clickhouse_target(
    clickhouse: ClickHouseConnector, database: str, table: str, cases: list[TypeCase]
) -> dict[str, Any]:
    clickhouse.execute_query(f"CREATE DATABASE IF NOT EXISTS `{database}`")
    clickhouse.execute_query(f"DROP TABLE IF EXISTS `{database}`.`{table}`")
    columns_sql = ", ".join(f"`{case.name}` String" for case in cases)
    clickhouse.execute_query(f"CREATE TABLE `{database}`.`{table}` ({columns_sql}) ENGINE = MergeTree ORDER BY tuple()")
    return {"types": len(cases)}


def mssql_export_query(cases: list[TypeCase]) -> str:
    export_sql = ", ".join(case.export_expression for case in cases)
    return f"SELECT {export_sql} FROM [typecert].[mssql_source] ORDER BY [typecert].[mssql_source].[id]"


def clickhouse_http_insert(args: argparse.Namespace, table: str, columns: list[str], file_path: str) -> int:
    column_sql = ", ".join(f"`{column}`" for column in columns)
    query = f"INSERT INTO `{args.clickhouse_database}`.`{table}` ({column_sql}) FORMAT TabSeparated"
    url = f"http://{args.clickhouse_host}:{args.clickhouse_http_port}/"
    with open(file_path, "rb") as handle:
        response = requests.post(
            url,
            params={"query": query, "database": args.clickhouse_database},
            data=handle,
            auth=(args.clickhouse_user, args.clickhouse_password),
            timeout=3600,
        )
    if response.status_code >= 400:
        raise RuntimeError(f"ClickHouse HTTP insert failed {response.status_code}: {response.text}")
    return sum(1 for _ in open(file_path, encoding="utf-8"))


def hash_mssql_rows(mssql: MSSQLConnector, schema: str, table: str, columns: list[str]) -> str:
    concat_sql = " + CHAR(9) + ".join(f"ISNULL(CONVERT(nvarchar(max), [{column}]), N'')" for column in columns)
    rows = mssql.get_records(
        f"SELECT {concat_sql} AS line FROM [{schema}].[{table}] ORDER BY TRY_CONVERT(bigint, [id])", as_dict=True
    )
    return hash_lines(str(row["line"]) for row in rows)


def hash_clickhouse_rows(clickhouse: ClickHouseConnector, database: str, table: str, columns: list[str]) -> str:
    concat_sql = " || '\\t' || ".join(f"ifNull(`{column}`, '')" for column in columns)
    rows = clickhouse.get_records(
        f"SELECT {concat_sql} AS line FROM `{database}`.`{table}` ORDER BY toInt64(`id`)", as_dict=True
    )
    return hash_lines(str(row["line"]) for row in rows)


def hash_file_lines(file_path: str) -> str:
    with open(file_path, encoding="utf-8") as handle:
        return hash_lines(line.rstrip("\n") for line in handle)


def hash_lines(lines: Iterable[str]) -> str:
    digest = hashlib.sha256()
    count = 0
    for line in lines:
        digest.update(line.encode("utf-8"))
        digest.update(b"\n")
        count += 1
    return f"sha256:{digest.hexdigest()}:rows:{count}"


def timed(name: str, fn, rows: int | None = None) -> tuple[Any, StageMetric]:
    started = time.perf_counter()
    result = fn()
    seconds = time.perf_counter() - started
    bytes_processed = result.get("total_bytes") if isinstance(result, dict) else None
    return result, StageMetric(
        name=name,
        seconds=round(seconds, 3),
        rows=rows,
        bytes_processed=bytes_processed,
        rows_per_second=round(rows / seconds, 2) if rows is not None and seconds > 0 else None,
        mib_per_second=round((bytes_processed / 1024 / 1024) / seconds, 2)
        if bytes_processed is not None and seconds > 0
        else None,
    )


def case_to_dict(case: TypeCase) -> dict[str, Any]:
    payload = asdict(case)
    payload.pop("source_expression", None)
    payload.pop("export_expression", None)
    return payload


def now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
