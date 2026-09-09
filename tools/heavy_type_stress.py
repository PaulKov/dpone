#!/usr/bin/env python
"""Run PostgreSQL -> MSSQL heavy-type stress checks.

The harness is intentionally resource-aware. A 50M-row / 10GB / 100-column run
needs much more local free space than the final table size because source table,
COPY artifact, SQL Server storage, and logs coexist during the run. When the
preflight fails, the script writes a structured JSON artifact instead of starting
a destructive partial load.
"""

from __future__ import annotations

import argparse
import json
import shutil
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dpone._compat import UTC
from dpone.runtime.connectors import MSSQLConnector, PostgresConnector
from dpone.runtime.connectors.mssql_bulk import BcpOptions

GIB = 1024**3
ACTOR = "Codex GPT-5"


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    name: str
    pg_expr: str
    pg_type: str
    mssql_type: str
    export_expr: str | None = None

    @property
    def source_sql(self) -> str:
        return f"({self.pg_expr})::{self.pg_type} AS {self.name}"

    @property
    def export_sql(self) -> str:
        return self.export_expr or self.name


@dataclass(frozen=True, slots=True)
class StageMetric:
    name: str
    seconds: float
    rows: int | None = None
    bytes_processed: int | None = None
    rows_per_second: float | None = None
    mib_per_second: float | None = None


def timed(name: str, fn, *, rows: int | None = None) -> tuple[Any, StageMetric]:
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="dpone 100-column heavy-type Postgres -> MSSQL stress harness")
    parser.add_argument("--rows", type=int, default=50_000_000)
    parser.add_argument("--target-size-gib", type=float, default=10.0)
    parser.add_argument("--required-free-multiplier", type=float, default=3.5)
    parser.add_argument("--allow-low-disk", action="store_true")
    parser.add_argument("--json-output", required=True)
    parser.add_argument("--artifact-label", default="heavy_type_stress")
    parser.add_argument("--schema", default="heavy")
    parser.add_argument("--source-table", default="heavy_type_source")
    parser.add_argument("--target-table", default="heavy_type_target")
    parser.add_argument("--batch-size", type=int, default=100_000)
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
    parser.add_argument("--bcp-path", default="bcp")
    return parser.parse_args()


def build_columns() -> list[ColumnSpec]:
    specs: list[ColumnSpec] = [ColumnSpec("id", "gs", "bigint", "bigint")]
    for i in range(1, 5):
        specs.append(
            ColumnSpec(
                f"bool_{i:02d}",
                f"(gs + {i}) % 2 = 0",
                "boolean",
                "bit",
                f"CASE WHEN bool_{i:02d} THEN 1 ELSE 0 END AS bool_{i:02d}",
            )
        )
    for i in range(1, 11):
        specs.append(ColumnSpec(f"int_{i:02d}", f"(gs * {i}) % 2147483647", "integer", "int"))
    for i in range(1, 6):
        specs.append(ColumnSpec(f"smallint_{i:02d}", f"(gs + {i}) % 32767", "smallint", "smallint"))
    for i in range(1, 6):
        specs.append(ColumnSpec(f"tinyint_{i:02d}", f"(gs + {i}) % 255", "smallint", "tinyint"))
    for i in range(1, 6):
        specs.append(ColumnSpec(f"bigint_{i:02d}", f"gs * {i} * 100000", "bigint", "bigint"))
    for i in range(1, 11):
        specs.append(ColumnSpec(f"decimal_{i:02d}", f"gs::numeric / {i + 10}.0", "numeric(38,10)", "decimal(38,10)"))
    for i in range(1, 7):
        specs.append(ColumnSpec(f"float_{i:02d}", f"gs::double precision / {i + 1}.0", "double precision", "float"))
    for i in range(1, 5):
        specs.append(ColumnSpec(f"real_{i:02d}", f"gs::real / {i + 1}.0", "real", "real"))
    for i in range(1, 5):
        target = "money" if i % 2 else "smallmoney"
        specs.append(ColumnSpec(f"money_{i:02d}", "(gs % 100000)::numeric / 100.0", "numeric(19,4)", target))
    for i in range(1, 6):
        specs.append(ColumnSpec(f"date_{i:02d}", f"date '2026-01-01' + ((gs + {i}) % 365)::int", "date", "date"))
    for i in range(1, 6):
        specs.append(
            ColumnSpec(
                f"time_{i:02d}", f"time '00:00:00' + ((gs + {i}) % 86400) * interval '1 second'", "time(6)", "time(6)"
            )
        )
    for i in range(1, 5):
        specs.append(
            ColumnSpec(
                f"datetime2_{i:02d}",
                f"timestamp '2026-01-01 00:00:00' + (gs + {i}) * interval '1 second'",
                "timestamp(6)",
                "datetime2(6)",
            )
        )
    for i in range(1, 5):
        specs.append(
            ColumnSpec(
                f"datetimeoffset_{i:02d}",
                f"timestamptz '2026-01-01 00:00:00+00' + (gs + {i}) * interval '1 second'",
                "timestamptz",
                "datetimeoffset(6)",
                f"to_char(datetimeoffset_{i:02d} AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI:SS.US') || ' +00:00' AS datetimeoffset_{i:02d}",
            )
        )
    for i in range(1, 5):
        specs.append(
            ColumnSpec(
                f"uuid_{i:02d}",
                f"('00000000-0000-0000-0000-' || lpad(((gs + {i}) % 999999999999)::text, 12, '0'))::uuid",
                "uuid",
                "uniqueidentifier",
            )
        )
    specs.extend(
        [
            ColumnSpec(
                "json_01",
                "jsonb_build_object('id', gs, 'kind', 'alpha', 'flag', gs % 2 = 0)",
                "jsonb",
                "nvarchar(max)",
                "json_01::text AS json_01",
            ),
            ColumnSpec(
                "json_02",
                "json_build_object('id', gs, 'kind', 'beta')",
                "json",
                "nvarchar(max)",
                "json_02::text AS json_02",
            ),
            ColumnSpec(
                "xml_01",
                "xmlelement(name row, xmlattributes(gs as id))",
                "xml",
                "xml",
                "xmlserialize(CONTENT xml_01 AS text) AS xml_01",
            ),
            ColumnSpec(
                "xml_02",
                "xmlelement(name payload, xmlattributes((gs % 10) as bucket), 'value')",
                "xml",
                "xml",
                "xmlserialize(CONTENT xml_02 AS text) AS xml_02",
            ),
            ColumnSpec(
                "inet_01",
                "('10.0.' || ((gs / 256) % 255)::int || '.' || (gs % 255)::int)::inet",
                "inet",
                "nvarchar(64)",
                "inet_01::text AS inet_01",
            ),
            ColumnSpec(
                "cidr_01",
                "('10.' || (gs % 255)::int || '.0.0/16')::cidr",
                "cidr",
                "nvarchar(64)",
                "cidr_01::text AS cidr_01",
            ),
            ColumnSpec(
                "macaddr_01",
                "('08:00:2b:' || substr(md5(gs::text), 1, 2) || ':' || substr(md5(gs::text), 3, 2) || ':' || substr(md5(gs::text), 5, 2))::macaddr",
                "macaddr",
                "nvarchar(32)",
                "macaddr_01::text AS macaddr_01",
            ),
            ColumnSpec(
                "interval_01",
                "make_interval(secs => (gs % 86400)::int)",
                "interval",
                "nvarchar(64)",
                "interval_01::text AS interval_01",
            ),
        ]
    )
    for i in range(1, 9):
        specs.append(
            ColumnSpec(
                f"binary_{i:02d}",
                f"decode(md5(gs::text || '-{i}'), 'hex')",
                "bytea",
                "varbinary(max)",
                f"encode(binary_{i:02d}, 'hex') AS binary_{i:02d}",
            )
        )
    for i in range(1, 7):
        specs.append(ColumnSpec(f"char_{i:02d}", f"rpad('c{i}-' || gs::text, 32, 'x')", "char(32)", "char(32)"))
    for i in range(1, 9):
        specs.append(ColumnSpec(f"text_{i:02d}", f"repeat(md5(gs::text || '-txt-{i}'), 4)", "text", "nvarchar(256)"))
    for i in range(1, 5):
        specs.append(
            ColumnSpec(f"wide_text_{i:02d}", f"repeat(md5(gs::text || '-wide-{i}'), 16)", "text", "nvarchar(max)")
        )
    if len(specs) < 100:
        raise RuntimeError(f"Column spec underflow: {len(specs)}")
    return specs[:100]


def main() -> int:
    args = parse_args()
    started_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    output_path = Path(args.json_output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    columns = build_columns()
    preflight = build_preflight(args, columns)
    if not args.allow_low_disk and not preflight["sufficient_disk"]:
        artifact = base_artifact(args, columns, started_at)
        artifact.update(
            {
                "status": "blocked",
                "result": "not_run",
                "reason": "insufficient_local_disk_for_requested_50m_10gb_stress",
                "preflight": preflight,
                "finished_at": now_utc(),
            }
        )
        write_artifact(output_path, artifact)
        print(json.dumps(artifact, indent=2, ensure_ascii=False))
        return 2

    artifact = base_artifact(args, columns, started_at)
    metrics: list[StageMetric] = []
    pg = PostgresConnector(
        host=args.pg_host,
        port=args.pg_port,
        database=args.pg_database,
        user=args.pg_user,
        password=args.pg_password,
        application_name="dpone-heavy-type-stress-pg",
    )
    mssql = MSSQLConnector(
        host=args.mssql_host,
        port=args.mssql_port,
        database=args.mssql_database,
        user=args.mssql_user,
        password=args.mssql_password,
        driver=args.mssql_driver,
        trust_server_certificate=True,
        application_name="dpone-heavy-type-stress-mssql",
        bcp_path=args.bcp_path,
        query_timeout=0,
    )
    tmp_file: str | None = None
    try:
        _, metric = timed(
            "prepare_postgres_100_column_source", lambda: prepare_postgres(pg, args, columns), rows=args.rows
        )
        metrics.append(metric)
        _, metric = timed("prepare_mssql_100_column_target", lambda: prepare_mssql(mssql, args, columns))
        metrics.append(metric)
        fd, tmp_file = tempfile.mkstemp(prefix="dpone_heavy_type_", suffix=".bcp")
        Path(tmp_file).unlink(missing_ok=True)
        export_result, metric = timed(
            "postgres_copy_to_bcp_file",
            lambda: export_postgres(pg, args, columns, tmp_file),
            rows=args.rows,
        )
        metrics.append(metric)
        imported_rows, metric = timed(
            "mssql_bcp_import",
            lambda: mssql.bcp_import(
                args.schema,
                args.target_table,
                tmp_file,
                options=BcpOptions(
                    bcp_path=args.bcp_path,
                    batch_size=args.batch_size,
                    trust_server_certificate=True,
                ),
            ),
            rows=args.rows,
        )
        metrics.append(metric)
        verification = verify(pg, mssql, args)
        artifact.update(
            {
                "status": "passed" if verification["passed"] and imported_rows == args.rows else "failed",
                "result": "passed" if verification["passed"] and imported_rows == args.rows else "failed",
                "preflight": preflight,
                "metrics": [asdict(item) for item in metrics],
                "export_result": export_result,
                "imported_rows": imported_rows,
                "verification": verification,
                "finished_at": now_utc(),
            }
        )
        write_artifact(output_path, artifact)
        print(json.dumps(artifact, indent=2, ensure_ascii=False))
        return 0 if artifact["status"] == "passed" else 1
    except Exception as exc:
        artifact.update(
            {
                "status": "failed",
                "result": "failed",
                "error_type": exc.__class__.__name__,
                "error": str(exc),
                "preflight": preflight,
                "metrics": [asdict(item) for item in metrics],
                "finished_at": now_utc(),
            }
        )
        write_artifact(output_path, artifact)
        print(json.dumps(artifact, indent=2, ensure_ascii=False))
        return 1
    finally:
        if tmp_file:
            Path(tmp_file).unlink(missing_ok=True)
        pg.close()
        mssql.close()


def base_artifact(args: argparse.Namespace, columns: list[ColumnSpec], started_at: str) -> dict[str, Any]:
    return {
        "artifact_schema": "dpone.test-artifact.v1",
        "artifact_type": "stress.heavy_type_postgres_to_mssql",
        "artifact_label": args.artifact_label,
        "created_by": ACTOR,
        "executed_by_os_user": "macbook",
        "started_at": started_at,
        "requested_rows": args.rows,
        "requested_target_size_gib": args.target_size_gib,
        "column_count": len(columns),
        "source_system": "postgresql",
        "target_system": "mssql",
        "load_path": "PostgreSQL COPY TO STDOUT -> local bcp character file -> SQL Server bcp in",
        "native_methods": ["PostgreSQL COPY", "Microsoft bcp"],
        "type_matrix": [
            {"name": column.name, "postgres_type": column.pg_type, "mssql_type": column.mssql_type}
            for column in columns
        ],
    }


def build_preflight(args: argparse.Namespace, columns: list[ColumnSpec]) -> dict[str, Any]:
    usage = shutil.disk_usage(Path.cwd())
    required_free_gib = args.target_size_gib * args.required_free_multiplier
    return {
        "available_free_gib": round(usage.free / GIB, 3),
        "required_free_gib": round(required_free_gib, 3),
        "required_free_multiplier": args.required_free_multiplier,
        "sufficient_disk": usage.free >= required_free_gib * GIB,
        "column_count": len(columns),
        "why_multiplier": "source table, export artifact, SQL Server data/log files, Docker overlay, and cleanup headroom coexist during stress",
    }


def prepare_postgres(
    connector: PostgresConnector, args: argparse.Namespace, columns: list[ColumnSpec]
) -> dict[str, Any]:
    connector.execute_query(f"CREATE SCHEMA IF NOT EXISTS {args.schema}")
    connector.execute_query(f"DROP TABLE IF EXISTS {args.schema}.{args.source_table}")
    select_sql = ",\n            ".join(column.source_sql for column in columns)
    connector.execute_query(
        f"""
        CREATE UNLOGGED TABLE {args.schema}.{args.source_table} AS
        SELECT
            {select_sql}
        FROM generate_series(1, {int(args.rows)}) AS gs
        """
    )
    return {"rows": args.rows}


def prepare_mssql(connector: MSSQLConnector, args: argparse.Namespace, columns: list[ColumnSpec]) -> dict[str, Any]:
    connector.execute_query(f"IF SCHEMA_ID('{args.schema}') IS NULL EXEC('CREATE SCHEMA [{args.schema}]')")
    connector.execute_query(f"DROP TABLE IF EXISTS [{args.schema}].[{args.target_table}]")
    ddl = ",\n            ".join(
        f"[{column.name}] {column.mssql_type} {'NOT NULL' if column.name == 'id' else 'NULL'}" for column in columns
    )
    connector.execute_query(
        f"""
        CREATE TABLE [{args.schema}].[{args.target_table}] (
            {ddl}
        )
        """
    )
    return {"columns": len(columns)}


def export_postgres(
    pg: PostgresConnector, args: argparse.Namespace, columns: list[ColumnSpec], output_path: str
) -> dict[str, Any]:
    select_sql = ", ".join(column.export_sql for column in columns)
    query = f"SELECT {select_sql} FROM {args.schema}.{args.source_table} ORDER BY id"
    return pg.copy_to_file(query, output_path, format="MSSQL_DELIMITED", compress=False)


def verify(pg: PostgresConnector, mssql: MSSQLConnector, args: argparse.Namespace) -> dict[str, Any]:
    pg_rows = pg.get_records(
        f"""
        SELECT
            COUNT(*)::bigint AS count_rows,
            SUM(id)::numeric AS sum_id,
            SUM(int_01)::numeric AS sum_int_01,
            SUM(CASE WHEN bool_01 THEN 1 ELSE 0 END)::numeric AS sum_bool_01,
            SUM(length(text_01))::numeric AS sum_text_01_len,
            MIN(uuid_01::text) AS min_uuid_01,
            MAX(uuid_01::text) AS max_uuid_01
        FROM {args.schema}.{args.source_table}
        """,
        as_dict=True,
    )[0]
    ms_rows = mssql.get_records(
        f"""
        SELECT
            COUNT_BIG(*) AS count_rows,
            SUM(CAST([id] AS decimal(38,0))) AS sum_id,
            SUM(CAST([int_01] AS decimal(38,0))) AS sum_int_01,
            SUM(CAST([bool_01] AS int)) AS sum_bool_01,
            SUM(LEN([text_01])) AS sum_text_01_len,
            MIN(CONVERT(nvarchar(36), [uuid_01])) AS min_uuid_01,
            MAX(CONVERT(nvarchar(36), [uuid_01])) AS max_uuid_01
        FROM [{args.schema}].[{args.target_table}]
        """,
        as_dict=True,
    )[0]
    pg_normalized = normalize_verification_row(pg_rows)
    ms_normalized = normalize_verification_row(ms_rows)
    source_columns = pg.get_records(
        """
        SELECT COUNT(*) AS column_count
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """,
        (args.schema, args.source_table),
        as_dict=True,
    )[0]["column_count"]
    target_columns = mssql.get_records(
        """
        SELECT COUNT(*) AS column_count
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
        """,
        (args.schema, args.target_table),
        as_dict=True,
    )[0]["column_count"]
    return {
        "passed": pg_normalized == ms_normalized and int(source_columns) == 100 and int(target_columns) == 100,
        "postgres": pg_normalized,
        "mssql": ms_normalized,
        "source_column_count": int(source_columns),
        "target_column_count": int(target_columns),
    }


def normalize_verification_row(row: dict[str, Any]) -> dict[str, str]:
    return {key: str(value) for key, value in row.items()}


def now_utc() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def write_artifact(path: Path, artifact: dict[str, Any]) -> None:
    path.write_text(json.dumps(artifact, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")


if __name__ == "__main__":
    raise SystemExit(main())
