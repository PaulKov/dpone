from __future__ import annotations

from dpone.strategy_intelligence.models import NativeFastPath


class NativeFastPathCatalog:
    """Target-specific native bulk path catalog.

    The catalog is intentionally static and credential-free. Runtime adapters can
    later turn a path into executable commands after validating local tools.
    """

    def resolve(self, source_type: str, sink_type: str) -> NativeFastPath:
        key = (_normalize(source_type), _normalize(sink_type))
        return _FAST_PATHS.get(key) or _FAST_PATHS[("default", "default")]


_FAST_PATHS: dict[tuple[str, str], NativeFastPath] = {
    ("postgres", "mssql"): NativeFastPath(
        path_id="postgres_copy_to_mssql_bcp",
        source_type="postgres",
        sink_type="mssql",
        summary=(
            "Export with PostgreSQL COPY using dpone BulkTextCodec projection, "
            "verify and decode one immutable artifact pass into a length-prefixed UTF-8 host file, "
            "then import directly into native staging with MSSQL bcp."
        ),
        commands=(
            "COPY (SELECT dpone BulkTextCodec-projected columns FROM source WHERE ...) "
            "TO STDOUT WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE E'\\x1f', ESCAPE E'\\x1f', NULL '')",
            "dpone verifies SHA/row checksums and writes data.typed.bin + data.fmt with four-byte field prefixes",
            "bcp target_staging in data.typed.bin -f data.fmt -C 65001 -b 100000 -S <server> -d <db> -k",
        ),
        required_tools=("psycopg", "bcp"),
        fallback_path="fail_closed_for_certified_key_snapshot",
        expected_impact="highest for 1M+ rows; avoids raw SQL normalization and row-by-row inserts",
    ),
    ("mysql", "mssql"): NativeFastPath(
        path_id="mysql_stream_to_mssql_bcp",
        source_type="mysql",
        sink_type="mssql",
        summary=(
            "Stream MySQL SELECT through BulkTextCodec into mssql-delimited artifacts, "
            "import with MSSQL bcp into staging, then set-based finalize."
        ),
        commands=(
            "SELECT projected_columns FROM `db`.`table` WHERE watermark_predicate",
            "dpone encodes text with BulkTextCodec and writes mssql-delimited .bcp",
            "bcp target_staging in data.bcp -c -C 65001 -t\\t -r\\n -b 100000 -S <server> -d <db> -k",
        ),
        required_tools=("PyMySQL", "bcp"),
        fallback_path="pyodbc_fast_executemany",
        expected_impact="high for batch MySQL→MSSQL; staging-first preserves recovery vs direct bulk",
    ),
    ("mssql", "clickhouse"): NativeFastPath(
        path_id="mssql_bcp_queryout_to_clickhouse_typed_wire",
        source_type="mssql",
        sink_type="clickhouse",
        summary=(
            "Export raw typed bcp queryout files without MSSQL-side REPLACE, "
            "load them into ClickHouse typed staging with CustomSeparated/typed wire settings."
        ),
        commands=(
            'bcp "SELECT projected_columns FROM source WHERE ..." queryout data.bcp '
            "-c -t0x1f -r0x1e0x0a -S <server> -d <db>",
            'clickhouse-client --query="INSERT INTO raw_staging FORMAT CustomSeparated" < data.bcp',
            "dpone finalizes decoded staging into the target only after gates pass.",
        ),
        required_tools=("bcp", "clickhouse-client"),
        fallback_path="mssql_bcp_queryout_to_clickhouse_direct_tsv",
        expected_impact="very high; avoids row parsing and avoids source-side text escaping CPU",
    ),
    ("clickhouse", "mssql"): NativeFastPath(
        path_id="clickhouse_streaming_to_mssql_bcp_staging",
        source_type="clickhouse",
        sink_type="mssql",
        summary=(
            "Read ClickHouse in bounded source-fetch batches, stream them into one immutable "
            "BulkTextCodec spool, and run one MSSQL bcp import whose transaction batches are "
            "independent from source fetching. Set-based finalization is implemented; exact "
            "route and performance status remain UNVERIFIED pending the manual live gate."
        ),
        commands=(
            "SELECT name, type FROM system.columns WHERE database=<source_db> AND table=<source_table>",
            "bcp target_staging in data.bcp -c -C 65001 -t\\t -r\\n -b 100000 -S <server> -d <db> -k",
            "dpone keeps source fetch batches independent from bcp -b transaction batches, validates "
            "row counts/nulls/distincts, and commits state only after MSSQL finalization.",
        ),
        required_tools=("clickhouse-driver or clickhouse-connect", "bcp"),
        fallback_path="streaming_rows",
        expected_impact=(
            "removes per-fetch BCP process overhead; actual throughput remains environment-specific "
            "and UNVERIFIED until the manual live benchmark"
        ),
    ),
    ("postgres", "clickhouse"): NativeFastPath(
        path_id="postgres_copy_to_clickhouse_http_tsv",
        source_type="postgres",
        sink_type="clickhouse",
        summary="Pipe PostgreSQL COPY output into ClickHouse HTTP/Native TSV ingest.",
        commands=(
            "COPY (SELECT ... FROM source WHERE ...) TO STDOUT WITH (FORMAT csv, DELIMITER E'\\t')",
            "curl --data-binary @data.tsv 'http://clickhouse:8123/?query=INSERT%20INTO%20target%20FORMAT%20TabSeparated'",
        ),
        required_tools=("psycopg", "clickhouse-client or curl"),
        fallback_path="streaming_rows",
        expected_impact="high for wide analytical tables",
    ),
    ("mssql", "mssql"): NativeFastPath(
        path_id="mssql_bcp_queryout_to_bcp_import",
        source_type="mssql",
        sink_type="mssql",
        summary="Use bcp queryout from source and bcp import into target staging.",
        commands=(
            'bcp "SELECT ... FROM source" queryout data.tsv -c -t\\t -r\\n -S <source>',
            "bcp target_staging in data.tsv -c -t\\t -r\\n -S <target> -b 100000",
        ),
        required_tools=("bcp",),
        fallback_path="pyodbc_fast_executemany",
        expected_impact="high when source and target are different SQL Server instances",
    ),
    ("default", "default"): NativeFastPath(
        path_id="streaming_rows",
        source_type="default",
        sink_type="default",
        summary="Portable streaming rows path with staging-first sink finalization when available.",
        commands=("dpone run manifest.yaml",),
        required_tools=("python",),
        fallback_path="streaming_rows",
        expected_impact="portable baseline",
    ),
}


def _normalize(value: str) -> str:
    aliases = {"sqlserver": "mssql", "sql_server": "mssql", "api": "rest"}
    normalized = str(value).strip().lower().replace("-", "_")
    return aliases.get(normalized, normalized)
