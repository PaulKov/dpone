"""Fixed PostgreSQL seed expressions, without DDL or backend evaluation."""

from dpone.adapters.nonproduction_postgres_fixture_recipe import postgres_fixture_columns
from dpone.contracts.nonproduction_plan_values import NonproductionPlanObject
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError


def postgres_seed_rows(seed_step: str) -> tuple[tuple[str, ...], ...]:
    """Return this step's own rows, retaining explicit enum and serial values.

    Initial creates two intended rows; watermark_3 describes only the third.
    Neither these values nor the selected step establish an actual generation.
    """
    if type(seed_step) is not str or seed_step not in {"initial", "watermark_3"}:
        raise NonproductionAuthorityError("fixture_recipe_step")
    columns = postgres_fixture_columns()
    if seed_step == "watermark_3":
        changes = {"id": "3", "updated_at": "TIMESTAMP '2026-07-21 12:00:00'", "c_name": "'row-three'"}
        return (tuple(changes.get(column.name, column.seed_sql) for column in columns),)
    changes = {"id": "2", "updated_at": "TIMESTAMP '2026-07-21 11:00:00'", "c_name": "'row-two'"}
    second = tuple(
        changes.get(column.name, column.seed_sql if "NOT NULL" in column.pg_ddl.upper() else "NULL")
        for column in columns
    )
    return tuple(column.seed_sql for column in columns), second


def render_postgres_fixture_bound_query(source: NonproductionPlanObject) -> str:
    """Conservatively account complete UTF-8 COPY payload before export.

    The fixed 128-column profile allows raw PostgreSQL CSV or the default MSSQL
    BulkTextCodec v2 projection. Codec control escaping expands at most twofold;
    CSV escaping can double it again. Each field additionally reserves 64 bytes
    for fixed temporal formatting, empty markers, quotes and delimiters. Bytea
    hex is already bounded by its raw text representation. No gzip or binary
    COPY variant is covered. UTF-8 is explicit instead of session encoding.

    The caller must independently verify the source schema and database and
    hold the same immutable generation through accounting and COPY. This query
    returns observations, not an admission grant, and scans without a row limit.
    """
    if type(source) is not NonproductionPlanObject:
        raise NonproductionAuthorityError("fixture_recipe_source")
    source.__post_init__()
    if (source.connector, source.object_kind) != ("postgres", "table"):
        raise NonproductionAuthorityError("fixture_recipe_source")
    table = ".".join(f'"{part}"' for part in source.qualified_name[1:])
    sizes = (
        f'''(4 * COALESCE(octet_length(convert_to("{column.name}"::text, 'UTF8')), 0)::bigint + 64)'''
        for column in postgres_fixture_columns()
    )
    return (
        "SELECT COUNT(*) AS source_rows, COALESCE(SUM("
        + " + ".join(sizes)
        + "), 0)::bigint AS transport_bytes_upper_bound FROM "
        + table
    )


def postgres_snapshot_rows(*, row_count: int) -> tuple[tuple[str, ...], ...]:
    """Describe one complete finite source generation with keys 1..N, N<=3.

    Two rows reproduce initial; three include the watermark row; reducing N
    removes previously present keys; zero describes an empty source. These are
    source seed vectors, never a LIMIT on extraction. Existing signed fixture
    plans still admit only their original initial/watermark steps. The caller
    must freshly seal each generation and independently account actual rows and
    transport bytes before exporting it. Declared N is not observed evidence.
    """
    if type(row_count) is not int or not 0 <= row_count <= 3:
        raise NonproductionAuthorityError("fixture_recipe_rows")
    rows = (*postgres_seed_rows("initial"), *postgres_seed_rows("watermark_3"))
    return tuple(row for index, row in enumerate(rows) if index < row_count)
