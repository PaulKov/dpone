"""Fixed BCP column expressions and pure finite seed SQL, without execution.

The variable-count inventory preserves the legacy tool's Python behavior. Only
the separate guarded renderer restricts parameters to the closed 202-column
recipe. A rendered statement is no generation, bound or admission proof.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from typing import Protocol

from dpone.contracts.nonproduction_plan_values import NonproductionPlanObject
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError


class NativeBoundColumn(Protocol):
    """Canonical transport widths consumed without depending on its runtime."""

    @property
    def name(self) -> str: ...

    @property
    def fixed_length(self) -> int | None: ...

    @property
    def prefix_width(self) -> int: ...


class NativeBoundContract(Protocol):
    @property
    def columns(self) -> Sequence[NativeBoundColumn]: ...

    @property
    def blockers(self) -> Sequence[str]: ...


class NativeBoundBuilder(Protocol):
    def __call__(self, *, schema: Sequence[tuple[str, str]], query: str) -> NativeBoundContract: ...


@dataclass(frozen=True, slots=True)
class BcpFixtureColumn:
    """One trusted source recipe expression, never caller-selected SQL."""

    name: str
    mssql_type: str
    insert_expression: str


def bcp_fixture_columns(column_count: int) -> tuple[BcpFixtureColumn, ...]:
    """Return a wide fixture covered by the v0.26 BCP-native decoder."""

    columns = (
        BcpFixtureColumn("order_id", "int NOT NULL", "CAST(gs AS int)"),
        BcpFixtureColumn("tiny_id", "tinyint", "CAST(gs % 256 AS tinyint)"),
        BcpFixtureColumn("small_id", "smallint", "CAST((gs % 32768) - 16384 AS smallint)"),
        BcpFixtureColumn("customer_id", "bigint", "CAST(gs * 1000003 AS bigint)"),
        BcpFixtureColumn("amount", "decimal(18,4)", "CAST(((gs % 200000) - 100000) / 10000.0 AS decimal(18,4))"),
        BcpFixtureColumn("amount_high", "numeric(38,9)", "CAST((gs % 1000000) / 1000000000.0 AS numeric(38,9))"),
        BcpFixtureColumn("money_amount", "money", "CAST(((gs % 20000) - 10000) / 100.0 AS money)"),
        BcpFixtureColumn("small_money_amount", "smallmoney", "CAST(((gs % 2000) - 1000) / 100.0 AS smallmoney)"),
        BcpFixtureColumn("is_active", "bit", "CAST(gs % 2 AS bit)"),
        BcpFixtureColumn("float_value", "float", "CAST((gs % 100000) / 7.0 AS float)"),
        BcpFixtureColumn("real_value", "real", "CAST((gs % 10000) / 3.0 AS real)"),
        BcpFixtureColumn(
            "row_guid", "uniqueidentifier", "CONVERT(uniqueidentifier, HASHBYTES('MD5', CONVERT(varchar(32), gs)))"
        ),
        BcpFixtureColumn(
            "business_date",
            "date",
            "CASE WHEN gs = 1 THEN CONVERT(date, '1970-01-01') "
            "WHEN gs = 2 THEN CONVERT(date, '2149-06-06') "
            "ELSE DATEADD(day, gs % 365, CONVERT(date, '2026-01-01')) END",
        ),
        BcpFixtureColumn("business_time", "time(7)", "CAST('12:34:56.1234567' AS time(7))"),
        BcpFixtureColumn(
            "created_dt",
            "datetime",
            "CASE WHEN gs = 1 THEN CONVERT(datetime, '1900-01-01T00:00:00') "
            "WHEN gs = 2 THEN CONVERT(datetime, '2299-12-31T23:59:59.997') "
            "ELSE DATEADD(millisecond, gs % 997, CONVERT(datetime, '2026-01-01T00:00:00')) END",
        ),
        BcpFixtureColumn(
            "created_at",
            "datetime2(7)",
            "CASE WHEN gs = 1 THEN CONVERT(datetime2(7), '1900-01-01T00:00:00.0000000') "
            "WHEN gs = 2 THEN CONVERT(datetime2(7), '2299-12-31T23:59:59.9999999') "
            "ELSE CONVERT(datetime2(7), '2026-06-22T12:34:56.1234567') END",
        ),
        BcpFixtureColumn(
            "offset_at",
            "datetimeoffset(7)",
            "CAST('2026-06-22T15:34:56.7654321+03:00' AS datetimeoffset(7))",
        ),
        BcpFixtureColumn(
            "rounded_dt", "smalldatetime", "DATEADD(minute, gs % 1440, CONVERT(smalldatetime, '2026-01-01T00:00:00'))"
        ),
        BcpFixtureColumn("fixed_ascii", "char(8)", "CAST('a' AS char(8))"),
        BcpFixtureColumn("ascii_text", "varchar(200)", "'ascii-' + CONVERT(varchar(40), gs) + CHAR(9) + 'tab'"),
        BcpFixtureColumn(
            "ascii_max", "varchar(max)", "REPLICATE(CONVERT(varchar(max), 'x'), 128) + CONVERT(varchar(40), gs)"
        ),
        BcpFixtureColumn("fixed_unicode", "nchar(8)", "CAST(N'я' AS nchar(8))"),
        BcpFixtureColumn(
            "unicode_text", "nvarchar(200)", "N'Привет-' + CONVERT(nvarchar(40), gs) + NCHAR(10) + N'line'"
        ),
        BcpFixtureColumn(
            "unicode_max", "nvarchar(max)", "REPLICATE(CONVERT(nvarchar(max), N'ж'), 128) + CONVERT(nvarchar(40), gs)"
        ),
        BcpFixtureColumn("empty_string", "nvarchar(20)", "CASE WHEN gs % 7 = 0 THEN N'' ELSE N'not-empty' END"),
        BcpFixtureColumn("nullable_text", "nvarchar(20)", "CASE WHEN gs % 11 = 0 THEN NULL ELSE N'value' END"),
        BcpFixtureColumn(
            "fixed_payload", "binary(4)", "CONVERT(binary(4), HASHBYTES('MD5', CONVERT(varchar(32), gs)))"
        ),
        BcpFixtureColumn("payload_bin", "varbinary(16)", "HASHBYTES('MD5', CONVERT(varchar(32), gs))"),
        BcpFixtureColumn(
            "payload_bin_max",
            "varbinary(max)",
            "CONVERT(varbinary(max), HASHBYTES('SHA2_256', CONVERT(varchar(32), gs)))",
        ),
    )
    if column_count <= len(columns):
        return columns[:column_count]
    return (*columns, *_extra_columns(column_count - len(columns)))


def _extra_columns(count: int) -> tuple[BcpFixtureColumn, ...]:
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
    return tuple(
        BcpFixtureColumn(
            name=f"{prefix}_{index + 1:03d}",
            mssql_type=mssql_type,
            insert_expression=expression.format(i=index + 1),
        )
        for index, (prefix, mssql_type, expression) in ((i, templates[i % len(templates)]) for i in range(count))
    )


def render_bcp_finite_insert(source: NonproductionPlanObject, *, row_count: int) -> str:
    """Render exactly keys 1..N with a bigint key before all reused expressions.

    Five fixed decimal digits define the finite domain. The early bigint cast
    preserves the legacy ROW_NUMBER type, notably for gs * 1000003. Physical
    identity, fresh creation, commit and continuing closure require later proof.
    """
    if type(source) is not NonproductionPlanObject:
        raise NonproductionAuthorityError("fixture_recipe_source")
    source.__post_init__()
    if (source.connector, source.object_kind) != ("mssql", "table"):
        raise NonproductionAuthorityError("fixture_recipe_source")
    if type(row_count) is not int or not 0 <= row_count <= 100000:
        raise NonproductionAuthorityError("fixture_recipe_rows")
    columns = bcp_fixture_columns(202)
    table = ".".join(f"[{part}]" for part in source.qualified_name)
    names = ", ".join(f"[{column.name}]" for column in columns)
    expressions = ", ".join(f"{column.insert_expression} AS [{column.name}]" for column in columns)
    return f"""
    WITH digits(n) AS (
        SELECT n FROM (VALUES (0),(1),(2),(3),(4),(5),(6),(7),(8),(9)) AS digit(n)
    ),
    source_rows AS (
        SELECT CONVERT(bigint, 1 + a.n + 10*b.n + 100*c.n + 1000*d.n + 10000*e.n) AS gs
        FROM digits AS a CROSS JOIN digits AS b CROSS JOIN digits AS c
        CROSS JOIN digits AS d CROSS JOIN digits AS e
    )
    INSERT INTO {table} ({names})
    SELECT {expressions} FROM source_rows
    WHERE gs >= 1 AND gs <= {row_count}
    ORDER BY gs
    """


def render_bcp_fixture_bound_query(
    source: NonproductionPlanObject, *, wire_contract_builder: NativeBoundBuilder
) -> str:
    """Account the complete 202-column native BCP file before queryout.

    The app injects build_mssql_bcp_native_contract as wire_contract_builder.
    Fixed payloads use the canonical native wire layout (not SQL page sizes:
    decimal/numeric payloads occupy 19 bytes). Variable fields retain original
    DATALENGTH, including UTF-16 and binary values, with native length prefixes.
    NULL contributes its prefix only. No value is cast to text for transport.
    The caller must validate the fixed schema, pin the actual source database
    and hold the same immutable generation through this scan and queryout.
    The query alone proves neither source ownership nor a stable snapshot.
    """
    if type(source) is not NonproductionPlanObject:
        raise NonproductionAuthorityError("fixture_recipe_source")
    source.__post_init__()
    if (source.connector, source.object_kind) != ("mssql", "table"):
        raise NonproductionAuthorityError("fixture_recipe_source")
    columns = bcp_fixture_columns(202)
    table = ".".join(f"[{part}]" for part in source.qualified_name)
    projection = ", ".join(f"[{column.name}]" for column in columns)
    contract = wire_contract_builder(
        schema=tuple(
            (
                column.name,
                column.mssql_type.removesuffix(" NOT NULL")
                if column.mssql_type.endswith(" NOT NULL")
                else column.mssql_type + " nullable",
            )
            for column in columns
        ),
        query=f"SELECT {projection} FROM {table}",
    )
    if contract.blockers:
        raise NonproductionAuthorityError("fixture_recipe_transport")
    sizes = []
    for column in contract.columns:
        name = f"[{column.name}]"
        payload = (
            f"CASE WHEN {name} IS NULL THEN 0 ELSE {column.fixed_length} END"
            if column.fixed_length is not None
            else f"COALESCE(DATALENGTH({name}), 0)"
        )
        sizes.append(f"CONVERT(bigint, {column.prefix_width}) + CONVERT(bigint, {payload})")
    return (
        "SELECT COUNT_BIG(*) AS source_rows, COALESCE(SUM("
        + " + ".join(sizes)
        + "), CONVERT(bigint, 0)) AS transport_bytes_upper_bound FROM "
        + table
    )
