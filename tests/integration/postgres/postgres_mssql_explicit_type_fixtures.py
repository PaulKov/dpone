"""Real PostgreSQL types that require an explicit MSSQL text contract.

The canonical type registry contains aliases and catalog identities that can
collapse to the same PostgreSQL ``pg_catalog.format_type`` declaration.  Each
fixture therefore records the exact certification case ids represented by one
real source column instead of pretending that aliases are different transports.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ExplicitTypeColumn:
    """One vendor-backed explicit-text source column and its case identities."""

    name: str
    pg_ddl: str
    seed_sql: str
    case_ids: tuple[str, ...]
    nullable: bool = False


EXPLICIT_TYPE_COLUMNS: tuple[ExplicitTypeColumn, ...] = (
    ExplicitTypeColumn("c_numeric_negative_overflow", "NUMERIC(38,-1)", "120", ("numeric_negative_scale_overflow",)),
    ExplicitTypeColumn("c_numeric_scale_overflow", "NUMERIC(3,39)", "1.23e-37", ("numeric_scale_overflow",)),
    ExplicitTypeColumn("c_numeric", "NUMERIC", "12345678901234567890.12345678901234567890", ("numeric_unconstrained",)),
    ExplicitTypeColumn(
        "c_decimal", "DECIMAL", "-98765432109876543210.00000000000000000001", ("decimal_unconstrained",)
    ),
    ExplicitTypeColumn("c_varchar_wide", "VARCHAR(2001)", "repeat('Ω', 2001)", ("varchar_2001_unicode_overflow",)),
    ExplicitTypeColumn("c_interval", "INTERVAL", "INTERVAL '2 days 03:04:05.123456'", ("interval",)),
    ExplicitTypeColumn(
        "c_interval_qualified",
        "INTERVAL DAY TO SECOND(6)",
        "INTERVAL '4 days 05:06:07.654321'",
        ("interval_qualified",),
    ),
    ExplicitTypeColumn("c_timetz", "TIME WITH TIME ZONE", "TIMETZ '12:34:56.123456+05:30'", ("timetz",)),
    ExplicitTypeColumn("c_timetz_alias", "TIMETZ", "TIMETZ '22:11:10.654321-03:30'", ("timetz_alias",)),
    ExplicitTypeColumn("c_bit", "BIT", "B'1'", ("bit",)),
    ExplicitTypeColumn("c_bit_bounded", "BIT(8)", "B'10101010'", ("bit_bounded",)),
    ExplicitTypeColumn("c_bit_varying", "BIT VARYING", "B'101001'", ("bit_varying",)),
    ExplicitTypeColumn("c_varbit_unbounded", "VARBIT", "B'1100101'", ("varbit_unbounded",)),
    ExplicitTypeColumn("c_varbit", "VARBIT(8)", "B'001011'", ("varbit",)),
    ExplicitTypeColumn("c_money", "MONEY", "1234.56::money", ("money",)),
    ExplicitTypeColumn("c_inet", "INET", "'198.51.100.7/24'::inet", ("network_1",)),
    ExplicitTypeColumn("c_cidr", "CIDR", "'198.51.100.0/24'::cidr", ("network_2",)),
    ExplicitTypeColumn("c_macaddr", "MACADDR", "'08:00:2b:01:02:03'::macaddr", ("network_3",)),
    ExplicitTypeColumn("c_macaddr8", "MACADDR8", "'08:00:2b:ff:fe:01:02:03'::macaddr8", ("network_4",)),
    ExplicitTypeColumn("c_xml", "XML", "'<root>Привет Ω</root>'::xml", ("xml",)),
    ExplicitTypeColumn("c_jsonpath", "JSONPATH", "'$.a ? (@ > 1)'::jsonpath", ("jsonpath",)),
    ExplicitTypeColumn("c_aclitem", "ACLITEM", "'=r/dpone'::aclitem", ("aclitem",)),
    ExplicitTypeColumn("c_internal_char", 'PG_CATALOG."char"', "'1'::pg_catalog.\"char\"", ("internal_char",)),
    ExplicitTypeColumn("c_name_type", "NAME", "'catalog-name'::name", ("name",)),
    ExplicitTypeColumn("c_bpchar", "PG_CATALOG.BPCHAR", "'pad '::pg_catalog.bpchar", ("bpchar_catalog",)),
    ExplicitTypeColumn("c_refcursor", "REFCURSOR", "'portal-name'::refcursor", ("refcursor",)),
    ExplicitTypeColumn("c_oid", "OID", "1234::oid", ("oid_1",)),
    ExplicitTypeColumn("c_xid", "XID", "'42'::xid", ("oid_2",)),
    ExplicitTypeColumn("c_xid8", "XID8", "'43'::xid8", ("oid_3",)),
    ExplicitTypeColumn("c_cid", "CID", "'44'::cid", ("oid_4",)),
    ExplicitTypeColumn("c_tid", "TID", "'(1,2)'::tid", ("oid_5",)),
    ExplicitTypeColumn("c_regclass", "REGCLASS", "'0'::regclass", ("oid_6",)),
    ExplicitTypeColumn("c_regcollation", "REGCOLLATION", "'0'::regcollation", ("oid_7",)),
    ExplicitTypeColumn("c_regconfig", "REGCONFIG", "'0'::regconfig", ("oid_8",)),
    ExplicitTypeColumn("c_regdictionary", "REGDICTIONARY", "'0'::regdictionary", ("oid_9",)),
    ExplicitTypeColumn("c_regnamespace", "REGNAMESPACE", "'0'::regnamespace", ("oid_10",)),
    ExplicitTypeColumn("c_regoper", "REGOPER", "'0'::regoper", ("oid_11",)),
    ExplicitTypeColumn("c_regoperator", "REGOPERATOR", "'0'::regoperator", ("oid_12",)),
    ExplicitTypeColumn("c_regproc", "REGPROC", "'0'::regproc", ("oid_13",)),
    ExplicitTypeColumn("c_regprocedure", "REGPROCEDURE", "'0'::regprocedure", ("oid_14",)),
    ExplicitTypeColumn("c_regrole", "REGROLE", "'0'::regrole", ("oid_15",)),
    ExplicitTypeColumn("c_regtype", "REGTYPE", "'0'::regtype", ("oid_16",)),
    ExplicitTypeColumn("c_tsvector", "TSVECTOR", "to_tsvector('simple', 'fat cats ate rats')", ("search_1",)),
    ExplicitTypeColumn("c_tsquery", "TSQUERY", "to_tsquery('simple', 'fat & rat')", ("search_2",)),
    ExplicitTypeColumn("c_pg_lsn", "PG_LSN", "'0/16B6C50'::pg_lsn", ("pg_lsn",)),
    ExplicitTypeColumn("c_pg_snapshot", "PG_SNAPSHOT", "'10:20:12,14'::pg_snapshot", ("pg_snapshot",)),
    ExplicitTypeColumn(
        "c_txid_snapshot",
        "TXID_SNAPSHOT",
        "'10:20:12,14'::txid_snapshot",
        ("txid_snapshot",),
    ),
    ExplicitTypeColumn("c_point", "POINT", "'(1,2)'::point", ("geometric_1",)),
    ExplicitTypeColumn("c_line", "LINE", "'{1,2,3}'::line", ("geometric_2",)),
    ExplicitTypeColumn("c_lseg", "LSEG", "'[(1,2),(3,4)]'::lseg", ("geometric_3",)),
    ExplicitTypeColumn("c_box", "BOX", "'((1,2),(3,4))'::box", ("geometric_4",)),
    ExplicitTypeColumn("c_path", "PATH", "'[(1,2),(3,4),(5,6)]'::path", ("geometric_5",)),
    ExplicitTypeColumn(
        "c_polygon",
        "POLYGON",
        "'((1,2),(3,4),(5,6))'::polygon",
        ("geometric_6",),
    ),
    ExplicitTypeColumn("c_circle", "CIRCLE", "'<(1,2),3>'::circle", ("geometric_7",)),
    ExplicitTypeColumn(
        "c_array",
        "TEXT[]",
        "ARRAY['alpha', 'comma,value', E'line\\nbreak', NULL]::text[]",
        ("array_contract_required", "array_catalog"),
    ),
    ExplicitTypeColumn("c_enum", '"__ENUM__"', "'alpha'::\"__ENUM_CAST__\"", ("enum_contract_required",)),
    ExplicitTypeColumn("c_domain", '"__DOMAIN__"', '12.34::"__DOMAIN_CAST__"', ("domain_catalog",)),
    ExplicitTypeColumn(
        "c_composite",
        '"__COMPOSITE__"',
        "ROW('Main, Ω', 12345)::\"__COMPOSITE_CAST__\"",
        ("composite_catalog",),
    ),
    ExplicitTypeColumn("c_range", "INT4RANGE", "'[1,8)'::int4range", ("range_contract_required", "range_catalog")),
    ExplicitTypeColumn("c_int8range", "INT8RANGE", "'[1,8000000000)'::int8range", ("range_builtin_1",)),
    ExplicitTypeColumn("c_numrange", "NUMRANGE", "'[1.25,8.75)'::numrange", ("range_builtin_2",)),
    ExplicitTypeColumn(
        "c_tsrange",
        "TSRANGE",
        "'[2026-01-01 00:00:00,2026-02-01 00:00:00)'::tsrange",
        ("range_builtin_3",),
    ),
    ExplicitTypeColumn(
        "c_tstzrange",
        "TSTZRANGE",
        "'[2026-01-01 00:00:00+03,2026-02-01 00:00:00+03)'::tstzrange",
        ("range_builtin_4",),
    ),
    ExplicitTypeColumn(
        "c_daterange",
        "DATERANGE",
        "'[2026-01-01,2026-02-01)'::daterange",
        ("range_builtin_5",),
    ),
    ExplicitTypeColumn(
        "c_multirange",
        "INT4MULTIRANGE",
        "'{[1,3),[5,8)}'::int4multirange",
        ("multirange_catalog", "multirange_contract_required"),
    ),
    ExplicitTypeColumn(
        "c_int8multirange",
        "INT8MULTIRANGE",
        "'{[1,3),[5000000000,8000000000)}'::int8multirange",
        ("multirange_builtin_1",),
    ),
    ExplicitTypeColumn(
        "c_nummultirange",
        "NUMMULTIRANGE",
        "'{[1.25,3.50),[5.75,8.00)}'::nummultirange",
        ("multirange_builtin_2",),
    ),
    ExplicitTypeColumn(
        "c_tsmultirange",
        "TSMULTIRANGE",
        "'{[2026-01-01 00:00:00,2026-01-02 00:00:00)}'::tsmultirange",
        ("multirange_builtin_3",),
    ),
    ExplicitTypeColumn(
        "c_tstzmultirange",
        "TSTZMULTIRANGE",
        "'{[2026-01-01 00:00:00+03,2026-01-02 00:00:00+03)}'::tstzmultirange",
        ("multirange_builtin_4",),
    ),
    ExplicitTypeColumn(
        "c_datemultirange",
        "DATEMULTIRANGE",
        "'{[2026-01-01,2026-01-02)}'::datemultirange",
        ("multirange_builtin_5",),
    ),
    ExplicitTypeColumn("c_citext", "public.citext", "'Case Preserved Ω'::public.citext", ("custom_catalog",)),
    # Boundary representatives below intentionally have no additional case id:
    # the canonical family is already classified above, while these values
    # certify PostgreSQL's exact serializer rather than inflate type coverage.
    ExplicitTypeColumn("b_numeric_nan", "NUMERIC", "'NaN'::numeric", ()),
    ExplicitTypeColumn("b_numeric_pos_inf", "NUMERIC", "'Infinity'::numeric", ()),
    ExplicitTypeColumn("b_numeric_neg_inf", "NUMERIC", "'-Infinity'::numeric", ()),
    ExplicitTypeColumn(
        "b_interval_mixed",
        "INTERVAL",
        "make_interval(days => -2, hours => 3, mins => -4, secs => 5.123456)",
        (),
    ),
    ExplicitTypeColumn("b_timetz_pos14", "TIMETZ", "TIMETZ '00:00:00+14'", ()),
    ExplicitTypeColumn("b_timetz_neg14", "TIMETZ", "TIMETZ '23:59:59.999999-14'", ()),
    ExplicitTypeColumn("b_varbit_empty", "VARBIT", "B''", ()),
    ExplicitTypeColumn("b_varbit_large", "VARBIT", "repeat('10', 128)::varbit", ()),
    ExplicitTypeColumn("b_inet_v6", "INET", "'2001:db8::1/64'::inet", ()),
    ExplicitTypeColumn("b_cidr_v6", "CIDR", "'2001:db8::/32'::cidr", ()),
    ExplicitTypeColumn("b_xml_escape", "XML", "'<r a=\"&quot;\">&lt;Ω&amp;</r>'::xml", ()),
    ExplicitTypeColumn("b_jsonpath_escape", "JSONPATH", '\'$."a b" ? (@ == "Ω")\'::jsonpath', ()),
    ExplicitTypeColumn("b_point_extreme", "POINT", "'(-1e300,1e300)'::point", ()),
    ExplicitTypeColumn("b_lseg_degenerate", "LSEG", "'[(0,0),(0,0)]'::lseg", ()),
    ExplicitTypeColumn("b_box_degenerate", "BOX", "'((0,0),(0,0))'::box", ()),
    ExplicitTypeColumn("b_circle_zero", "CIRCLE", "'<(0,0),0>'::circle", ()),
    ExplicitTypeColumn(
        "b_array_multidimensional",
        "TEXT[]",
        "ARRAY[[E'Ω\\t', NULL], ['comma,value', E'line\\nbreak']]::text[]",
        (),
    ),
    ExplicitTypeColumn(
        "b_array_non_one_bounds",
        "TEXT[]",
        "'[-2:0]={alpha,NULL,\"comma,value\"}'::text[]",
        (),
    ),
    ExplicitTypeColumn("b_enum_unicode", '"__ENUM__"', '\'β,"quoted"\'::"__ENUM_CAST__"', ()),
    ExplicitTypeColumn(
        "b_domain_unicode",
        '"__TEXT_DOMAIN__"',
        'E\'domain Ω,"quoted"\\t\\n\'::"__TEXT_DOMAIN_CAST__"',
        (),
    ),
    ExplicitTypeColumn(
        "b_composite_null",
        '"__COMPOSITE__"',
        'ROW(NULL, NULL)::"__COMPOSITE_CAST__"',
        (),
    ),
    ExplicitTypeColumn("b_range_empty", "INT4RANGE", "'empty'::int4range", ()),
    ExplicitTypeColumn("b_range_unbounded", "INT4RANGE", "'(,)'::int4range", ()),
    ExplicitTypeColumn("b_range_inclusive", "INT4RANGE", "'[1,8]'::int4range", ()),
    ExplicitTypeColumn("b_multirange_empty", "INT4MULTIRANGE", "'{}'::int4multirange", ()),
    ExplicitTypeColumn("b_multirange_unbounded", "INT4MULTIRANGE", "'{(,)}'::int4multirange", ()),
    ExplicitTypeColumn("c_null_interval", "INTERVAL", "NULL", (), nullable=True),
)


POSTGIS_SPATIAL_CASES = frozenset({"geometry", "geography", "postgis_raster", "postgis_catalog"})


def create_explicit_type_table(postgres, *, schema: str, table: str, suffix: str) -> None:
    """Create one source relation with every available explicit-policy family."""

    enum_name = f"explicit_enum_{suffix}"
    domain_name = f"explicit_domain_{suffix}"
    text_domain_name = f"explicit_text_domain_{suffix}"
    composite_name = f"explicit_address_{suffix}"
    postgres.execute_query("CREATE EXTENSION IF NOT EXISTS citext WITH SCHEMA public")
    postgres.execute_query(f"CREATE TYPE \"{schema}\".\"{enum_name}\" AS ENUM ('alpha', 'beta', 'β,\"quoted\"')")
    postgres.execute_query(f'CREATE DOMAIN "{schema}"."{domain_name}" AS NUMERIC(12,2)')
    postgres.execute_query(f'CREATE DOMAIN "{schema}"."{text_domain_name}" AS TEXT')
    postgres.execute_query(f'CREATE TYPE "{schema}"."{composite_name}" AS (street text, zip integer)')
    replacements = {
        "__ENUM__": f'{schema}"."{enum_name}',
        "__ENUM_CAST__": f'{schema}"."{enum_name}',
        "__DOMAIN__": f'{schema}"."{domain_name}',
        "__DOMAIN_CAST__": f'{schema}"."{domain_name}',
        "__TEXT_DOMAIN__": f'{schema}"."{text_domain_name}',
        "__TEXT_DOMAIN_CAST__": f'{schema}"."{text_domain_name}',
        "__COMPOSITE__": f'{schema}"."{composite_name}',
        "__COMPOSITE_CAST__": f'{schema}"."{composite_name}',
    }

    def render(value: str) -> str:
        for marker, replacement in replacements.items():
            value = value.replace(marker, replacement)
        return value

    declarations = ["id INTEGER NOT NULL"]
    declarations.extend(
        f'"{column.name}" {render(column.pg_ddl)} {"NULL" if column.nullable else "NOT NULL"}'
        for column in EXPLICIT_TYPE_COLUMNS
    )
    postgres.execute_query(f'CREATE TABLE "{schema}"."{table}" ({", ".join(declarations)}, PRIMARY KEY (id))')
    names = ["id", *(column.name for column in EXPLICIT_TYPE_COLUMNS)]
    values = ["1", *(render(column.seed_sql) for column in EXPLICIT_TYPE_COLUMNS)]
    quoted_names = ", ".join(f'"{name}"' for name in names)
    postgres.execute_query(f'INSERT INTO "{schema}"."{table}" ({quoted_names}) VALUES ({", ".join(values)})')


def explicit_schema_contract(*, excluded: str | None = None, wrong: str | None = None) -> dict[str, object]:
    """Return the strict public contract; optionally make one policy cell invalid."""

    columns: dict[str, dict[str, object]] = {"id": {"type": "integer", "nullable": False}}
    for column in EXPLICIT_TYPE_COLUMNS:
        if column.name == excluded:
            continue
        columns[column.name] = {
            "type": "number" if column.name == wrong else "string",
            "nullable": column.nullable,
        }
    return {"enforcement": "strict", "columns": columns}


def explicit_case_ids() -> frozenset[str]:
    return frozenset(case_id for column in EXPLICIT_TYPE_COLUMNS for case_id in column.case_ids)


__all__ = [
    "create_explicit_type_table",
    "explicit_case_ids",
    "explicit_schema_contract",
    "EXPLICIT_TYPE_COLUMNS",
    "ExplicitTypeColumn",
    "POSTGIS_SPATIAL_CASES",
]
