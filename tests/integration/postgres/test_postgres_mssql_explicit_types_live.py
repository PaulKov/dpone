"""Vendor-live certification for PostgreSQL families gated by text contracts."""

from __future__ import annotations

import os
import uuid
from pathlib import Path

import pytest
from tools.route_live_certification.pytest_plugin import (
    record_route_live_subcase_finished,
    record_route_live_subcase_started,
)
from tools.route_live_certification.recorder import RouteLiveObservationRecorder
from tools.route_live_certification.reviewed_cases_types import explicit_type_suite

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.postgres_mssql_type_policy import declared_postgres_mssql_contract_blockers
from dpone.runtime.connectors.postgres import PostgresConnector
from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
from dpone.runtime.support.postgres_mssql_projection import PostgresMssqlProjectionError
from dpone.type_system.source_sink.certification_suites import TypeCertificationSuiteRegistry
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper
from tests.integration.postgres.postgres_live_support import (
    NoopLogger,
    drop_mssql_table,
    ensure_mssql_database_and_schemas,
    ensure_postgres_schemas,
    mssql_connector,
    postgres_connector,
    postgres_mssql_enabled,
    wait_until_ready,
)
from tests.integration.postgres.postgres_mssql_explicit_type_fixtures import (
    EXPLICIT_TYPE_COLUMNS,
    POSTGIS_SPATIAL_CASES,
    create_explicit_type_table,
    explicit_case_ids,
    explicit_schema_contract,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedMssqlCampaign,
    GovernedPostgresSnapshotSource,
    GovernedStandardEtlRunner,
    bind_factual_postgres_source_authority,
    governed_mssql_route,
)
from tests.integration.postgres.postgres_mssql_wide_fixtures import SOURCE_SCHEMA
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import QuietIntegrationLogger

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
]

_PG16_SYSTEM_ONLY_BUILTINS = {
    "gtsvector": "GiST index signature with no accepted user input",
    "pg_brin_bloom_summary": "BRIN internal summary with no stable user I/O contract",
    "pg_brin_minmax_multi_summary": "BRIN internal summary with no stable user I/O contract",
    "pg_dependencies": "planner statistics payload with no stable user I/O contract",
    "pg_mcv_list": "planner statistics payload with no stable user I/O contract",
    "pg_ndistinct": "planner statistics payload with no stable user I/O contract",
    "pg_node_tree": "server-internal parse tree with no accepted user input",
}


def _config(
    *,
    source_table: str,
    target_table: str,
    tmp_path: Path,
    contract: dict[str, object],
    physical_design: dict[str, object] | None = None,
) -> LoadConfig:
    options: dict[str, object] = {
        "source_type": "postgres",
        "sink_type": "mssql",
        "batch_commit_mode": "whole",
        "work_dir": str(tmp_path),
        "technical_columns": "forbidden",
        "schema_contract": contract,
    }
    if physical_design is not None:
        options["physical_design"] = physical_design
    return LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_schema=SOURCE_SCHEMA,
        source_table=source_table,
        target_schema="dpone_it",
        target_table=target_table,
        target_database=os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it"),
        staging_schema="staging",
        staging_database=os.environ.get("DPONE_IT_MSSQL_DATABASE", "dpone_it"),
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="csv",
        compress_export=False,
        options=options,
    )


def test_explicit_type_fixture_classifies_every_policy_case_exactly_once() -> None:
    """Keep the live representatives synchronized with the canonical registry."""

    suite = TypeCertificationSuiteRegistry.default().suite("postgres", "mssql")
    policy_case_ids = {case.name for case in suite.cases if case.decision_category == "incompatible_requires_policy"}
    representatives = [case_id for column in EXPLICIT_TYPE_COLUMNS for case_id in column.case_ids]

    assert len(representatives) == len(set(representatives))
    assert explicit_case_ids().isdisjoint(POSTGIS_SPATIAL_CASES)
    assert explicit_case_ids() | POSTGIS_SPATIAL_CASES == policy_case_ids


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_pg16_builtin_catalog_inventory_has_policy_or_typed_exclusion_live() -> None:
    """Inventory every non-array pg_catalog base/range/multirange type."""

    postgres = postgres_connector()
    rows = postgres.get_records(
        "SELECT t.typname, t.typtype, t.typcategory, t.typinput::regproc::text "
        "FROM pg_catalog.pg_type AS t "
        "INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace "
        "WHERE n.nspname = 'pg_catalog' "
        "AND t.typtype IN ('b','r','m') AND t.typisdefined "
        "AND t.typcategory <> 'A' AND t.typname NOT LIKE '\\_%' "
        "ORDER BY t.typname",
        as_dict=True,
    )
    observed = {str(row["typname"]) for row in rows}
    suite = TypeCertificationSuiteRegistry.default().suite("postgres", "mssql")
    cases_by_source = {case.source_type: case for case in suite.cases}
    mapper = PostgresMssqlTypeMapper()
    covered: set[str] = set()
    for type_name in sorted(observed.difference(_PG16_SYSTEM_ONLY_BUILTINS)):
        source_spec = '"char"' if type_name == "char" else type_name
        case = cases_by_source.get(source_spec)
        assert case is not None, f"unclassified PG16 built-in: {type_name}"
        decision = mapper.resolve(source_spec)
        assert decision.requires_explicit_contract == (case.decision_category == "incompatible_requires_policy")
        covered.add(type_name)

    assert set(_PG16_SYSTEM_ONLY_BUILTINS) <= observed
    assert all(_PG16_SYSTEM_ONLY_BUILTINS.values())
    assert covered | set(_PG16_SYSTEM_ONLY_BUILTINS) == observed

    array_rows = postgres.get_records(
        "SELECT t.typname FROM pg_catalog.pg_type AS t "
        "INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace "
        "WHERE n.nspname = 'pg_catalog' AND t.typtype = 'b' "
        "AND t.typcategory = 'A' AND t.typisdefined",
        as_dict=True,
    )
    array_types = {str(row["typname"]) for row in array_rows}
    assert all(name.startswith("_") or name in {"int2vector", "oidvector"} for name in array_types)


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_available_explicit_families_reject_missing_or_wrong_contract_before_copy_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Execute all three negative contract partitions for every policy id."""

    suffix = uuid.uuid4().hex[:8]
    source_table = f"explicit_policy_source_{suffix}"
    target_table = f"explicit_policy_target_{suffix}"
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    wait_until_ready("mssql", lambda: mssql.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    create_explicit_type_table(postgres, schema=SOURCE_SCHEMA, table=source_table, suffix=suffix)
    drop_mssql_table(mssql, target_table)
    strategy = PostgresFullExtractStrategy(postgres, logger=NoopLogger())

    for column in (item for item in EXPLICIT_TYPE_COLUMNS if item.case_ids):
        source_identity = _source_column_identity(postgres, source_table, column.name)
        for policy_case_id in column.case_ids:
            for contract_case, contract, physical_design in (
                ("missing_contract", explicit_schema_contract(excluded=column.name), None),
                (
                    "physical_only",
                    explicit_schema_contract(excluded=column.name),
                    {
                        "columns": {
                            column.name: {
                                "target_type": {"mssql": "nvarchar(max)"},
                            }
                        }
                    },
                ),
                ("wrong_logical_type", explicit_schema_contract(wrong=column.name), None),
            ):
                config = _config(
                    source_table=source_table,
                    target_table=target_table,
                    tmp_path=tmp_path,
                    contract=contract,
                    physical_design=physical_design,
                )
                before = {
                    "target_object_count": _target_object_count(mssql, target_table),
                    "source_column": source_identity,
                    "artifact_entries": sorted(path.name for path in tmp_path.iterdir()),
                }
                with pytest.raises(PostgresMssqlProjectionError) as raised:
                    strategy.fetch_schema_projection(config)
                assert raised.value.blocker == "postgres_mssql.type_contract.explicit_contract_required"
                assert raised.value.column == column.name
                after = {
                    "target_object_count": _target_object_count(mssql, target_table),
                    "source_column": _source_column_identity(postgres, source_table, column.name),
                    "artifact_entries": sorted(path.name for path in tmp_path.iterdir()),
                }
                assert after == before
                route_live_recorder.observe_case(
                    "explicit_types",
                    f"{policy_case_id}__{contract_case}",
                    before_image=before,
                    after_image=after,
                    observations={
                        "blocker": raised.value.blocker,
                        "column": raised.value.column,
                        "blocked_before_source_copy": True,
                    },
                )


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_available_explicit_families_roundtrip_with_string_contract_standard_etl_live(
    tmp_path: Path,
) -> None:
    """One standard ETL run round-trips every available explicit family."""

    suffix = uuid.uuid4().hex[:8]
    source_table = f"explicit_roundtrip_source_{suffix}"
    target_table = f"explicit_roundtrip_target_{suffix}"
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    wait_until_ready("mssql", lambda: mssql.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    create_explicit_type_table(postgres, schema=SOURCE_SCHEMA, table=source_table, suffix=suffix)
    drop_mssql_table(mssql, target_table)
    config = _config(
        source_table=source_table,
        target_table=target_table,
        tmp_path=tmp_path,
        contract=explicit_schema_contract(),
    )
    logger = QuietIntegrationLogger()
    with governed_mssql_route(
        mssql,
        target_database=str(config.target_database),
        target_schema=config.target_schema,
        target_table=config.target_table,
    ) as route:
        result = GovernedStandardEtlRunner(
            route,
            postgres,
            logger=logger,
        ).run(
            config,
            label="explicit_types",
        )

    text_columns = ", ".join(f'"{column.name}"::text AS "{column.name}"' for column in EXPLICIT_TYPE_COLUMNS)
    expected = postgres.get_records(
        f'SELECT id, {text_columns} FROM "{SOURCE_SCHEMA}"."{source_table}"',
        as_dict=True,
    )
    selected = ", ".join(f"[{column.name}]" for column in EXPLICIT_TYPE_COLUMNS)
    actual = mssql.get_records(
        f"SELECT [id], {selected} FROM [dpone_it].[{target_table}]",
        as_dict=True,
    )
    catalog = mssql.get_records(
        "SELECT c.name, ty.name AS type_name, c.max_length "
        "FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "INNER JOIN sys.columns AS c ON c.object_id = t.object_id "
        "INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id "
        "WHERE s.name = N'dpone_it' AND t.name = ?",
        (target_table,),
        as_dict=True,
    )

    assert result["status"] == "success"
    assert result["loaded_rows"] == 1, result
    assert actual == expected
    catalog_by_name = {str(row["name"]): row for row in catalog}
    business_names = {column.name for column in EXPLICIT_TYPE_COLUMNS}
    assert set(catalog_by_name) == {
        "id",
        *business_names,
        "__dpone__load_id",
        "__dpone__loaded_at",
        "__dpone__row_id",
        "__dpone__extracted_at",
    }
    assert {(catalog_by_name[name]["type_name"], catalog_by_name[name]["max_length"]) for name in business_names} == {
        ("nvarchar", -1)
    }
    assert {
        name: (
            str(catalog_by_name[name]["type_name"]).lower(),
            int(catalog_by_name[name]["max_length"]),
        )
        for name in (
            "__dpone__load_id",
            "__dpone__loaded_at",
            "__dpone__row_id",
            "__dpone__extracted_at",
        )
    } == {
        "__dpone__load_id": ("varchar", 26),
        "__dpone__loaded_at": ("datetime2", 8),
        "__dpone__row_id": ("varchar", 64),
        "__dpone__extracted_at": ("datetime2", 8),
    }
    assert not tuple(tmp_path.iterdir())


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_every_reviewed_explicit_value_cell_roundtrips_independently_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Run one real catalog-typed source and governed load per positive cell."""

    suffix = uuid.uuid4().hex[:8]
    aggregate = f"explicit_cells_authority_{suffix}"
    postgres = postgres_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    create_explicit_type_table(postgres, schema=SOURCE_SCHEMA, table=aggregate, suffix=suffix)
    mssql = governed_mssql_live_campaign.target
    logger = QuietIntegrationLogger()
    source_column_by_case = {case_id: column for column in EXPLICIT_TYPE_COLUMNS for case_id in column.case_ids}
    policy_cases = {
        case.name: case for case in TypeCertificationSuiteRegistry.default().suite("postgres", "mssql").cases
    }
    reviewed = [item for item in explicit_type_suite().cases if "__string_contract__" in item.case_id]
    assert len(reviewed) == 324
    try:
        for ordinal, reviewed_case in enumerate(reviewed):
            record_route_live_subcase_started(
                suite_id="explicit_types",
                case_id=reviewed_case.case_id,
                ordinal=ordinal,
            )
            type_case_id, value_case = reviewed_case.case_id.split("__string_contract__", 1)
            policy_case = policy_cases[type_case_id]
            source_column = source_column_by_case[type_case_id]
            authority = _source_column_identity(postgres, aggregate, source_column.name)
            declared_type = str(authority["declared_type"])
            source_table = f"explicit_cell_{suffix}_{ordinal:03d}"
            target_table = f"explicit_cell_target_{suffix}_{ordinal:03d}"
            postgres.execute_query(
                f'CREATE TABLE "{SOURCE_SCHEMA}"."{source_table}" (id integer NULL, value {declared_type} NULL)'
            )
            expression = _explicit_value_expression(
                canonical=policy_case.expected_canonical_type,
                source_type=policy_case.source_type,
                value_case=value_case,
                declared_type=declared_type,
                source_column=source_column.name,
            )
            postgres.execute_query(
                f'INSERT INTO "{SOURCE_SCHEMA}"."{source_table}" (id, value) '
                f'SELECT 1, {expression} FROM "{SOURCE_SCHEMA}"."{aggregate}" AS src'
            )
            config = _config(
                source_table=source_table,
                target_table=target_table,
                tmp_path=tmp_path,
                contract={
                    "enforcement": "strict",
                    "columns": {
                        "id": {"type": "integer", "nullable": True},
                        "value": {"type": "string", "nullable": True},
                    },
                },
            )
            config.target_database = governed_mssql_live_campaign.target_database
            config.staging_database = governed_mssql_live_campaign.target_database
            route = governed_mssql_live_campaign.route(
                target_schema=config.target_schema,
                target_table=config.target_table,
            )
            runner = GovernedStandardEtlRunner(
                route,
                postgres,
                logger=logger,
                source_type=GovernedPostgresSnapshotSource,
            )
            source_identity = _source_column_identity(postgres, source_table, "value")
            assert source_identity["type_oid"] == authority["type_oid"]
            before = {
                "target_object_count": _target_object_count(mssql, target_table),
                "source_column": source_identity,
            }
            expected = postgres.get_records(
                f'SELECT value::text AS value FROM "{SOURCE_SCHEMA}"."{source_table}"',
                as_dict=True,
            )
            result = runner.run(
                config,
                label=f"explicit_{ordinal:03d}_{type_case_id}_{value_case}",
            )
            actual = mssql.get_records(
                f"SELECT [value] FROM [dpone_it].[{target_table}] ORDER BY [id]",
                as_dict=True,
            )
            catalog = mssql.get_records(
                "SELECT ty.name AS type_name, c.max_length, c.is_nullable "
                "FROM sys.columns AS c "
                "INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id "
                "WHERE c.object_id = OBJECT_ID(?) AND c.name = N'value'",
                (f"dpone_it.{target_table}",),
                as_dict=True,
            )
            assert result["status"] == "success"
            assert result["loaded_rows"] == 1
            assert actual == expected
            assert catalog == [{"type_name": "nvarchar", "max_length": -1, "is_nullable": True}]
            assert _target_staging_count(mssql, target_table) == 0
            after = {
                "target_object_count": _target_object_count(mssql, target_table),
                "source_column": _source_column_identity(postgres, source_table, "value"),
                "target_catalog": catalog,
                "source_text": expected,
                "target_text": actual,
            }
            route_live_recorder.observe_case(
                "explicit_types",
                reviewed_case.case_id,
                before_image=before,
                after_image=after,
                observations={
                    "loaded_rows": 1,
                    "exact_postgres_cast_text": actual == expected,
                    "staging_objects_after": 0,
                },
            )
            mssql.execute_query(f"DROP TABLE [dpone_it].[{target_table}]")
            postgres.execute_query(f'DROP TABLE "{SOURCE_SCHEMA}"."{source_table}"')
            record_route_live_subcase_finished(
                suite_id="explicit_types",
                case_id=reviewed_case.case_id,
                ordinal=ordinal,
            )
    finally:
        _drop_explicit_cell_tables(postgres, suffix=suffix)


def _explicit_value_expression(
    *,
    canonical: str,
    source_type: str,
    value_case: str,
    declared_type: str,
    source_column: str,
) -> str:
    column = f'src."{source_column.replace(chr(34), chr(34) * 2)}"'

    def cast(expression: str) -> str:
        return f"CAST({expression} AS {declared_type})"

    if value_case == "null":
        return f"CASE WHEN TRUE THEN NULL ELSE {column} END"
    if value_case == "nominal":
        return column
    normalized_source = source_type.strip().lower()
    if canonical == "decimal":
        return {
            "finite_large": column,
            "nan": cast("'NaN'"),
            "positive_infinity": cast("'Infinity'"),
            "negative_infinity": cast("'-Infinity'"),
        }[value_case]
    if canonical == "string":
        length = 2001 if "2001" in normalized_source else 32
        return cast(f"repeat('Ω', {length})")
    if canonical == "interval":
        return {
            "zero": cast("INTERVAL '0'"),
            "mixed_sign": cast("make_interval(months => -14, days => 3, secs => -5.123456)"),
            "months_days_microseconds": cast("make_interval(months => 1200, days => -366, secs => 86399.999999)"),
        }[value_case]
    if canonical == "time":
        return {
            "positive_offset": cast("TIMETZ '00:00:00+14'"),
            "negative_offset": cast("TIMETZ '23:59:59.999999-14'"),
            "maximum_microsecond": cast("TIMETZ '23:59:59.999999+00'"),
        }[value_case]
    if canonical == "bit_string":
        width = 1 if normalized_source == "bit" else 8
        bits = {
            "empty_varying": "B''",
            "all_zero": f"B'{('0' * width)}'",
            "all_one": f"B'{('1' * width)}'",
            "declared_boundary": "repeat('10', 512)"
            if "varying" in normalized_source or normalized_source == "varbit"
            else f"B'{('10' * ((width + 1) // 2))[:width]}'",
        }[value_case]
        return cast(bits)
    if canonical == "network":
        values = {
            "inet": {
                "ipv4_host": "'255.255.255.255/32'",
                "ipv4_network": "'203.0.113.0/24'",
                "ipv6_host": "'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128'",
                "ipv6_network": "'2001:db8::/32'",
                "minimum_prefix": "'0.0.0.0/0'",
                "maximum_prefix": "'::1/128'",
            },
            "cidr": {
                "ipv4_network": "'0.0.0.0/0'",
                "ipv6_network": "'::/0'",
                "minimum_prefix": "'0.0.0.0/0'",
                "maximum_prefix": "'ffff:ffff:ffff:ffff:ffff:ffff:ffff:ffff/128'",
            },
            "macaddr": {
                "all_zero": "'00:00:00:00:00:00'",
                "all_one": "'ff:ff:ff:ff:ff:ff'",
                "multicast": "'01:00:5e:00:00:01'",
                "local_administered": "'02:00:00:00:00:01'",
                "boundary": "'ff:ff:ff:ff:ff:fe'",
            },
            "macaddr8": {
                "all_zero": "'00:00:00:00:00:00:00:00'",
                "all_one": "'ff:ff:ff:ff:ff:ff:ff:ff'",
                "multicast": "'01:00:5e:ff:fe:00:00:01'",
                "local_administered": "'02:00:00:ff:fe:00:00:01'",
                "eui64": "'08:00:2b:ff:fe:01:02:03'",
            },
        }
        return cast(values[normalized_source][value_case])
    if canonical == "geometric":
        return _geometric_value_expression(
            source_type=normalized_source,
            value_case=value_case,
            declared_type=declared_type,
        )
    if canonical == "xml":
        return cast(
            {
                "empty_element": "'<empty/>'",
                "unicode_attributes": '\'<r city="Москва" symbol="Ω"/>\'',
                "escaped_entities": "'<r>&lt;&amp;&gt;&quot;</r>'",
            }[value_case]
        )
    if canonical == "jsonpath":
        return cast(
            {
                "quoted_unicode": "'$.\"ключ Ω\"'",
                "predicate_expression": "'$.a ? (@ >= 1 && @ < 10)'",
            }[value_case]
        )
    if canonical == "array":
        return {
            "empty": cast("ARRAY[]::text[]"),
            "multidimensional_non_one_bounds": cast("array_fill('Ω'::text, ARRAY[2,2], ARRAY[-2,5])"),
            "null_element": cast("ARRAY['alpha',NULL,'omega']::text[]"),
            "quoted_unicode": cast("ARRAY['Ω','comma,value','\"quoted\"']::text[]"),
        }[value_case]
    if canonical == "enum":
        return cast({"escaped_unicode": "'β,\"quoted\"'", "delimiter_text": "'beta'"}[value_case])
    if canonical == "domain":
        return cast({"minimum": "'-9999999999.99'", "maximum": "'9999999999.99'"}[value_case])
    if canonical == "composite":
        return {
            "null_attribute": cast("ROW(NULL, NULL)"),
            "quoted_delimiter": cast("ROW('comma,\"quoted\"', -2147483648)"),
            "escaped_unicode": cast("ROW(E'Ω\\t\\n', 2147483647)"),
        }[value_case]
    if canonical in {"range", "range_builtin"}:
        return _range_value_expression(
            source_type=_catalog_base_type(declared_type),
            value_case=value_case,
            declared_type=declared_type,
        )
    if canonical in {"multirange", "multirange_builtin"}:
        return _multirange_value_expression(
            source_type=_catalog_base_type(declared_type),
            value_case=value_case,
            declared_type=declared_type,
        )
    return _default_explicit_boundary(
        canonical=canonical,
        source_type=normalized_source,
        declared_type=declared_type,
        source_column=column,
        value_case=value_case,
    )


def _geometric_value_expression(*, source_type: str, value_case: str, declared_type: str) -> str:
    values = {
        "point": {
            "degenerate": "'(0,0)'",
            "negative_coordinates": "'(-1e300,-1e300)'",
            "maximum_finite": "'(1e300,1e300)'",
        },
        "line": {
            "degenerate": "'{1,0,0}'",
            "negative_coordinates": "'{1,-2,-3}'",
            "maximum_finite": "'{1e300,1e300,1e300}'",
        },
        "lseg": {
            "degenerate": "'[(0,0),(0,0)]'",
            "negative_coordinates": "'[(-1e300,-1e300),(-1,-1)]'",
            "maximum_finite": "'[(1e300,1e300),(0,0)]'",
        },
        "box": {
            "degenerate": "'((0,0),(0,0))'",
            "negative_coordinates": "'((-1,-2),(-3,-4))'",
            "maximum_finite": "'((1e300,1e300),(0,0))'",
        },
        "path": {
            "degenerate": "'[(0,0),(0,0)]'",
            "negative_coordinates": "'[(-1,-2),(-3,-4)]'",
            "maximum_finite": "'[(1e300,1e300),(0,0)]'",
        },
        "polygon": {
            "degenerate": "'((0,0),(0,0),(0,0))'",
            "negative_coordinates": "'((-1,-2),(-3,-4),(-5,-6))'",
            "maximum_finite": "'((1e300,1e300),(0,0),(1,0))'",
        },
        "circle": {
            "degenerate": "'<(0,0),0>'",
            "negative_coordinates": "'<(-1e300,-1e300),1>'",
            "maximum_finite": "'<(1e300,1e300),1e300>'",
        },
    }
    return f"CAST({values[source_type][value_case]} AS {declared_type})"


def _catalog_base_type(declared_type: str) -> str:
    return declared_type.rsplit(".", 1)[-1].replace('"', "").strip().lower()


def _range_value_expression(*, source_type: str, value_case: str, declared_type: str) -> str:
    inclusive = {
        "int4range": "'[1,8)'",
        "int8range": "'[1,8000000000)'",
        "numrange": "'[1.25,8.75)'",
        "tsrange": "'[2026-01-01 00:00:00,2026-02-01 00:00:00)'",
        "tstzrange": "'[2026-01-01 00:00:00+03,2026-02-01 00:00:00+03)'",
        "daterange": "'[2026-01-01,2026-02-01)'",
    }
    value = {
        "empty": "'empty'",
        "unbounded": "'(,)'",
        "inclusive_exclusive": inclusive[source_type],
        "infinite_bound": "'(,)'",
    }[value_case]
    return f"CAST({value} AS {declared_type})"


def _multirange_value_expression(*, source_type: str, value_case: str, declared_type: str) -> str:
    disjoint = {
        "int4multirange": "'{[1,3),[5,8)}'",
        "int8multirange": "'{[1,3),[5000000000,8000000000)}'",
        "nummultirange": "'{[1.25,3.50),[5.75,8.00)}'",
        "tsmultirange": "'{[2026-01-01,2026-01-02),[2026-02-01,2026-02-02)}'",
        "tstzmultirange": "'{[2026-01-01 00:00+03,2026-01-02 00:00+03)}'",
        "datemultirange": "'{[2026-01-01,2026-01-02),[2026-02-01,2026-02-02)}'",
    }
    value = {
        "empty": "'{}'",
        "disjoint": disjoint[source_type],
        "unbounded": "'{(,)}'",
    }[value_case]
    return f"CAST({value} AS {declared_type})"


def _default_explicit_boundary(
    *,
    canonical: str,
    source_type: str,
    declared_type: str,
    source_column: str,
    value_case: str,
) -> str:
    assert value_case == "boundary"
    if canonical == "money":
        value = "'922337203685477.5807'"
    elif canonical == "acl":
        value = "'=arwdDxt/dpone'"
    elif canonical == "internal_char":
        value = "'Z'"
    elif canonical == "name":
        return f"CAST(repeat('Ω', 31) AS {declared_type})"
    elif canonical == "cursor":
        return f"CAST(repeat('portal_', 9) AS {declared_type})"
    elif canonical == "oid":
        value = {
            "xid8": "'18446744073709551615'",
            "tid": "'(4294967295,65535)'",
        }.get(source_type, "'4294967295'")
    elif canonical == "search":
        if source_type == "tsvector":
            return "to_tsvector('simple', repeat('Ω alpha ', 128))"
        return "to_tsquery('simple', 'alpha & beta | gamma')"
    elif canonical == "lsn":
        value = "'FFFFFFFF/FFFFFFFF'"
    elif canonical == "snapshot":
        value = "'10:4294967295:12,14'"
    elif canonical == "custom":
        value = "'Straße Ω CASE preserved'"
    elif canonical == "string":
        return f"CAST(repeat('Ω', 32) AS {declared_type})"
    else:
        return source_column
    return f"CAST({value} AS {declared_type})"


def _drop_explicit_cell_tables(postgres: PostgresConnector, *, suffix: str) -> None:
    rows = postgres.get_records(
        "SELECT c.relname FROM pg_catalog.pg_class AS c "
        "INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
        "WHERE n.nspname = %s AND c.relkind IN ('r','p') "
        "AND (c.relname LIKE %s OR c.relname = %s)",
        (SOURCE_SCHEMA, f"explicit_cell_{suffix}_%", f"explicit_cells_authority_{suffix}"),
        as_dict=True,
    )
    for row in rows:
        safe = str(row["relname"]).replace('"', '""')
        postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{safe}" CASCADE')


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_explicit_family_non_text_physical_override_rejects_before_copy_live(tmp_path: Path) -> None:
    """A physical override never substitutes for the required text landing."""

    suffix = uuid.uuid4().hex[:8]
    source_table = f"explicit_override_source_{suffix}"
    target_table = f"explicit_override_target_{suffix}"
    postgres = postgres_connector()
    mssql = mssql_connector()
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    create_explicit_type_table(postgres, schema=SOURCE_SCHEMA, table=source_table, suffix=suffix)
    drop_mssql_table(mssql, target_table)
    config = _config(
        source_table=source_table,
        target_table=target_table,
        tmp_path=tmp_path,
        contract=explicit_schema_contract(),
        physical_design={
            "columns": {"c_interval": {"target_type": {"mssql": "int"}}},
        },
    )

    strategy = PostgresFullExtractStrategy(postgres, logger=NoopLogger())
    bind_factual_postgres_source_authority(strategy, postgres, load_config=config)
    with pytest.raises(PostgresMssqlProjectionError) as raised:
        strategy.extract(config, None)
    assert raised.value.blocker == "postgres_mssql.type_contract.textual_target_required"
    assert raised.value.column == "c_interval"
    assert not mssql.table_exists("dpone_it", target_table)
    assert not tuple(tmp_path.iterdir())


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_vanilla_postgres_reports_postgis_as_unavailable_but_policy_is_explicit() -> None:
    """Pinned vanilla PostgreSQL rejects unapproved spatial shapes before row IO."""

    postgres = postgres_connector()
    available = postgres.get_records("SELECT COUNT(*) FROM pg_available_extensions WHERE name = 'postgis'")[0][0]
    assert int(available) == 0
    suite = TypeCertificationSuiteRegistry.default().suite("postgres", "mssql")
    by_name = {case.name: case for case in suite.cases}
    for case_id in sorted(POSTGIS_SPATIAL_CASES):
        source_type = by_name[case_id].source_type
        assert declared_postgres_mssql_contract_blockers((("shape", source_type),), {}) == (
            "postgres_mssql.type_contract.explicit_contract_required:shape",
        )
        assert not declared_postgres_mssql_contract_blockers(
            (("shape", source_type),),
            {"schema_contract": {"columns": {"shape": {"type": "string"}}}},
        )


def _postgis_connector() -> PostgresConnector:
    host = os.getenv("DPONE_IT_POSTGIS_HOST")
    if not host:
        pytest.skip("DPONE_IT_POSTGIS_HOST is not configured")
    return PostgresConnector(
        host=host,
        port=int(os.getenv("DPONE_IT_POSTGIS_PORT", "5432")),
        database=os.getenv("DPONE_IT_POSTGIS_DATABASE", "dpone_it"),
        user=os.getenv("DPONE_IT_POSTGIS_USER", "dpone"),
        password=os.getenv("DPONE_IT_POSTGIS_PASSWORD", "dpone"),
        application_name="dpone-postgis-certification",
    )


def _source_column_identity(postgres: PostgresConnector, table: str, column: str) -> dict[str, object]:
    rows = postgres.get_records(
        "SELECT a.attnum, a.attname, a.atttypid::bigint AS type_oid, "
        "pg_catalog.format_type(a.atttypid, a.atttypmod) AS declared_type, "
        "t.typtype, n.nspname AS type_schema, t.typname AS type_name "
        "FROM pg_catalog.pg_attribute AS a "
        "INNER JOIN pg_catalog.pg_class AS c ON c.oid = a.attrelid "
        "INNER JOIN pg_catalog.pg_namespace AS cn ON cn.oid = c.relnamespace "
        "INNER JOIN pg_catalog.pg_type AS t ON t.oid = a.atttypid "
        "INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = t.typnamespace "
        "WHERE cn.nspname = %s AND c.relname = %s AND a.attname = %s "
        "AND a.attnum > 0 AND NOT a.attisdropped",
        (SOURCE_SCHEMA, table, column),
        as_dict=True,
    )
    assert len(rows) == 1
    row = rows[0]
    return {
        "ordinal": int(row["attnum"]),
        "name": str(row["attname"]),
        "type_oid": int(row["type_oid"]),
        "declared_type": str(row["declared_type"]),
        "type_kind": str(row["typtype"]),
        "type_schema": str(row["type_schema"]),
        "type_name": str(row["type_name"]),
    }


def _target_object_count(mssql, table: str) -> int:
    rows = mssql.get_records(
        "SELECT COUNT_BIG(*) AS object_count FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = N'dpone_it' AND t.name = ?",
        (table,),
        as_dict=True,
    )
    return int(rows[0]["object_count"])


def _create_postgis_boundary_table(postgres: PostgresConnector, *, table: str) -> tuple[str, ...]:
    postgres.execute_query("CREATE EXTENSION IF NOT EXISTS postgis")
    postgres.execute_query("CREATE EXTENSION IF NOT EXISTS postgis_raster")
    postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{SOURCE_SCHEMA}"')
    postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{table}" CASCADE')
    postgres.execute_query(
        f'CREATE TABLE "{SOURCE_SCHEMA}"."{table}" ('
        "id integer NOT NULL PRIMARY KEY, "
        "geom_point geometry(Point,4326) NOT NULL, "
        "geom_boundary geometry(Geometry,4326) NOT NULL, "
        "geog_point geography(Point,4326) NOT NULL, "
        "rast raster NOT NULL)"
    )
    postgres.execute_query(
        f'INSERT INTO "{SOURCE_SCHEMA}"."{table}" '
        "(id, geom_point, geom_boundary, geog_point, rast) VALUES "
        "(1, ST_SetSRID(ST_MakePoint(180,-90),4326), "
        "ST_GeomFromText('LINESTRING(-180 -90,180 90)',4326), "
        "ST_SetSRID(ST_MakePoint(-180,90),4326)::geography, "
        "ST_AddBand(ST_MakeEmptyRaster(2,2,0,0,1,-1,0,0,4326),'8BUI'::text,255,0)), "
        "(2, ST_GeomFromText('POINT EMPTY',4326), "
        "ST_GeomFromText('GEOMETRYCOLLECTION EMPTY',4326), "
        "ST_GeogFromText('SRID=4326;POINT EMPTY'), "
        "ST_MakeEmptyRaster(1,1,0,0,1,-1,0,0,4326))"
    )
    return ("geom_point", "geom_boundary", "geog_point", "rast")


def _postgis_contract(columns: tuple[str, ...], *, excluded: str | None = None, wrong: str | None = None):
    contract_columns: dict[str, dict[str, object]] = {"id": {"type": "integer", "nullable": False}}
    for column in columns:
        if column == excluded:
            continue
        contract_columns[column] = {
            "type": "number" if column == wrong else "string",
            "nullable": False,
        }
    return {"enforcement": "strict", "columns": contract_columns}


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgis_spatial_families_reject_missing_or_wrong_contract_before_copy_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Execute all contract-negative partitions against real PostGIS catalogs."""

    suffix = uuid.uuid4().hex[:8]
    source_table = f"postgis_policy_source_{suffix}"
    target_table = f"postgis_policy_target_{suffix}"
    postgres = _postgis_connector()
    mssql = mssql_connector()
    columns = _create_postgis_boundary_table(postgres, table=source_table)
    ensure_mssql_database_and_schemas()
    drop_mssql_table(mssql, target_table)
    strategy = PostgresFullExtractStrategy(postgres, logger=NoopLogger())

    case_by_column = {
        "geom_point": "geometry",
        "geom_boundary": "postgis_catalog",
        "geog_point": "geography",
        "rast": "postgis_raster",
    }
    assert set(case_by_column) == set(columns)
    for column in columns:
        source_identity = _source_column_identity(postgres, source_table, column)
        for contract_case, contract, physical_design in (
            ("missing_contract", _postgis_contract(columns, excluded=column), None),
            (
                "physical_only",
                _postgis_contract(columns, excluded=column),
                {
                    "columns": {
                        column: {
                            "target_type": {"mssql": "nvarchar(max)"},
                        }
                    }
                },
            ),
            ("wrong_logical_type", _postgis_contract(columns, wrong=column), None),
        ):
            config = _config(
                source_table=source_table,
                target_table=target_table,
                tmp_path=tmp_path,
                contract=contract,
                physical_design=physical_design,
            )
            before = {
                "target_object_count": _target_object_count(mssql, target_table),
                "source_column": source_identity,
                "artifact_entries": sorted(path.name for path in tmp_path.iterdir()),
            }
            with pytest.raises(PostgresMssqlProjectionError) as raised:
                strategy.fetch_schema_projection(config)
            assert raised.value.blocker == "postgres_mssql.type_contract.explicit_contract_required"
            assert raised.value.column == column
            after = {
                "target_object_count": _target_object_count(mssql, target_table),
                "source_column": _source_column_identity(postgres, source_table, column),
                "artifact_entries": sorted(path.name for path in tmp_path.iterdir()),
            }
            assert after == before
            route_live_recorder.observe_case(
                "postgis_types",
                f"{case_by_column[column]}__{contract_case}",
                before_image=before,
                after_image=after,
                observations={
                    "blocker": raised.value.blocker,
                    "column": raised.value.column,
                    "blocked_before_source_copy": True,
                },
            )


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgis_spatial_families_roundtrip_exact_catalog_text_standard_etl_live(
    tmp_path: Path,
) -> None:
    """Geometry, geography and raster round-trip exact ``::text`` bytes."""

    suffix = uuid.uuid4().hex[:8]
    source_table = f"postgis_roundtrip_source_{suffix}"
    target_table = f"postgis_roundtrip_target_{suffix}"
    postgres = _postgis_connector()
    mssql = mssql_connector()
    columns = _create_postgis_boundary_table(postgres, table=source_table)
    ensure_mssql_database_and_schemas()
    drop_mssql_table(mssql, target_table)
    config = _config(
        source_table=source_table,
        target_table=target_table,
        tmp_path=tmp_path,
        contract=_postgis_contract(columns),
    )
    logger = QuietIntegrationLogger()
    with governed_mssql_route(
        mssql,
        target_database=str(config.target_database),
        target_schema=config.target_schema,
        target_table=config.target_table,
    ) as route:
        result = GovernedStandardEtlRunner(
            route,
            postgres,
            logger=logger,
        ).run(
            config,
            label="postgis_types",
        )

    serialized_columns = ", ".join(f'"{column}"::text AS "{column}"' for column in columns)
    expected = postgres.get_records(
        f'SELECT id, {serialized_columns} FROM "{SOURCE_SCHEMA}"."{source_table}" ORDER BY id',
        as_dict=True,
    )
    actual = mssql.get_records(
        f"SELECT [id], {', '.join(f'[{column}]' for column in columns)} FROM [dpone_it].[{target_table}] ORDER BY [id]",
        as_dict=True,
    )
    versions = postgres.get_records(
        "SELECT postgis_full_version(), (SELECT extversion FROM pg_extension WHERE extname = 'postgis_raster')"
    )[0]

    assert result["status"] == "success"
    assert result["loaded_rows"] == 2
    assert actual == expected
    assert 'POSTGIS="3.5.7' in str(versions[0])
    assert str(versions[1]) == "3.5.7"
    assert not tuple(tmp_path.iterdir())


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgis_reviewed_boundary_cells_roundtrip_exact_text_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Execute each reviewed geometry/geography/raster value independently."""

    suffix = uuid.uuid4().hex[:8]
    postgres = _postgis_connector()
    postgres.execute_query("CREATE EXTENSION IF NOT EXISTS postgis")
    postgres.execute_query("CREATE EXTENSION IF NOT EXISTS postgis_raster")
    postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{SOURCE_SCHEMA}"')
    mssql = governed_mssql_live_campaign.target
    logger = QuietIntegrationLogger()
    cases = _postgis_reviewed_value_expressions()
    assert len(cases) == 20
    try:
        for ordinal, (type_case, value_case, expression) in enumerate(cases):
            source_table = f"postgis_cell_{suffix}_{ordinal:02d}"
            target_table = f"postgis_cell_target_{suffix}_{ordinal:02d}"
            postgres.execute_query(
                f'CREATE VIEW "{SOURCE_SCHEMA}"."{source_table}" AS SELECT 1::integer AS id, {expression} AS value'
            )
            config = _config(
                source_table=source_table,
                target_table=target_table,
                tmp_path=tmp_path,
                contract={
                    "enforcement": "strict",
                    "columns": {
                        "id": {"type": "integer", "nullable": True},
                        "value": {"type": "string", "nullable": True},
                    },
                },
            )
            config.target_database = governed_mssql_live_campaign.target_database
            config.staging_database = governed_mssql_live_campaign.target_database
            route = governed_mssql_live_campaign.route(
                target_schema=config.target_schema,
                target_table=config.target_table,
            )
            runner = GovernedStandardEtlRunner(
                route,
                postgres,
                logger=logger,
                source_type=GovernedPostgresSnapshotSource,
            )
            before = {
                "target_object_count": _target_object_count(mssql, target_table),
                "source_column": _source_column_identity(postgres, source_table, "value"),
            }
            expected = postgres.get_records(
                f'SELECT value::text AS value FROM "{SOURCE_SCHEMA}"."{source_table}"',
                as_dict=True,
            )
            result = runner.run(config, label=f"postgis_{type_case}_{value_case}")
            actual = mssql.get_records(
                f"SELECT [value] FROM [dpone_it].[{target_table}] ORDER BY [id]",
                as_dict=True,
            )
            catalog = mssql.get_records(
                "SELECT ty.name AS type_name, c.max_length "
                "FROM sys.columns AS c "
                "INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id "
                "WHERE c.object_id = OBJECT_ID(?) AND c.name = N'value'",
                (f"dpone_it.{target_table}",),
                as_dict=True,
            )
            assert result["status"] == "success"
            assert result["loaded_rows"] == 1
            assert actual == expected
            assert catalog == [{"type_name": "nvarchar", "max_length": -1}]
            assert _target_staging_count(mssql, target_table) == 0
            after = {
                "target_object_count": _target_object_count(mssql, target_table),
                "source_column": _source_column_identity(postgres, source_table, "value"),
                "target_catalog": catalog,
                "source_text": expected,
                "target_text": actual,
            }
            route_live_recorder.observe_case(
                "postgis_types",
                f"{type_case}__string_contract__{value_case}",
                before_image=before,
                after_image=after,
                observations={
                    "loaded_rows": 1,
                    "exact_postgis_text": actual == expected,
                    "staging_objects_after": 0,
                },
            )
            mssql.execute_query(f"DROP TABLE [dpone_it].[{target_table}]")
            postgres.execute_query(f'DROP VIEW "{SOURCE_SCHEMA}"."{source_table}"')
    finally:
        for row in postgres.get_records(
            "SELECT c.relname FROM pg_catalog.pg_class AS c "
            "INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
            "WHERE n.nspname = %s AND c.relkind = 'v' AND c.relname LIKE %s",
            (SOURCE_SCHEMA, f"postgis_cell_{suffix}_%"),
            as_dict=True,
        ):
            safe = str(row["relname"]).replace('"', '""')
            postgres.execute_query(f'DROP VIEW IF EXISTS "{SOURCE_SCHEMA}"."{safe}" CASCADE')


def _postgis_reviewed_value_expressions() -> tuple[tuple[str, str, str], ...]:
    geometry = {
        "null": "NULL::geometry",
        "point_srid_4326": "ST_SetSRID(ST_MakePoint(12.5,-45.25),4326)",
        "empty": "ST_GeomFromText('GEOMETRYCOLLECTION EMPTY',4326)",
        "negative_boundary": "ST_GeomFromText('LINESTRING(-180 -90,180 90)',4326)",
        "z_dimension": "ST_SetSRID(ST_MakePoint(1,2,3),4326)",
    }
    geography = {
        "null": "NULL::geography",
        "point_srid_4326": "ST_SetSRID(ST_MakePoint(12.5,-45.25),4326)::geography",
        "antimeridian": "ST_GeogFromText('SRID=4326;LINESTRING(179 0,-179 0)')",
        "polar_boundary": "ST_GeogFromText('SRID=4326;POINT(0 89.999999)')",
        "empty": "ST_GeogFromText('SRID=4326;POINT EMPTY')",
    }
    raster = {
        "null": "NULL::raster",
        "one_pixel": ("ST_AddBand(ST_MakeEmptyRaster(1,1,0,0,1,-1,0,0,4326),'8BUI'::text,255,0)"),
        "empty": "ST_MakeEmptyRaster(1,1,0,0,1,-1,0,0,4326)",
        "nodata": ("ST_AddBand(ST_MakeEmptyRaster(2,2,0,0,1,-1,0,0,4326),'8BUI'::text,0,0)"),
        "multi_band": (
            "ST_AddBand(ST_AddBand(ST_MakeEmptyRaster(2,2,0,0,1,-1,0,0,4326),'8BUI'::text,1,0),'16BUI'::text,1024,0)"
        ),
    }
    return (
        *(
            (type_case, value_case, expression)
            for type_case in ("geometry", "postgis_catalog")
            for value_case, expression in geometry.items()
        ),
        *(("geography", value_case, expression) for value_case, expression in geography.items()),
        *(("postgis_raster", value_case, expression) for value_case, expression in raster.items()),
    )


def _target_staging_count(mssql, target_table: str) -> int:
    rows = mssql.get_records(
        "SELECT COUNT_BIG(*) AS object_count FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = N'staging' AND t.name LIKE ?",
        (f"stg_{target_table}_%",),
        as_dict=True,
    )
    return int(rows[0]["object_count"])
