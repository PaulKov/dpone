"""Docker Postgres → MSSQL vendor-live: wide types + all MSSQL load strategies.

``bytea`` uses hex character BCP + ``CONVERT(varbinary, …, 2)`` decode.
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any

import pytest
from tools.route_live_certification.recorder import RouteLiveObservationRecorder
from tools.route_live_certification.reviewed_cases_types import boundary_type_suite

from dpone.config import LoadConfig, LoadStrategy
from dpone.config.mssql_strategy_contract import MSSQLStrategyContractError
from dpone.contracts.postgres_incremental_cursor import POSTGRES_MSSQL_COLUMN_CURSOR_ERROR_CODE
from dpone.contracts.postgres_mssql_type_policy import declared_postgres_mssql_contract_blockers
from dpone.runtime.connectors.mssql import MSSQLDatetimeOffsetDecodeError
from dpone.runtime.etl.file_contract_validation import FileContractValidationError
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.postgres import PostgresSource
from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy
from dpone.runtime.sources.strategies.postgres.postgres_incremental_extract import (
    PostgresIncrementalExtractStrategy,
)
from dpone.runtime.sources.strategies.postgres.postgres_mssql_source_value_guard import (
    PostgresMssqlSourceValueError,
)
from dpone.runtime.streaming_rows import StreamingRowsArtifact
from dpone.type_system.source_sink.certification_suites import TypeCertificationSuiteRegistry
from tests.integration.postgres import postgres_mssql_strategy_configs as cfg
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
from tests.integration.postgres.postgres_mssql_assertions import (
    WideRoundtripProof,
    assert_complete_wide_roundtrip,
    assert_row_count,
    assert_unique_ids,
    fq,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedMssqlCampaign,
    GovernedPostgresSnapshotSource,
    GovernedStandardEtlRunner,
    bind_factual_postgres_source_authority,
)
from tests.integration.postgres.postgres_mssql_wide_fixtures import (
    SOURCE_SCHEMA,
    WIDE_COLUMN_TARGET,
    WIDE_TABLE,
    create_wide_postgres_table,
    insert_wide_watermark_row,
    wide_columns,
)
from tests.integration.postgres.postgres_xmin_mssql_snapshot_live_support import (
    QuietIntegrationLogger,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.integration_live,
    pytest.mark.integration_postgres,
    pytest.mark.integration_mssql,
]

_EVIDENCE_ROOT = Path("test_artifacts/live_certification/postgres_mssql_wide_strategy_cases")
_EVIDENCE_SUMMARY = Path("test_artifacts/live_certification/postgres_mssql_wide_strategy_matrix.json")
_MATRIX_CASES = frozenset(
    {
        "full_refresh",
        "incremental_append",
        "incremental_merge",
        "replace",
        "partition_replace",
        "snapshot_diff",
        "scd2",
        "backfill",
    }
)
_FAIL_CLOSED_BLOCKERS = {
    "incremental_append": POSTGRES_MSSQL_COLUMN_CURSOR_ERROR_CODE,
    "incremental_merge": POSTGRES_MSSQL_COLUMN_CURSOR_ERROR_CODE,
    "replace": "mssql.strategy.replace.cross_dialect_raw_predicate",
    "backfill": "mssql.strategy.backfill.replace_cross_dialect_raw_predicate",
}
_FAIL_CLOSED_CASES = frozenset(_FAIL_CLOSED_BLOCKERS)
_SUPPORTED_CASES = _MATRIX_CASES - _FAIL_CLOSED_CASES


class _PostgresCopyToStreamingAdapter:
    """Test adapter that changes only the completed artifact representation.

    The delegate still performs a real PostgreSQL catalog projection and COPY.
    The adapter decodes that immutable, receipted character wire into a streaming
    row artifact while preserving all source provenance.  This certifies
    file/stream semantic parity without pretending that the adapter is a second
    PostgreSQL connector implementation.
    """

    def __init__(self, delegate: PostgresSource) -> None:
        self._delegate = delegate

    def __getattr__(self, name: str) -> Any:
        return getattr(self._delegate, name)

    def get_incremental_state(self, load_config: LoadConfig) -> Any | None:
        return self._delegate.get_incremental_state(load_config)

    def extract(self, load_config: LoadConfig, last_state: Any | None) -> ExtractResult:
        extracted = self._delegate.extract(load_config, last_state)
        artifact = extracted.artifact
        if not isinstance(artifact, FileExportArtifact) or artifact.format != "mssql-delimited":
            raise AssertionError("transport parity adapter requires a completed mssql-delimited COPY artifact")
        codec = artifact.bulk_text_codec
        if codec is None:
            raise AssertionError("transport parity adapter requires BulkTextCodec authority")
        rows: list[dict[str, object]] = []
        try:
            for raw_line in Path(artifact.file_path).read_bytes().splitlines():
                fields = raw_line.split(codec.field_terminator.encode("utf-8"))
                if len(fields) != len(artifact.columns):
                    raise AssertionError("transport parity adapter observed a malformed COPY record")
                rows.append(
                    {
                        str(column): (None if value == b"" else codec.decode(value.decode("utf-8")))
                        for column, value in zip(artifact.columns, fields, strict=True)
                    }
                )
        finally:
            artifact.cleanup()
        return replace(
            extracted,
            artifact=StreamingRowsArtifact(iter(rows), batch_size=1, estimated_rows=len(rows)),
        )


class _PostgresCopyToStreamingSource(_PostgresCopyToStreamingAdapter):
    """Constructor-compatible governed source for transport parity cases."""

    def __init__(
        self,
        connector: Any,
        state_storage: Any,
        logger: Any,
        *,
        sink_connector: Any,
    ) -> None:
        super().__init__(
            PostgresSource(
                connector,
                state_storage,
                logger,
                sink_connector=sink_connector,
            )
        )


def _governed_runner(
    campaign: GovernedMssqlCampaign,
    postgres: Any,
    config: LoadConfig,
    *,
    logger: Any,
    source_type: type[PostgresSource] = PostgresSource,
) -> GovernedStandardEtlRunner:
    """Bind one exact target and route every live load through admission."""

    config.target_database = campaign.target_database
    return GovernedStandardEtlRunner(
        campaign.route(
            target_schema=config.target_schema,
            target_table=config.target_table,
        ),
        postgres,
        logger=logger,
        source_type=source_type,
    )


@pytest.fixture(scope="module", autouse=True)
def _fresh_strategy_evidence() -> None:
    """Prevent an interrupted earlier run from being mistaken for evidence."""

    shutil.rmtree(_EVIDENCE_ROOT, ignore_errors=True)
    _EVIDENCE_SUMMARY.unlink(missing_ok=True)


def _certifies(strategy: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Record one case only after its vendor assertions returned successfully."""

    def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(function)
        def certified(*args: Any, **kwargs: Any) -> Any:
            result = function(*args, **kwargs)
            proof = result.as_evidence() if isinstance(result, WideRoundtripProof) else {}
            recorder = kwargs.get("route_live_recorder")
            if not isinstance(recorder, RouteLiveObservationRecorder):
                raise AssertionError("route-live recorder fixture was not injected")
            recorder.observe_case(
                "wide_strategy",
                strategy,
                before_image={"target_exists": False, "rows": [], "catalog": []},
                after_image={"target_exists": True, **proof},
                observations={
                    "wide_column_count": WIDE_COLUMN_TARGET,
                    "complete_value_and_catalog_assertion": bool(proof),
                },
            )
            _EVIDENCE_ROOT.mkdir(parents=True, exist_ok=True)
            (_EVIDENCE_ROOT / f"{strategy}.json").write_text(
                json.dumps(
                    {
                        "schema_version": "dpone.postgres_mssql.wide_strategy_case.v1",
                        "source": "postgres",
                        "sink": "mssql",
                        "strategy": strategy,
                        "passed": True,
                        "vendor_transactions": True,
                        "connector_doubles": False,
                        "wide_column_count": WIDE_COLUMN_TARGET,
                        "complete_value_and_catalog_assertion": bool(proof),
                        **proof,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )
            return None

        return certified

    return decorate


def _certifies_fail_closed(strategy: str) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Record an intentionally unsupported case only after its pre-I/O rejection."""

    if strategy not in _FAIL_CLOSED_CASES:
        raise ValueError(f"Unknown fail-closed strategy evidence case: {strategy}")
    blocker = _FAIL_CLOSED_BLOCKERS[strategy]

    def decorate(function: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(function)
        def certified(*args: Any, **kwargs: Any) -> None:
            function(*args, **kwargs)
            recorder = kwargs.get("route_live_recorder")
            if not isinstance(recorder, RouteLiveObservationRecorder):
                raise AssertionError("route-live recorder fixture was not injected")
            absent_image = {"target_exists": False, "staging_objects": 0}
            recorder.observe_case(
                "wide_strategy",
                strategy,
                before_image=absent_image,
                after_image=absent_image,
                observations={
                    "blocker": blocker,
                    "fail_closed": True,
                },
            )
            _EVIDENCE_ROOT.mkdir(parents=True, exist_ok=True)
            (_EVIDENCE_ROOT / f"{strategy}.json").write_text(
                json.dumps(
                    {
                        "schema_version": "dpone.postgres_mssql.wide_strategy_case.v1",
                        "source": "postgres",
                        "sink": "mssql",
                        "strategy": strategy,
                        "passed": True,
                        "supported": False,
                        "fail_closed": True,
                        "blocker": blocker,
                        "vendor_transactions": True,
                        "connector_doubles": False,
                        "wide_column_count": WIDE_COLUMN_TARGET,
                        "complete_value_and_catalog_assertion": False,
                    },
                    indent=2,
                    sort_keys=True,
                )
                + "\n",
                encoding="utf-8",
            )

        return certified

    return decorate


def _ready(table: str):
    postgres = postgres_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    mssql = mssql_connector()
    wait_until_ready("mssql", lambda: mssql.get_records("SELECT 1"))
    drop_mssql_table(mssql, table)
    postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{table}" CASCADE')
    columns = create_wide_postgres_table(postgres, table=table, schema=SOURCE_SCHEMA)
    return postgres, mssql, columns


def _clone_complete_target_sentinel(
    mssql,
    *,
    table: str,
    row_id: int,
    business_date: str,
    updated_at: str,
    name: str,
) -> None:
    """Clone one physical row so every non-null wide column stays populated."""

    columns = mssql.get_records(
        "SELECT c.name FROM sys.columns AS c "
        "INNER JOIN sys.tables AS t ON t.object_id = c.object_id "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name = ? AND c.is_computed = 0 AND c.is_identity = 0 "
        "ORDER BY c.column_id",
        (cfg.TARGET_SCHEMA, table),
        as_dict=True,
    )
    names = tuple(str(row["name"]) for row in columns)
    assert {"id", "business_date", "updated_at", "c_name"} <= set(names)
    quoted = ", ".join(f"[{column}]" for column in names)
    expressions = {
        "id": str(row_id),
        "business_date": f"CAST('{business_date}' AS date)",
        "updated_at": f"CAST('{updated_at}' AS datetime2(6))",
        "c_name": "N'" + name.replace("'", "''") + "'",
    }
    select = ", ".join(expressions.get(column, f"[{column}]") for column in names)
    mssql.execute_query(f"INSERT INTO {fq(table)} ({quoted}) SELECT {select} FROM {fq(table)} WHERE [id] = 1")


def test_wide_fixture_covers_authoritative_postgres_mssql_type_suite() -> None:
    """Classify every canonical case without treating text padding as coverage."""

    columns = wide_columns()
    certified = TypeCertificationSuiteRegistry.default().suite("postgres", "mssql")
    covered_types = {column.pg_type for column in columns}
    targets_by_type = {column.pg_type: column.expected_mssql_type for column in columns}
    live_explicit_representatives = {
        "array_contract_required": "c_array",
        "array_catalog": "c_array",
        "enum_contract_required": "c_enum",
        "range_contract_required": "c_range",
        "range_catalog": "c_range",
    }
    classifications: dict[str, str] = {}

    assert len(columns) == WIDE_COLUMN_TARGET
    for case in certified.cases:
        if case.decision_category == "auto_inferred":
            assert case.source_spec in covered_types
            assert targets_by_type[case.source_spec] == case.expected_target_type
            classifications[case.name] = "supported_roundtrip"
            continue
        blocker = declared_postgres_mssql_contract_blockers(
            (("certification_value", case.source_type),),
            {},
        )
        assert blocker == ("postgres_mssql.type_contract.explicit_contract_required:certification_value",)
        classifications[case.name] = (
            "supported_with_explicit_contract"
            if case.name in live_explicit_representatives
            else "typed_reject_without_contract"
        )
    assert set(classifications) == {case.name for case in certified.cases}
    assert set(classifications.values()) == {
        "supported_roundtrip",
        "supported_with_explicit_contract",
        "typed_reject_without_contract",
    }
    assert {"text[]", "my_enum", "int4range"} <= covered_types


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_mssql_connector_decodes_vendor_datetimeoffset_struct_live() -> None:
    """Prove representable scale-6 values decode and scale-7 loss is rejected."""

    connector = mssql_connector()
    try:
        row = connector.get_records(
            "SELECT "
            "CAST('2026-08-15T12:34:56.123456+05:30' AS datetimeoffset(6)), "
            "CAST('2026-08-15T12:34:56.999999-05:30' AS datetimeoffset(6)), "
            "CAST('2026-08-15T12:34:56.000000+00:00' AS datetimeoffset(6))"
        )[0]
        with pytest.raises(MSSQLDatetimeOffsetDecodeError) as raised:
            connector.get_records("SELECT CAST('2026-08-15T12:34:56.1234567+05:30' AS datetimeoffset(7))")
    finally:
        connector.close()

    positive, negative, zero = row
    assert positive == datetime(2026, 8, 15, 12, 34, 56, 123_456, tzinfo=positive.tzinfo)
    assert positive.utcoffset() == timedelta(hours=5, minutes=30)
    assert negative == datetime(2026, 8, 15, 12, 34, 56, 999_999, tzinfo=negative.tzinfo)
    assert negative.utcoffset() == -timedelta(hours=5, minutes=30)
    assert zero == datetime(2026, 8, 15, 12, 34, 56, tzinfo=zero.tzinfo)
    assert zero.utcoffset() == timedelta(0)
    assert raised.value.code == "DPONE_MSSQL_DATETIMEOFFSET_DECODE_FAILED"


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_to_mssql_standard_etl_full_refresh_wire_lifecycle_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> None:
    """Certify COPY→receipt→BCP→native decoding for the full control corpus."""

    source_table = "standard_etl_wire_source"
    target_table = "standard_etl_wire_target"
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    drop_mssql_table(mssql, target_table)
    postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{source_table}" CASCADE')
    postgres.execute_query(
        f'CREATE TABLE "{SOURCE_SCHEMA}"."{source_table}" ('
        "id integer NOT NULL, value character varying(64) NOT NULL, note text NULL)"
    )
    expected_rows = [
        {"id": 1, "value": "tab\tline\nmarker\x1d", "note": None},
        {"id": 2, "value": "", "note": "Привет Ω"},
        {"id": 3, "value": "\t\n\r", "note": "all-delimiters"},
        {"id": 4, "value": "\x1fstart", "note": "end\x1f"},
        {"id": 5, "value": "\x1d\x1e\x1f", "note": "\x1d\x1dE\x1dN\x1dP"},
        {"id": 6, "value": "\\backslash", "note": "\t\x1dN\n\x1f\r\x1e"},
    ]
    for row in expected_rows:
        postgres.execute_query(
            f'INSERT INTO "{SOURCE_SCHEMA}"."{source_table}" (id, value, note) VALUES (%s, %s, %s)',
            (row["id"], row["value"], row["note"]),
        )
    config = LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        target_database=mssql.database,
        source_schema=SOURCE_SCHEMA,
        source_table=source_table,
        target_schema="dpone_it",
        target_table=target_table,
        staging_schema="staging",
        staging_database=mssql.database,
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "work_dir": str(tmp_path),
            "technical_columns": "forbidden",
            "schema_contract": {
                "enforcement": "strict",
                "columns": {
                    "id": {"type": "integer", "nullable": False},
                    "value": {"type": "string", "nullable": False},
                    "note": {"type": "string", "nullable": True},
                },
            },
        },
    )
    logger = QuietIntegrationLogger()
    result = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=logger,
    ).run(config, label="standard_wire_control_corpus")
    rows = mssql.get_records(
        f"SELECT [id], [value], [note] FROM [dpone_it].[{target_table}] ORDER BY [id]",
        as_dict=True,
    )

    assert result["status"] == "success"
    assert result["loaded_rows"] == len(expected_rows)
    assert rows == expected_rows
    assert not tuple(tmp_path.iterdir())


@pytest.mark.parametrize(
    (
        "reviewed_case_id",
        "pg_type",
        "source_literal",
        "mssql_type",
        "pg_send_function",
        "expect_lossy",
    ),
    (
        pytest.param(
            "real__negative_zero",
            "real",
            "'-0'::real",
            "real",
            "float4send",
            True,
            id="real-negative-zero",
        ),
        pytest.param(
            "real_catalog_float4__negative_zero",
            "float4",
            "'-0'::float4",
            "real",
            "float4send",
            True,
            id="float4-negative-zero",
        ),
        pytest.param(
            "double_precision__negative_zero",
            "double precision",
            "'-0'::double precision",
            "float(53)",
            "float8send",
            True,
            id="double-precision-negative-zero",
        ),
        pytest.param(
            "double_catalog_float8__negative_zero",
            "float8",
            "'-0'::float8",
            "float(53)",
            "float8send",
            True,
            id="float8-negative-zero",
        ),
        pytest.param(
            "real__minimum_positive_subnormal",
            "real",
            "'1e-45'::real",
            "real",
            "float4send",
            False,
            id="real-positive-subnormal",
        ),
        pytest.param(
            "real_catalog_float4__minimum_positive_subnormal",
            "float4",
            "'1e-45'::float4",
            "real",
            "float4send",
            False,
            id="float4-positive-subnormal",
        ),
        pytest.param(
            "double_precision__minimum_positive_subnormal",
            "double precision",
            "'5e-324'::double precision",
            "float(53)",
            "float8send",
            True,
            id="double-precision-positive-subnormal",
        ),
        pytest.param(
            "double_catalog_float8__minimum_positive_subnormal",
            "float8",
            "'5e-324'::float8",
            "float(53)",
            "float8send",
            True,
            id="float8-positive-subnormal",
        ),
        pytest.param(
            "real__maximum_negative_subnormal",
            "real",
            "'-1e-45'::real",
            "real",
            "float4send",
            False,
            id="real-negative-subnormal",
        ),
        pytest.param(
            "real_catalog_float4__maximum_negative_subnormal",
            "float4",
            "'-1e-45'::float4",
            "real",
            "float4send",
            False,
            id="float4-negative-subnormal",
        ),
        pytest.param(
            "double_precision__maximum_negative_subnormal",
            "double precision",
            "'-5e-324'::double precision",
            "float(53)",
            "float8send",
            True,
            id="double-precision-negative-subnormal",
        ),
        pytest.param(
            "double_catalog_float8__maximum_negative_subnormal",
            "float8",
            "'-5e-324'::float8",
            "float(53)",
            "float8send",
            True,
            id="float8-negative-subnormal",
        ),
    ),
)
@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_to_mssql_float_ieee_identity_is_exact_or_fails_closed_live(
    tmp_path: Path,
    reviewed_case_id: str,
    pg_type: str,
    source_literal: str,
    mssql_type: str,
    pg_send_function: str,
    expect_lossy: bool,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Preserve IEEE bits exactly or reject before target mutation."""

    suffix = uuid.uuid4().hex[:8]
    source_table = f"float_loss_source_{suffix}"
    target_table = f"float_loss_target_{suffix}"
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    postgres.execute_query(
        f'CREATE TABLE "{SOURCE_SCHEMA}"."{source_table}" (id integer NOT NULL, value {pg_type} NOT NULL)'
    )
    postgres.execute_query(f'INSERT INTO "{SOURCE_SCHEMA}"."{source_table}" VALUES (1, {source_literal})')
    mssql.execute_query(f"CREATE TABLE [dpone_it].[{target_table}] ([id] int NOT NULL, [value] {mssql_type} NOT NULL)")
    mssql.execute_query(
        f"INSERT INTO [dpone_it].[{target_table}] ([id], [value]) VALUES (99, CONVERT({mssql_type}, 1))"
    )
    target_rows_query = (
        f"SELECT [id], CONVERT(varbinary(8), [value]) AS [value_bits] FROM [dpone_it].[{target_table}] ORDER BY [id]"
    )
    staging_count_query = (
        "SELECT COUNT_BIG(*) AS object_count FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name LIKE ?"
    )
    before = {
        "target_rows": mssql.get_records(target_rows_query, as_dict=True),
        "staging_objects": int(
            mssql.get_records(
                staging_count_query,
                ("staging", f"stg_{target_table}_%"),
                as_dict=True,
            )[0]["object_count"]
        ),
        "artifact_entries": sorted(path.name for path in tmp_path.iterdir()),
    }
    config = LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        target_database=mssql.database,
        source_schema=SOURCE_SCHEMA,
        source_table=source_table,
        target_schema="dpone_it",
        target_table=target_table,
        staging_schema="staging",
        staging_database=mssql.database,
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "work_dir": str(tmp_path),
            "technical_columns": "forbidden",
            "lineage": False,
        },
    )
    logger = QuietIntegrationLogger()
    runner = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=logger,
    )
    if expect_lossy:
        with pytest.raises(
            SnapshotReconciliationError,
            match="mssql_native_projection.value_lossy",
        ):
            runner.run(config, label=f"float_bit_loss_{suffix}")
    else:
        result = runner.run(config, label=f"float_bit_exact_{suffix}")
        assert result["status"] == "success"
        assert result["loaded_rows"] == 1

    after = {
        "target_rows": mssql.get_records(target_rows_query, as_dict=True),
        "staging_objects": int(
            mssql.get_records(
                staging_count_query,
                ("staging", f"stg_{target_table}_%"),
                as_dict=True,
            )[0]["object_count"]
        ),
        "artifact_entries": sorted(path.name for path in tmp_path.iterdir()),
    }
    if expect_lossy:
        assert after == before
        exact_ieee_bits = None
    else:
        assert after != before
        source_bits = str(
            postgres.get_records(
                f'SELECT encode({pg_send_function}(value), \'hex\') AS bits FROM "{SOURCE_SCHEMA}"."{source_table}"',
                as_dict=True,
            )[0]["bits"]
        )
        target_rows = after["target_rows"]
        assert len(target_rows) == 1
        target_bits = bytes(target_rows[0]["value_bits"]).hex()
        assert target_bits == source_bits
        exact_ieee_bits = source_bits
    assert not tuple(tmp_path.iterdir())
    route_live_recorder.observe_case(
        "boundary_types",
        reviewed_case_id,
        before_image=before,
        after_image=after,
        observations={
            "blocker": "mssql_native_projection.value_lossy" if expect_lossy else None,
            "source_type": pg_type,
            "source_literal": source_literal,
            "target_type": mssql_type,
            "expected_lossy_rejection": expect_lossy,
            "exact_ieee_bits": exact_ieee_bits,
            "target_ieee_bits_preserved": not expect_lossy,
            "staging_cleanup_asserted": True,
            "artifact_cleanup_asserted": True,
        },
    )


_IEEE_BOUNDARY_CASES = frozenset(
    {
        "negative_zero",
        "minimum_positive_subnormal",
        "maximum_negative_subnormal",
    }
)


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_every_other_reviewed_auto_boundary_cell_executes_independently_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    """Execute every non-IEEE auto boundary against both real vendors."""

    suffix = uuid.uuid4().hex[:6]
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    wait_until_ready("mssql", lambda: mssql.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()

    reviewed_cases = [
        reviewed
        for reviewed in boundary_type_suite().cases
        if not _is_separately_certified_ieee_boundary(reviewed.config_json)
    ]
    requested_case_ids = tuple(
        case_id.strip()
        for case_id in os.environ.get("DPONE_ROUTE_LIVE_BOUNDARY_CASE_IDS", "").split(",")
        if case_id.strip()
    )
    if requested_case_ids:
        requested = frozenset(requested_case_ids)
        selected = [reviewed for reviewed in reviewed_cases if reviewed.case_id in requested]
        selected_ids = {reviewed.case_id for reviewed in selected}
        if selected_ids != requested or len(selected) != len(requested_case_ids):
            missing = sorted(requested - selected_ids)
            raise AssertionError(
                f"DPONE_ROUTE_LIVE_BOUNDARY_CASE_IDS must contain unique reviewed non-IEEE IDs; missing={missing!r}"
            )
        reviewed_cases = selected
    else:
        assert len(reviewed_cases) == 392

    for ordinal, reviewed in enumerate(reviewed_cases):
        parameters = json.loads(reviewed.config_json)["parameters"]
        source_type = str(parameters["source_type"])
        canonical = str(parameters["canonical_type"])
        value_case = str(parameters["value_case"])
        expected_target_type = str(parameters["target_type"])
        source_table = f"boundary_source_{ordinal}_{suffix}"
        target_table = f"boundary_target_{ordinal}_{suffix}"
        case_work_dir = tmp_path / f"case_{ordinal:03d}"
        case_work_dir.mkdir()
        drop_mssql_table(mssql, target_table)
        postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{source_table}" CASCADE')
        postgres.execute_query(
            f'CREATE TABLE "{SOURCE_SCHEMA}"."{source_table}" (id integer NOT NULL, value {source_type} NULL)'
        )
        config = _boundary_config(source_table, target_table, case_work_dir, mssql.database)

        if reviewed.expected_mutation:
            before = _boundary_target_image(mssql, target_table, case_work_dir)
            _insert_boundary_value(
                postgres,
                source_table=source_table,
                expression=_boundary_value_expression(canonical, source_type, value_case),
            )
            result = _governed_runner(
                governed_mssql_live_campaign,
                postgres,
                config,
                logger=QuietIntegrationLogger(),
            ).run(config, label=f"boundary_{ordinal}_{suffix}")
            assert result["status"] == "success", (reviewed.case_id, result)
            assert result["loaded_rows"] == 1, (reviewed.case_id, result)
            _assert_boundary_value_roundtrip(
                postgres,
                mssql,
                source_table=source_table,
                target_table=target_table,
                canonical=canonical,
                source_type=source_type,
                case_id=reviewed.case_id,
            )
            catalog = _assert_boundary_target_catalog(
                mssql,
                target_table=target_table,
                expected_target_type=expected_target_type,
            )
            after = _boundary_target_image(mssql, target_table, case_work_dir)
            assert after != before
            normalized_source = _boundary_source_value(
                postgres,
                source_table=source_table,
                canonical=canonical,
                source_type=source_type,
            )
            route_live_recorder.observe_case(
                "boundary_types",
                reviewed.case_id,
                before_image=before,
                after_image=after,
                observations={
                    "action_class": reviewed.action_class,
                    "source_catalog": _boundary_source_catalog(postgres, source_table),
                    "target_catalog": catalog,
                    "normalized_source_value": normalized_source,
                    "normalization_asserted": reviewed.action_class == "postgres_typmod_normalization_roundtrip",
                    "staging_cleanup_asserted": after["staging_objects"] == 0,
                    "artifact_cleanup_asserted": after["artifact_entries"] == [],
                },
            )
            continue

        _insert_boundary_value(
            postgres,
            source_table=source_table,
            expression=_boundary_value_expression(canonical, source_type, "nominal"),
        )
        baseline = _governed_runner(
            governed_mssql_live_campaign,
            postgres,
            config,
            logger=QuietIntegrationLogger(),
        ).run(config, label=f"boundary_baseline_{ordinal}_{suffix}")
        assert baseline["status"] == "success", (reviewed.case_id, baseline)
        postgres.execute_query(f'DELETE FROM "{SOURCE_SCHEMA}"."{source_table}"')
        before = _boundary_target_image(mssql, target_table, case_work_dir)
        source_error: Exception | None = None
        try:
            _insert_boundary_value(
                postgres,
                source_table=source_table,
                expression=_boundary_value_expression(canonical, source_type, value_case),
            )
        except Exception as exc:  # Real PostgreSQL domain authority is asserted below.
            source_error = exc

        route_error: Exception | None = None
        if reviewed.action_class == "postgres_source_domain_reject":
            assert source_error is not None, reviewed.case_id
            assert getattr(source_error, "sqlstate", None) in {"22001", "22003", "22007", "22008", "22023"}, (
                reviewed.case_id,
                source_error,
            )
        else:
            assert source_error is None, (reviewed.case_id, source_error)
            try:
                _governed_runner(
                    governed_mssql_live_campaign,
                    postgres,
                    config,
                    logger=QuietIntegrationLogger(),
                ).run(config, label=f"boundary_guard_{ordinal}_{suffix}")
            except Exception as exc:  # Typed dpone/vendor failure is classified below.
                route_error = exc
            assert route_error is not None, reviewed.case_id
            assert isinstance(
                route_error,
                (
                    SnapshotReconciliationError,
                    FileContractValidationError,
                    MSSQLDatetimeOffsetDecodeError,
                    PostgresMssqlSourceValueError,
                ),
            ), (reviewed.case_id, route_error)

        after = _boundary_target_image(mssql, target_table, case_work_dir)
        assert after == before, reviewed.case_id
        route_live_recorder.observe_case(
            "boundary_types",
            reviewed.case_id,
            before_image=before,
            after_image=after,
            observations={
                "action_class": reviewed.action_class,
                "source_catalog": _boundary_source_catalog(postgres, source_table),
                "source_error_type": type(source_error).__name__ if source_error is not None else None,
                "source_error_sqlstate": getattr(source_error, "sqlstate", None),
                "route_error_type": type(route_error).__name__ if route_error is not None else None,
                "route_error_code": getattr(route_error, "code", None),
                "route_error_message": str(route_error) if route_error is not None else None,
                "blocked_before_etl": source_error is not None,
                "blocked_before_source_copy": False,
                "blocked_during_single_source_copy": isinstance(route_error, PostgresMssqlSourceValueError),
                "blocked_before_business_dml": True,
                "staging_cleanup_asserted": after["staging_objects"] == 0,
                "artifact_cleanup_asserted": after["artifact_entries"] == [],
            },
        )


def _is_separately_certified_ieee_boundary(config_json: str) -> bool:
    parameters = json.loads(config_json)["parameters"]
    return parameters["canonical_type"] == "float" and parameters["value_case"] in _IEEE_BOUNDARY_CASES


def _boundary_config(source_table: str, target_table: str, work_dir: Path, database: str) -> LoadConfig:
    return LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        target_database=database,
        source_schema=SOURCE_SCHEMA,
        source_table=source_table,
        target_schema="dpone_it",
        target_table=target_table,
        staging_schema="staging",
        staging_database=database,
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "work_dir": str(work_dir),
            "technical_columns": "forbidden",
            "lineage": False,
        },
    )


def _insert_boundary_value(postgres: Any, *, source_table: str, expression: str) -> None:
    postgres.execute_query(f'INSERT INTO "{SOURCE_SCHEMA}"."{source_table}" (id, value) VALUES (1, {expression})')


def _boundary_value_expression(canonical: str, source_type: str, value_case: str) -> str:
    normalized = source_type.strip().lower()
    if value_case == "null":
        return "NULL"
    if value_case == "nominal":
        return _boundary_nominal_expression(canonical, normalized)
    if canonical == "integer":
        return _integer_boundary_expression(normalized, value_case)
    if canonical == "decimal":
        return _decimal_boundary_expression(normalized, value_case)
    if canonical == "float":
        return _float_boundary_expression(normalized, value_case)
    if canonical == "boolean":
        return "FALSE" if value_case == "false" else "TRUE"
    if canonical == "string":
        return _string_boundary_expression(normalized, value_case)
    if canonical == "uuid":
        value = (
            "00000000-0000-0000-0000-000000000000"
            if value_case == "all_zero"
            else "ffffffff-ffff-ffff-ffff-ffffffffffff"
        )
        return f"'{value}'::uuid"
    if canonical == "binary":
        values = {
            "empty": "",
            "nul": "00",
            "all_byte_values": "".join(f"{value:02x}" for value in range(256)),
        }
        if value_case == "large_1mib":
            return "decode(repeat('a5', 1048576), 'hex')"
        if value_case == "invalid_hex_constructor":
            return "decode('0g', 'hex')"
        return f"decode('{values[value_case]}', 'hex')"
    if canonical == "date":
        return _temporal_cast(
            normalized,
            {
                "mssql_minimum": "0001-01-01",
                "mssql_maximum": "9999-12-31",
                "before_mssql_minimum": "0001-01-01 BC",
                "postgres_infinity": "infinity",
                "postgres_negative_infinity": "-infinity",
            }[value_case],
        )
    if canonical in {"timestamp", "offset_timestamp"}:
        return _timestamp_boundary_expression(normalized, canonical, value_case)
    if canonical == "time":
        value = {
            "minimum": "00:00:00",
            "maximum_at_source_scale": "23:59:59" if "(0)" in normalized else "23:59:59.999999",
            "midnight_24h": "24:00:00",
            "source_typmod_fraction_normalization": "12:34:56.1234567",
        }[value_case]
        return _temporal_cast(normalized, value)
    if canonical == "json":
        if value_case == "empty_object":
            return f"'{{}}'::{normalized}"
        if value_case == "nested_unicode_controls":
            return f"json_build_object('nested', json_build_array(E'line\\n', chr(29), chr(128578)))::{normalized}"
        if value_case == "large_document":
            return f"json_build_object('payload', repeat('Ж', 1048576))::{normalized}"
        if value_case == "json_null":
            return f"'null'::{normalized}"
    raise AssertionError(f"unhandled boundary expression:{canonical}:{source_type}:{value_case}")


def _boundary_nominal_expression(canonical: str, source_type: str) -> str:
    if canonical == "decimal":
        return _decimal_boundary_expression(source_type, "midpoint_exact")
    values = {
        "integer": "1",
        "float": "1.5",
        "boolean": "TRUE",
        "string": "E'x'",
        "uuid": "'12345678-1234-5678-9abc-1234567890ab'::uuid",
        "binary": "decode('0001ff', 'hex')",
        "date": "'2024-01-02'::date",
        "timestamp": _temporal_cast(source_type, "2024-01-02 03:04:05.123456"),
        "offset_timestamp": _temporal_cast(source_type, "2024-01-02 03:04:05.123456+05:30"),
        "time": _temporal_cast(source_type, "12:34:56.123456"),
        "json": f'\'{{"value":"nominal"}}\'::{source_type}',
    }
    return values[canonical]


def _integer_boundary_expression(source_type: str, value_case: str) -> str:
    if source_type in {"smallint", "int2"}:
        minimum, maximum = -32768, 32767
    elif source_type in {"integer", "int", "int4"}:
        minimum, maximum = -2147483648, 2147483647
    else:
        minimum, maximum = -9223372036854775808, 9223372036854775807
    return str(
        {
            "source_minimum": minimum,
            "source_maximum": maximum,
            "below_source_minimum": minimum - 1,
            "above_source_maximum": maximum + 1,
        }[value_case]
    )


def _decimal_boundary_expression(source_type: str, value_case: str) -> str:
    values = {
        "numeric(18,4)": {
            "source_minimum_exact": "-99999999999999.9999",
            "source_maximum_exact": "99999999999999.9999",
            "midpoint_exact": "0.5000",
            "source_typmod_scale_normalization": "1.23456",
            "source_precision_overflow": "100000000000000.0000",
        },
        "decimal(18,4)": {
            "source_minimum_exact": "-99999999999999.9999",
            "source_maximum_exact": "99999999999999.9999",
            "midpoint_exact": "0.5000",
            "source_typmod_scale_normalization": "1.23456",
            "source_precision_overflow": "100000000000000.0000",
        },
        "numeric(3,5)": {
            "source_minimum_exact": "-0.00999",
            "source_maximum_exact": "0.00999",
            "midpoint_exact": "0.00500",
            "source_typmod_scale_normalization": "0.000006",
            "source_precision_overflow": "0.01000",
        },
        "numeric(2,-3)": {
            "source_minimum_exact": "-99000",
            "source_maximum_exact": "99000",
            "midpoint_exact": "5000",
            "source_typmod_scale_normalization": "1499",
            "source_precision_overflow": "100000",
        },
        "numeric(38,38)": {
            "source_minimum_exact": "-0." + "9" * 38,
            "source_maximum_exact": "0." + "9" * 38,
            "midpoint_exact": "0.5",
            "source_typmod_scale_normalization": "0." + "0" * 38 + "6",
            "source_precision_overflow": "1.0",
        },
    }
    if value_case in {"nan", "positive_infinity", "negative_infinity"}:
        literal = {"nan": "NaN", "positive_infinity": "Infinity", "negative_infinity": "-Infinity"}[value_case]
    else:
        literal = values[source_type][value_case]
    return f"CAST('{literal}' AS {source_type})"


def _float_boundary_expression(source_type: str, value_case: str) -> str:
    binary32 = source_type in {"real", "float4"}
    values = {
        "positive_zero": "0",
        "maximum_finite": "3.4028234e38" if binary32 else "1.7976931348623157e308",
        "minimum_finite": "-3.4028234e38" if binary32 else "-1.7976931348623157e308",
        "nan": "NaN",
        "positive_infinity": "Infinity",
        "negative_infinity": "-Infinity",
        "source_overflow": "1e1000" if binary32 else "1e10000",
    }
    return f"CAST('{values[value_case]}' AS {source_type})"


def _string_boundary_expression(source_type: str, value_case: str) -> str:
    length = _boundary_string_length(source_type)
    values = {
        "empty": "E''",
        "u0020": "E' '",
        "tab": "E'\\t'",
        "lf": "E'\\n'",
        "cr": "E'\\r'",
        "backslash": "E'\\\\'",
        "codec_marker_prefix": "chr(29)",
        "codec_marker_prefix_doubled": "repeat(chr(29), 2)",
        "unicode_bmp": "E'Ж'",
        "unicode_nonbmp": "chr(128578)",
    }
    if value_case in values:
        return values[value_case]
    if value_case == "large_1mib":
        return "repeat('Ж', 1048576)"
    if length is None:
        raise AssertionError(f"unbounded string has no declared boundary:{source_type}:{value_case}")
    amount = length if value_case == "max_declared_length" else length + 1
    return f"repeat('Ж', {amount})"


def _boundary_string_length(source_type: str) -> int | None:
    if source_type in {"char", "character"}:
        return 1
    if "(" not in source_type:
        return None
    return int(source_type.rsplit("(", 1)[1].rstrip(")"))


def _timestamp_boundary_expression(source_type: str, canonical: str, value_case: str) -> str:
    scale = 0 if "(0)" in source_type else 3 if "(3)" in source_type else 6
    fraction = "" if scale == 0 else "." + "9" * scale
    offset = "+00:00" if canonical == "offset_timestamp" else ""
    values = {
        "mssql_minimum_at_source_scale": f"0001-01-01 00:00:00{offset}",
        "mssql_maximum_at_source_scale": f"9999-12-31 23:59:59{fraction}{offset}",
        "dst_overlap_local_time": "2024-11-03 01:30:00",
        "dst_overlap_instant": "2024-11-03 01:30:00-04:00",
        "pre_epoch": "1900-01-01 00:00:00",
        "post_epoch": "2038-01-19 03:14:07",
        "offset_positive": "2024-01-02 03:04:05.123456+14:00",
        "offset_negative": "2024-01-02 03:04:05.123456-14:00",
        "before_mssql_minimum": f"0001-01-01 00:00:00 BC{offset}",
        "postgres_infinity": "infinity",
        "postgres_negative_infinity": "-infinity",
        "source_typmod_100ns_normalization": f"2024-01-02 03:04:05.1234567{offset}",
    }
    return _temporal_cast(source_type, values[value_case])


def _temporal_cast(source_type: str, value: str) -> str:
    return f"CAST('{value}' AS {source_type})"


def _boundary_target_image(mssql: Any, target_table: str, work_dir: Path) -> dict[str, object]:
    rows = mssql.get_records(
        f"SELECT [id], [value] FROM [dpone_it].[{target_table}] ORDER BY [id]"
        if _boundary_target_exists(mssql, target_table)
        else "SELECT CAST(NULL AS int) AS [id], CAST(NULL AS int) AS [value] WHERE 1 = 0",
        as_dict=True,
    )
    catalog = mssql.get_records(
        "SELECT c.name, ty.name AS type_name, c.max_length, c.precision, c.scale, c.is_nullable "
        "FROM sys.tables AS t INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "INNER JOIN sys.columns AS c ON c.object_id = t.object_id "
        "INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id "
        "WHERE s.name = ? AND t.name = ? ORDER BY c.column_id",
        ("dpone_it", target_table),
        as_dict=True,
    )
    staging = mssql.get_records(
        "SELECT COUNT_BIG(*) AS object_count FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name LIKE ?",
        ("staging", f"stg_{target_table}_%"),
        as_dict=True,
    )
    return {
        "target_exists": bool(catalog),
        "target_rows": rows,
        "target_catalog": catalog,
        "staging_objects": int(staging[0]["object_count"]),
        "artifact_entries": sorted(path.name for path in work_dir.iterdir()),
    }


def _boundary_target_exists(mssql: Any, target_table: str) -> bool:
    rows = mssql.get_records(
        "SELECT COUNT_BIG(*) AS object_count FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id WHERE s.name = ? AND t.name = ?",
        ("dpone_it", target_table),
        as_dict=True,
    )
    return int(rows[0]["object_count"]) == 1


def _assert_boundary_target_catalog(
    mssql: Any,
    *,
    target_table: str,
    expected_target_type: str,
) -> dict[str, object]:
    rows = mssql.get_records(
        "SELECT ty.name AS type_name, c.max_length, c.precision, c.scale, c.is_nullable "
        "FROM sys.tables AS t INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "INNER JOIN sys.columns AS c ON c.object_id = t.object_id "
        "INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id "
        "WHERE s.name = ? AND t.name = ? AND c.name = N'value'",
        ("dpone_it", target_table),
        as_dict=True,
    )
    assert len(rows) == 1
    row = rows[0]
    actual = _render_boundary_catalog_type(row)
    assert actual == expected_target_type.lower(), (target_table, actual, expected_target_type, row)
    assert bool(row["is_nullable"])
    return {str(key): value for key, value in row.items()}


def _render_boundary_catalog_type(row: dict[str, Any]) -> str:
    type_name = str(row["type_name"]).lower()
    if type_name in {"nvarchar", "nchar"}:
        max_length = int(row["max_length"])
        return f"{type_name}({'max' if max_length == -1 else max_length // 2})"
    if type_name in {"varchar", "char", "varbinary", "binary"}:
        max_length = int(row["max_length"])
        return f"{type_name}({'max' if max_length == -1 else max_length})"
    if type_name in {"decimal", "numeric"}:
        return f"{type_name}({int(row['precision'])},{int(row['scale'])})"
    if type_name in {"datetime2", "datetimeoffset", "time"}:
        return f"{type_name}({int(row['scale'])})"
    return type_name


def _assert_boundary_value_roundtrip(
    postgres: Any,
    mssql: Any,
    *,
    source_table: str,
    target_table: str,
    canonical: str,
    source_type: str,
    case_id: str,
) -> None:
    source = _boundary_source_value(
        postgres,
        source_table=source_table,
        canonical=canonical,
        source_type=source_type,
    )
    if canonical == "float":
        target_rows = mssql.get_records(
            f"SELECT CONVERT(varbinary(8), [value]) AS value_bits FROM [dpone_it].[{target_table}]",
            as_dict=True,
        )
        target = None if target_rows[0]["value_bits"] is None else bytes(target_rows[0]["value_bits"]).hex()
    else:
        target_raw = mssql.get_records(
            f"SELECT [value] FROM [dpone_it].[{target_table}]",
            as_dict=True,
        )[0]["value"]
        target = _normalize_boundary_value(canonical, target_raw)
    assert target == source, (case_id, source, target)


def _boundary_source_value(
    postgres: Any,
    *,
    source_table: str,
    canonical: str,
    source_type: str,
) -> object:
    if canonical == "float":
        send = "float4send" if source_type in {"real", "float4"} else "float8send"
        raw = postgres.get_records(
            f'SELECT encode({send}(value), \'hex\') AS value FROM "{SOURCE_SCHEMA}"."{source_table}"',
            as_dict=True,
        )[0]["value"]
        return None if raw is None else str(raw)
    source_expression = (
        "value::text" if canonical == "string" and source_type.split("(", 1)[0] in {"char", "character"} else "value"
    )
    raw = postgres.get_records(
        f'SELECT {source_expression} AS value FROM "{SOURCE_SCHEMA}"."{source_table}"',
        as_dict=True,
    )[0]["value"]
    return _normalize_boundary_value(canonical, raw)


def _normalize_boundary_value(canonical: str, value: object) -> object:
    if value is None:
        return None
    if canonical == "json":
        return json.loads(value) if isinstance(value, str) else value
    if canonical in {"decimal", "uuid"}:
        return str(value).lower()
    if canonical == "binary":
        return bytes(value).hex()
    if canonical in {"date", "timestamp", "offset_timestamp", "time"}:
        return value.isoformat() if hasattr(value, "isoformat") else str(value)
    return value


def _boundary_source_catalog(postgres: Any, source_table: str) -> dict[str, object]:
    rows = postgres.get_records(
        "SELECT c.oid AS relation_oid, a.atttypid AS type_oid, a.atttypmod AS typmod, "
        "format_type(a.atttypid, a.atttypmod) AS declared_type "
        "FROM pg_catalog.pg_class AS c "
        "INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace "
        "INNER JOIN pg_catalog.pg_attribute AS a ON a.attrelid = c.oid "
        "WHERE n.nspname = %s AND c.relname = %s AND a.attname = 'value' AND NOT a.attisdropped",
        (SOURCE_SCHEMA, source_table),
        as_dict=True,
    )
    assert len(rows) == 1
    return {str(key): value for key, value in rows[0].items()}


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_equal_alias_same_named_relations_handoff_postgres_file_not_mssql_query_live(tmp_path: Path) -> None:
    """Equal authoring aliases never execute the PostgreSQL query on MSSQL."""

    table = "internal_query_alias_collision"
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    wait_until_ready("mssql", lambda: mssql.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{table}" CASCADE')
    postgres.execute_query(f'CREATE TABLE "{SOURCE_SCHEMA}"."{table}" (id integer NOT NULL, sentinel text NOT NULL)')
    postgres.execute_query(
        f'INSERT INTO "{SOURCE_SCHEMA}"."{table}" (id, sentinel) VALUES (%s, %s)',
        (1, "postgres-authority"),
    )
    drop_mssql_table(mssql, table)
    mssql.execute_query(f"CREATE TABLE [dpone_it].[{table}] ([id] int NOT NULL, [sentinel] nvarchar(64) NOT NULL)")
    mssql.execute_query(f"INSERT INTO [dpone_it].[{table}] ([id], [sentinel]) VALUES (1, N'mssql-decoy')")
    config = LoadConfig(
        source_conn_id="shared-authoring-alias",
        target_conn_id="shared-authoring-alias",
        source_schema=SOURCE_SCHEMA,
        source_table=table,
        target_schema="dpone_it",
        target_table=table,
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "work_dir": str(tmp_path),
            "technical_columns": "forbidden",
        },
    )

    source = PostgresFullExtractStrategy(postgres, logger=NoopLogger())
    bind_factual_postgres_source_authority(source, postgres, load_config=config)
    extracted = source.extract(config, None)
    artifact = extracted.artifact
    try:
        assert isinstance(artifact, FileExportArtifact)
        assert artifact.format == "mssql-delimited"
        assert artifact.rows_exported == 1
        wire_bytes = Path(artifact.file_path).read_bytes()
        assert b"postgres-authority" in wire_bytes
        assert b"mssql-decoy" not in wire_bytes
        assert mssql.get_records(
            f"SELECT [id], [sentinel] FROM [dpone_it].[{table}]",
            as_dict=True,
        ) == [{"id": 1, "sentinel": "mssql-decoy"}]
    finally:
        artifact.cleanup()


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_standard_etl_fresh_keyed_target_owns_physical_design_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> None:
    """Prove one target owner creates one BIN2 physical-PK authority before DML."""

    source_table = "standard_etl_keyed_physical_source"
    target_table = "standard_etl_keyed_physical_target"
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    drop_mssql_table(mssql, target_table)
    postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{source_table}" CASCADE')
    postgres.execute_query(
        f'CREATE TABLE "{SOURCE_SCHEMA}"."{source_table}" ('
        "id character varying(16) NOT NULL, value character varying(64) NOT NULL, "
        "updated_at timestamp(6) without time zone NOT NULL)"
    )
    postgres.execute_query(
        f'INSERT INTO "{SOURCE_SCHEMA}"."{source_table}" (id, value, updated_at) VALUES '
        "('A', 'upper', TIMESTAMP '2026-08-15 10:00:00.000001'), "
        "('a', 'lower', TIMESTAMP '2026-08-15 10:00:00.000002')"
    )
    physical_design = {
        "enabled": True,
        "mode": "explicit",
        "apply": "online",
        "apply_runtime": True,
        "indexes": {"primary_key": ["id"]},
        "storage": {"mssql": {"compression": "row"}},
    }
    config = LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_schema=SOURCE_SCHEMA,
        source_table=source_table,
        target_schema="dpone_it",
        target_table=target_table,
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        unique_key=["id"],
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "work_dir": str(tmp_path),
            "technical_columns": "forbidden",
            "schema_contract": {
                "enforcement": "strict",
                "columns": {
                    "id": {"type": "string", "nullable": False},
                    "value": {"type": "string", "nullable": False},
                    "updated_at": {"type": "datetime", "nullable": False},
                },
            },
            "physical_design": physical_design,
        },
    )
    logger = QuietIntegrationLogger()
    processor = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=logger,
    )

    first = processor.run(config, label="keyed_physical_first")
    metadata = mssql.get_records(
        "SELECT c.collation_name, kc.type AS constraint_type, i.type_desc, p.data_compression_desc "
        "FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "INNER JOIN sys.columns AS c ON c.object_id = t.object_id AND c.name = N'id' "
        "INNER JOIN sys.indexes AS i ON i.object_id = t.object_id AND i.is_unique = 1 "
        "LEFT JOIN sys.key_constraints AS kc ON kc.parent_object_id = t.object_id "
        "AND kc.unique_index_id = i.index_id "
        "INNER JOIN sys.partitions AS p ON p.object_id = i.object_id AND p.index_id = i.index_id "
        "WHERE s.name = N'dpone_it' AND t.name = ? ORDER BY i.index_id",
        (target_table,),
        as_dict=True,
    )
    assert first["status"] == "success"
    assert first["loaded_rows"] == 2
    assert len(metadata) == 1
    assert {row["collation_name"] for row in metadata} == {"Latin1_General_100_BIN2"}
    assert any(row["constraint_type"] == "PK" for row in metadata)
    assert any(row["type_desc"] == "CLUSTERED" and row["data_compression_desc"] == "ROW" for row in metadata)
    assert not any(row["type_desc"] == "NONCLUSTERED" for row in metadata)

    postgres.execute_query(
        f'UPDATE "{SOURCE_SCHEMA}"."{source_table}" SET value = %s, updated_at = %s WHERE id = %s',
        ("upper-updated", datetime(2026, 8, 15, 11, 0, 0, 123456), "A"),
    )
    second_options = dict(config.options)
    second_options["physical_design"] = {**physical_design, "apply_runtime": False}
    second = processor.run(
        replace(config, options=second_options),
        label="keyed_physical_reconcile",
    )
    rows = mssql.get_records(
        f"SELECT [id], [value] FROM [dpone_it].[{target_table}] ORDER BY [id] COLLATE Latin1_General_100_BIN2",
        as_dict=True,
    )
    assert second["status"] == "success"
    assert rows == [{"id": "A", "value": "upper-updated"}, {"id": "a", "value": "lower"}]


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_standard_etl_scd2_history_physical_key_contract_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> None:
    """Reject a business-only PK pre-COPY and retain two real SCD2 versions."""

    source_table = "standard_etl_scd2_physical_source"
    target_table = "standard_etl_scd2_physical_target"
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    drop_mssql_table(mssql, target_table)
    postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{source_table}" CASCADE')
    postgres.execute_query(
        f'CREATE TABLE "{SOURCE_SCHEMA}"."{source_table}" ('
        "id character varying(16) NOT NULL, value character varying(64) NOT NULL)"
    )
    postgres.execute_query(
        f'INSERT INTO "{SOURCE_SCHEMA}"."{source_table}" (id, value) VALUES (%s, %s)',
        ("history-key", "v1"),
    )

    def config(primary_key: list[str]) -> LoadConfig:
        return LoadConfig(
            source_conn_id="postgres_source",
            target_conn_id="mssql_sink",
            source_schema=SOURCE_SCHEMA,
            source_table=source_table,
            target_schema="dpone_it",
            target_table=target_table,
            staging_schema="staging",
            load_strategy=LoadStrategy.SCD2,
            unique_key=["id"],
            export_format="csv",
            compress_export=False,
            options={
                "source_type": "postgres",
                "sink_type": "mssql",
                "batch_commit_mode": "whole",
                "work_dir": str(tmp_path),
                "technical_columns": "required",
                "schema_contract": {
                    "enforcement": "strict",
                    "columns": {
                        "id": {"type": "string", "nullable": False},
                        "value": {"type": "string", "nullable": False},
                    },
                },
                "scd2": {"delete_policy": "ignore"},
                "physical_design": {
                    "enabled": True,
                    "mode": "explicit",
                    "apply_runtime": True,
                    "indexes": {"primary_key": primary_key},
                    "storage": {"mssql": {"compression": "row"}},
                },
            },
        )

    logger = QuietIntegrationLogger()
    invalid = config(["id"])
    processor = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        invalid,
        logger=logger,
    )
    before_files = set(tmp_path.iterdir())
    with pytest.raises(MSSQLStrategyContractError) as blocked:
        processor.run(
            invalid,
            label="scd2_invalid_physical",
        )
    assert blocked.value.blocker == "mssql.strategy.scd2.physical_primary_key"
    assert not mssql.table_exists("dpone_it", target_table)
    assert set(tmp_path.iterdir()) == before_files

    valid = config(["id", "__dpone__valid_from_at"])
    valid.target_database = governed_mssql_live_campaign.target_database
    first = processor.run(valid, label="scd2_physical_first")
    postgres.execute_query(
        f'UPDATE "{SOURCE_SCHEMA}"."{source_table}" SET value = %s WHERE id = %s',
        ("v2", "history-key"),
    )
    second = processor.run(valid, label="scd2_physical_change")
    versions = mssql.get_records(
        f"SELECT [value], [__dpone__is_current], [__dpone__valid_to_at] "
        f"FROM [dpone_it].[{target_table}] ORDER BY [__dpone__valid_from_at]",
        as_dict=True,
    )
    pk = mssql.get_records(
        "SELECT c.name AS column_name, ic.key_ordinal, p.data_compression_desc "
        "FROM sys.key_constraints AS kc "
        "INNER JOIN sys.tables AS t ON t.object_id = kc.parent_object_id "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "INNER JOIN sys.index_columns AS ic ON ic.object_id = t.object_id "
        "AND ic.index_id = kc.unique_index_id AND ic.key_ordinal > 0 "
        "INNER JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id "
        "INNER JOIN sys.partitions AS p ON p.object_id = t.object_id AND p.index_id = kc.unique_index_id "
        "WHERE s.name = N'dpone_it' AND t.name = ? AND kc.type = N'PK' "
        "ORDER BY ic.key_ordinal",
        (target_table,),
        as_dict=True,
    )
    assert first["status"] == second["status"] == "success"
    assert [(row["value"], bool(row["__dpone__is_current"])) for row in versions] == [
        ("v1", False),
        ("v2", True),
    ]
    assert versions[0]["__dpone__valid_to_at"] is not None
    assert versions[1]["__dpone__valid_to_at"] is None
    assert [row["column_name"] for row in pk] == ["id", "__dpone__valid_from_at"]
    assert {row["data_compression_desc"] for row in pk} == {"ROW"}


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_standard_etl_postgres_replace_raw_scope_rejects_before_copy_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> None:
    """A PostgreSQL predicate is not silently reused as SQL Server scope SQL."""

    source_table = "standard_etl_replace_scope_source"
    target_table = "standard_etl_replace_scope_target"
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{source_table}" CASCADE')
    postgres.execute_query(
        f'CREATE TABLE "{SOURCE_SCHEMA}"."{source_table}" ('
        "id integer NOT NULL, partition_id integer NOT NULL, value text NOT NULL)"
    )
    postgres.execute_query(
        f'INSERT INTO "{SOURCE_SCHEMA}"."{source_table}" VALUES (1, 1, %s)',
        ("must-not-land",),
    )
    drop_mssql_table(mssql, target_table)
    mssql.execute_query(
        f"CREATE TABLE [dpone_it].[{target_table}] ("
        "[id] int NOT NULL, [partition_id] int NOT NULL, [value] nvarchar(max) NOT NULL)"
    )
    mssql.execute_query(f"INSERT INTO [dpone_it].[{target_table}] VALUES (99, 2, N'before-image')")
    config = LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_schema=SOURCE_SCHEMA,
        source_table=source_table,
        target_schema="dpone_it",
        target_table=target_table,
        staging_schema="staging",
        load_strategy=LoadStrategy.REPLACE,
        custom_predicate="partition_id = 1",
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "work_dir": str(tmp_path),
            "technical_columns": "forbidden",
        },
    )
    logger = QuietIntegrationLogger()
    processor = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=logger,
    )
    before_files = set(tmp_path.iterdir())
    before_rows = mssql.get_records(
        f"SELECT [id], [partition_id], [value] FROM [dpone_it].[{target_table}]",
        as_dict=True,
    )

    with pytest.raises(MSSQLStrategyContractError) as blocked:
        processor.run(config, label="replace_raw_scope")

    assert blocked.value.blocker == "mssql.strategy.replace.cross_dialect_raw_predicate"
    assert set(tmp_path.iterdir()) == before_files
    assert (
        mssql.get_records(
            f"SELECT [id], [partition_id], [value] FROM [dpone_it].[{target_table}]",
            as_dict=True,
        )
        == before_rows
    )
    assert (
        mssql.get_records(
            "SELECT COUNT_BIG(*) FROM sys.tables AS t "
            "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
            "WHERE s.name = N'staging' AND t.name LIKE ?",
            (f"{target_table}%",),
        )[0][0]
        == 0
    )


@pytest.mark.parametrize(
    ("target_collation", "old_value", "new_value"),
    (
        ("Latin1_General_100_CI_AI", "Cafe", "café"),
        ("Latin1_General_100_CI_AI", "trail", "trail "),
        ("Latin1_General_100_CS_AS", "Value", "value"),
    ),
)
@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_standard_etl_snapshot_all_columns_uses_native_binary_equality_live(
    tmp_path: Path,
    target_collation: str,
    old_value: str,
    new_value: str,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> None:
    """Case/accent/trailing-space changes cannot hide behind target collation."""

    suffix = uuid.uuid4().hex[:8]
    source_table = f"snapshot_binary_source_{suffix}"
    target_table = f"snapshot_binary_target_{suffix}"
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    postgres.execute_query(
        f'CREATE TABLE "{SOURCE_SCHEMA}"."{source_table}" ('
        "id character varying(16) NOT NULL, value character varying(64) NOT NULL)"
    )
    postgres.execute_query(
        f'INSERT INTO "{SOURCE_SCHEMA}"."{source_table}" VALUES (%s, %s)',
        ("identity", old_value),
    )
    config = LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_schema=SOURCE_SCHEMA,
        source_table=source_table,
        target_schema="dpone_it",
        target_table=target_table,
        staging_schema="staging",
        load_strategy=LoadStrategy.SNAPSHOT_DIFF,
        unique_key=["id"],
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "work_dir": str(tmp_path),
            "technical_columns": "required",
            "diff": {"compare": "all_columns", "delete_policy": "ignore"},
            "schema_contract": {
                "enforcement": "strict",
                "columns": {
                    "id": {"type": "string", "nullable": False},
                    "value": {"type": "string", "nullable": False},
                },
            },
            "physical_design": {
                "columns": {
                    "value": {"collation": {"mssql": target_collation}},
                },
            },
        },
    )
    logger = QuietIntegrationLogger()
    processor = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=logger,
    )

    baseline = processor.run(config, label=f"snapshot_binary_{suffix}_baseline")
    postgres.execute_query(
        f'UPDATE "{SOURCE_SCHEMA}"."{source_table}" SET value = %s WHERE id = %s',
        (new_value, "identity"),
    )
    changed = processor.run(config, label=f"snapshot_binary_{suffix}_changed")
    identical = processor.run(config, label=f"snapshot_binary_{suffix}_identical")
    rows = mssql.get_records(
        f"SELECT [id], [value] FROM [dpone_it].[{target_table}]",
        as_dict=True,
    )

    assert baseline["inserted_rows"] == 1
    assert changed["updated_rows"] == 1
    assert identical["updated_rows"] == 0
    assert identical["unchanged_rows"] == 1
    assert rows == [{"id": "identity", "value": new_value}]
    assert (
        mssql.get_records(
            "SELECT COUNT_BIG(*) FROM sys.tables AS t "
            "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
            "WHERE s.name = N'staging' AND t.name LIKE ?",
            (f"{target_table}%",),
        )[0][0]
        == 0
    )


@pytest.mark.parametrize("strategy", (LoadStrategy.SNAPSHOT_DIFF, LoadStrategy.SCD2))
@pytest.mark.parametrize(
    ("target_collation", "old_value", "new_value"),
    (
        ("Latin1_General_100_CI_AI", "Cafe\tline\n\x1d", "café\tline\n\x1d"),
        ("Latin1_General_100_CS_AS", "Value\tline\n\x1d", "value\tline\n\x1d"),
    ),
)
@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_standard_etl_file_stream_native_hash_parity_live(
    tmp_path: Path,
    strategy: LoadStrategy,
    target_collation: str,
    old_value: str,
    new_value: str,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> None:
    """Switching artifact mode cannot manufacture an update or SCD2 version."""

    suffix = uuid.uuid4().hex[:8]
    source_table = f"hash_parity_source_{suffix}"
    target_table = f"hash_parity_target_{suffix}"
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    postgres.execute_query(
        f'CREATE TABLE "{SOURCE_SCHEMA}"."{source_table}" ('
        "id character varying(64) NOT NULL, value character varying(128) NOT NULL)"
    )
    key = "identity\tline\n\x1d"
    postgres.execute_query(
        f'INSERT INTO "{SOURCE_SCHEMA}"."{source_table}" VALUES (%s, %s)',
        (key, old_value),
    )
    strategy_options = (
        {"diff": {"compare": "all_columns", "delete_policy": "ignore"}}
        if strategy == LoadStrategy.SNAPSHOT_DIFF
        else {"scd2": {"delete_policy": "ignore"}}
    )
    config = LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_schema=SOURCE_SCHEMA,
        source_table=source_table,
        target_schema="dpone_it",
        target_table=target_table,
        staging_schema="staging",
        load_strategy=strategy,
        unique_key=["id"],
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "work_dir": str(tmp_path),
            "technical_columns": "required",
            # Lineage parity has its own source-receipt contract.  This case
            # isolates strategy metadata/hash parity across artifact modes.
            "lineage": False,
            "schema_contract": {
                "enforcement": "strict",
                "columns": {
                    "id": {"type": "string", "nullable": False},
                    "value": {"type": "string", "nullable": False},
                },
            },
            "physical_design": {
                "columns": {
                    "value": {"collation": {"mssql": target_collation}},
                },
            },
            **strategy_options,
        },
    )
    logger = QuietIntegrationLogger()
    file_processor = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=logger,
    )
    stream_processor = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=logger,
        source_type=_PostgresCopyToStreamingSource,
    )

    baseline = file_processor.run(config, label=f"hash_parity_{suffix}_file_baseline")
    baseline_rows = mssql.get_records(
        "SELECT [value], [__dpone__row_hash], "
        + ("[__dpone__is_current] " if strategy == LoadStrategy.SCD2 else "CAST(1 AS bit) AS [__dpone__is_current] ")
        + f"FROM [dpone_it].[{target_table}] ORDER BY [__dpone__row_hash]",
        as_dict=True,
    )
    identical_stream = stream_processor.run(
        config,
        label=f"hash_parity_{suffix}_stream_identical",
    )
    after_identical = mssql.get_records(
        "SELECT [value], [__dpone__row_hash], "
        + ("[__dpone__is_current] " if strategy == LoadStrategy.SCD2 else "CAST(1 AS bit) AS [__dpone__is_current] ")
        + f"FROM [dpone_it].[{target_table}] ORDER BY [__dpone__row_hash]",
        as_dict=True,
    )
    assert baseline["inserted_rows"] == 1
    assert identical_stream["inserted_rows"] == 0
    assert identical_stream["updated_rows"] == 0
    assert after_identical == baseline_rows

    postgres.execute_query(
        f'UPDATE "{SOURCE_SCHEMA}"."{source_table}" SET value = %s WHERE id = %s',
        (new_value, key),
    )
    changed_stream = stream_processor.run(
        config,
        label=f"hash_parity_{suffix}_stream_changed",
    )
    changed_rows = mssql.get_records(
        "SELECT [value], [__dpone__row_hash], "
        + ("[__dpone__is_current] " if strategy == LoadStrategy.SCD2 else "CAST(1 AS bit) AS [__dpone__is_current] ")
        + f"FROM [dpone_it].[{target_table}] ORDER BY [__dpone__row_hash]",
        as_dict=True,
    )
    assert changed_stream["updated_rows"] == 1
    assert len(changed_rows) == (2 if strategy == LoadStrategy.SCD2 else 1)
    assert [row["value"] for row in changed_rows if row["__dpone__is_current"]] == [new_value]
    assert {row["__dpone__row_hash"] for row in changed_rows} != {row["__dpone__row_hash"] for row in baseline_rows}

    identical_file = file_processor.run(
        config,
        label=f"hash_parity_{suffix}_file_identical",
    )
    final_rows = mssql.get_records(
        "SELECT [value], [__dpone__row_hash], "
        + ("[__dpone__is_current] " if strategy == LoadStrategy.SCD2 else "CAST(1 AS bit) AS [__dpone__is_current] ")
        + f"FROM [dpone_it].[{target_table}] ORDER BY [__dpone__row_hash]",
        as_dict=True,
    )
    assert identical_file["inserted_rows"] == 0
    assert identical_file["updated_rows"] == 0
    assert final_rows == changed_rows
    assert not tuple(tmp_path.iterdir())


@pytest.mark.parametrize(
    ("amount", "payload_value", "blocker"),
    (
        ("not-decimal", "present", "logical_value_violation:amount:schema.type_mismatch"),
        ("123.456", "present", "logical_value_violation:amount:schema.type_mismatch"),
        ("12.34", None, "not_null_violation"),
    ),
)
@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_standard_etl_strict_file_contract_rejects_decoded_values_before_staging_live(
    tmp_path: Path,
    amount: str,
    payload_value: str | None,
    blocker: str,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> None:
    """Opaque COPY validates the same public logical contract as row artifacts."""

    suffix = uuid.uuid4().hex[:8]
    source_table = f"file_contract_source_{suffix}"
    target_table = f"file_contract_target_{suffix}"
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    postgres.execute_query(
        f'CREATE TABLE "{SOURCE_SCHEMA}"."{source_table}" ('
        "id integer NOT NULL, amount text NOT NULL, payload text NULL)"
    )
    postgres.execute_query(
        f'INSERT INTO "{SOURCE_SCHEMA}"."{source_table}" VALUES (%s, %s, %s)',
        (1, amount, payload_value),
    )
    config = LoadConfig(
        source_conn_id="postgres_source",
        target_conn_id="mssql_sink",
        source_schema=SOURCE_SCHEMA,
        source_table=source_table,
        target_schema="dpone_it",
        target_table=target_table,
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "work_dir": str(tmp_path),
            "technical_columns": "forbidden",
            "lineage": False,
            "schema_contract": {
                "enforcement": "strict",
                "columns": {
                    "id": {"type": "integer", "nullable": False},
                    "amount": {
                        "type": "decimal",
                        "precision": 5,
                        "scale": 2,
                        "nullable": False,
                    },
                    "payload": {"type": "string", "nullable": False},
                },
            },
        },
    )
    logger = QuietIntegrationLogger()
    processor = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=logger,
    )

    with pytest.raises(FileContractValidationError, match=blocker):
        processor.run(config, label=f"file_contract_{suffix}")

    assert not mssql.table_exists("dpone_it", target_table)
    assert not tuple(tmp_path.iterdir())
    assert (
        mssql.get_records(
            "SELECT COUNT_BIG(*) FROM sys.tables AS t "
            "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
            "WHERE s.name = N'staging' AND t.name LIKE ?",
            (f"{target_table}%",),
        )[0][0]
        == 0
    )


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_standard_etl_partition_identity_cardinality_and_scoped_replace_live(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> None:
    """Binary partition identity guards cardinality and preserves exact siblings."""

    suffix = uuid.uuid4().hex[:8]
    source_table = f"partition_identity_source_{suffix}"
    target_table = f"partition_identity_target_{suffix}"
    postgres = postgres_connector()
    mssql = mssql_connector()
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    ensure_postgres_schemas(postgres, source_schema=SOURCE_SCHEMA)
    ensure_mssql_database_and_schemas()
    postgres.execute_query(
        f'CREATE TABLE "{SOURCE_SCHEMA}"."{source_table}" ('
        "id integer NOT NULL, partition_id character varying(32) NOT NULL, value text NOT NULL)"
    )
    partition_values = ("A", "a", "Á", "Ａ", "ア", "ｱ", "trail")

    def set_source(values: tuple[str, ...], *, prefix: str) -> None:
        postgres.execute_query(f'TRUNCATE TABLE "{SOURCE_SCHEMA}"."{source_table}"')
        for index, partition_value in enumerate(values, 1):
            postgres.execute_query(
                f'INSERT INTO "{SOURCE_SCHEMA}"."{source_table}" VALUES (%s, %s, %s)',
                (index, partition_value, f"{prefix}:{partition_value}"),
            )

    def config(max_partitions: int) -> LoadConfig:
        return LoadConfig(
            source_conn_id="postgres_source",
            target_conn_id="mssql_sink",
            source_schema=SOURCE_SCHEMA,
            source_table=source_table,
            target_schema="dpone_it",
            target_table=target_table,
            staging_schema="staging",
            load_strategy=LoadStrategy.PARTITION_REPLACE,
            partition={
                "column": "partition_id",
                "values_from_staging": True,
                "max_partitions_per_run": max_partitions,
            },
            export_format="csv",
            compress_export=False,
            options={
                "source_type": "postgres",
                "sink_type": "mssql",
                "batch_commit_mode": "whole",
                "work_dir": str(tmp_path),
                "technical_columns": "forbidden",
                "lineage": False,
                "schema_contract": {
                    "enforcement": "strict",
                    "columns": {
                        "id": {"type": "integer", "nullable": False},
                        "partition_id": {"type": "string", "nullable": False},
                        "value": {"type": "string", "nullable": False},
                    },
                },
            },
        )

    logger = QuietIntegrationLogger()
    baseline_config = config(len(partition_values))
    processor = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        baseline_config,
        logger=logger,
    )
    set_source(partition_values, prefix="baseline")
    baseline = processor.run(
        baseline_config,
        label=f"partition_identity_{suffix}_baseline",
    )
    assert baseline["inserted_rows"] == len(partition_values)
    before = mssql.get_records(
        f"SELECT [partition_id], [value] FROM [dpone_it].[{target_table}] "
        "ORDER BY CONVERT(varbinary(max), [partition_id])",
        as_dict=True,
    )

    set_source(partition_values, prefix="must-not-land")
    with pytest.raises(ValueError, match="above max_partitions_per_run=1"):
        limited = config(1)
        limited.target_database = governed_mssql_live_campaign.target_database
        processor.run(limited, label=f"partition_identity_{suffix}_limit")
    assert (
        mssql.get_records(
            f"SELECT [partition_id], [value] FROM [dpone_it].[{target_table}] "
            "ORDER BY CONVERT(varbinary(max), [partition_id])",
            as_dict=True,
        )
        == before
    )

    set_source(("trail", "trail "), prefix="must-not-land")
    with pytest.raises(
        SnapshotReconciliationError,
        match="mssql_native_projection.text_key_trailing_space_unsupported",
    ):
        trailing = config(2)
        trailing.target_database = governed_mssql_live_campaign.target_database
        processor.run(trailing, label=f"partition_identity_{suffix}_trailing")
    assert (
        mssql.get_records(
            f"SELECT [partition_id], [value] FROM [dpone_it].[{target_table}] "
            "ORDER BY CONVERT(varbinary(max), [partition_id])",
            as_dict=True,
        )
        == before
    )

    set_source(("A",), prefix="changed")
    changed_config = config(1)
    changed_config.target_database = governed_mssql_live_campaign.target_database
    changed = processor.run(changed_config, label=f"partition_identity_{suffix}_changed")
    after = mssql.get_records(
        f"SELECT [partition_id], [value] FROM [dpone_it].[{target_table}] "
        "ORDER BY CONVERT(varbinary(max), [partition_id])",
        as_dict=True,
    )
    assert changed["replaced_rows"] == 1
    assert {row["partition_id"]: row["value"] for row in after} == {
        value: ("changed:A" if value == "A" else f"baseline:{value}") for value in partition_values
    }
    assert not tuple(tmp_path.iterdir())


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_to_mssql_batch_strategy_capability_is_exact_and_unsupported_fails_closed(
    tmp_path: Path,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> None:
    """Prove all implemented strategies are enumerated and CDC mutates nothing."""

    table = f"{WIDE_TABLE}_unsupported"
    postgres, mssql, _columns = _ready(table)
    config = cfg.cdc_apply(tmp_path=tmp_path, table=table)
    logger = QuietIntegrationLogger()
    runner = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=logger,
        source_type=GovernedPostgresSnapshotSource,
    )
    supported = set(LoadStrategy) - {LoadStrategy.CDC_APPLY}
    assert set(runner.sink._strategy_map) == supported

    mssql.execute_query(f"CREATE TABLE {fq(table)} ([id] int NOT NULL, [sentinel] nvarchar(32) NOT NULL)")
    mssql.execute_query(f"INSERT INTO {fq(table)} ([id], [sentinel]) VALUES (1, N'before-image')")
    before = mssql.get_records(f"SELECT [id], [sentinel] FROM {fq(table)}", as_dict=True)

    with pytest.raises(MSSQLStrategyContractError) as raised:
        runner.run(config, label="wide_cdc_unsupported")
    assert raised.value.code == "DPONE_MSSQL_STRATEGY_CONTRACT_BLOCKED"
    assert raised.value.blocker == "mssql.strategy.mode"

    assert mssql.get_records(f"SELECT [id], [sentinel] FROM {fq(table)}", as_dict=True) == before
    staging = mssql.get_records(
        "SELECT COUNT_BIG(*) AS object_count FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = N'staging' AND t.name LIKE ?",
        (f"{table}%",),
        as_dict=True,
    )
    assert int(staging[0]["object_count"]) == 0


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
@_certifies("full_refresh")
def test_postgres_to_mssql_full_refresh_wide_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> WideRoundtripProof:
    del route_live_recorder  # Consumed by the certification decorator.
    table = f"{WIDE_TABLE}_fr"
    postgres, mssql, columns = _ready(table)
    config = cfg.full_refresh(tmp_path=tmp_path, table=table)
    logger = QuietIntegrationLogger()
    result = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=logger,
    ).run(config, label="wide_full_refresh")
    assert result["status"] == "success"
    assert result["loaded_rows"] == 2
    assert_row_count(mssql, table=table, expected=2)
    proof = assert_complete_wide_roundtrip(postgres, mssql, table=table, columns=columns)
    assert not tuple(tmp_path.iterdir())
    return proof


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
@_certifies_fail_closed("incremental_append")
def test_postgres_to_mssql_incremental_append_column_cursor_fails_closed_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    del route_live_recorder  # Consumed by the certification decorator.
    table = f"{WIDE_TABLE}_append"
    postgres, mssql, _columns = _ready(table)
    config = cfg.incremental_append(tmp_path=tmp_path, table=table)
    strategy = PostgresIncrementalExtractStrategy(postgres, sink_connector=mssql, logger=NoopLogger())

    with pytest.raises(ValueError, match=POSTGRES_MSSQL_COLUMN_CURSOR_ERROR_CODE):
        strategy.get_state(config)

    assert mssql.table_exists(cfg.TARGET_SCHEMA, table, database=config.target_database) is False
    assert not tuple(tmp_path.iterdir())


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
@_certifies_fail_closed("incremental_merge")
def test_postgres_to_mssql_incremental_merge_column_cursor_fails_closed_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
) -> None:
    del route_live_recorder  # Consumed by the certification decorator.
    table = f"{WIDE_TABLE}_merge"
    postgres, mssql, _columns = _ready(table)
    config = cfg.incremental_merge(tmp_path=tmp_path, table=table)
    strategy = PostgresIncrementalExtractStrategy(postgres, sink_connector=mssql, logger=NoopLogger())

    with pytest.raises(ValueError, match=POSTGRES_MSSQL_COLUMN_CURSOR_ERROR_CODE):
        strategy.extract(config, last_state={"last_value": "2026-07-21 10:00:00.000000"})

    assert mssql.table_exists(cfg.TARGET_SCHEMA, table, database=config.target_database) is False
    assert not tuple(tmp_path.iterdir())


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
@_certifies_fail_closed("replace")
def test_postgres_to_mssql_replace_wide_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> None:
    del route_live_recorder  # Consumed by the certification decorator.
    table = f"{WIDE_TABLE}_replace"
    postgres, mssql, _columns = _ready(table)
    config = cfg.replace(tmp_path=tmp_path, table=table)
    runner = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=QuietIntegrationLogger(),
    )
    with pytest.raises(MSSQLStrategyContractError) as raised:
        runner.run(config, label="wide_replace_cross_dialect_scope")
    assert raised.value.blocker == _FAIL_CLOSED_BLOCKERS["replace"]
    assert not mssql.table_exists(cfg.TARGET_SCHEMA, table, database=config.target_database)
    assert not tuple(tmp_path.iterdir())


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
@_certifies("partition_replace")
def test_postgres_to_mssql_partition_replace_wide_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> WideRoundtripProof:
    del route_live_recorder  # Consumed by the certification decorator.
    table = f"{WIDE_TABLE}_part"
    postgres, mssql, columns = _ready(table)
    fr = cfg.full_refresh(tmp_path=tmp_path, table=table)
    logger = QuietIntegrationLogger()
    runner = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        fr,
        logger=logger,
    )
    runner.run(fr, label="wide_partition_baseline")
    _clone_complete_target_sentinel(
        mssql,
        table=table,
        row_id=99,
        business_date="2020-01-01",
        updated_at="2020-01-01T00:00:00",
        name="other-partition",
    )
    postgres.execute_query(f'UPDATE "{SOURCE_SCHEMA}"."{table}" SET c_name = \'part-replaced\' WHERE id = 1')
    config = cfg.partition_replace(tmp_path=tmp_path, table=table)
    config.target_database = governed_mssql_live_campaign.target_database
    runner.run(config, label="wide_partition_changed")
    rows = mssql.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id", as_dict=True)
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert by_id[1] == "part-replaced"
    assert by_id[99] == "other-partition"
    assert_unique_ids(mssql, table=table)
    return assert_complete_wide_roundtrip(postgres, mssql, table=table, columns=columns)


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
@_certifies("snapshot_diff")
def test_postgres_to_mssql_snapshot_diff_wide_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> WideRoundtripProof:
    del route_live_recorder  # Consumed by the certification decorator.
    table = f"{WIDE_TABLE}_diff"
    postgres, mssql, columns = _ready(table)
    config = cfg.snapshot_diff(tmp_path=tmp_path, table=table)
    runner = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=QuietIntegrationLogger(),
    )
    runner.run(config, label="wide_snapshot_baseline")
    assert_row_count(mssql, table=table, expected=2)
    postgres.execute_query(f'DELETE FROM "{SOURCE_SCHEMA}"."{table}" WHERE id = 2')
    postgres.execute_query(f'UPDATE "{SOURCE_SCHEMA}"."{table}" SET c_name = \'diff-updated\' WHERE id = 1')
    insert_wide_watermark_row(postgres, table=table)
    runner.run(config, label="wide_snapshot_changed")
    rows = mssql.get_records(f"SELECT id, c_name FROM {fq(table)} ORDER BY id", as_dict=True)
    by_id = {int(r["id"]): r["c_name"] for r in rows}
    assert 2 not in by_id
    assert by_id[1] == "diff-updated"
    assert by_id[3] == "row-three"
    return assert_complete_wide_roundtrip(postgres, mssql, table=table, columns=columns)


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
@_certifies("scd2")
def test_postgres_to_mssql_scd2_wide_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> WideRoundtripProof:
    del route_live_recorder  # Consumed by the certification decorator.
    table = f"{WIDE_TABLE}_scd2"
    postgres, mssql, columns = _ready(table)
    config = cfg.scd2(tmp_path=tmp_path, table=table)
    runner = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=QuietIntegrationLogger(),
    )
    runner.run(config, label="wide_scd2_baseline")
    cols = mssql.get_records(
        f"""
        SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = 'dpone_it' AND TABLE_NAME = '{table}'
        """,
        as_dict=True,
    )
    names = {row["COLUMN_NAME"] for row in cols}
    assert "__dpone__is_current" in names
    assert "__dpone__row_hash" in names
    postgres.execute_query(
        f'UPDATE "{SOURCE_SCHEMA}"."{table}" '
        f"SET c_name = 'scd2-v2', updated_at = TIMESTAMP '2026-07-21 13:00:00' WHERE id = 1"
    )
    runner.run(config, label="wide_scd2_changed")
    currents = mssql.get_records(
        f"SELECT id, c_name, __dpone__is_current AS is_current FROM {fq(table)} "
        f"WHERE id = 1 ORDER BY __dpone__is_current DESC",
        as_dict=True,
    )
    assert any(bool(r["is_current"]) and r["c_name"] == "scd2-v2" for r in currents)
    assert any(not bool(r["is_current"]) for r in currents)
    return assert_complete_wide_roundtrip(
        postgres,
        mssql,
        table=table,
        columns=columns,
        current_only=True,
    )


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
@_certifies_fail_closed("backfill")
def test_postgres_to_mssql_backfill_replace_wide_live(
    tmp_path: Path,
    route_live_recorder: RouteLiveObservationRecorder,
    governed_mssql_live_campaign: GovernedMssqlCampaign,
) -> None:
    del route_live_recorder  # Consumed by the certification decorator.
    table = f"{WIDE_TABLE}_backfill"
    postgres, mssql, _columns = _ready(table)
    config = cfg.backfill_replace(tmp_path=tmp_path, table=table)
    runner = _governed_runner(
        governed_mssql_live_campaign,
        postgres,
        config,
        logger=QuietIntegrationLogger(),
    )
    with pytest.raises(MSSQLStrategyContractError) as raised:
        runner.run(config, label="wide_backfill_cross_dialect_scope")
    assert raised.value.blocker == _FAIL_CLOSED_BLOCKERS["backfill"]
    assert not mssql.table_exists(cfg.TARGET_SCHEMA, table, database=config.target_database)
    assert not tuple(tmp_path.iterdir())


@pytest.mark.skipif(not postgres_mssql_enabled(), reason="Postgres/MSSQL Docker IT not configured")
def test_postgres_to_mssql_wide_strategy_matrix_is_complete() -> None:
    """Reject partial runs: every implemented batch strategy must have PASS evidence."""

    completed = {path.stem for path in _EVIDENCE_ROOT.glob("*.json")}
    assert completed == _MATRIX_CASES
    for path in _EVIDENCE_ROOT.glob("*.json"):
        payload = json.loads(path.read_text(encoding="utf-8"))
        assert payload["passed"] is True
        assert int(payload["wide_column_count"]) >= 120

    postgres = postgres_connector()
    mssql = mssql_connector()
    try:
        postgres_version = str(postgres.get_records("SHOW server_version")[0][0])
        version_row = mssql.get_records(
            "SELECT CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)), "
            "CAST(SERVERPROPERTY('Edition') AS nvarchar(128))"
        )[0]
        mssql_version = str(version_row[0])
        mssql_edition = str(version_row[1])
    finally:
        postgres.close()
        mssql.close()

    certified = TypeCertificationSuiteRegistry.default().suite("postgres", "mssql")
    _EVIDENCE_SUMMARY.parent.mkdir(parents=True, exist_ok=True)
    _EVIDENCE_SUMMARY.write_text(
        json.dumps(
            {
                "schema_version": "dpone.postgres_mssql.wide_strategy_matrix.v1",
                "status": "local_certification_passed",
                "release_ready": False,
                "source": {"vendor": "PostgreSQL", "version": postgres_version},
                "sink": {
                    "vendor": "Microsoft SQL Server",
                    "version": mssql_version,
                    "edition": mssql_edition,
                },
                "connector_doubles": False,
                "wide_column_count": WIDE_COLUMN_TARGET,
                "authoritative_type_case_count": len(certified.cases),
                "supported_batch_strategies": sorted(_SUPPORTED_CASES),
                "unsupported_fail_closed": ["cdc_apply", *sorted(_FAIL_CLOSED_CASES)],
                "case_evidence": sorted(str(path) for path in _EVIDENCE_ROOT.glob("*.json")),
                "remaining_release_gates": [
                    "published_release_pin",
                    "production_compression_benchmark",
                    "production_soak",
                ],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
