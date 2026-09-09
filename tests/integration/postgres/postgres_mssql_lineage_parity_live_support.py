"""Real-vendor support for the reviewed MSSQL lineage transport matrix.

This module deliberately certifies a sink-side contract.  Its PostgreSQL
adapter opens one read-only ``REPEATABLE READ`` snapshot before reading either
the relation catalog or rows and declares that complete relation snapshot
stateless to generic MSSQL transaction admission.  It does not emulate or
claim support for a production column cursor.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, ClassVar

from psycopg import sql

from dpone.config import LoadConfig, LoadStrategy
from dpone.config.source_scope_contract import resolve_source_scope
from dpone.contracts.mssql_source_checkpoint import MssqlTransactionCheckpointMode
from dpone.contracts.portable_relation_scope import (
    parse_portable_relation_scope,
    portable_scope_sha256,
    resolve_portable_relation_scope,
)
from dpone.contracts.portable_scope_binding import (
    PortableScopeBinding,
    PortableScopeColumnContract,
    bind_portable_scope,
    require_portable_scope_binding,
)
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleAuthority
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sinks.load_result import AtomicCommitOutcome
from dpone.runtime.sinks.strategies.mssql.mssql_portable_scope import (
    render_mssql_portable_scope,
)
from dpone.runtime.sources.extract_result import ExtractResult
from dpone.runtime.sources.strategies.postgres.postgres_portable_scope import (
    render_postgres_portable_scope,
)
from dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary import (
    prepared_postgres_source_boundary,
)
from dpone.runtime.sources.streaming_row_guards import assert_dict_rows_preserve_columns
from dpone.runtime.streaming_rows import StreamingRowsArtifact
from dpone.type_system.source_sink.provenance import SourceRelationDialect
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    GovernedMssqlRoute,
    GovernedPostgresSnapshotSource,
    GovernedRunContext,
    GovernedStandardEtlRunner,
)

SOURCE_SCHEMA = "dpone_lineage"
SOURCE_TABLE = "lineage_parity_source"
TARGET_SCHEMA = "dpone_it"
STAGING_SCHEMA = "staging"
SCOPE_REPLAY_TARGET_TABLE = "lin_portable_scope_replay"
SOURCE_ROWS: tuple[tuple[int, int, str | None], ...] = (
    (1, 1, "alpha"),
    (2, 2, "βeta"),
    (3, 1, None),
)

STRATEGIES: tuple[str, ...] = (
    "full_refresh",
    "incremental_append",
    "incremental_merge",
    "replace",
    "partition_replace",
    "snapshot_diff",
    "scd2",
    "backfill",
)
TRANSPORTS: tuple[str, ...] = (
    "python_rows",
    "file_stream",
    "postgres_copy_mssql_bcp",
)
SOURCE_BOUNDARY = "postgres_complete_relation_snapshot_test_adapter"

_SAFE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")
_HEX_32 = re.compile(r"^[0-9a-fA-F]{64}$")


@dataclass(frozen=True, slots=True)
class LineageLiveCase:
    """One reviewed strategy/transport configuration."""

    case_id: str
    strategy: str
    transport: str
    load_config: LoadConfig


@dataclass(frozen=True, slots=True)
class LineageRunEvidence:
    """Runtime objects retained only for terminal and identity assertions."""

    result: Mapping[str, Any]
    artifact: Any
    source_receipt: Any
    invocation: GovernedRunContext
    source_identity: Mapping[str, Any]
    portable_scope_binding: PortableScopeBinding | None


class CompleteSnapshotLineagePostgresSource(GovernedPostgresSnapshotSource):
    """Read real PostgreSQL catalog and rows in one explicit RR snapshot."""

    lineage_transport: ClassVar[str]
    checkpoint_persist_calls: int
    last_artifact: Any | None
    last_portable_scope_binding: PortableScopeBinding | None
    extract_calls: int
    last_verified_source_identity: Mapping[str, Any] | None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.checkpoint_persist_calls = 0
        self.last_artifact = None
        self.last_portable_scope_binding = None
        self.extract_calls = 0
        self.last_verified_source_identity = None

    def mssql_transaction_checkpoint_mode(self, _load_config: Any) -> MssqlTransactionCheckpointMode:
        """Declare only the reviewed complete-relation snapshot as stateless."""

        return MssqlTransactionCheckpointMode.STATELESS

    def save_state(self, _load_config: Any, _state: Any) -> None:
        """Expose an assertion counter; a stateless adapter must never persist."""

        self.checkpoint_persist_calls += 1

    def extract(self, load_config: LoadConfig, _last_state: Any | None) -> ExtractResult:
        """Build one of three artifacts without changing the snapshot boundary."""

        self.extract_calls += 1
        strategy = self._full_extract
        prepared = prepared_postgres_source_boundary(load_config)
        if prepared is None:
            lifecycle = strategy._new_extraction_lifecycle()
            lease = strategy._begin_repeatable_read_snapshot(lifecycle)
        else:
            prepared.require_active(self.connector)
            lifecycle = prepared.snapshot_lease.lifecycle
            lease = prepared.snapshot_lease
        transaction_owned = True
        artifact: Any | None = None
        try:
            lease.require_for(self.connector)
            if prepared is None:
                verified_identity = strategy._verify_postgres_source_authority(
                    lease,
                    load_config,
                )
                projection = strategy.fetch_schema_projection(load_config)
            else:
                verified_identity = prepared.source_identity
                projection = prepared.schema_projection
            self.last_verified_source_identity = verified_identity.to_dict()
            schema = list(projection.projected_schema)
            portable_scope = resolve_portable_relation_scope(load_config)
            predicate = resolve_source_scope(load_config).predicate
            query = strategy.format_select_query(
                load_config.source_schema,
                load_config.source_table,
                [name for name, _dtype in schema],
                predicate if portable_scope is None else None,
            )
            query_params: tuple[object, ...] = ()
            if portable_scope is not None:
                binding = require_portable_scope_binding(load_config, portable_scope)
                self.last_portable_scope_binding = binding
                rendered = render_postgres_portable_scope(
                    portable_scope,
                    binding,
                )
                query = sql.Composed([query, sql.SQL(" WHERE "), rendered.sql])
                query_params = rendered.params
            expected_columns = tuple(name for name, _dtype in schema)
            if self.lineage_transport == "python_rows":
                rows = self.connector.get_records(query, params=query_params, as_dict=True)
                assert_dict_rows_preserve_columns(rows, expected_columns=expected_columns)
                artifact = InMemoryRowsArtifact(rows)
                artifact.bind_extraction_lifecycle(lifecycle)
                if prepared is None:
                    lifecycle.complete()
                    self.connector.commit_transaction()
                else:
                    prepared.complete()
                transaction_owned = False
            elif self.lineage_transport == "file_stream":
                iterator = strategy._iter_guarded_dict_rows(
                    self.connector,
                    query,
                    params=query_params,
                    batch_size=load_config.batch_size,
                    expected_columns=expected_columns,
                )
                artifact = StreamingRowsArtifact(
                    iterator,
                    batch_size=load_config.batch_size,
                    extraction_lifecycle=lifecycle,
                    on_success=self.connector.commit_transaction,
                    on_abort=self.connector.rollback,
                )
                # The artifact now owns the open snapshot until the top-level
                # target terminal decision is known.
                transaction_owned = False
            elif self.lineage_transport == "postgres_copy_mssql_bcp":
                artifact = strategy._export_to_file_whole(
                    query,
                    schema,
                    load_config,
                    snapshot_lease=lease,
                    relation_schema=projection.relation_schema,
                    query_params=query_params,
                )
                if prepared is None:
                    lifecycle.complete()
                    self.connector.commit_transaction()
                else:
                    prepared.complete()
                transaction_owned = False
            else:  # pragma: no cover - the reviewed finite transport set owns this guard.
                raise AssertionError(f"unsupported lineage transport: {self.lineage_transport}")
        except BaseException as primary:
            if transaction_owned:
                if prepared is None:
                    with suppress(Exception):
                        self.connector.rollback()
                else:
                    with suppress(Exception):
                        prepared.abort_preserving(primary)
            if artifact is not None:
                with suppress(Exception):
                    artifact.cleanup()
            raise

        self.last_artifact = artifact
        return ExtractResult(
            artifact=artifact,
            schema=schema,
            relation_schema=projection.relation_schema,
            relation_metadata=projection.relation_metadata,
            relation_dialect=SourceRelationDialect.POSTGRES,
            target_projection=projection.target_projection,
            state=None,
            extraction_lifecycle=lifecycle,
        )


class PythonRowsLineagePostgresSource(CompleteSnapshotLineagePostgresSource):
    lineage_transport = "python_rows"


class FileStreamLineagePostgresSource(CompleteSnapshotLineagePostgresSource):
    lineage_transport = "file_stream"


class PostgresCopyMssqlBcpLineagePostgresSource(CompleteSnapshotLineagePostgresSource):
    lineage_transport = "postgres_copy_mssql_bcp"


SOURCE_TYPES: Mapping[str, type[CompleteSnapshotLineagePostgresSource]] = {
    "python_rows": PythonRowsLineagePostgresSource,
    "file_stream": FileStreamLineagePostgresSource,
    "postgres_copy_mssql_bcp": PostgresCopyMssqlBcpLineagePostgresSource,
}


def ensure_source(postgres: Any) -> None:
    """Install the stable three-row lineage relation on the live PostgreSQL."""

    postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{SOURCE_SCHEMA}"')
    postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{SOURCE_TABLE}"')
    postgres.execute_query(
        f'CREATE TABLE "{SOURCE_SCHEMA}"."{SOURCE_TABLE}" ('
        "id integer NOT NULL, partition_id integer NOT NULL, value text NULL)"
    )
    for row in SOURCE_ROWS:
        postgres.execute_query(
            f'INSERT INTO "{SOURCE_SCHEMA}"."{SOURCE_TABLE}" (id, partition_id, value) VALUES (%s, %s, %s)',
            row,
        )


def drop_source(postgres: Any) -> None:
    """Remove only the relation owned by this matrix."""

    postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{SOURCE_TABLE}"')


def lineage_cases(tmp_path: Path) -> tuple[LineageLiveCase, ...]:
    """Return the exact reviewed 8x3 finite matrix."""

    cases: list[LineageLiveCase] = []
    for strategy in STRATEGIES:
        for transport in TRANSPORTS:
            case_id = f"{strategy}__{transport}"
            cases.append(
                LineageLiveCase(
                    case_id=case_id,
                    strategy=strategy,
                    transport=transport,
                    load_config=_config(case_id, strategy, tmp_path / case_id),
                )
            )
    return tuple(cases)


def run_lineage_case(
    route: GovernedMssqlRoute,
    postgres: Any,
    case: LineageLiveCase,
    *,
    logger: Any,
) -> LineageRunEvidence:
    """Run normal governed ETL with only the reviewed source artifact adapter."""

    source_type = SOURCE_TYPES[case.transport]
    runner = GovernedStandardEtlRunner(
        route,
        postgres,
        logger=logger,
        source_type=source_type,
    )
    source = runner.source
    if not isinstance(source, CompleteSnapshotLineagePostgresSource):
        raise AssertionError("lineage live runner did not retain the reviewed source adapter")
    invocation = route.run_context(f"lineage_{case.case_id}")
    result = runner.run(
        case.load_config,
        label=f"lineage_{case.case_id}",
        run_context=invocation,
    )
    expected_source_identity = source.mssql_transaction_source_physical_identity(case.load_config).to_dict()
    if source.last_verified_source_identity != expected_source_identity:
        raise AssertionError("lineage source RR identity differs from its signed authority")
    artifact = source.last_artifact
    if artifact is None:
        raise AssertionError("lineage source artifact was not published")
    lifecycle = getattr(artifact, "extraction_lifecycle", None)
    receipt = lifecycle.require_completed() if isinstance(lifecycle, ExtractionLifecycleAuthority) else None
    if receipt is None:
        raise AssertionError("lineage source lifecycle receipt is missing")
    if source.checkpoint_persist_calls != 0:
        raise AssertionError("stateless complete-snapshot adapter persisted a source checkpoint")
    return LineageRunEvidence(
        result=dict(result),
        artifact=artifact,
        source_receipt=receipt,
        invocation=invocation,
        source_identity=source.mssql_transaction_source_physical_identity(case.load_config).to_dict(),
        portable_scope_binding=source.last_portable_scope_binding,
    )


def assert_replace_scope_catalog_binding(
    postgres: Any,
    case: LineageLiveCase,
    run: LineageRunEvidence,
    catalog: Mapping[str, Any],
) -> None:
    """Prove the integer scope binding against both real vendor catalogs."""

    if case.strategy != "replace":
        if run.portable_scope_binding is not None:
            raise AssertionError("non-replace lineage case unexpectedly retained a portable scope binding")
        return
    binding = run.portable_scope_binding
    if binding is None:
        raise AssertionError("replace lineage case did not retain its pre-admission catalog binding")
    scope = resolve_portable_relation_scope(case.load_config)
    if scope is None:
        raise AssertionError("replace lineage case lost its portable scope AST")
    source_rows = postgres.get_records(
        """
        SELECT data_type, numeric_precision, numeric_scale, collation_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s AND column_name = %s
        """,
        params=(SOURCE_SCHEMA, SOURCE_TABLE, scope.column),
        as_dict=True,
    )
    if len(source_rows) != 1:
        raise AssertionError("portable scope source catalog column is not exact")
    assert dict(source_rows[0]) == {
        "data_type": "integer",
        "numeric_precision": 32,
        "numeric_scale": 0,
        "collation_name": None,
    }
    target_rows = [row for row in catalog["columns"] if row["column_name"] == scope.column]
    if len(target_rows) != 1:
        raise AssertionError("portable scope target catalog column is not exact")
    target = target_rows[0]
    assert {
        "type_name": target["type_name"],
        "precision": target["precision"],
        "scale": target["scale"],
        "collation_name": target["collation_name"],
    } == {
        "type_name": "int",
        "precision": 10,
        "scale": 0,
        "collation_name": None,
    }
    assert binding.to_contract() == {
        "ast_sha256": portable_scope_sha256(scope).hex(),
        "column": "partition_id",
        "literal_type": "integer",
        "source_type": "integer",
        "source_collation": None,
        "target_type": "int",
        "target_collation": None,
        "comparison_contract": "exact_integer_v1",
        "version": 1,
    }


def prove_temporal_convert_vendor_surface(postgres: Any, mssql: Any) -> None:
    """Execute all temporal portable predicates against both vendor catalogs."""

    source_table = "portable_temporal_scope_probe"
    target_table = "lin_portable_temporal_scope_probe"
    postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{source_table}"')
    mssql.execute_query(f"DROP TABLE IF EXISTS [{TARGET_SCHEMA}].[{target_table}]")
    try:
        postgres.execute_query(
            f'CREATE TABLE "{SOURCE_SCHEMA}"."{source_table}" ('
            "boundary_date date NOT NULL, "
            "boundary_timestamp timestamp(6) without time zone NOT NULL, "
            "boundary_timestamptz timestamp(6) with time zone NOT NULL)"
        )
        postgres.execute_query(
            f'INSERT INTO "{SOURCE_SCHEMA}"."{source_table}" VALUES (%s, %s, %s)',
            (
                date(2026, 8, 16),
                datetime(2026, 8, 16, 12, 34, 56, 123456),
                datetime(2026, 8, 16, 9, 34, 56, 123456, tzinfo=UTC),
            ),
        )
        mssql.execute_query(
            f"CREATE TABLE [{TARGET_SCHEMA}].[{target_table}] ("
            "[boundary_date] date NOT NULL, "
            "[boundary_timestamp] datetime2(6) NOT NULL, "
            "[boundary_timestamptz] datetimeoffset(6) NOT NULL)"
        )
        mssql.execute_query(
            f"INSERT INTO [{TARGET_SCHEMA}].[{target_table}] VALUES ("
            "CONVERT(date, ?), CONVERT(datetime2(6), ?), CONVERT(datetimeoffset(6), ?))",
            (
                "2026-08-16",
                "2026-08-16T12:34:56.123456",
                "2026-08-16T09:34:56.123456Z",
            ),
        )
        source_catalog = postgres.get_records(
            """
            SELECT a.attname AS column_name,
                   format_type(a.atttypid, a.atttypmod) AS type_name,
                   CASE WHEN a.attcollation = 0 THEN NULL ELSE c.collname END AS collation_name
            FROM pg_catalog.pg_attribute AS a
            INNER JOIN pg_catalog.pg_class AS t ON t.oid = a.attrelid
            INNER JOIN pg_catalog.pg_namespace AS n ON n.oid = t.relnamespace
            LEFT JOIN pg_catalog.pg_collation AS c ON c.oid = a.attcollation
            WHERE n.nspname = %s AND t.relname = %s
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY a.attnum
            """,
            params=(SOURCE_SCHEMA, source_table),
            as_dict=True,
        )
        target_catalog_rows = mssql.get_records(
            """
            SELECT c.name AS column_name, ty.name AS type_name,
                   c.max_length, c.precision, c.scale, c.collation_name
            FROM sys.columns AS c
            INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(?)
            ORDER BY c.column_id
            """,
            (f"{TARGET_SCHEMA}.{target_table}",),
            as_dict=True,
        )
        expected_source = (
            ("boundary_date", "date", None),
            ("boundary_timestamp", "timestamp(6) without time zone", None),
            ("boundary_timestamptz", "timestamp(6) with time zone", None),
        )
        expected_target = (
            ("boundary_date", "date", 3, 10, 0, None),
            ("boundary_timestamp", "datetime2", 8, 26, 6, None),
            ("boundary_timestamptz", "datetimeoffset", 10, 33, 6, None),
        )
        assert tuple(tuple(row.values()) for row in source_catalog) == expected_source
        assert tuple(tuple(row.values()) for row in target_catalog_rows) == expected_target

        literals = (
            ("boundary_date", "date", "2026-08-16", "date", "date", "CONVERT(date, ?)"),
            (
                "boundary_timestamp",
                "timestamp",
                "2026-08-16T12:34:56.123456",
                "timestamp(6) without time zone",
                "datetime2(6)",
                "CONVERT(datetime2(6), ?)",
            ),
            (
                "boundary_timestamptz",
                "timestamptz",
                "2026-08-16T09:34:56.123456Z",
                "timestamp(6) with time zone",
                "datetimeoffset(6)",
                "CONVERT(datetimeoffset(6), ?)",
            ),
        )
        for column, literal_type, value, source_type, target_type, conversion in literals:
            scope = parse_portable_relation_scope(
                {
                    "kind": "equality",
                    "column": column,
                    "value": {"type": literal_type, "value": value},
                }
            )
            binding = bind_portable_scope(
                scope,
                PortableScopeColumnContract(
                    column,
                    source_type,
                    None,
                    column,
                    target_type,
                    None,
                ),
            )
            pg_predicate = render_postgres_portable_scope(scope, binding)
            pg_query = (
                sql.SQL("SELECT COUNT(*) FROM {}.{} WHERE ").format(
                    sql.Identifier(SOURCE_SCHEMA),
                    sql.Identifier(source_table),
                )
                + pg_predicate.sql
            )
            assert int(postgres.get_records(pg_query, params=pg_predicate.params)[0][0]) == 1
            mssql_predicate = render_mssql_portable_scope(
                scope,
                binding,
                quote_identifier=_quote_mssql_identifier,
            )
            assert mssql_predicate.sql == f"[{column}] = {conversion}"
            target_query = f"SELECT COUNT_BIG(*) FROM [{TARGET_SCHEMA}].[{target_table}] WHERE {mssql_predicate.sql}"
            assert int(mssql.get_records(target_query, mssql_predicate.params)[0][0]) == 1
    finally:
        mssql.execute_query(f"DROP TABLE IF EXISTS [{TARGET_SCHEMA}].[{target_table}]")
        postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{source_table}"')


def prove_existing_target_scope_replay(
    route: GovernedMssqlRoute,
    postgres: Any,
    *,
    target_database: str,
    work_dir: Path,
    logger: Any,
) -> None:
    """Commit smallint→int evolution, then replay one exact invocation."""

    target_table = SCOPE_REPLAY_TARGET_TABLE
    target = route.target
    config = LoadConfig(
        source_conn_id="postgres_lineage_source",
        target_conn_id="mssql_lineage_sink",
        source_schema=SOURCE_SCHEMA,
        source_table=SOURCE_TABLE,
        target_database=target_database,
        target_schema=TARGET_SCHEMA,
        target_table=target_table,
        staging_database=target_database,
        staging_schema=STAGING_SCHEMA,
        load_strategy=LoadStrategy.REPLACE,
        portable_scope={
            "version": 1,
            "kind": "equality",
            "column": "partition_id",
            "value": {"type": "integer", "value": 1},
        },
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "work_dir": str(work_dir),
            "lineage": False,
            "technical_columns": "forbidden",
            "schema_evolution": {"allow_blocking_online": True},
        },
    )
    target.execute_query(f"DROP TABLE IF EXISTS [{TARGET_SCHEMA}].[{target_table}]")
    try:
        target.execute_query(
            f"CREATE TABLE [{TARGET_SCHEMA}].[{target_table}] ("
            "[id] int NOT NULL, [partition_id] smallint NOT NULL, [value] nvarchar(max) NULL)"
        )
        target.execute_query(
            f"INSERT INTO [{TARGET_SCHEMA}].[{target_table}] "
            "([id], [partition_id], [value]) VALUES (98, 1, N'stale'), (99, 2, N'preserve')"
        )
        runner = GovernedStandardEtlRunner(
            route,
            postgres,
            logger=logger,
            source_type=PythonRowsLineagePostgresSource,
        )
        source = runner.source
        if not isinstance(source, PythonRowsLineagePostgresSource):
            raise AssertionError("portable replay probe lost its complete-snapshot source adapter")
        invocation = route.run_context("portable_scope_schema_replay")
        first = runner.run(
            config,
            label="portable_scope_schema_replay",
            run_context=invocation,
        )
        expected_source_identity = source.mssql_transaction_source_physical_identity(config).to_dict()
        if source.last_verified_source_identity != expected_source_identity:
            raise AssertionError("portable replay RR identity differs from its signed authority")
        assert source.extract_calls == 1
        assert str(first["commit_outcome"]) == AtomicCommitOutcome.COMMITTED.value
        first_receipt = receipt_for_target(route, target_table)
        target_shape = target.get_records(
            """
            SELECT ty.name AS type_name, c.precision, c.scale, c.collation_name
            FROM sys.columns AS c
            INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
            WHERE c.object_id = OBJECT_ID(?) AND c.name = N'partition_id'
            """,
            (f"{TARGET_SCHEMA}.{target_table}",),
            as_dict=True,
        )
        assert target_shape == [{"type_name": "int", "precision": 10, "scale": 0, "collation_name": None}]
        assert target.get_records(
            f"SELECT [id], [partition_id], [value] FROM [{TARGET_SCHEMA}].[{target_table}] ORDER BY [id]"
        ) == [
            (1, 1, "alpha"),
            (3, 1, None),
            (99, 2, "preserve"),
        ]

        replay = runner.run(
            config,
            label="portable_scope_schema_replay",
            run_context=invocation,
        )
        assert str(replay["commit_outcome"]) == AtomicCommitOutcome.REPLAY_SUPPRESSED.value
        assert replay["commit_receipt_id"] == first["commit_receipt_id"]
        assert source.extract_calls == 1
        assert receipt_for_target(route, target_table) == first_receipt
    finally:
        target.execute_query(f"DROP TABLE IF EXISTS [{TARGET_SCHEMA}].[{target_table}]")


def target_clock(mssql: Any) -> datetime:
    """Read one SQL Server UTC clock boundary."""

    rows = mssql.get_records("SELECT SYSUTCDATETIME() AS observed_at", as_dict=True)
    if len(rows) != 1 or not isinstance(rows[0].get("observed_at"), datetime):
        raise AssertionError("SQL Server target clock is unavailable")
    return rows[0]["observed_at"]


def target_catalog(mssql: Any, table: str) -> dict[str, Any]:
    """Read a closed physical catalog image for one target table."""

    table = _safe(table)
    columns = mssql.get_records(
        """
        SELECT c.column_id, c.name AS column_name, ty.name AS type_name,
               c.max_length, c.precision, c.scale, c.is_nullable,
               c.collation_name, c.is_ansi_padded, c.is_rowguidcol,
               c.is_identity, c.is_computed, c.generated_always_type,
               c.is_filestream, c.is_sparse, c.is_column_set, c.is_hidden,
               c.is_masked, c.encryption_type, c.column_encryption_key_id,
               c.default_object_id, c.rule_object_id
        FROM sys.columns AS c
        INNER JOIN sys.types AS ty ON ty.user_type_id = c.user_type_id
        WHERE c.object_id = OBJECT_ID(?)
        ORDER BY c.column_id
        """,
        (f"{TARGET_SCHEMA}.{table}",),
        as_dict=True,
    )
    indexes = mssql.get_records(
        """
        SELECT i.name AS index_name, i.type_desc, i.is_unique,
               i.is_primary_key, i.is_unique_constraint, i.is_disabled,
               i.is_hypothetical, i.ignore_dup_key, i.filter_definition,
               ds.type_desc AS data_space_type_desc,
               c.name AS column_name, ic.key_ordinal,
               ic.is_descending_key, ic.is_included_column,
               p.partition_count, p.minimum_compression, p.maximum_compression
        FROM sys.indexes AS i
        INNER JOIN sys.index_columns AS ic
          ON ic.object_id = i.object_id AND ic.index_id = i.index_id
        INNER JOIN sys.columns AS c
          ON c.object_id = ic.object_id AND c.column_id = ic.column_id
        LEFT JOIN sys.data_spaces AS ds ON ds.data_space_id = i.data_space_id
        OUTER APPLY (
            SELECT COUNT_BIG(*) AS partition_count,
                   MIN(UPPER(data_compression_desc)) AS minimum_compression,
                   MAX(UPPER(data_compression_desc)) AS maximum_compression
            FROM sys.partitions
            WHERE object_id = i.object_id AND index_id = i.index_id
        ) AS p
        WHERE i.object_id = OBJECT_ID(?) AND i.index_id > 0
        ORDER BY i.index_id, ic.key_ordinal, ic.index_column_id
        """,
        (f"{TARGET_SCHEMA}.{table}",),
        as_dict=True,
    )
    object_counts = mssql.get_records(
        """
        SELECT
          (SELECT COUNT_BIG(*) FROM sys.triggers WHERE parent_id = OBJECT_ID(?)) AS triggers,
          (SELECT COUNT_BIG(*) FROM sys.check_constraints WHERE parent_object_id = OBJECT_ID(?)) AS checks,
          (SELECT COUNT_BIG(*) FROM sys.default_constraints WHERE parent_object_id = OBJECT_ID(?)) AS defaults,
          (SELECT COUNT_BIG(*) FROM sys.foreign_keys WHERE parent_object_id = OBJECT_ID(?)) AS foreign_keys,
          (SELECT COUNT_BIG(*) FROM sys.key_constraints WHERE parent_object_id = OBJECT_ID(?)) AS key_constraints
        """,
        (f"{TARGET_SCHEMA}.{table}",) * 5,
        as_dict=True,
    )
    exists = bool(columns)
    return {
        "exists": exists,
        "columns": [dict(row) for row in columns],
        "indexes": [dict(row) for row in indexes],
        "object_counts": dict(object_counts[0]) if object_counts else {},
    }


def target_rows(mssql: Any, table: str) -> list[dict[str, Any]]:
    """Read every target value in deterministic business/history order."""

    table = _safe(table)
    columns = mssql.get_records(
        "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) ORDER BY column_id",
        (f"{TARGET_SCHEMA}.{table}",),
    )
    names = [str(row[0]) for row in columns]
    selected = ", ".join(f"[{name}]" for name in names)
    order = "[id]" + (", [__dpone__valid_from_at]" if "__dpone__valid_from_at" in names else "")
    return mssql.get_records(
        f"SELECT {selected} FROM [{TARGET_SCHEMA}].[{table}] ORDER BY {order}",
        as_dict=True,
    )


def receipt_for_target(route: GovernedMssqlRoute, table: str) -> dict[str, Any]:
    """Read the one immutable generic receipt and all five identity classes."""

    rows = route.state.get_records(
        f"""
        SELECT r.receipt_id, r.operation_key, r.attempt_key, r.scope_hash,
               r.operation_epoch, r.owner_digest, r.load_id,
               r.payload_manifest_sha256, r.declared_rows, r.actual_raw_rows,
               r.actual_native_rows, r.native_contract_sha256,
               r.mutation_plan_sha256, r.target_before_sha256,
               r.target_after_sha256, r.extraction_started_at_utc,
               r.extraction_completed_at_utc, r.extraction_clock_authority,
               r.snapshot_acquired_at_utc, r.snapshot_authority,
               r.source_token_sha256, r.loaded_at_utc, r.committed_at_utc,
               r.inserted_rows, r.updated_rows, r.total_rows, r.staging_rows,
               r.replaced_rows, r.soft_deleted_rows, r.reactivated_rows,
               r.unchanged_rows, r.hard_deleted_rows, r.active_rows,
               a.target_identity, a.route_fingerprint, a.generation,
               a.target_database, a.target_schema, a.target_table, a.strategy,
               f.current_generation, f.current_attempt_key,
               f.current_route_fingerprint
        FROM [{route.state_database}].[system].[dpone_load_receipt] AS r
        INNER JOIN [{route.state_database}].[system].[dpone_load_attempt] AS a
          ON a.attempt_key = r.attempt_key
        INNER JOIN [{route.state_database}].[system].[dpone_target_fence] AS f
          ON f.target_identity = a.target_identity
        WHERE a.target_schema = ? AND a.target_table = ?
        """,
        (TARGET_SCHEMA, _safe(table)),
        as_dict=True,
    )
    if len(rows) != 1:
        raise AssertionError(f"expected one generic receipt for {table}, got {len(rows)}")
    return dict(rows[0])


def expected_row_ids(case: LineageLiveCase, rows: Sequence[Mapping[str, Any]]) -> dict[int, str]:
    """Compute SQL Server native-lineage SHA-256 independently in Python."""

    keyed = bool(case.load_config.unique_key)
    expected: dict[int, str] = {}
    for row in rows:
        business = {
            "id": int(row["id"]),
            "partition_id": int(row["partition_id"]),
            "value": row["value"],
        }
        selected = (
            (("id", "int"),)
            if keyed
            else (
                ("id", "int"),
                ("partition_id", "int"),
                ("value", "nvarchar(max)"),
            )
        )
        payload = f"v1|postgres|{SOURCE_SCHEMA}|{SOURCE_TABLE};"
        payload += "".join(_native_identity_hex(business[name], dtype) for name, dtype in selected)
        expected[int(row["id"])] = hashlib.sha256(payload.encode("utf-16le")).hexdigest().upper()
    return expected


def expected_row_hashes(rows: Sequence[Mapping[str, Any]]) -> dict[int, str]:
    """Compute snapshot/SCD native row hashes over the exact business schema."""

    expected: dict[int, str] = {}
    for row in rows:
        payload = "".join(
            (
                _native_identity_hex(int(row["id"]), "int"),
                _native_identity_hex(int(row["partition_id"]), "int"),
                _native_identity_hex(row["value"], "nvarchar(max)"),
            )
        )
        expected[int(row["id"])] = hashlib.sha256(payload.encode("utf-16le")).hexdigest().upper()
    return expected


def semantic_parity_image(
    case: LineageLiveCase,
    catalog: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    receipt: Mapping[str, Any],
) -> dict[str, Any]:
    """Normalize only invocation/clock/artifact identities for 3-way parity."""

    normalized_rows = []
    excluded = {
        "__dpone__run_id",
        "__dpone__load_id",
        "__dpone__loaded_at",
        "__dpone__extracted_at",
        "__dpone__valid_from_at",
        "__dpone__valid_to_at",
    }
    for row in rows:
        normalized_rows.append({key: value for key, value in row.items() if key not in excluded})
    normalized_indexes = []
    for index in catalog["indexes"]:
        value = dict(index)
        value["index_name"] = str(value["index_name"]).replace(case.load_config.target_table, "<target>")
        value["filter_definition"] = _canonical_filter(value.get("filter_definition"))
        normalized_indexes.append(value)
    return {
        "columns": catalog["columns"],
        "indexes": normalized_indexes,
        "object_counts": catalog["object_counts"],
        "rows": normalized_rows,
        "receipt_semantics": {
            name: receipt.get(name)
            for name in (
                "declared_rows",
                "actual_raw_rows",
                "actual_native_rows",
                "inserted_rows",
                "updated_rows",
                "total_rows",
                "staging_rows",
                "replaced_rows",
                "soft_deleted_rows",
                "reactivated_rows",
                "unchanged_rows",
                "hard_deleted_rows",
                "active_rows",
                "extraction_clock_authority",
                "snapshot_authority",
                "strategy",
            )
        },
    }


def assert_exact_catalog(case: LineageLiveCase, catalog: Mapping[str, Any], database_collation: str) -> None:
    """Assert the complete target column/index/behavior image."""

    expected_columns = [
        _column("id", "int", 4, 10, 0, False),
        _column("partition_id", "int", 4, 10, 0, False),
        _column("value", "nvarchar", -1, 0, 0, True, database_collation, ansi_padded=True),
    ]
    if case.strategy == "snapshot_diff":
        expected_columns.extend(
            (
                _column("__dpone__row_hash", "varchar", 64, 0, 0, False, database_collation, ansi_padded=True),
                _column("__dpone__deleted_at", "datetime2", 8, 27, 7, True),
            )
        )
    elif case.strategy == "scd2":
        expected_columns.extend(
            (
                _column("__dpone__row_hash", "varchar", 64, 0, 0, False, database_collation, ansi_padded=True),
                _column("__dpone__valid_from_at", "datetime2", 8, 27, 7, False),
                _column("__dpone__valid_to_at", "datetime2", 8, 27, 7, True),
                _column("__dpone__is_current", "bit", 1, 1, 0, False),
            )
        )
    expected_columns.extend(
        (
            _column("__dpone__run_id", "varchar", 26, 0, 0, False, database_collation, ansi_padded=True),
            _column("__dpone__load_id", "varchar", 26, 0, 0, False, database_collation, ansi_padded=True),
            _column("__dpone__loaded_at", "datetime2", 8, 27, 7, False),
            _column("__dpone__row_id", "varchar", 64, 0, 0, True, database_collation, ansi_padded=True),
            _column("__dpone__extracted_at", "datetime2", 8, 27, 7, False),
        )
    )
    observed_columns = [{key: value for key, value in row.items() if key != "column_id"} for row in catalog["columns"]]
    assert observed_columns == expected_columns, {
        "observed": observed_columns,
        "expected": expected_columns,
    }
    assert catalog["object_counts"] == {
        "triggers": 0,
        "checks": 0,
        "defaults": 0,
        "foreign_keys": 0,
        "key_constraints": 0,
    }

    keyed = bool(case.load_config.unique_key)
    indexes = list(catalog["indexes"])
    if not keyed:
        assert indexes == []
        return
    assert len(indexes) == 1
    index = indexes[0]
    expected_filter = "__dpone__is_current=1" if case.strategy == "scd2" else ""
    assert index == {
        "index_name": f"ux_dpone_{case.load_config.target_table}_id",
        "type_desc": "NONCLUSTERED",
        "is_unique": True,
        "is_primary_key": False,
        "is_unique_constraint": False,
        "is_disabled": False,
        "is_hypothetical": False,
        "ignore_dup_key": False,
        "filter_definition": index["filter_definition"],
        "data_space_type_desc": "ROWS_FILEGROUP",
        "column_name": "id",
        "key_ordinal": 1,
        "is_descending_key": False,
        "is_included_column": False,
        "partition_count": 1,
        "minimum_compression": "NONE",
        "maximum_compression": "NONE",
    }
    assert _canonical_filter(index["filter_definition"]) == expected_filter


def _config(case_id: str, strategy: str, work_dir: Path) -> LoadConfig:
    load_strategy = LoadStrategy(strategy)
    table = _safe("lin_" + case_id.replace("postgres_copy_mssql_bcp", "copy_bcp").replace("file_stream", "stream"))
    options: dict[str, Any] = {
        "source_type": "postgres",
        "sink_type": "mssql",
        "batch_commit_mode": "whole",
        "work_dir": str(work_dir),
        "technical_columns": "required",
        "lineage": {
            "enabled": True,
            "preset": "standard",
            "features": {"run_identity": True},
        },
        "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000}},
    }
    values: dict[str, Any] = {
        "source_conn_id": "postgres_lineage_source",
        "target_conn_id": "mssql_lineage_sink",
        "source_schema": SOURCE_SCHEMA,
        "source_table": SOURCE_TABLE,
        "target_schema": TARGET_SCHEMA,
        "target_table": table,
        "staging_schema": STAGING_SCHEMA,
        "load_strategy": load_strategy,
        "export_format": "csv",
        "compress_export": False,
        "options": options,
    }
    if strategy in {"incremental_merge", "snapshot_diff", "scd2", "backfill"}:
        values["unique_key"] = ["id"]
    if strategy == "replace":
        values["portable_scope"] = {
            "version": 1,
            "kind": "equality",
            "column": "partition_id",
            "value": {"type": "integer", "value": 1},
        }
    elif strategy == "partition_replace":
        values["partition"] = {
            "column": "partition_id",
            "native_mode": "fallback",
            "max_partitions_per_run": 4,
        }
    elif strategy == "snapshot_diff":
        options["diff"] = {"compare": "row_hash", "delete_policy": "ignore"}
    elif strategy == "scd2":
        options["scd2"] = {"delete_policy": "ignore"}
    elif strategy == "backfill":
        options["backfill"] = {"inner_mode": "incremental_merge", "parallel_workers": 1}
    return LoadConfig(**values)


def _column(
    name: str,
    type_name: str,
    max_length: int,
    precision: int,
    scale: int,
    nullable: bool,
    collation: str | None = None,
    *,
    ansi_padded: bool = False,
) -> dict[str, Any]:
    return {
        "column_name": name,
        "type_name": type_name,
        "max_length": max_length,
        "precision": precision,
        "scale": scale,
        "is_nullable": nullable,
        "collation_name": collation,
        "is_ansi_padded": ansi_padded,
        "is_rowguidcol": False,
        "is_identity": False,
        "is_computed": False,
        "generated_always_type": 0,
        "is_filestream": False,
        "is_sparse": False,
        "is_column_set": False,
        "is_hidden": False,
        "is_masked": False,
        "encryption_type": None,
        "column_encryption_key_id": None,
        "default_object_id": 0,
        "rule_object_id": 0,
    }


def _native_identity_hex(value: object, target_type: str) -> str:
    normalized = re.sub(r"\s+", "", target_type.strip().lower())
    if value is None:
        frame = f"{normalized}:N;"
    else:
        canonical = str(value)
        frame = f"{normalized}:V{len(canonical.encode('utf-16le'))}:{canonical};"
    return frame.encode("utf-16le").hex().upper()


def _canonical_filter(value: object) -> str:
    return "".join(character for character in str(value or "").casefold() if character not in "[]() \t\r\n")


def _safe(value: str) -> str:
    if _SAFE.fullmatch(value) is None:
        raise ValueError(f"unsafe lineage integration identifier: {value!r}")
    return value


def _quote_mssql_identifier(value: str) -> str:
    return "[" + value.replace("]", "]]") + "]"


def require_digest(value: object, label: str) -> bytes:
    """Normalize one pyodbc binary digest and assert exact SHA-256 width."""

    normalized = bytes(value) if isinstance(value, (bytes, bytearray, memoryview)) else b""
    if len(normalized) != 32 or _HEX_32.fullmatch(normalized.hex()) is None:
        raise AssertionError(f"{label} is not a SHA-256 identity")
    return normalized


__all__ = [
    "CompleteSnapshotLineagePostgresSource",
    "LineageLiveCase",
    "LineageRunEvidence",
    "SOURCE_BOUNDARY",
    "SOURCE_ROWS",
    "SOURCE_SCHEMA",
    "SOURCE_TABLE",
    "SCOPE_REPLAY_TARGET_TABLE",
    "STRATEGIES",
    "TARGET_SCHEMA",
    "TRANSPORTS",
    "assert_exact_catalog",
    "assert_replace_scope_catalog_binding",
    "drop_source",
    "ensure_source",
    "expected_row_hashes",
    "expected_row_ids",
    "lineage_cases",
    "prove_existing_target_scope_replay",
    "prove_temporal_convert_vendor_surface",
    "receipt_for_target",
    "require_digest",
    "run_lineage_case",
    "semantic_parity_image",
    "target_catalog",
    "target_clock",
    "target_rows",
]
