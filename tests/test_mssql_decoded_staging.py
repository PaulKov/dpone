"""Owned decoded-staging lifecycle and SQL-shape regression tests."""

from __future__ import annotations

import re
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.sinks.strategies.mssql.mssql_decoded_staging import (
    MssqlDecodedStagingCapacityPolicy,
    MssqlDecodedStagingMaterializer,
    cleanup_staging_after_primary,
    mssql_native_phase,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_schema import ResolvedMssqlNativeSchema
from dpone.runtime.sinks.strategies.mssql.mssql_native_staging import MssqlNativeStagingNormalizer


def _config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="source",
        source_table="events",
        target_database="DWH_Dev",
        target_schema="staging",
        target_table="events",
        staging_database="DWH_Dev",
        staging_schema="staging",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )


class _Connector:
    def __init__(
        self,
        *,
        decoded_rows: int = 2,
        raw_reserved_bytes: int = 1_048_576,
        available_data_bytes: int = 1_073_741_824,
    ) -> None:
        self.decoded_rows = decoded_rows
        self.raw_reserved_bytes = raw_reserved_bytes
        self.available_data_bytes = available_data_bytes
        self.capacity_error: Exception | None = None
        self.queries: list[str] = []
        self.authority_tables: dict[str, int] = {}

    @staticmethod
    def quote_identifier(value: str) -> str:
        return f"[{value}]"

    def execute_query(self, query: str, *_args, **_kwargs) -> int:
        self._assert_internal_stage_authority(query)
        self.queries.append(str(query))
        return self.decoded_rows

    def get_records(self, query: str, *_args, **_kwargs):
        self._assert_internal_stage_authority(query)
        self.queries.append(str(query))
        if "AS raw_reserved_bytes" in str(query):
            if self.capacity_error is not None:
                raise self.capacity_error
            return [
                {
                    "raw_reserved_bytes": self.raw_reserved_bytes,
                    "available_data_bytes": self.available_data_bytes,
                }
            ]
        if "AS [invalid_hash]" in str(query):
            return [{"invalid_hash": 0, "too_long": 0, "invalid": 0, "lossy": 0}]
        if "COUNT_BIG(*) AS row_count" in str(query):
            return [(self.decoded_rows,)]
        return []

    def _assert_internal_stage_authority(self, query: str) -> None:
        rendered = str(query)
        for marker in ("_decoded_", "_native"):
            if marker in rendered and not any(
                marker in table and depth > 0 for table, depth in self.authority_tables.items()
            ):
                raise AssertionError(f"missing database authority for {marker}")


class _Staging:
    def __init__(self, connector: _Connector) -> None:
        self.connector = connector
        self.created: list[StagingTableArtifact] = []
        self.dropped: list[StagingTableArtifact] = []
        self.finalized: tuple[StagingTableArtifact, StagingTableArtifact] | None = None
        self.fail_drop = lambda _artifact: False

    @contextmanager
    def database_authority_scope(self, artifact):
        self.connector.authority_tables[artifact.table] = self.connector.authority_tables.get(artifact.table, 0) + 1
        try:
            yield
        finally:
            depth = self.connector.authority_tables[artifact.table] - 1
            if depth:
                self.connector.authority_tables[artifact.table] = depth
            else:
                del self.connector.authority_tables[artifact.table]

    def create(self, config, schema):
        artifact = StagingTableArtifact(
            database=config.staging_database,
            schema=config.staging_schema,
            table=config.staging_table,
            columns=[name for name, _dtype in schema],
            staging_manager=self,
            column_types=dict(schema),
            target_column_types=dict(schema),
        )
        self.created.append(artifact)
        return artifact

    def drop(self, artifact: StagingTableArtifact) -> None:
        self.dropped.append(artifact)
        if self.fail_drop(artifact):
            raise RuntimeError(f"drop failed for {artifact.table}")

    def finalize_native_evidence(self, raw, native, *, columns) -> None:
        assert columns
        self.finalized = (raw, native)


def _fixture(
    *,
    decoded_rows: int = 2,
    raw_reserved_bytes: int = 1_048_576,
    available_data_bytes: int = 1_073_741_824,
):
    connector = _Connector(
        decoded_rows=decoded_rows,
        raw_reserved_bytes=raw_reserved_bytes,
        available_data_bytes=available_data_bytes,
    )
    staging = _Staging(connector)
    strategy = SimpleNamespace(
        connector=connector,
        staging_manager=staging,
        _staging_name=lambda artifact: f"[{artifact.database}].[{artifact.schema}].[{artifact.table}]",
    )
    raw = StagingTableArtifact(
        database="DWH_Dev",
        schema="staging",
        table="stg_events_12345678",
        columns=("name", "payload", "amount", "__dpone__row_hash"),
        staging_manager=staging,
        row_count=2,
        column_types={
            "name": "nvarchar(max)",
            "payload": "nvarchar(max)",
            "amount": "nvarchar(max)",
            "__dpone__row_hash": "char(64)",
        },
        target_column_types={
            "name": "nvarchar(max)",
            "payload": "nvarchar(max)",
            "amount": "nvarchar(max)",
            "__dpone__row_hash": "char(64)",
        },
        bulk_text_codec=BulkTextCodec(),
    )
    return connector, staging, strategy, raw


def test_decoded_stage_materializes_each_text_column_once_and_is_owned() -> None:
    connector, staging, strategy, raw = _fixture()

    with MssqlDecodedStagingMaterializer(strategy).materialize(
        _config(),
        raw,
        error_prefix="mssql_native_projection",
    ) as decoded:
        assert decoded is not raw
        assert decoded.bulk_text_codec is None
        assert decoded.row_count == raw.row_count
        assert decoded.column_types == raw.column_types
        assert staging.dropped == []

    assert staging.dropped == [decoded]
    insert = next(query for query in connector.queries if query.startswith("INSERT INTO"))
    assert " WITH (TABLOCK) " in insert
    assert insert.count("CASE WHEN (r.[name]) COLLATE Latin1_General_100_BIN2") == 1
    assert insert.count("CASE WHEN (r.[payload]) COLLATE Latin1_General_100_BIN2") == 1
    assert insert.count("CASE WHEN (r.[amount]) COLLATE Latin1_General_100_BIN2") == 1
    assert "CASE WHEN r.[__dpone__row_hash]" not in insert
    assert re.search(r"stg_events_12345678_decoded_[0-9a-f]{12}", insert)
    capacity = next(query for query in connector.queries if "AS raw_reserved_bytes" in query)
    assert "sys.dm_db_partition_stats" in capacity
    assert "sys.database_files" in capacity
    assert "INNER JOIN sys.filegroups AS fg ON fg.data_space_id = f.data_space_id" in capacity
    assert "fg.is_default = 1" in capacity
    assert "FILEPROPERTY" in capacity
    assert capacity.startswith("EXEC [DWH_Dev].sys.sp_executesql")
    assert "COUNT_BIG(" not in capacity.upper()


def test_capacity_probe_excludes_free_space_from_non_default_filegroups() -> None:
    policy = MssqlDecodedStagingCapacityPolicy()
    raw_reserved_bytes = 1_048_576
    required = policy.required_data_bytes(raw_reserved_bytes=raw_reserved_bytes, row_count=2)
    connector, staging, strategy, raw = _fixture(
        raw_reserved_bytes=raw_reserved_bytes,
        available_data_bytes=required - 1,
    )
    default_filegroup_free_bytes = required - 1
    unrelated_filegroup_free_bytes = required * 10
    original_get_records = connector.get_records

    def multi_filegroup_capacity(query: str, *_args, **_kwargs):
        if "AS raw_reserved_bytes" not in str(query):
            return original_get_records(query, *_args, **_kwargs)
        connector.queries.append(str(query))
        available = default_filegroup_free_bytes
        if "f.state = 0 AND fg.is_default = 1" not in str(query):
            available += unrelated_filegroup_free_bytes
        return [{"raw_reserved_bytes": raw_reserved_bytes, "available_data_bytes": available}]

    connector.get_records = multi_filegroup_capacity  # type: ignore[method-assign]

    with MssqlDecodedStagingMaterializer(strategy).materialize(
        _config(),
        raw,
        error_prefix="mssql_native_projection",
    ) as decoded:
        assert decoded is raw

    capacity = next(query for query in connector.queries if "AS raw_reserved_bytes" in query)
    assert "INNER JOIN sys.filegroups AS fg ON fg.data_space_id = f.data_space_id" in capacity
    assert "f.state = 0 AND fg.is_default = 1" in capacity
    assert staging.created == []


def test_capacity_policy_accounts_for_decoded_native_and_fixed_headroom() -> None:
    policy = MssqlDecodedStagingCapacityPolicy()

    assert policy.required_data_bytes(raw_reserved_bytes=1_000, row_count=1) == 67_184_592
    assert policy.required_data_bytes(raw_reserved_bytes=1_000, row_count=10_000) == 159_270_864


def test_capacity_policy_reserves_full_pages_and_extent_overhead_for_wide_rows() -> None:
    policy = MssqlDecodedStagingCapacityPolicy()
    raw_reserved_bytes = 64 * 1024
    row_count = 700_000

    required = policy.required_data_bytes(
        raw_reserved_bytes=raw_reserved_bytes,
        row_count=row_count,
    )

    assert required == 6_518_439_936
    unsafe_record_payload_estimate = 2 * raw_reserved_bytes + row_count * 8_060 + 64 * 1024 * 1024
    assert required > unsafe_record_payload_estimate


@pytest.mark.parametrize(
    "policy_kwargs",
    (
        {"native_in_row_reserve_bytes": 8_191},
        {"native_in_row_reserve_bytes": True},
        {"headroom_bytes": 64 * 1024 * 1024 - 1},
        {"headroom_bytes": True},
    ),
)
def test_capacity_policy_rejects_configuration_that_can_understate_peak(
    policy_kwargs: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="mssql_decoded_staging.capacity_policy_invalid"):
        MssqlDecodedStagingCapacityPolicy(**policy_kwargs)  # type: ignore[arg-type]


def test_exact_capacity_boundary_admits_decoded_materialization() -> None:
    policy = MssqlDecodedStagingCapacityPolicy()
    required = policy.required_data_bytes(raw_reserved_bytes=1_048_576, row_count=2)
    _connector, staging, strategy, raw = _fixture(available_data_bytes=required)

    with MssqlDecodedStagingMaterializer(strategy).materialize(
        _config(),
        raw,
        error_prefix="mssql_native_projection",
    ) as decoded:
        assert decoded is not raw

    assert len(staging.created) == 1


def test_one_byte_below_capacity_boundary_uses_inline_decoder_without_extra_stage() -> None:
    policy = MssqlDecodedStagingCapacityPolicy()
    required = policy.required_data_bytes(raw_reserved_bytes=1_048_576, row_count=2)
    connector, staging, strategy, raw = _fixture(available_data_bytes=required - 1)
    resolved = ResolvedMssqlNativeSchema(
        types={
            "name": "nvarchar(128)",
            "payload": "nvarchar(max)",
            "amount": "decimal(18, 2)",
            "__dpone__row_hash": "char(64)",
        },
        source_types={
            "name": "nvarchar(max)",
            "payload": "nvarchar(max)",
            "amount": "decimal(18, 2)",
            "__dpone__row_hash": "char(64)",
        },
        nullability={column: True for column in raw.columns},
        collations={},
        wire_to_target={column: column for column in raw.columns},
    )

    native = MssqlNativeStagingNormalizer(strategy).normalize(
        _config(),
        raw,
        tuple(resolved.source_types.items()),
        resolved,
        lineage=SimpleNamespace(columns=()),
    )

    assert [artifact for artifact in staging.created if "_decoded_" in artifact.table] == []
    assert native in staging.created
    validation_sql = next(query for query in connector.queries if "AS [invalid_hash]" in query)
    native_sql = next(query for query in connector.queries if "_native_" in query and "INSERT INTO" in query)
    assert "NCHAR(29)" in validation_sql
    assert "NCHAR(29)" in native_sql


@pytest.mark.parametrize(
    ("probe_result", "probe_error"),
    [
        ({"raw_reserved_bytes": True, "available_data_bytes": 1_000_000_000}, None),
        ({"raw_reserved_bytes": 1_000, "available_data_bytes": -1}, None),
        ({"raw_reserved_bytes": 1_000}, None),
        (None, RuntimeError("capacity metadata denied")),
    ],
)
def test_untrusted_or_unavailable_capacity_evidence_falls_back_inline(
    probe_result: dict[str, object] | None,
    probe_error: Exception | None,
) -> None:
    connector, staging, strategy, raw = _fixture()
    connector.capacity_error = probe_error
    original_get_records = connector.get_records

    if probe_error is None:

        def capacity_result(query: str, *_args, **_kwargs):
            if "AS raw_reserved_bytes" in str(query):
                connector.queries.append(str(query))
                return [probe_result]
            return original_get_records(query, *_args, **_kwargs)

        connector.get_records = capacity_result  # type: ignore[method-assign]

    with MssqlDecodedStagingMaterializer(strategy).materialize(
        _config(),
        raw,
        error_prefix="mssql_native_projection",
    ) as decoded:
        assert decoded is raw
        assert decoded.bulk_text_codec is not None

    assert staging.created == []


def test_no_codec_reuses_raw_stage_without_create_or_cleanup() -> None:
    _connector, staging, strategy, raw = _fixture()
    raw.bulk_text_codec = None

    with MssqlDecodedStagingMaterializer(strategy).materialize(
        _config(),
        raw,
        error_prefix="mssql_native_projection",
    ) as decoded:
        assert decoded is raw

    assert staging.created == []
    assert staging.dropped == []


def test_decoded_stage_count_mismatch_fails_closed_and_cleans_up() -> None:
    _connector, staging, strategy, raw = _fixture(decoded_rows=1)

    with pytest.raises(
        SnapshotReconciliationError,
        match="mssql_native_projection.decoded_staging_count_mismatch",
    ):
        with MssqlDecodedStagingMaterializer(strategy).materialize(
            _config(),
            raw,
            error_prefix="mssql_native_projection",
        ):
            raise AssertionError("mismatched stage must not be yielded")

    assert len(staging.created) == 1
    assert staging.dropped == staging.created


def test_decoded_stage_population_failure_cleans_up() -> None:
    connector, staging, strategy, raw = _fixture()

    def fail(_query: str, *_args, **_kwargs) -> int:
        raise RuntimeError("decode failed")

    connector.execute_query = fail  # type: ignore[method-assign]

    with pytest.raises(RuntimeError, match="decode failed"):
        with MssqlDecodedStagingMaterializer(strategy).materialize(
            _config(),
            raw,
            error_prefix="mssql_native_projection",
        ):
            raise AssertionError("failed stage must not be yielded")

    assert len(staging.created) == 1
    assert staging.dropped == staging.created


@pytest.mark.parametrize("invalid_count", [True, -1, 1.0, None])
def test_invalid_raw_count_fails_before_decoded_stage_creation(invalid_count: object) -> None:
    _connector, staging, strategy, raw = _fixture()
    raw.row_count = invalid_count  # type: ignore[assignment]

    with pytest.raises(
        SnapshotReconciliationError,
        match="mssql_native_projection.decoded_staging_raw_count_invalid",
    ):
        with MssqlDecodedStagingMaterializer(strategy).materialize(
            _config(),
            raw,
            error_prefix="mssql_native_projection",
        ):
            raise AssertionError("invalid receipt must not be materialized")

    assert staging.created == []


@pytest.mark.parametrize("invalid_count", [True, -1, 1.0, None])
def test_invalid_raw_count_without_codec_fails_before_native_work(invalid_count: object) -> None:
    connector, staging, strategy, raw = _fixture()
    raw.bulk_text_codec = None
    raw.row_count = invalid_count  # type: ignore[assignment]

    with pytest.raises(
        SnapshotReconciliationError,
        match="mssql_native_projection.decoded_staging_raw_count_invalid",
    ):
        with MssqlDecodedStagingMaterializer(strategy).materialize(
            _config(),
            raw,
            error_prefix="mssql_native_projection",
        ):
            raise AssertionError("invalid receipt must not be reused")

    assert staging.created == []
    assert connector.queries == []


def test_generic_normalizer_validates_and_projects_from_materialized_decode() -> None:
    connector, staging, strategy, raw = _fixture()
    resolved = ResolvedMssqlNativeSchema(
        types={
            "name": "nvarchar(128)",
            "payload": "nvarchar(max)",
            "amount": "decimal(18, 2)",
            "__dpone__row_hash": "char(64)",
        },
        source_types={
            "name": "nvarchar(max)",
            "payload": "nvarchar(max)",
            "amount": "decimal(18, 2)",
            "__dpone__row_hash": "char(64)",
        },
        nullability={
            "name": True,
            "payload": True,
            "amount": True,
            "__dpone__row_hash": False,
        },
        collations={},
        wire_to_target={column: column for column in raw.columns},
    )

    native = MssqlNativeStagingNormalizer(strategy).normalize(
        _config(),
        raw,
        tuple(resolved.source_types.items()),
        resolved,
        lineage=SimpleNamespace(columns=()),
    )

    decoded_table = next(artifact.table for artifact in staging.created if "_decoded_" in artifact.table)
    decode_sql = next(query for query in connector.queries if "_decoded_" in query and "INSERT INTO" in query)
    validation_sql = next(query for query in connector.queries if "AS [invalid_hash]" in query)
    native_sql = next(query for query in connector.queries if "_native_" in query and "INSERT INTO" in query)
    assert "NCHAR(29)" in decode_sql
    assert "NCHAR(29)" not in validation_sql
    assert "NCHAR(29)" not in native_sql
    assert f"[{decoded_table}] AS r" in validation_sql
    assert f"[{decoded_table}] AS r" in native_sql
    assert " WITH (TABLOCK) " in native_sql
    assert staging.finalized == (raw, native)
    assert [artifact.table for artifact in staging.dropped] == [decoded_table]


def test_conversion_failure_cleans_decode_before_native_stage_exists() -> None:
    connector, staging, strategy, raw = _fixture()
    original_get_records = connector.get_records

    def fail_validation(query: str, *_args, **_kwargs):
        if "AS [invalid_hash]" in str(query):
            connector.queries.append(str(query))
            return [{"invalid_hash": 0, "too_long": 0, "invalid": 0, "lossy": 1}]
        return original_get_records(query, *_args, **_kwargs)

    connector.get_records = fail_validation  # type: ignore[method-assign]
    resolved = ResolvedMssqlNativeSchema(
        types={column: "nvarchar(max)" for column in raw.columns},
        source_types={column: "nvarchar(max)" for column in raw.columns},
        nullability={column: True for column in raw.columns},
        collations={},
        wire_to_target={column: column for column in raw.columns},
    )

    with pytest.raises(SnapshotReconciliationError, match="mssql_native_projection.value_lossy"):
        MssqlNativeStagingNormalizer(strategy).normalize(
            _config(),
            raw,
            tuple(resolved.source_types.items()),
            resolved,
            lineage=SimpleNamespace(columns=()),
        )

    assert len(staging.created) == 1
    assert re.fullmatch(r"stg_events_12345678_decoded_[0-9a-f]{12}", staging.created[0].table)
    assert staging.dropped == staging.created
    assert staging.finalized is None


def test_derived_decoded_names_preserve_concurrent_raw_identity_at_length_boundary() -> None:
    _connector, staging, strategy, raw = _fixture()
    first = "same_prefix_" + "x" * 110 + "_first"
    second = "same_prefix_" + "x" * 110 + "_second"
    decoded_names: list[str] = []

    for raw_name in (first, second):
        raw.table = raw_name
        with MssqlDecodedStagingMaterializer(strategy).materialize(
            _config(),
            raw,
            error_prefix="mssql_native_projection",
        ) as decoded:
            decoded_names.append(decoded.table)

    assert decoded_names[0] != decoded_names[1]
    assert all(len(name) <= 120 for name in decoded_names)
    assert all(re.search(r"_decoded_[0-9a-f]{12}$", name) for name in decoded_names)


def test_conversion_error_remains_authoritative_when_decoded_cleanup_fails() -> None:
    connector, staging, strategy, raw = _fixture()
    original_get_records = connector.get_records

    def fail_validation(query: str, *_args, **_kwargs):
        if "AS [invalid_hash]" in str(query):
            connector.queries.append(str(query))
            return [{"invalid_hash": 0, "too_long": 0, "invalid": 0, "lossy": 1}]
        return original_get_records(query, *_args, **_kwargs)

    connector.get_records = fail_validation  # type: ignore[method-assign]
    staging.fail_drop = lambda artifact: "_decoded_" in artifact.table
    resolved = ResolvedMssqlNativeSchema(
        types={column: "nvarchar(max)" for column in raw.columns},
        source_types={column: "nvarchar(max)" for column in raw.columns},
        nullability={column: True for column in raw.columns},
        collations={},
        wire_to_target={column: column for column in raw.columns},
    )

    with pytest.raises(SnapshotReconciliationError, match="mssql_native_projection.value_lossy"):
        MssqlNativeStagingNormalizer(strategy).normalize(
            _config(),
            raw,
            tuple(resolved.source_types.items()),
            resolved,
            lineage=SimpleNamespace(columns=()),
        )

    assert len(staging.dropped) == 3
    assert all("_decoded_" in artifact.table for artifact in staging.dropped)


def test_decoded_cleanup_failure_after_projection_cleans_native_and_fails_closed() -> None:
    _connector, staging, strategy, raw = _fixture()
    staging.fail_drop = lambda artifact: "_decoded_" in artifact.table
    resolved = ResolvedMssqlNativeSchema(
        types={column: "nvarchar(max)" for column in raw.columns},
        source_types={column: "nvarchar(max)" for column in raw.columns},
        nullability={column: True for column in raw.columns},
        collations={},
        wire_to_target={column: column for column in raw.columns},
    )

    with pytest.raises(RuntimeError, match="drop failed"):
        MssqlNativeStagingNormalizer(strategy).normalize(
            _config(),
            raw,
            tuple(resolved.source_types.items()),
            resolved,
            lineage=SimpleNamespace(columns=()),
        )

    assert len(staging.created) == 2
    assert staging.dropped[:3] == [staging.created[0]] * 3
    assert staging.dropped[3:] == [staging.created[1]]
    assert "_decoded_" in staging.dropped[0].table
    assert re.search(r"_native_[0-9a-f]{12}$", staging.dropped[-1].table)


def test_success_path_cleanup_retries_a_transient_drop_failure() -> None:
    _connector, staging, strategy, raw = _fixture()
    failures_remaining = 1

    def fail_once(_artifact: StagingTableArtifact) -> bool:
        nonlocal failures_remaining
        if failures_remaining:
            failures_remaining -= 1
            return True
        return False

    staging.fail_drop = fail_once

    with MssqlDecodedStagingMaterializer(strategy).materialize(
        _config(),
        raw,
        error_prefix="mssql_native_projection",
    ):
        pass

    assert len(staging.dropped) == 2
    assert staging.dropped[0] is staging.dropped[1]


def test_primary_failure_cleanup_is_bounded_and_logs_exact_residue() -> None:
    messages: list[str] = []
    strategy = SimpleNamespace(
        logger=SimpleNamespace(warning=messages.append),
    )
    attempts = 0

    def fail_cleanup() -> None:
        nonlocal attempts
        attempts += 1
        raise RuntimeError("locked")

    artifact = SimpleNamespace(
        database="DWH_Dev",
        schema="staging",
        table="stg_events_decoded_deadbeef1234",
        cleanup=fail_cleanup,
    )

    cleanup_staging_after_primary(strategy, artifact, phase="decoded_cleanup")

    assert attempts == 3
    assert len(messages) == 1
    assert "status=residue_retained" in messages[0]
    assert "attempts=3" in messages[0]
    assert "table=[DWH_Dev].[staging].[stg_events_decoded_deadbeef1234]" in messages[0]


def test_generic_native_names_preserve_concurrent_raw_identity_at_length_boundary() -> None:
    _connector, staging, strategy, raw = _fixture()
    resolved = ResolvedMssqlNativeSchema(
        types={column: "nvarchar(max)" for column in raw.columns},
        source_types={column: "nvarchar(max)" for column in raw.columns},
        nullability={column: True for column in raw.columns},
        collations={},
        wire_to_target={column: column for column in raw.columns},
    )
    native_names: list[str] = []

    for raw_name in ("x" * 115 + "_first", "x" * 115 + "_second"):
        raw.table = raw_name
        native = MssqlNativeStagingNormalizer(strategy).normalize(
            _config(),
            raw,
            tuple(resolved.source_types.items()),
            resolved,
            lineage=SimpleNamespace(columns=()),
        )
        native_names.append(native.table)
        native.cleanup()

    assert native_names[0] != native_names[1]
    assert all(len(name) <= 120 for name in native_names)
    assert all(re.search(r"_native_[0-9a-f]{12}$", name) for name in native_names)


def test_phase_logs_have_stable_single_message_success_and_failure_contract() -> None:
    class Logger:
        def __init__(self) -> None:
            self.messages: list[tuple[str, str]] = []

        def info(self, message: str) -> None:
            self.messages.append(("info", message))

        def warning(self, message: str) -> None:
            self.messages.append(("warning", message))

    logger = Logger()
    strategy = SimpleNamespace(logger=logger)

    with mssql_native_phase(strategy, "decode_materialize", rows=7):
        pass
    with pytest.raises(ValueError, match="primary"):
        with mssql_native_phase(strategy, "conversion_validate", rows=7):
            raise ValueError("primary")

    assert len(logger.messages) == 2
    assert re.fullmatch(
        r"MSSQL native phase: phase=decode_materialize status=success rows=7 duration_seconds=\d+\.\d{6}",
        logger.messages[0][1],
    )
    assert re.fullmatch(
        r"MSSQL native phase: phase=conversion_validate status=failed rows=7 duration_seconds=\d+\.\d{6}",
        logger.messages[1][1],
    )


def test_best_effort_phase_logging_and_secondary_cleanup_cannot_change_outcome() -> None:
    class DiagnosticAbort(BaseException):
        pass

    class FailingLogger:
        @staticmethod
        def info(_message: str) -> None:
            raise DiagnosticAbort("telemetry")

        @staticmethod
        def warning(_message: str) -> None:
            raise DiagnosticAbort("telemetry")

    strategy = SimpleNamespace(logger=FailingLogger())

    with mssql_native_phase(strategy, "native_project", rows=1):
        pass

    artifact = SimpleNamespace(
        cleanup=lambda: (_ for _ in ()).throw(DiagnosticAbort("cleanup")),
    )
    cleanup_staging_after_primary(strategy, artifact, phase="decoded_cleanup")
