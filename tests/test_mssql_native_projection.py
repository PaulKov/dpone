"""Shared generic/snapshot SQL Server native-projection contracts."""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.contracts.mssql_physical_design import (
    MssqlIndexKeyContractError,
    require_mssql_index_key_width,
)
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.extraction_lifecycle import ExtractionLifecycleReceipt
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.sinks.mssql_target_catalog_model import MssqlSchemaCatalogSnapshot
from dpone.runtime.sinks.strategies.mssql.mssql_concrete_load_strategies import (
    MSSQLIncrementMergeStrategy,
)
from dpone.runtime.sinks.strategies.mssql.mssql_generic_target_contract import (
    MssqlGenericTargetContract,
    render_unique_authority_ddl,
    resolve_unique_authority_contract,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_lineage import (
    MssqlNativeLineageProjection,
    resolve_mssql_native_lineage_columns,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_staging import MssqlNativeStagingNormalizer
from dpone.runtime.support.mssql_native_projection import (
    bounded_binary_overflow,
    bounded_character_overflow,
    floating_point_text_loss,
    lossless_roundtrip_mismatch,
    validate_native_conversions,
)
from dpone.runtime.support.postgres_mssql_projection import (
    RetainedMssqlTargetColumn,
    project_postgres_mssql_relation,
)
from dpone.type_system.source_sink.provenance import SourceColumnProvenance, SourceRelationDialect


def _load_config(**options):
    return LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="source_table",
        target_database="target_database",
        target_schema="dbo",
        target_table="target_table",
        load_strategy=LoadStrategy.INCREMENTAL_MERGE,
        unique_key=["id"],
        options=options,
    )


def _normalizer() -> MssqlNativeStagingNormalizer:
    return MssqlNativeStagingNormalizer(SimpleNamespace(connector=SimpleNamespace(), staging_manager=SimpleNamespace()))


def test_ansi_guard_compares_same_unicode_semantics_for_raw_char_wire() -> None:
    predicate = bounded_character_overflow("char(64)", "r.[__dpone__delta_hash]")

    assert predicate is not None
    assert "DATALENGTH(CONVERT(varchar(max), r.[__dpone__delta_hash])) > 64" in predicate
    assert "CONVERT(nvarchar(max), r.[__dpone__delta_hash])" in predicate
    assert (
        "CONVERT(varbinary(max), CONVERT(nvarchar(max), CONVERT(varchar(max), r.[__dpone__delta_hash])))"
    ) in predicate


def test_binary_roundtrip_keeps_hex_style_and_byte_identity() -> None:
    predicate = lossless_roundtrip_mismatch("varbinary(max)", "binary(16)", "r.[payload]")

    assert "TRY_CONVERT(varbinary(max), r.[payload], 2)" in predicate
    assert "TRY_CONVERT(binary(16), r.[payload], 2)" in predicate
    assert predicate.count("CONVERT(varbinary(max)") >= 2


def test_binary_overflow_measures_decoded_bytes_not_hex_wire_characters() -> None:
    predicate = bounded_binary_overflow("varbinary(1)", "r.[payload]")

    assert predicate == "DATALENGTH(TRY_CONVERT(varbinary(max), r.[payload], 2)) > 1"


def test_float_text_guard_rejects_subnormal_underflow_and_signed_zero() -> None:
    predicate = floating_point_text_loss("float", "float", "r.[value]")

    assert predicate is not None
    assert "TRY_CONVERT(float, r.[value]) = CONVERT(float, 0)" in predicate
    assert "LIKE N'%[1-9]%'" in predicate
    assert "LEFT(LOWER(LTRIM(RTRIM(CONVERT(nvarchar(max), r.[value])))), 1) = N'-'" in predicate
    assert floating_point_text_loss("decimal(18,4)", "decimal(18,4)", "r.[value]") is None


def test_internal_hash_guard_is_exact_hex_and_bypasses_text_codec() -> None:
    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        get_records=lambda query, **_kwargs: (
            queries.append(str(query)) or [{"invalid_hash": 0, "too_long": 0, "invalid": 0, "lossy": 0}]
        ),
    )
    queries: list[str] = []
    strategy = SimpleNamespace(
        connector=connector,
        _staging_name=lambda _artifact: "[staging].[raw]",
        _staging_select_expression=lambda artifact, column, alias: f"{alias}.[{column}]",
    )
    raw = StagingTableArtifact(
        schema="staging",
        table="raw",
        columns=["__dpone__row_hash"],
        staging_manager=SimpleNamespace(),
        column_types={"__dpone__row_hash": "nvarchar(max)"},
        target_column_types={"__dpone__row_hash": "char(64)"},
        bulk_text_codec=BulkTextCodec(),
    )

    validate_native_conversions(
        strategy,
        raw,
        [("__dpone__row_hash", "char(64)")],
        {"__dpone__row_hash": "char(64)"},
        error_prefix="mssql_native_projection",
    )

    rendered = "\n".join(queries)
    assert "DATALENGTH(CONVERT(varchar(max), r.[__dpone__row_hash])) <> 64" in rendered
    assert "[^0123456789abcdefABCDEF]" in rendered
    assert "CASE WHEN" in rendered
    assert len(queries) == 1


def test_domain_safe_projection_validates_wire_once_without_redundant_roundtrips() -> None:
    queries: list[str] = []
    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        get_records=lambda query, **_kwargs: (
            queries.append(str(query)) or [{"invalid_hash": 0, "too_long": 0, "invalid": 0, "lossy": 0}]
        ),
    )
    strategy = SimpleNamespace(
        connector=connector,
        _staging_name=lambda _artifact: "[staging].[decoded]",
    )
    raw = StagingTableArtifact(
        schema="staging",
        table="decoded",
        columns=["event_date", "sequence", "comment"],
        staging_manager=SimpleNamespace(),
        column_types={
            "event_date": "nvarchar(max)",
            "sequence": "nvarchar(max)",
            "comment": "nvarchar(max)",
        },
        target_column_types={
            "event_date": "date",
            "sequence": "bigint",
            "comment": "nvarchar(max)",
        },
    )

    validate_native_conversions(
        strategy,
        raw,
        [("event_date", "date"), ("sequence", "int"), ("comment", "nvarchar(max)")],
        {"event_date": "date", "sequence": "bigint", "comment": "nvarchar(max)"},
        error_prefix="mssql_native_projection",
    )

    rendered = queries[0]
    assert rendered.count("TRY_CONVERT(date, r.[event_date])") == 1
    assert rendered.count("TRY_CONVERT(int, r.[sequence])") == 1
    assert "TRY_CONVERT(bigint, r.[sequence])" not in rendered
    assert "TRY_CONVERT(nvarchar(max), r.[comment])" not in rendered
    assert "CONVERT(varbinary(max), r.[comment])" not in rendered


def test_value_guarded_text_narrowing_keeps_full_batch_losslessness_proof() -> None:
    queries: list[str] = []
    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        get_records=lambda query, **_kwargs: (
            queries.append(str(query)) or [{"invalid_hash": 0, "too_long": 0, "invalid": 0, "lossy": 0}]
        ),
    )
    strategy = SimpleNamespace(
        connector=connector,
        _staging_name=lambda _artifact: "[staging].[decoded]",
    )
    raw = StagingTableArtifact(
        schema="staging",
        table="decoded",
        columns=["guid"],
        staging_manager=SimpleNamespace(),
        column_types={"guid": "nvarchar(max)"},
        target_column_types={"guid": "nvarchar(36)"},
    )

    validate_native_conversions(
        strategy,
        raw,
        [("guid", "nvarchar(max)")],
        {"guid": "nvarchar(36)"},
        error_prefix="mssql_native_projection",
    )

    rendered = queries[0]
    assert "DATALENGTH(r.[guid]) > 72" in rendered
    assert "TRY_CONVERT(nvarchar(36), r.[guid])" in rendered
    assert "TRY_CONVERT(nvarchar(max), r.[guid])" in rendered
    assert "CONVERT(varbinary(max)" in rendered


def test_domain_safe_binary_projection_still_validates_hex_wire() -> None:
    queries: list[str] = []
    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        get_records=lambda query, **_kwargs: (
            queries.append(str(query)) or [{"invalid_hash": 0, "too_long": 0, "invalid": 0, "lossy": 0}]
        ),
    )
    strategy = SimpleNamespace(
        connector=connector,
        _staging_name=lambda _artifact: "[staging].[decoded]",
    )
    raw = StagingTableArtifact(
        schema="staging",
        table="decoded",
        columns=["payload"],
        staging_manager=SimpleNamespace(),
        column_types={"payload": "nvarchar(max)"},
        target_column_types={"payload": "varbinary(max)"},
    )

    validate_native_conversions(
        strategy,
        raw,
        [("payload", "varbinary(max)")],
        {"payload": "varbinary(max)"},
        error_prefix="mssql_native_projection",
    )

    assert "TRY_CONVERT(varbinary(max), r.[payload], 2)" in queries[0]


def test_numeric_narrowing_cannot_be_misclassified_as_value_guarded() -> None:
    queries: list[str] = []
    strategy = SimpleNamespace(
        connector=SimpleNamespace(
            quote_identifier=lambda value: f"[{value}]",
            get_records=lambda query, **_kwargs: queries.append(str(query)) or [],
        ),
        _staging_name=lambda _artifact: "[staging].[decoded]",
    )
    raw = StagingTableArtifact(
        schema="staging",
        table="decoded",
        columns=["amount"],
        staging_manager=SimpleNamespace(),
        column_types={"amount": "nvarchar(max)"},
        target_column_types={"amount": "decimal(10,2)"},
    )

    with pytest.raises(
        SnapshotReconciliationError,
        match="mssql_native_projection.lossy_target_type_forbidden",
    ):
        validate_native_conversions(
            strategy,
            raw,
            [("amount", "decimal(20,2)")],
            {"amount": "decimal(10,2)"},
            error_prefix="mssql_native_projection",
        )

    assert queries == []


@pytest.mark.parametrize(
    ("flags", "expected"),
    (
        ({"invalid_hash": 1, "too_long": 1, "invalid": 1, "lossy": 1}, "internal_hash_invalid"),
        ({"invalid_hash": 0, "too_long": 1, "invalid": 1, "lossy": 1}, "value_too_long"),
        ({"invalid_hash": 0, "too_long": 0, "invalid": 1, "lossy": 1}, "value_invalid"),
        ({"invalid_hash": 0, "too_long": 0, "invalid": 0, "lossy": 1}, "value_lossy"),
    ),
)
def test_native_conversion_scan_preserves_failure_precedence_in_one_query(flags, expected) -> None:
    queries: list[str] = []
    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        get_records=lambda query, **_kwargs: queries.append(str(query)) or [flags],
    )
    strategy = SimpleNamespace(
        connector=connector,
        _staging_name=lambda _artifact: "[staging].[raw]",
    )
    raw = StagingTableArtifact(
        schema="staging",
        table="raw",
        columns=["value"],
        staging_manager=SimpleNamespace(),
        column_types={"value": "nvarchar(max)"},
        target_column_types={"value": "nvarchar(20)"},
    )

    with pytest.raises(SnapshotReconciliationError, match=expected):
        validate_native_conversions(
            strategy,
            raw,
            [("value", "nvarchar(max)")],
            {"value": "nvarchar(20)"},
            error_prefix="mssql_native_projection",
        )

    assert len(queries) == 1


def test_native_conversion_evidence_failure_preserves_caller_prefix() -> None:
    strategy = SimpleNamespace(
        connector=SimpleNamespace(
            quote_identifier=lambda value: f"[{value}]",
            get_records=lambda *_args, **_kwargs: [],
        ),
        _staging_name=lambda _artifact: "[staging].[raw]",
    )
    raw = StagingTableArtifact(
        schema="staging",
        table="raw",
        columns=["value"],
        staging_manager=SimpleNamespace(),
        column_types={"value": "nvarchar(max)"},
        target_column_types={"value": "nvarchar(20)"},
    )

    with pytest.raises(SnapshotReconciliationError, match="mssql_snapshot_projection.conversion_evidence_unavailable"):
        validate_native_conversions(
            strategy,
            raw,
            [("value", "nvarchar(max)")],
            {"value": "nvarchar(20)"},
            error_prefix="mssql_snapshot_projection",
        )


def test_mssql_relation_uses_native_types_and_observed_nullability() -> None:
    resolved = _normalizer().resolve_schema(
        _load_config(),
        [("id", "bigint"), ("name", "nvarchar(50) nullable")],
        relation_schema=[("id", "bigint"), ("name", "nvarchar(50) nullable")],
        relation_metadata=None,
        relation_dialect=SourceRelationDialect.MSSQL,
        target_projection=None,
    )

    assert resolved.types == {"id": "bigint", "name": "nvarchar(50)"}
    assert resolved.source_types == resolved.types
    assert resolved.nullability == {"id": False, "name": True}


def test_generic_contract_nullability_is_shared_by_native_and_target_shape() -> None:
    resolved = _normalizer().resolve_schema(
        _load_config(schema_contract={"columns": {"name": {"nullable": False}}}),
        [("id", "bigint"), ("name", "nvarchar(50)")],
        relation_schema=None,
        relation_metadata=None,
        relation_dialect=None,
        target_projection=None,
    )

    assert resolved.nullability == {"id": False, "name": False}


def test_native_lineage_shape_is_pure_and_shared_with_runtime_values() -> None:
    config = _load_config(
        lineage={"preset": "bulk_standard"},
        __dpone_load_identity={"run_id": "R" * 26, "load_id": "L" * 26},
        source_type="postgres",
    )
    shape = resolve_mssql_native_lineage_columns(config)
    receipt = ExtractionLifecycleReceipt(
        extraction_started_at=datetime(2026, 8, 15, 9, 30, tzinfo=UTC),
        extraction_completed_at=datetime(2026, 8, 15, 9, 31, tzinfo=UTC),
        clock_authority="dpone.test.utc",
    )

    projection = MssqlNativeLineageProjection.resolve(config, receipt)

    assert shape == (
        ("__dpone__run_id", "varchar(26)", False, None, "run_id"),
        ("__dpone__load_id", "varchar(26)", False, None, "load_id"),
        ("__dpone__loaded_at", "datetime2(7)", False, None, "loaded_at"),
        ("__dpone__extracted_at", "datetime2(7)", False, None, "extracted_at"),
    )
    assert tuple((column.name, column.target_type, column.nullable) for column in projection.columns) == tuple(
        (name, dtype, nullable) for name, dtype, nullable, _collation, _role in shape
    )
    assignments = projection.assignments(
        quote_identifier=lambda value: f"[{value}]",
        business_schema=(("id", "int"), ("name", "nvarchar(20)")),
        wire_to_target={"id": "id", "name": "name"},
        alias="n",
    )
    rendered = " ".join(assignments)
    assert "[__dpone__run_id] = N'RRRR" in rendered
    assert "[__dpone__load_id] = N'LLLL" in rendered
    assert "[__dpone__loaded_at] = CONVERT(datetime2(7)" in rendered
    assert "[__dpone__extracted_at] = CONVERT(datetime2(7)" in rendered


def test_native_lineage_extends_target_shape_once_and_binds_row_identity() -> None:
    config = _load_config(
        lineage=True,
        __dpone_load_identity={"run_id": "R" * 26, "load_id": "L" * 26},
        source_type="postgres",
    )
    base = _normalizer().resolve_schema(
        config,
        [("id", "int"), ("name", "nvarchar(20)")],
        relation_schema=None,
        relation_metadata=None,
        relation_dialect=None,
        target_projection=None,
    )
    receipt = ExtractionLifecycleReceipt(
        extraction_started_at=datetime(2026, 8, 15, 9, 30, tzinfo=UTC),
        extraction_completed_at=datetime(2026, 8, 15, 9, 31, tzinfo=UTC),
        clock_authority="dpone.test.utc",
    )
    lineage = MssqlNativeLineageProjection.resolve(config, receipt)

    resolved = base.with_lineage(lineage)

    assert resolved.target_schema == (
        ("id", "int"),
        ("name", "nvarchar(20)"),
        ("__dpone__load_id", "varchar(26)"),
        ("__dpone__loaded_at", "datetime2(7)"),
        ("__dpone__row_id", "varchar(64)"),
        ("__dpone__extracted_at", "datetime2(7)"),
    )
    assignments = lineage.assignments(
        quote_identifier=lambda value: f"[{value}]",
        business_schema=(("id", "int"), ("name", "nvarchar(20)")),
        wire_to_target=resolved.wire_to_target,
        alias="n",
    )
    assert "HASHBYTES('SHA2_256'" in " ".join(assignments)


def test_postgres_projection_binds_wire_positions_to_distinct_target_names() -> None:
    config = _load_config()
    relation = (("id", "integer"), ("name", "character varying(20)"))
    metadata = (
        SourceColumnProvenance(name="id", declared_type="integer", nullable=False),
        SourceColumnProvenance(
            name="name",
            declared_type="character varying(20)",
            nullable=True,
        ),
    )
    projection = project_postgres_mssql_relation(
        relation,
        config,
        relation_metadata=metadata,
    ).project_target_names(
        {"name": "__dpone__nc__name"},
        retained_catalog=(("name", "int", True, None),),
    )

    resolved = _normalizer().resolve_schema(
        config,
        projection.projected_schema,
        relation_schema=relation,
        relation_metadata=metadata,
        relation_dialect=SourceRelationDialect.POSTGRES,
        target_projection=projection,
    )

    assert resolved.wire_to_target == {
        "id": "id",
        "name": "__dpone__nc__name",
    }
    assert resolved.source_types == {"id": "int", "name": "nvarchar(40)"}
    assert resolved.target_schema == (
        ("id", "int"),
        ("__dpone__nc__name", "nvarchar(40)"),
    )
    assert projection.retained_target_columns[0].name == "name"
    assert projection.retained_target_columns[0].target_type == "int"


def test_fresh_target_ddl_uses_native_target_names_not_payload_wire_names() -> None:
    queries: list[str] = []
    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        execute_query=lambda query, *_args, **_kwargs: queries.append(str(query)) or 0,
    )
    strategy = MSSQLIncrementMergeStrategy(
        connector,
        SimpleNamespace(),
        SimpleNamespace(),
    )
    staging = StagingTableArtifact(
        schema="staging",
        table="native",
        columns=["id", "__dpone__nc__name"],
        staging_manager=SimpleNamespace(),
        target_column_types={"id": "int", "__dpone__nc__name": "nvarchar(40)"},
        target_column_nullability={"id": False, "__dpone__nc__name": True},
    )

    strategy._execute_create_table(  # noqa: SLF001
        _load_config(),
        [("id", "int"), ("name", "nvarchar(40)")],
        staging=staging,
    )

    ddl = "\n".join(queries)
    assert "[__dpone__nc__name] nvarchar(40) NULL" in ddl
    assert "[name]" not in ddl


def test_observed_relation_without_typed_dialect_fails_closed() -> None:
    with pytest.raises(SnapshotReconciliationError) as raised:
        _normalizer().resolve_schema(
            _load_config(),
            [("id", "bigint")],
            relation_schema=[("id", "bigint")],
            relation_metadata=None,
            relation_dialect=None,
            target_projection=None,
        )

    assert raised.value.code == "mssql_native_projection.source_relation_dialect_required"


def _target_staging() -> StagingTableArtifact:
    return StagingTableArtifact(
        schema="staging",
        table="native",
        columns=["id", "value"],
        staging_manager=SimpleNamespace(),
        target_column_types={"id": "int", "value": "nvarchar(20)"},
        target_column_nullability={"id": False, "value": True},
    )


def _target_strategy(catalog_rows, index_rows=()):
    def records(query, *_args, **_kwargs):
        if "sys.indexes AS i" in str(query):
            return list(index_rows)
        if "sys.columns AS c" in str(query):
            return list(catalog_rows)
        if "FROM sys.tables AS t" in str(query):
            return [
                {
                    "temporal_type": 0,
                    "ledger_type": 0,
                    "is_memory_optimized": 0,
                    "is_filetable": 0,
                    "is_node": 0,
                    "is_edge": 0,
                }
            ]
        return []

    connector = SimpleNamespace(
        quote_identifier=lambda value: f"[{value}]",
        get_records=records,
    )
    return SimpleNamespace(
        connector=connector,
        get_target_catalog_snapshot=lambda _config: MssqlSchemaCatalogSnapshot(
            True,
            "Latin1_General_100_BIN2",
        ),
        _table_exists=lambda _config: True,
        _target_name=lambda _config: "[target_database].[dbo].[target_table]",
        _staging_name=lambda _staging: "[staging].[native]",
    )


def _catalog(name: str, dtype: str, *, nullable: bool):
    base, _, width = dtype.partition("(")
    character = base in {"nvarchar", "nchar", "varchar", "char"}
    declared_width = int(width.removesuffix(")")) if character else 0
    return {
        "column_name": name,
        "type_name": base,
        "max_length": declared_width * 2 if base in {"nvarchar", "nchar"} else declared_width if character else 4,
        "precision": 0 if character else 10,
        "scale": 0,
        "is_nullable": nullable,
        "is_computed": False,
        "collation_name": None,
    }


def _framework_unique_authority(**overrides):
    row = {
        "index_id": 1,
        "index_name": "ux_dpone_target_table_id",
        "type_desc": "NONCLUSTERED",
        "is_primary_key": False,
        "is_unique_constraint": False,
        "is_disabled": False,
        "is_hypothetical": False,
        "filter_definition": None,
        "ignore_dup_key": False,
        "data_space_type_desc": "ROWS_FILEGROUP",
        "column_name": "id",
        "key_ordinal": 1,
        "is_descending_key": False,
        "partition_count": 1,
        "minimum_compression": "NONE",
        "maximum_compression": "NONE",
    }
    row.update(overrides)
    return row


def test_target_contract_accepts_explicit_legacy_nullable_superset() -> None:
    strategy = _target_strategy(
        [_catalog("id", "int", nullable=True), _catalog("value", "nvarchar(20)", nullable=True)],
        [_framework_unique_authority()],
    )

    MssqlGenericTargetContract(strategy).validate_existing(
        _load_config(schema_evolution={"target_nullability": "accept_existing_nullable"}),
        _target_staging(),
    )


def test_case_variant_target_column_cannot_satisfy_exact_contract() -> None:
    strategy = _target_strategy(
        [_catalog("ID", "int", nullable=False), _catalog("value", "nvarchar(20)", nullable=True)]
    )

    with pytest.raises(SnapshotReconciliationError) as raised:
        MssqlGenericTargetContract(strategy).validate_existing(_load_config(), _target_staging())

    assert raised.value.code == "mssql_native_projection.target_shape_invalid"


def test_partition_date_target_type_mismatch_is_rejected_before_dml() -> None:
    staging = StagingTableArtifact(
        schema="staging",
        table="native",
        columns=["business_date"],
        staging_manager=SimpleNamespace(),
        target_column_types={"business_date": "date"},
        target_column_nullability={"business_date": False},
    )
    strategy = _target_strategy([_catalog("business_date", "datetime2", nullable=False)])

    with pytest.raises(SnapshotReconciliationError) as raised:
        MssqlGenericTargetContract(strategy).validate_existing(_load_config(), staging)

    assert raised.value.code == "mssql_native_projection.target_shape_invalid:type:business_date"


def test_casefold_equivalent_target_columns_are_rejected_explicitly() -> None:
    strategy = _target_strategy(
        [
            _catalog("id", "int", nullable=False),
            _catalog("ID", "int", nullable=False),
            _catalog("value", "nvarchar(20)", nullable=True),
        ]
    )

    with pytest.raises(SnapshotReconciliationError) as raised:
        MssqlGenericTargetContract(strategy).validate_existing(_load_config(), _target_staging())

    assert raised.value.code == "mssql_native_projection.target_column_casefold_collision"


def test_fixed_framework_char_target_is_compatible_with_native_varchar_projection() -> None:
    staging = StagingTableArtifact(
        schema="staging",
        table="native",
        columns=["id", "__dpone__run_id"],
        staging_manager=SimpleNamespace(),
        target_column_types={"id": "int", "__dpone__run_id": "varchar(26)"},
        target_column_nullability={"id": False, "__dpone__run_id": False},
    )
    catalog = [
        _catalog("id", "int", nullable=False),
        _catalog("__dpone__run_id", "char(26)", nullable=False),
    ]

    MssqlGenericTargetContract(_target_strategy(catalog, [_framework_unique_authority()])).validate_existing(
        _load_config(),
        staging,
    )


def test_filtered_unique_index_is_not_ordinary_key_authority() -> None:
    catalog = [_catalog("id", "int", nullable=False), _catalog("value", "nvarchar(20)", nullable=True)]
    filtered = [_framework_unique_authority(filter_definition="([id]>(0))")]

    with pytest.raises(SnapshotReconciliationError) as raised:
        MssqlGenericTargetContract(_target_strategy(catalog, filtered)).validate_existing(
            _load_config(), _target_staging()
        )

    assert raised.value.code == "mssql_native_projection.target_unique_authority_missing"


def test_ignore_dup_key_unique_index_is_not_exact_authority() -> None:
    catalog = [_catalog("id", "int", nullable=False), _catalog("value", "nvarchar(20)", nullable=True)]
    lossy = [_framework_unique_authority(ignore_dup_key=True)]

    with pytest.raises(SnapshotReconciliationError) as raised:
        MssqlGenericTargetContract(_target_strategy(catalog, lossy)).validate_existing(
            _load_config(), _target_staging()
        )

    assert raised.value.code == "mssql_native_projection.target_unique_authority_missing"


def test_fresh_renderer_unique_catalog_shape_is_accepted_on_existing_target() -> None:
    catalog = [_catalog("id", "int", nullable=False), _catalog("value", "nvarchar(20)", nullable=True)]
    contract = resolve_unique_authority_contract(
        _load_config(),
        {"id": "int", "value": "nvarchar(20)"},
    )
    assert contract is not None
    observed = _framework_unique_authority()
    assert observed["index_name"] == contract.name
    assert observed["type_desc"] == contract.type_desc
    assert observed["minimum_compression"] == contract.data_compression
    assert observed["maximum_compression"] == contract.data_compression

    MssqlGenericTargetContract(_target_strategy(catalog, [observed])).validate_existing(
        _load_config(),
        _target_staging(),
    )


def test_unique_included_payload_is_accepted_but_never_becomes_key_authority() -> None:
    catalog = [_catalog("id", "int", nullable=False), _catalog("value", "nvarchar(20)", nullable=True)]
    key = _framework_unique_authority()
    included = _framework_unique_authority(
        column_name="value",
        key_ordinal=0,
        is_included_column=True,
    )

    MssqlGenericTargetContract(_target_strategy(catalog, [key, included])).validate_existing(
        _load_config(),
        _target_staging(),
    )


@pytest.mark.parametrize(
    "drift",
    (
        {"index_name": "unowned_unique_name"},
        {"type_desc": "CLUSTERED"},
        {"is_primary_key": True},
        {"is_unique_constraint": True},
        {"is_disabled": True},
        {"is_hypothetical": True},
        {"is_descending_key": True},
        {"data_space_type_desc": "PARTITION_SCHEME", "partition_count": 2},
        {"partition_count": 2},
        {"minimum_compression": "ROW", "maximum_compression": "ROW"},
        {"minimum_compression": "NONE", "maximum_compression": "ROW"},
    ),
)
def test_framework_unique_catalog_drift_is_rejected_exactly(
    drift: dict[str, object],
) -> None:
    catalog = [_catalog("id", "int", nullable=False), _catalog("value", "nvarchar(20)", nullable=True)]

    with pytest.raises(SnapshotReconciliationError) as raised:
        MssqlGenericTargetContract(_target_strategy(catalog, [_framework_unique_authority(**drift)])).validate_existing(
            _load_config(), _target_staging()
        )

    assert raised.value.code == "mssql_native_projection.target_unique_authority_missing"


def test_unique_authority_renderer_delegates_to_frozen_catalog_contract() -> None:
    config = _load_config()

    contract = resolve_unique_authority_contract(
        config,
        {"id": "int", "value": "nvarchar(20)"},
    )

    assert contract is not None
    assert contract.name == "ux_dpone_target_table_id"
    assert contract.key_columns == ("id",)
    assert contract.filter_definition is None
    assert contract.type_desc == "NONCLUSTERED"
    assert contract.data_compression == "NONE"
    assert contract.is_unique is True
    assert contract.ignore_dup_key is False
    assert render_unique_authority_ddl(
        config,
        "[target_db].[dbo].[target_table]",
        {"id": "int", "value": "nvarchar(20)"},
    ) == (
        "CREATE UNIQUE NONCLUSTERED INDEX [ux_dpone_target_table_id] ON "
        "[target_db].[dbo].[target_table] ([id]) WITH (DATA_COMPRESSION = NONE)"
    )


def test_scd2_unique_authority_contract_binds_exact_current_row_filter() -> None:
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="source_table",
        target_schema="dbo",
        target_table="target_table",
        load_strategy=LoadStrategy.SCD2,
        unique_key=["id"],
    )

    contract = resolve_unique_authority_contract(config, {"id": "int"})

    assert contract is not None
    assert contract.filter_definition == "[__dpone__is_current] = 1"
    assert render_unique_authority_ddl(
        config,
        "[target_db].[dbo].[target_table]",
        {"id": "int"},
    ) == (
        "CREATE UNIQUE NONCLUSTERED INDEX [ux_dpone_target_table_id] ON "
        "[target_db].[dbo].[target_table] ([id]) WHERE [__dpone__is_current] = 1 "
        "WITH (DATA_COMPRESSION = NONE)"
    )


def test_exact_authored_primary_key_replaces_redundant_ordinary_unique_index() -> None:
    config = _load_config(
        physical_design={"indexes": {"primary_key": ["id"]}},
    )

    assert resolve_unique_authority_contract(config, {"id": "int"}) is None
    assert (
        render_unique_authority_ddl(
            config,
            "[target_db].[dbo].[target_table]",
            {"id": "int"},
        )
        is None
    )


def test_authored_primary_key_authority_is_left_to_physical_contract() -> None:
    config = _load_config(
        physical_design={"indexes": {"primary_key": ["id"]}},
    )
    catalog = [_catalog("id", "int", nullable=False), _catalog("value", "nvarchar(20)", nullable=True)]

    MssqlGenericTargetContract(_target_strategy(catalog)).validate_existing(
        config,
        _target_staging(),
    )


def test_external_physical_primary_key_is_revalidated_as_unique_authority() -> None:
    config = _load_config(
        physical_design={
            "apply_runtime": False,
            "indexes": {"primary_key": ["id"]},
            "storage": {"mssql": {"compression": "row"}},
        },
    )
    catalog = [_catalog("id", "int", nullable=False), _catalog("value", "nvarchar(20)", nullable=True)]
    primary = _framework_unique_authority(
        index_name="pk_target_database_dbo_target_table_id",
        type_desc="CLUSTERED",
        is_primary_key=True,
        minimum_compression="ROW",
        maximum_compression="ROW",
    )

    MssqlGenericTargetContract(_target_strategy(catalog, [primary])).validate_existing(
        config,
        _target_staging(),
    )

    with pytest.raises(SnapshotReconciliationError) as raised:
        MssqlGenericTargetContract(_target_strategy(catalog)).validate_existing(
            config,
            _target_staging(),
        )

    assert raised.value.code == "mssql_native_projection.target_unique_authority_missing"


def test_scd2_business_key_primary_key_is_rejected_before_target_ddl() -> None:
    config = LoadConfig(
        source_conn_id="source",
        target_conn_id="target",
        source_schema="dbo",
        source_table="source_table",
        target_schema="dbo",
        target_table="target_table",
        load_strategy=LoadStrategy.SCD2,
        unique_key=["tenant_id", "id"],
        options={"physical_design": {"indexes": {"primary_key": ["id"]}}},
    )

    with pytest.raises(SnapshotReconciliationError) as raised:
        resolve_unique_authority_contract(
            config,
            {"tenant_id": "int", "id": "int"},
        )

    assert raised.value.code == "mssql_native_projection.scd2_physical_primary_key_conflict"


def test_declared_retained_predecessor_is_allowed_with_exact_catalog_shape() -> None:
    catalog = [
        _catalog("id", "int", nullable=False),
        _catalog("value", "nvarchar(20)", nullable=True),
        _catalog("legacy_value", "int", nullable=True),
    ]
    authority = [_framework_unique_authority()]
    projection = SimpleNamespace(
        retained_target_columns=(RetainedMssqlTargetColumn("legacy_value", "int", True, None),)
    )

    MssqlGenericTargetContract(_target_strategy(catalog, authority)).validate_existing(
        _load_config(),
        _target_staging(),
        target_projection=projection,
    )


@pytest.mark.parametrize(
    ("extra", "expected"),
    [
        (_catalog("unrelated", "int", nullable=True), "target_shape_invalid"),
        (_catalog("legacy_value", "nvarchar(20)", nullable=True), "retained_target_shape_invalid"),
        (_catalog("legacy_value", "int", nullable=False), "retained_target_shape_invalid"),
    ],
)
def test_retained_predecessor_rejects_unrelated_or_catalog_drift(extra, expected: str) -> None:
    catalog = [
        _catalog("id", "int", nullable=False),
        _catalog("value", "nvarchar(20)", nullable=True),
        extra,
    ]
    authority = [_framework_unique_authority()]
    projection = SimpleNamespace(
        retained_target_columns=(RetainedMssqlTargetColumn("legacy_value", "int", True, None),)
    )

    with pytest.raises(SnapshotReconciliationError) as raised:
        MssqlGenericTargetContract(_target_strategy(catalog, authority)).validate_existing(
            _load_config(),
            _target_staging(),
            target_projection=projection,
        )

    assert raised.value.code == f"mssql_native_projection.{expected}"


@pytest.mark.parametrize(
    ("kind", "dtype", "expected"),
    [
        ("clustered", "varchar(900)", 900),
        ("nonclustered", "nvarchar(850)", 1700),
    ],
)
def test_index_key_declared_width_boundary_is_accepted(kind, dtype: str, expected: int) -> None:
    assert require_mssql_index_key_width({"id": dtype}, ["id"], kind=kind) == expected


@pytest.mark.parametrize(
    ("kind", "dtype"),
    [
        ("clustered", "varchar(901)"),
        ("nonclustered", "nvarchar(851)"),
    ],
)
def test_index_key_declared_width_plus_one_is_rejected(kind, dtype: str) -> None:
    with pytest.raises(MssqlIndexKeyContractError, match="width_exceeded"):
        require_mssql_index_key_width({"id": dtype}, ["id"], kind=kind)


def test_index_key_column_count_16_passes_and_17_fails() -> None:
    columns = {f"key_{index}": "int" for index in range(17)}

    assert require_mssql_index_key_width(columns, list(columns)[:16], kind="nonclustered") == 64
    with pytest.raises(MssqlIndexKeyContractError) as raised:
        require_mssql_index_key_width(columns, list(columns), kind="nonclustered")

    assert raised.value.code == "mssql.index_key.column_count_exceeded"
