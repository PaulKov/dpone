"""Lossless PostgreSQL→MSSQL physical projection contracts."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type
from dpone.runtime.incremental_snapshot import SnapshotReconciliationError
from dpone.runtime.sources.strategies.postgres.postgres_snapshot_extract import (
    PostgresSnapshotEnvelopeExtractor,
)
from dpone.runtime.support.mssql_lossless_projection import (
    MssqlLosslessGuardMode,
    lossless_target_projection_guard_mode,
    resolved_mssql_base_type,
)
from dpone.runtime.support.mssql_native_projection import lossless_roundtrip_mismatch
from dpone.runtime.support.mssql_snapshot_projection import (
    require_lossless_target_projection,
    resolved_source_type,
    resolved_target_type,
)


@pytest.mark.parametrize(
    ("source", "target"),
    [
        ("decimal(10,3)", "decimal(12,4)"),
        ("int", "bigint"),
        ("real", "float(53)"),
        ("datetime2(6)", "datetime2(7)"),
        ("datetimeoffset(6)", "datetimeoffset(7)"),
        ("nvarchar(max)", "nvarchar(450)"),
        ("nvarchar(max)", "varchar(128)"),
        ("varbinary(max)", "binary(32)"),
    ],
)
def test_structurally_lossless_or_value_guarded_projection_is_admitted(source: str, target: str) -> None:
    require_lossless_target_projection(source, target, column="value")


@pytest.mark.parametrize(
    ("source", "target"),
    [
        ("decimal(10,3)", "decimal(10,2)"),
        ("decimal(10,3)", "decimal(9,3)"),
        ("bigint", "int"),
        ("float(53)", "real"),
        ("datetime2(6)", "datetime2(3)"),
        ("datetimeoffset(6)", "datetime2(6)"),
        ("uniqueidentifier", "nvarchar(36)"),
        ("bit", "tinyint"),
    ],
)
def test_structurally_lossy_projection_is_rejected_before_export(source: str, target: str) -> None:
    with pytest.raises(SnapshotReconciliationError, match="lossy_target_type_forbidden:value"):
        require_lossless_target_projection(source, target, column="value")


@pytest.mark.parametrize(
    ("source", "target", "expected"),
    [
        ("int", "bigint", MssqlLosslessGuardMode.STRUCTURAL),
        ("nvarchar(max)", "nvarchar(36)", MssqlLosslessGuardMode.VALUE_GUARDED),
        ("varbinary(max)", "binary(16)", MssqlLosslessGuardMode.VALUE_GUARDED),
    ],
)
def test_runtime_facade_classifies_required_losslessness_proof(
    source: str,
    target: str,
    expected: MssqlLosslessGuardMode,
) -> None:
    assert lossless_target_projection_guard_mode(source, target, column="value") is expected


def test_runtime_facade_classifies_structurally_forbidden_projection() -> None:
    assert (
        lossless_target_projection_guard_mode("decimal(20,2)", "decimal(10,2)", column="value")
        is MssqlLosslessGuardMode.FORBIDDEN
    )


def test_runtime_facade_does_not_reclassify_invalid_physical_type() -> None:
    with pytest.raises(ValueError, match="unsupported MSSQL physical type"):
        lossless_target_projection_guard_mode("int", "not_a_sql_type", column="value")


@pytest.mark.parametrize(
    ("resolved", "expected"),
    [
        (" NVARCHAR ( 36 ) ", "nvarchar"),
        ("datetime2(7)", "datetime2"),
        ("varbinary(max)", "varbinary"),
    ],
)
def test_runtime_facade_exposes_resolved_mssql_base_type(resolved: str, expected: str) -> None:
    assert resolved_mssql_base_type(resolved) == expected


def test_resolver_preserves_bounded_postgres_character_width_and_checks_override() -> None:
    assert resolved_source_type("character varying(255)") == "nvarchar(510)"
    assert resolved_source_type("character(8)") == "nvarchar(16)"
    config = SimpleNamespace(
        options={"physical_design": {"columns": {"amount": {"target_type": {"mssql": "decimal(10,2)"}}}}}
    )

    with pytest.raises(SnapshotReconciliationError, match=r"numeric\(10,3\)->decimal\(10,2\)"):
        resolved_target_type(config, "amount", "numeric(10,3)")


@pytest.mark.parametrize(
    "target_type",
    [
        "decimal(39,2)",
        "decimal(10,11)",
        "float(54)",
        "datetimeoffset(8)",
        "nvarchar(4001)",
        "varchar(8001)",
        "binary(max)",
        "not_a_sql_type",
    ],
)
def test_invalid_mssql_physical_override_is_rejected_before_export(target_type: str) -> None:
    config = SimpleNamespace(
        options={"physical_design": {"columns": {"value": {"target_type": {"mssql": target_type}}}}}
    )

    with pytest.raises(SnapshotReconciliationError, match="target_physical_type_invalid:value"):
        resolved_target_type(config, "value", "nvarchar(max)")


@pytest.mark.parametrize(
    ("declared", "canonical"),
    [
        ("DECIMAL ( 1 , 0 )", "decimal(1,0)"),
        ("numeric(38,38)", "numeric(38,38)"),
        ("float(1)", "float(1)"),
        ("float(53)", "float(53)"),
        ("datetime2(0)", "datetime2(0)"),
        ("datetimeoffset(7)", "datetimeoffset(7)"),
        ("time", "time(7)"),
        ("nvarchar(4000)", "nvarchar(4000)"),
        ("varchar(8000)", "varchar(8000)"),
        ("varbinary(max)", "varbinary(max)"),
    ],
)
def test_mssql_physical_type_boundaries_are_canonical(declared: str, canonical: str) -> None:
    assert normalize_mssql_physical_type(declared) == canonical


def test_value_roundtrip_predicate_uses_binary_equality_for_text_and_binary() -> None:
    text = lossless_roundtrip_mismatch("nvarchar(max)", "char(8)", "r.[value]")
    binary = lossless_roundtrip_mismatch("varbinary(max)", "binary(8)", "r.[payload]")

    assert text.count("CONVERT(varbinary(max)") >= 2
    assert binary.count("CONVERT(varbinary(max)") >= 2
    assert "TRY_CONVERT(char(8), r.[value])" in text
    assert "TRY_CONVERT(binary(8), r.[payload], 2)" in binary


def test_numeric_roundtrip_predicate_compares_source_native_value() -> None:
    predicate = lossless_roundtrip_mismatch("decimal(12,4)", "decimal(18,6)", "r.[amount]")

    assert "TRY_CONVERT(decimal(12,4), r.[amount])" in predicate
    assert "TRY_CONVERT(decimal(18,6), r.[amount])" in predicate
    assert "<> TRY_CONVERT(decimal(12,4), r.[amount])" in predicate


def test_snapshot_state_contract_rejects_non_key_narrowing_before_row_export() -> None:
    strategy = SimpleNamespace(
        connector=SimpleNamespace(database="source"),
        sink_connector=SimpleNamespace(database="target"),
        physical_target_identity=lambda _config: b"t" * 32,
    )
    config = SimpleNamespace(
        source_database="source",
        source_schema="public",
        source_table="metrics",
        source_conn_id="postgres_source",
        target_database="target",
        target_schema="dbo",
        target_table="metrics",
        unique_key=("id",),
        options={
            "state_identity": {"environment": "dev", "process": "metrics"},
            "unique_key": ["id"],
            "schema_contract": {
                "columns": {
                    "id": {"nullable": False},
                    "amount": {"nullable": False},
                }
            },
            "physical_design": {"columns": {"amount": {"target_type": {"mssql": "decimal(10,2)"}}}},
        },
    )

    with pytest.raises(SnapshotReconciliationError, match="lossy_target_type_forbidden:amount"):
        PostgresSnapshotEnvelopeExtractor(strategy).state_key(
            config,
            [("id", "uniqueidentifier"), ("amount", "decimal(10,3)")],
        )
