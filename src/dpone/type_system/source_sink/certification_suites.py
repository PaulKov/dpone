"""Default type-matrix certification suites."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.type_system.source_sink.certification_models import (
    DecisionCategory,
    TypeCertificationCase,
    TypeCertificationSuite,
)
from dpone.type_system.source_sink.certification_payloads import case_matrix_entry
from dpone.type_system.source_sink.certification_suites_postgres_mssql import postgres_mssql_suite
from dpone.type_system.source_sink.mssql_clickhouse import MssqlClickHouseMatrixPolicy


class TypeCertificationSuiteRegistry:
    """In-memory registry for production-critical source -> sink type suites."""

    def __init__(self, suites: Sequence[TypeCertificationSuite]) -> None:
        self._suites = {(suite.source, suite.sink): suite for suite in suites}

    @classmethod
    def default(cls) -> TypeCertificationSuiteRegistry:
        return cls((_mssql_clickhouse_suite(), postgres_mssql_suite(), _clickhouse_mssql_suite()))

    def suite(self, source: str, sink: str) -> TypeCertificationSuite:
        key = (source.strip().lower(), sink.strip().lower())
        if key not in self._suites:
            raise ValueError(f"type certification suite is not available for {key[0]} -> {key[1]}")
        return self._suites[key]

    def build_matrix_payload(self, source: str, sink: str) -> dict[str, Any]:
        suite = self.suite(source, sink)
        payload: dict[str, Any] = {
            "source": suite.source,
            "sink": suite.sink,
            "profile": suite.profile,
            "entries": [case_matrix_entry(case) for case in suite.cases],
            "runbook": suite.runbook,
            "certification": {
                "suite": f"{suite.source}_to_{suite.sink}",
                "case_count": len(suite.cases),
                "artifact_names": [
                    "type_matrix_decisions.json",
                    "physical_ddl_plan.sql",
                    "schema_evolution_rerun.json",
                    "typed_reconciliation.json",
                    "live_fixture_summary.md",
                ],
            },
        }
        if suite.source == "mssql" and suite.sink == "clickhouse":
            payload["type_fidelity"] = MssqlClickHouseMatrixPolicy().to_dict()
        return payload


def _mssql_clickhouse_suite() -> TypeCertificationSuite:
    source = "mssql"
    sink = "clickhouse"
    return TypeCertificationSuite(
        source=source,
        sink=sink,
        profile="mssql_to_clickhouse_lossless_v2",
        runbook="docs/source-sink/mssql-to-clickhouse.md#type-matrix-certification",
        cases=(
            _case("tinyint", source, sink, "tinyint", "integer", "UInt8", "numeric TSV"),
            _case("smallint", source, sink, "smallint", "integer", "Int16", "numeric TSV"),
            _case("int_nullable", source, sink, "int", "integer", "Nullable(Int32)", "numeric TSV", nullable=True),
            _case("bigint", source, sink, "bigint", "integer", "Int64", "numeric TSV"),
            _case("decimal_18_4", source, sink, "decimal(18,4)", "decimal", "Decimal(18,4)", "decimal TSV"),
            _case("money", source, sink, "money", "decimal", "Decimal(19,4)", "decimal TSV"),
            _case("float", source, sink, "float", "float", "Float64", "numeric/scalar TSV", lossless=False),
            _case(
                "bit_nullable", source, sink, "bit", "boolean", "Nullable(Bool)", "numeric/scalar TSV", nullable=True
            ),
            _case("uuid", source, sink, "uniqueidentifier", "uuid", "UUID", "numeric/scalar TSV"),
            _case("date", source, sink, "date", "date", "Date", "temporal text"),
            _case(
                "datetime_epoch",
                source,
                sink,
                "datetime",
                "timestamp",
                "DateTime64(3)",
                "epoch_or_text DateTime64 wire format",
            ),
            _case(
                "datetime2_7_epoch",
                source,
                sink,
                "datetime2(7)",
                "timestamp",
                "DateTime64(7)",
                "epoch_or_text DateTime64 wire format",
            ),
            _case(
                "smalldatetime",
                source,
                sink,
                "smalldatetime",
                "timestamp",
                "DateTime64(0)",
                "epoch_or_text DateTime64 wire format",
            ),
            _case(
                "datetimeoffset_preserve_offset",
                source,
                sink,
                "datetimeoffset(7)",
                "offset_timestamp",
                "DateTime64(7, 'UTC')",
                "UTC/fixed/text/offset companion projection",
                type_fidelity={"temporal": {"offset_timestamp": {"mode": "preserve_offset"}}},
            ),
            _case("time_text", source, sink, "time(7)", "time", "String", "temporal text"),
            _case(
                "nvarchar_nullable",
                source,
                sink,
                "nvarchar(510)",
                "string",
                "Nullable(String)",
                "escaped TSV text",
                nullable=True,
            ),
            _case(
                "varbinary",
                source,
                sink,
                "varbinary(max)",
                "binary",
                "String",
                "configured binary codec",
                lossless=False,
            ),
            _case(
                "rowversion_hex",
                source,
                sink,
                "rowversion",
                "binary",
                "String",
                "configured binary codec",
                type_fidelity={"binary_encoding": "hex"},
            ),
            _case(
                "sql_variant_policy",
                source,
                sink,
                "sql_variant",
                "variant",
                "String",
                "escaped TSV text",
                requires_contract=True,
            ),
        ),
    )


def _clickhouse_mssql_suite() -> TypeCertificationSuite:
    source = "clickhouse"
    sink = "mssql"
    return TypeCertificationSuite(
        source=source,
        sink=sink,
        profile="clickhouse_to_mssql_landing_v1",
        runbook="docs/source-sink/clickhouse-to-mssql.md#type-matrix-certification",
        cases=(
            _case("uint8", source, sink, "UInt8", "integer", "tinyint", "integer text"),
            _case("int32", source, sink, "Int32", "integer", "int", "integer text"),
            _case("uint64", source, sink, "Nullable(UInt64)", "integer", "decimal(20,0)", "integer text"),
            _case("float64", source, sink, "Float64", "float", "float", "float text"),
            _case("bool", source, sink, "Bool", "boolean", "bit", "0/1 text"),
            _case("string_nullable", source, sink, "Nullable(String)", "string", "nvarchar(max)", "BulkTextCodec text"),
            _case("uuid", source, sink, "UUID", "uuid", "uniqueidentifier", "uuid text"),
            _case("date", source, sink, "Date", "date", "date", "ISO date text"),
            _case(
                "datetime64_3",
                source,
                sink,
                "DateTime64(3, 'Europe/Moscow')",
                "timestamp",
                "datetime2(3)",
                "timestamp text",
            ),
            _case("decimal_18_4", source, sink, "Decimal(18,4)", "decimal", "decimal(18,4)", "decimal text"),
            _case(
                "datetime64_9_contract_required",
                source,
                sink,
                "DateTime64(9)",
                "timestamp",
                "datetime2(7)",
                "timestamp text",
                lossless=False,
                requires_contract=True,
                compatible=False,
            ),
            _case(
                "decimal_76_contract_required",
                source,
                sink,
                "Decimal(76,4)",
                "decimal",
                "nvarchar(max)",
                "BulkTextCodec text/json",
                lossless=False,
                requires_contract=True,
                compatible=False,
            ),
            _case(
                "array_contract_required",
                source,
                sink,
                "Array(String)",
                "complex",
                "nvarchar(max)",
                "BulkTextCodec text/json",
                lossless=False,
                requires_contract=True,
                compatible=False,
            ),
            # Enum values land deterministically as their names under the
            # strict full-inventory policy (see docs/type-mapping-matrix.md).
            _case(
                "enum_mapped_as_names",
                source,
                sink,
                "Enum8('a' = 1)",
                "enum",
                "nvarchar(max)",
                "BulkTextCodec text",
            ),
        ),
    )


def _case(
    name: str,
    source: str,
    sink: str,
    source_type: str,
    canonical_type: str,
    target_type: str,
    transport: str,
    *,
    nullable: bool = False,
    lossless: bool = True,
    compatible: bool = True,
    decision_category: DecisionCategory = "auto_inferred",
    requires_contract: bool = False,
    type_fidelity: Mapping[str, Any] | None = None,
) -> TypeCertificationCase:
    category: DecisionCategory = "incompatible_requires_policy" if requires_contract else decision_category
    return TypeCertificationCase(
        name=name,
        source=source,
        sink=sink,
        source_type=source_type,
        expected_canonical_type=canonical_type,
        expected_target_type=target_type,
        expected_transport=transport,
        nullable=nullable,
        lossless=lossless,
        compatible=compatible,
        decision_category=category,
        type_fidelity=type_fidelity,
    )


__all__ = ["TypeCertificationSuiteRegistry"]
