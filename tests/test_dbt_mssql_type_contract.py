from __future__ import annotations

import pytest

from dpone.contracts.mssql_type_contract import mssql_logical_type
from dpone.runtime.dbt_schema_readiness import DbtSchemaReadinessGate


@pytest.mark.parametrize(
    ("source", "expected"),
    (
        ("uniqueidentifier", {"type": "uuid"}),
        ("money", {"type": "decimal", "precision": 19, "scale": 4}),
        ("smallmoney", {"type": "decimal", "precision": 10, "scale": 4}),
        ("datetimeoffset(7)", {"type": "timestamp with time zone"}),
        ("varbinary(256)", {"type": "binary"}),
        ("float(53)", {"type": "float64"}),
    ),
)
def test_mssql_logical_contract_has_one_canonical_mapping(
    source: str,
    expected: dict[str, object],
) -> None:
    assert mssql_logical_type(source) == expected


@pytest.mark.parametrize(
    "source",
    ("sql_variant", "xml", "geometry", "geography", "hierarchyid"),
)
def test_unsafe_mssql_contract_types_require_explicit_fidelity_policy(
    source: str,
) -> None:
    with pytest.raises(ValueError, match="requires an explicit contract"):
        mssql_logical_type(source)


def test_schema_readiness_consumes_the_same_uuid_contract() -> None:
    report = DbtSchemaReadinessGate().validate(
        readiness={
            "enabled": True,
            "source_relation": {
                "database": "DWH",
                "schema": "dbo",
                "name": "orders",
            },
            "column_order": ["order_id"],
            "extra_columns": "block",
        },
        schema_contract={
            "enforcement": "strict",
            "columns": {
                "order_id": {"type": "uuid", "nullable": False},
            },
        },
        source_database="DWH",
        source_schema="dbo",
        source_table="orders",
        observed_schema=[("order_id", "uniqueidentifier")],
    )

    assert report.checked_types == 1
