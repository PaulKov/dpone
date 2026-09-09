from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.runtime.dbt_schema_readiness import DbtSchemaReadinessError, DbtSchemaReadinessGate
from dpone.runtime.etl.lifecycle import RuntimeLifecycleService
from dpone.runtime.sinks.load_payload import LoadPayload


def _readiness() -> dict[str, object]:
    return {
        "enabled": True,
        "source_relation": {"database": "DWH", "schema": "mart", "name": "orders"},
        "column_order": ["order_id", "amount"],
        "extra_columns": "block",
    }


def _contract() -> dict[str, object]:
    return {
        "enforcement": "strict",
        "columns": {
            "order_id": {"type": "bigint", "nullable": False},
            "amount": {"type": "decimal", "precision": 18, "scale": 2, "nullable": True},
        },
    }


def test_dbt_schema_readiness_accepts_exact_relation_order_and_types() -> None:
    report = DbtSchemaReadinessGate().validate(
        readiness=_readiness(),
        schema_contract=_contract(),
        source_database="DWH",
        source_schema="mart",
        source_table="orders",
        observed_schema=[("order_id", "bigint"), ("amount", "decimal(18,2) nullable")],
    )
    assert report.checked_types == 2
    assert report.checked_nullability == 2


@pytest.mark.parametrize(
    "observed",
    [
        [("amount", "decimal(18,2)"), ("order_id", "bigint")],
        [("order_id", "bigint"), ("amount", "nvarchar(50)")],
        [("order_id", "bigint"), ("amount", "decimal(18,2)"), ("extra", "int")],
        [("order_id", "bigint"), ("amount", "decimal(38,9) nullable")],
        [("order_id", "bigint nullable"), ("amount", "decimal(18,2) nullable")],
    ],
)
def test_dbt_schema_readiness_fails_before_sink_for_drift(observed: list[tuple[str, str]]) -> None:
    with pytest.raises(DbtSchemaReadinessError) as exc_info:
        DbtSchemaReadinessGate().validate(
            readiness=_readiness(),
            schema_contract=_contract(),
            source_database="DWH",
            source_schema="mart",
            source_table="orders",
            observed_schema=observed,
        )
    assert exc_info.value.code == "DPONE_DBT_SCHEMA_DRIFT"


def test_dbt_schema_readiness_rejects_same_schema_and_table_in_wrong_database() -> None:
    with pytest.raises(DbtSchemaReadinessError) as exc_info:
        DbtSchemaReadinessGate().validate(
            readiness=_readiness(),
            schema_contract=_contract(),
            source_database="Reporting",
            source_schema="mart",
            source_table="orders",
            observed_schema=[("order_id", "bigint"), ("amount", "decimal(18,2) nullable")],
        )

    assert exc_info.value.code == "DPONE_DBT_SCHEMA_DRIFT"


def test_runtime_lifecycle_passes_source_database_to_dbt_readiness_gate() -> None:
    load_config = SimpleNamespace(
        source_database="Reporting",
        source_schema="mart",
        source_table="orders",
        options={
            "dbt_schema_readiness": _readiness(),
            "schema_contract": _contract(),
        },
    )
    payload = LoadPayload(
        artifact=object(),  # type: ignore[arg-type]
        schema=(),
        relation_schema=[("order_id", "bigint"), ("amount", "decimal(18,2) nullable")],
    )

    with pytest.raises(DbtSchemaReadinessError) as exc_info:
        RuntimeLifecycleService().prepare_before_schema_evolution(
            load_config=load_config,
            payload=payload,
            run_id="run-1",
            load_id="load-1",
        )

    assert exc_info.value.code == "DPONE_DBT_SCHEMA_DRIFT"
