"""Compose build- and runtime-side dbt JSON Schema contracts."""

from __future__ import annotations

from typing import Any

from dpone.contracts.dbt_publish_schema_contract_reports import (
    report_schema_contracts,
)
from dpone.contracts.dbt_publish_schema_contract_runtime_evidence import (
    execution_evidence_schema,
    source_snapshot_schema,
)
from dpone.contracts.dbt_publish_schema_contract_runtime_inputs import (
    runtime_input_schema_contracts,
)
from dpone.contracts.dbt_source_inventory_schema import dbt_source_inventory_schema


def runtime_schema_contracts() -> dict[str, dict[str, Any]]:
    """Return the complete dbt build/runtime schema catalog."""

    return {
        **runtime_input_schema_contracts(),
        "dpone.dbt-execution-evidence.v1": execution_evidence_schema(),
        "dpone.dbt-source-snapshot.v1": source_snapshot_schema(),
        "dpone.dbt-source-snapshot.v2": dbt_source_inventory_schema(),
        **report_schema_contracts(),
    }


__all__ = ["runtime_schema_contracts"]
