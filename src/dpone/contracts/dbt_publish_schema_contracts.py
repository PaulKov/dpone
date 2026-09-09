"""Published JSON Schemas for dbt self-service authoring, build and runtime."""

from __future__ import annotations

from typing import Any

from dpone.contracts.dbt_publish_schema_contract_authoring import authoring_schema_contracts
from dpone.contracts.dbt_publish_schema_contract_common import document
from dpone.contracts.dbt_publish_schema_contract_runtime import runtime_schema_contracts


def dbt_schema_contracts() -> dict[str, dict[str, Any]]:
    """Return fresh schemas keyed by their canonical contract identifier."""

    contracts = {
        **authoring_schema_contracts(),
        **runtime_schema_contracts(),
    }
    return {key: document(key, schema) for key, schema in contracts.items()}


__all__ = ["dbt_schema_contracts"]
