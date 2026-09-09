"""Deterministic public JSON Schema generator for semantic-refresh V1."""

from __future__ import annotations

import json
from typing import Any

from dpone.contracts.semantic_refresh_schema_assurance import assurance_contract_schemas
from dpone.contracts.semantic_refresh_schema_authority import authority_contract_schemas
from dpone.contracts.semantic_refresh_schema_bindings import binding_contract_schemas
from dpone.contracts.semantic_refresh_schema_cleanup import cleanup_contract_schemas
from dpone.contracts.semantic_refresh_schema_evidence import evidence_contract_schemas
from dpone.contracts.semantic_refresh_schema_plans import plan_contract_schemas
from dpone.contracts.semantic_refresh_schema_proofs import proof_contract_schemas


def semantic_refresh_contract_schemas() -> dict[str, dict[str, Any]]:
    """Return fresh schemas keyed by their canonical contract identifier."""

    return {
        **plan_contract_schemas(),
        **binding_contract_schemas(),
        **proof_contract_schemas(),
        **cleanup_contract_schemas(),
        **evidence_contract_schemas(),
        **authority_contract_schemas(),
        **assurance_contract_schemas(),
    }


def render_semantic_refresh_schema(schema_id: str) -> bytes:
    """Render one checked-in schema with stable formatting and a trailing newline."""

    try:
        schema = semantic_refresh_contract_schemas()[schema_id]
    except KeyError as exc:
        raise ValueError(f"unknown semantic-refresh schema: {schema_id}") from exc
    return (json.dumps(schema, allow_nan=False, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


__all__ = ["render_semantic_refresh_schema", "semantic_refresh_contract_schemas"]
