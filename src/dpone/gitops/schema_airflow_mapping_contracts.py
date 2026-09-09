"""JSON Schema contracts for bounded Airflow backfill mapping."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract, documented_contract

_DIGEST = {"type": "string", "pattern": r"^sha256:[0-9a-f]{64}$"}


def airflow_mapping_schema_contracts() -> tuple[GitOpsSchemaContract, ...]:
    return (airflow_mapping_plan_contract(), airflow_mapping_item_contract())


def airflow_mapping_plan_contract() -> GitOpsSchemaContract:
    schema = airflow_mapping_plan_schema()
    return documented_contract(
        name="airflow-mapping-plan",
        kind="dpone.airflow-mapping-plan.v1",
        title="dpone GitOps bounded Airflow backfill mapping plan",
        required=tuple(schema["required"]),
        properties=dict(schema["properties"]),
        additional_properties=False,
    )


def airflow_mapping_item_contract() -> GitOpsSchemaContract:
    schema = airflow_mapping_item_schema()
    return documented_contract(
        name="airflow-mapping-item",
        kind="dpone.airflow-mapping-item.v1",
        title="dpone GitOps bounded Airflow backfill mapping item",
        required=tuple(schema["required"]),
        properties=dict(schema["properties"]),
        additional_properties=False,
    )


def airflow_mapping_plan_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "schema",
            "mode",
            "plan_fingerprint",
            "backfill_plan_hash",
            "chunks_total",
            "items_total",
            "limits",
            "items",
        ],
        "additionalProperties": False,
        "properties": {
            "schema": {"const": "dpone.airflow-mapping-plan.v1"},
            "mode": {"enum": ["internal", "visible", "summary"]},
            "plan_fingerprint": dict(_DIGEST),
            "backfill_plan_hash": {"anyOf": [dict(_DIGEST), {"type": "null"}]},
            "chunks_total": {"type": "integer", "minimum": 0},
            "items_total": {"type": "integer", "minimum": 0, "maximum": 200},
            "limits": {
                "type": "object",
                "required": ["max_items", "max_active", "pool"],
                "additionalProperties": False,
                "properties": {
                    "max_items": {"type": "integer", "minimum": 1, "maximum": 200},
                    "max_active": {"type": "integer", "minimum": 1, "maximum": 64},
                    "pool": {"type": "string", "pattern": r"^[A-Za-z0-9_.-]*$"},
                },
            },
            "items": {"type": "array", "maxItems": 200, "items": airflow_mapping_range_schema()},
        },
    }


def airflow_mapping_range_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["item_index", "first_chunk_index", "last_chunk_index", "chunks_count"],
        "additionalProperties": False,
        "properties": {
            "item_index": {"type": "integer", "minimum": 0},
            "first_chunk_index": {"type": "integer", "minimum": 1},
            "last_chunk_index": {"type": "integer", "minimum": 1},
            "chunks_count": {"type": "integer", "minimum": 1},
        },
    }


def airflow_mapping_item_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": [
            "schema",
            "mode",
            "mapping_plan_fingerprint",
            "backfill_plan_hash",
            "item_index",
            "first_chunk_index",
            "last_chunk_index",
            "chunks_count",
        ],
        "additionalProperties": False,
        "properties": {
            "schema": {"const": "dpone.airflow-mapping-item.v1"},
            "mode": {"enum": ["visible", "summary"]},
            "mapping_plan_fingerprint": dict(_DIGEST),
            "backfill_plan_hash": dict(_DIGEST),
            **airflow_mapping_range_schema()["properties"],
        },
    }


def airflow_mapping_summary_schema() -> dict[str, Any]:
    properties = dict(airflow_mapping_item_schema()["properties"])
    properties.pop("schema")
    properties["plan_fingerprint"] = properties.pop("mapping_plan_fingerprint")
    properties["item_status"] = {"enum": ["success", "error", "cancelled"]}
    return {
        "type": "object",
        "required": list(properties),
        "additionalProperties": False,
        "properties": properties,
    }


__all__ = [
    "airflow_mapping_item_contract",
    "airflow_mapping_item_schema",
    "airflow_mapping_plan_contract",
    "airflow_mapping_plan_schema",
    "airflow_mapping_schema_contracts",
    "airflow_mapping_summary_schema",
]
