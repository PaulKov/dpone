"""Public GitOps schema fragments for signed PostgreSQL source authority."""

from __future__ import annotations

from typing import Any

from dpone.gitops.schema_contract_primitives import object_schema


def postgres_source_authority_schema() -> dict[str, Any]:
    """Signed PostgreSQL cluster, database, principals, and finite relations."""

    named_oid = object_schema(
        required=("canonical_name", "oid"),
        properties={
            "canonical_name": {
                "type": "string",
                "minLength": 1,
                "maxLength": 63,
            },
            "oid": {"type": "integer", "minimum": 1, "maximum": 4294967295},
        },
        additional_properties=False,
    )
    relation = object_schema(
        required=("schema", "relation", "namespace_oid", "relation_oid"),
        properties={
            "schema": {"type": "string", "minLength": 1, "maxLength": 63},
            "relation": {"type": "string", "minLength": 1, "maxLength": 63},
            "namespace_oid": {
                "type": "integer",
                "minimum": 1,
                "maximum": 4294967295,
            },
            "relation_oid": {
                "type": "integer",
                "minimum": 1,
                "maximum": 4294967295,
            },
        },
        additional_properties=False,
    )
    common_properties = {
        "topology_role": {"enum": ["primary", "standby"]},
        "database": named_oid,
        "principals": object_schema(
            required=("effective", "session"),
            properties={"effective": named_oid, "session": named_oid},
            additional_properties=False,
        ),
        "relations": {
            "type": "object",
            "minProperties": 1,
            "propertyNames": {"type": "string", "minLength": 3},
            "additionalProperties": relation,
        },
    }
    physical = object_schema(
        required=(
            "version",
            "system_identifier",
            "timeline_id",
            "topology_role",
            "database",
            "principals",
            "relations",
        ),
        properties={
            "version": {"const": 1},
            "system_identifier": {
                "type": "string",
                "pattern": "^[1-9][0-9]{0,19}$",
            },
            "timeline_id": {
                "type": "integer",
                "minimum": 1,
                "maximum": 4294967295,
            },
            **common_properties,
        },
        additional_properties=False,
    )
    catalog = object_schema(
        required=(
            "version",
            "verification_profile",
            "topology_role",
            "database",
            "principals",
            "relations",
        ),
        properties={
            "version": {"const": 2},
            "verification_profile": {"const": "catalog_identity"},
            **common_properties,
        },
        additional_properties=False,
    )
    return {"oneOf": [physical, catalog]}


__all__ = ["postgres_source_authority_schema"]
