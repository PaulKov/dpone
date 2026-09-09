"""JSON Schema fragment + registered contract for MSSQL outlet projections."""

from __future__ import annotations

from typing import Any

MSSQL_OUTLET_PROJECTION_PROPERTY: dict[str, Any] = {
    "mssql_asset_outlet_projection": {"$ref": "#/$defs/mssqlAssetOutletProjection"},
}

# Companion pattern for docs/schema; runtime authority is
# ``dpone_airflow_pack.mssql_asset_uri_codec.require_canonical_mssql_asset_uri``.
_MSSQL_CANONICAL_URI_PATTERN = (
    "^mssql://[a-z0-9](?:[a-z0-9.-]*[a-z0-9])?:[1-9][0-9]{0,4}/(?:[A-Za-z0-9._~%-]+/){2,3}[A-Za-z0-9._~%-]+$"
)


def mssql_asset_outlet_projection_schema() -> dict[str, Any]:
    """Closed ``dpone.mssql-asset-outlet-projection.v1`` wire document."""

    return {
        "type": "object",
        "additionalProperties": False,
        "required": [
            "schema",
            "environment",
            "binding_set_ref",
            "connection_registry_ref",
            "entries",
            "projection_sha256",
        ],
        "properties": {
            "schema": {"const": "dpone.mssql-asset-outlet-projection.v1"},
            "environment": {"type": "string", "minLength": 1},
            "binding_set_ref": {"$ref": "#/$defs/identity"},
            "connection_registry_ref": {"$ref": "#/$defs/identity"},
            "projection_sha256": {"$ref": "#/$defs/identity"},
            "entries": {
                "type": "array",
                "minItems": 1,
                "maxItems": 4096,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": [
                        "asset_ref",
                        "asset_ref_sha256",
                        "registry_connection_ref",
                        "resolved_binding",
                        "workload_ids",
                        "uri",
                    ],
                    "properties": {
                        "workload_ids": {
                            "type": "array",
                            "minItems": 1,
                            "maxItems": 1024,
                            "uniqueItems": True,
                            "items": {"type": "string", "minLength": 1},
                        },
                        "asset_ref": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": [
                                "engine",
                                "connection_ref",
                                "database",
                                "schema",
                                "table",
                            ],
                            "properties": {
                                "engine": {"const": "mssql"},
                                "connection_ref": {"type": "string", "minLength": 1},
                                "database": {"type": "string", "minLength": 1},
                                "schema": {"type": "string", "minLength": 1},
                                "table": {"type": "string", "minLength": 1},
                            },
                        },
                        "asset_ref_sha256": {"$ref": "#/$defs/identity"},
                        "registry_connection_ref": {"type": "string", "minLength": 1},
                        "resolved_binding": {
                            "type": "object",
                            "additionalProperties": False,
                            "required": ["registry_connection_ref"],
                            "properties": {
                                "registry_connection_ref": {
                                    "type": "string",
                                    "minLength": 1,
                                }
                            },
                        },
                        "uri": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 2048,
                            "pattern": _MSSQL_CANONICAL_URI_PATTERN,
                        },
                    },
                },
            },
        },
    }


__all__ = [
    "MSSQL_OUTLET_PROJECTION_PROPERTY",
    "mssql_asset_outlet_projection_schema",
]
