"""Closed offline admission projection for MSSQL native delivery v2."""

from __future__ import annotations

from typing import Any


def _closed(properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "object",
        "additionalProperties": False,
        "required": sorted(properties),
        "properties": properties,
    }


def mssql_native_admission_v2_schema() -> dict[str, Any]:
    """Return the deterministic closed Draft 7 admission schema."""
    diagnostic = {"type": "string", "pattern": "^[a-z][a-z0-9_]*\\.[a-z][a-z0-9_]*$"}
    return {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://dpone.dev/schemas/dpone.mssql-native-admission.v2.schema.json",
        "title": "dpone MSSQL native admission v2",
        **_closed(
            {
                "schema_version": {"const": 2},
                "kind": {"const": "dpone.mssql-native-admission.v2"},
                "status": {"enum": ["ready", "blocked"]},
                "import_backend": {"enum": ["bcp", "mssql_sqlclient"]},
                "verification_backend": {"const": "target_local"},
                "identity_version": {"const": 2},
                "capability_id": {"enum": ["bcp-supervised-stage-barrier-v1", "sqlclient-session-applock-v1"]},
                "blockers": {"type": "array", "uniqueItems": True, "items": diagnostic},
                "warnings": {"type": "array", "uniqueItems": True, "items": diagnostic},
            }
        ),
    }


__all__ = ["mssql_native_admission_v2_schema"]
