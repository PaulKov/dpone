"""Closed authoring and delivery schemas for verified release composition."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from dpone.contracts.airflow_release_artifacts import RELEASE_ARTIFACT_PATH_PATTERN
from dpone.contracts.release_composition import COMPOSITION_PRODUCER, COMPOSITION_SCHEMA
from dpone.gitops.schema_contract_primitives import documented_contract
from dpone.gitops.schema_release_deployment_definitions import release_artifacts_v2_schema, release_v2_defs
from dpone.gitops.schema_release_set_promotion import COMPACT_PROMOTION_PROFILE, COMPACT_PROMOTION_SCHEMA


def _object(properties: dict[str, Any]) -> dict[str, Any]:
    return {"type": "object", "additionalProperties": False, "required": list(properties), "properties": properties}


def _descriptor(*, pack: bool = False) -> dict[str, Any]:
    fields: dict[str, Any] = {
        "id": {"type": "string", "minLength": 1},
        "path": {"type": "string", "pattern": RELEASE_ARTIFACT_PATH_PATTERN},
        "sha256": {"$ref": "#/$defs/identity"},
        "bytes": {"type": "integer", "minimum": 1, "maximum": 256 * 1024 * 1024},
    }
    if pack:
        fields["pack_fingerprint"] = {"$ref": "#/$defs/identity"}
    return _object(fields)


def _array(items: dict[str, Any]) -> dict[str, Any]:
    return {"type": "array", "minItems": 1, "maxItems": 50_000, "items": items}


def release_composition_contract(native_schema: dict[str, Any]):
    """Reuse the v2 schema unchanged inside the new typed authority boundary."""
    definitions = release_v2_defs()
    native = _expand(native_schema, native_schema["$defs"])
    native.pop("$defs", None)
    artifacts = deepcopy(release_artifacts_v2_schema())
    artifacts["properties"]["composition_sources"] = _array(_descriptor())
    artifacts["required"].append("composition_sources")
    inventory = _object(
        {
            "schema": {"const": "dpone.workload-inventory.v1"},
            "dag_specs": _array(_descriptor()),
            "workload_packs": _array(_descriptor(pack=True)),
        }
    )
    return documented_contract(
        name="release-set-v3",
        kind=COMPOSITION_SCHEMA,
        title="dpone verified release composition",
        required=("schema", "release_id", "producer", "promotion", "artifacts", "constituents"),
        properties={
            "schema": {"const": COMPOSITION_SCHEMA},
            "release_id": {"$ref": "#/$defs/identity"},
            "producer": _object(
                {
                    "name": {"const": COMPOSITION_PRODUCER},
                    "version": {"type": "string", "minLength": 1, "maxLength": 64},
                }
            ),
            "promotion": _object(
                {"schema": {"const": COMPACT_PROMOTION_SCHEMA}, "profile": {"const": COMPACT_PROMOTION_PROFILE}}
            ),
            "artifacts": artifacts,
            "constituents": {
                "type": "array",
                "minItems": 2,
                "maxItems": 2,
                "items": {
                    "oneOf": [
                        _object({"id": {"const": "native"}, "kind": {"const": "dbt_workspace"}, "release": native}),
                        _object(
                            {
                                "id": {"const": "standalone"},
                                "kind": {"const": "workload_inventory"},
                                "inventory": inventory,
                                "inventory_sha256": {"$ref": "#/$defs/identity"},
                            }
                        ),
                    ]
                },
            },
        },
        defs=definitions,
        additional_properties=False,
    )


def release_composition_manifest_contract():
    digest = {"type": "string", "pattern": "^sha256:[0-9a-f]{64}$"}
    root = {"type": "string", "minLength": 1, "maxLength": 4096}
    fields = {
        "schema": {"const": COMPOSITION_PRODUCER},
        "native_workspace": _object({"root": root, "expected_release_id": digest}),
        "standalone": _object({"root": root, "expected_inventory_sha256": digest}),
        "transport": _object(
            {"profile": {"const": COMPACT_PROMOTION_PROFILE}, "xcom_sidecar_image": {"type": "string", "minLength": 1}}
        ),
    }
    return documented_contract(
        name="release-composition",
        kind=COMPOSITION_PRODUCER,
        title="dpone release composition manifest",
        required=tuple(fields),
        properties=fields,
        additional_properties=False,
    )


def _expand(value: Any, definitions: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        ref = value.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/$defs/"):
            return _expand(definitions[ref.removeprefix("#/$defs/")], definitions)
        return {key: _expand(item, definitions) for key, item in value.items() if key != "$defs"}
    if isinstance(value, list):
        return [_expand(item, definitions) for item in value]
    return value
