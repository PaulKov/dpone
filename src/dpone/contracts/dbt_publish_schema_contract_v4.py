"""Opt-in native policy schema and complete canonical policy validation.

Earlier policy schemas are produced unchanged. V4 shape validation does not
register a managed macro bundle or qualify a runtime; their consumers require
independent protected evidence before granting native execution.
"""

from __future__ import annotations

from typing import Any, cast

from jsonschema import Draft202012Validator

from dpone.contracts.dbt_invocation import DbtInvocationTarget
from dpone.contracts.dbt_native_execution_policy import native_execution_schema, validate_native_execution_policy
from dpone.contracts.dbt_publish_schema_contract_common import DIGEST, IDENTIFIER, TOKEN, object_schema
from dpone.contracts.dbt_publish_schema_contract_intent import intent_v2_schema
from dpone.contracts.dbt_publish_schema_contract_policy import policy_v3_schema
from dpone.contracts.native_delivery_json import (
    NativeJsonValue,
    decode_native_delivery_json,
    encode_native_delivery_json,
)

NATIVE_POLICY_SCHEMA = "dpone.dbt-publish-policy.v4"
NATIVE_AUTHORING_SCHEMA = "dpone.dbt-publish-authoring.v2"
NATIVE_INTENT_SCHEMA = "dpone.dbt-publish-intent.v3"
MODEL_STORAGE_LAYOUTS = ("rowstore_none", "rowstore_row", "rowstore_page", "columnstore")


def policy_v4_schema() -> dict[str, Any]:
    """Extend a fresh V3 schema body without mutating previous contract bytes."""
    root = policy_v3_schema()
    root["properties"]["schema"] = {"const": NATIVE_POLICY_SCHEMA}
    profile = root["properties"]["profiles"]["additionalProperties"]
    profile["required"] += ["dbt_model_physical_design", "native_execution"]
    properties = profile["properties"]
    properties["source"]["properties"]["type"] = {"const": "mssql"}
    properties["sink"]["properties"]["type"] = {"const": "clickhouse"}
    runtime = properties["runtime"]
    runtime["required"] += ["dbt_profile", "dbt_target", "dbt_threads", "dbt_timeout_seconds"]
    strategy = properties["strategy_policy"]
    strategy["required"] += ["publication_completion_timeout_seconds"]
    strategy["properties"]["publication_completion_timeout_seconds"] = {
        "type": "integer",
        "minimum": 1,
        "maximum": 2147483647,
    }
    strategy["properties"]["allowed_strategies"]["items"] = {"enum": ["full_refresh", "partition_replace"]}
    strategy["properties"]["full_refresh"] = object_schema(
        ("authorized", "serialized_payload_budget"),
        {
            "authorized": {"type": "boolean"},
            "serialized_payload_budget": object_schema(
                ("metric", "max_bytes"),
                {
                    "metric": {"const": "serialized_payload_v1"},
                    "max_bytes": {"type": "integer", "minimum": 1, "maximum": 9223372036854775807},
                },
            ),
        },
    )
    properties["dbt_model_physical_design"] = object_schema(
        ("policy",),
        {
            "policy": {"const": "sqlserver-table-physical-v1"},
            "allowed_layouts": {
                "type": "array",
                "minItems": 1,
                "uniqueItems": True,
                "items": {"enum": list(MODEL_STORAGE_LAYOUTS)},
            },
        },
    )
    properties["authoring_template"] = _authoring_template()
    properties["native_execution"] = native_execution_schema()
    return root


def _authoring_template() -> dict[str, Any]:
    return object_schema(
        ("project_name", "invocation_target", "source_relation"),
        {
            "project_name": IDENTIFIER,
            "invocation_target": object_schema(("database", "schema"), {"database": TOKEN, "schema": TOKEN}),
            "source_relation": object_schema(
                ("database", "schema", "name"),
                {
                    "database": IDENTIFIER,
                    "schema": IDENTIFIER,
                    "name": IDENTIFIER,
                },
            ),
        },
    )


def validate_native_policy_v4(payload: bytes, *, max_bytes: int) -> dict[str, NativeJsonValue]:
    """Validate every byte and complete profile before acquisition uses the policy.

    Error messages expose a structural location and schema keyword, never the
    supplied policy value. V4 rejects floats/bool-as-integer and noncanonical
    documents through the existing bounded native JSON contract.
    """
    if type(max_bytes) is not int or max_bytes <= 0 or type(payload) is not bytes or len(payload) > max_bytes:
        raise ValueError("native policy requires bytes within its exact positive admission bound")
    value = decode_native_delivery_json(payload)
    if encode_native_delivery_json(value) != payload:
        raise ValueError("native policy bytes must be canonical")
    validator = Draft202012Validator(policy_v4_schema())
    error = next(validator.iter_errors(value), None)
    if error is not None:
        location = "/" + "/".join(str(part) for part in error.absolute_path)
        raise ValueError(f"native policy violates {error.validator} at {location}")
    for profile in cast(dict[str, dict[str, Any]], value["profiles"]).values():
        template = profile.get("authoring_template")
        if template is not None:
            DbtInvocationTarget.from_mapping(template["invocation_target"])
        full_refresh = profile["strategy_policy"].get("full_refresh")
        maximum = None if full_refresh is None else full_refresh["serialized_payload_budget"]["max_bytes"]
        validate_native_execution_policy(profile["native_execution"], serialized_payload_max_bytes=maximum)
    return value


def intent_v3_schema() -> dict[str, Any]:
    """Bind the complete selected policy member and explicit managed layout."""
    root = intent_v2_schema()
    root["required"] += ["model_storage", "native_policy_document"]
    properties = root["properties"]
    properties["schema"] = {"const": NATIVE_INTENT_SCHEMA}
    properties["model_storage"] = {"enum": list(MODEL_STORAGE_LAYOUTS)}
    properties["strategy"]["properties"]["mode"] = {"enum": ["auto", "full_refresh", "partition_replace"]}
    properties["native_policy_document"] = object_schema(
        ("path", "sha256", "bytes"),
        {"path": {"type": "string", "minLength": 1}, "sha256": DIGEST, "bytes": {"type": "integer", "minimum": 1}},
    )
    return root


def require_native_document_selection(
    policy: dict[str, Any], *, profile: str, workflow: str, layout: str, strategy_mode: str
) -> None:
    profiles = cast(dict[str, dict[str, Any]], policy["profiles"])
    if profile not in profiles or workflow not in policy["workflows"]:
        raise ValueError("native intent profile or workflow is absent from the selected full policy")
    if strategy_mode != "auto" and strategy_mode not in profiles[profile]["strategy_policy"]["allowed_strategies"]:
        raise ValueError("native strategy is outside the selected platform profile")
    allowed = profiles[profile]["dbt_model_physical_design"].get("allowed_layouts", ["rowstore_none"])
    if layout not in allowed:
        raise ValueError("native model layout is outside the selected platform profile")
