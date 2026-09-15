"""Canonical generated native intent and policy member identities."""

from __future__ import annotations

from typing import Any, cast

from jsonschema import Draft202012Validator

from dpone.contracts.dbt_publish_models import DbtPublishIntent
from dpone.contracts.dbt_publish_schema_contract_intent import intent_v2_schema
from dpone.contracts.dbt_publish_schema_contract_v4 import NATIVE_INTENT_SCHEMA, intent_v3_schema
from dpone.contracts.native_delivery import NativePolicyDocumentRef
from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json

NATIVE_POLICY_MEMBER = "dpone/native-policy.json"
NATIVE_INTENT_MEMBER = "dpone/native-intent.json"


def encode_native_project_intent(intent: object, policy_document: object, *, model_storage: str) -> bytes:
    """Generate a v3 member from a validated v2 normalized intent without mutation."""

    if type(intent) is not DbtPublishIntent or type(policy_document) is not NativePolicyDocumentRef:
        raise ValueError("native intent requires exact normalized intent and policy descriptor")
    policy_document.__post_init__()
    payload = intent.to_jsonable()
    if not Draft202012Validator(intent_v2_schema()).is_valid(payload) or not intent.enabled:
        raise ValueError("native project requires one enabled, valid normalized intent")
    payload.update(
        schema=NATIVE_INTENT_SCHEMA, model_storage=model_storage, native_policy_document=policy_document.to_dict()
    )
    if not Draft202012Validator(intent_v3_schema()).is_valid(payload):
        raise ValueError("native project intent violates its closed v3 contract")
    return encode_native_delivery_json(payload)


def decode_native_project_intent(payload: bytes, *, max_bytes: int) -> dict[str, Any]:
    """Read canonical v3 bytes and semantically validate the full-policy reference."""

    if type(max_bytes) is not int or max_bytes <= 0 or type(payload) is not bytes or len(payload) > max_bytes:
        raise ValueError("native intent exceeds its explicit metadata bound")
    value = decode_native_delivery_json(payload)
    if encode_native_delivery_json(value) != payload or not Draft202012Validator(intent_v3_schema()).is_valid(value):
        raise ValueError("native intent must be canonical and match the closed v3 schema")
    descriptor = value["native_policy_document"]
    assert isinstance(descriptor, dict)
    NativePolicyDocumentRef(**cast(dict[str, Any], descriptor))
    if value["enabled"] is not True:
        raise ValueError("native project intent must enable exactly one published model")
    return value
