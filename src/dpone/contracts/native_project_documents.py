"""Canonical generated native intent and policy member identities."""

from __future__ import annotations

from hashlib import sha256
from typing import Any, cast

from jsonschema import Draft202012Validator

from dpone.contracts.dbt_project_bundle import DbtProjectBundle
from dpone.contracts.dbt_publish_models import DbtPublishIntent
from dpone.contracts.dbt_publish_schema_contract_intent import intent_v2_schema
from dpone.contracts.dbt_publish_schema_contract_v4 import (
    NATIVE_INTENT_SCHEMA,
    intent_v3_schema,
    require_native_document_selection,
    validate_native_policy_v4,
)
from dpone.contracts.native_delivery import NativePolicyDocumentRef
from dpone.contracts.native_delivery_json import decode_native_delivery_json, encode_native_delivery_json
from dpone.contracts.native_policy_document import verify_native_policy_member

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


def generate_native_project_intent(
    policy: bytes,
    intent: DbtPublishIntent,
    *,
    model_storage: str,
    max_bytes: int,
) -> bytes:
    """Validate the full policy and selection before producing bounded v3 bytes.

    This pure operation must complete before archive capture or file writes.
    The caller retains the unchanged policy bytes as the generated policy member.
    """
    validated = validate_native_policy_v4(policy, max_bytes=max_bytes)
    if type(intent) is not DbtPublishIntent:
        raise ValueError("native document production requires an exact normalized intent")
    require_native_document_selection(
        validated,
        profile=intent.profile,
        workflow=intent.workflow,
        layout=model_storage,
        strategy_mode=intent.strategy_mode,
    )
    descriptor = NativePolicyDocumentRef(NATIVE_POLICY_MEMBER, "sha256:" + sha256(policy).hexdigest(), len(policy))
    intent_bytes = encode_native_project_intent(intent, descriptor, model_storage=model_storage)
    if len(intent_bytes) > max_bytes:
        raise ValueError("generated native intent exceeds the metadata bound")
    return intent_bytes


def validate_native_project_policy(
    policy: bytes,
    intent: dict[str, Any],
    descriptor: NativePolicyDocumentRef,
    bundle: DbtProjectBundle,
    *,
    max_bytes: int,
) -> None:
    """Validate captured policy identity and the decoded intent's full selection.

    The file adapter supplies confined member bytes and a verified inventory;
    this function grants no filesystem or archive authenticity on its own.
    """
    verify_native_policy_member(policy, descriptor, bundle, max_bytes=max_bytes)
    validated = validate_native_policy_v4(policy, max_bytes=max_bytes)
    require_native_document_selection(
        validated,
        profile=intent["profile"],
        workflow=intent["workflow"],
        layout=intent["model_storage"],
        strategy_mode=intent["strategy"]["mode"],
    )
