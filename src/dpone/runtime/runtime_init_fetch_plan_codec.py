"""Bounded decoder for the untrusted indexed-KPO runtime plan."""

from __future__ import annotations

import base64
import binascii
import hashlib
import json
from collections.abc import Mapping
from typing import Any

from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import (
    MAX_RUNTIME_INIT_FETCH_PLAN_BYTES,
    RUNTIME_INIT_FETCH_PLAN_SCHEMA,
    RUNTIME_INIT_FETCH_PLAN_SCHEMA_V2,
    RUNTIME_INIT_FETCH_PLAN_SCHEMA_V3,
    RuntimeArtifactDescriptor,
    RuntimeExecutionSelection,
    RuntimeInitFetchPlan,
    RuntimePayloadDescriptor,
    RuntimeWorkloadPackRef,
    canonical_runtime_init_fetch_plan_bytes,
)

_ROOT_KEYS = frozenset(
    {
        "schema",
        "environment",
        "trust_tier",
        "release_id",
        "deployment_id",
        "runtime_image",
        "registry",
        "trust_policy",
        "identity",
        "release",
        "deployment",
        "binding_set",
        "connection_registry",
        "credential_runtime",
        "workload_pack",
        "execution",
        "verify",
    }
)
_ROOT_KEYS_V2 = _ROOT_KEYS | {"runtime_payloads"}
_ROOT_KEYS_V3 = _ROOT_KEYS_V2


def decode_runtime_init_fetch_plan(
    encoded_plan: str,
    expected_sha256: str,
    *,
    max_bytes: int = MAX_RUNTIME_INIT_FETCH_PLAN_BYTES,
) -> tuple[RuntimeInitFetchPlan, str]:
    """Decode and validate an untrusted provider plan before any external I/O."""

    if not is_canonical_sha256_digest(expected_sha256):
        raise _plan_error(
            "DPONE_INIT_FETCH_PLAN_INVALID",
            "DPONE_INIT_FETCH_PLAN_SHA256 must be a canonical sha256 digest",
        )
    if not isinstance(encoded_plan, str) or not encoded_plan:
        raise _plan_error("DPONE_INIT_FETCH_PLAN_INVALID", "runtime init-fetch plan is missing")
    try:
        payload = base64.b64decode(encoded_plan, validate=True)
    except (ValueError, binascii.Error) as exc:
        raise _plan_error("DPONE_INIT_FETCH_PLAN_INVALID", "runtime init-fetch plan is not valid base64") from exc
    if len(payload) > _positive_integer(max_bytes, "max_bytes"):
        raise _plan_error("DPONE_INIT_FETCH_PLAN_TOO_LARGE", "runtime init-fetch plan exceeds the byte limit")
    actual_sha256 = "sha256:" + hashlib.sha256(payload).hexdigest()
    if actual_sha256 != expected_sha256:
        raise _plan_error("DPONE_INIT_FETCH_PLAN_HASH_MISMATCH", "runtime init-fetch plan hash does not match")
    try:
        raw = json.loads(payload.decode("utf-8"), object_pairs_hook=_unique_object)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise _plan_error("DPONE_INIT_FETCH_PLAN_INVALID", "runtime init-fetch plan JSON is invalid") from exc
    if not isinstance(raw, Mapping):
        raise _plan_error("DPONE_INIT_FETCH_PLAN_INVALID", "runtime init-fetch plan must be an object")
    try:
        plan = _plan_from_mapping(raw)
    except (TypeError, ValueError) as exc:
        raise _plan_error("DPONE_INIT_FETCH_PLAN_INVALID", str(exc)) from exc
    if canonical_runtime_init_fetch_plan_bytes(plan) != payload:
        raise _plan_error("DPONE_INIT_FETCH_PLAN_NON_CANONICAL", "runtime init-fetch plan is not canonical JSON")
    return plan, actual_sha256


def _plan_from_mapping(raw: Mapping[str, Any]) -> RuntimeInitFetchPlan:
    schema = raw.get("schema")
    if schema == RUNTIME_INIT_FETCH_PLAN_SCHEMA:
        _require_keys("plan", raw, _ROOT_KEYS)
    elif schema == RUNTIME_INIT_FETCH_PLAN_SCHEMA_V2:
        _require_keys("plan", raw, _ROOT_KEYS_V2)
    elif schema == RUNTIME_INIT_FETCH_PLAN_SCHEMA_V3:
        _require_keys("plan", raw, _ROOT_KEYS_V3)
    else:
        raise ValueError("runtime init-fetch plan schema is unsupported")
    runtime_image = _mapping(raw["runtime_image"], "runtime_image", {"ref", "digest"})
    registry = _mapping(raw["registry"], "registry", {"logical_ref", "configuration"})
    return RuntimeInitFetchPlan(
        environment=_text(raw["environment"], "environment"),
        trust_tier=_text(raw["trust_tier"], "trust_tier"),
        release_id=_text(raw["release_id"], "release_id"),
        deployment_id=_text(raw["deployment_id"], "deployment_id"),
        runtime_image_ref=_text(runtime_image["ref"], "runtime_image.ref"),
        runtime_image_digest=_text(runtime_image["digest"], "runtime_image.digest"),
        artifact_registry_ref=_text(registry["logical_ref"], "registry.logical_ref"),
        registry_config_ref=_mapping(
            registry["configuration"],
            "registry.configuration",
            {"kind", "name", "key", "sha256"},
        ),
        trust_policy_ref=(
            _mapping(
                raw["trust_policy"],
                "trust_policy",
                {"kind", "name", "key", "sha256"},
            )
            if raw["trust_policy"] is not None
            else None
        ),
        identity=_mapping(
            raw["identity"],
            "identity",
            {"method", "service_account", "namespace"},
        ),
        release=_artifact(raw["release"], "release"),
        deployment=_artifact(raw["deployment"], "deployment"),
        binding_set=_artifact(raw["binding_set"], "binding_set"),
        connection_registry=_artifact(
            raw["connection_registry"],
            "connection_registry",
        ),
        credential_runtime=_artifact(
            raw["credential_runtime"],
            "credential_runtime",
        ),
        workload_pack=_workload_pack(raw["workload_pack"]),
        execution=_execution(
            raw["execution"],
            explicit_hook_execution=schema == RUNTIME_INIT_FETCH_PLAN_SCHEMA_V3,
        ),
        verify=_mapping(raw["verify"], "verify", {"checksums", "attestations"}),
        runtime_payloads=(
            _runtime_payloads(
                raw["runtime_payloads"],
                allow_empty=schema == RUNTIME_INIT_FETCH_PLAN_SCHEMA_V3,
            )
            if schema in {RUNTIME_INIT_FETCH_PLAN_SCHEMA_V2, RUNTIME_INIT_FETCH_PLAN_SCHEMA_V3}
            else ()
        ),
    )


def _artifact(value: object, field: str) -> RuntimeArtifactDescriptor:
    item = _mapping(value, field, {"artifact_ref", "sha256", "bytes"})
    return RuntimeArtifactDescriptor(
        artifact_ref=_text(item["artifact_ref"], f"{field}.artifact_ref"),
        sha256=_text(item["sha256"], f"{field}.sha256"),
        bytes=_positive_integer(item["bytes"], f"{field}.bytes"),
    )


def _workload_pack(value: object) -> RuntimeWorkloadPackRef:
    item = _mapping(
        value,
        "workload_pack",
        {"id", "artifact_ref", "sha256", "bytes", "pack_fingerprint"},
    )
    return RuntimeWorkloadPackRef(
        id=_text(item["id"], "workload_pack.id"),
        artifact_ref=_text(item["artifact_ref"], "workload_pack.artifact_ref"),
        sha256=_text(item["sha256"], "workload_pack.sha256"),
        bytes=_positive_integer(item["bytes"], "workload_pack.bytes"),
        pack_fingerprint=_text(item["pack_fingerprint"], "workload_pack.pack_fingerprint"),
    )


def _runtime_payloads(
    value: object,
    *,
    allow_empty: bool,
) -> tuple[RuntimePayloadDescriptor, ...]:
    if not isinstance(value, list) or (not value and not allow_empty) or len(value) > 16:
        minimum = 0 if allow_empty else 1
        raise ValueError(f"runtime_payloads must contain between {minimum} and 16 entries")
    result = []
    for index, raw in enumerate(value):
        field = f"runtime_payloads[{index}]"
        item = _mapping(
            raw,
            field,
            {"id", "kind", "artifact_ref", "sha256", "bytes", "media_type"},
        )
        result.append(
            RuntimePayloadDescriptor(
                id=_text(item["id"], f"{field}.id"),
                kind=_text(item["kind"], f"{field}.kind"),
                artifact_ref=_text(
                    item["artifact_ref"],
                    f"{field}.artifact_ref",
                ),
                sha256=_text(item["sha256"], f"{field}.sha256"),
                bytes=_positive_integer(item["bytes"], f"{field}.bytes"),
                media_type=_text(item["media_type"], f"{field}.media_type"),
            )
        )
    return tuple(result)


def _execution(
    value: object,
    *,
    explicit_hook_execution: bool,
) -> RuntimeExecutionSelection:
    keys = {"kind", "selector", "hook_name"}
    if explicit_hook_execution:
        keys |= {"scope", "process_selector", "hook_execution"}
    item = _mapping(value, "execution", keys)
    hook_name = item["hook_name"]
    if hook_name is not None and not isinstance(hook_name, str):
        raise ValueError("execution.hook_name must be a string or null")
    return RuntimeExecutionSelection(
        kind=_text(item["kind"], "execution.kind"),
        selector=_text(item["selector"], "execution.selector"),
        scope=(_text(item["scope"], "execution.scope") if explicit_hook_execution else None),
        process_selector=(
            _nullable_text(item["process_selector"], "execution.process_selector") if explicit_hook_execution else None
        ),
        hook_name=hook_name,
        hook_execution=(_text(item["hook_execution"], "execution.hook_execution") if explicit_hook_execution else None),
    )


def _mapping(value: object, field: str, keys: set[str]) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    _require_keys(field, value, frozenset(keys))
    return value


def _require_keys(field: str, value: Mapping[str, Any], keys: frozenset[str]) -> None:
    actual = frozenset(str(key) for key in value)
    if actual != keys:
        raise ValueError(f"{field} must contain exactly {sorted(keys)}")


def _text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _nullable_text(value: object, field: str) -> str | None:
    if value is None:
        return None
    return _text(value, field)


def _positive_integer(value: object, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _unique_object(items: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in items:
        if key in result:
            raise ValueError("runtime init-fetch plan contains duplicate JSON keys")
        result[key] = value
    return result


def _plan_error(code: str, message: str) -> InitFetchError:
    return InitFetchError(code, message)


__all__ = ["decode_runtime_init_fetch_plan"]
