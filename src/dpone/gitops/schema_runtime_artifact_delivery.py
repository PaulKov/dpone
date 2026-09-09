from __future__ import annotations

from typing import Any

from dpone.contracts.runtime_artifact_delivery import (
    ARTIFACT_REGISTRY_LOGICAL_REF_PATTERN,
    CONFIG_MAP_KEY_PATTERN,
    INIT_FETCH_REQUIRED_FIELDS,
    OCI_RUNTIME_IMAGE_REF_PATTERN,
    STRICT_INIT_FETCH_REQUIRED_FIELDS,
    TRUST_TIERS,
)
from dpone.gitops.schema_contract_primitives import artifact_registry_ref_schema
from dpone.kubernetes_names import (
    KUBERNETES_DNS_LABEL_MAX_LENGTH,
    KUBERNETES_DNS_LABEL_PATTERN,
)


def runtime_artifact_delivery_schema(
    *,
    require_mode: bool = True,
    strict_init_fetch: bool = False,
) -> dict[str, Any]:
    """Build either the frozen v1 delivery schema or the strict v2 contract."""

    if strict_init_fetch:
        return strict_init_fetch_delivery_schema()
    schema: dict[str, Any] = {
        "type": "object",
        "additionalProperties": True,
        "properties": {
            "mode": {"enum": ["local_preview", "init_fetch", "shared_pvc", "embedded_bundle", "csi_volume", "inline"]},
            "artifact_registry_ref": artifact_registry_ref_schema(),
            "identity": init_fetch_identity_schema(strict=False),
            "source": init_fetch_source_schema(strict=False),
            "verify": init_fetch_verification_schema(strict=False),
        },
        "allOf": [
            {
                "if": {
                    "properties": {"mode": {"const": "init_fetch"}},
                    "required": ["mode"],
                },
                "then": {
                    "required": list(INIT_FETCH_REQUIRED_FIELDS),
                },
            }
        ],
    }
    if require_mode:
        schema["required"] = ["mode"]
    return schema


def strict_init_fetch_delivery_schema() -> dict[str, Any]:
    """Build the closed executable init-fetch delivery contract."""

    return {
        "type": "object",
        "required": ["mode", *STRICT_INIT_FETCH_REQUIRED_FIELDS],
        "additionalProperties": False,
        "properties": {
            "mode": {"const": "init_fetch"},
            "trust_tier": {"enum": list(TRUST_TIERS)},
            "artifact_registry_ref": artifact_registry_logical_ref_schema(),
            "identity": init_fetch_identity_schema(strict=True),
            "registry_config_ref": config_map_ref_schema(),
            "source": init_fetch_source_schema(strict=True),
            "trust_policy_ref": config_map_ref_schema(),
            "verify": init_fetch_verification_schema(strict=True),
        },
        "allOf": [
            _trust_tier_delivery_guard(
                trust_tier="production",
                attestations="required_for_prod",
                require_trust_policy=True,
            ),
            _trust_tier_delivery_guard(
                trust_tier="non_production",
                attestations="optional",
                require_trust_policy=False,
            ),
        ],
    }


def init_fetch_identity_schema(*, strict: bool) -> dict[str, Any]:
    if not strict:
        return {
            "type": "object",
            "required": ["method", "service_account"],
            "additionalProperties": True,
            "properties": {
                "method": {"enum": ["kubernetes_workload_identity"]},
                "service_account": {"type": "string", "minLength": 1},
            },
        }
    return {
        "type": "object",
        "required": ["method", "service_account", "namespace"],
        "additionalProperties": False,
        "properties": {
            "method": {"enum": ["kubernetes_workload_identity"]},
            "service_account": kubernetes_dns_label_schema(),
            "namespace": kubernetes_dns_label_schema(),
        },
    }


def init_fetch_source_schema(*, strict: bool) -> dict[str, Any]:
    if not strict:
        return {
            "type": "object",
            "required": ["artifact_registry_ref"],
            "additionalProperties": True,
            "properties": {"artifact_registry_ref": artifact_registry_ref_schema()},
        }
    return {
        "type": "object",
        "required": ["artifact_registry_ref"],
        "additionalProperties": False,
        "properties": {
            "artifact_registry_ref": artifact_registry_logical_ref_schema(),
        },
    }


def init_fetch_verification_schema(*, strict: bool) -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["checksums", "attestations"],
        "additionalProperties": not strict,
        "properties": {
            "checksums": {"enum": ["required"]},
            "attestations": {"enum": ["optional", "required_for_prod"]},
        },
    }


def _trust_tier_delivery_guard(
    *,
    trust_tier: str,
    attestations: str,
    require_trust_policy: bool,
) -> dict[str, Any]:
    then: dict[str, Any] = {
        "properties": {
            "verify": {
                "properties": {"attestations": {"const": attestations}},
            }
        }
    }
    if require_trust_policy:
        then["required"] = ["trust_policy_ref"]
    return {
        "if": {
            "properties": {"trust_tier": {"const": trust_tier}},
            "required": ["trust_tier"],
        },
        "then": then,
    }


def artifact_registry_logical_ref_schema() -> dict[str, Any]:
    return {
        "type": "string",
        "pattern": ARTIFACT_REGISTRY_LOGICAL_REF_PATTERN,
        "maxLength": 128,
    }


def config_map_ref_schema() -> dict[str, Any]:
    return {
        "type": "object",
        "required": ["kind", "name", "key", "sha256"],
        "additionalProperties": False,
        "properties": {
            "kind": {"const": "kubernetes_config_map"},
            "name": kubernetes_dns_label_schema(),
            "key": {
                "type": "string",
                "pattern": CONFIG_MAP_KEY_PATTERN,
                "maxLength": 253,
            },
            "sha256": {
                "type": "string",
                "pattern": "^sha256:[0-9a-f]{64}$",
            },
        },
    }


def kubernetes_dns_label_schema() -> dict[str, Any]:
    return {
        "type": "string",
        "pattern": KUBERNETES_DNS_LABEL_PATTERN,
        "maxLength": KUBERNETES_DNS_LABEL_MAX_LENGTH,
    }


def runtime_image_ref_schema(*, nullable: bool = False) -> dict[str, Any]:
    schema: dict[str, Any] = {
        "type": "string",
        "pattern": OCI_RUNTIME_IMAGE_REF_PATTERN,
    }
    if nullable:
        return {"anyOf": [schema, {"type": "null"}]}
    return schema


__all__ = [
    "artifact_registry_logical_ref_schema",
    "config_map_ref_schema",
    "runtime_artifact_delivery_schema",
    "runtime_image_ref_schema",
    "strict_init_fetch_delivery_schema",
]
