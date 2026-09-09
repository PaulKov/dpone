"""Dependency-light structural authority for strict provider execution."""

from __future__ import annotations

import hashlib
import re

PROVIDER_EXECUTION_SCHEMA = "dpone.airflow-provider-execution.v1"
RETRY_AUTHORITY_SCHEMA = "dpone.airflow-retry-authority.v1"
RETRY_AUTHORITY_MODE = "postgres_xmin_initial_mssql_target_atomic_v1"
MAX_CERTIFIED_TASK_RETRIES = 3
WORKLOAD_ID_METADATA_KEY = "dpone.dev/workload-id"
RUNTIME_POD_MANAGED_BY_KEY = "dpone.dev/managed-by"
RUNTIME_POD_MANAGED_BY_VALUE = "airflow-provider"
RUNTIME_POD_CONTRACT_KEY = "dpone.dev/runtime-contract"
RUNTIME_POD_CONTRACT_VALUE = "init-fetch-v2"
RUNTIME_POD_LABEL_SELECTOR = (
    f"{RUNTIME_POD_MANAGED_BY_KEY}={RUNTIME_POD_MANAGED_BY_VALUE},"
    f"{RUNTIME_POD_CONTRACT_KEY}={RUNTIME_POD_CONTRACT_VALUE},"
    f"{WORKLOAD_ID_METADATA_KEY}"
)
PROVIDER_OWNED_RUNTIME_LABEL_KEYS = frozenset({RUNTIME_POD_MANAGED_BY_KEY, RUNTIME_POD_CONTRACT_KEY})

PROJECTION_REQUIRED_FIELDS = frozenset({"schema", "kpo_kwargs", "pod_spec"})
PROJECTION_FIELDS = PROJECTION_REQUIRED_FIELDS | {"retry_authority"}
RETRY_AUTHORITY_FIELDS = frozenset({"schema", "mode", "max_task_retries"})
KPO_REQUIRED_FIELDS = frozenset({"task_id", "name", "labels", "env_vars"})
KPO_FIELDS = KPO_REQUIRED_FIELDS | {"pool", "execution_timeout_seconds", "executor"}
POD_FIELDS = frozenset({"spec"})
POD_SPEC_FIELDS = frozenset({"nodeSelector", "tolerations", "containers", "imagePullSecrets"})
BASE_CONTAINER_FIELDS = frozenset({"name", "resources"})
RESOURCE_FIELDS = frozenset({"limits", "requests"})
TOLERATION_FIELDS = frozenset({"effect", "key", "operator", "value"})
IMAGE_PULL_SECRET_FIELDS = frozenset({"name"})
TOLERATION_EFFECTS = frozenset({"NoExecute", "NoSchedule", "PreferNoSchedule"})

MAX_NODE_SELECTOR_ENTRIES = 64
MAX_RESOURCE_ENTRIES = 64
MAX_TOLERATIONS = 32
MAX_IMAGE_PULL_SECRETS = 8
MAX_EXECUTION_TIMEOUT_SECONDS = 86_700

_KUBERNETES_LABEL_VALUE = re.compile(r"[A-Za-z0-9](?:[A-Za-z0-9_.-]{0,61}[A-Za-z0-9])?\Z")
_KUBERNETES_DNS_LABEL = re.compile(r"[a-z0-9](?:[a-z0-9.-]{0,61}[a-z0-9])?\Z")
_TRIMMED_TEXT_PATTERN = r"^[^\u0000-\u0020](?:[^\u0000-\u001f]*[^\u0000-\u0020])?$"
_OPTIONAL_TRIMMED_TEXT_PATTERN = r"^(?:|[^\u0000-\u0020](?:[^\u0000-\u001f]*[^\u0000-\u0020])?)$"
_IDENTITY_DIGEST_LENGTH = 16


def provider_execution_json_schema() -> dict[str, object]:
    """Return a fresh JSON Schema derived from the executable limits."""

    string_map: dict[str, object] = {
        "type": "object",
        "additionalProperties": {"type": "string"},
    }
    labels = {**string_map, "required": [WORKLOAD_ID_METADATA_KEY]}
    bounded_resource_map = {
        "type": "object",
        "minProperties": 1,
        "maxProperties": MAX_RESOURCE_ENTRIES,
        "propertyNames": {
            "type": "string",
            "minLength": 1,
            "maxLength": 253,
            "pattern": _TRIMMED_TEXT_PATTERN,
        },
        "additionalProperties": {
            "type": "string",
            "minLength": 1,
            "maxLength": 64,
            "pattern": _TRIMMED_TEXT_PATTERN,
        },
    }
    resources = {
        "type": "object",
        "minProperties": 1,
        "additionalProperties": False,
        "properties": {
            "limits": bounded_resource_map,
            "requests": bounded_resource_map,
        },
    }
    toleration = {
        "type": "object",
        "required": sorted(TOLERATION_FIELDS),
        "additionalProperties": False,
        "properties": {
            "key": _bounded_text_schema(317),
            "operator": {"const": "Equal"},
            "value": _bounded_text_schema(63, allow_empty=True),
            "effect": {"enum": sorted(TOLERATION_EFFECTS)},
        },
    }
    base_container = {
        "type": "object",
        "required": ["name"],
        "additionalProperties": False,
        "properties": {
            "name": {"const": "base"},
            "resources": resources,
        },
    }
    pod_spec = {
        "type": "object",
        "required": ["spec"],
        "additionalProperties": False,
        "properties": {
            "spec": {
                "type": "object",
                "required": ["containers"],
                "additionalProperties": False,
                "properties": {
                    "nodeSelector": {
                        "type": "object",
                        "maxProperties": MAX_NODE_SELECTOR_ENTRIES,
                        "propertyNames": {
                            "type": "string",
                            "minLength": 1,
                            "maxLength": 317,
                            "pattern": _TRIMMED_TEXT_PATTERN,
                        },
                        "additionalProperties": _bounded_text_schema(63),
                    },
                    "tolerations": {
                        "type": "array",
                        "maxItems": MAX_TOLERATIONS,
                        "items": toleration,
                    },
                    "imagePullSecrets": {
                        "type": "array",
                        "maxItems": MAX_IMAGE_PULL_SECRETS,
                        "items": {
                            "type": "object",
                            "required": ["name"],
                            "additionalProperties": False,
                            "properties": {
                                "name": {
                                    "type": "string",
                                    "minLength": 1,
                                    "maxLength": 253,
                                    "pattern": _TRIMMED_TEXT_PATTERN,
                                }
                            },
                        },
                    },
                    "containers": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 1,
                        "items": base_container,
                    },
                },
            }
        },
    }
    return {
        "type": "object",
        "required": ["schema", "kpo_kwargs", "pod_spec"],
        "additionalProperties": False,
        "properties": {
            "schema": {"const": PROVIDER_EXECUTION_SCHEMA},
            "retry_authority": {
                "type": "object",
                "required": sorted(RETRY_AUTHORITY_FIELDS),
                "additionalProperties": False,
                "properties": {
                    "schema": {"const": RETRY_AUTHORITY_SCHEMA},
                    "mode": {"const": RETRY_AUTHORITY_MODE},
                    "max_task_retries": {"const": MAX_CERTIFIED_TASK_RETRIES},
                },
            },
            "kpo_kwargs": {
                "type": "object",
                "required": ["task_id", "name", "labels", "env_vars"],
                "additionalProperties": False,
                "properties": {
                    "task_id": {"type": "string", "minLength": 1},
                    "name": {"type": "string", "minLength": 1},
                    "labels": labels,
                    "env_vars": string_map,
                    "pool": _bounded_text_schema(256),
                    "executor": _bounded_text_schema(253),
                    "execution_timeout_seconds": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": MAX_EXECUTION_TIMEOUT_SECONDS,
                    },
                },
            },
            "pod_spec": pod_spec,
        },
    }


def kubernetes_label_value(logical_id: str) -> str:
    """Project one logical identity into a stable Kubernetes label value."""

    if _KUBERNETES_LABEL_VALUE.fullmatch(logical_id):
        return logical_id
    return _digest_projection(logical_id, allowed_separators="._-")


def kubernetes_pod_name(workload_id: str) -> str:
    """Project one workload identity into the existing dpone pod-name lane."""

    candidate = f"dpone-{workload_id.replace('_', '-').lower()}"
    if _KUBERNETES_DNS_LABEL.fullmatch(candidate):
        return candidate
    return _digest_projection(candidate, allowed_separators=".-")


def _digest_projection(value: str, *, allowed_separators: str) -> str:
    normalized = re.sub(
        rf"[^A-Za-z0-9{re.escape(allowed_separators)}]+",
        "-",
        value,
    ).strip(allowed_separators)
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()[:_IDENTITY_DIGEST_LENGTH]
    max_prefix = 63 - len(digest) - 1
    prefix = normalized[:max_prefix].rstrip(allowed_separators) or "workload"
    return f"{prefix}-{digest}".lower()


def _bounded_text_schema(max_length: int, *, allow_empty: bool = False) -> dict[str, object]:
    return {
        "type": "string",
        "minLength": 0 if allow_empty else 1,
        "maxLength": max_length,
        "pattern": _OPTIONAL_TRIMMED_TEXT_PATTERN if allow_empty else _TRIMMED_TEXT_PATTERN,
    }


__all__ = [
    "IMAGE_PULL_SECRET_FIELDS",
    "KPO_REQUIRED_FIELDS",
    "MAX_CERTIFIED_TASK_RETRIES",
    "MAX_EXECUTION_TIMEOUT_SECONDS",
    "MAX_IMAGE_PULL_SECRETS",
    "POD_SPEC_FIELDS",
    "PROVIDER_EXECUTION_SCHEMA",
    "PROVIDER_OWNED_RUNTIME_LABEL_KEYS",
    "RUNTIME_POD_CONTRACT_KEY",
    "RUNTIME_POD_CONTRACT_VALUE",
    "RUNTIME_POD_LABEL_SELECTOR",
    "RUNTIME_POD_MANAGED_BY_KEY",
    "RUNTIME_POD_MANAGED_BY_VALUE",
    "RETRY_AUTHORITY_MODE",
    "RETRY_AUTHORITY_SCHEMA",
    "WORKLOAD_ID_METADATA_KEY",
    "kubernetes_label_value",
    "kubernetes_pod_name",
    "provider_execution_json_schema",
]
