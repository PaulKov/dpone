"""Closed scheduler-side execution projection for strict indexed packs."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from dpone_airflow_pack.init_fetch_contract import InitFetchProviderError
from dpone_airflow_pack.kubernetes_resources import KubernetesResourceError, validate_kubernetes_resources
from dpone_airflow_pack.provider_execution_contract import (
    BASE_CONTAINER_FIELDS,
    IMAGE_PULL_SECRET_FIELDS,
    KPO_FIELDS,
    KPO_REQUIRED_FIELDS,
    MAX_EXECUTION_TIMEOUT_SECONDS,
    MAX_IMAGE_PULL_SECRETS,
    MAX_NODE_SELECTOR_ENTRIES,
    MAX_TOLERATIONS,
    POD_FIELDS,
    POD_SPEC_FIELDS,
    PROJECTION_FIELDS,
    PROJECTION_REQUIRED_FIELDS,
    PROVIDER_EXECUTION_SCHEMA,
    RUNTIME_POD_CONTRACT_KEY,
    RUNTIME_POD_CONTRACT_VALUE,
    RUNTIME_POD_MANAGED_BY_KEY,
    RUNTIME_POD_MANAGED_BY_VALUE,
    TOLERATION_EFFECTS,
    TOLERATION_FIELDS,
    WORKLOAD_ID_METADATA_KEY,
    kubernetes_label_value,
    kubernetes_pod_name,
)
from dpone_airflow_pack.provider_retry_authority import (
    ProviderRetryAuthority,
    validated_provider_retry_authority,
)


@dataclass(frozen=True, slots=True)
class ProviderExecutionProjection:
    """One validated, non-executable set of scheduler extensions."""

    workload_id: str
    kpo_kwargs: dict[str, Any]
    pod_spec: dict[str, Any]
    retry_authority: ProviderRetryAuthority | None


def require_provider_execution(
    pack: Mapping[str, Any],
    *,
    expected_workload_id: str | None = None,
) -> ProviderExecutionProjection:
    """Read and validate the only strict scheduler-authority projection."""

    workload = _mapping(pack.get("workload"), field="workload")
    workload_id = _require_nonempty_string(
        workload.get("workload_id"),
        field="workload.workload_id",
    )
    if expected_workload_id is not None and workload_id != expected_workload_id:
        raise _invalid("workload.workload_id does not match the indexed or DAG-spec workload id")

    raw = pack.get("provider_execution")
    if raw is None:
        raise InitFetchProviderError(
            "DPONE_INIT_FETCH_PACK_MIGRATION_REQUIRED",
            "strict init-fetch requires a workload pack rebuilt with provider_execution v1",
        )
    projection = _mapping(raw, field="provider_execution")
    _require_exact_fields(
        projection,
        allowed=PROJECTION_FIELDS,
        required=PROJECTION_REQUIRED_FIELDS,
    )
    if projection["schema"] != PROVIDER_EXECUTION_SCHEMA:
        raise _invalid("provider_execution.schema is unsupported")

    kpo_kwargs = _mapping(projection["kpo_kwargs"], field="provider_execution.kpo_kwargs")
    _require_exact_fields(
        kpo_kwargs,
        allowed=KPO_FIELDS,
        required=KPO_REQUIRED_FIELDS,
    )
    task_id = _require_nonempty_string(
        kpo_kwargs["task_id"],
        field="provider_execution.kpo_kwargs.task_id",
    )
    name = _require_nonempty_string(
        kpo_kwargs["name"],
        field="provider_execution.kpo_kwargs.name",
    )
    if task_id != f"{workload_id}__dpone_runtime":
        raise _invalid("provider_execution.kpo_kwargs.task_id does not match workload.workload_id")
    if name != kubernetes_pod_name(workload_id):
        raise _invalid("provider_execution.kpo_kwargs.name does not match workload.workload_id")
    labels = _string_mapping(
        kpo_kwargs["labels"],
        field="provider_execution.kpo_kwargs.labels",
    )
    if labels.get(WORKLOAD_ID_METADATA_KEY) != kubernetes_label_value(workload_id):
        raise _invalid(
            "provider_execution.kpo_kwargs.labels[dpone.dev/workload-id] does not match workload.workload_id"
        )
    _mapping(kpo_kwargs["env_vars"], field="provider_execution.kpo_kwargs.env_vars")
    _string_mapping(
        kpo_kwargs["env_vars"],
        field="provider_execution.kpo_kwargs.env_vars",
    )
    if "pool" in kpo_kwargs:
        _bounded_string(
            kpo_kwargs["pool"],
            field="provider_execution.kpo_kwargs.pool",
            max_length=256,
        )
    if "executor" in kpo_kwargs:
        _bounded_string(
            kpo_kwargs["executor"],
            field="provider_execution.kpo_kwargs.executor",
            max_length=253,
        )
    execution_timeout_seconds = None
    if "execution_timeout_seconds" in kpo_kwargs:
        execution_timeout_seconds = _bounded_positive_integer(
            kpo_kwargs["execution_timeout_seconds"],
            field="provider_execution.kpo_kwargs.execution_timeout_seconds",
            max_value=MAX_EXECUTION_TIMEOUT_SECONDS,
        )

    pod_spec = _mapping(projection["pod_spec"], field="provider_execution.pod_spec")
    _require_exact_fields(pod_spec, allowed=POD_FIELDS, required=POD_FIELDS)
    spec = _mapping(pod_spec["spec"], field="provider_execution.pod_spec.spec")
    _require_exact_fields(spec, allowed=POD_SPEC_FIELDS, required=frozenset({"containers"}))
    _validate_scheduling_fields(spec)
    _validate_image_pull_secrets(spec.get("imagePullSecrets"))
    _validate_base_container(spec["containers"])
    validated_kpo_kwargs = deepcopy(dict(kpo_kwargs))
    if execution_timeout_seconds is not None:
        validated_kpo_kwargs["execution_timeout_seconds"] = execution_timeout_seconds
    return ProviderExecutionProjection(
        workload_id=workload_id,
        kpo_kwargs=validated_kpo_kwargs,
        pod_spec=deepcopy(dict(pod_spec)),
        retry_authority=validated_provider_retry_authority(projection.get("retry_authority")),
    )


def _validate_scheduling_fields(spec: Mapping[str, Any]) -> None:
    node_selector = spec.get("nodeSelector")
    if node_selector is not None:
        _bounded_string_mapping(
            node_selector,
            field="provider_execution.pod_spec.spec.nodeSelector",
            max_entries=MAX_NODE_SELECTOR_ENTRIES,
            max_key_length=317,
            max_value_length=63,
        )
    tolerations = spec.get("tolerations")
    if tolerations is None:
        return
    if not isinstance(tolerations, list):
        raise _invalid("provider_execution.pod_spec.spec.tolerations must be a list")
    if len(tolerations) > MAX_TOLERATIONS:
        raise _invalid("provider_execution.pod_spec.spec.tolerations exceeds the 32-item limit")
    for index, raw in enumerate(tolerations):
        field = f"provider_execution.pod_spec.spec.tolerations[{index}]"
        toleration = _mapping(raw, field=field)
        _require_exact_fields(
            toleration,
            allowed=TOLERATION_FIELDS,
            required=TOLERATION_FIELDS,
        )
        _bounded_string(toleration["key"], field=f"{field}.key", max_length=317)
        operator = _bounded_string(
            toleration["operator"],
            field=f"{field}.operator",
            max_length=16,
        )
        if operator != "Equal":
            raise _invalid(f"{field}.operator must be Equal")
        _bounded_string(
            toleration["value"],
            field=f"{field}.value",
            max_length=63,
            allow_empty=True,
        )
        effect = _bounded_string(
            toleration["effect"],
            field=f"{field}.effect",
            max_length=32,
        )
        if effect not in TOLERATION_EFFECTS:
            raise _invalid(f"{field}.effect is unsupported")


def _validate_image_pull_secrets(value: object) -> None:
    if value is None:
        return
    if not isinstance(value, list):
        raise _invalid("provider_execution.pod_spec.spec.imagePullSecrets must be a list")
    if len(value) > MAX_IMAGE_PULL_SECRETS:
        raise _invalid("provider_execution.pod_spec.spec.imagePullSecrets exceeds the 8-item limit")
    for index, raw in enumerate(value):
        field = f"provider_execution.pod_spec.spec.imagePullSecrets[{index}]"
        secret = _mapping(raw, field=field)
        _require_exact_fields(
            secret,
            allowed=IMAGE_PULL_SECRET_FIELDS,
            required=IMAGE_PULL_SECRET_FIELDS,
        )
        _bounded_string(secret["name"], field=f"{field}.name", max_length=253)


def _validate_base_container(value: object) -> None:
    if not isinstance(value, list) or len(value) != 1:
        raise _collision("provider_execution must contain exactly one resource-only base container")
    container = _mapping(value[0], field="provider_execution.pod_spec.spec.containers[0]")
    _require_exact_fields(
        container,
        allowed=BASE_CONTAINER_FIELDS,
        required=frozenset({"name"}),
    )
    if container["name"] != "base":
        raise _collision("provider_execution container name must be base")
    resources = container.get("resources")
    if "resources" in container:
        _validate_resources(resources)


def _validate_resources(value: object) -> None:
    field = "provider_execution.pod_spec.spec.containers[0].resources"
    try:
        validate_kubernetes_resources(value, field=field, allow_extended=True)
    except KubernetesResourceError as exc:
        raise _invalid(str(exc)) from exc


def _require_exact_fields(
    value: Mapping[str, Any],
    *,
    allowed: frozenset[str],
    required: frozenset[str],
) -> None:
    unknown = sorted(str(key) for key in value if str(key) not in allowed)
    if unknown:
        raise _collision("provider_execution contains reserved or unknown fields: " + ", ".join(unknown))
    missing = sorted(required.difference(str(key) for key in value))
    if missing:
        raise _invalid("provider_execution is missing required fields: " + ", ".join(missing))


def _mapping(value: object, *, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _invalid(f"{field} must be an object")
    return value


def _string_mapping(value: object, *, field: str) -> Mapping[str, Any]:
    mapping = _mapping(value, field=field)
    if any(not isinstance(key, str) or not isinstance(item, str) for key, item in mapping.items()):
        raise _invalid(f"{field} keys and values must be strings")
    return mapping


def _bounded_string_mapping(
    value: object,
    *,
    field: str,
    max_entries: int,
    max_key_length: int,
    max_value_length: int,
) -> None:
    mapping = _mapping(value, field=field)
    if len(mapping) > max_entries:
        raise _invalid(f"{field} exceeds the {max_entries}-item limit")
    for key, item in mapping.items():
        _bounded_string(key, field=f"{field} key", max_length=max_key_length)
        _bounded_string(
            item,
            field=f"{field}.{key}",
            max_length=max_value_length,
        )


def _require_nonempty_string(value: object, *, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _invalid(f"{field} must be a non-empty string")
    return value


def _bounded_string(
    value: object,
    *,
    field: str,
    max_length: int,
    allow_empty: bool = False,
) -> str:
    if not isinstance(value, str):
        raise _invalid(f"{field} must be a string")
    if (not allow_empty and not value) or len(value) > max_length:
        qualifier = "bounded" if allow_empty else "non-empty bounded"
        raise _invalid(f"{field} must be a {qualifier} string")
    if value != value.strip() or any(ord(character) < 32 for character in value):
        raise _invalid(f"{field} contains unsupported whitespace or control characters")
    return value


def _bounded_positive_integer(
    value: object,
    *,
    field: str,
    max_value: int,
) -> int:
    if isinstance(value, bool):
        raise _invalid(f"{field} must be an integer")
    if isinstance(value, float):
        if not value.is_integer():
            raise _invalid(f"{field} must be an integer")
        value = int(value)
    if not isinstance(value, int):
        raise _invalid(f"{field} must be an integer")
    if value < 1 or value > max_value:
        raise _invalid(f"{field} must be between 1 and {max_value}")
    return value


def _invalid(message: str) -> InitFetchProviderError:
    return InitFetchProviderError(
        "DPONE_INIT_FETCH_PROVIDER_EXECUTION_INVALID",
        message,
    )


def _collision(message: str) -> InitFetchProviderError:
    return InitFetchProviderError(
        "DPONE_INIT_FETCH_RESERVED_COLLISION",
        message,
    )


__all__ = [
    "PROVIDER_EXECUTION_SCHEMA",
    "ProviderExecutionProjection",
    "ProviderRetryAuthority",
    "RUNTIME_POD_CONTRACT_KEY",
    "RUNTIME_POD_CONTRACT_VALUE",
    "RUNTIME_POD_MANAGED_BY_KEY",
    "RUNTIME_POD_MANAGED_BY_VALUE",
    "WORKLOAD_ID_METADATA_KEY",
    "require_provider_execution",
]
