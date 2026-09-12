"""Deterministic pod composition for strict v2 indexed ``init_fetch``."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from datetime import timedelta
from typing import Any

from dpone_airflow_pack.composition_supervisor_pod import (
    apply_composition_supervisor,
)
from dpone_airflow_pack.init_fetch_contract import (
    ConfigMapReference,
    InitFetchDeliveryContext,
)
from dpone_airflow_pack.init_fetch_pod_contract import (
    ARTIFACT_ROOT,
    DEV_DBT_EVIDENCE_ROOT,
    DEV_DBT_EVIDENCE_SUBPATH,
    DEV_EVIDENCE_BOOTSTRAP_ROOT,
    DEV_EVIDENCE_BOOTSTRAP_ROOT_ENV,
    DEV_EVIDENCE_VOLUME,
    FETCHED_VOLUME,
    INIT_CONTAINER_NAME,
    PLAN_B64_ENV,
    PLAN_SHA256_ANNOTATION,
    PLAN_SHA256_ENV,
    REGISTRY_CONFIG_DIRECTORY,
    REGISTRY_CONFIG_PATH,
    REGISTRY_CONFIG_VOLUME,
    RUN_OUTPUT_ROOT,
    RUN_OUTPUT_VOLUME,
    TRUST_POLICY_DIRECTORY,
    TRUST_POLICY_PATH,
    TRUST_POLICY_VOLUME,
    WORKTREE_ROOT,
    WORKTREE_VOLUME,
)
from dpone_airflow_pack.init_fetch_pod_guard import (
    provider_env,
    reserved_collision,
    validate_operator_kwargs,
    validate_strict_pack_extensions,
)
from dpone_airflow_pack.provider_execution import (
    RUNTIME_POD_CONTRACT_KEY,
    RUNTIME_POD_CONTRACT_VALUE,
    RUNTIME_POD_MANAGED_BY_KEY,
    RUNTIME_POD_MANAGED_BY_VALUE,
    WORKLOAD_ID_METADATA_KEY,
    require_provider_execution,
)
from dpone_airflow_pack.run_identity import (
    COMPOSITION_SUPERVISOR_B64_ENV,
    encode_composition_supervisor,
)
from dpone_airflow_pack.xcom_sidecar import require_strict_xcom_sidecar_image

_PRESERVED_KPO_FIELDS = frozenset(
    {
        "task_id",
        "name",
        "labels",
        "env_vars",
        "task_group",
        "outlets",
        "params",
        "pool",
        "executor",
        "do_xcom_push",
        "retries",
    }
)
_PRESERVED_POD_SPEC_FIELDS = frozenset({"nodeSelector", "tolerations", "imagePullSecrets"})

REPAIR_AUTHORITY_REF_ENV = "DPONE_REPAIR_AUTHORITY_REF"
REPAIR_AUTHORITY_REF_TEMPLATE = (
    "{% set _refs = (dag_run.conf.get('DPONE_REPAIR_AUTHORITY_REFS') "
    "if dag_run is defined and dag_run else none) %}"
    "{{ (_refs.get(ti.task_id, '') if _refs is mapping else '') "
    "or (dag_run.conf.get('DPONE_REPAIR_AUTHORITY_REF', '') "
    "if dag_run is defined and dag_run else '') }}"
)


def compose_init_fetch_operator_kwargs(
    *,
    pack: Mapping[str, Any],
    kwargs: Mapping[str, Any],
    context: InitFetchDeliveryContext,
    workload_id: str,
    execution_kind: str,
    execution_scope: str,
    hook_execution: str,
    process_selector: str | None = None,
    hook_name: str | None = None,
) -> dict[str, Any]:
    """Replace every executable pack field with the strict trusted contract."""

    validate_strict_pack_extensions(pack)
    require_strict_xcom_sidecar_image(pack)
    projection = require_provider_execution(
        pack,
        expected_workload_id=workload_id,
    )
    effective_kwargs = dict(kwargs)
    if execution_kind != "runtime":
        effective_kwargs["retries"] = 0
    validate_operator_kwargs(
        effective_kwargs,
        retry_authority=projection.retry_authority if execution_kind == "runtime" else None,
    )
    encoded = context.encode_plan(
        workload_id=workload_id,
        execution_kind=execution_kind,
        execution_scope=execution_scope,
        process_selector=process_selector,
        hook_execution=hook_execution,
        hook_name=hook_name,
    )
    env_vars = provider_env(kwargs.get("env_vars"))
    if PLAN_B64_ENV in env_vars or PLAN_SHA256_ENV in env_vars:
        raise reserved_collision("pack cannot provide strict init-fetch plan variables")
    env_vars[PLAN_B64_ENV] = encoded.base64
    env_vars[PLAN_SHA256_ENV] = encoded.sha256
    if REPAIR_AUTHORITY_REF_ENV in env_vars:
        raise reserved_collision("pack cannot provide DPONE_REPAIR_AUTHORITY_REF")
    env_vars[REPAIR_AUTHORITY_REF_ENV] = REPAIR_AUTHORITY_REF_TEMPLATE
    if context.dev_evidence_delivery is not None and workload_id.startswith("dbt__"):
        env_vars["DPONE_DBT_EVIDENCE_EXPORT_ROOT"] = DEV_DBT_EVIDENCE_ROOT
        env_vars["DPONE_DBT_EVIDENCE_SET_ID"] = (
            "{{ dag_run.conf.get('dpone_evidence_authority', {})"
            ".get('evidence_set_id', '') "
            "if dag_run is defined and dag_run else '' }}"
        )

    # Materialize init-fetch before adding the supervisor transport to operator
    # env_vars: only the base process may receive supervisor authority.
    full_pod_spec = _strict_pod(
        projection.pod_spec,
        context=context,
        env_vars=env_vars,
        plan_sha256=encoded.sha256,
        name=str(effective_kwargs.get("name") or effective_kwargs.get("task_id") or "dpone-runtime"),
        labels=_runtime_labels(projection.kpo_kwargs["labels"]),
        workload_id=workload_id,
    )
    if context.composition_supervisor is not None:
        env_vars[COMPOSITION_SUPERVISOR_B64_ENV] = encode_composition_supervisor(
            context.composition_supervisor.to_dict()
        )
    clean = {key: deepcopy(value) for key, value in effective_kwargs.items() if key in _PRESERVED_KPO_FIELDS}
    runtime_labels = _runtime_labels(projection.kpo_kwargs["labels"])
    annotations = {
        PLAN_SHA256_ANNOTATION: encoded.sha256,
        WORKLOAD_ID_METADATA_KEY: workload_id,
    }
    clean.update(
        {
            "retries": effective_kwargs.get("retries", 0),
            "trigger_rule": "all_success",
            # Live-stream `base` (runtime-pack-exec / dbt) into the Airflow task
            # log. KPO get_logs follows the base container only — not init-fetch.
            # dbt live tee / PACK_EXEC / phase markers write to base stderr and
            # require get_logs=True to appear in the UI. PodInitializing 400 is
            # an await-before-follow race; absorb cold dpone-dbt pull+init via
            # startup_timeout_seconds rather than silencing base logs.
            "get_logs": True,
            "startup_timeout_seconds": 900,
            "on_finish_action": "delete_succeeded_pod",
            "do_xcom_push": execution_kind == "runtime",
            "image": context.runtime_image_for_workload(workload_id)[0],
            "cmds": ["dpone", "airflow", "runtime-pack-exec"],
            "arguments": [],
            "namespace": context.identity.namespace,
            "service_account_name": context.identity.service_account,
            "env_vars": env_vars,
            "annotations": annotations,
            "labels": runtime_labels,
            "full_pod_spec": full_pod_spec,
        }
    )
    execution_timeout_seconds = projection.kpo_kwargs.get("execution_timeout_seconds")
    if execution_timeout_seconds is not None:
        clean["execution_timeout"] = timedelta(seconds=execution_timeout_seconds)
    return clean


def _runtime_labels(
    labels: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        **deepcopy(dict(labels)),
        RUNTIME_POD_MANAGED_BY_KEY: RUNTIME_POD_MANAGED_BY_VALUE,
        RUNTIME_POD_CONTRACT_KEY: RUNTIME_POD_CONTRACT_VALUE,
    }


def attach_init_fetch_context(operator: Any, context: InitFetchDeliveryContext) -> Any:
    """Attach the exact same frozen DTO to one materialized Airflow operator."""

    setattr(operator, "_dpone_init_fetch_context", context)
    return operator


def _strict_pod(
    pod_spec: Mapping[str, Any],
    *,
    context: InitFetchDeliveryContext,
    env_vars: Mapping[str, Any],
    plan_sha256: str,
    name: str,
    labels: Mapping[str, Any],
    workload_id: str,
) -> dict[str, Any]:
    raw_spec_value = pod_spec.get("spec")
    raw_spec: Mapping[str, Any] = raw_spec_value if isinstance(raw_spec_value, Mapping) else {}
    spec = {key: deepcopy(value) for key, value in raw_spec.items() if key in _PRESERVED_POD_SPEC_FIELDS}
    resources = _base_resources(raw_spec)
    runtime_image_ref, _runtime_image_digest = context.runtime_image_for_workload(workload_id)
    base = {
        "name": "base",
        "image": runtime_image_ref,
        "command": ["dpone", "airflow", "runtime-pack-exec"],
        "args": [],
        "workingDir": WORKTREE_ROOT,
        "env": _env_list(env_vars),
        "volumeMounts": _base_mounts(
            context,
            include_dev_evidence=workload_id.startswith("dbt__"),
        ),
    }
    if resources is not None:
        base["resources"] = resources
    # Init-fetch uses the same operator env_vars as the base container so
    # non-production S3 registries can authenticate through the default boto3
    # chain (for example templated AWS_* from an Airflow Connection). Plan
    # variables remain authoritative when both are present.
    init = {
        "name": INIT_CONTAINER_NAME,
        "image": runtime_image_ref,
        "command": ["dpone", "airflow", "runtime-init-fetch"],
        "args": [],
        "env": _env_list(
            _init_env_vars(
                env_vars,
                include_dev_evidence=workload_id.startswith("dbt__"),
                context=context,
            )
        ),
        "volumeMounts": _init_mounts(
            context,
            include_dev_evidence=workload_id.startswith("dbt__"),
        ),
    }
    spec.update(
        {
            "restartPolicy": "Never",
            "serviceAccountName": context.identity.service_account,
            "containers": [base],
            "initContainers": [init],
            "volumes": _volumes(
                context,
                include_dev_evidence=workload_id.startswith("dbt__"),
            ),
        }
    )
    metadata: dict[str, Any] = {
        "name": _pod_name(name),
        "namespace": context.identity.namespace,
        "labels": deepcopy(dict(labels)),
        "annotations": {
            PLAN_SHA256_ANNOTATION: plan_sha256,
            WORKLOAD_ID_METADATA_KEY: workload_id,
        },
    }
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": metadata,
        "spec": spec,
    }
    if context.composition_supervisor is not None:
        apply_composition_supervisor(pod, context.composition_supervisor)
    return pod


def _volumes(
    context: InitFetchDeliveryContext,
    *,
    include_dev_evidence: bool,
) -> list[dict[str, Any]]:
    volumes = [
        {"name": FETCHED_VOLUME, "emptyDir": {}},
        {"name": WORKTREE_VOLUME, "emptyDir": {}},
        {"name": RUN_OUTPUT_VOLUME, "emptyDir": {}},
        _config_map_volume(
            name=REGISTRY_CONFIG_VOLUME,
            reference=context.registry_configuration,
            file_name="registry.json",
        ),
    ]
    if context.trust_policy is not None:
        volumes.append(
            {
                "name": TRUST_POLICY_VOLUME,
                "configMap": {"name": context.trust_policy.name},
            }
        )
    if include_dev_evidence and context.dev_evidence_delivery is not None:
        volumes.append(
            {
                "name": DEV_EVIDENCE_VOLUME,
                "persistentVolumeClaim": {
                    "claimName": context.dev_evidence_delivery.claim_name,
                },
            }
        )
    return volumes


def _config_map_volume(
    *,
    name: str,
    reference: ConfigMapReference,
    file_name: str,
) -> dict[str, Any]:
    return {
        "name": name,
        "configMap": {
            "name": reference.name,
            "items": [{"key": reference.key, "path": file_name}],
        },
    }


def _base_mounts(
    context: InitFetchDeliveryContext,
    *,
    include_dev_evidence: bool,
) -> list[dict[str, Any]]:
    mounts = [
        {"name": FETCHED_VOLUME, "mountPath": ARTIFACT_ROOT, "readOnly": True},
        {"name": WORKTREE_VOLUME, "mountPath": WORKTREE_ROOT, "readOnly": True},
        {"name": RUN_OUTPUT_VOLUME, "mountPath": RUN_OUTPUT_ROOT, "readOnly": False},
    ]
    if include_dev_evidence and context.dev_evidence_delivery is not None:
        mounts.append(
            {
                "name": DEV_EVIDENCE_VOLUME,
                "mountPath": DEV_DBT_EVIDENCE_ROOT,
                "readOnly": False,
                "subPath": DEV_DBT_EVIDENCE_SUBPATH,
            }
        )
    return mounts


def _init_mounts(
    context: InitFetchDeliveryContext,
    *,
    include_dev_evidence: bool,
) -> list[dict[str, Any]]:
    mounts = [
        {"name": FETCHED_VOLUME, "mountPath": ARTIFACT_ROOT, "readOnly": False},
        {"name": WORKTREE_VOLUME, "mountPath": WORKTREE_ROOT, "readOnly": False},
        {
            "name": REGISTRY_CONFIG_VOLUME,
            "mountPath": REGISTRY_CONFIG_DIRECTORY,
            "readOnly": True,
        },
    ]
    if context.trust_policy is not None:
        mounts.append(
            {
                "name": TRUST_POLICY_VOLUME,
                "mountPath": TRUST_POLICY_DIRECTORY,
                "readOnly": True,
            }
        )
    if include_dev_evidence and context.dev_evidence_delivery is not None:
        mounts.append(
            {
                "name": DEV_EVIDENCE_VOLUME,
                "mountPath": DEV_EVIDENCE_BOOTSTRAP_ROOT,
                "readOnly": False,
            }
        )
    return mounts


def _init_env_vars(
    env_vars: Mapping[str, Any],
    *,
    include_dev_evidence: bool,
    context: InitFetchDeliveryContext,
) -> dict[str, Any]:
    values = dict(env_vars)
    if include_dev_evidence and context.dev_evidence_delivery is not None:
        values[DEV_EVIDENCE_BOOTSTRAP_ROOT_ENV] = DEV_EVIDENCE_BOOTSTRAP_ROOT
    return values


def _base_resources(spec: Mapping[str, Any]) -> Any | None:
    containers = spec.get("containers")
    if not isinstance(containers, list) or len(containers) != 1:
        return None
    container = containers[0]
    if not isinstance(container, Mapping) or "resources" not in container:
        return None
    resources = container["resources"]
    if not isinstance(resources, Mapping):
        raise reserved_collision("pack base resources must be an object")
    return deepcopy(dict(resources))


def _env_list(values: Mapping[str, Any]) -> list[dict[str, str]]:
    return [{"name": name, "value": str(value)} for name, value in sorted(values.items())]


def _pod_name(value: str) -> str:
    normalized = value.strip().lower().replace("_", "-")
    return normalized[:63].strip("-") or "dpone-runtime"


__all__ = [
    "ARTIFACT_ROOT",
    "apply_composition_supervisor",
    "attach_init_fetch_context",
    "compose_init_fetch_operator_kwargs",
    "PLAN_B64_ENV",
    "PLAN_SHA256_ENV",
    "REGISTRY_CONFIG_PATH",
    "REPAIR_AUTHORITY_REF_ENV",
    "REPAIR_AUTHORITY_REF_TEMPLATE",
    "RUN_OUTPUT_ROOT",
    "TRUST_POLICY_PATH",
    "WORKTREE_ROOT",
]
