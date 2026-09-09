"""Runtime command and KPO projection for one compact Airflow pack."""

from __future__ import annotations

import shlex
from collections.abc import Mapping
from copy import deepcopy
from typing import TYPE_CHECKING, Any

from dpone_airflow_pack.provider_execution_contract import (
    WORKLOAD_ID_METADATA_KEY,
    kubernetes_label_value,
    kubernetes_pod_name,
)

from dpone.gitops.airflow_interval_env import airflow_interval_env_vars

if TYPE_CHECKING:
    from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
    from dpone.manifest.runtime_materialization import RuntimeManifestMaterialization

PROVIDER_EXECUTION_SCHEMA = "dpone.airflow-provider-execution.v1"
_PROVIDER_KPO_FIELDS = (
    "task_id",
    "name",
    "labels",
    "env_vars",
    "pool",
    "executor",
    "execution_timeout_seconds",
)
_PROVIDER_POD_SPEC_FIELDS = (
    "nodeSelector",
    "tolerations",
    "imagePullSecrets",
)


def compact_kpo_kwargs(
    workload: GitOpsWorkloadDefinition,
    *,
    runtime_manifest: RuntimeManifestMaterialization,
) -> dict[str, Any]:
    """Return scheduler-static KPO kwargs targeting canonical runtime IR."""

    config = workload.effective_config
    kwargs: dict[str, Any] = {
        "task_id": f"{workload.workload_id}__dpone_runtime",
        "name": kubernetes_pod_name(workload.workload_id),
        "namespace": config.get("namespace", "default"),
        "image": config.get("image", "<IMAGE>"),
        "cmds": ["dpone"],
        "arguments": ["run", runtime_manifest.path, "--format", "json"],
        "labels": {
            "app.kubernetes.io/component": "dpone-runner",
            WORKLOAD_ID_METADATA_KEY: kubernetes_label_value(workload.workload_id),
        },
        "env_vars": airflow_interval_env_vars(),
    }
    airflow_config = config.get("airflow")
    execution = airflow_config.get("execution") if isinstance(airflow_config, Mapping) else {}
    execution = execution if isinstance(execution, Mapping) else {}
    # Coerce exactly like the legacy apply_execution_policy path so a YAML
    # scalar is never silently dropped from the strict wire only.
    task_executor = str(execution.get("task_executor") or "").strip()
    if task_executor:
        # The workload-set execution policy is the scheduler-placement
        # authority; without this projection strict init_fetch tasks fall
        # back to the deployment default executor (Celery).
        kwargs["executor"] = task_executor
    return kwargs


def compact_provider_execution(
    *,
    kpo_kwargs: Mapping[str, Any],
    pod_spec: Mapping[str, Any] | None,
    retry_authority: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Project only bounded scheduler extensions into the strict provider wire."""

    projected_kwargs = {key: deepcopy(kpo_kwargs[key]) for key in _PROVIDER_KPO_FIELDS if key in kpo_kwargs}
    raw_spec = pod_spec.get("spec") if isinstance(pod_spec, Mapping) else None
    spec = raw_spec if isinstance(raw_spec, Mapping) else {}
    projected_spec = {key: deepcopy(spec[key]) for key in _PROVIDER_POD_SPEC_FIELDS if key in spec}
    base: dict[str, Any] = {"name": "base"}
    containers = spec.get("containers")
    if isinstance(containers, list) and containers and isinstance(containers[0], Mapping):
        resources = containers[0].get("resources")
        if isinstance(resources, Mapping):
            base["resources"] = deepcopy(dict(resources))
    projected_spec["containers"] = [base]
    projection = {
        "schema": PROVIDER_EXECUTION_SCHEMA,
        "kpo_kwargs": projected_kwargs,
        "pod_spec": {"spec": projected_spec},
    }
    if retry_authority is not None:
        projection["retry_authority"] = deepcopy(dict(retry_authority))
    return projection


def compact_runtime_command(
    *,
    workload_id: str,
    manifest: str,
    selector: str | None = None,
    skip_separate_hooks: bool = True,
) -> str:
    """Build the fail-closed runtime/evidence shell contract."""

    xcom_path = "/airflow/xcom/return.json"
    evidence_dir = f".dpone/runs/{workload_id}"
    evidence_path = f".dpone/runs/{workload_id}/runtime-evidence.json"
    stderr_path = f".dpone/runs/{workload_id}/runtime-stderr.log"
    quoted_manifest = shlex.quote(manifest)
    quoted_evidence_path = shlex.quote(evidence_path)
    quoted_stderr_path = shlex.quote(stderr_path)
    quoted_xcom_path = shlex.quote(xcom_path)
    run_parts: tuple[str, ...] = ("dpone", "run", quoted_manifest, "--format", "json")
    if selector is not None:
        run_parts = (*run_parts, "--selector", shlex.quote(selector))
    run_command = " ".join(run_parts)
    hook_policy = "DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1 " if skip_separate_hooks else ""
    xcom_command = " ".join(
        (
            "dpone gitops airflow xcom-from-evidence",
            "--evidence-path",
            quoted_evidence_path,
            "--runtime-evidence-path",
            quoted_evidence_path,
            "--stderr-path",
            quoted_stderr_path,
            "--xcom-output",
            quoted_xcom_path,
            "--status",
            '"$dpone_status"',
        )
    )
    fallback_xcom = (
        "printf "
        + shlex.quote(
            '{"kind":"gitops.airflow_xcom_summary","schema_version":"1",'
            '"producer":"dpone compact airflow runtime",'
            '"status":"failed","runtime_evidence_path":"'
            + evidence_path
            + '","artifact_paths":{"runtime_evidence":"'
            + evidence_path
            + '"},"blockers":[{"code":"xcom_summary_builder_failed",'
            '"message":"Runtime evidence could not be converted to Airflow XCom summary",'
            '"path":"' + evidence_path + '","source":"dpone compact airflow runtime"}]}'
        )
        + f" > {quoted_xcom_path}"
    )
    return (
        f"status=0; mkdir -p {shlex.quote(evidence_dir)}; "
        f"{hook_policy}{run_command} > {quoted_evidence_path} 2> {quoted_stderr_path} || status=$?; "
        f"if [ -s {quoted_stderr_path} ]; then cat {quoted_stderr_path} >&2; fi; "
        "mkdir -p /airflow/xcom; "
        'dpone_status="passed"; if [ "$status" -ne 0 ]; then dpone_status="failed"; fi; '
        f"{xcom_command} || {fallback_xcom}; exit 0"
    )


__all__ = [
    "PROVIDER_EXECUTION_SCHEMA",
    "compact_kpo_kwargs",
    "compact_provider_execution",
    "compact_runtime_command",
]
