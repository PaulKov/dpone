"""Build strict Airflow workload packs for one locked dbt workflow."""

from __future__ import annotations

import json
import shlex
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack.pack_identity import (
    PACK_IDENTITY_SCHEMA,
    compute_pack_fingerprint,
)
from dpone_airflow_pack.provider_execution_contract import (
    WORKLOAD_ID_METADATA_KEY,
    kubernetes_label_value,
    kubernetes_pod_name,
)

from dpone.contracts.dbt_publishing import DbtExecutionPack, DbtPublishingError
from dpone.gitops.airflow_compact_pack_bootstrap import (
    DEFAULT_PROCESS_BOOTSTRAP_KEY,
    WORKLOAD_BOOTSTRAP_KEY,
    RuntimePayloadBuilder,
)
from dpone.gitops.airflow_compact_process_plans import DEFAULT_PROCESS_PLAN_KEY
from dpone.gitops.airflow_compact_runtime import compact_provider_execution
from dpone.gitops.airflow_interval_env import airflow_interval_env_vars
from dpone.manifest.confined_files import read_confined_file
from dpone.readiness.dbt_semantic_refresh_airflow_pack import (
    ActivatedSemanticRefreshPack,
    SemanticRefreshTemplateProofAuthority,
    semantic_refresh_activated_pack,
    semantic_refresh_template_pack,
    validate_semantic_refresh_activation_readiness,
)

DBT_EXECUTION_PACK_PATH = "runtime/dbt-execution-pack.json"
_MAX_TEMPLATE_BYTES = 8 * 1024 * 1024


class DbtAirflowExecutionPackBuilder:
    """Project a dbt execution contract into the closed init-fetch pack wire."""

    def build(
        self,
        *,
        workflow_id: str,
        execution_pack: DbtExecutionPack,
        runtime_payload_ids: tuple[str, ...],
        xcom_sidecar_image: str,
        pool: str,
        pod_spec: Mapping[str, Any] | None = None,
        executor: str | None = None,
    ) -> dict[str, Any]:
        workload_id = f"dbt__{workflow_id}"
        task_id = f"{workload_id}__dpone_runtime"
        name = kubernetes_pod_name(workload_id)
        labels = {
            "app.kubernetes.io/component": "dbt-runner",
            WORKLOAD_ID_METADATA_KEY: kubernetes_label_value(workload_id),
        }
        env_vars = airflow_interval_env_vars()
        kpo_kwargs: dict[str, Any] = {
            "task_id": task_id,
            "name": name,
            "labels": labels,
            "env_vars": env_vars,
            "pool": _airflow_pool(pool),
            "execution_timeout_seconds": (
                execution_pack.adapter_runtime.airflow_execution_timeout_seconds(
                    execution_pack.timeout_seconds,
                )
            ),
        }
        normalized_executor = str(executor or "").strip()
        if normalized_executor:
            # Keep the dbt build task on the same declared placement as the
            # workflow's transfer tasks; without this the strict wire falls
            # back to the deployment default executor.
            kpo_kwargs["executor"] = normalized_executor
        runtime_argv = [
            "dpone",
            "dbt",
            "execute-pack",
            DBT_EXECUTION_PACK_PATH,
            "--format",
            "json",
        ]
        runtime_payload = RuntimePayloadBuilder(
            repo_root=Path("."),
            paths=(),
            generated_files={DBT_EXECUTION_PACK_PATH: _canonical_json_bytes(execution_pack.to_dict())},
        ).build()

        def bootstrap_command() -> dict[str, Any]:
            return {
                "argv": list(runtime_argv),
                "env": {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"},
            }

        payload: dict[str, Any] = {
            "kind": "gitops.airflow_pack",
            "schema_version": "3",
            "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
            "producer": "dpone dbt compile",
            "airflow": {"execution": {}},
            "steps": [],
            "runtime_command": "",
            "runtime_payload_ids": list(runtime_payload_ids),
            "runtime_payload": runtime_payload.to_jsonable(),
            "runtime_bootstrap": {
                "schema": "dpone.airflow-runtime-bootstrap.v1",
                "commands": {
                    workload_id: bootstrap_command(),
                    WORKLOAD_BOOTSTRAP_KEY: bootstrap_command(),
                    # A dbt DAG-spec node carries no selector, so the strict
                    # provider requests scope=process without a selector and
                    # the launcher resolves ``__default_process__`` — mirror
                    # the whole-workflow command exactly like the legacy
                    # ``dpone run`` flavor does for selectorless processes.
                    DEFAULT_PROCESS_BOOTSTRAP_KEY: bootstrap_command(),
                },
            },
            # One selectorless plan mirroring the implicit whole-workflow
            # process. The launcher requires this inventory for every
            # scope=process execution; ``dag_node`` mirrors the legacy
            # preview node so DAG-spec previews stay byte-identical.
            # ``runtime_selection`` stays absent on purpose: the strict
            # provider must keep projecting the whole pack for the
            # selectorless dbt node instead of the process-plan view.
            "process_plans": {
                DEFAULT_PROCESS_PLAN_KEY: {
                    "selector": None,
                    "dag_node": {
                        "process_name": workflow_id,
                        "visibility": "task",
                        "task_group": None,
                        "estimated_visible_tasks": 2,
                        "depends_on_process_selectors": [],
                    },
                    "runtime_commands": {
                        "inline": shlex.join(runtime_argv),
                        "expanded": shlex.join(runtime_argv),
                    },
                    "steps": [],
                },
            },
            "provider_execution": compact_provider_execution(
                kpo_kwargs=kpo_kwargs,
                pod_spec=pod_spec,
            ),
            # Compatibility readers may still inspect kpo_kwargs, but the
            # strict provider never trusts executable fields from this view.
            "kpo_kwargs": {
                **kpo_kwargs,
                "cmds": ["dpone"],
                "arguments": runtime_argv[1:],
            },
            "connection_projection": {},
            "xcom": {"sidecar_image": _exact_image(xcom_sidecar_image)},
            "workload": {
                "workload_id": workload_id,
                "manifest": DBT_EXECUTION_PACK_PATH,
                "effective_config": {"authoring": {"mode": "dbt"}},
            },
        }
        payload["pack_fingerprint"] = compute_pack_fingerprint(payload)
        return payload


def strict_transfer_pack(
    pack: Mapping[str, Any],
    *,
    xcom_sidecar_image: str,
) -> dict[str, Any]:
    """Close scheduler authority on a normal generated transfer pack."""

    rewritten = dict(pack)
    raw_airflow = rewritten.get("airflow")
    airflow = dict(raw_airflow) if isinstance(raw_airflow, Mapping) else {}
    raw_execution = airflow.get("execution")
    execution = dict(raw_execution) if isinstance(raw_execution, Mapping) else {}
    airflow["execution"] = {key: execution[key] for key in ("inlets", "outlets") if key in execution}
    rewritten["airflow"] = airflow
    rewritten["connection_projection"] = {}
    rewritten["xcom"] = {"sidecar_image": _exact_image(xcom_sidecar_image)}
    rewritten.pop("pack_fingerprint", None)
    rewritten["pack_fingerprint"] = compute_pack_fingerprint(rewritten)
    return rewritten


def reject_non_executable_semantic_template(
    relative_pack_path: str,
    *,
    runtime_root: Path | None = None,
) -> None:
    """Reject a release-only V2 template before generic dbt execution."""

    root = (runtime_root or Path.cwd()).absolute()
    try:
        payload = json.loads(
            read_confined_file(
                root,
                relative_pack_path,
                max_bytes=_MAX_TEMPLATE_BYTES,
            )
        )
    except Exception:
        return
    if not isinstance(payload, dict):
        return
    semantic = payload.get("semantic_refresh")
    semantic_mode = semantic.get("mode") if isinstance(semantic, dict) else None
    if (
        payload.get("activation") == "POST_DEPLOYMENT_AUTHORITY_REQUIRED"
        or semantic_mode == "semantic_refresh_v2_template"
    ):
        raise DbtPublishingError(
            "DPONE_DBT_V2_TEMPLATE_NOT_EXECUTABLE",
            "semantic-refresh release templates require protected post-deployment activation",
        )


def _exact_image(value: str) -> str:
    normalized = str(value or "").strip()
    digest = normalized.rpartition("@sha256:")[2]
    if not digest or len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        raise ValueError("xcom_sidecar_image must be an exact digest-pinned OCI reference")
    return normalized


def _airflow_pool(value: str) -> str:
    normalized = str(value or "").strip()
    if not normalized or len(normalized) > 256:
        raise ValueError("Airflow pool must contain 1..256 non-whitespace characters")
    if any(ord(character) < 33 or ord(character) == 127 for character in normalized):
        raise ValueError("Airflow pool must not contain whitespace or control characters")
    return normalized


def _canonical_json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(payload, allow_nan=False, ensure_ascii=False, separators=(",", ":"), sort_keys=True) + "\n"
    ).encode("utf-8")


__all__ = [
    "ActivatedSemanticRefreshPack",
    "DBT_EXECUTION_PACK_PATH",
    "DbtAirflowExecutionPackBuilder",
    "SemanticRefreshTemplateProofAuthority",
    "semantic_refresh_activated_pack",
    "semantic_refresh_template_pack",
    "reject_non_executable_semantic_template",
    "strict_transfer_pack",
    "validate_semantic_refresh_activation_readiness",
]
