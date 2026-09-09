"""Run-neutral, parse-safe semantic-refresh DAG projection authority."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass

from dpone_airflow_pack.pack_identity import verify_pack_fingerprint
from dpone_airflow_pack.semantic_refresh_dag_authority import (
    SemanticRefreshDagProjectionAuthority,
    SemanticRefreshDagProjectionIdentity,
)
from dpone_airflow_pack.semantic_refresh_topology import (
    SemanticRefreshDagPolicy,
    SemanticRefreshTopologyTemplate,
)

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_SCHEMA = "dpone.semantic-refresh-v2-dag-projection.v1"
_FIELDS = {
    "dag_projection_sha256",
    "deployment_id",
    "operation_ids_by_model",
    "package_artifacts_sha256",
    "plan_bundle",
    "plan_bundle_sha256",
    "pre_release_bundle_sha256",
    "release_id",
    "schema",
    "task_projection",
    "template_pack",
    "template_pack_fingerprint",
    "topology",
    "topology_sha256",
    "workflow_plan_sha256",
}


class SemanticRefreshProjectionError(ValueError):
    """Raised when static DAG or durable runtime projection is invalid."""


@dataclass(frozen=True)
class SemanticRefreshTask:
    """One framework-neutral task in a static semantic-refresh DAG."""

    task_id: str
    task_kind: str
    upstream_task_ids: tuple[str, ...]
    trigger_rule: str = "all_success"
    operation_id: str | None = None
    model_unique_id: str | None = None

    def __post_init__(self) -> None:
        has_operation = self.operation_id is not None
        if has_operation != (self.model_unique_id is not None):
            raise SemanticRefreshProjectionError("task model and operation identities must be bound together")
        if self.operation_id is not None:
            _digest(self.operation_id, "operation_id")
        if self.model_unique_id is not None and not self.model_unique_id.strip():
            raise SemanticRefreshProjectionError("task model_unique_id must be non-empty")

    def to_mapping(self) -> dict[str, object]:
        return {
            "model_unique_id": self.model_unique_id,
            "operation_id": self.operation_id,
            "task_id": self.task_id,
            "task_kind": self.task_kind,
            "trigger_rule": self.trigger_rule,
            "upstream_task_ids": list(self.upstream_task_ids),
        }


@dataclass(frozen=True)
class SemanticRefreshDagTaskProjection:
    """Static graph whose execution identity is resolved only inside workers."""

    topology_sha256: str
    dag_id: str
    asset_uris: tuple[str, ...]
    dbt_task: SemanticRefreshTask
    prepare_tasks: tuple[SemanticRefreshTask, ...]
    commit_tasks: tuple[SemanticRefreshTask, ...]
    summary_task: SemanticRefreshTask

    def to_mapping(self) -> dict[str, object]:
        return {
            "asset_uris": list(self.asset_uris),
            "commit_tasks": [task.to_mapping() for task in self.commit_tasks],
            "dag_id": self.dag_id,
            "dbt_task": self.dbt_task.to_mapping(),
            "prepare_tasks": [task.to_mapping() for task in self.prepare_tasks],
            "schema": "dpone.semantic-refresh-v2-dag-task-projection.v1",
            "summary_task": self.summary_task.to_mapping(),
            "topology_sha256": self.topology_sha256,
        }


@dataclass(frozen=True, slots=True)
class AuthenticatedSemanticRefreshDagProjection:
    """Validated deployment sidecar consumed by the Airflow materializer."""

    identity: SemanticRefreshDagProjectionIdentity
    workflow_name: str
    dag_policy: SemanticRefreshDagPolicy
    projection: SemanticRefreshDagTaskProjection
    topology: Mapping[str, object]
    template_pack: Mapping[str, object]
    plan_bundle: Mapping[str, object]
    dbt_execution_pack: Mapping[str, object]
    project_config_overlay: Mapping[str, object]
    profile_sha256: str

    def descriptor(self) -> dict[str, object]:
        """Return index metadata for exact local path/byte binding by the root writer."""

        content = self.canonical_bytes()
        return {
            "artifact_bytes": len(content),
            "artifact_sha256": "sha256:" + hashlib.sha256(content).hexdigest(),
            "dag_id": self.projection.dag_id,
            "dag_projection_sha256": self.identity.dag_projection_sha256,
            "projection_id": f"semantic_refresh_v2::{self.projection.dag_id}",
            "workflow_name": self.workflow_name,
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.to_mapping())

    def to_mapping(self) -> dict[str, object]:
        unsigned = {
            "deployment_id": self.identity.deployment_id,
            "operation_ids_by_model": {
                task.model_unique_id: task.operation_id for task in self.projection.prepare_tasks
            },
            "package_artifacts_sha256": self.identity.package_artifacts_sha256,
            "plan_bundle": dict(self.plan_bundle),
            "plan_bundle_sha256": self.identity.plan_bundle_sha256,
            "pre_release_bundle_sha256": self.identity.pre_release_bundle_sha256,
            "release_id": self.identity.release_id,
            "schema": _SCHEMA,
            "task_projection": self.projection.to_mapping(),
            "template_pack": dict(self.template_pack),
            "template_pack_fingerprint": self.identity.template_pack_fingerprint,
            "topology": dict(self.topology),
            "topology_sha256": self.identity.topology_sha256,
            "workflow_plan_sha256": self.identity.workflow_plan_sha256,
        }
        return {**unsigned, "dag_projection_sha256": self.identity.dag_projection_sha256}


def build_semantic_refresh_dag_task_projection(
    *, topology: Mapping[str, object], operation_ids_by_model: Mapping[str, str]
) -> SemanticRefreshDagTaskProjection:
    """Build deterministic static tasks without a logical DagRun identity."""

    protected = SemanticRefreshTopologyTemplate.from_mapping(topology)
    models = protected.model_unique_ids
    if not isinstance(operation_ids_by_model, Mapping) or set(operation_ids_by_model) != set(models):
        raise SemanticRefreshProjectionError("operation closure has missing or extra topology models")
    operation_ids = tuple(operation_ids_by_model[model] for model in models)
    if len(set(operation_ids)) != len(operation_ids):
        raise SemanticRefreshProjectionError("model operation IDs must be unique")
    for operation_id in operation_ids:
        _digest(operation_id, "operation_id")
    slugs = tuple(_task_slug(model) for model in models)
    if len(set(slugs)) != len(slugs):
        raise SemanticRefreshProjectionError("model operation IDs collide as Airflow task IDs")
    dbt = SemanticRefreshTask("semantic_refresh__dbt_build_test", "DBT_BUILD_TEST_GATE", ())
    prepares = tuple(
        SemanticRefreshTask(
            f"semantic_refresh__prepare__{slug}",
            "CLICKHOUSE_PREPARE",
            (dbt.task_id,),
            operation_id=operation_id,
            model_unique_id=model,
        )
        for model, operation_id, slug in zip(models, operation_ids, slugs, strict=True)
    )
    commits: list[SemanticRefreshTask] = []
    prepare_ids = tuple(task.task_id for task in prepares)
    for ordinal, (model, operation_id, slug) in enumerate(zip(models, operation_ids, slugs, strict=True)):
        commits.append(
            SemanticRefreshTask(
                f"semantic_refresh__commit__{ordinal:04d}__{slug}",
                "CLICKHOUSE_COMMIT",
                prepare_ids if ordinal == 0 else (commits[-1].task_id,),
                operation_id=operation_id,
                model_unique_id=model,
            )
        )
    summary = SemanticRefreshTask(
        "semantic_refresh__durable_workflow_summary",
        "DURABLE_WORKFLOW_SUMMARY",
        (commits[-1].task_id,),
        trigger_rule="all_done",
    )
    return SemanticRefreshDagTaskProjection(
        topology_sha256=protected.topology_sha256,
        dag_id=protected.dag_id,
        asset_uris=tuple(protected.logical_output_asset_uris[model] for model in models),
        dbt_task=dbt,
        prepare_tasks=prepares,
        commit_tasks=tuple(commits),
        summary_task=summary,
    )


def authenticate_semantic_refresh_dag_projection(
    payload: Mapping[str, object], *, authority: SemanticRefreshDagProjectionAuthority
) -> AuthenticatedSemanticRefreshDagProjection:
    authenticated = validate_semantic_refresh_dag_projection(payload)
    authority.assert_authorized(authenticated.identity)
    return authenticated


def validate_semantic_refresh_dag_projection(
    payload: Mapping[str, object],
) -> AuthenticatedSemanticRefreshDagProjection:
    """Recompute every closed sidecar identity without I/O."""

    if not isinstance(payload, Mapping) or set(payload) != _FIELDS or payload.get("schema") != _SCHEMA:
        raise SemanticRefreshProjectionError("semantic-refresh DAG projection fields are not closed")
    supplied = _digest(payload.get("dag_projection_sha256"), "dag_projection_sha256")
    unsigned = {key: value for key, value in payload.items() if key != "dag_projection_sha256"}
    if _canonical_digest(unsigned) != supplied:
        raise SemanticRefreshProjectionError("semantic-refresh DAG projection digest differs")
    topology = _mapping(payload.get("topology"), "topology")
    protected = SemanticRefreshTopologyTemplate.from_mapping(topology)
    if protected.topology_sha256 != payload.get("topology_sha256"):
        raise SemanticRefreshProjectionError("semantic-refresh DAG topology identity differs")
    template, template_fingerprint, dbt_execution_pack = _validated_template(payload, topology)
    plan, plan_operations = _validated_plan(payload)
    operations = _mapping(payload.get("operation_ids_by_model"), "operation_ids_by_model")
    if dict(operations) != dict(plan_operations):
        raise SemanticRefreshProjectionError("semantic-refresh operation closure differs")
    projection = build_semantic_refresh_dag_task_projection(
        topology=topology,
        operation_ids_by_model={str(key): str(value) for key, value in operations.items()},
    )
    if projection.to_mapping() != payload.get("task_projection"):
        raise SemanticRefreshProjectionError("semantic-refresh DAG task projection differs")
    identity = SemanticRefreshDagProjectionIdentity(
        dag_projection_sha256=supplied,
        release_id=_digest(payload.get("release_id"), "release_id"),
        deployment_id=_digest(payload.get("deployment_id"), "deployment_id"),
        topology_sha256=protected.topology_sha256,
        template_pack_fingerprint=template_fingerprint,
        plan_bundle_sha256=_digest(payload.get("plan_bundle_sha256"), "plan_bundle_sha256"),
        workflow_plan_sha256=_digest(payload.get("workflow_plan_sha256"), "workflow_plan_sha256"),
        pre_release_bundle_sha256=_digest(payload.get("pre_release_bundle_sha256"), "pre_release_bundle_sha256"),
        package_artifacts_sha256=_digest(payload.get("package_artifacts_sha256"), "package_artifacts_sha256"),
    )
    return AuthenticatedSemanticRefreshDagProjection(
        identity=identity,
        workflow_name=protected.workflow_name,
        dag_policy=protected.dag_policy,
        projection=projection,
        topology=dict(topology),
        template_pack=dict(template),
        plan_bundle=dict(plan),
        dbt_execution_pack=dict(dbt_execution_pack),
        project_config_overlay=dict(protected.project_config_overlay),
        profile_sha256=protected.profile_sha256,
    )


def build_semantic_refresh_dag_projection(
    *,
    template_pack: Mapping[str, object],
    plan_bundle: Mapping[str, object],
) -> AuthenticatedSemanticRefreshDagProjection:
    """Create then independently validate canonical controller-side bytes."""

    semantic = _mapping(template_pack.get("semantic_refresh"), "template semantic_refresh")
    topology = _mapping(semantic.get("topology"), "template topology")
    authority = _mapping(plan_bundle.get("release_deployment_authority"), "release deployment authority")
    workflow = _mapping(plan_bundle.get("workflow_plan"), "workflow plan")
    operation_ids_by_model = _operation_mapping(plan_bundle)
    projection = build_semantic_refresh_dag_task_projection(
        topology=topology, operation_ids_by_model=operation_ids_by_model
    )
    unsigned: dict[str, object] = {
        "deployment_id": authority.get("deployment_id"),
        "operation_ids_by_model": dict(operation_ids_by_model),
        "package_artifacts_sha256": plan_bundle.get("package_artifacts_sha256"),
        "plan_bundle": dict(plan_bundle),
        "plan_bundle_sha256": plan_bundle.get("plan_bundle_sha256"),
        "pre_release_bundle_sha256": plan_bundle.get("pre_release_bundle_sha256"),
        "release_id": authority.get("release_id"),
        "schema": _SCHEMA,
        "task_projection": projection.to_mapping(),
        "template_pack": dict(template_pack),
        "template_pack_fingerprint": template_pack.get("pack_fingerprint"),
        "topology": dict(topology),
        "topology_sha256": topology.get("topology_sha256"),
        "workflow_plan_sha256": workflow.get("workflow_plan_sha256"),
    }
    return validate_semantic_refresh_dag_projection({**unsigned, "dag_projection_sha256": _canonical_digest(unsigned)})


def _validated_template(
    payload: Mapping[str, object],
    topology: Mapping[str, object],
) -> tuple[Mapping[str, object], str, Mapping[str, object]]:
    template = _mapping(payload.get("template_pack"), "template_pack")
    try:
        fingerprint = str(verify_pack_fingerprint(template))
    except (TypeError, ValueError) as exc:
        raise SemanticRefreshProjectionError("semantic-refresh template fingerprint differs") from exc
    if _digest(fingerprint, "template_pack_fingerprint") != payload.get("template_pack_fingerprint"):
        raise SemanticRefreshProjectionError("semantic-refresh template authority differs")
    semantic = _mapping(template.get("semantic_refresh"), "template semantic_refresh")
    if (
        template.get("activation") != "POST_DEPLOYMENT_AUTHORITY_REQUIRED"
        or template.get("executable") is not False
        or semantic.get("mode") != "semantic_refresh_v2_template"
        or semantic.get("topology") != topology
        or semantic.get("pre_release_bundle_sha256") != payload.get("pre_release_bundle_sha256")
        or semantic.get("package_artifacts_sha256") != payload.get("package_artifacts_sha256")
    ):
        raise SemanticRefreshProjectionError("semantic-refresh template closure differs")
    return template, fingerprint, _mapping(template.get("dbt_execution_pack"), "dbt_execution_pack")


def _validated_plan(
    payload: Mapping[str, object],
) -> tuple[Mapping[str, object], Mapping[str, str]]:
    plan = _mapping(payload.get("plan_bundle"), "plan_bundle")
    supplied = _digest(plan.get("plan_bundle_sha256"), "plan_bundle_sha256")
    if _canonical_digest({key: value for key, value in plan.items() if key != "plan_bundle_sha256"}) != supplied:
        raise SemanticRefreshProjectionError("semantic-refresh plan bundle digest differs")
    authority = _mapping(plan.get("release_deployment_authority"), "release deployment authority")
    workflow = _mapping(plan.get("workflow_plan"), "workflow plan")
    operations = _operation_mapping(plan)
    if (
        plan.get("schema") != "dpone.dbt-semantic-refresh-plan-bundle.v1"
        or supplied != payload.get("plan_bundle_sha256")
        or authority.get("release_id") != payload.get("release_id")
        or authority.get("deployment_id") != payload.get("deployment_id")
        or workflow.get("workflow_plan_sha256") != payload.get("workflow_plan_sha256")
        or plan.get("pre_release_bundle_sha256") != payload.get("pre_release_bundle_sha256")
        or plan.get("package_artifacts_sha256") != payload.get("package_artifacts_sha256")
        or operations != payload.get("operation_ids_by_model")
    ):
        raise SemanticRefreshProjectionError("semantic-refresh plan projection differs")
    return plan, operations


def _operation_mapping(plan: Mapping[str, object]) -> dict[str, str]:
    raw = plan.get("operation_plans")
    if not isinstance(raw, list) or not raw:
        raise SemanticRefreshProjectionError("semantic-refresh operation plans are absent")
    result: dict[str, str] = {}
    for item in raw:
        operation = _mapping(item, "operation plan")
        model = operation.get("model_unique_id")
        operation_id = operation.get("operation_id")
        if not isinstance(model, str) or not model or model in result:
            raise SemanticRefreshProjectionError("semantic-refresh operation model closure is invalid")
        result[model] = _digest(operation_id, "operation_id")
    return dict(sorted(result.items()))


def _mapping(value: object, field: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping):
        raise SemanticRefreshProjectionError(f"semantic-refresh {field} must be an object")
    return value


def _digest(value: object, field: str) -> str:
    if not isinstance(value, str) or _DIGEST_RE.fullmatch(value) is None:
        raise SemanticRefreshProjectionError(f"semantic-refresh {field} must be a canonical digest")
    return value


def _task_slug(model_unique_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]+", "_", model_unique_id).strip("_").lower()


def _canonical_json(value: Mapping[str, object]) -> bytes:
    try:
        return json.dumps(
            value,
            allow_nan=False,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    except (TypeError, ValueError) as exc:
        raise SemanticRefreshProjectionError("semantic-refresh DAG projection is not canonical JSON") from exc


def _canonical_digest(value: Mapping[str, object]) -> str:
    return "sha256:" + hashlib.sha256(_canonical_json(value)).hexdigest()


__all__ = [
    "AuthenticatedSemanticRefreshDagProjection",
    "SemanticRefreshDagProjectionAuthority",
    "SemanticRefreshDagProjectionIdentity",
    "SemanticRefreshDagTaskProjection",
    "SemanticRefreshProjectionError",
    "SemanticRefreshTask",
    "authenticate_semantic_refresh_dag_projection",
    "build_semantic_refresh_dag_projection",
    "build_semantic_refresh_dag_task_projection",
    "validate_semantic_refresh_dag_projection",
]
