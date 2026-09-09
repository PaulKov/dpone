"""Parse-safe Airflow projection for durable semantic-refresh authority.

This module deliberately contains no Airflow import. It projects the approved
graph and reduces durable database receipts; Airflow task/process success is
never accepted as publication authority.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from dataclasses import dataclass

from dpone_airflow_pack.semantic_refresh_dag_projection import (
    SemanticRefreshProjectionError,
    SemanticRefreshTask,
    build_semantic_refresh_dag_task_projection,
)
from dpone_airflow_pack.semantic_refresh_execution_binding import (
    SemanticRefreshExecutionBinding,
)
from dpone_airflow_pack.semantic_refresh_topology import (
    SemanticRefreshTopologyTemplate,
)

_DIGEST_RE = re.compile(r"^sha256:[0-9a-f]{64}$")
_CANONICAL_SUMMARY_SCHEMA = "dpone.semantic-refresh-durable-workflow-summary.v1"
_INTERNAL_REDUCTION_SCHEMA = "dpone-airflow.semantic-refresh-workflow-reduction.v1"
_NON_COMPLETE_STATUSES = frozenset(
    {
        "PREPARING",
        "PREPARED",
        "COMMITTING",
        "TARGET_COMMITTED",
        "FAILED_PRE_COMMIT",
        "COMMIT_UNKNOWN",
        "COMMITTED_INCOMPLETE",
    }
)


@dataclass(frozen=True)
class SemanticRefreshTaskProjection:
    """One dbt gate, parallel PREPARE, sequential COMMIT, and summary."""

    workflow_execution_id: str
    workflow_execution_binding_sha256: str
    topology_sha256: str
    dag_id: str
    asset_uris: tuple[str, ...]
    dbt_task: SemanticRefreshTask
    prepare_tasks: tuple[SemanticRefreshTask, ...]
    commit_tasks: tuple[SemanticRefreshTask, ...]
    summary_task: SemanticRefreshTask

    def __post_init__(self) -> None:
        if not self.workflow_execution_id.strip():
            raise SemanticRefreshProjectionError("workflow_execution_id must be non-empty")
        _require_digest(self.workflow_execution_binding_sha256, "workflow_execution_binding_sha256")
        _require_digest(self.topology_sha256, "topology_sha256")
        if not self.dag_id.strip():
            raise SemanticRefreshProjectionError("dag_id must be non-empty")
        if not self.asset_uris or len(set(self.asset_uris)) != len(self.asset_uris):
            raise SemanticRefreshProjectionError("projection Asset URIs must be unique and non-empty")

    def to_mapping(self) -> dict[str, object]:
        """Serialize the authenticated task graph without executable objects."""

        return {
            "schema": "dpone.semantic-refresh-task-projection.v1",
            "workflow_execution_id": self.workflow_execution_id,
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
            "topology_sha256": self.topology_sha256,
            "dag_id": self.dag_id,
            "asset_uris": list(self.asset_uris),
            "dbt_task": self.dbt_task.to_mapping(),
            "prepare_tasks": [task.to_mapping() for task in self.prepare_tasks],
            "commit_tasks": [task.to_mapping() for task in self.commit_tasks],
            "summary_task": self.summary_task.to_mapping(),
        }


@dataclass(frozen=True)
class DurableModelPublication:
    """Durable terminal/nonterminal model state read by the summary task."""

    operation_id: str
    operation_plan_sha256: str
    workflow_execution_binding_sha256: str
    attempt_binding_sha256: str
    artifact_manifest_sha256: str | None
    clickhouse_terminal_receipt_sha256: str | None
    terminal_receipt_sha256: str | None
    target_generation: int | None
    scope_revision: int | None
    status: str

    def __post_init__(self) -> None:
        _require_digest(self.operation_id, "operation_id")
        _require_digest(
            self.workflow_execution_binding_sha256,
            "workflow_execution_binding_sha256",
        )
        _require_digest(self.operation_plan_sha256, "operation_plan_sha256")
        _require_digest(self.attempt_binding_sha256, "attempt_binding_sha256")
        if self.status == "COMPLETE":
            _require_digest(self.artifact_manifest_sha256, "artifact_manifest_sha256")
            _require_digest(
                self.clickhouse_terminal_receipt_sha256,
                "clickhouse_terminal_receipt_sha256",
            )
            _require_digest(self.terminal_receipt_sha256, "terminal_receipt_sha256")
            _require_positive_int(self.target_generation, "target_generation")
            _require_positive_int(self.scope_revision, "scope_revision")
        elif self.status in _NON_COMPLETE_STATUSES:
            if any(
                value is not None
                for value in (
                    self.artifact_manifest_sha256,
                    self.clickhouse_terminal_receipt_sha256,
                    self.terminal_receipt_sha256,
                    self.target_generation,
                    self.scope_revision,
                )
            ):
                raise SemanticRefreshProjectionError("non-complete durable model cannot claim terminal authority")
        else:
            raise SemanticRefreshProjectionError("durable model status is not canonical")

    def to_mapping(self) -> dict[str, object]:
        return {
            "artifact_manifest_sha256": self.artifact_manifest_sha256,
            "attempt_binding_sha256": self.attempt_binding_sha256,
            "clickhouse_terminal_receipt_sha256": self.clickhouse_terminal_receipt_sha256,
            "operation_id": self.operation_id,
            "operation_plan_sha256": self.operation_plan_sha256,
            "scope_revision": self.scope_revision,
            "status": self.status,
            "target_generation": self.target_generation,
            "terminal_receipt_sha256": self.terminal_receipt_sha256,
            "workflow_execution_binding_sha256": self.workflow_execution_binding_sha256,
        }


@dataclass(frozen=True)
class DurableWorkflowSummary:
    """Workflow result derived exclusively from exact durable model receipts."""

    schema: str
    workflow_execution_id: str
    workflow_plan_sha256: str
    workflow_execution_binding_sha256: str
    expected_operation_ids: tuple[str, ...]
    publications: tuple[DurableModelPublication, ...]
    status: str
    terminal_summary_sha256: str | None

    def __post_init__(self) -> None:
        if not self.workflow_execution_id.strip():
            raise SemanticRefreshProjectionError("workflow_execution_id must be non-empty")
        _require_digest(self.workflow_plan_sha256, "workflow_plan_sha256")
        _require_digest(
            self.workflow_execution_binding_sha256,
            "workflow_execution_binding_sha256",
        )
        expected = _identity_closure(self.expected_operation_ids, field="expected operation IDs")
        if expected != self.expected_operation_ids:
            raise SemanticRefreshProjectionError("durable workflow expected identities are not canonical")
        observed = tuple(publication.operation_id for publication in self.publications)
        if observed != expected or len(set(observed)) != len(observed):
            raise SemanticRefreshProjectionError("durable workflow publication identity closure is invalid")
        if any(
            publication.workflow_execution_binding_sha256 != self.workflow_execution_binding_sha256
            for publication in self.publications
        ):
            raise SemanticRefreshProjectionError("durable workflow publications use another execution binding")
        derived_status = (
            "FULLY_COMPLETE"
            if all(publication.status == "COMPLETE" for publication in self.publications)
            else "INCOMPLETE"
        )
        if self.status != derived_status:
            raise SemanticRefreshProjectionError("durable workflow summary status is not derived from receipts")
        expected_schema = (
            _CANONICAL_SUMMARY_SCHEMA if derived_status == "FULLY_COMPLETE" else _INTERNAL_REDUCTION_SCHEMA
        )
        if self.schema != expected_schema:
            raise SemanticRefreshProjectionError("durable workflow summary schema is invalid")
        expected_digest = _canonical_digest(_summary_unsigned(self)) if derived_status == "FULLY_COMPLETE" else None
        if self.terminal_summary_sha256 != expected_digest:
            raise SemanticRefreshProjectionError("durable workflow terminal summary digest is invalid")

    def to_mapping(self) -> dict[str, object]:
        return {
            **_summary_unsigned(self),
            "terminal_summary_sha256": self.terminal_summary_sha256,
        }


def build_semantic_refresh_task_projection(
    *,
    execution_binding: Mapping[str, object],
    topology: Mapping[str, object],
    operation_ids_by_model: Mapping[str, str],
) -> SemanticRefreshTaskProjection:
    """Build deterministic tasks from one closed protected dependency topology."""

    protected_binding = SemanticRefreshExecutionBinding.from_mapping(execution_binding)
    protected_topology = SemanticRefreshTopologyTemplate.from_mapping(topology)
    model_order = protected_topology.model_unique_ids
    if tuple(sorted(model_order)) != protected_binding.model_unique_ids:
        raise SemanticRefreshProjectionError("execution binding and topology model closures differ")
    static = build_semantic_refresh_dag_task_projection(
        topology=topology,
        operation_ids_by_model=operation_ids_by_model,
    )
    return SemanticRefreshTaskProjection(
        workflow_execution_id=protected_binding.workflow_execution_id,
        workflow_execution_binding_sha256=protected_binding.workflow_execution_binding_sha256,
        topology_sha256=static.topology_sha256,
        dag_id=static.dag_id,
        asset_uris=static.asset_uris,
        dbt_task=static.dbt_task,
        prepare_tasks=static.prepare_tasks,
        commit_tasks=static.commit_tasks,
        summary_task=static.summary_task,
    )


def reduce_durable_workflow_summary(
    *,
    workflow_execution_id: str,
    workflow_plan_sha256: str,
    workflow_execution_binding_sha256: str,
    expected_operation_ids: tuple[str, ...],
    publications: tuple[DurableModelPublication, ...],
) -> DurableWorkflowSummary:
    """Reduce exact durable state; never inspect Airflow task state or XCom."""

    if not workflow_execution_id.strip():
        raise SemanticRefreshProjectionError("workflow_execution_id must be non-empty")
    _require_digest(workflow_plan_sha256, "workflow_plan_sha256")
    _require_digest(
        workflow_execution_binding_sha256,
        "workflow_execution_binding_sha256",
    )
    expected = _identity_closure(expected_operation_ids, field="expected operation IDs")
    observed_ids = tuple(publication.operation_id for publication in publications)
    if len(set(observed_ids)) != len(observed_ids):
        raise SemanticRefreshProjectionError("duplicate durable model receipts are forbidden")
    if set(observed_ids) != set(expected):
        raise SemanticRefreshProjectionError("durable model identity closure differs from expected")
    ordered = tuple(sorted(publications, key=lambda publication: publication.operation_id))
    if any(
        publication.workflow_execution_binding_sha256 != workflow_execution_binding_sha256 for publication in ordered
    ):
        raise SemanticRefreshProjectionError("durable model identity closure uses another execution binding")
    fully_complete = all(publication.status == "COMPLETE" for publication in ordered)
    unsigned: dict[str, object] = {
        "schema": _CANONICAL_SUMMARY_SCHEMA if fully_complete else _INTERNAL_REDUCTION_SCHEMA,
        "workflow_execution_id": workflow_execution_id,
        "workflow_plan_sha256": workflow_plan_sha256,
        "workflow_execution_binding_sha256": workflow_execution_binding_sha256,
        "expected_operation_ids": list(expected),
        "publications": [publication.to_mapping() for publication in ordered],
        "status": "FULLY_COMPLETE" if fully_complete else "INCOMPLETE",
    }
    return DurableWorkflowSummary(
        schema=str(unsigned["schema"]),
        workflow_execution_id=workflow_execution_id,
        workflow_plan_sha256=workflow_plan_sha256,
        workflow_execution_binding_sha256=workflow_execution_binding_sha256,
        expected_operation_ids=expected,
        publications=ordered,
        status=str(unsigned["status"]),
        terminal_summary_sha256=_canonical_digest(unsigned) if fully_complete else None,
    )


def project_success_assets(
    summary: DurableWorkflowSummary,
    *,
    durable_summary_sha256: str | None,
    asset_uris: tuple[str, ...],
) -> tuple[str, ...]:
    """Return success Assets only after the exact durable summary is persisted."""

    validate_durable_workflow_summary(summary)
    if (
        summary.status != "FULLY_COMPLETE"
        or not _is_digest(summary.terminal_summary_sha256)
        or durable_summary_sha256 != summary.terminal_summary_sha256
    ):
        return ()
    if not asset_uris or any(not uri.strip() for uri in asset_uris):
        raise SemanticRefreshProjectionError("success Asset URIs must be non-empty")
    if len(set(asset_uris)) != len(asset_uris):
        raise SemanticRefreshProjectionError("success Asset URIs must be unique")
    return tuple(sorted(asset_uris))


def persist_and_project_success_assets(
    summary: DurableWorkflowSummary,
    *,
    asset_uris: tuple[str, ...],
    persist_summary: Callable[[Mapping[str, object]], Mapping[str, object]],
) -> tuple[str, ...]:
    """Persist the durable summary before projecting success Asset URIs."""

    validate_durable_workflow_summary(summary)
    if summary.status != "FULLY_COMPLETE":
        return ()
    acknowledgement = dict(persist_summary(summary.to_mapping()))
    if set(acknowledgement) != {"persisted", "status", "terminal_summary_sha256"}:
        raise SemanticRefreshProjectionError("durable workflow persistence acknowledgement is not closed")
    if acknowledgement.get("persisted") is not True or acknowledgement.get("status") != "FULLY_COMPLETE":
        raise SemanticRefreshProjectionError("durable workflow persistence is not acknowledged")
    persisted_digest = acknowledgement.get("terminal_summary_sha256")
    return project_success_assets(
        summary,
        durable_summary_sha256=(persisted_digest if isinstance(persisted_digest, str) else None),
        asset_uris=asset_uris,
    )


def validate_durable_workflow_summary(summary: DurableWorkflowSummary) -> None:
    """Recompute all closure and digest invariants for a supplied summary."""

    summary.__post_init__()


def _identity_closure(values: tuple[str, ...], *, field: str) -> tuple[str, ...]:
    if not values or len(set(values)) != len(values):
        raise SemanticRefreshProjectionError(f"{field} must be unique and non-empty")
    for value in values:
        _require_digest(value, "operation_id")
    return tuple(sorted(values))


def _summary_unsigned(summary: DurableWorkflowSummary) -> dict[str, object]:
    return {
        "schema": summary.schema,
        "workflow_execution_id": summary.workflow_execution_id,
        "workflow_plan_sha256": summary.workflow_plan_sha256,
        "workflow_execution_binding_sha256": summary.workflow_execution_binding_sha256,
        "expected_operation_ids": list(summary.expected_operation_ids),
        "publications": [publication.to_mapping() for publication in summary.publications],
        "status": summary.status,
    }


def _canonical_digest(payload: Mapping[str, object]) -> str:
    raw = json.dumps(
        payload,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _is_digest(value: object) -> bool:
    return isinstance(value, str) and _DIGEST_RE.fullmatch(value) is not None


def _require_digest(value: object, field: str) -> None:
    if not _is_digest(value):
        raise SemanticRefreshProjectionError(f"{field} must be a canonical sha256 digest")


def _require_positive_int(value: object, field: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise SemanticRefreshProjectionError(f"{field} must be a positive integer")


__all__ = [
    "DurableModelPublication",
    "DurableWorkflowSummary",
    "SemanticRefreshProjectionError",
    "SemanticRefreshTask",
    "SemanticRefreshTaskProjection",
    "build_semantic_refresh_task_projection",
    "persist_and_project_success_assets",
    "project_success_assets",
    "reduce_durable_workflow_summary",
    "validate_durable_workflow_summary",
]
