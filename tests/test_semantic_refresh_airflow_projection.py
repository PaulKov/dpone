from __future__ import annotations

import hashlib
import json

import pytest
from dpone_airflow_pack.semantic_refresh_projection import (
    DurableModelPublication,
    DurableWorkflowSummary,
    SemanticRefreshProjectionError,
    build_semantic_refresh_task_projection,
    persist_and_project_success_assets,
    project_success_assets,
    reduce_durable_workflow_summary,
)

from dpone.contracts.semantic_refresh_workflow_summary import (
    SemanticRefreshDurableWorkflowSummary,
)


def _digest(char: str) -> str:
    return "sha256:" + char * 64


def _publication(
    operation: str,
    attempt: str,
    terminal: str | None,
    *,
    status: str = "COMPLETE",
    generation: int = 1,
    revision: int = 1,
) -> DurableModelPublication:
    complete = status == "COMPLETE"
    return DurableModelPublication(
        operation_id=_digest(operation),
        operation_plan_sha256=_digest("9"),
        workflow_execution_binding_sha256=_digest("a"),
        attempt_binding_sha256=_digest(attempt),
        artifact_manifest_sha256=(_digest("7") if complete else None),
        clickhouse_terminal_receipt_sha256=(_digest("8") if complete else None),
        terminal_receipt_sha256=(_digest(terminal) if terminal is not None else None),
        target_generation=(generation if complete else None),
        scope_revision=(revision if complete else None),
        status=status,
    )


def _operations() -> dict[str, str]:
    return {
        "model.orders": _digest("2"),
        "model.customers": _digest("1"),
    }


def _execution_binding(
    model_ids: tuple[str, ...] = ("model.customers", "model.orders"),
) -> dict[str, object]:
    unsigned: dict[str, object] = {
        "binding_set_ref": "binding-set://semantic-refresh",
        "connection_registry_ref": "connection://registry",
        "credential_runtime_ref": "credential://runtime",
        "deployment_id": _digest("7"),
        "expected_model_outcome_ids": list(model_ids),
        "model_operation_plan_ids": list(model_ids),
        "replacement_action_ids": [],
        "schema": "dpone.semantic-refresh-workflow-execution-binding.v1",
        "selected_mutating_node_ids": list(model_ids),
        "workflow_execution_id": "scheduled__2026-08-08",
        "workflow_mode": "normal",
        "workflow_plan_sha256": _digest("f"),
    }
    raw = json.dumps(unsigned, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return {**unsigned, "workflow_execution_binding_sha256": "sha256:" + hashlib.sha256(raw).hexdigest()}


def _dependencies() -> dict[str, tuple[str, ...]]:
    return {
        "model.orders": ("model.customers",),
        "model.customers": (),
    }


def _topology(
    dependencies: dict[str, tuple[str, ...]] | None = None,
    *,
    model_order: tuple[str, ...] = ("model.customers", "model.orders"),
) -> dict[str, object]:
    dependency_values = dependencies or _dependencies()
    unsigned: dict[str, object] = {
        "activation": "POST_DEPLOYMENT_AUTHORITY_REQUIRED",
        "dag_id": "semantic_refresh_daily_marts",
        "dag_policy": {
            "catchup": False,
            "max_active_runs": 1,
            "max_active_tasks": 2,
            "owner": "data",
            "schedule": None,
            "start_date": "2026-01-01",
            "tags": ["dbt", "dpone", "semantic-refresh-v2"],
            "timezone": "UTC",
        },
        "dependencies": {key: list(value) for key, value in dependency_values.items()},
        "logical_output_asset_uris": {
            model_id: f"dpone://mart/{model_id.rsplit('.', maxsplit=1)[-1]}" for model_id in model_order
        },
        "model_unique_ids": list(model_order),
        "profile_sha256": _digest("8"),
        "project_config_overlay": {},
        "schema": "dpone.dbt-semantic-refresh-topology-template.v1",
        "workflow_name": "daily_marts",
    }
    raw = json.dumps(unsigned, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return {**unsigned, "topology_sha256": "sha256:" + hashlib.sha256(raw).hexdigest()}


def test_task_projection_has_parallel_prepare_and_deterministic_sequential_commit() -> None:
    execution_binding = _execution_binding()
    topology = _topology()
    projection = build_semantic_refresh_task_projection(
        execution_binding=execution_binding,
        topology=topology,
        operation_ids_by_model=_operations(),
    )

    assert projection.dbt_task.task_id == "semantic_refresh__dbt_build_test"
    assert [task.task_id for task in projection.prepare_tasks] == [
        "semantic_refresh__prepare__model_customers",
        "semantic_refresh__prepare__model_orders",
    ]
    assert all(task.upstream_task_ids == (projection.dbt_task.task_id,) for task in projection.prepare_tasks)
    assert projection.commit_tasks[0].upstream_task_ids == tuple(task.task_id for task in projection.prepare_tasks)
    assert projection.commit_tasks[1].upstream_task_ids == (projection.commit_tasks[0].task_id,)
    assert [task.operation_id for task in projection.commit_tasks] == [_digest("1"), _digest("2")]
    assert projection.summary_task.trigger_rule == "all_done"
    assert projection.summary_task.upstream_task_ids == (projection.commit_tasks[-1].task_id,)
    assert projection.to_mapping() == {
        "schema": "dpone.semantic-refresh-task-projection.v1",
        "workflow_execution_id": "scheduled__2026-08-08",
        "workflow_execution_binding_sha256": execution_binding["workflow_execution_binding_sha256"],
        "topology_sha256": topology["topology_sha256"],
        "dag_id": "semantic_refresh_daily_marts",
        "asset_uris": ["dpone://mart/customers", "dpone://mart/orders"],
        "dbt_task": projection.dbt_task.to_mapping(),
        "prepare_tasks": [task.to_mapping() for task in projection.prepare_tasks],
        "commit_tasks": [task.to_mapping() for task in projection.commit_tasks],
        "summary_task": projection.summary_task.to_mapping(),
    }


@pytest.mark.parametrize(
    ("dependencies", "message"),
    [
        ({"model.orders": ("model.customers",)}, "missing or extra"),
        (
            {
                "model.orders": ("model.customers",),
                "model.customers": ("model.orders",),
            },
            "cycle",
        ),
        (
            {
                "model.orders": ("model.unknown",),
                "model.customers": (),
            },
            "closure is invalid",
        ),
    ],
)
def test_task_projection_rejects_incomplete_or_cyclic_topology(
    dependencies: dict[str, tuple[str, ...]],
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        build_semantic_refresh_task_projection(
            execution_binding=_execution_binding(),
            topology=_topology(dependencies),
            operation_ids_by_model=_operations(),
        )


def test_task_projection_rejects_model_unique_id_as_operation_identity() -> None:
    with pytest.raises(SemanticRefreshProjectionError, match="operation_id"):
        build_semantic_refresh_task_projection(
            execution_binding=_execution_binding(("model.orders",)),
            topology=_topology(
                {"model.orders": ()},
                model_order=("model.orders",),
            ),
            operation_ids_by_model={"model.orders": "model.orders"},
        )


def test_task_projection_rejects_dependency_change_under_original_topology_digest() -> None:
    topology = _topology()
    topology["dependencies"] = {
        "model.customers": [],
        "model.orders": [],
    }

    with pytest.raises(ValueError, match="topology digest differs"):
        build_semantic_refresh_task_projection(
            execution_binding=_execution_binding(),
            topology=topology,
            operation_ids_by_model=_operations(),
        )


def test_task_projection_rejects_changed_execution_identity_under_original_digest() -> None:
    binding = _execution_binding()
    binding["workflow_execution_id"] = "manual__forged"

    with pytest.raises(ValueError, match="execution binding digest differs"):
        build_semantic_refresh_task_projection(
            execution_binding=binding,
            topology=_topology(),
            operation_ids_by_model=_operations(),
        )


def test_success_assets_require_exact_fully_complete_durable_summary() -> None:
    publications = (
        _publication("1", "b", "c", generation=2),
        _publication("2", "d", "e", generation=3),
    )

    summary = reduce_durable_workflow_summary(
        workflow_execution_id="scheduled__2026-08-08",
        workflow_plan_sha256=_digest("f"),
        workflow_execution_binding_sha256=_digest("a"),
        expected_operation_ids=(_digest("2"), _digest("1")),
        publications=publications,
    )

    assert summary.status == "FULLY_COMPLETE"
    assert SemanticRefreshDurableWorkflowSummary.from_mapping(summary.to_mapping()).to_dict() == summary.to_mapping()
    assert persist_and_project_success_assets(
        summary,
        asset_uris=("dpone://mart/customers", "dpone://mart/orders"),
        persist_summary=lambda payload: {
            "persisted": True,
            "status": "FULLY_COMPLETE",
            "terminal_summary_sha256": payload["terminal_summary_sha256"],
        },
    ) == ("dpone://mart/customers", "dpone://mart/orders")


@pytest.mark.parametrize("durable_status", ["PREPARED", "TARGET_COMMITTED", "COMMITTED_INCOMPLETE"])
def test_task_success_cannot_substitute_for_incomplete_durable_state(
    durable_status: str,
) -> None:
    summary = reduce_durable_workflow_summary(
        workflow_execution_id="scheduled__2026-08-08",
        workflow_plan_sha256=_digest("f"),
        workflow_execution_binding_sha256=_digest("a"),
        expected_operation_ids=(_digest("2"),),
        publications=(_publication("2", "b", None, status=durable_status),),
    )

    assert summary.status == "INCOMPLETE"
    assert summary.schema == "dpone-airflow.semantic-refresh-workflow-reduction.v1"
    assert (
        project_success_assets(
            summary,
            durable_summary_sha256=None,
            asset_uris=("dpone://mart/orders",),
        )
        == ()
    )


def test_summary_rejects_missing_or_unexpected_durable_model_identity() -> None:
    with pytest.raises(SemanticRefreshProjectionError, match="identity closure"):
        reduce_durable_workflow_summary(
            workflow_execution_id="scheduled__2026-08-08",
            workflow_plan_sha256=_digest("f"),
            workflow_execution_binding_sha256=_digest("a"),
            expected_operation_ids=(_digest("2"),),
            publications=(_publication("1", "b", "c"),),
        )


def test_summary_rejects_duplicate_durable_model_receipts() -> None:
    publication = _publication("2", "b", "c")

    with pytest.raises(SemanticRefreshProjectionError, match="duplicate"):
        reduce_durable_workflow_summary(
            workflow_execution_id="scheduled__2026-08-08",
            workflow_plan_sha256=_digest("f"),
            workflow_execution_binding_sha256=_digest("a"),
            expected_operation_ids=(_digest("2"),),
            publications=(publication, publication),
        )


def test_direct_fully_complete_summary_cannot_forge_asset_authority() -> None:
    with pytest.raises(SemanticRefreshProjectionError, match="publication identity closure"):
        DurableWorkflowSummary(
            schema="dpone.semantic-refresh-durable-workflow-summary.v1",
            workflow_execution_id="scheduled__2026-08-08",
            workflow_plan_sha256=_digest("f"),
            workflow_execution_binding_sha256=_digest("a"),
            expected_operation_ids=(_digest("2"),),
            publications=(),
            status="FULLY_COMPLETE",
            terminal_summary_sha256=_digest("9"),
        )


def test_unpersisted_complete_summary_emits_no_assets() -> None:
    publication = _publication("2", "b", "c")
    summary = reduce_durable_workflow_summary(
        workflow_execution_id="scheduled__2026-08-08",
        workflow_plan_sha256=_digest("f"),
        workflow_execution_binding_sha256=_digest("a"),
        expected_operation_ids=(_digest("2"),),
        publications=(publication,),
    )

    assert (
        project_success_assets(
            summary,
            durable_summary_sha256=None,
            asset_uris=("dpone://mart/orders",),
        )
        == ()
    )


def test_complete_summary_rejects_missing_canonical_terminal_field() -> None:
    publication = _publication("2", "b", "c")
    summary = reduce_durable_workflow_summary(
        workflow_execution_id="scheduled__2026-08-08",
        workflow_plan_sha256=_digest("f"),
        workflow_execution_binding_sha256=_digest("a"),
        expected_operation_ids=(_digest("2"),),
        publications=(publication,),
    ).to_mapping()
    raw_publication = dict(summary["publications"][0])  # type: ignore[index]
    raw_publication.pop("artifact_manifest_sha256")
    summary["publications"] = [raw_publication]

    with pytest.raises(ValueError, match="publication"):
        SemanticRefreshDurableWorkflowSummary.from_mapping(summary)
