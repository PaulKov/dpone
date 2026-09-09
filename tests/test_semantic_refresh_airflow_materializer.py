"""Actual Airflow DAG/task materialization from semantic-refresh projection."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from pathlib import Path
from types import ModuleType, SimpleNamespace

import pytest
from dpone_airflow_pack.pack_identity import PACK_IDENTITY_SCHEMA, compute_pack_fingerprint
from dpone_airflow_pack.semantic_refresh_activated_pack import (
    validate_semantic_refresh_activated_pack,
)
from dpone_airflow_pack.semantic_refresh_airflow import (
    SemanticRefreshAirflowCallables,
    materialize_semantic_refresh_dag,
)
from dpone_airflow_pack.semantic_refresh_dag_authority import (
    LocalSemanticRefreshDagProjectionAuthority,
)
from dpone_airflow_pack.semantic_refresh_dag_projection import (
    build_semantic_refresh_dag_projection,
    validate_semantic_refresh_dag_projection,
)
from dpone_airflow_pack.semantic_refresh_projection import (
    DurableModelPublication,
    SemanticRefreshProjectionError,
    build_semantic_refresh_task_projection,
)
from dpone_airflow_pack.semantic_refresh_topology import SemanticRefreshTopologyTemplate

from dpone.adapters.semantic_refresh_mssql_publication_activated_pack import (
    MssqlActivatedPackAuthorityError,
    MssqlSemanticRefreshActivatedPackAuthority,
)


def _digest(char: str) -> str:
    return "sha256:" + char * 64


def _publication(operation: str, attempt: str, terminal: str, *, status: str = "COMPLETE") -> DurableModelPublication:
    complete = status == "COMPLETE"
    return DurableModelPublication(
        operation_id=_digest(operation),
        operation_plan_sha256=_digest("9"),
        workflow_execution_binding_sha256=_binding_digest(),
        attempt_binding_sha256=_digest(attempt),
        artifact_manifest_sha256=(_digest("7") if complete else None),
        clickhouse_terminal_receipt_sha256=(_digest("8") if complete else None),
        terminal_receipt_sha256=(_digest(terminal) if complete else None),
        target_generation=(2 if complete else None),
        scope_revision=(1 if complete else None),
        status=status,
    )


class _DAG:
    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.tasks: list[_PythonOperator] = []


class _Asset:
    def __init__(self, uri: str) -> None:
        self.uri = uri


class _PythonOperator:
    def __init__(self, *, dag: _DAG, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.task_id = str(kwargs["task_id"])
        self.upstream_task_ids: set[str] = set()
        dag.tasks.append(self)

    def __rshift__(self, other: _PythonOperator) -> _PythonOperator:
        other.upstream_task_ids.add(self.task_id)
        return other


@pytest.fixture
def airflow_33(monkeypatch: pytest.MonkeyPatch) -> None:
    airflow = ModuleType("airflow")
    sdk = ModuleType("airflow.sdk")
    sdk.DAG = _DAG  # type: ignore[attr-defined]
    sdk.Asset = _Asset  # type: ignore[attr-defined]
    sdk.get_current_context = lambda: {  # type: ignore[attr-defined]
        "dag_run": SimpleNamespace(run_id="scheduled__2026-08-08")
    }
    providers = ModuleType("airflow.providers")
    standard = ModuleType("airflow.providers.standard")
    operators = ModuleType("airflow.providers.standard.operators")
    python = ModuleType("airflow.providers.standard.operators.python")
    python.PythonOperator = _PythonOperator  # type: ignore[attr-defined]
    for name, module in {
        "airflow": airflow,
        "airflow.sdk": sdk,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": operators,
        "airflow.providers.standard.operators.python": python,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def _projection():
    return build_semantic_refresh_task_projection(
        execution_binding=_execution_binding(),
        topology=_topology(),
        operation_ids_by_model={
            "model.orders": _digest("2"),
            "model.customers": _digest("1"),
        },
    )


class _ExecutionPack:
    def to_dict(self) -> dict[str, object]:
        return {"schema": "test.semantic-refresh-dbt-execution-pack.v1"}


def _canonical_digest(value: dict[str, object]) -> str:
    raw = json.dumps(value, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def _activated_pack() -> dict[str, object]:
    projection = _projection()
    execution_pack = _ExecutionPack()
    plan_bundle_sha256 = _digest("a")
    activation_unsigned: dict[str, object] = {
        "authority_store_ref": "mssql://control/semantic-refresh-authority",
        "baseline_receipts": [["model.customers", _digest("1")], ["model.orders", _digest("2")]],
        "deployment_id": _digest("3"),
        "persisted_at": "2026-08-08T00:00:00Z",
        "plan_bundle_sha256": plan_bundle_sha256,
        "release_id": _digest("5"),
        "route_certification_receipt_sha256": _digest("6"),
        "runtime_assurance_receipts": [
            ["model.customers", "ddl_freeze", _digest("7")],
            ["model.customers", "writer_exclusivity", _digest("8")],
            ["model.orders", "ddl_freeze", _digest("9")],
            ["model.orders", "writer_exclusivity", _digest("a")],
        ],
        "schema": "dpone.dbt-semantic-refresh-activation-authority-receipt.v1",
    }
    payload: dict[str, object] = {
        "activation": "PROTECTED_RUN_AUTHORITY_BOUND",
        "dbt_execution_pack": execution_pack.to_dict(),
        "executable": True,
        "kind": "gitops.airflow_pack",
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "producer": "dpone semantic-refresh activate",
        "runtime_command": "",
        "runtime_payload_ids": [],
        "schema_version": "3",
        "semantic_refresh": {
            "activation_authority": {
                **activation_unsigned,
                "activation_authority_receipt_sha256": _canonical_digest(activation_unsigned),
            },
            "execution_binding": _execution_binding(),
            "mode": "semantic_refresh_v2_activated",
            "operation_ids_by_model": {
                "model.customers": _digest("1"),
                "model.orders": _digest("2"),
            },
            "package_artifacts_sha256": _digest("c"),
            "plan_bundle_sha256": plan_bundle_sha256,
            "pre_release_bundle_sha256": _digest("d"),
            "run_execution_bundle_sha256": _digest("b"),
            "task_projection": projection.to_mapping(),
            "topology": _topology(),
            "topology_sha256": _topology()["topology_sha256"],
            "workflow_execution_binding_sha256": projection.workflow_execution_binding_sha256,
            "workflow_execution_id": projection.workflow_execution_id,
            "workflow_plan_sha256": _digest("f"),
        },
        "workload": {
            "effective_config": {"authoring": {"mode": "semantic_refresh_v2_activated"}},
            "workload_id": "semantic__daily_marts",
        },
    }
    payload["pack_fingerprint"] = compute_pack_fingerprint(payload)
    return payload


def _execution_binding() -> dict[str, object]:
    unsigned: dict[str, object] = {
        "binding_set_ref": "binding-set://semantic-refresh",
        "connection_registry_ref": "connection://registry",
        "credential_runtime_ref": "credential://runtime",
        "deployment_id": _digest("7"),
        "expected_model_outcome_ids": ["model.customers", "model.orders"],
        "model_operation_plan_ids": ["model.customers", "model.orders"],
        "replacement_action_ids": [],
        "schema": "dpone.semantic-refresh-workflow-execution-binding.v1",
        "selected_mutating_node_ids": ["model.customers", "model.orders"],
        "workflow_execution_id": "scheduled__2026-08-08",
        "workflow_mode": "normal",
        "workflow_plan_sha256": _digest("f"),
    }
    raw = json.dumps(unsigned, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return {**unsigned, "workflow_execution_binding_sha256": "sha256:" + hashlib.sha256(raw).hexdigest()}


def _binding_digest() -> str:
    return str(_execution_binding()["workflow_execution_binding_sha256"])


def _topology() -> dict[str, object]:
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
        "dependencies": {
            "model.customers": [],
            "model.orders": ["model.customers"],
        },
        "logical_output_asset_uris": {
            "model.customers": "dpone://mart/customers",
            "model.orders": "dpone://mart/orders",
        },
        "model_unique_ids": ["model.customers", "model.orders"],
        "profile_sha256": _digest("8"),
        "project_config_overlay": {},
        "schema": "dpone.dbt-semantic-refresh-topology-template.v1",
        "workflow_name": "daily_marts",
    }
    raw = json.dumps(unsigned, allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return {**unsigned, "topology_sha256": "sha256:" + hashlib.sha256(raw).hexdigest()}


def _template_pack() -> dict[str, object]:
    payload: dict[str, object] = {
        "activation": "POST_DEPLOYMENT_AUTHORITY_REQUIRED",
        "dbt_execution_pack": _ExecutionPack().to_dict(),
        "executable": False,
        "kind": "gitops.airflow_pack",
        "pack_identity": {"schema": PACK_IDENTITY_SCHEMA},
        "producer": "dpone dbt compile",
        "runtime_command": "",
        "runtime_payload_ids": [],
        "schema_version": "3",
        "semantic_refresh": {
            "mode": "semantic_refresh_v2_template",
            "package_artifacts_sha256": _digest("c"),
            "pre_release_bundle_sha256": _digest("d"),
            "topology": _topology(),
        },
        "workload": {
            "effective_config": {"authoring": {"mode": "semantic_refresh_v2_template"}},
            "workload_id": "semantic__daily_marts",
        },
    }
    payload["pack_fingerprint"] = compute_pack_fingerprint(payload)
    return payload


def _plan_bundle() -> dict[str, object]:
    unsigned: dict[str, object] = {
        "operation_plans": [
            {"model_unique_id": "model.customers", "operation_id": _digest("1")},
            {"model_unique_id": "model.orders", "operation_id": _digest("2")},
        ],
        "package_artifacts_sha256": _digest("c"),
        "pre_release_bundle_sha256": _digest("d"),
        "release_deployment_authority": {
            "deployment_id": _digest("3"),
            "release_id": _digest("5"),
        },
        "schema": "dpone.dbt-semantic-refresh-plan-bundle.v1",
        "targets": [],
        "workflow_plan": {"workflow_plan_sha256": _digest("f")},
    }
    return {**unsigned, "plan_bundle_sha256": _canonical_digest(unsigned)}


def _dag_projection() -> dict[str, object]:
    return build_semantic_refresh_dag_projection(
        template_pack=_template_pack(),
        plan_bundle=_plan_bundle(),
    ).to_mapping()


def _dag_authority(payload: dict[str, object]) -> LocalSemanticRefreshDagProjectionAuthority:
    identity = validate_semantic_refresh_dag_projection(payload).identity
    return LocalSemanticRefreshDagProjectionAuthority.from_mapping(
        LocalSemanticRefreshDagProjectionAuthority.build(identity).to_mapping()
    )


def _callables(
    *,
    publications: tuple[DurableModelPublication, ...] = (),
    persist_summary=lambda payload: payload,
) -> SemanticRefreshAirflowCallables:
    return SemanticRefreshAirflowCallables(
        dbt_build_test=lambda **_kwargs: None,
        resolve_execution_binding=lambda **_kwargs: _binding_digest(),
        prepare=lambda **_kwargs: None,
        commit=lambda **_kwargs: None,
        read_publications=lambda **_kwargs: publications,
        persist_summary=persist_summary,
    )


def test_run_neutral_sidecar_has_canonical_index_descriptor_and_no_run_identity() -> None:
    authenticated = validate_semantic_refresh_dag_projection(_dag_projection())
    descriptor = authenticated.descriptor()

    assert descriptor == {
        "artifact_bytes": len(authenticated.canonical_bytes()),
        "artifact_sha256": "sha256:" + hashlib.sha256(authenticated.canonical_bytes()).hexdigest(),
        "dag_id": "semantic_refresh_daily_marts",
        "dag_projection_sha256": authenticated.identity.dag_projection_sha256,
        "projection_id": "semantic_refresh_v2::semantic_refresh_daily_marts",
        "workflow_name": "daily_marts",
    }
    encoded = authenticated.canonical_bytes().decode()
    assert "workflow_execution_id" not in encoded
    assert "workflow_execution_binding_sha256" not in encoded


def test_airflow_33_materializer_builds_real_dag_and_exact_dependencies(airflow_33: None) -> None:
    callables = _callables(
        publications=(
            _publication("1", "b", "c"),
            _publication("2", "d", "e"),
        ),
        persist_summary=lambda payload: {
            "persisted": True,
            "status": "FULLY_COMPLETE",
            "terminal_summary_sha256": payload["terminal_summary_sha256"],
        },
    )

    projection = _dag_projection()
    dag = materialize_semantic_refresh_dag(
        dag_projection=projection,
        dag_projection_authority=_dag_authority(projection),
        callables=callables,
    )

    assert isinstance(dag, _DAG)
    tasks = {task.task_id: task for task in dag.tasks}
    assert set(tasks) == {
        "semantic_refresh__dbt_build_test",
        "semantic_refresh__prepare__model_customers",
        "semantic_refresh__prepare__model_orders",
        "semantic_refresh__commit__0000__model_customers",
        "semantic_refresh__commit__0001__model_orders",
        "semantic_refresh__durable_workflow_summary",
    }
    assert tasks["semantic_refresh__commit__0000__model_customers"].upstream_task_ids == {
        "semantic_refresh__prepare__model_customers",
        "semantic_refresh__prepare__model_orders",
    }
    summary = tasks["semantic_refresh__durable_workflow_summary"]
    assert summary.kwargs["trigger_rule"] == "all_done"
    assert summary.kwargs["retries"] == 0
    assert summary.kwargs["do_xcom_push"] is True
    assert all(
        task.kwargs["do_xcom_push"] is False
        for task_id, task in tasks.items()
        if task_id != "semantic_refresh__durable_workflow_summary"
    )
    assert all(task.kwargs["retries"] == 0 for task in tasks.values())
    assert dag.kwargs == {
        "catchup": False,
        "dag_id": "semantic_refresh_daily_marts",
        "default_args": {"owner": "data"},
        "max_active_runs": 1,
        "max_active_tasks": 2,
        "schedule": None,
        "start_date": dag.kwargs["start_date"],
        "tags": ["dbt", "dpone", "semantic-refresh-v2"],
    }
    assert str(dag.kwargs["start_date"]).startswith("2026-01-01")
    assert tasks["semantic_refresh__dbt_build_test"].kwargs["op_kwargs"] == {
        "dag_projection_sha256": projection["dag_projection_sha256"],
        "dbt_execution_pack": {"schema": "test.semantic-refresh-dbt-execution-pack.v1"},
        "package_artifacts_sha256": _digest("c"),
        "deployment_id": _digest("3"),
        "plan_bundle_sha256": _plan_bundle()["plan_bundle_sha256"],
        "pre_release_bundle_sha256": _digest("d"),
        "profile_sha256": _digest("8"),
        "projection_identity": {
            "dag_projection_sha256": projection["dag_projection_sha256"],
            "deployment_id": _digest("3"),
            "package_artifacts_sha256": _digest("c"),
            "plan_bundle_sha256": _plan_bundle()["plan_bundle_sha256"],
            "pre_release_bundle_sha256": _digest("d"),
            "release_id": _digest("5"),
            "template_pack_fingerprint": _template_pack()["pack_fingerprint"],
            "topology_sha256": _topology()["topology_sha256"],
            "workflow_plan_sha256": _digest("f"),
        },
        "project_config_overlay": {},
        "release_id": _digest("5"),
        "template_pack_fingerprint": _template_pack()["pack_fingerprint"],
        "topology_sha256": _topology()["topology_sha256"],
        "workflow_execution_id": "{{ dag_run.run_id }}",
        "workflow_plan_sha256": _digest("f"),
    }
    assert [asset.uri for asset in summary.kwargs["outlets"]] == [
        "dpone://mart/customers",
        "dpone://mart/orders",
    ]
    result = summary.kwargs["python_callable"]()
    assert result["status"] == "FULLY_COMPLETE"


def test_summary_task_raises_on_incomplete_durable_state_so_assets_do_not_emit(airflow_33: None) -> None:
    callables = _callables(
        publications=(
            _publication("1", "b", "c"),
            _publication("2", "d", "e", status="COMMITTED_INCOMPLETE"),
        ),
        persist_summary=lambda _payload: pytest.fail("incomplete summary must not persist"),
    )
    projection = _dag_projection()
    dag = materialize_semantic_refresh_dag(
        dag_projection=projection,
        dag_projection_authority=_dag_authority(projection),
        callables=callables,
    )
    summary = next(task for task in dag.tasks if task.task_id.endswith("durable_workflow_summary"))

    with pytest.raises(SemanticRefreshProjectionError, match="not fully complete"):
        summary.kwargs["python_callable"]()


def test_summary_task_raises_on_persistence_ack_mismatch(airflow_33: None) -> None:
    publications = (
        _publication("1", "b", "c"),
        _publication("2", "d", "e"),
    )
    callables = _callables(
        publications=publications,
        persist_summary=lambda _payload: {
            "persisted": True,
            "status": "FULLY_COMPLETE",
            "terminal_summary_sha256": _digest("0"),
        },
    )
    projection = _dag_projection()
    dag = materialize_semantic_refresh_dag(
        dag_projection=projection,
        dag_projection_authority=_dag_authority(projection),
        callables=callables,
    )
    summary = next(task for task in dag.tasks if task.task_id.endswith("durable_workflow_summary"))

    with pytest.raises(SemanticRefreshProjectionError, match="acknowledgement differs"):
        summary.kwargs["python_callable"]()


def test_materialization_rejects_rehashed_asset_projection_before_dag_creation() -> None:
    protected = _dag_projection()
    payload = dict(protected)
    task_projection = dict(payload["task_projection"])
    task_projection["asset_uris"] = ["dpone://attacker/target"]
    payload["task_projection"] = task_projection
    unsigned = {key: value for key, value in payload.items() if key != "dag_projection_sha256"}
    payload["dag_projection_sha256"] = _canonical_digest(unsigned)

    with pytest.raises(ValueError, match="task projection differs|local deployment authority"):
        materialize_semantic_refresh_dag(
            dag_projection=payload,
            dag_projection_authority=_dag_authority(protected),
            callables=_callables(),
        )


def test_topology_rejects_missing_static_dag_policy() -> None:
    topology = _topology()
    del topology["dag_policy"]

    with pytest.raises(ValueError, match="topology fields are not closed"):
        SemanticRefreshTopologyTemplate.from_mapping(topology)


def test_materializer_rejects_rehashed_schedule_outside_verified_index() -> None:
    protected = _dag_projection()
    template = _template_pack()
    semantic = dict(template["semantic_refresh"])
    topology = dict(semantic["topology"])
    dag_policy = dict(topology["dag_policy"])
    dag_policy["schedule"] = "0 2 * * *"
    topology["dag_policy"] = dag_policy
    topology_unsigned = {key: value for key, value in topology.items() if key != "topology_sha256"}
    topology["topology_sha256"] = _canonical_digest(topology_unsigned)
    semantic["topology"] = topology
    template["semantic_refresh"] = semantic
    del template["pack_fingerprint"]
    template["pack_fingerprint"] = compute_pack_fingerprint(template)
    altered = build_semantic_refresh_dag_projection(
        template_pack=template,
        plan_bundle=_plan_bundle(),
    ).to_mapping()

    with pytest.raises(ValueError, match="local deployment authority"):
        materialize_semantic_refresh_dag(
            dag_projection=altered,
            dag_projection_authority=_dag_authority(protected),
            callables=_callables(),
        )


class _PackAuthorityCursor:
    def __init__(self, row: tuple[object, ...] | None) -> None:
        self.row = row
        self.statements: list[str] = []

    def execute(self, statement: str, *_parameters: object) -> _PackAuthorityCursor:
        self.statements.append(statement)
        return self

    def fetchone(self) -> tuple[object, ...] | None:
        value = self.row
        self.row = None
        return value

    def close(self) -> None:
        return None


class _PackAuthorityConnection:
    autocommit = True

    def __init__(self, cursor: _PackAuthorityCursor) -> None:
        self._cursor = cursor
        self.committed = False
        self.rolled_back = False

    def cursor(self) -> _PackAuthorityCursor:
        return self._cursor

    def commit(self) -> None:
        self.committed = True

    def rollback(self) -> None:
        self.rolled_back = True

    def close(self) -> None:
        return None


def _pack_authority_row(pack: dict[str, object]) -> tuple[object, ...]:
    _, identity = validate_semantic_refresh_activated_pack(pack)
    return (
        identity.pack_fingerprint,
        identity.activation_authority_receipt_sha256,
        identity.authority_store_ref,
        identity.workflow_execution_id,
        identity.workflow_execution_binding_sha256,
        identity.workflow_plan_sha256,
        identity.plan_bundle_sha256,
        identity.run_execution_bundle_sha256,
        "ACTIVE",
    )


def test_materializer_uses_run_neutral_index_authority_without_parse_io(airflow_33: None) -> None:
    projection = _dag_projection()
    dag = materialize_semantic_refresh_dag(
        dag_projection=projection,
        dag_projection_authority=_dag_authority(projection),
        callables=_callables(persist_summary=lambda _payload: pytest.fail("worker callable ran while parsing")),
    )

    assert isinstance(dag, _DAG)
    serialized = json.dumps([task.kwargs for task in dag.tasks], default=str)
    assert "scheduled__2026-08-08" not in serialized
    assert _binding_digest() not in serialized
    assert "{{ dag_run.run_id }}" in serialized


def test_materializer_rejects_rehashed_cross_release_sidecar_before_dag() -> None:
    protected = _dag_projection()
    payload = dict(protected)
    payload["release_id"] = _digest("e")
    unsigned = {key: value for key, value in payload.items() if key != "dag_projection_sha256"}
    payload["dag_projection_sha256"] = _canonical_digest(unsigned)

    with pytest.raises(SemanticRefreshProjectionError, match="plan projection|local deployment authority"):
        materialize_semantic_refresh_dag(
            dag_projection=payload,
            dag_projection_authority=_dag_authority(protected),
            callables=_callables(),
        )


def test_materializer_rejects_valid_sidecar_absent_from_verified_index() -> None:
    protected = _dag_projection()
    other_plan = _plan_bundle()
    authority = dict(other_plan["release_deployment_authority"])
    authority["release_id"] = _digest("e")
    other_plan["release_deployment_authority"] = authority
    unsigned = {key: value for key, value in other_plan.items() if key != "plan_bundle_sha256"}
    other_plan["plan_bundle_sha256"] = _canonical_digest(unsigned)
    other = build_semantic_refresh_dag_projection(
        template_pack=_template_pack(),
        plan_bundle=other_plan,
    ).to_mapping()

    with pytest.raises(ValueError, match="local deployment authority"):
        materialize_semantic_refresh_dag(
            dag_projection=other,
            dag_projection_authority=_dag_authority(protected),
            callables=_callables(),
        )


def test_dbt_worker_delegates_actual_dag_run_to_single_admission_capability(airflow_33: None) -> None:
    calls: list[dict[str, object]] = []

    def dbt_build_test(**kwargs: object) -> None:
        calls.append(dict(kwargs))

    projection = _dag_projection()
    dag = materialize_semantic_refresh_dag(
        dag_projection=projection,
        dag_projection_authority=_dag_authority(projection),
        callables=SemanticRefreshAirflowCallables(
            dbt_build_test=dbt_build_test,
            resolve_execution_binding=lambda **_kwargs: _binding_digest(),
            prepare=lambda **_kwargs: None,
            commit=lambda **_kwargs: None,
            read_publications=lambda **_kwargs: (),
            persist_summary=lambda payload: payload,
        ),
    )
    dbt_task = next(task for task in dag.tasks if task.task_id.endswith("dbt_build_test"))
    rendered = {**dbt_task.kwargs["op_kwargs"], "workflow_execution_id": "scheduled__2026-08-08"}

    dbt_task.kwargs["python_callable"](**rendered)

    assert calls == [
        {
            "dbt_execution_pack": {"schema": "test.semantic-refresh-dbt-execution-pack.v1"},
            "plan_bundle": _plan_bundle(),
            "profile_sha256": _digest("8"),
            "projection_identity": {
                "dag_projection_sha256": projection["dag_projection_sha256"],
                "deployment_id": _digest("3"),
                "package_artifacts_sha256": _digest("c"),
                "plan_bundle_sha256": _plan_bundle()["plan_bundle_sha256"],
                "pre_release_bundle_sha256": _digest("d"),
                "release_id": _digest("5"),
                "template_pack_fingerprint": _template_pack()["pack_fingerprint"],
                "topology_sha256": _topology()["topology_sha256"],
                "workflow_plan_sha256": _digest("f"),
            },
            "project_config_overlay": {},
            "topology_sha256": _topology()["topology_sha256"],
            "workflow_execution_id": "scheduled__2026-08-08",
        }
    ]


def test_mssql_activated_pack_authority_rejects_wrong_run_tuple_at_activation_boundary() -> None:
    pack = _activated_pack()
    wrong = list(_pack_authority_row(pack))
    wrong[3] = "scheduled__other-run"
    connection = _PackAuthorityConnection(_PackAuthorityCursor(tuple(wrong)))

    _, identity = validate_semantic_refresh_activated_pack(pack)
    with pytest.raises(MssqlActivatedPackAuthorityError, match="differs or is absent"):
        MssqlSemanticRefreshActivatedPackAuthority(lambda: connection).assert_authorized(identity)

    assert connection.rolled_back is True


@pytest.mark.integration_live
def test_real_airflow_33_dag_serializes_with_assets_only_on_terminal_task() -> None:
    if str(os.getenv("DPONE_RUN_INTEGRATION_LIVE", "0")).lower() not in {"1", "true", "yes", "on"}:
        pytest.skip("real Airflow 3.3 container probe is disabled")
    repository = Path(__file__).resolve().parents[1]
    command = [
        "docker",
        "run",
        "--rm",
        "-v",
        f"{repository}:/workspace:ro",
        "-e",
        (
            "PYTHONPATH=/workspace/src:"
            "/workspace/packages/dpone-airflow-pack/src:"
            "/workspace/packages/apache-airflow-providers-dpone/src"
        ),
        "apache/airflow:3.3.0-python3.12",
        "python",
        "/workspace/tests/fixtures/semantic-refresh-v2/publication/airflow_33_probe.py",
    ]
    completed = subprocess.run(command, check=True, capture_output=True, text=True)  # noqa: S603
    evidence = json.loads(completed.stdout.splitlines()[-1])

    assert evidence == {
        "airflow_version": "3.3.0",
        "dag_id": "semantic_refresh_airflow_33_probe",
        "outlet_task_ids": ["semantic_refresh__durable_workflow_summary"],
        "serialized": True,
        "task_count": 6,
    }
