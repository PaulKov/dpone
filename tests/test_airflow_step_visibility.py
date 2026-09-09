from __future__ import annotations

import importlib.util
import json
import sys
import types
from pathlib import Path

import pytest

from dpone.gitops.airflow_compact_process_plans import build_compact_process_plans
from dpone.gitops.airflow_compact_runtime import compact_runtime_command
from dpone.gitops.airflow_step_visibility import (
    AIRFLOW_VISIBILITY_GROUP,
    AIRFLOW_VISIBILITY_INLINE,
    AIRFLOW_VISIBILITY_TASK,
    AirflowStepVisibilityError,
    VisibleTaskBudget,
    build_visible_task_plan,
    estimate_visible_tasks,
    resolve_step_visibility,
)
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from tests.airflow_dag_spec_repo import write_batch_manifest, write_flow_manifest


def test_visibility_defaults_inline_and_accepts_all_public_modes() -> None:
    assert resolve_step_visibility({}) == AIRFLOW_VISIBILITY_INLINE
    assert resolve_step_visibility({"execution": {"visibility": "inline"}}) == AIRFLOW_VISIBILITY_INLINE
    assert resolve_step_visibility({"execution": {"visibility": "task"}}) == AIRFLOW_VISIBILITY_TASK
    assert resolve_step_visibility({"execution": {"visibility": "group"}}) == AIRFLOW_VISIBILITY_GROUP


def test_visibility_rejects_unknown_or_malformed_values() -> None:
    for raw in (
        {"execution": "inline"},
        {"execution": {"visibility": "hidden"}},
        {"execution": {"visibility": 1}},
    ):
        with pytest.raises(AirflowStepVisibilityError) as exc_info:
            resolve_step_visibility(raw)

        assert exc_info.value.code == "DPONE_AIRFLOW_VISIBILITY_INVALID"


@pytest.mark.parametrize(
    ("visibility", "separate_hooks", "expected"),
    [
        (AIRFLOW_VISIBILITY_INLINE, 0, 1),
        (AIRFLOW_VISIBILITY_INLINE, 4, 1),
        (AIRFLOW_VISIBILITY_TASK, 0, 2),
        (AIRFLOW_VISIBILITY_TASK, 4, 6),
        (AIRFLOW_VISIBILITY_GROUP, 4, 6),
    ],
)
def test_visible_task_estimate_counts_grouped_tasks_as_real_tasks(
    visibility: str,
    separate_hooks: int,
    expected: int,
) -> None:
    assert estimate_visible_tasks(visibility=visibility, separate_hook_count=separate_hooks) == expected


def test_visible_task_plan_warns_then_blocks_at_deterministic_boundaries() -> None:
    budget = VisibleTaskBudget(warn=3, maximum=5)

    within = build_visible_task_plan((1, 2), budget=budget)
    warning = build_visible_task_plan((1, 3), budget=budget)
    blocked = build_visible_task_plan((1, 5), budget=budget)

    assert within.to_jsonable() == {
        "estimated_total": 3,
        "warn_threshold": 3,
        "max_tasks": 5,
        "status": "within_budget",
    }
    assert warning.status == "warning"
    assert blocked.status == "blocked"


@pytest.mark.parametrize(
    ("warn", "maximum"),
    [(0, 5), (5, 0), (6, 5), (1, 251)],
)
def test_visible_task_budget_rejects_unbounded_or_inverted_limits(warn: int, maximum: int) -> None:
    with pytest.raises(AirflowStepVisibilityError) as exc_info:
        VisibleTaskBudget(warn=warn, maximum=maximum)

    assert exc_info.value.code == "DPONE_AIRFLOW_VISIBLE_TASK_BUDGET_INVALID"


def test_runtime_command_quotes_selector_on_build_plane_and_separates_hook_modes() -> None:
    selector = "dbo.orders; printf unsafe"
    manifest = "runtime manifests/orders daily.yaml"

    inline = compact_runtime_command(
        workload_id="orders",
        manifest=manifest,
        selector=selector,
        skip_separate_hooks=False,
    )
    expanded = compact_runtime_command(
        workload_id="orders",
        manifest=manifest,
        selector=selector,
        skip_separate_hooks=True,
    )

    assert "--selector 'dbo.orders; printf unsafe'" in inline
    assert "dpone run 'runtime manifests/orders daily.yaml'" in inline
    assert "DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1" not in inline
    assert "DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1" in expanded


def test_compact_process_plans_preserve_internal_batch_dependencies(tmp_path: Path) -> None:
    manifest = write_batch_manifest(tmp_path, "orders")
    workload = GitOpsWorkloadDefinition(
        workload_id="orders",
        manifest=manifest,
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={"image": "dpone:test", "airflow": {}},
        provenance={},
    )

    plans = build_compact_process_plans(
        workload=workload,
        runtime_manifest_path="runtime/orders.yaml",
        repo_root=tmp_path,
        output_path=".dpone/gitops/airflow/orders/airflow-pack.json",
    )

    by_selector = {plan.selector: plan for plan in plans}
    assert by_selector["public.t1"].depends_on_process_selectors == ()
    assert by_selector["public.t2"].depends_on_process_selectors == ("public.t1",)


def test_single_process_flow_uses_selector_bound_plan_with_stable_workload_node(tmp_path: Path) -> None:
    manifest = write_flow_manifest(tmp_path, "orders")
    workload = GitOpsWorkloadDefinition(
        workload_id="orders",
        manifest=manifest,
        domain="sales",
        catalog_path="domains/sales.yaml",
        effective_config={"image": "dpone:test", "airflow": {}},
        provenance={},
    )

    plans = build_compact_process_plans(
        workload=workload,
        runtime_manifest_path="runtime/orders.yaml",
        repo_root=tmp_path,
        output_path=".dpone/gitops/airflow/orders/airflow-pack.json",
    )

    assert len(plans) == 1
    assert plans[0].key == "orders"
    assert plans[0].node_id == "orders"
    assert plans[0].selector == "orders"
    assert "--selector orders" in plans[0].expanded_runtime_command


def test_selected_provider_node_requires_matching_immutable_process_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.node_materialization import PackNodeMaterialization
    from dpone_airflow_pack.pack_tasks import build_dpone_gitops_task_group_from_pack

    pack_path = tmp_path / "airflow-pack.json"
    pack_path.write_text(
        """{
          "kind": "gitops.airflow_pack",
          "schema_version": "3",
          "workload": {"workload_id": "orders"},
          "kpo_kwargs": {"task_id": "orders__dpone_runtime"},
          "runtime_selection": {"mode": "process_plan", "required_for_selected_nodes": true},
          "process_plans": {}
        }""",
        encoding="utf-8",
    )
    node = PackNodeMaterialization(
        node_id="orders__load_orders",
        workload_id="orders",
        selector="dbo.orders",
        visibility="inline",
        task_group=None,
    )

    with pytest.raises(ValueError, match="DPONE_AIRFLOW_PACK_SELECTOR_UNSUPPORTED"):
        build_dpone_gitops_task_group_from_pack(pack_path, dag=object(), node=node)


def test_legacy_unselected_pack_keeps_expanded_compatibility(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.node_materialization import PackNodeMaterialization
    from dpone_airflow_pack.pack_tasks import build_dpone_gitops_task_group_from_pack

    _install_python_operator(monkeypatch)
    pack_path = tmp_path / "legacy-pack.json"
    pack_path.write_text(
        """{
          "kind": "gitops.airflow_pack",
          "schema_version": "3",
          "workload": {"workload_id": "orders"},
          "kpo_kwargs": {"task_id": "orders__dpone_runtime"},
          "runtime_command": "dpone run orders.yaml --format json",
          "steps": [],
          "outcome_gate": {"required_status": "passed"}
        }""",
        encoding="utf-8",
    )
    node = PackNodeMaterialization(
        node_id="orders",
        workload_id="orders",
        selector=None,
        visibility="task",
        task_group=None,
    )

    tasks = build_dpone_gitops_task_group_from_pack(pack_path, dag=object(), node=node)

    # Legacy/non-exact packs (no launch_pin_required) keep outcome_gate without pin cleanup.
    assert sorted(tasks) == ["dpone_runtime", "outcome_gate"]
    assert tasks["dpone_runtime"].kwargs["arguments"] == ["dpone run orders.yaml --format json"]


def test_process_scoped_task_ids_are_stable_and_unique() -> None:
    from dpone_airflow_pack.node_materialization import PackNodeMaterialization

    node = PackNodeMaterialization(
        node_id="orders__load_orders",
        workload_id="orders",
        selector="dbo.orders",
        visibility="task",
        task_group=None,
    )

    assert node.task_id("pre_hook_refresh") == "orders__load_orders__pre_hook_refresh"
    assert node.task_id("dpone_runtime") == "orders__load_orders__dpone_runtime"
    assert node.task_id("outcome_gate") == "orders__load_orders__outcome_gate"


def test_dag_materializer_rejects_duplicate_node_ids_before_pack_wiring(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone_airflow_pack.dag_materializer as materializer

    monkeypatch.setattr(
        materializer,
        "wire_pack_workload",
        lambda *args, **kwargs: types.SimpleNamespace(entrypoints=(), terminal=object()),
    )
    node = {
        "node_id": "orders",
        "workload_id": "orders",
        "selector": "dbo.orders",
        "visibility": "inline",
    }

    with pytest.raises(ValueError, match="DPONE_AIRFLOW_TASK_ID_CONFLICT"):
        materializer._wire_nodes(
            {"nodes": [node, node]},
            dag=object(),
            repo_root=tmp_path,
            operator_overrides={},
        )


def test_provider_materializes_inline_and_expanded_selector_plans_without_duplication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.node_materialization import PackNodeMaterialization
    from dpone_airflow_pack.pack_tasks import build_dpone_gitops_task_group_from_pack

    _install_python_operator(monkeypatch)
    pack_path = _write_selector_pack(tmp_path)
    inline_node = PackNodeMaterialization(
        node_id="orders__load_orders",
        workload_id="orders",
        selector="dbo.orders",
        visibility="inline",
        task_group=None,
    )
    task_node = PackNodeMaterialization(
        node_id="orders__load_customers",
        workload_id="orders",
        selector="dbo.customers",
        visibility="task",
        task_group=None,
    )

    inline = build_dpone_gitops_task_group_from_pack(pack_path, dag=object(), node=inline_node)
    expanded = build_dpone_gitops_task_group_from_pack(pack_path, dag=object(), node=task_node)

    assert list(inline) == ["dpone_runtime"]
    assert inline["dpone_runtime"].kwargs["task_id"] == "orders__load_orders__dpone_runtime"
    assert "--selector dbo.orders" in inline["dpone_runtime"].kwargs["arguments"][0]
    assert inline["dpone_runtime"].inline_outcome_required_status == "passed"
    assert sorted(expanded) == ["dpone_runtime", "outcome_gate", "pre_hook_refresh_customers"]
    assert expanded["dpone_runtime"].kwargs["task_id"] == "orders__load_customers__dpone_runtime"
    assert "--selector dbo.customers" in expanded["dpone_runtime"].kwargs["arguments"][0]
    assert expanded["pre_hook_refresh_customers"].kwargs["task_id"] == (
        "orders__load_customers__pre_hook_refresh_customers"
    )
    assert expanded["outcome_gate"].kwargs["task_id"] == "orders__load_customers__outcome_gate"


def test_inline_process_with_separate_hook_stays_one_airflow_task(
    tmp_path: Path,
) -> None:
    from dpone_airflow_pack.node_materialization import PackNodeMaterialization
    from dpone_airflow_pack.pack_tasks import build_dpone_gitops_task_group_from_pack

    pack_path = _write_selector_pack(tmp_path)
    node = PackNodeMaterialization(
        node_id="orders__load_customers",
        workload_id="orders",
        selector="dbo.customers",
        visibility="inline",
        task_group=None,
    )

    tasks = build_dpone_gitops_task_group_from_pack(pack_path, dag=object(), node=node)

    assert list(tasks) == ["dpone_runtime"]
    assert "DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1" not in tasks["dpone_runtime"].kwargs["arguments"][0]


def test_complete_multi_process_pack_materializes_inside_explicit_task_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.pack_tasks import build_dpone_gitops_task_group_from_pack

    _install_python_operator(monkeypatch)
    pack_path = _write_selector_pack(tmp_path)
    group = object()

    tasks = build_dpone_gitops_task_group_from_pack(pack_path, dag=object(), task_group=group)

    assert sorted(tasks) == ["dpone_runtime", "outcome_gate"]
    assert all(task.kwargs["task_group"] is group for task in tasks.values())
    assert "--selector" not in tasks["dpone_runtime"].kwargs["arguments"][0]


def test_complete_multi_process_pack_without_group_keeps_legacy_whole_pack_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.pack_tasks import build_dpone_gitops_task_group_from_pack

    _install_python_operator(monkeypatch)
    tasks = build_dpone_gitops_task_group_from_pack(_write_selector_pack(tmp_path), dag=object())

    assert sorted(tasks) == ["dpone_runtime", "outcome_gate"]
    assert "--selector" not in tasks["dpone_runtime"].kwargs["arguments"][0]


def test_group_visibility_places_every_internal_task_in_the_same_task_group(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.node_materialization import PackNodeMaterialization
    from dpone_airflow_pack.pack_tasks import build_dpone_gitops_task_group_from_pack

    _install_python_operator(monkeypatch)
    pack_path = _write_selector_pack(tmp_path)
    group = object()
    node = PackNodeMaterialization(
        node_id="orders__load_customers",
        workload_id="orders",
        selector="dbo.customers",
        visibility="group",
        task_group="loads",
    )

    tasks = build_dpone_gitops_task_group_from_pack(pack_path, dag=object(), node=node, task_group=group)

    assert tasks
    assert all(task.kwargs["task_group"] is group for task in tasks.values())


def _run_identity(
    *,
    release_digest: str = "a",
    deployment_digest: str = "b",
    workload_digest: str = "d",
) -> dict[str, object]:
    return {
        "schema": "dpone.airflow-run-identity.v1",
        "release_id": "sha256:" + release_digest * 64,
        "deployment_id": "sha256:" + deployment_digest * 64,
        "dag_spec": {"id": "orders_daily", "sha256": "sha256:" + "c" * 64},
        "workload_pack": {"id": "orders", "sha256": "sha256:" + workload_digest * 64},
        "runtime_image_digest": "sha256:" + "e" * 64,
        "binding_set_ref": "sha256:" + "1" * 64,
        "connection_registry_ref": "sha256:" + "2" * 64,
        "credential_runtime_ref": "sha256:" + "3" * 64,
        "airflow_bundle": {
            "backend": "git",
            "ref": "git:7ac31f2",
            "versioned": True,
            "version": "7ac31f2",
            "snapshot_ref": None,
        },
    }


def _deployment_identity(
    *,
    release_digest: str = "a",
    deployment_digest: str = "b",
    activation_id: str = "3f60628e-ef48-48b0-84c3-a9e27a82a7f2",
) -> dict[str, object]:
    return {
        "schema": "dpone.airflow-deployment-identity.v1",
        "release_id": "sha256:" + release_digest * 64,
        "deployment_id": "sha256:" + deployment_digest * 64,
        "activation_id": activation_id,
    }


def _xcom_summary(
    *,
    status: str,
    run_identity: dict[str, object] | None = None,
    deployment_identity: dict[str, object] | None = None,
    evidence_digest: str = "f",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "kind": "gitops.airflow_xcom_summary",
        "schema_version": "1",
        "producer": "dpone gitops airflow run-spec-exec",
        "status": status,
        "runtime_profile_path": "runtime-profile.json",
        "run_spec_path": "run-spec.json",
        "runtime_evidence_path": "runtime-evidence.json",
        "runtime_evidence_sha256": "sha256:" + evidence_digest * 64,
        "runtime_evidence": {
            "schema_version": "dpone.airflow.inline_runtime_evidence.v1",
            "status": status,
            "metrics": {"duration_seconds": 1.0, "step_count": 1},
            "step_timeline": [],
        },
        "failed_step": None if status == "passed" else "runtime",
        "step_counts": {
            "total": 1,
            "passed": 1 if status == "passed" else 0,
            "failed": 0 if status == "passed" else 1,
        },
        "artifact_paths": {"runtime_evidence": "runtime-evidence.json"},
        "warnings": [],
        "blockers": [],
    }
    if run_identity is not None:
        payload["run_identity"] = run_identity
    if deployment_identity is not None:
        payload["deployment_identity"] = deployment_identity
    return payload


def test_inline_outcome_fails_the_runtime_task_without_a_second_airflow_task() -> None:
    from dpone_airflow_pack.outcome import evaluate_inline_pack_outcome

    passed = _xcom_summary(status="passed")
    failed = _xcom_summary(status="failed")

    assert evaluate_inline_pack_outcome(passed, task_id="orders__runtime") is passed
    with pytest.raises(RuntimeError, match="DPONE_AIRFLOW_INLINE_OUTCOME_FAILED"):
        evaluate_inline_pack_outcome(failed, task_id="orders__runtime")


def test_inline_outcome_rejects_duplicate_json_keys() -> None:
    from dpone_airflow_pack.outcome import evaluate_inline_pack_outcome

    with pytest.raises(RuntimeError, match="strict JSON object"):
        evaluate_inline_pack_outcome(
            '{"kind":"gitops.airflow_xcom_summary","status":"failed","status":"passed","blockers":[]}',
            task_id="orders__runtime",
        )


def test_inline_outcome_ignores_stale_local_success_when_current_operator_result_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import outcome as outcome_module
    from dpone_airflow_pack.outcome import evaluate_inline_pack_outcome

    sidecar = tmp_path / "return.json"
    sidecar.write_text(json.dumps(_xcom_summary(status="passed")), encoding="utf-8")
    monkeypatch.setattr(outcome_module, "_XCOM_RETURN_PATH", sidecar, raising=False)

    with pytest.raises(RuntimeError, match="DPONE_AIRFLOW_INLINE_OUTCOME_FAILED"):
        evaluate_inline_pack_outcome(
            _xcom_summary(status="failed"),
            task_id="orders__runtime",
        )


def test_separate_outcome_ignores_stale_local_success_when_current_task_xcom_failed(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack import outcome as outcome_module
    from dpone_airflow_pack.outcome import evaluate_pack_outcome

    sidecar = tmp_path / "return.json"
    sidecar.write_text(json.dumps(_xcom_summary(status="passed")), encoding="utf-8")
    monkeypatch.setattr(outcome_module, "_XCOM_RETURN_PATH", sidecar, raising=False)

    def _xcom_pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        del task_ids
        if key != "return_value":
            return None
        return _xcom_summary(status="failed")

    task_instance = types.SimpleNamespace(xcom_pull=_xcom_pull)

    with pytest.raises(RuntimeError, match="airflow_outcome_failed"):
        evaluate_pack_outcome(upstream_task_id="orders__runtime", ti=task_instance)


def test_separate_outcome_receives_expected_provider_run_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.outcome import build_pack_outcome_task

    _install_python_operator(monkeypatch)
    identity = _run_identity()
    deployment_identity = _deployment_identity()
    task = build_pack_outcome_task(
        pack={
            "outcome_gate": {"required_status": "passed"},
            "_dpone_run_identity": identity,
            "_dpone_deployment_identity": deployment_identity,
        },
        dag=object(),
        upstream_task_id="orders__runtime",
    )

    assert task.kwargs["op_kwargs"]["expected_run_identity"] == identity
    assert task.kwargs["op_kwargs"]["expected_deployment_identity"] == deployment_identity
    assert task.kwargs["op_kwargs"]["launch_pin_required"] is True


def test_inline_outcome_requires_exact_deployment_identity_when_expected() -> None:
    from dpone_airflow_pack.outcome import evaluate_inline_pack_outcome

    expected = _deployment_identity()
    matching = _xcom_summary(
        status="passed",
        run_identity=_run_identity(),
        deployment_identity=expected,
    )
    assert (
        evaluate_inline_pack_outcome(
            matching,
            task_id="orders__runtime",
            expected_run_identity=_run_identity(),
            expected_deployment_identity=expected,
        )
        is matching
    )

    for observed in (None, _deployment_identity(activation_id="4f60628e-ef48-48b0-84c3-a9e27a82a7f2")):
        with pytest.raises(RuntimeError, match="airflow_outcome_deployment_identity"):
            evaluate_inline_pack_outcome(
                _xcom_summary(
                    status="passed",
                    run_identity=_run_identity(),
                    deployment_identity=observed,
                ),
                task_id="orders__runtime",
                expected_run_identity=_run_identity(),
                expected_deployment_identity=expected,
            )


def test_inline_outcome_accepts_valid_exact_identity_without_provider_expectation() -> None:
    from dpone_airflow_pack.outcome import evaluate_inline_pack_outcome

    summary = _xcom_summary(
        status="passed",
        deployment_identity=_deployment_identity(),
    )

    assert evaluate_inline_pack_outcome(summary, task_id="orders__runtime") is summary


def test_inline_outcome_rejects_incomplete_or_open_success_summary() -> None:
    from dpone_airflow_pack.outcome import evaluate_inline_pack_outcome

    incomplete = {"kind": "gitops.airflow_xcom_summary", "status": "passed", "blockers": []}
    open_summary = _xcom_summary(status="passed")
    open_summary["unexpected"] = "must fail closed"

    for summary in (incomplete, open_summary):
        with pytest.raises(RuntimeError, match="DPONE_AIRFLOW_INLINE_OUTCOME_FAILED"):
            evaluate_inline_pack_outcome(summary, task_id="orders__runtime")


@pytest.mark.parametrize(
    ("summary", "expected_identity", "expected_evidence_digest"),
    [
        (
            _xcom_summary(
                status="passed",
                run_identity=_run_identity(release_digest="9"),
            ),
            _run_identity(),
            "sha256:" + "f" * 64,
        ),
        (
            _xcom_summary(
                status="passed",
                run_identity=_run_identity(),
                evidence_digest="9",
            ),
            _run_identity(),
            "sha256:" + "f" * 64,
        ),
    ],
)
def test_inline_outcome_rejects_mismatched_release_or_runtime_evidence_digest(
    summary: dict[str, object],
    expected_identity: dict[str, object],
    expected_evidence_digest: str,
) -> None:
    from dpone_airflow_pack.outcome import evaluate_inline_pack_outcome

    with pytest.raises(RuntimeError, match="DPONE_AIRFLOW_INLINE_OUTCOME_FAILED"):
        evaluate_inline_pack_outcome(
            summary,
            task_id="orders__runtime",
            expected_run_identity=expected_identity,
            expected_runtime_evidence_sha256=expected_evidence_digest,
        )


def test_inline_outcome_rejects_incomplete_identity_when_provider_identity_is_expected() -> None:
    from dpone_airflow_pack.outcome import evaluate_inline_pack_outcome

    incomplete_identity = _run_identity()
    incomplete_identity.pop("workload_pack")

    with pytest.raises(RuntimeError, match="DPONE_AIRFLOW_INLINE_OUTCOME_FAILED"):
        evaluate_inline_pack_outcome(
            _xcom_summary(status="passed", run_identity=incomplete_identity),
            task_id="orders__runtime",
            expected_run_identity=_run_identity(),
        )


def test_inline_outcome_rejects_passed_status_with_commit_unknown_recovery() -> None:
    from dpone_airflow_pack.outcome import evaluate_inline_pack_outcome

    summary = _xcom_summary(status="passed")
    summary["recovery"] = {
        "code": "COMMIT_UNKNOWN",
        "failure_boundary": "checkpoint_persistence",
        "target_state": "unknown",
        "checkpoint_state": "incomplete",
        "source_state": "not_advanced",
        "safe_to_retry": False,
        "operator_verification_required": True,
        "recovery_action": "operator_verification_required",
    }
    with pytest.raises(RuntimeError, match="COMMIT_UNKNOWN recovery"):
        evaluate_inline_pack_outcome(
            summary,
            task_id="orders__runtime",
        )


def test_inline_operator_validates_sync_and_deferrable_kpo_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.operators import PinnedXComSidecarKubernetesPodOperator

    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    passed = _xcom_summary(status="passed")
    failed = _xcom_summary(status="failed")
    monkeypatch.setattr(base_operator, "execute", lambda _self, _context: passed)
    monkeypatch.setattr(base_operator, "trigger_reentry", lambda _self, _context, _event: failed)
    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="orders__runtime",
        inline_outcome_required_status="passed",
    )

    assert operator.execute({}) is passed
    with pytest.raises(RuntimeError, match="DPONE_AIRFLOW_INLINE_OUTCOME_FAILED"):
        operator.trigger_reentry({}, {"status": "success"})


def test_inline_operator_binds_current_result_to_provider_run_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.operators import PinnedXComSidecarKubernetesPodOperator

    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    mismatched = _xcom_summary(
        status="passed",
        run_identity=_run_identity(release_digest="9"),
    )
    monkeypatch.setattr(base_operator, "execute", lambda _self, _context: mismatched)
    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="orders__runtime",
        inline_outcome_required_status="passed",
        params={"dpone_run_identity": _run_identity()},
    )

    with pytest.raises(RuntimeError, match="DPONE_AIRFLOW_INLINE_OUTCOME_FAILED"):
        operator.execute({})


def test_inline_operator_binds_current_result_to_provider_activation_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.operators import PinnedXComSidecarKubernetesPodOperator

    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    expected = _deployment_identity()
    mismatched = _xcom_summary(
        status="passed",
        deployment_identity=_deployment_identity(activation_id="4f60628e-ef48-48b0-84c3-a9e27a82a7f2"),
    )
    monkeypatch.setattr(base_operator, "execute", lambda _self, _context: mismatched)
    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="orders__runtime",
        inline_outcome_required_status="passed",
        params={"dpone_deployment_identity": expected},
    )
    task_instance = types.SimpleNamespace(xcom_push=lambda **_: None)

    with pytest.raises(RuntimeError, match="airflow_outcome_deployment_identity_mismatch"):
        operator.execute({"ti": task_instance})


def test_runtime_operator_pins_launch_envelope_after_pod_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone_airflow_pack.launch_pin import (
        AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY,
        ensure_launch_envelope_on_pod,
    )
    from dpone_airflow_pack.launch_pin_pod import InMemoryLaunchPinPodReader, set_launch_pin_pod_reader
    from dpone_airflow_pack.launch_pin_store import InMemoryLaunchPinStore, set_launch_pin_store
    from dpone_airflow_pack.operators import PinnedXComSidecarKubernetesPodOperator

    set_launch_pin_store(InMemoryLaunchPinStore())
    set_launch_pin_pod_reader(InMemoryLaunchPinPodReader())
    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    expected = _deployment_identity()
    run_identity = _run_identity()
    pushed: dict[str, object] = {}
    store: dict[str, object] = {}

    def _get_or_create_pod(_self: object, pod_request_obj: object, context: dict[str, object]) -> object:
        del context
        return pod_request_obj

    monkeypatch.setattr(base_operator, "get_or_create_pod", _get_or_create_pod)
    operator = PinnedXComSidecarKubernetesPodOperator(
        task_id="orders__runtime",
        namespace="airflow",
        pin_deployment_identity_for_separate_outcome_gate=True,
        params={
            "dpone_deployment_identity": expected,
            "dpone_run_identity": run_identity,
            "dpone_runtime_evidence_sha256": "sha256:" + "f" * 64,
        },
    )

    def _xcom_push(*, key: str, value: object) -> None:
        pushed.update({"key": key, "value": value})
        store[key] = value

    task_instance = types.SimpleNamespace(
        dag_id="dag",
        run_id="run",
        task_id="orders__runtime",
        map_index=-1,
        try_number=1,
        xcom_push=_xcom_push,
        xcom_pull=lambda **kwargs: store.get(str(kwargs.get("key"))),
    )
    pod = {
        "metadata": {
            "name": "orders-runtime-pod",
            "namespace": "airflow",
            "uid": "11111111-1111-4111-8111-111111111111",
        },
        "spec": {"containers": [{"name": "base", "env": []}]},
    }
    ensure_launch_envelope_on_pod(
        pod,
        run_identity=run_identity,
        deployment_identity=expected,
        expected_runtime_evidence_sha256="sha256:" + "f" * 64,
    )

    pinned = operator.get_or_create_pod(pod, {"ti": task_instance})
    assert pinned is pod
    assert pushed["key"] == AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY
    assert pushed["value"]["deployment_identity"] == expected
    assert pushed["value"]["run_identity"] == run_identity
    assert pushed["value"]["expected_runtime_evidence_sha256"] == "sha256:" + "f" * 64
    assert pushed["value"]["pod_uid"] == "11111111-1111-4111-8111-111111111111"
    set_launch_pin_store(None)
    set_launch_pin_pod_reader(None)


def test_separate_outcome_prefers_launch_pin_envelope_over_stale_parse_time() -> None:
    """Exact-cache tip flip mid-run must not fail a successful pack-exec."""

    from dpone_airflow_pack.launch_pin import build_launch_pin, ensure_launch_envelope_on_pod
    from dpone_airflow_pack.launch_pin_pod import (
        InMemoryLaunchPinPodReader,
        remember_launch_pin_pod,
        set_launch_pin_pod_reader,
    )
    from dpone_airflow_pack.launch_pin_resolve import AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY
    from dpone_airflow_pack.launch_pin_store import InMemoryLaunchPinStore, get_launch_pin_store, set_launch_pin_store
    from dpone_airflow_pack.outcome import evaluate_pack_outcome

    set_launch_pin_store(InMemoryLaunchPinStore())
    set_launch_pin_pod_reader(InMemoryLaunchPinPodReader())
    launch_deployment = _deployment_identity()
    launch_run = _run_identity()
    live_deployment = _deployment_identity(
        deployment_digest="c",
        activation_id="4f60628e-ef48-48b0-84c3-a9e27a82a7f2",
    )
    live_run = _run_identity(deployment_digest="c")
    pod = {
        "metadata": {
            "name": "orders-runtime-pod",
            "namespace": "airflow",
            "uid": "11111111-1111-4111-8111-111111111111",
            "annotations": {},
        },
        "spec": {"containers": [{"name": "base", "env": []}]},
    }
    ensure_launch_envelope_on_pod(
        pod,
        run_identity=launch_run,
        deployment_identity=launch_deployment,
        expected_runtime_evidence_sha256="sha256:" + "f" * 64,
    )
    remember_launch_pin_pod(pod)
    pin = build_launch_pin(
        attempt={
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "orders__runtime",
            "map_index": -1,
            "try_number": 1,
        },
        pod_namespace="airflow",
        pod_name="orders-runtime-pod",
        pod_uid="11111111-1111-4111-8111-111111111111",
        run_identity=launch_run,
        deployment_identity=launch_deployment,
        expected_runtime_evidence_sha256="sha256:" + "f" * 64,
    )
    from dpone_airflow_pack.launch_pin_locator import store_authority_digest

    pin["store_namespace"] = "airflow"
    pin["store_authority_digest"] = store_authority_digest(kubernetes_conn_id=None, namespace="airflow")
    store = get_launch_pin_store()
    assert store is not None
    locator = store.create_once(pin)
    from dpone_airflow_pack.launch_pin_commit import build_launch_pin_ref
    from dpone_airflow_pack.launch_pin_locator import LaunchPinStoreLocator, launch_pin_store_locator_to_mapping

    summary = _xcom_summary(
        status="passed",
        deployment_identity=launch_deployment,
        run_identity=launch_run,
    )
    summary["launch_pin_ref"] = build_launch_pin_ref(locator)
    attempt_store = launch_pin_store_locator_to_mapping(
        LaunchPinStoreLocator(kubernetes_conn_id=None, namespace="airflow")
    )
    enriched_locator = {
        **summary["launch_pin_ref"],
        **dict(locator),
        "launch_pin_ref": summary["launch_pin_ref"],
        "launch_pin_store": attempt_store,
    }

    def _xcom_pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if key == "return_value":
            return summary
        if key == AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY:
            return enriched_locator
        return None

    payload = evaluate_pack_outcome(
        upstream_task_id="orders__runtime",
        ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_xcom_pull),
        expected_deployment_identity=live_deployment,
        expected_run_identity=live_run,
        expected_runtime_evidence_sha256="sha256:" + "9" * 64,
        launch_pin_required=True,
        launch_pin_store={
            "kubernetes_conn_id": "kubernetes_default",
            "namespace": "other-tip-namespace",
            "authority_digest": store_authority_digest(
                kubernetes_conn_id="kubernetes_default",
                namespace="other-tip-namespace",
            ),
        },
    )
    assert payload["passed"] is True
    set_launch_pin_store(None)
    set_launch_pin_pod_reader(None)


def test_separate_outcome_still_fails_when_xcom_diverges_from_launch_pin() -> None:
    from dpone_airflow_pack.launch_pin import build_launch_pin, ensure_launch_envelope_on_pod
    from dpone_airflow_pack.launch_pin_pod import (
        InMemoryLaunchPinPodReader,
        remember_launch_pin_pod,
        set_launch_pin_pod_reader,
    )
    from dpone_airflow_pack.launch_pin_resolve import AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY
    from dpone_airflow_pack.launch_pin_store import InMemoryLaunchPinStore, get_launch_pin_store, set_launch_pin_store
    from dpone_airflow_pack.outcome import evaluate_pack_outcome

    set_launch_pin_store(InMemoryLaunchPinStore())
    set_launch_pin_pod_reader(InMemoryLaunchPinPodReader())
    launch = _deployment_identity()
    launch_run = _run_identity()
    other = _deployment_identity(
        deployment_digest="c",
        activation_id="4f60628e-ef48-48b0-84c3-a9e27a82a7f2",
    )
    pod = {
        "metadata": {
            "name": "orders-runtime-pod",
            "namespace": "airflow",
            "uid": "11111111-1111-4111-8111-111111111111",
            "annotations": {},
        },
        "spec": {"containers": [{"name": "base", "env": []}]},
    }
    ensure_launch_envelope_on_pod(
        pod,
        run_identity=launch_run,
        deployment_identity=launch,
        expected_runtime_evidence_sha256=None,
    )
    remember_launch_pin_pod(pod)
    pin = build_launch_pin(
        attempt={
            "dag_id": "dag",
            "run_id": "run",
            "task_id": "orders__runtime",
            "map_index": -1,
            "try_number": 1,
        },
        pod_namespace="airflow",
        pod_name="orders-runtime-pod",
        pod_uid="11111111-1111-4111-8111-111111111111",
        run_identity=launch_run,
        deployment_identity=launch,
        expected_runtime_evidence_sha256=None,
    )
    from dpone_airflow_pack.launch_pin_locator import (
        LaunchPinStoreLocator,
        launch_pin_store_locator_to_mapping,
        store_authority_digest,
    )

    pin["store_namespace"] = "airflow"
    pin["store_authority_digest"] = store_authority_digest(kubernetes_conn_id=None, namespace="airflow")
    store = get_launch_pin_store()
    assert store is not None
    locator = store.create_once(pin)
    from dpone_airflow_pack.launch_pin_commit import build_launch_pin_ref

    summary = _xcom_summary(status="passed", deployment_identity=other)
    summary["launch_pin_ref"] = build_launch_pin_ref(locator)
    attempt_store = launch_pin_store_locator_to_mapping(
        LaunchPinStoreLocator(kubernetes_conn_id=None, namespace="airflow")
    )
    enriched_locator = {
        **summary["launch_pin_ref"],
        **dict(locator),
        "launch_pin_ref": summary["launch_pin_ref"],
        "launch_pin_store": attempt_store,
    }

    def _xcom_pull(*, task_ids: str, key: str = "return_value", **_: object) -> object:
        if key == "return_value":
            return summary
        if key == AIRFLOW_RUNTIME_LAUNCH_PIN_XCOM_KEY:
            return enriched_locator
        return None

    with pytest.raises(RuntimeError, match="airflow_outcome_deployment_identity_mismatch"):
        evaluate_pack_outcome(
            upstream_task_id="orders__runtime",
            ti=types.SimpleNamespace(dag_id="dag", run_id="run", xcom_pull=_xcom_pull),
            expected_deployment_identity=other,
            launch_pin_required=True,
        )
    set_launch_pin_store(None)
    set_launch_pin_pod_reader(None)


def _load_operators_with_airflow_core_without_kpo(
    monkeypatch: pytest.MonkeyPatch,
) -> types.ModuleType:
    airflow = types.ModuleType("airflow")
    setattr(airflow, "__path__", [])
    setattr(
        airflow,
        "__spec__",
        importlib.util.spec_from_loader("airflow", loader=None, is_package=True),
    )
    monkeypatch.setitem(sys.modules, "airflow", airflow)
    for module_name in tuple(sys.modules):
        if module_name.startswith("airflow."):
            monkeypatch.delitem(sys.modules, module_name)
    airflow_models = types.ModuleType("airflow.models")
    setattr(
        airflow_models,
        "__spec__",
        importlib.util.spec_from_loader("airflow.models", loader=None),
    )
    monkeypatch.setitem(sys.modules, "airflow.models", airflow_models)

    module_name = "dpone_airflow_pack._missing_kpo_test_operators"
    module_path = (
        Path(__file__).resolve().parents[1] / "packages/dpone-airflow-pack/src/dpone_airflow_pack/operators.py"
    )
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, module_name, module)
    spec.loader.exec_module(module)
    return module


def test_airflow_core_without_kpo_fails_before_materializing_a_fake_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operators = _load_operators_with_airflow_core_without_kpo(monkeypatch)
    dag = types.SimpleNamespace(task_dict={})

    with pytest.raises(
        operators.KubernetesPodOperatorDependencyError,
        match="DPONE_AIRFLOW_KUBERNETES_PROVIDER_UNAVAILABLE",
    ):
        operators.PinnedXComSidecarKubernetesPodOperator(
            task_id="orders__runtime",
            dag=dag,
        )

    assert dag.task_dict == {}


def test_dependency_light_kpo_fallback_constructs_but_fails_closed_on_execution() -> None:
    from dpone_airflow_pack.operators import PinnedXComSidecarKubernetesPodOperator

    base_operator = PinnedXComSidecarKubernetesPodOperator.__mro__[1]
    if base_operator.__module__ != "dpone_airflow_pack.operators":
        pytest.skip("real Airflow Kubernetes provider is installed")
    operator = PinnedXComSidecarKubernetesPodOperator(task_id="orders__runtime")

    with pytest.raises(
        RuntimeError,
        match="DPONE_AIRFLOW_KUBERNETES_OPERATOR_EXECUTION_UNAVAILABLE",
    ):
        operator.execute({})
    with pytest.raises(
        RuntimeError,
        match="DPONE_AIRFLOW_KUBERNETES_OPERATOR_EXECUTION_UNAVAILABLE",
    ):
        operator.trigger_reentry({}, {"status": "success"})


def test_airflow_namespace_without_core_keeps_dependency_light_fallback(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone_airflow_pack.operators as operators

    monkeypatch.setattr(
        operators,
        "find_spec",
        lambda name: object() if name == "airflow" else None,
    )

    assert operators._airflow_core_available() is False


def test_airflow_models_module_identifies_installed_core(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone_airflow_pack.operators as operators

    monkeypatch.setattr(
        operators,
        "find_spec",
        lambda name: object() if name == "airflow.models" else None,
    )

    assert operators._airflow_core_available() is True


def _install_python_operator(monkeypatch: pytest.MonkeyPatch) -> None:
    airflow = sys.modules.get("airflow") or types.ModuleType("airflow")
    providers = sys.modules.get("airflow.providers") or types.ModuleType("airflow.providers")
    standard = types.ModuleType("airflow.providers.standard")
    operators = types.ModuleType("airflow.providers.standard.operators")
    python = types.ModuleType("airflow.providers.standard.operators.python")

    class PythonOperator:
        def __init__(self, **kwargs: object) -> None:
            self.kwargs = kwargs

        def __rshift__(self, other: object) -> object:
            return other

    python.PythonOperator = PythonOperator
    for name, module in {
        "airflow": airflow,
        "airflow.providers": providers,
        "airflow.providers.standard": standard,
        "airflow.providers.standard.operators": operators,
        "airflow.providers.standard.operators.python": python,
    }.items():
        monkeypatch.setitem(sys.modules, name, module)


def _write_selector_pack(tmp_path: Path) -> Path:
    import json

    def plan(
        selector: str,
        *,
        hook: str | None = None,
        depends_on: tuple[str, ...] = (),
    ) -> dict[str, object]:
        steps: list[dict[str, object]] = []
        if hook is not None:
            steps.append(
                {
                    "name": hook,
                    "phase": "pre_hook",
                    "command": f"dpone hooks execute manifest.yaml --selector {selector}",
                    "depends_on": [],
                }
            )
        steps.extend(
            [
                {
                    "name": "dpone_runtime",
                    "phase": "runtime",
                    "depends_on": [hook] if hook is not None else [],
                },
                {"name": "outcome_gate", "phase": "evidence", "depends_on": ["dpone_runtime"]},
            ]
        )
        return {
            "selector": selector,
            "dag_node": {
                "process_name": selector.rsplit(".", 1)[-1],
                "visibility": "inline",
                "task_group": None,
                "estimated_visible_tasks": 1,
                "depends_on_process_selectors": list(depends_on),
            },
            "runtime_commands": {
                "inline": f"dpone run runtime.yaml --selector {selector}",
                "expanded": f"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1 dpone run runtime.yaml --selector {selector}",
            },
            "steps": steps,
        }

    path = tmp_path / "airflow-pack.json"
    path.write_text(
        json.dumps(
            {
                "kind": "gitops.airflow_pack",
                "schema_version": "3",
                "workload": {"workload_id": "orders"},
                "kpo_kwargs": {"task_id": "orders__dpone_runtime", "image": "dpone:test"},
                "runtime_command": "dpone run runtime.yaml --format json",
                "steps": [
                    {"name": "dpone_runtime", "phase": "runtime", "depends_on": []},
                    {"name": "outcome_gate", "phase": "evidence", "depends_on": ["dpone_runtime"]},
                ],
                "outcome_gate": {"required_status": "passed"},
                "runtime_selection": {"mode": "process_plan", "required_for_selected_nodes": True},
                "process_plans": {
                    "dbo.orders": plan("dbo.orders"),
                    "dbo.customers": plan(
                        "dbo.customers",
                        hook="pre_hook_refresh_customers",
                        depends_on=("dbo.orders",),
                    ),
                },
            }
        ),
        encoding="utf-8",
    )
    return path
