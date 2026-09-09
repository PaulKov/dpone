"""Process-scope execution contract for dbt execute-pack flavor packs.

A dbt DAG-spec node carries no selector, so the strict provider requests
``scope=process`` without a selector and the runtime launcher resolves the
``__default_process__`` bootstrap key plus the ``__default__`` process plan.
The dbt execute-pack generator must mirror both, exactly like the legacy
``dpone run`` flavor does for selectorless processes.
"""

from __future__ import annotations

import shlex
from copy import deepcopy
from types import SimpleNamespace
from typing import Any, cast

import pytest
from dpone_airflow_pack.node_materialization import (
    PackNodeMaterialization,
    materialize_process_plan,
)
from dpone_airflow_pack.pack_hook_ownership import require_complete_workload_hook_ownership
from dpone_airflow_pack.pack_identity import compute_pack_fingerprint

from dpone.gitops.airflow_compact_pack_bootstrap import (
    DEFAULT_PROCESS_BOOTSTRAP_KEY,
    WORKLOAD_BOOTSTRAP_KEY,
    runtime_workload_bootstrap,
)
from dpone.gitops.airflow_compact_process_plans import DEFAULT_PROCESS_PLAN_KEY
from dpone.readiness.airflow_preview_dag_spec import preview_process_edges, preview_process_nodes
from dpone.readiness.dbt_airflow_execution_pack import (
    DBT_EXECUTION_PACK_PATH,
    DbtAirflowExecutionPackBuilder,
)
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import RuntimeExecutionSelection, RuntimeInitFetchPlan
from dpone.runtime.verified_pack_command_selection import selected_verified_command

_XCOM_IMAGE = "registry.example/xcom-sidecar@sha256:" + "a" * 64
_WORKFLOW_ID = "daily_marts"
_WORKLOAD_ID = f"dbt__{_WORKFLOW_ID}"
_EXPECTED_ARGV = (
    "dpone",
    "dbt",
    "execute-pack",
    DBT_EXECUTION_PACK_PATH,
    "--format",
    "json",
)


def _dbt_pack() -> dict[str, Any]:
    execution_pack = SimpleNamespace(
        to_dict=lambda: {},
        timeout_seconds=3600,
        adapter_runtime=SimpleNamespace(airflow_execution_timeout_seconds=lambda timeout: timeout),
    )
    return DbtAirflowExecutionPackBuilder().build(
        workflow_id=_WORKFLOW_ID,
        execution_pack=execution_pack,  # type: ignore[arg-type]
        runtime_payload_ids=("dbt_project",),
        xcom_sidecar_image=_XCOM_IMAGE,
        pool="dpone_dbt",
    )


def _plan(execution: RuntimeExecutionSelection) -> RuntimeInitFetchPlan:
    return cast(RuntimeInitFetchPlan, SimpleNamespace(execution=execution))


def _process_scope_execution(hook_execution: str = "externalized") -> RuntimeExecutionSelection:
    return RuntimeExecutionSelection(
        kind="runtime",
        selector=_WORKLOAD_ID,
        scope="process",
        process_selector=None,
        hook_execution=hook_execution,
    )


def test_generator_emits_default_process_bootstrap_mirroring_workload() -> None:
    commands = _dbt_pack()["runtime_bootstrap"]["commands"]

    assert sorted(commands) == sorted((_WORKLOAD_ID, WORKLOAD_BOOTSTRAP_KEY, DEFAULT_PROCESS_BOOTSTRAP_KEY))
    assert commands[DEFAULT_PROCESS_BOOTSTRAP_KEY] == commands[WORKLOAD_BOOTSTRAP_KEY]
    assert commands[DEFAULT_PROCESS_BOOTSTRAP_KEY]["argv"] == list(_EXPECTED_ARGV)
    assert commands[DEFAULT_PROCESS_BOOTSTRAP_KEY]["env"] == {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"}


def test_generator_emits_one_selectorless_default_process_plan() -> None:
    plans = _dbt_pack()["process_plans"]

    assert sorted(plans) == [DEFAULT_PROCESS_PLAN_KEY]
    plan = plans[DEFAULT_PROCESS_PLAN_KEY]
    assert plan["selector"] is None
    assert plan["steps"] == []
    assert plan["runtime_commands"] == {
        "inline": shlex.join(_EXPECTED_ARGV),
        "expanded": shlex.join(_EXPECTED_ARGV),
    }
    assert plan["dag_node"] == {
        "process_name": _WORKFLOW_ID,
        "visibility": "task",
        "task_group": None,
        "estimated_visible_tasks": 2,
        "depends_on_process_selectors": [],
    }


def test_process_scope_without_selector_selects_execute_pack_command() -> None:
    argv, environment = selected_verified_command(_dbt_pack(), _plan(_process_scope_execution()))

    assert argv == _EXPECTED_ARGV
    assert environment == {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"}


def test_process_scope_inline_hooks_returns_empty_environment() -> None:
    argv, environment = selected_verified_command(
        _dbt_pack(),
        _plan(_process_scope_execution(hook_execution="inline")),
    )

    assert argv == _EXPECTED_ARGV
    assert environment == {}


def test_workload_scope_still_selects_workload_command() -> None:
    execution = RuntimeExecutionSelection(
        kind="runtime",
        selector=_WORKLOAD_ID,
        scope="workload",
        hook_execution="externalized",
    )

    argv, environment = selected_verified_command(_dbt_pack(), _plan(execution))

    assert argv == _EXPECTED_ARGV
    assert environment == {"DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS": "1"}


def test_process_scope_with_unknown_selector_still_fails_closed() -> None:
    execution = RuntimeExecutionSelection(
        kind="runtime",
        selector=_WORKLOAD_ID,
        scope="process",
        process_selector="unknown_process",
        hook_execution="externalized",
    )

    with pytest.raises(InitFetchError) as excinfo:
        selected_verified_command(_dbt_pack(), _plan(execution))

    assert excinfo.value.code == "DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED"


def test_pack_fingerprint_binds_default_process_command() -> None:
    pack = _dbt_pack()
    assert compute_pack_fingerprint(pack) == pack["pack_fingerprint"]

    tampered = deepcopy(pack)
    tampered["runtime_bootstrap"]["commands"][DEFAULT_PROCESS_BOOTSTRAP_KEY]["argv"][-1] = "text"

    assert compute_pack_fingerprint(tampered) != pack["pack_fingerprint"]


def test_legacy_flavor_bootstrap_unchanged() -> None:
    bootstrap = runtime_workload_bootstrap(
        runtime_manifest_path="runtime/manifest.json",
        workload_id="orders",
        process_selectors=(None,),
    )

    commands = cast(dict[str, Any], bootstrap["commands"])
    assert sorted(commands) == sorted(("orders", WORKLOAD_BOOTSTRAP_KEY, DEFAULT_PROCESS_BOOTSTRAP_KEY))
    assert commands[DEFAULT_PROCESS_BOOTSTRAP_KEY]["argv"] == [
        "dpone",
        "run",
        "runtime/manifest.json",
        "--format",
        "json",
    ]


def test_preview_dag_spec_projection_stays_legacy_shaped() -> None:
    pack = _dbt_pack()

    nodes = preview_process_nodes(_WORKLOAD_ID, pack)

    assert nodes == [
        {
            "node_id": _WORKLOAD_ID,
            "workload_id": _WORKLOAD_ID,
            "selector": None,
            "task_group": None,
            "visibility": "task",
            "estimated_visible_tasks": 2,
            "pack_ref": f"cached://workloads/{_WORKLOAD_ID}",
            "pack_path": f".dpone/gitops/airflow/{_WORKLOAD_ID}/airflow-pack.json",
        }
    ]
    assert preview_process_edges(_WORKLOAD_ID, pack, nodes) == []


def test_provider_materialization_keeps_whole_pack_for_selectorless_node() -> None:
    pack = _dbt_pack()
    node = PackNodeMaterialization(
        node_id=_WORKLOAD_ID,
        workload_id=_WORKLOAD_ID,
        selector=None,
        visibility="task",
        task_group=None,
    )

    assert materialize_process_plan(pack, node) == pack


def test_workload_hook_ownership_accepts_hookless_default_plan() -> None:
    require_complete_workload_hook_ownership(_dbt_pack())
