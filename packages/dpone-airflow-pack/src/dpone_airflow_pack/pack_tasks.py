"""Build Airflow tasks from one compact dpone GitOps pack."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone_airflow_pack import pack_task_helpers as task_helpers
from dpone_airflow_pack.asset_outlets import build_asset_outlets, outlet_specs_from_pack
from dpone_airflow_pack.init_fetch_contract import (
    InitFetchDeliveryContext,
    InitFetchProviderError,
)
from dpone_airflow_pack.init_fetch_pod import (
    attach_init_fetch_context,
    compose_init_fetch_operator_kwargs,
)
from dpone_airflow_pack.init_fetch_pod_guard import validate_strict_pack_extensions
from dpone_airflow_pack.launch_pin_cleanup import build_pack_launch_pin_cleanup_task
from dpone_airflow_pack.launch_pin_wiring import (
    apply_pin_keep_pod,
    attach_outcome_and_cleanup,
    closed_locator_mapping,
    pin_lifecycle_enabled,
)
from dpone_airflow_pack.mapped_tasks import (
    build_mapped_runtime,
    mapped_inline_required_status,
)
from dpone_airflow_pack.mapping import mapping_plan_from_pack
from dpone_airflow_pack.node_materialization import PackNodeMaterialization, materialize_process_plan
from dpone_airflow_pack.outcome import build_pack_outcome_task
from dpone_airflow_pack.pack_hook_ownership import (
    pre_hook_execution_selection,
    require_complete_workload_hook_ownership,
)
from dpone_airflow_pack.pack_identity import PackIdentityError, verify_pack_fingerprint
from dpone_airflow_pack.pack_provenance import load_dpone_airflow_pack_with_provenance
from dpone_airflow_pack.pack_task_runtime import (
    apply_run_identity,
    deserialize_pod_spec,
    operator_init_kwargs,
    pack_workload_id,
    runtime_operator_kwargs,
    with_run_identity,
)
from dpone_airflow_pack.pack_task_runtime import (
    operator_class as _operator_class,
)
from dpone_airflow_pack.pack_task_runtime import (
    operator_selection_pack as _operator_selection_pack,
)
from dpone_airflow_pack.provider_execution import require_provider_execution
from dpone_airflow_pack.xcom_sidecar import require_strict_xcom_sidecar_image

_chain = task_helpers.chain


def load_dpone_airflow_pack(
    pack_path: str | Path,
    *,
    expected_sha256: str | None = None,
) -> dict[str, Any]:
    """Load and validate a compact `gitops.airflow_pack` JSON file."""

    pack, _ = load_dpone_airflow_pack_with_provenance(pack_path, expected_sha256=expected_sha256)
    return pack


def build_dpone_gitops_task_group_from_pack(
    pack_path: str | Path,
    *,
    dag: Any = None,
    operator_overrides: Mapping[str, Any] | None = None,
    node: PackNodeMaterialization | None = None,
    task_group: Any = None,
    expected_sha256: str | None = None,
    run_identity_context: Mapping[str, Any] | None = None,
    confined_root: Path | None = None,
    delivery_context: InitFetchDeliveryContext | None = None,
) -> dict[str, Any]:
    """Build compact dpone Airflow tasks from one static pack.

    The helper intentionally performs no manifest parsing, GitOps rendering,
    Kubernetes API calls, or network I/O at Airflow parse time.
    """

    loaded_pack, provenance = load_dpone_airflow_pack_with_provenance(
        pack_path,
        expected_sha256=expected_sha256,
        confined_root=confined_root,
    )
    return _build_dpone_gitops_task_group_from_loaded_pack(
        loaded_pack,
        provenance=provenance,
        dag=dag,
        operator_overrides=operator_overrides,
        node=node,
        task_group=task_group,
        run_identity_context=run_identity_context,
        delivery_context=delivery_context,
    )


def _build_dpone_gitops_task_group_from_loaded_pack(
    loaded_pack: Mapping[str, Any],
    *,
    provenance: Mapping[str, Any],
    dag: Any,
    operator_overrides: Mapping[str, Any] | None,
    node: PackNodeMaterialization | None,
    task_group: Any,
    run_identity_context: Mapping[str, Any] | None = None,
    delivery_context: InitFetchDeliveryContext | None = None,
) -> dict[str, Any]:
    """Build tasks from the exact verified pack snapshot already consumed."""

    strict_workload_id = _preflight_delivery_pack(
        loaded_pack,
        provenance=provenance,
        delivery_context=delivery_context,
        expected_workload_id=node.workload_id if node is not None else None,
    )
    context = run_identity_context
    if context is None:
        context = getattr(dag, "_dpone_run_identity_context", None)
    pack = (
        with_run_identity(
            loaded_pack,
            provenance=provenance,
            dag=dag,
            node=node,
            run_identity_context=context,
            workload_id=strict_workload_id,
        )
        if isinstance(context, Mapping)
        else loaded_pack
    )
    return _build_tasks_from_loaded_pack(
        pack,
        dag=dag,
        operator_overrides=operator_overrides,
        node=node,
        task_group=task_group,
        delivery_context=delivery_context,
    )


def _preflight_delivery_pack(
    pack: Mapping[str, Any],
    *,
    provenance: Mapping[str, Any],
    delivery_context: InitFetchDeliveryContext | None,
    expected_workload_id: str | None,
) -> str | None:
    if delivery_context is None:
        return None
    validate_strict_pack_extensions(pack)
    require_strict_xcom_sidecar_image(pack)
    workload_id = require_provider_execution(
        pack,
        expected_workload_id=expected_workload_id,
    ).workload_id
    expected = delivery_context.workload_pack(workload_id)
    verified_pack_fingerprint = provenance.get("verified_pack_fingerprint")
    if _canonical_pack_sha256(verified_pack_fingerprint) is None:
        raise InitFetchProviderError(
            "DPONE_INIT_FETCH_PACK_MIGRATION_REQUIRED",
            "strict init-fetch requires a workload pack rebuilt with canonical whole-pack identity",
            path=workload_id,
        )
    try:
        derived_pack_fingerprint = verify_pack_fingerprint(pack)
    except PackIdentityError as exc:
        raise InitFetchProviderError(
            "DPONE_CACHE_CHECKSUM_MISMATCH",
            "strict workload pack identity does not match its loaded contents",
            path=workload_id,
        ) from exc
    if (
        _canonical_pack_sha256(provenance.get("pack_sha256")) != expected.sha256
        or verified_pack_fingerprint != derived_pack_fingerprint
        or verified_pack_fingerprint != expected.pack_fingerprint
    ):
        raise InitFetchProviderError(
            "DPONE_CACHE_CHECKSUM_MISMATCH",
            "verified scheduler pack does not match the strict deployment context",
            path=workload_id,
        )
    return workload_id


def _canonical_pack_sha256(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    digest = value.removeprefix("sha256:")
    if len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
        return None
    return f"sha256:{digest}"


def _build_tasks_from_loaded_pack(
    loaded_pack: Mapping[str, Any],
    *,
    dag: Any,
    operator_overrides: Mapping[str, Any] | None,
    node: PackNodeMaterialization | None,
    task_group: Any,
    delivery_context: InitFetchDeliveryContext | None = None,
) -> dict[str, Any]:
    pack = materialize_process_plan(loaded_pack, node) if node is not None else loaded_pack
    if delivery_context is not None and node is None:
        require_complete_workload_hook_ownership(pack)
    strict_workload_id = (
        pack_workload_id(
            pack,
            strict_provider_execution=True,
            expected_workload_id=node.workload_id if node is not None else None,
        )
        if delivery_context is not None
        else None
    )
    mapping_plan = mapping_plan_from_pack(pack)
    kwargs = runtime_operator_kwargs(
        pack,
        dag=dag,
        node=node,
        task_group=task_group,
        strict_provider_execution=delivery_context is not None,
        expected_workload_id=strict_workload_id,
    )
    kwargs["do_xcom_push"] = True
    outlets = build_asset_outlets(
        outlet_specs_from_pack(
            pack,
            mssql_asset_uri_by_ref=(delivery_context.mssql_asset_uri_by_ref if delivery_context is not None else None),
        )
    )
    if outlets:
        kwargs["outlets"] = outlets
    kwargs.update(
        task_helpers.safe_overrides(
            operator_overrides,
            node_scoped=node is not None,
            mapped=mapping_plan.is_mapped,
            strict=delivery_context is not None,
        )
    )
    apply_run_identity(kwargs=kwargs, pack=pack)
    if delivery_context is not None:
        assert strict_workload_id is not None
        kwargs = compose_init_fetch_operator_kwargs(
            pack=pack,
            kwargs=kwargs,
            context=delivery_context,
            workload_id=strict_workload_id,
            execution_kind="runtime",
            execution_scope="process" if node is not None else "workload",
            process_selector=node.selector if node is not None else None,
            hook_execution=("inline" if node is not None and node.visibility == "inline" else "externalized"),
        )
        kwargs["full_pod_spec"] = deserialize_pod_spec(kwargs["full_pod_spec"])
    step_tasks = (
        {}
        if node is not None and node.visibility == "inline"
        else _hook_step_tasks(
            pack=pack,
            dag=dag,
            operator_overrides=operator_overrides,
            node=node,
            task_group=task_group,
            delivery_context=delivery_context,
        )
    )
    inline_required_status = (
        mapped_inline_required_status(pack)
        if mapping_plan.is_mapped
        else task_helpers.inline_required_status(pack=pack, node=node)
    )
    separate_outcome_gate = (
        inline_required_status is None
        and not mapping_plan.is_mapped
        and isinstance(pack.get("outcome_gate"), Mapping)
        and bool(pack.get("outcome_gate"))
    )
    # Authority C pin lifecycle only when the pack contract requires a launch pin.
    pin_enabled = pin_lifecycle_enabled(pack=pack, separate_outcome_gate=separate_outcome_gate)
    kwargs = apply_pin_keep_pod(kwargs, pin_enabled=pin_enabled)
    init_kwargs = operator_init_kwargs(
        pack,
        kwargs,
        inline_required_status=inline_required_status,
        strict_runtime_image_ref=(
            delivery_context.runtime_image_for_workload(strict_workload_id)[0]
            if delivery_context is not None and strict_workload_id is not None
            else None
        ),
        pin_deployment_identity_for_separate_outcome_gate=pin_enabled,
        launch_pin_store=(closed_locator_mapping(pack, kpo_kwargs=kwargs) if pin_enabled else None),
    )
    selected_operator_class = _operator_class(_operator_selection_pack(pack, strict=delivery_context is not None))
    runtime = (
        build_mapped_runtime(
            plan=mapping_plan,
            dag=dag,
            init_kwargs=init_kwargs,
            operator_class=selected_operator_class,
        )
        if mapping_plan.is_mapped
        else selected_operator_class(dag=dag, **init_kwargs)
    )
    if delivery_context is not None:
        runtime = attach_init_fetch_context(runtime, delivery_context)
    tasks = {**step_tasks, "dpone_runtime": runtime}
    task_helpers.chain_pack_steps(pack, tasks)
    for terminal in task_helpers.terminal_hook_task_ids(pack):
        if terminal in tasks:
            _chain(tasks[terminal], runtime)
    return attach_outcome_and_cleanup(
        pack=pack,
        dag=dag,
        tasks=tasks,
        runtime=runtime,
        upstream_task_id=str(kwargs["task_id"]),
        pin_enabled=pin_enabled,
        closed_locator=init_kwargs.get("launch_pin_store"),
        inline_required_status=inline_required_status,
        is_mapped=mapping_plan.is_mapped,
        node=node,
        task_group=task_group,
        chain=_chain,
        build_outcome=build_pack_outcome_task,
        build_cleanup=build_pack_launch_pin_cleanup_task,
    )


def _hook_step_tasks(
    *,
    pack: Mapping[str, Any],
    dag: Any,
    operator_overrides: Mapping[str, Any] | None,
    node: PackNodeMaterialization | None,
    task_group: Any,
    delivery_context: InitFetchDeliveryContext | None = None,
) -> dict[str, Any]:
    tasks: dict[str, Any] = {}
    strict_workload_id = (
        pack_workload_id(
            pack,
            strict_provider_execution=True,
            expected_workload_id=node.workload_id if node is not None else None,
        )
        if delivery_context is not None
        else None
    )
    for step in task_helpers.pack_steps(pack):
        if step.get("phase") != "pre_hook":
            continue
        kwargs = runtime_operator_kwargs(
            pack,
            dag=dag,
            node=node,
            task_group=task_group,
            strict_provider_execution=delivery_context is not None,
            expected_workload_id=strict_workload_id,
        )
        step_name = str(step["name"])
        kwargs["task_id"] = node.task_id(step_name) if node is not None else step_name
        kwargs["name"] = str(kwargs["task_id"]).replace("_", "-")
        if delivery_context is None:
            kwargs["cmds"] = ["/bin/sh", "-ec"]
            kwargs["arguments"] = [str(step["command"])]
        kwargs["do_xcom_push"] = False
        if delivery_context is None and isinstance(kwargs.get("full_pod_spec"), Mapping):
            kwargs["full_pod_spec"] = task_helpers.pod_spec_for_step(
                kwargs["full_pod_spec"],
                step_name=str(step["name"]),
            )
        kwargs.update(
            task_helpers.safe_overrides(
                operator_overrides,
                node_scoped=node is not None,
                strict=delivery_context is not None,
            )
        )
        apply_run_identity(kwargs=kwargs, pack=pack)
        if delivery_context is not None:
            assert strict_workload_id is not None
            hook_scope, hook_selector = pre_hook_execution_selection(
                step,
                node_selector=node.selector if node is not None else None,
                node_scoped=node is not None,
            )
            kwargs = compose_init_fetch_operator_kwargs(
                pack=pack,
                kwargs=kwargs,
                context=delivery_context,
                workload_id=strict_workload_id,
                execution_kind="pre_hook",
                execution_scope=hook_scope,
                process_selector=hook_selector,
                hook_execution="externalized",
                hook_name=step_name,
            )
            kwargs["full_pod_spec"] = deserialize_pod_spec(kwargs["full_pod_spec"])
        operator = _operator_class(_operator_selection_pack(pack, strict=delivery_context is not None))(
            dag=dag,
            **operator_init_kwargs(
                pack,
                kwargs,
                strict_runtime_image_ref=(
                    delivery_context.runtime_image_for_workload(strict_workload_id)[0]
                    if delivery_context is not None and strict_workload_id is not None
                    else None
                ),
            ),
        )
        if delivery_context is not None:
            operator = attach_init_fetch_context(operator, delivery_context)
        tasks[str(step["name"])] = operator
    return tasks


__all__ = ["build_dpone_gitops_task_group_from_pack", "load_dpone_airflow_pack"]
