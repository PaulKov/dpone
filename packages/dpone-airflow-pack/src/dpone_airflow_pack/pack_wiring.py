"""Wire compact dpone packs into Airflow DAG dependencies."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone_airflow_pack.init_fetch_contract import (
    InitFetchDeliveryContext,
    InitFetchProviderError,
)
from dpone_airflow_pack.node_materialization import PackNodeMaterialization, materialize_process_plan
from dpone_airflow_pack.pack_provenance import load_dpone_airflow_pack_with_provenance
from dpone_airflow_pack.pack_storage_consumer import allows_git_fallback_on_cache_miss
from dpone_airflow_pack.pack_tasks import _build_dpone_gitops_task_group_from_loaded_pack
from dpone_airflow_pack.runtime_adapter import DponeAirflowContractError

PACK_CACHE_REF_PREFIX = "cached://"
CACHE_MISS_BLOCKER_CODE = "airflow_pack_cache_missing"
LEGACY_MISSING_BLOCKER_CODE = "airflow_pack_missing"


@dataclass(frozen=True)
class WiredPackWorkload:
    """Entry and terminal tasks for one wired workload node."""

    entrypoints: tuple[Any, ...]
    terminal: Any
    workload_id: str = ""
    runtime: Any = None


def pack_path(repo_root: Path, workload_id: str) -> Path:
    return repo_root / ".dpone/gitops/airflow" / workload_id / "airflow-pack.json"


def cached_pack_ref(workload_id: str) -> str:
    return f"{PACK_CACHE_REF_PREFIX}{workload_id}"


def resolve_pack_reference(
    repo_root: Path,
    workload_id: str,
) -> tuple[str | Path, Mapping[str, Any], Mapping[str, Any]]:
    ref = cached_pack_ref(workload_id)
    try:
        pack, provenance = load_dpone_airflow_pack_with_provenance(ref)
        return ref, pack, provenance
    except DponeAirflowContractError as exc:
        local_path = pack_path(repo_root, workload_id)
        if not allows_git_fallback_on_cache_miss() or not _is_cache_miss(exc, ref) or not local_path.exists():
            raise
        pack, provenance = load_dpone_airflow_pack_with_provenance(local_path)
        fallback_provenance = {
            **provenance,
            "source": "local_path",
            "fallback_reason": CACHE_MISS_BLOCKER_CODE,
            "cache_error_blockers": tuple(exc.blockers),
        }
        return local_path, pack, fallback_provenance


def wire_pack_workload(
    workload_id: str,
    *,
    dag: Any,
    repo_root: Path,
    operator_overrides: Mapping[str, Any] | None = None,
    pack_ref: str | Path | None = None,
    expected_sha256: str | None = None,
    confined_root: Path | None = None,
    node: PackNodeMaterialization | None = None,
    task_group: Any = None,
    delivery_context: InitFetchDeliveryContext | None = None,
) -> WiredPackWorkload:
    ref: str | Path
    pack: Mapping[str, Any]
    provenance: Mapping[str, Any]
    resolved_ref = str(pack_ref).strip() if pack_ref is not None else ""
    if resolved_ref:
        ref = resolved_ref
        try:
            pack, provenance = _load_pack_with_expected_digest(
                ref,
                expected_sha256,
                confined_root=confined_root,
            )
        except DponeAirflowContractError as exc:
            if delivery_context is not None:
                raise InitFetchProviderError(
                    "DPONE_INIT_FETCH_PACK_INVALID",
                    "strict indexed workload pack could not be verified",
                    path=workload_id,
                ) from None
            local_path = pack_path(repo_root, workload_id)
            if not allows_git_fallback_on_cache_miss() or not _is_cache_miss(exc, ref) or not local_path.exists():
                raise
            pack, provenance = load_dpone_airflow_pack_with_provenance(local_path)
            provenance = {
                **provenance,
                "source": "local_path",
                "fallback_reason": CACHE_MISS_BLOCKER_CODE,
                "cache_error_blockers": tuple(exc.blockers),
            }
    else:
        if delivery_context is not None:
            raise InitFetchProviderError(
                "DPONE_AIRFLOW_INDEX_FIELD_INVALID",
                "strict init-fetch requires an exact indexed workload pack_ref",
                path=workload_id,
            )
        ref, pack, provenance = resolve_pack_reference(repo_root, workload_id)
    tasks = _build_dpone_gitops_task_group_from_loaded_pack(
        pack,
        provenance=provenance,
        dag=dag,
        operator_overrides=operator_overrides,
        node=node,
        task_group=task_group,
        delivery_context=delivery_context,
    )
    _attach_pack_provenance(tasks, provenance)
    materialized_pack = materialize_process_plan(pack, node) if node is not None else pack
    entrypoints = tuple(tasks[name] for name in _pack_root_task_names(materialized_pack) if name in tasks) or (
        tasks["dpone_runtime"],
    )
    return WiredPackWorkload(
        entrypoints=entrypoints,
        terminal=_terminal_task(tasks),
        workload_id=workload_id,
        runtime=tasks["dpone_runtime"],
    )


def wire_workloads_in_waves(
    workload_ids: Sequence[str],
    *,
    dag: Any,
    repo_root: Path,
    operator_overrides: Mapping[str, Any] | None = None,
    max_parallel_workloads: int = 2,
    upstream: Any | None = None,
) -> list[Any]:
    if max_parallel_workloads < 1:
        raise ValueError("max_parallel_workloads must be positive")

    previous_wave = [upstream] if upstream is not None else []
    final_wave: list[Any] = []
    for offset in range(0, len(workload_ids), max_parallel_workloads):
        current_wave: list[Any] = []
        for workload_id in workload_ids[offset : offset + max_parallel_workloads]:
            wired = wire_pack_workload(
                workload_id,
                dag=dag,
                repo_root=repo_root,
                operator_overrides=operator_overrides,
            )
            for previous in previous_wave:
                for entrypoint in wired.entrypoints:
                    previous >> entrypoint
            current_wave.append(wired.terminal)
        previous_wave = current_wave
        final_wave = current_wave
    return final_wave


def _pack_root_task_names(pack: Mapping[str, Any]) -> tuple[str, ...]:
    steps = pack.get("steps")
    if not isinstance(steps, list):
        return ("dpone_runtime",)
    step_names = {str(step.get("name")) for step in steps if isinstance(step, Mapping) and step.get("name")}
    root_names = []
    for step in steps:
        if not isinstance(step, Mapping):
            continue
        name = str(step.get("name") or "")
        dependencies = step.get("depends_on") or []
        if name in step_names and not any(str(item) in step_names for item in dependencies):
            root_names.append(name)
    return tuple(root_names) or ("dpone_runtime",)


def _terminal_task(tasks: Mapping[str, Any]) -> Any:
    return tasks.get("outcome_gate") or tasks["dpone_runtime"]


def _load_pack_with_expected_digest(
    ref: str | Path,
    expected_sha256: str | None,
    *,
    confined_root: Path | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if expected_sha256 is None:
        return load_dpone_airflow_pack_with_provenance(ref)
    return load_dpone_airflow_pack_with_provenance(
        ref,
        expected_sha256=expected_sha256,
        confined_root=confined_root,
    )


def _is_cache_miss(exc: DponeAirflowContractError, ref: str) -> bool:
    if not str(ref).startswith(PACK_CACHE_REF_PREFIX):
        return False
    return any(_is_cache_miss_blocker(blocker) for blocker in exc.blockers)


def _is_cache_miss_blocker(blocker: Mapping[str, Any]) -> bool:
    code = blocker.get("code")
    if code == CACHE_MISS_BLOCKER_CODE:
        return True
    if code != LEGACY_MISSING_BLOCKER_CODE:
        return False
    return str(blocker.get("path") or "").startswith("cached:")


def _attach_pack_provenance(tasks: Mapping[str, Any], provenance: Mapping[str, Any]) -> None:
    compact = _compact_pack_provenance(provenance)
    annotations = {
        "dpone.io/pack-source": str(compact.get("source") or "unknown"),
        "dpone.io/pack-generation": str(compact.get("generation") or ""),
        "dpone.io/pack-sha256": str(compact.get("pack_sha256") or ""),
    }
    for task in tasks.values():
        _merge_task_mapping(task, "params", {"dpone_pack_provenance": compact})
    runtime = tasks.get("dpone_runtime")
    if runtime is not None:
        _merge_task_mapping(runtime, "annotations", annotations)


def _compact_pack_provenance(provenance: Mapping[str, Any]) -> dict[str, Any]:
    cache_status = provenance.get("cache_status")
    last_sync = cache_status.get("last_sync_status") if isinstance(cache_status, Mapping) else {}
    return {
        "kind": "dpone.airflow_pack_provenance",
        "schema_version": "1",
        "source": provenance.get("source"),
        "generation": provenance.get("generation"),
        "pack_sha256": provenance.get("pack_sha256"),
        "index_sha256": provenance.get("index_sha256"),
        "path": provenance.get("path"),
        "fallback_reason": provenance.get("fallback_reason"),
        "cache_status": {
            "status": cache_status.get("status") if isinstance(cache_status, Mapping) else None,
            "current_generation": cache_status.get("current_generation") if isinstance(cache_status, Mapping) else None,
            "last_sync_status": {
                "status": last_sync.get("status") if isinstance(last_sync, Mapping) else None,
                "reason": last_sync.get("reason") if isinstance(last_sync, Mapping) else None,
                "remote_latest_git_sha": last_sync.get("remote_latest_git_sha")
                if isinstance(last_sync, Mapping)
                else None,
            },
        },
    }


def _merge_task_mapping(task: object, field: str, values: Mapping[str, Any]) -> None:
    current = getattr(task, field, None)
    merged = dict(current) if isinstance(current, Mapping) else {}
    merged.update(values)
    try:
        setattr(task, field, merged)
    except Exception:  # noqa: BLE001 - operator implementations vary across Airflow versions.
        pass
    kwargs = getattr(task, "kwargs", None)
    if isinstance(kwargs, dict):
        current_kwargs = kwargs.get(field)
        merged_kwargs = dict(current_kwargs) if isinstance(current_kwargs, Mapping) else {}
        merged_kwargs.update(values)
        kwargs[field] = merged_kwargs


__all__ = [
    "WiredPackWorkload",
    "cached_pack_ref",
    "pack_path",
    "resolve_pack_reference",
    "wire_pack_workload",
    "wire_workloads_in_waves",
]
