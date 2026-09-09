"""Resolve Airflow node identity independently from process selection."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.manifest.models import LoadedManifest, ProcessSpec

_BATCH_KIND = "dpone.batch.v1"
_FLOW_KIND = "dpone.flow.v1"
_SELF_SERVICE_MODES = frozenset({"classic", "flow", "folder"})


@dataclass(frozen=True, slots=True)
class AirflowProcessIdentity:
    """One canonical process and its stable Airflow build-plane identity."""

    process: ProcessSpec
    node_id: str
    selector_required: bool


def resolve_airflow_process_identities(
    manifest: LoadedManifest,
    *,
    workload_id: str,
) -> tuple[AirflowProcessIdentity, ...]:
    """Return deterministic process nodes without weakening selector scope."""

    selector_required = requires_selector_scoped_execution(manifest)
    processes = manifest.processes if selector_required else manifest.processes[:1]
    preserve_workload_node = _preserves_single_process_workload_node(manifest, process_count=len(processes))
    return tuple(
        AirflowProcessIdentity(
            process=process,
            node_id=(
                workload_id if preserve_workload_node or not selector_required else f"{workload_id}__{process.name}"
            ),
            selector_required=selector_required,
        )
        for process in processes
    )


def requires_selector_scoped_execution(manifest: LoadedManifest) -> bool:
    """Return whether every emitted Airflow process must carry a selector."""

    return (
        manifest.source_kind == _FLOW_KIND
        or manifest.kind == _BATCH_KIND
        or any(process.selector for process in manifest.processes)
    )


def _preserves_single_process_workload_node(manifest: LoadedManifest, *, process_count: int) -> bool:
    if process_count != 1:
        return False
    authoring = manifest.raw.get("authoring")
    authoring_mode = str(authoring.get("mode") or "") if isinstance(authoring, dict) else ""
    return manifest.source_kind == _FLOW_KIND or authoring_mode in _SELF_SERVICE_MODES


__all__ = [
    "AirflowProcessIdentity",
    "requires_selector_scoped_execution",
    "resolve_airflow_process_identities",
]
