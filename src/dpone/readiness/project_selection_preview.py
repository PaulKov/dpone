"""Materialize one static non-runnable Airflow preview for selected workloads."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.readiness.project_selection_loader import ProjectSelectionOutcome


import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from dpone.readiness.airflow_authoring_check_service import (
    AirflowAuthoringCheckService,
    inspect_selected_sources,
)
from dpone.readiness.airflow_authoring_validation import is_airflow_authoring_enabled
from dpone.readiness.airflow_deployment_projection import compute_deployment_id, compute_release_id
from dpone.readiness.airflow_local_workload_pack import (
    LocalWorkloadPackBuildError,
    build_verified_local_airflow_workload_pack,
    source_file_provenance,
)
from dpone.readiness.airflow_preview_contract import PREVIEW_RUNTIME_IMAGE, PREVIEW_RUNTIME_IMAGE_DIGEST
from dpone.readiness.airflow_self_service_cache_sync import PromotionPrecondition, local_cache_sync_result
from dpone.readiness.airflow_self_service_models import SelfServiceResult
from dpone.readiness.airflow_self_service_templates import PREVIEW_ENV, dag_spec_payload
from dpone.readiness.project_selection import ProjectSelectionService, SelectionError, selection_error_result
from dpone.readiness.project_selection_dag_projection import selected_dag_payload
from dpone.runtime.immutable_local_release import ImmutableLocalReleaseError, materialize_immutable_local_release
from dpone.runtime.immutable_local_tree import ImmutableLocalTreeError, materialize_immutable_local_tree


class ProjectSelectionPreviewService:
    """Build and atomically promote a standard preview deployment projection."""

    def __init__(self, *, root: str | Path = ".") -> None:
        self._root = Path(root).resolve(strict=True)
        self._selection = ProjectSelectionService(root=self._root)
        self._authoring = AirflowAuthoringCheckService(root=self._root)

    def preview(
        self,
        *,
        target: str | Path,
        select: tuple[str, ...],
        exclude: tuple[str, ...],
        state_path: str | Path | None,
        selectors_path: str,
        max_selected: int,
    ) -> SelfServiceResult:
        try:
            outcome = self._selection.select(
                target=target,
                select=select,
                exclude=exclude,
                state_path=state_path,
                selectors_path=selectors_path,
                max_selected=max_selected,
            )
            reports, errors, exit_codes = inspect_selected_sources(
                outcome,
                authoring=self._authoring,
                mode="static",
                environment="local-preview",
            )
            if errors:
                return SelfServiceResult(
                    passed=False,
                    errors=tuple(errors),
                    details={
                        "selection": outcome.report.to_jsonable(),
                        "selected_checks": reports,
                    },
                    exit_code=max(exit_codes) if exit_codes else None,
                )
            selected_ids = {entry.node.node_id for entry in outcome.report.selected}
            disabled = tuple(
                sorted(
                    workload_id
                    for workload_id, checked in outcome.checked_sources.items()
                    if workload_id in selected_ids
                    and checked.payload is not None
                    and not is_airflow_authoring_enabled(checked.payload)
                )
            )
            if disabled:
                raise SelectionError(
                    "DPONE_AIRFLOW_DISABLED",
                    "Selected workloads disable Airflow materialization in their primary authoring source.",
                    context={"workload_ids": disabled},
                )
            artifacts = self._build_artifacts(outcome)
            self._selection.verify_consumed_files(dict(outcome.consumed_files))
            return self._materialize(
                outcome,
                artifacts,
                target=target,
                select=select,
                exclude=exclude,
                state_path=state_path,
                selectors_path=selectors_path,
                max_selected=max_selected,
            )
        except SelectionError as exc:
            return selection_error_result(exc, stage="airflow_preview_selection")
        except LocalWorkloadPackBuildError as exc:
            return selection_error_result(
                SelectionError(exc.code, str(exc)),
                stage="airflow_preview_selection",
            )
        except (ImmutableLocalReleaseError, ImmutableLocalTreeError):
            return selection_error_result(
                SelectionError("DPONE_SELECTION_STATE_CHANGED", "Selected preview artifacts changed during build."),
                stage="airflow_preview_selection",
            )

    def _build_artifacts(self, outcome: ProjectSelectionOutcome) -> _SelectedArtifacts:
        selected_ids = {entry.node.node_id for entry in outcome.report.selected}
        packs: dict[str, bytes] = {}
        pack_payloads: dict[str, dict[str, Any]] = {}
        source_files: dict[str, list[dict[str, str]]] = {}
        for workload_id in sorted(selected_ids):
            checked = outcome.checked_sources[workload_id]
            pack = build_verified_local_airflow_workload_pack(
                root=self._root,
                source_path=checked.source_path,
                pipeline_payload=dict(checked.payload),
                pipeline_id=workload_id,
                runtime_image=PREVIEW_RUNTIME_IMAGE,
                runtime_image_digest=PREVIEW_RUNTIME_IMAGE_DIGEST,
                expected_authoring_dependencies=cast(tuple[Any, ...], checked.compilation.dependencies),
                expected_primary_path=checked.source_label,
                expected_primary_sha256=checked.source_sha256,
            )
            pack_payloads[workload_id] = pack
            packs[workload_id] = _json_bytes(pack)
            source_files[workload_id] = source_file_provenance(pack)
        dag_specs, pruned = _selected_dag_specs(
            outcome,
            selected_ids=selected_ids,
            packs=pack_payloads,
        )
        return _SelectedArtifacts(packs, dag_specs, pruned, source_files)

    def _materialize(
        self,
        outcome: ProjectSelectionOutcome,
        artifacts: _SelectedArtifacts,
        *,
        target: str | Path,
        select: tuple[str, ...],
        exclude: tuple[str, ...],
        state_path: str | Path | None,
        selectors_path: str,
        max_selected: int,
    ) -> SelfServiceResult:
        dag_entries: list[dict[str, str | int]] = [
            {
                "id": dag_id,
                "path": f"dags/{dag_id}.dag-spec.json",
                "sha256": _sha256_bytes(content),
                "bytes": len(content),
            }
            for dag_id, content in sorted(artifacts.dag_specs.items())
        ]
        pack_entries: list[dict[str, str | int]] = [
            {
                "id": workload_id,
                "path": f"packs/{workload_id}.airflow-pack.json",
                "sha256": _sha256_bytes(content),
                "bytes": len(content),
            }
            for workload_id, content in sorted(artifacts.packs.items())
        ]
        release = {
            "schema": "dpone.release-set.v1",
            "release_id": "",
            "selection_fingerprint": outcome.report.selection_fingerprint,
            "artifacts": {
                "dag_specs": dag_entries,
                "workload_packs": pack_entries,
                "canonical_schemas": [],
            },
            "provenance": {
                "built_by": "dpone airflow preview --select",
                "selection": outcome.report.to_jsonable(),
                "source_files": artifacts.source_files,
                "pruned_boundary_edges": list(artifacts.pruned_boundary_edges),
            },
        }
        release_id = compute_release_id(release)
        release["release_id"] = release_id
        release_dir_name = release_id.replace(":", "-")
        cache_root = self._root / ".dpone-cache"
        release_dir = cache_root / "releases" / release_dir_name
        release_files: dict[str, bytes] = {
            **{f"dags/{dag_id}.dag-spec.json": content for dag_id, content in artifacts.dag_specs.items()},
            **{f"packs/{workload_id}.airflow-pack.json": content for workload_id, content in artifacts.packs.items()},
            "release-set.json": _json_bytes(release),
        }
        materialize_immutable_local_release(release_dir, release_files)
        deployment = _preview_deployment(release_id)
        deployment_id = compute_deployment_id(deployment)
        deployment["deployment_id"] = deployment_id
        index = _preview_index(
            release_id=release_id,
            deployment_id=deployment_id,
            release_dir_name=release_dir_name,
            dag_entries=dag_entries,
            pack_entries=pack_entries,
        )
        deployment_parent = cache_root / "deployments" / PREVIEW_ENV
        deployment_dir = deployment_parent / deployment_id.replace(":", "-")
        materialize_immutable_local_tree(
            deployment_dir,
            {
                "deployment.json": _json_bytes(deployment),
                "airflow-index.json": _json_bytes(index),
                "selection-state.json": _json_bytes(outcome.state.to_jsonable()),
                "_SUCCESS": b"ok\n",
            },
            allowed_parent=deployment_parent,
            root=cache_root,
        )
        promotion = local_cache_sync_result(
            cache_root=cache_root,
            deployment_dir=deployment_dir,
            environment=PREVIEW_ENV,
            promoted_by="local://dpone-airflow-preview",
            promotion_precondition=PromotionPrecondition(
                check=lambda: _selection_inputs_unchanged(
                    self._selection,
                    outcome,
                    target=target,
                    select=select,
                    exclude=exclude,
                    state_path=state_path,
                    selectors_path=selectors_path,
                    max_selected=max_selected,
                ),
                code="DPONE_SELECTION_STATE_CHANGED",
                message="Selection inputs changed before preview promotion.",
            ),
        )
        if not promotion.passed:
            return promotion
        return SelfServiceResult(
            passed=True,
            details={
                "selection_scope": True,
                "selected_workload_ids": sorted(entry.node.node_id for entry in outcome.report.selected),
                "release": release,
                "deployment": deployment,
                "selection": outcome.report.to_jsonable(),
                "pruned_boundary_edges": list(artifacts.pruned_boundary_edges),
                "airflow_index_path": ".dpone-cache/current/airflow-index.json",
                "selection_state_path": ".dpone-cache/current/selection-state.json",
            },
        )


@dataclass(frozen=True, slots=True)
class _SelectedArtifacts:
    packs: dict[str, bytes]
    dag_specs: dict[str, bytes]
    pruned_boundary_edges: tuple[dict[str, str], ...]
    source_files: dict[str, list[dict[str, str]]]


def _selection_inputs_unchanged(
    service: ProjectSelectionService,
    outcome: ProjectSelectionOutcome,
    *,
    target: str | Path,
    select: tuple[str, ...],
    exclude: tuple[str, ...],
    state_path: str | Path | None,
    selectors_path: str,
    max_selected: int,
) -> bool:
    try:
        current = service.select(
            target=target,
            select=select,
            exclude=exclude,
            state_path=state_path,
            selectors_path=selectors_path,
            max_selected=max_selected,
        )
    except SelectionError:
        return False
    return (
        current.graph.catalog_fingerprint == outcome.graph.catalog_fingerprint
        and current.report.selection_fingerprint == outcome.report.selection_fingerprint
        and current.state.state_fingerprint == outcome.state.state_fingerprint
        and dict(current.consumed_files) == dict(outcome.consumed_files)
    )


def _selected_dag_specs(
    outcome: ProjectSelectionOutcome,
    *,
    selected_ids: set[str],
    packs: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, bytes], tuple[dict[str, str], ...]]:
    dag_specs: dict[str, bytes] = {}
    covered: set[str] = set()
    represented_edges: set[tuple[str, str]] = set()
    selected_edges = {edge for edge in outcome.graph.edges if edge[0] in selected_ids and edge[1] in selected_ids}
    pruned = {edge for edge in outcome.graph.edges if (edge[0] in selected_ids) != (edge[1] in selected_ids)}
    for dag in outcome.dags:
        members = tuple(sorted(selected_ids.intersection(dag.workload_ids)))
        if not members:
            continue
        covered.update(members)
        member_set = set(members)
        edges = tuple(edge for edge in selected_edges if edge[0] in member_set and edge[1] in member_set)
        represented_edges.update(edges)
        payload = selected_dag_payload(dag, members=members, edges=edges, packs=packs)
        if dag.declaration.dag_id in dag_specs:
            raise SelectionError("DPONE_SELECTION_CATALOG_INVALID", "Duplicate DAG id in selected project.")
        dag_specs[dag.declaration.dag_id] = _json_bytes(payload)
    for workload_id in sorted(selected_ids - covered):
        checked = outcome.checked_sources[workload_id]
        payload = dag_spec_payload(workload_id, dict(checked.payload), pack=dict(packs[workload_id]))
        dag_specs[workload_id] = _json_bytes(payload)
    missing_edges = tuple(sorted(selected_edges - represented_edges))
    if missing_edges:
        raise SelectionError(
            "DPONE_SELECTION_CATALOG_INVALID",
            "Selected workload dependencies must be representable inside at least one declared DAG.",
            context={"unrepresented_edges": missing_edges[:20]},
        )
    return dag_specs, tuple({"upstream": upstream, "downstream": downstream} for upstream, downstream in sorted(pruned))


def _preview_deployment(release_id: str) -> dict[str, Any]:
    return {
        "schema": "dpone.deployment-set.v1",
        "deployment_id": "",
        "deployment_type": "preview",
        "runnable": False,
        "environment": PREVIEW_ENV,
        "release_ref": release_id,
        "binding_set_ref": None,
        "connection_registry_ref": None,
        "credential_runtime_ref": None,
        "runtime_image_digest": None,
        "airflow_bundle_ref": None,
        "runtime_artifact_delivery": {"mode": "local_preview"},
    }


def _preview_index(
    *,
    release_id: str,
    deployment_id: str,
    release_dir_name: str,
    dag_entries: list[dict[str, str | int]],
    pack_entries: list[dict[str, str | int]],
) -> dict[str, Any]:
    def indexed(entries: list[dict[str, str | int]]) -> list[dict[str, str | int]]:
        return [
            {
                "id": entry["id"],
                "artifact_ref": f"cache://releases/{release_dir_name}/{entry['path']}",
                "sha256": entry["sha256"],
                "bytes": entry["bytes"],
            }
            for entry in entries
        ]

    return {
        "schema": "dpone.airflow-deployment-index.v1",
        "release_id": release_id,
        "deployment_id": deployment_id,
        "environment": PREVIEW_ENV,
        "dag_specs": indexed(dag_entries),
        "workload_packs": indexed(pack_entries),
        "binding_set_ref": None,
        "connection_registry_ref": None,
        "credential_runtime_ref": None,
        "runtime_image_digest": None,
        "airflow_bundle_ref": None,
        "runtime_artifact_delivery": {"mode": "local_preview"},
    }


def _json_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _sha256_bytes(content: bytes) -> str:
    return "sha256:" + hashlib.sha256(content).hexdigest()


__all__ = ["ProjectSelectionPreviewService"]
