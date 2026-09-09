"""Compensating retirement for an active preview of an Airflow-disabled workload."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from dpone.readiness.airflow_authoring_check_service import (
    CheckedPipelineSource,
    CheckedPipelineSourceChangedError,
    checked_pipeline_source_unchanged,
    verify_checked_pipeline_source,
)
from dpone.readiness.airflow_deployment_projection import compute_deployment_id, compute_release_id
from dpone.readiness.airflow_self_service_cache_sync import PromotionPrecondition, local_cache_sync_result
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error
from dpone.readiness.airflow_self_service_templates import PREVIEW_ENV
from dpone.readiness.error_contract import error_docs_url, manual_fix
from dpone.runtime.deployment_cache import (
    DeploymentCacheCurrentState,
    DeploymentCacheError,
    DeploymentCacheMaterializer,
)
from dpone.runtime.deployment_cache_common import resolve_relative_current_symlink
from dpone.runtime.immutable_local_release import (
    ImmutableLocalReleaseError,
    materialize_immutable_local_release,
)

DeploymentTreeMaterializer = Callable[[Path, Mapping[str, bytes], Path], bool]


def airflow_disabled_preview_result(
    *,
    root: Path,
    pipeline_id: str,
    checked: CheckedPipelineSource,
    materialize_deployment_tree: DeploymentTreeMaterializer,
) -> SelfServiceResult:
    """Retire only a current single-workload preview, then report disabled."""

    cache_root = root / ".dpone-cache"
    try:
        active_id, advertised_ids = _active_preview_state(cache_root)
    except DeploymentCacheError as exc:
        return _failure(exc.code, str(exc), pipeline_id=pipeline_id, exit_code=4)
    if pipeline_id not in advertised_ids:
        return _disabled_result(pipeline_id, artifact_state="not_materialized")
    if advertised_ids != frozenset({pipeline_id}):
        return _disabled_result(
            pipeline_id,
            artifact_state="project_preview_refresh_required",
            current_deployment_id=active_id,
            refresh_project=True,
        )

    release = _empty_release()
    release_id = compute_release_id(release)
    release["release_id"] = release_id
    release_dir = cache_root / "releases" / release_id.replace(":", "-")
    deployment = _empty_deployment(release_id)
    deployment_id = compute_deployment_id(deployment)
    deployment["deployment_id"] = deployment_id
    deployment_dir = cache_root / "deployments" / PREVIEW_ENV / deployment_id.replace(":", "-")
    try:
        materialize_immutable_local_release(
            release_dir,
            {"release-set.json": _json_bytes(release)},
        )
        tree_materialized = materialize_deployment_tree(
            deployment_dir,
            {
                "deployment.json": _json_bytes(deployment),
                "airflow-index.json": _json_bytes(_empty_index(release_id=release_id, deployment_id=deployment_id)),
                "_SUCCESS": b"ok\n",
            },
            cache_root,
        )
        if not tree_materialized:
            return _failure(
                "DPONE_DEPLOYMENT_ALREADY_EXISTS",
                "The immutable preview retirement projection already exists with different content.",
                pipeline_id=pipeline_id,
                exit_code=4,
            )
        verify_checked_pipeline_source(root, checked)
    except CheckedPipelineSourceChangedError:
        return _failure(
            "DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD",
            "Pipeline authority changed while its stale Airflow preview was being retired.",
            pipeline_id=pipeline_id,
        )
    except ImmutableLocalReleaseError:
        return _failure(
            "DPONE_DEPLOYMENT_ALREADY_EXISTS",
            "The immutable preview retirement projection already exists with different content.",
            pipeline_id=pipeline_id,
            exit_code=4,
        )

    promotion = local_cache_sync_result(
        cache_root=cache_root,
        deployment_dir=deployment_dir,
        environment=PREVIEW_ENV,
        promoted_by="local://dpone-airflow-preview",
        expected_current_deployment_id=active_id,
        promotion_precondition=PromotionPrecondition(
            check=lambda: _retirement_preconditions_hold(
                cache_root=cache_root,
                active_id=active_id,
                pipeline_id=pipeline_id,
                root=root,
                checked=checked,
            ),
            code="DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD",
            message="Pipeline authority changed before stale preview retirement.",
        ),
    )
    if not promotion.passed:
        return promotion
    return _disabled_result(
        pipeline_id,
        artifact_state="retired",
        retired_deployment_id=active_id,
        deployment_id=deployment_id,
    )


def _active_preview_state(cache_root: Path) -> tuple[str | None, frozenset[str]]:
    active_id = DeploymentCacheCurrentState(cache_root).active_deployment_id(expected_environment=PREVIEW_ENV)
    if active_id is None:
        return None, frozenset()
    current_target = resolve_relative_current_symlink(cache_root)
    projection = DeploymentCacheMaterializer(cache_root).validate_current_details(
        current_target,
        environment=PREVIEW_ENV,
    )
    deployment = projection.deployment
    delivery = deployment.get("runtime_artifact_delivery")
    if (
        deployment.get("deployment_type") != "preview"
        or deployment.get("runnable") is not False
        or not isinstance(delivery, Mapping)
        or delivery.get("mode") != "local_preview"
    ):
        raise DeploymentCacheError(
            "DPONE_DEPLOYMENT_INVALID",
            "Only a verified non-runnable local preview can be retired automatically.",
            path=current_target.as_posix(),
        )
    descriptors = (
        *projection.airflow_index.get("dag_specs", ()),
        *projection.airflow_index.get("workload_packs", ()),
    )
    return active_id, frozenset(
        str(item["id"]) for item in descriptors if isinstance(item, Mapping) and isinstance(item.get("id"), str)
    )


def _retirement_preconditions_hold(
    *,
    cache_root: Path,
    active_id: str,
    pipeline_id: str,
    root: Path,
    checked: CheckedPipelineSource,
) -> bool:
    if not checked_pipeline_source_unchanged(root, checked):
        return False
    current_id, advertised_ids = _active_preview_state(cache_root)
    if current_id != active_id or advertised_ids != frozenset({pipeline_id}):
        raise DeploymentCacheError(
            "DPONE_CURRENT_POINTER_CAS_MISMATCH",
            "The active preview changed after retirement eligibility was checked.",
            path=(cache_root / "current").as_posix(),
        )
    return True


def _empty_release() -> dict[str, Any]:
    return {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": [],
            "workload_packs": [],
            "canonical_schemas": [],
        },
        "provenance": {
            "source": "local-preview-retirement",
            "built_by": "dpone airflow preview",
        },
    }


def _empty_deployment(release_id: str) -> dict[str, Any]:
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


def _empty_index(*, release_id: str, deployment_id: str) -> dict[str, Any]:
    return {
        "schema": "dpone.airflow-deployment-index.v1",
        "release_id": release_id,
        "deployment_id": deployment_id,
        "environment": PREVIEW_ENV,
        "dag_specs": [],
        "workload_packs": [],
        "binding_set_ref": None,
        "connection_registry_ref": None,
        "credential_runtime_ref": None,
        "runtime_image_digest": None,
        "airflow_bundle_ref": None,
        "runtime_artifact_delivery": {"mode": "local_preview"},
    }


def _disabled_result(
    pipeline_id: str,
    *,
    artifact_state: str,
    current_deployment_id: str | None = None,
    retired_deployment_id: str | None = None,
    deployment_id: str | None = None,
    refresh_project: bool = False,
) -> SelfServiceResult:
    code = "DPONE_AIRFLOW_DISABLED"
    fixes = [manual_fix("enable_airflow_in_authoring_source")]
    if refresh_project:
        fixes.insert(
            0,
            manual_fix(
                "refresh_project_preview_without_disabled_workload",
                command=f"dpone airflow preview . --exclude id:{pipeline_id}",
            ),
        )
    details: dict[str, Any] = {
        "pipeline_id": pipeline_id,
        "artifact_state": artifact_state,
    }
    if current_deployment_id is not None:
        details["current_deployment_id"] = current_deployment_id
    if retired_deployment_id is not None:
        details["retired_deployment_id"] = retired_deployment_id
    if deployment_id is not None:
        details["deployment_id"] = deployment_id
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                code,
                "The pipeline is valid but its primary authoring source disables Airflow materialization.",
                stage="airflow_preview",
                entity={"kind": "pipeline", "id": pipeline_id},
                fixes=fixes,
                docs_url=error_docs_url(code),
            ),
        ),
        details=details,
        exit_code=1,
    )


def _failure(
    code: str,
    message: str,
    *,
    pipeline_id: str,
    exit_code: int = 1,
) -> SelfServiceResult:
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                code,
                message,
                stage="airflow_preview",
                entity={"kind": "pipeline", "id": pipeline_id},
                docs_url=error_docs_url(code),
            ),
        ),
        exit_code=exit_code,
    )


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


__all__ = ["DeploymentTreeMaterializer", "airflow_disabled_preview_result"]
