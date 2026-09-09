"""Materialize one non-runnable Airflow preview deployment."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.readiness.airflow_authoring_check_service import AirflowAuthoringCheckService


import hashlib
import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.readiness.airflow_authoring_check_service import (
    CheckedPipelineSourceChangedError,
    checked_pipeline_source_unchanged,
    verify_checked_pipeline_source,
)
from dpone.readiness.airflow_authoring_validation import is_airflow_authoring_enabled
from dpone.readiness.airflow_deployment_projection import (
    compute_deployment_id,
    compute_release_id,
)
from dpone.readiness.airflow_local_workload_pack import (
    LocalWorkloadPackBuildError,
    build_verified_local_airflow_workload_pack,
    source_file_provenance,
)
from dpone.readiness.airflow_pipeline_scaffold import safe_pipeline_id
from dpone.readiness.airflow_preview_contract import PREVIEW_RUNTIME_IMAGE, PREVIEW_RUNTIME_IMAGE_DIGEST
from dpone.readiness.airflow_preview_dag_spec import preview_result_details
from dpone.readiness.airflow_preview_retirement import airflow_disabled_preview_result
from dpone.readiness.airflow_self_service_cache_sync import PromotionPrecondition, local_cache_sync_result
from dpone.readiness.airflow_self_service_models import SelfServiceResult, dpone_error
from dpone.readiness.airflow_self_service_templates import PREVIEW_ENV, dag_spec_payload
from dpone.readiness.error_contract import error_docs_url
from dpone.runtime.immutable_local_release import (
    ImmutableLocalReleaseError,
    materialize_immutable_local_release,
)
from dpone.runtime.immutable_local_tree import (
    ImmutableLocalTreeError,
    materialize_immutable_local_tree,
)


class AirflowPreviewService:
    """Compile and atomically activate a local preview projection."""

    def __init__(self, *, root: Path, authoring_check: AirflowAuthoringCheckService) -> None:
        self._root = root
        self._authoring_check = authoring_check

    def preview(self, pipeline_ref: str) -> SelfServiceResult:
        checked = self._authoring_check.inspect(pipeline_ref)
        if not checked.result.passed:
            return checked.result
        source_path = checked.source_path
        assert checked.payload is not None
        assert checked.compilation is not None
        assert checked.source_label is not None
        assert checked.source_sha256 is not None
        payload = checked.payload
        compilation = checked.compilation
        pipeline_id = _pipeline_id_from_source(payload, source_path)
        if not is_airflow_authoring_enabled(payload):
            return airflow_disabled_preview_result(
                root=self._root,
                pipeline_id=pipeline_id,
                checked=checked,
                materialize_deployment_tree=_materialize_retirement_deployment_tree,
            )
        try:
            pack = build_verified_local_airflow_workload_pack(
                root=self._root,
                source_path=source_path,
                pipeline_payload=payload,
                pipeline_id=pipeline_id,
                runtime_image=PREVIEW_RUNTIME_IMAGE,
                runtime_image_digest=PREVIEW_RUNTIME_IMAGE_DIGEST,
                expected_authoring_dependencies=compilation.dependencies,
                expected_primary_path=checked.source_label,
                expected_primary_sha256=checked.source_sha256,
            )
            dag_spec = dag_spec_payload(pipeline_id, payload, pack=pack)
        except LocalWorkloadPackBuildError as exc:
            return _failed_preview(exc, pipeline_id=pipeline_id)

        dag_bytes = _json_bytes(dag_spec)
        pack_bytes = _json_bytes(pack)
        dag_sha = _sha256_bytes(dag_bytes)
        pack_sha = _sha256_bytes(pack_bytes)
        release_payload = _release_payload(
            pipeline_id=pipeline_id,
            source_label=checked.source_label,
            workload_fingerprint=checked.workload_fingerprint,
            dag_sha=dag_sha,
            pack_sha=pack_sha,
            dag_bytes=len(dag_bytes),
            pack_bytes=len(pack_bytes),
            compilation=compilation,
            pack=pack,
        )
        release_id = compute_release_id(release_payload)
        release_payload["release_id"] = release_id
        release_dir_name = release_id.replace(":", "-")
        release_dir = self._root / ".dpone-cache" / "releases" / release_dir_name
        try:
            materialize_immutable_local_release(
                release_dir,
                {
                    f"dags/{pipeline_id}.dag-spec.json": dag_bytes,
                    f"packs/{pipeline_id}.airflow-pack.json": pack_bytes,
                    "release-set.json": _json_bytes(release_payload),
                },
            )
        except ImmutableLocalReleaseError:
            return _failed_immutable_preview(
                "DPONE_RELEASE_ALREADY_EXISTS",
                "The content-addressed preview release already exists with different content.",
                pipeline_id=pipeline_id,
            )

        deployment_payload = _preview_deployment_payload(release_id)
        deployment_id = compute_deployment_id(deployment_payload)
        deployment_payload["deployment_id"] = deployment_id
        deployment_dir = self._root / ".dpone-cache" / "deployments" / PREVIEW_ENV / deployment_id.replace(":", "-")
        index_payload = _preview_index_payload(
            pipeline_id=pipeline_id,
            release_id=release_id,
            deployment_id=deployment_id,
            release_dir_name=release_dir_name,
            dag_sha=dag_sha,
            pack_sha=pack_sha,
            dag_bytes=len(dag_bytes),
            pack_bytes=len(pack_bytes),
        )
        try:
            materialize_immutable_local_tree(
                deployment_dir,
                {
                    "deployment.json": _json_bytes(deployment_payload),
                    "airflow-index.json": _json_bytes(index_payload),
                    "_SUCCESS": b"ok\n",
                },
                allowed_parent=deployment_dir.parent,
                root=self._root / ".dpone-cache",
            )
        except ImmutableLocalTreeError:
            return _failed_immutable_preview(
                "DPONE_DEPLOYMENT_ALREADY_EXISTS",
                "The immutable preview deployment already exists with different content.",
                pipeline_id=pipeline_id,
            )
        try:
            verify_checked_pipeline_source(self._root, checked)
        except CheckedPipelineSourceChangedError:
            return _failed_immutable_preview(
                "DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD",
                "Pipeline authority changed while preview artifacts were being built.",
                pipeline_id=pipeline_id,
            )
        promotion = local_cache_sync_result(
            cache_root=self._root / ".dpone-cache",
            deployment_dir=deployment_dir,
            environment=PREVIEW_ENV,
            promoted_by="local://dpone-airflow-preview",
            promotion_precondition=PromotionPrecondition(
                check=lambda: checked_pipeline_source_unchanged(self._root, checked),
                code="DPONE_AUTHORING_SOURCE_CHANGED_DURING_BUILD",
                message="Pipeline authority changed before preview promotion.",
            ),
        )
        if not promotion.passed:
            return promotion
        return SelfServiceResult(
            passed=True,
            details=preview_result_details(
                pipeline_id=pipeline_id,
                release=release_payload,
                deployment=deployment_payload,
                dag_spec=dag_spec,
            ),
        )


def _release_payload(
    *,
    pipeline_id: str,
    source_label: str,
    workload_fingerprint: str | None,
    dag_sha: str,
    pack_sha: str,
    dag_bytes: int,
    pack_bytes: int,
    compilation: Any,
    pack: dict[str, Any],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": [
                {
                    "id": pipeline_id,
                    "path": f"dags/{pipeline_id}.dag-spec.json",
                    "sha256": dag_sha,
                    "bytes": dag_bytes,
                }
            ],
            "workload_packs": [
                {
                    "id": pipeline_id,
                    "path": f"packs/{pipeline_id}.airflow-pack.json",
                    "sha256": pack_sha,
                    "bytes": pack_bytes,
                }
            ],
            "canonical_schemas": [],
        },
        "provenance": {
            "source": source_label,
            "built_by": "dpone airflow preview",
            "authoring_mode": compilation.authoring_mode,
            "source_fingerprint": compilation.source_fingerprint,
            "semantic_fingerprint": compilation.semantic_fingerprint,
            "deprecated_aliases": list(compilation.deprecated_aliases),
            "source_files": source_file_provenance(pack),
        },
    }
    if workload_fingerprint is not None:
        payload["selection_fingerprint"] = canonical_fingerprint(
            {
                "schema": "dpone.single-workload-selection.v1",
                "workload_id": pipeline_id,
                "workload_fingerprint": workload_fingerprint,
            }
        )
        payload["provenance"]["workload_fingerprint"] = workload_fingerprint
    if compilation.recipe_provenance is not None:
        payload["provenance"]["recipe_resolution"] = dict(compilation.recipe_provenance)
    return payload


def _preview_deployment_payload(release_id: str) -> dict[str, Any]:
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


def _preview_index_payload(
    *,
    pipeline_id: str,
    release_id: str,
    deployment_id: str,
    release_dir_name: str,
    dag_sha: str,
    pack_sha: str,
    dag_bytes: int,
    pack_bytes: int,
) -> dict[str, Any]:
    return {
        "schema": "dpone.airflow-deployment-index.v1",
        "release_id": release_id,
        "deployment_id": deployment_id,
        "environment": PREVIEW_ENV,
        "dag_specs": [
            {
                "id": pipeline_id,
                "artifact_ref": f"cache://releases/{release_dir_name}/dags/{pipeline_id}.dag-spec.json",
                "sha256": dag_sha,
                "bytes": dag_bytes,
            }
        ],
        "workload_packs": [
            {
                "id": pipeline_id,
                "artifact_ref": f"cache://releases/{release_dir_name}/packs/{pipeline_id}.airflow-pack.json",
                "sha256": pack_sha,
                "bytes": pack_bytes,
            }
        ],
        "binding_set_ref": None,
        "connection_registry_ref": None,
        "credential_runtime_ref": None,
        "runtime_image_digest": None,
        "airflow_bundle_ref": None,
        "runtime_artifact_delivery": {"mode": "local_preview"},
    }


def _pipeline_id_from_source(payload: dict[str, Any], source_path: Path) -> str:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict) and isinstance(metadata.get("id"), str):
        return safe_pipeline_id(metadata["id"])
    return safe_pipeline_id(source_path.parent.name)


def _failed_preview(error: LocalWorkloadPackBuildError, *, pipeline_id: str) -> SelfServiceResult:
    return SelfServiceResult(
        passed=False,
        errors=(
            dpone_error(
                error.code,
                str(error),
                stage="airflow_preview",
                entity={"kind": "pipeline", "id": pipeline_id},
                docs_url=error_docs_url(error.code),
            ),
        ),
    )


def _failed_immutable_preview(code: str, message: str, *, pipeline_id: str) -> SelfServiceResult:
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
        exit_code=4,
    )


def _materialize_retirement_deployment_tree(
    deployment_dir: Path,
    files: Mapping[str, bytes],
    cache_root: Path,
) -> bool:
    try:
        materialize_immutable_local_tree(
            deployment_dir,
            files,
            allowed_parent=deployment_dir.parent,
            root=cache_root,
        )
    except ImmutableLocalTreeError:
        return False
    return True


def _json_bytes(payload: dict[str, Any]) -> bytes:
    return (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


__all__ = ["AirflowPreviewService"]
