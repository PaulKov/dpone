"""Immutable publication use case for pinned Airflow artifacts."""

from __future__ import annotations

import os
import shutil
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from dpone.runtime.airflow_artifact_delivery_models import (
    AirflowArtifactDeliveryError,
    ArtifactFile,
    ArtifactInventory,
    PublishReport,
    PublishRequest,
)
from dpone.runtime.airflow_artifact_delivery_support import (
    ArtifactRegistry,
    ArtifactRegistryError,
    download,
    from_cache_error,
    registry_unavailable,
    require_registry_ref,
    require_registry_scope,
    verify_download,
)
from dpone.runtime.airflow_artifact_inventory import build_publish_inventory
from dpone.runtime.airflow_artifact_publication_receipt import build_publication_commitment
from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    open_regular_file,
)
from dpone.runtime.deployment_cache_models import ValidatedDeploymentProjection
from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator


class AirflowArtifactPublisher:
    """Publish one already verified local release/deployment without overwrite."""

    def __init__(self, *, registry: ArtifactRegistry) -> None:
        self._registry = registry

    def publish(self, request: PublishRequest) -> PublishReport:
        inventory = prepare_publication(request)
        if request.publication_mode == "compatible":
            return self._publish_compatible(request, inventory)
        return self._publish_exact(request, inventory)

    def _publish_exact(
        self,
        request: PublishRequest,
        inventory: ArtifactInventory,
    ) -> PublishReport:
        registry_scope_id = require_registry_scope(
            self._registry,
            request.registry_scope_id,
        )
        created = 0
        existing = 0
        verified = 0
        published_release = False
        published_deployment = False
        try:
            with _verified_snapshot(inventory, request=request) as snapshot:
                with tempfile.TemporaryDirectory(prefix=".dpone-artifact-readback-") as readback_dir:
                    readback_root = Path(readback_dir).resolve()
                    verified_release: list[ArtifactFile] = []
                    verified_deployment: list[ArtifactFile] = []
                    for items, verified_items in (
                        (snapshot.release, verified_release),
                        (snapshot.deployment, verified_deployment),
                    ):
                        for item in items:
                            was_created = self._create_or_compare(item)
                            created += int(was_created)
                            existing += int(not was_created)
                            verified_items.append(self._readback(item, root=readback_root, request=request))
                            verified += 1
                            if item.completion_marker and item.key.parts[0] == "releases":
                                published_release = True
                            elif item.completion_marker and item.key.parts[0] == "deployments":
                                published_deployment = True
                    verified_inventory = ArtifactInventory(
                        release=tuple(verified_release),
                        deployment=tuple(verified_deployment),
                    )
                    _validate_remote_projection(
                        readback_root,
                        request=request,
                    )
                    commitment = build_publication_commitment(
                        verified_inventory,
                        request=request,
                        registry_scope_id=registry_scope_id,
                    )
        except AirflowArtifactDeliveryError as exc:
            exc.details.update(
                {
                    "created_objects": created,
                    "existing_equal_objects": existing,
                    "verified_objects": verified,
                    "published_release": published_release,
                    "published_deployment": published_deployment,
                }
            )
            raise
        except Exception as exc:
            raise AirflowArtifactDeliveryError(
                "DPONE_INTERNAL_AIRFLOW_ARTIFACT_DELIVERY_FAILED",
                "artifact publication failed unexpectedly",
                details={
                    "created_objects": created,
                    "existing_equal_objects": existing,
                    "verified_objects": verified,
                    "published_release": published_release,
                    "published_deployment": published_deployment,
                },
            ) from exc
        return PublishReport(
            status="published" if created else "no_op",
            release_id=request.release_id,
            deployment_id=request.deployment_id,
            environment=request.environment,
            artifact_registry_ref=request.artifact_registry_ref,
            created_objects=created,
            existing_equal_objects=existing,
            published_release=published_release,
            published_deployment=published_deployment,
            verified_objects=verified,
            publication_commitment=commitment,
            publication_mode=request.publication_mode,
        )

    def _publish_compatible(
        self,
        request: PublishRequest,
        inventory: ArtifactInventory,
    ) -> PublishReport:
        created = 0
        existing = 0
        published_release = False
        published_deployment = False
        try:
            with _verified_snapshot(inventory, request=request) as snapshot:
                for item in snapshot.objects:
                    was_created = self._create_or_compare_compatible(item, request=request)
                    created += int(was_created)
                    existing += int(not was_created)
                    if item.completion_marker and item.key.parts[0] == "releases":
                        published_release = True
                    elif item.completion_marker and item.key.parts[0] == "deployments":
                        published_deployment = True
        except AirflowArtifactDeliveryError as exc:
            exc.details.update(
                {
                    "created_objects": created,
                    "existing_equal_objects": existing,
                    "published_release": published_release,
                    "published_deployment": published_deployment,
                }
            )
            raise
        except Exception as exc:
            raise AirflowArtifactDeliveryError(
                "DPONE_INTERNAL_AIRFLOW_ARTIFACT_DELIVERY_FAILED",
                "artifact publication failed unexpectedly",
                details={
                    "created_objects": created,
                    "existing_equal_objects": existing,
                    "published_release": published_release,
                    "published_deployment": published_deployment,
                },
            ) from exc
        return PublishReport(
            status="published" if created else "no_op",
            release_id=request.release_id,
            deployment_id=request.deployment_id,
            environment=request.environment,
            artifact_registry_ref=request.artifact_registry_ref,
            created_objects=created,
            existing_equal_objects=existing,
            published_release=published_release,
            published_deployment=published_deployment,
        )

    def _create_or_compare(self, item: ArtifactFile) -> bool:
        try:
            result = self._registry.create_file(item.key, item.local_path)
        except ArtifactRegistryError as exc:
            raise registry_unavailable() from exc
        if result.created:
            if result.metadata.size_bytes != item.size_bytes or (
                result.metadata.advertised_sha256 is not None
                and result.metadata.advertised_sha256.lower() != item.sha256.lower()
            ):
                raise _immutability_conflict(item)
            return True
        if result.metadata.size_bytes != item.size_bytes:
            raise _immutability_conflict(item)
        return False

    def _readback(
        self,
        item: ArtifactFile,
        *,
        root: Path,
        request: PublishRequest,
    ) -> ArtifactFile:
        downloaded = root.joinpath(*item.key.parts)
        downloaded.parent.mkdir(parents=True, exist_ok=True)
        download(self._registry, item.key, downloaded, max_bytes=item.size_bytes)
        verify_download(
            downloaded,
            expected_size=item.size_bytes,
            expected_sha256=item.sha256,
            max_object_bytes=request.max_object_bytes,
        )
        return ArtifactFile(
            key=item.key,
            local_path=downloaded,
            size_bytes=item.size_bytes,
            sha256=item.sha256,
            completion_marker=item.completion_marker,
        )

    def _create_or_compare_compatible(
        self,
        item: ArtifactFile,
        *,
        request: PublishRequest,
    ) -> bool:
        was_created = self._create_or_compare(item)
        if was_created:
            return True
        with tempfile.TemporaryDirectory(prefix=".dpone-artifact-compare-") as temp_dir:
            downloaded = Path(temp_dir) / "object"
            download(self._registry, item.key, downloaded, max_bytes=item.size_bytes)
            verify_download(
                downloaded,
                expected_size=item.size_bytes,
                expected_sha256=item.sha256,
                max_object_bytes=request.max_object_bytes,
            )
        return False


def _validate_local_projection(request: PublishRequest) -> ValidatedDeploymentProjection:
    deployment_dir = request.cache_root / "deployments" / request.environment / request.deployment_dir_name
    try:
        projection = DeploymentCacheProjectionValidator(
            request.cache_root,
            max_artifact_bytes=request.max_object_bytes,
        ).validate_details(deployment_dir, environment=request.environment)
    except DeploymentCacheError as exc:
        raise from_cache_error(exc) from exc
    if projection.release_id != request.release_id or projection.deployment_id != request.deployment_id:
        raise AirflowArtifactDeliveryError(
            "DPONE_DEPLOYMENT_ID_MISMATCH",
            "validated deployment does not match the requested release/deployment pins",
        )
    _require_exact_projection(request, projection)
    return projection


def _validate_remote_projection(
    root: Path,
    *,
    request: PublishRequest,
) -> ValidatedDeploymentProjection:
    deployment_dir = root / "deployments" / request.environment / request.deployment_dir_name
    try:
        projection = DeploymentCacheProjectionValidator(
            root,
            max_artifact_bytes=request.max_object_bytes,
        ).validate_details(deployment_dir, environment=request.environment)
    except DeploymentCacheError as exc:
        raise from_cache_error(exc) from exc
    if projection.release_id != request.release_id or projection.deployment_id != request.deployment_id:
        raise AirflowArtifactDeliveryError(
            "DPONE_DEPLOYMENT_ID_MISMATCH",
            "remote read-back projection does not match the requested exact identities",
        )
    _require_exact_projection(request, projection)
    require_registry_ref(
        projection.deployment,
        projection.airflow_index,
        request.artifact_registry_ref,
    )
    return projection


def _require_exact_projection(
    request: PublishRequest,
    projection: ValidatedDeploymentProjection,
) -> None:
    if request.publication_mode != "exact":
        return
    if (projection.deployment.get("schema"), projection.airflow_index.get("schema")) not in {
        ("dpone.deployment-set.v2", "dpone.airflow-deployment-index.v2"),
        ("dpone.deployment-set.v3", "dpone.airflow-deployment-index.v3"),
    }:
        raise AirflowArtifactDeliveryError(
            "DPONE_EXACT_PUBLICATION_PROJECTION_REQUIRED",
            "exact publication requires matching deployment-set/index v2 or v3",
        )


def prepare_publication(request: PublishRequest) -> ArtifactInventory:
    """Validate and inventory all local bytes without registry or credential I/O."""

    projection = _validate_local_projection(request)
    require_registry_ref(projection.deployment, projection.airflow_index, request.artifact_registry_ref)
    try:
        return build_publish_inventory(request, projection)
    except DeploymentCacheError as exc:
        raise from_cache_error(exc) from exc


@contextmanager
def _verified_snapshot(
    inventory: ArtifactInventory,
    *,
    request: PublishRequest,
) -> Iterator[ArtifactInventory]:
    """Freeze and reverify all source bytes before the first registry operation."""

    with tempfile.TemporaryDirectory(prefix=".dpone-artifact-publish-") as temp_dir:
        root = Path(temp_dir)
        root.chmod(0o700)
        release = _snapshot_files(inventory.release, root=root / "release", request=request)
        deployment = _snapshot_files(inventory.deployment, root=root / "deployment", request=request)
        yield ArtifactInventory(release=release, deployment=deployment)


def _snapshot_files(
    files: tuple[ArtifactFile, ...],
    *,
    root: Path,
    request: PublishRequest,
) -> tuple[ArtifactFile, ...]:
    snapshot: list[ArtifactFile] = []
    for index, item in enumerate(files):
        destination = root / f"{index:06d}"
        destination.parent.mkdir(parents=True, exist_ok=True)
        try:
            descriptor = open_regular_file(
                item.local_path,
                missing_code="DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
                invalid_code="DPONE_ARTIFACT_REGISTRY_INCOMPLETE",
                label="artifact publication source",
                root=item.source_root or request.cache_root,
            )
        except DeploymentCacheError as exc:
            raise from_cache_error(exc) from exc
        with os.fdopen(descriptor, "rb") as source, destination.open("xb") as target:
            shutil.copyfileobj(source, target, length=1024 * 1024)
            target.flush()
            os.fsync(target.fileno())
        verify_download(
            destination,
            expected_size=item.size_bytes,
            expected_sha256=item.sha256,
            max_object_bytes=request.max_object_bytes,
        )
        snapshot.append(
            ArtifactFile(
                key=item.key,
                local_path=destination,
                size_bytes=item.size_bytes,
                sha256=item.sha256,
                completion_marker=item.completion_marker,
                source_root=root,
            )
        )
    return tuple(snapshot)


def _immutability_conflict(item: ArtifactFile) -> AirflowArtifactDeliveryError:
    return AirflowArtifactDeliveryError(
        "DPONE_ARTIFACT_REGISTRY_IMMUTABILITY_CONFLICT",
        "existing content-addressed object differs from local content",
        details={"logical_key": item.key.as_posix()},
    )


__all__ = ["AirflowArtifactPublisher", "prepare_publication"]
