"""Local integrity verification for Airflow deployment cache promotion."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.contracts import airflow_deployment
from dpone.gitops.release_set_validation import release_activation_failure, validate_release_set
from dpone.runtime.deployment_cache_artifact_verifier import (
    DEFAULT_MAX_CACHE_ARTIFACT_BYTES,
    DeploymentCacheArtifactVerifier,
)
from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    require_path_without_symlinks,
)
from dpone.runtime.deployment_cache_integrity_artifacts import (
    IndexArtifact as _IndexArtifact,
)
from dpone.runtime.deployment_cache_integrity_artifacts import (
    ReleaseArtifact as _ReleaseArtifact,
)
from dpone.runtime.deployment_cache_integrity_artifacts import (
    digest_dir as _digest_dir,
)
from dpone.runtime.deployment_cache_integrity_artifacts import (
    index_artifacts as _index_artifacts,
)
from dpone.runtime.deployment_cache_integrity_artifacts import (
    read_release_set as _read_release_set,
)
from dpone.runtime.deployment_cache_integrity_artifacts import (
    release_artifacts as _release_artifacts,
)
from dpone.runtime.deployment_cache_integrity_artifacts import (
    require_inside_root as _require_inside_root,
)
from dpone.runtime.deployment_cache_integrity_artifacts import (
    required_digest as _required_digest,
)
from dpone.runtime.deployment_cache_integrity_artifacts import (
    required_positive_size as _required_positive_size,
)
from dpone.runtime.deployment_cache_integrity_artifacts import (
    required_text as _required_text,
)

_BASE_ARTIFACT_SECTIONS = ("dag_specs", "workload_packs")


def require_release_activation_support(dbt_wire: str | None) -> None:
    """Translate release activation policy before pointer or audit mutation."""

    failure = release_activation_failure(dbt_wire)
    if failure is not None:
        raise DeploymentCacheError(failure.code, failure.message)


class DeploymentCacheIntegrityVerifier:
    """Verify one deployment's immutable release projection before promotion."""

    def __init__(
        self,
        cache_root: str | Path,
        *,
        max_artifact_bytes: int = DEFAULT_MAX_CACHE_ARTIFACT_BYTES,
    ) -> None:
        self._cache_root = Path(cache_root).resolve(strict=False)
        if max_artifact_bytes <= 0:
            raise ValueError("max_artifact_bytes must be positive")
        self._artifact_verifier = DeploymentCacheArtifactVerifier(
            self._cache_root,
            max_artifact_bytes=max_artifact_bytes,
        )

    def verify(self, *, index: Mapping[str, Any], index_path: Path, release_id: str) -> None:
        """Fail unless the release-set and every indexed DAG/pack are intact."""

        self.verify_details(index=index, index_path=index_path, release_id=release_id)

    def verify_details(self, *, index: Mapping[str, Any], index_path: Path, release_id: str) -> str | None:
        """Return the producer wire only after complete indexed-byte verification.

        Generic release v1 has no dbt producer. The compatibility ``verify``
        method keeps its original None return and performs these same checks.
        """

        if not airflow_deployment.is_canonical_sha256_digest(release_id):
            raise DeploymentCacheError(
                "DPONE_RELEASE_ID_INVALID",
                "deployment release identity must be a sha256 digest",
                path=index_path.as_posix(),
            )
        release_dir = self._cache_root / "releases" / _digest_dir(release_id)
        release_path = release_dir / "release-set.json"
        require_path_without_symlinks(release_path, root=self._cache_root, error_path=release_path)
        resolved_release_dir = release_dir.resolve(strict=False)
        _require_inside_root(
            resolved_release_dir,
            root=self._cache_root,
            code="DPONE_CACHE_PATH_ESCAPE",
            message="pinned release directory resolves outside the configured cache root",
            error_path=release_path,
        )
        _require_inside_root(
            release_path.resolve(strict=False),
            root=resolved_release_dir,
            code="DPONE_CACHE_PATH_ESCAPE",
            message="release-set resolves outside its pinned release directory",
            error_path=release_path,
        )
        release = _read_release_set(release_path, root=self._cache_root)
        release_schema = release.get("schema")
        if release_schema not in {
            "dpone.release-set.v1",
            "dpone.release-set.v2",
            "dpone.release-set.v3",
        }:
            raise DeploymentCacheError(
                "DPONE_RELEASE_SCHEMA_INVALID",
                "release-set schema is invalid",
                path=release_path.as_posix(),
            )
        declared_release_id = release.get("release_id")
        if not airflow_deployment.is_canonical_sha256_digest(declared_release_id):
            raise DeploymentCacheError(
                "DPONE_RELEASE_ID_INVALID",
                "release-set identity must be a canonical lowercase sha256 digest",
                path=release_path.as_posix(),
            )
        if declared_release_id != release_id:
            raise DeploymentCacheError(
                "DPONE_RELEASE_ID_MISMATCH",
                "release-set identity does not match the deployment release",
                path=release_path.as_posix(),
            )
        if airflow_deployment.release_id(release) != release_id:
            raise DeploymentCacheError(
                "DPONE_RELEASE_FINGERPRINT_MISMATCH",
                "release-set content does not match its content-addressed identity",
                path=release_path.as_posix(),
            )
        validation = validate_release_set(release)
        validation_failure = validation.failure
        if validation_failure is not None:
            raise DeploymentCacheError(
                validation_failure.code,
                validation_failure.message,
                path=release_path.as_posix(),
            )
        release_artifacts = release.get("artifacts")
        if not isinstance(release_artifacts, Mapping):
            raise DeploymentCacheError(
                "DPONE_RELEASE_ARTIFACTS_INVALID",
                "release-set artifacts must be an object",
                path=release_path.as_posix(),
            )
        from dpone.runtime.airflow_artifact_inventory import release_includes_runtime_payloads

        sections = (
            *_BASE_ARTIFACT_SECTIONS,
            *(("runtime_payloads",) if release_includes_runtime_payloads(release_schema, release_artifacts) else ()),
        )
        for section in sections:
            expected = _release_artifacts(
                release_artifacts.get(section),
                section=section,
                path=release_path,
            )
            indexed = _index_artifacts(index.get(section), section=section, path=index_path)
            self._verify_section(
                expected=expected,
                indexed=indexed,
                release_dir=release_dir,
                release_id=release_id,
                index_path=index_path,
            )
        if release_schema == "dpone.release-set.v3":
            from dpone.manifest.release_composition_files import verify_composition_transport_files

            try:
                verify_composition_transport_files(release_dir, release)
            except (ValueError, OSError) as exc:
                raise DeploymentCacheError(
                    "DPONE_COMPOSITION_INVALID", "composition transport inventory is incomplete or corrupt"
                ) from exc
        self._verify_semantic_refresh_sidecars(index=index, index_path=index_path)
        return validation.dbt_runtime_wire_contract

    def _verify_semantic_refresh_sidecars(
        self,
        *,
        index: Mapping[str, Any],
        index_path: Path,
    ) -> None:
        raw = index.get("semantic_refresh_dag_projections", [])
        if not isinstance(raw, list) or len(raw) > 64:
            raise DeploymentCacheError(
                "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
                "semantic-refresh DAG projection inventory is invalid",
                path=index_path.as_posix(),
            )
        release_id = index.get("release_id")
        deployment_id = index.get("deployment_id")
        if not isinstance(release_id, str) or not isinstance(deployment_id, str):
            raise DeploymentCacheError(
                "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
                "semantic-refresh DAG projection deployment identity is absent",
                path=index_path.as_posix(),
            )
        seen: set[str] = set()
        for value in raw:
            if not isinstance(value, Mapping):
                raise DeploymentCacheError(
                    "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
                    "semantic-refresh DAG projection descriptor must be an object",
                    path=index_path.as_posix(),
                )
            projection_sha256 = _required_digest(
                value,
                "dag_projection_sha256",
                code="DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
                path=index_path,
            )
            artifact_ref = _required_text(
                value,
                "artifact_ref",
                code="DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
                path=index_path,
            )
            artifact_sha256 = _required_digest(
                value,
                "artifact_sha256",
                code="DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
                path=index_path,
            )
            declared_bytes = _required_positive_size(
                value.get("artifact_bytes"),
                path=index_path,
            )
            filename = f"semantic-refresh-{projection_sha256.removeprefix('sha256:')}.dag-projection.json"
            expected_suffix = f"/{_digest_dir(deployment_id)}/{filename}"
            if (
                not artifact_ref.startswith("cache://deployments/")
                or not artifact_ref.endswith(expected_suffix)
                or filename in seen
            ):
                raise DeploymentCacheError(
                    "DPONE_AIRFLOW_INDEX_ARTIFACT_INVALID",
                    "semantic-refresh DAG projection is outside its pinned deployment",
                    path=index_path.as_posix(),
                )
            seen.add(filename)
            artifact_path = self._cache_root / artifact_ref.removeprefix("cache://")
            require_path_without_symlinks(
                artifact_path,
                root=self._cache_root,
                error_path=index_path,
            )
            self._artifact_verifier.verify_file(
                artifact_path,
                expected_sha256=artifact_sha256,
                declared_bytes=declared_bytes,
            )

    def _verify_section(
        self,
        *,
        expected: Mapping[str, _ReleaseArtifact],
        indexed: Mapping[str, _IndexArtifact],
        release_dir: Path,
        release_id: str,
        index_path: Path,
    ) -> None:
        if set(expected) != set(indexed):
            raise DeploymentCacheError(
                "DPONE_RELEASE_INDEX_ARTIFACT_MISMATCH",
                "airflow index artifacts do not match the immutable release-set",
                path=index_path.as_posix(),
            )
        for logical_id in sorted(expected):
            release_artifact = expected[logical_id]
            index_artifact = indexed[logical_id]
            artifact_ref = index_artifact.artifact_ref
            artifact_path = self._artifact_verifier.resolve(
                release_dir=release_dir,
                artifact_ref=artifact_ref,
                error_path=index_path,
            )
            expected_ref = f"cache://releases/{_digest_dir(release_id)}/{release_artifact.relative_path.as_posix()}"
            if artifact_ref != expected_ref or index_artifact.sha256.lower() != release_artifact.sha256.lower():
                raise DeploymentCacheError(
                    "DPONE_RELEASE_INDEX_ARTIFACT_MISMATCH",
                    "airflow index artifact identity does not match the immutable release-set",
                    path=index_path.as_posix(),
                )
            self._artifact_verifier.verify_file(
                artifact_path,
                expected_sha256=index_artifact.sha256,
                declared_bytes=index_artifact.declared_bytes,
            )


__all__ = ["DEFAULT_MAX_CACHE_ARTIFACT_BYTES", "DeploymentCacheIntegrityVerifier"]
