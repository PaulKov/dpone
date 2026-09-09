"""Strict indexed-KPO init-fetch execution and receipt verification."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.ports.airflow_deployment_attestation import (
        AirflowDeploymentAttestationVerifier,
    )
    from dpone.ports.artifact_registry import ArtifactRegistryReader
    from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan


import os
import tempfile
from pathlib import Path

from dpone.ports.artifact_registry import ArtifactRegistryAuthority
from dpone.ports.runtime_artifact_attestation import (
    RuntimeArtifactAttestationVerifier,
)
from dpone.runtime.airflow_deployment_attestation_subject import (
    subject_from_runtime_artifacts,
)
from dpone.runtime.dbt_project_bundle import extract_dbt_project_bundle
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_artifacts import (
    RuntimeArtifactStager,
    publish_runtime_artifacts,
)
from dpone.runtime.runtime_init_fetch_attestation import (
    StagedRuntimeArtifact,
)
from dpone.runtime.runtime_init_fetch_attestation_verification import (
    verify_runtime_attestation,
)
from dpone.runtime.runtime_init_fetch_ready import (
    ATTESTATION_REQUIREMENT_REQUIRED,
    RUNTIME_FETCH_READY_NAME,
    RuntimeFetchReady,
    effective_attestation_requirement,
    parse_ready_manifest,
    ready_manifest_bytes,
)
from dpone.runtime.runtime_init_fetch_ready_builder import build_runtime_fetch_ready
from dpone.runtime.runtime_init_fetch_receipts import validate_runtime_receipts
from dpone.runtime.runtime_init_fetch_storage import (
    ReadyPublicationError,
    read_bounded_regular_file,
    read_verified_file,
    require_safe_directory,
    write_ready_last,
)
from dpone.runtime.runtime_payload_archive import (
    discard_runtime_payload,
    extract_runtime_payload,
    runtime_payload_archive,
)
from dpone.runtime.verified_pack_launcher import VerifiedPackLauncher

DEFAULT_STRICT_MAX_ARTIFACT_BYTES = 256 * 1024 * 1024
DEFAULT_STRICT_MAX_TOTAL_BYTES = 512 * 1024 * 1024


RuntimeInitFetchAttestationVerifier = RuntimeArtifactAttestationVerifier


class RuntimeInitFetchExecutor:
    """Fetch exact v2 artifacts and publish a ready record only after verification."""

    def __init__(
        self,
        *,
        registry: ArtifactRegistryReader,
        artifact_root: Path,
        worktree_root: Path,
        attestation_verifier: RuntimeInitFetchAttestationVerifier | None = None,
        deployment_attestation_verifier: AirflowDeploymentAttestationVerifier | None = None,
        trusted_attestation_required: bool = False,
        max_artifact_bytes: int = DEFAULT_STRICT_MAX_ARTIFACT_BYTES,
        max_total_bytes: int = DEFAULT_STRICT_MAX_TOTAL_BYTES,
    ) -> None:
        self._registry = registry
        self._artifact_root = artifact_root.absolute()
        self._worktree_root = worktree_root.absolute()
        self._attestation_verifier = attestation_verifier
        self._deployment_attestation_verifier = deployment_attestation_verifier
        self._trusted_attestation_required = bool(trusted_attestation_required)
        self._max_artifact_bytes = _positive_limit(max_artifact_bytes, "max_artifact_bytes")
        self._max_total_bytes = _positive_limit(max_total_bytes, "max_total_bytes")

    def execute(self, plan: RuntimeInitFetchPlan, *, plan_sha256: str) -> RuntimeFetchReady:
        """Execute after the caller has decoded and hash-validated the untrusted plan."""

        attestation_required = self._preflight(plan)
        self._artifact_root.mkdir(parents=True, exist_ok=True, mode=0o700)
        require_safe_directory(self._artifact_root)
        existing_ready = load_existing_runtime_ready(
            plan,
            plan_sha256=plan_sha256,
            attestation_required=attestation_required,
            deployment_attestation_required=self._deployment_attestation_verifier is not None,
            artifact_root=self._artifact_root,
            worktree_root=self._worktree_root,
        )
        if existing_ready is not None:
            return existing_ready
        with tempfile.TemporaryDirectory(
            prefix=".dpone-runtime-init-fetch-",
            dir=self._artifact_root,
        ) as staging_name:
            staging_root = Path(staging_name)
            staged = RuntimeArtifactStager(self._registry).stage(
                staging_root,
                plan.artifacts,
            )
            payloads = {
                item.descriptor.artifact_ref: read_verified_file(
                    item.path,
                    expected_sha256=item.descriptor.sha256,
                    expected_bytes=item.descriptor.bytes,
                    root=staging_root,
                )
                for item in staged
            }
            pack, verified_pack_fingerprint = validate_runtime_receipts(plan, payloads)
            attestation = verify_runtime_attestation(
                plan=plan,
                staged=staged,
                required=attestation_required,
                release_verifier=self._attestation_verifier,
                deployment_verifier=self._deployment_attestation_verifier,
                deployment_subject=(
                    subject_from_runtime_artifacts(
                        plan=plan,
                        staged_artifacts=staged,
                        registry_scope_id=_registry_scope_id(self._registry),
                    )
                    if self._deployment_attestation_verifier is not None
                    else None
                ),
            )
            archive = runtime_payload_archive(pack)
            published = publish_runtime_artifacts(
                staged,
                artifact_root=self._artifact_root,
            )
            try:
                extract_runtime_payload(archive, self._worktree_root)
                self._extract_external_runtime_payloads(plan, staged)
                ready = build_runtime_fetch_ready(
                    plan,
                    plan_sha256=plan_sha256,
                    published=published,
                    attestation_required=attestation_required,
                    attestation_status=attestation.status,
                    attestation_verification=attestation.deployment_verification,
                    attestation_subject=attestation.deployment_subject,
                    runtime_payload_sha256=archive.sha256,
                    verified_pack_fingerprint=verified_pack_fingerprint,
                )
                write_ready_last(self._artifact_root / RUNTIME_FETCH_READY_NAME, ready_manifest_bytes(ready))
                return ready
            except BaseException as exc:
                if isinstance(exc, ReadyPublicationError) and exc.may_have_committed:
                    raise
                try:
                    discard_runtime_payload(self._worktree_root)
                except InitFetchError as cleanup_error:
                    raise cleanup_error from exc
                raise

    def _extract_external_runtime_payloads(
        self,
        plan: RuntimeInitFetchPlan,
        staged: tuple[StagedRuntimeArtifact, ...],
    ) -> None:
        projects = [item for item in staged if getattr(item.descriptor, "kind", None) == "dbt_project_bundle"]
        if not projects:
            return
        if len(projects) != 1:
            raise _integrity_error("runtime init-fetch requires exactly one dbt project bundle")
        descriptor = projects[0].descriptor
        if descriptor not in plan.runtime_payloads:
            raise _integrity_error("dbt project bundle is not selected by the pinned runtime plan")
        try:
            extract_dbt_project_bundle(projects[0].path, self._worktree_root / "dbt-project")
        except Exception as exc:
            raise _integrity_error("dbt project bundle could not be extracted safely") from exc

    def _preflight(self, plan: RuntimeInitFetchPlan) -> bool:
        if self._attestation_verifier is not None and self._deployment_attestation_verifier is not None:
            raise InitFetchError(
                "DPONE_ARTIFACT_ATTESTATION_AUTHORITY_CONFLICT",
                "runtime init-fetch has multiple artifact attestation authorities",
            )
        descriptors = plan.artifacts
        if not descriptors:
            raise _integrity_error("runtime init-fetch plan contains no selected artifacts")
        total = 0
        for descriptor in descriptors:
            if descriptor.bytes > self._max_artifact_bytes:
                raise InitFetchError(
                    "DPONE_CACHE_ARTIFACT_TOO_LARGE",
                    "runtime init-fetch artifact exceeds the per-artifact byte limit",
                    artifact_ref=descriptor.artifact_ref,
                )
            total += descriptor.bytes
        if total > self._max_total_bytes:
            raise InitFetchError(
                "DPONE_ARTIFACT_REGISTRY_TOTAL_LIMIT_EXCEEDED",
                "runtime init-fetch artifacts exceed the aggregate byte limit",
            )
        required = (
            effective_attestation_requirement(
                plan,
                trusted_attestation_required=self._trusted_attestation_required,
            )
            == ATTESTATION_REQUIREMENT_REQUIRED
        )
        if required and self._attestation_verifier is None and self._deployment_attestation_verifier is None:
            raise InitFetchError(
                "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
                "runtime init-fetch requires a configured artifact attestation verifier",
            )
        return required


def _registry_scope_id(registry: ArtifactRegistryReader) -> str:
    if not isinstance(registry, ArtifactRegistryAuthority) or not registry.authority_scope_id:
        raise InitFetchError(
            "DPONE_ARTIFACT_REGISTRY_AUTHORITY_REQUIRED",
            "runtime artifact registry endpoint-bound authority is unavailable",
        )
    return registry.authority_scope_id


def load_existing_runtime_ready(
    plan: RuntimeInitFetchPlan,
    *,
    plan_sha256: str,
    attestation_required: bool,
    deployment_attestation_required: bool = False,
    artifact_root: Path,
    worktree_root: Path,
) -> RuntimeFetchReady | None:
    """Revalidate and reuse one complete ready state without registry access."""

    artifact_root = artifact_root.absolute()
    ready_path = artifact_root / RUNTIME_FETCH_READY_NAME
    if not os.path.lexists(ready_path):
        return None
    require_safe_directory(artifact_root)
    VerifiedPackLauncher(
        artifact_root=artifact_root,
        worktree_root=worktree_root.absolute(),
    ).prepare(
        plan,
        plan_sha256=plan_sha256,
        attestation_required=attestation_required,
        deployment_attestation_required=deployment_attestation_required,
    )
    ready_payload = read_bounded_regular_file(
        ready_path,
        root=artifact_root,
        max_bytes=64 * 1024,
        missing_code="DPONE_RUNTIME_FETCH_READY_INVALID",
        invalid_code="DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED",
        label="runtime ready manifest",
    )
    return parse_ready_manifest(
        ready_payload,
        plan=plan,
        plan_sha256=plan_sha256,
        attestation_required=attestation_required,
        deployment_attestation_required=deployment_attestation_required,
    )


def _positive_limit(value: int, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer")
    return value


def _integrity_error(message: str) -> InitFetchError:
    return InitFetchError("DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED", message)


__all__ = [
    "DEFAULT_STRICT_MAX_ARTIFACT_BYTES",
    "DEFAULT_STRICT_MAX_TOTAL_BYTES",
    "RuntimeInitFetchAttestationVerifier",
    "RuntimeInitFetchExecutor",
    "StagedRuntimeArtifact",
    "load_existing_runtime_ready",
]
