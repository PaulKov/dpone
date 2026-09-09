"""Runtime-image composition root for strict indexed Airflow artifact delivery."""

from __future__ import annotations

import os
import stat
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from dpone.ports.airflow_deployment_attestation import (
        AirflowDeploymentAttestationVerifier,
    )
    from dpone.ports.artifact_registry import ArtifactRegistryReader
    from dpone.ports.runtime_artifact_attestation import (
        RuntimeArtifactAttestationVerifier,
    )
    from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan
    from dpone.runtime.verified_pack_launcher import VerifiedPackCommand

from dpone.readiness.airflow_artifact_delivery import (
    AirflowArtifactDeliveryError,
    ArtifactRegistryOptions,
)
from dpone.readiness.airflow_artifact_trust_material import (
    AirflowArtifactTrustMaterialError,
    load_airflow_artifact_trust_material,
)
from dpone.readiness.airflow_deployment_attestation_verifier import (
    RegistryAirflowDeploymentAttestationVerifier,
)
from dpone.readiness.airflow_runtime_init_fetch_config import (
    DEFAULT_REGISTRY_CONFIG_PATH,
    DEFAULT_TRUST_POLICY_PATH,
    RuntimeAttestationAuthority,
    RuntimeRegistryConfiguration,
    stock_attestation_verifier,
    trusted_attestation_authority,
    verified_snapshot,
)
from dpone.readiness.airflow_runtime_init_fetch_config import (
    registry_configuration as parse_registry_configuration,
)
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan_codec import decode_runtime_init_fetch_plan
from dpone.runtime.runtime_init_fetch_ready import (
    ATTESTATION_REQUIREMENT_REQUIRED,
    effective_attestation_requirement,
)
from dpone.runtime.runtime_init_fetch_service import RuntimeInitFetchExecutor, load_existing_runtime_ready
from dpone.runtime.verified_pack_launcher import VerifiedPackLauncher

AirflowRuntimeDeliveryError = InitFetchError

PLAN_B64_ENV = "DPONE_INIT_FETCH_PLAN_B64"
PLAN_SHA256_ENV = "DPONE_INIT_FETCH_PLAN_SHA256"
DEFAULT_ARTIFACT_ROOT = Path("/var/lib/dpone/artifacts")
DEFAULT_WORKTREE_ROOT = Path("/workspace/repo")
DEFAULT_TRUST_KEY_ROOT = Path("/etc/dpone/artifact-trust")
DEV_EVIDENCE_BOOTSTRAP_ROOT_ENV = "DPONE_DBT_EVIDENCE_BOOTSTRAP_ROOT"
DEFAULT_DEV_EVIDENCE_BOOTSTRAP_ROOT = Path("/var/lib/dpone/dev-evidence-bootstrap")


class RuntimeRegistryFactory(Protocol):
    def build(
        self,
        configuration: RuntimeRegistryConfiguration,
    ) -> ArtifactRegistryReader: ...


class WorkloadIdentityRegistryFactory:
    """Construct the existing object-storage adapter after trusted preflight."""

    def build(
        self,
        configuration: RuntimeRegistryConfiguration,
    ) -> ArtifactRegistryReader:
        if configuration.access_mode != "workload_identity":
            raise InitFetchError(
                "DPONE_ARTIFACT_REGISTRY_CONFIG_INVALID",
                "runtime registry access mode is unsupported",
            )
        try:
            return ArtifactRegistryOptions(
                registry_uri=configuration.registry_uri,
                identity_mode="workload_identity",
            ).build()
        except AirflowArtifactDeliveryError as exc:
            dependency_error = exc.code in {
                "DPONE_ARTIFACT_REGISTRY_SDK_UNAVAILABLE",
                "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE",
            }
            raise InitFetchError(
                "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE" if dependency_error else "DPONE_ARTIFACT_REGISTRY_CONFIG_INVALID",
                "runtime artifact registry could not be constructed"
                if dependency_error
                else "runtime artifact registry configuration is invalid",
            ) from exc
        except Exception as exc:  # noqa: BLE001 - URI/parser details cannot cross the runtime CLI boundary.
            raise InitFetchError(
                "DPONE_ARTIFACT_REGISTRY_CONFIG_INVALID",
                "runtime artifact registry configuration is invalid",
            ) from exc


class AirflowRuntimeInitFetchService:
    """Decode first, snapshot configuration once, then compose runtime adapters."""

    def __init__(
        self,
        *,
        registry_factory: RuntimeRegistryFactory | None = None,
        attestation_verifier: RuntimeArtifactAttestationVerifier | None = None,
        deployment_attestation_verifier: AirflowDeploymentAttestationVerifier | None = None,
        registry_config_path: Path = DEFAULT_REGISTRY_CONFIG_PATH,
        trust_policy_path: Path = DEFAULT_TRUST_POLICY_PATH,
        trust_key_root: Path = DEFAULT_TRUST_KEY_ROOT,
        artifact_root: Path = DEFAULT_ARTIFACT_ROOT,
        worktree_root: Path = DEFAULT_WORKTREE_ROOT,
        dev_evidence_bootstrap_root: Path = (DEFAULT_DEV_EVIDENCE_BOOTSTRAP_ROOT),
    ) -> None:
        self._registry_factory = registry_factory or WorkloadIdentityRegistryFactory()
        self._attestation_verifier = attestation_verifier
        self._deployment_attestation_verifier = deployment_attestation_verifier
        self._registry_config_path = registry_config_path
        self._trust_policy_path = trust_policy_path
        self._trust_key_root = trust_key_root
        self._artifact_root = artifact_root
        self._worktree_root = worktree_root
        self._dev_evidence_bootstrap_root = dev_evidence_bootstrap_root

    def init_fetch(self, environment: Mapping[str, str] | None = None) -> Mapping[str, Any]:
        _ensure_dev_evidence_spool(
            environment,
            expected_root=self._dev_evidence_bootstrap_root,
        )
        plan, plan_sha256 = _plan_from_environment(environment)
        authority = trusted_attestation_authority(
            plan,
            path=self._trust_policy_path,
        )
        self._validate_attestation_injection(authority)
        trusted_required = authority.requires_attestation
        attestation_required = (
            effective_attestation_requirement(
                plan,
                trusted_attestation_required=trusted_required,
            )
            == ATTESTATION_REQUIREMENT_REQUIRED
        )
        if (
            attestation_required
            and self._attestation_verifier is None
            and self._deployment_attestation_verifier is None
            and not authority.supports_stock_verification
        ):
            raise InitFetchError(
                "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
                "runtime init-fetch requires a configured artifact attestation verifier",
            )
        existing_ready = load_existing_runtime_ready(
            plan,
            plan_sha256=plan_sha256,
            attestation_required=attestation_required,
            deployment_attestation_required=authority.deployment_policy_bytes is not None,
            artifact_root=self._artifact_root,
            worktree_root=self._worktree_root,
        )
        if existing_ready is not None:
            return existing_ready.to_dict()
        registry_snapshot = verified_snapshot(
            self._registry_config_path,
            expected_sha256=plan.registry_config_ref["sha256"],
            label="artifact registry configuration",
            invalid_code="DPONE_ARTIFACT_REGISTRY_CONFIG_INVALID",
            mismatch_code="DPONE_ARTIFACT_REGISTRY_CONFIG_MISMATCH",
        )
        registry_configuration = parse_registry_configuration(
            registry_snapshot,
            logical_ref=plan.artifact_registry_ref,
        )
        registry = self._registry_factory.build(registry_configuration)
        release_verifier, deployment_verifier = self._attestation_verifiers(
            registry=registry,
            authority=authority,
        )
        ready = RuntimeInitFetchExecutor(
            registry=registry,
            artifact_root=self._artifact_root,
            worktree_root=self._worktree_root,
            attestation_verifier=release_verifier,
            deployment_attestation_verifier=deployment_verifier,
            trusted_attestation_required=trusted_required,
        ).execute(plan, plan_sha256=plan_sha256)
        return ready.to_dict()

    def _validate_attestation_injection(
        self,
        authority: RuntimeAttestationAuthority,
    ) -> None:
        if self._attestation_verifier is not None and self._deployment_attestation_verifier is not None:
            raise InitFetchError(
                "DPONE_ARTIFACT_ATTESTATION_AUTHORITY_CONFLICT",
                "runtime init-fetch has multiple artifact attestation authorities",
            )
        if authority.deployment_policy_bytes is not None and self._attestation_verifier is not None:
            raise InitFetchError(
                "DPONE_ARTIFACT_TRUST_POLICY_INVALID",
                "Airflow deployment trust policy cannot use a release verifier",
            )
        if authority.deployment_policy_bytes is None and self._deployment_attestation_verifier is not None:
            raise InitFetchError(
                "DPONE_ARTIFACT_TRUST_POLICY_INVALID",
                "GitHub/SLSA runtime trust policy cannot use a deployment verifier",
            )

    def _attestation_verifiers(
        self,
        *,
        registry: ArtifactRegistryReader,
        authority: RuntimeAttestationAuthority,
    ) -> tuple[
        RuntimeArtifactAttestationVerifier | None,
        AirflowDeploymentAttestationVerifier | None,
    ]:
        if authority.deployment_policy_bytes is not None:
            verifier = self._deployment_attestation_verifier
            if verifier is None:
                try:
                    material = load_airflow_artifact_trust_material(
                        policy_bytes=authority.deployment_policy_bytes,
                        key_root=self._trust_key_root,
                    )
                    verifier = RegistryAirflowDeploymentAttestationVerifier(
                        registry=registry,
                        trust_material=material,
                    )
                except (AirflowArtifactTrustMaterialError, ValueError) as exc:
                    raise InitFetchError(
                        "DPONE_ARTIFACT_TRUST_POLICY_INVALID",
                        "runtime artifact trust material is invalid",
                    ) from exc
            return None, verifier
        policy = authority.github_policy
        release_verifier = self._attestation_verifier
        if release_verifier is None and policy is not None:
            release_verifier = stock_attestation_verifier(registry, policy)
        return release_verifier, None

    def prepare_pack_exec(
        self,
        environment: Mapping[str, str] | None = None,
    ) -> VerifiedPackCommand:
        plan, plan_sha256 = _plan_from_environment(environment)
        return VerifiedPackLauncher(
            artifact_root=self._artifact_root,
            worktree_root=self._worktree_root,
        ).prepare(
            plan,
            plan_sha256=plan_sha256,
        )


def _ensure_dev_evidence_spool(
    environment: Mapping[str, str] | None,
    *,
    expected_root: Path,
) -> None:
    values = os.environ if environment is None else environment
    raw_root = values.get(DEV_EVIDENCE_BOOTSTRAP_ROOT_ENV)
    if raw_root is None:
        return
    root = Path(raw_root)
    if root != expected_root:
        raise InitFetchError(
            "DPONE_DEV_EVIDENCE_BOOTSTRAP_INVALID",
            "dev evidence bootstrap root is not provider-owned",
        )
    try:
        metadata = root.lstat()
        if stat.S_ISLNK(metadata.st_mode) or not stat.S_ISDIR(metadata.st_mode):
            raise OSError("unsafe root")
        spool = root / "dbt-spool"
        try:
            spool.mkdir(mode=0o750)
        except FileExistsError:
            pass
        spool_metadata = spool.lstat()
        if stat.S_ISLNK(spool_metadata.st_mode) or not stat.S_ISDIR(spool_metadata.st_mode):
            raise OSError("unsafe spool")
        if metadata.st_dev != spool_metadata.st_dev:
            raise OSError("spool escaped mounted filesystem")
    except OSError as exc:
        raise InitFetchError(
            "DPONE_DEV_EVIDENCE_BOOTSTRAP_INVALID",
            "dev evidence spool could not be initialized safely",
        ) from exc


def _plan_from_environment(
    environment: Mapping[str, str] | None,
) -> tuple[RuntimeInitFetchPlan, str]:
    values = environment if environment is not None else os.environ
    return decode_runtime_init_fetch_plan(
        str(values.get(PLAN_B64_ENV) or ""),
        str(values.get(PLAN_SHA256_ENV) or ""),
    )


__all__ = [
    "AirflowRuntimeDeliveryError",
    "AirflowRuntimeInitFetchService",
    "DEFAULT_ARTIFACT_ROOT",
    "DEFAULT_REGISTRY_CONFIG_PATH",
    "DEFAULT_TRUST_KEY_ROOT",
    "DEFAULT_TRUST_POLICY_PATH",
    "DEFAULT_WORKTREE_ROOT",
    "PLAN_B64_ENV",
    "PLAN_SHA256_ENV",
    "RuntimeRegistryConfiguration",
    "RuntimeRegistryFactory",
    "WorkloadIdentityRegistryFactory",
]
