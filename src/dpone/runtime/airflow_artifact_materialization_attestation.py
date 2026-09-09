"""Attestation policy gate for one staged Airflow deployment projection."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from dpone.ports.airflow_deployment_attestation import (
    AirflowArtifactAttestationRegistryError,
    AirflowArtifactAttestationRejected,
    AirflowDeploymentAttestationVerifier,
)
from dpone.ports.artifact_registry import ArtifactRegistryAuthority
from dpone.runtime.airflow_artifact_delivery_models import AirflowArtifactDeliveryError
from dpone.runtime.airflow_artifact_delivery_support import ArtifactRegistryReader
from dpone.runtime.airflow_deployment_attestation_subject import (
    subject_from_cache_projection,
)
from dpone.runtime.deployment_cache_models import ValidatedDeploymentProjection


class ArtifactAttestationVerifier(Protocol):
    def verify(self, projection: ValidatedDeploymentProjection) -> None:
        """Raise when the staged projection lacks a valid trusted attestation."""


@dataclass(frozen=True, slots=True)
class MaterializationAttestationGate:
    """Select and enforce exactly one attestation authority."""

    registry: ArtifactRegistryReader
    release_verifier: ArtifactAttestationVerifier | None
    deployment_verifier: AirflowDeploymentAttestationVerifier | None

    def preflight(self) -> None:
        if self.release_verifier is not None and self.deployment_verifier is not None:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_ATTESTATION_AUTHORITY_CONFLICT",
                "artifact materialization has multiple attestation authorities",
            )

    def verify(
        self,
        projection: ValidatedDeploymentProjection,
        *,
        cache_root: Path,
    ) -> None:
        if not _attestation_required(projection.deployment):
            return
        if self.deployment_verifier is not None:
            self._verify_deployment(projection, cache_root=cache_root)
            return
        if self.release_verifier is None:
            raise AirflowArtifactDeliveryError(
                "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
                "production deployment requires a configured artifact attestation verifier",
            )
        try:
            self.release_verifier.verify(projection)
        except AirflowArtifactAttestationRejected as exc:
            raise _rejected(exc) from exc
        except AirflowArtifactAttestationRegistryError as exc:
            raise _registry_error(exc) from exc
        except AirflowArtifactDeliveryError:
            raise
        except Exception as exc:  # noqa: BLE001 - verifier internals must not leak.
            raise _verification_unavailable() from exc

    def _verify_deployment(
        self,
        projection: ValidatedDeploymentProjection,
        *,
        cache_root: Path,
    ) -> None:
        verifier = self.deployment_verifier
        if verifier is None:
            raise AssertionError("deployment attestation verifier is missing")
        _require_deployment_policy_ref(
            projection.deployment,
            expected_sha256=verifier.policy_sha256,
        )
        try:
            verification = verifier.verify(
                subject_from_cache_projection(
                    cache_root=cache_root,
                    projection=projection,
                    registry_scope_id=_registry_scope_id(self.registry),
                )
            )
            if not verification.is_verified:
                raise AirflowArtifactDeliveryError(
                    verification.code,
                    verification.message,
                )
        except AirflowArtifactAttestationRejected as exc:
            raise _rejected(exc) from exc
        except AirflowArtifactAttestationRegistryError as exc:
            raise _registry_error(exc) from exc
        except AirflowArtifactDeliveryError:
            raise
        except Exception as exc:  # noqa: BLE001 - verifier internals must not leak.
            raise _verification_unavailable() from exc


def _attestation_required(deployment: Mapping[str, Any]) -> bool:
    delivery = deployment.get("runtime_artifact_delivery")
    verify = delivery.get("verify") if isinstance(delivery, Mapping) else None
    return isinstance(verify, Mapping) and verify.get("attestations") == "required_for_prod"


def _require_deployment_policy_ref(
    deployment: Mapping[str, Any],
    *,
    expected_sha256: str,
) -> None:
    delivery = deployment.get("runtime_artifact_delivery")
    policy_ref = delivery.get("trust_policy_ref") if isinstance(delivery, Mapping) else None
    if not isinstance(policy_ref, Mapping) or policy_ref.get("sha256") != expected_sha256:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_TRUST_POLICY_MISMATCH",
            "deployment trust policy does not match mounted policy bytes",
        )


def _registry_scope_id(registry: ArtifactRegistryReader) -> str:
    if not isinstance(registry, ArtifactRegistryAuthority) or not registry.authority_scope_id:
        raise AirflowArtifactDeliveryError(
            "DPONE_ARTIFACT_REGISTRY_AUTHORITY_REQUIRED",
            "artifact registry endpoint-bound authority is unavailable",
        )
    return registry.authority_scope_id


def _rejected(exc: AirflowArtifactAttestationRejected) -> AirflowArtifactDeliveryError:
    return AirflowArtifactDeliveryError(
        exc.verification.code,
        exc.verification.message,
    )


def _registry_error(exc: AirflowArtifactAttestationRegistryError) -> AirflowArtifactDeliveryError:
    return AirflowArtifactDeliveryError(
        exc.code,
        "artifact attestation package could not be read",
    )


def _verification_unavailable() -> AirflowArtifactDeliveryError:
    return AirflowArtifactDeliveryError(
        "DPONE_ARTIFACT_ATTESTATION_REQUIRED",
        "artifact attestation could not be verified",
    )


__all__ = ["ArtifactAttestationVerifier", "MaterializationAttestationGate"]
