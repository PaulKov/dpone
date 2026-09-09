"""Application service for exact worker-time dbt proof revalidation."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.contracts import dbt_semantic_refresh_runtime_proof as proof_domain

if TYPE_CHECKING:
    from dpone.runtime.dbt_semantic_refresh_run_authority import (
        SemanticRefreshDbtProofObservationPort,
        SemanticRefreshDbtStaticProjectionIdentity,
    )


@dataclass(frozen=True, slots=True)
class SemanticRefreshImmutableProofRechecker:
    """Load protected proof authority and re-prove current immutable inputs."""

    authority_loader: proof_domain.SemanticRefreshImmutableProofAuthorityLoaderPort
    sql_proof: proof_domain.SemanticRefreshRuntimeSqlProofPort
    catalog_proof_service: proof_domain.SemanticRefreshRuntimeCatalogProofPort
    lifecycle_observer: proof_domain.SemanticRefreshRuntimeLifecycleObserverPort

    def recheck_sources(
        self,
        *,
        manifest: Mapping[str, Any],
        plan_bundle: Mapping[str, object],
        projection_identity: SemanticRefreshDbtStaticProjectionIdentity,
        selected_model_unique_ids: tuple[str, ...],
    ) -> None:
        """Authenticate and compare raw model/macro closure before compilation."""

        authority = self._load_authority(plan_bundle, projection_identity)
        proof_domain.recheck_semantic_refresh_immutable_sources(
            manifest=manifest,
            authority=authority,
            selected_model_unique_ids=selected_model_unique_ids,
        )

    def recheck(
        self,
        *,
        manifest: Mapping[str, Any],
        plan_bundle: Mapping[str, object],
        projection_identity: SemanticRefreshDbtStaticProjectionIdentity,
        selected_model_unique_ids: tuple[str, ...],
    ) -> SemanticRefreshDbtProofObservationPort:
        """Authenticate exact coordinates before any current proof observation."""

        authority = self._load_authority(plan_bundle, projection_identity)
        return proof_domain.recheck_semantic_refresh_immutable_proofs(
            manifest=manifest,
            authority=authority,
            selected_model_unique_ids=selected_model_unique_ids,
            sql_proof=self.sql_proof,
            catalog_proof_service=self.catalog_proof_service,
            lifecycle_observer=self.lifecycle_observer,
        )

    def _load_authority(
        self,
        plan_bundle: Mapping[str, object],
        projection_identity: SemanticRefreshDbtStaticProjectionIdentity,
    ) -> proof_domain.SemanticRefreshImmutableProofAuthority:
        coordinates = _coordinates(plan_bundle, projection_identity)
        try:
            authority = self.authority_loader.load_exact(**coordinates)
        except Exception as exc:
            raise proof_domain.SemanticRefreshRuntimeProofError(
                "DPONE_DBT_V2_PROOF_UNVERIFIED",
                "protected immutable dbt proof authority is unavailable",
            ) from exc
        if not isinstance(authority, proof_domain.SemanticRefreshImmutableProofAuthority) or (
            authority.pre_release_bundle.pre_release_bundle_sha256 != coordinates["pre_release_bundle_sha256"]
            or authority.pre_release_bundle.lifecycle_policy.package_artifacts_digest
            != coordinates["package_artifacts_sha256"]
        ):
            raise proof_domain.SemanticRefreshRuntimeProofError(
                "DPONE_DBT_V2_PROOF_UNVERIFIED",
                "protected immutable dbt proof authority differs from the deployment plan",
            )
        return authority


def _coordinates(
    plan_bundle: Mapping[str, object],
    projection: SemanticRefreshDbtStaticProjectionIdentity,
) -> dict[str, str]:
    release = plan_bundle.get("release_deployment_authority")
    if not isinstance(release, Mapping):
        raise proof_domain.SemanticRefreshRuntimeProofError(
            "DPONE_DBT_V2_PROOF_UNVERIFIED",
            "deployment plan omitted its release authority",
        )
    expected = {
        "deployment_id": release.get("deployment_id"),
        "package_artifacts_sha256": plan_bundle.get("package_artifacts_sha256"),
        "plan_bundle_sha256": plan_bundle.get("plan_bundle_sha256"),
        "pre_release_bundle_sha256": plan_bundle.get("pre_release_bundle_sha256"),
        "release_id": release.get("release_id"),
    }
    actual = {
        "deployment_id": projection.deployment_id,
        "package_artifacts_sha256": projection.package_artifacts_sha256,
        "plan_bundle_sha256": projection.plan_bundle_sha256,
        "pre_release_bundle_sha256": projection.pre_release_bundle_sha256,
        "release_id": projection.release_id,
    }
    if expected != actual or any(not _digest(value) for value in actual.values()):
        raise proof_domain.SemanticRefreshRuntimeProofError(
            "DPONE_DBT_V2_PROOF_UNVERIFIED",
            "runtime projection differs from the exact deployment proof authority",
        )
    return actual


def _digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and value.startswith("sha256:")
        and len(value) == 71
        and all(character in "0123456789abcdef" for character in value[7:])
    )


__all__ = ["SemanticRefreshImmutableProofRechecker"]
