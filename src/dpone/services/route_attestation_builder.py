"""Build deterministic route authorization blobs for external signing."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime  # type: ignore[attr-defined]
from typing import Any

from dpone.contracts.certification_trust import certification_trust
from dpone.contracts.route_attestation import (
    RouteAttestationArtifact,
    is_canonical_sha256_digest,
    parse_aware_datetime,
    route_attestation_id,
    sha256_bytes,
)
from dpone.services.sample_route_certifications import CertifiedSamplingRoute, certified_sampling_route_catalog


class RouteAttestationBuildError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class RouteAttestationBuilder:
    """Join immutable certification and deployment facts without signing."""

    def build(
        self,
        *,
        route_id: str,
        certification_bundle: Mapping[str, Any],
        certification_bundle_bytes: bytes,
        deployment: Mapping[str, Any],
        authorization_profile: str,
        issued_at: str,
        not_before: str,
        expires_at: str,
    ) -> RouteAttestationArtifact:
        route = _route(route_id)
        _validate_certification(route, certification_bundle)
        subject = _deployment_subject(deployment, authorization_profile=authorization_profile)
        validity = _validity(issued_at=issued_at, not_before=not_before, expires_at=expires_at)
        claims: dict[str, Any] = {
            "route": _route_claims(route),
            "certification": {
                "bundle_sha256": sha256_bytes(certification_bundle_bytes),
                "profile": str(certification_bundle.get("profile") or ""),
                "level": str(certification_bundle.get("level") or ""),
            },
            "subject": subject,
            "validity": validity,
        }
        return RouteAttestationArtifact(route_attestation_id(claims), claims)


def _route(route_id: str) -> CertifiedSamplingRoute:
    normalized = str(route_id or "").strip()
    for candidate in certified_sampling_route_catalog():
        if candidate.certification_id == normalized:
            return candidate
    raise RouteAttestationBuildError(
        "DPONE_ROUTE_ATTESTATION_ROUTE_UNKNOWN",
        "The requested route ID is not present in the safe-sample route catalog.",
    )


def _validate_certification(route: CertifiedSamplingRoute, bundle: Mapping[str, Any]) -> None:
    if bundle.get("schema_version") != "dpone.route_certification_bundle.v1":
        raise RouteAttestationBuildError(
            "DPONE_ROUTE_CERTIFICATION_BUNDLE_INVALID",
            "The route-certification bundle schema is invalid.",
        )
    if not certification_trust(bundle).passed or bundle.get("level") != "certified":
        raise RouteAttestationBuildError(
            "DPONE_ROUTE_CERTIFICATION_NOT_CERTIFIED",
            "The route-certification bundle must pass at level certified.",
        )
    raw_route = bundle.get("route")
    actual = _route_key(raw_route if isinstance(raw_route, Mapping) else {})
    if actual != route.key:
        raise RouteAttestationBuildError(
            "DPONE_ROUTE_CERTIFICATION_ROUTE_MISMATCH",
            "The route-certification bundle does not match the requested route ID.",
        )


def _deployment_subject(deployment: Mapping[str, Any], *, authorization_profile: str) -> dict[str, str]:
    if deployment.get("schema") != "dpone.deployment-set.v1":
        raise _deployment_error("The deployment-set schema is invalid.")
    if deployment.get("runnable") is not True or deployment.get("deployment_type") == "preview":
        raise RouteAttestationBuildError(
            "DPONE_ROUTE_ATTESTATION_DEPLOYMENT_NOT_RUNNABLE",
            "Route attestations require a runnable environment deployment.",
        )
    release_id = str(deployment.get("release_ref") or "")
    deployment_id = str(deployment.get("deployment_id") or "")
    image = str(deployment.get("runtime_image_digest") or "")
    if not all(is_canonical_sha256_digest(item) for item in (release_id, deployment_id, image)):
        raise _deployment_error("Deployment release, deployment, and runtime image identities must be SHA-256 digests.")
    environment = _environment(deployment.get("environment"))
    profile = str(authorization_profile or "").strip()
    if not environment or not profile:
        raise _deployment_error("Deployment environment and authorization profile are required.")
    return {
        "release_id": release_id,
        "deployment_id": deployment_id,
        "environment": environment,
        "runtime_image_digest": image,
        "authorization_profile": profile,
    }


def _validity(*, issued_at: str, not_before: str, expires_at: str) -> dict[str, str]:
    issued = parse_aware_datetime(issued_at)
    start = parse_aware_datetime(not_before)
    end = parse_aware_datetime(expires_at)
    if issued is None or start is None or end is None or issued > start or start >= end:
        raise RouteAttestationBuildError(
            "DPONE_ROUTE_ATTESTATION_VALIDITY_INVALID",
            "Route-attestation timestamps must be ordered, offset-aware values.",
        )
    return {
        "issued_at": _utc_text(issued),
        "not_before": _utc_text(start),
        "expires_at": _utc_text(end),
    }


def _route_claims(route: CertifiedSamplingRoute) -> dict[str, str]:
    return {
        "route_id": route.certification_id,
        "source": route.source,
        "sink": route.sink,
        "strategy": route.strategy,
        "transport": route.transport,
        "schema_evolution": route.schema_evolution,
        "airflow_runtime_mode": route.airflow_runtime_mode,
        "sampling_mode": route.sampling_mode,
    }


def _route_key(raw: Mapping[str, Any]) -> tuple[str, str, str]:
    return tuple(_normalized(raw.get(key)) for key in ("source", "sink", "strategy"))  # type: ignore[return-value]


def _environment(value: object) -> str:
    normalized = _normalized(value)
    if normalized in {"prod", "production"}:
        return "production"
    if normalized in {"dev", "development", "local"}:
        return "development"
    return normalized


def _normalized(value: object) -> str:
    return str(value or "").strip().lower().replace("-", "_")


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _deployment_error(message: str) -> RouteAttestationBuildError:
    return RouteAttestationBuildError("DPONE_ROUTE_ATTESTATION_DEPLOYMENT_INVALID", message)


__all__ = ["RouteAttestationBuildError", "RouteAttestationBuilder"]
