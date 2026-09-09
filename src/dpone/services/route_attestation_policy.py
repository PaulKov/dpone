"""Route subject, validity, profile, and revocation policy."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from dpone.contracts.certification_trust import certification_trust
from dpone.contracts.route_attestation import (
    POLICY_SCHEMA,
    CosignVerificationPolicy,
    RouteAttestationExpectedSubject,
    canonical_fingerprint,
    is_canonical_sha256_digest,
    parse_aware_datetime,
)
from dpone.services.sample_route_certifications import certified_sampling_route_catalog

_POLICY_FIELDS = frozenset(
    {
        "schema",
        "backend",
        "certificate_identity",
        "certificate_oidc_issuer",
        "trusted_root",
        "cosign",
        "allowed_environments",
        "allowed_authorization_profiles",
        "allowed_certification_profiles",
        "minimum_certification_level",
        "max_validity_seconds",
        "clock_skew_seconds",
        "revoked_attestation_ids",
        "revoked_certificate_identities",
    }
)
_VERSION_PATTERN = re.compile(r"^3\.(\d+)\.(\d+)$")


@dataclass(frozen=True, slots=True)
class RouteAttestationPolicy:
    adapter: CosignVerificationPolicy
    fingerprint: str
    trusted_root_sha256: str
    environments: frozenset[str]
    authorization_profiles: frozenset[str]
    certification_profiles: frozenset[str]
    minimum_certification_level: str
    max_validity_seconds: int
    clock_skew_seconds: int
    revoked_attestations: frozenset[str]
    revoked_signers: frozenset[str]


def parse_route_attestation_policy(payload: Mapping[str, Any]) -> RouteAttestationPolicy | tuple[str, str]:
    """Normalize one strict policy or return its stable public error."""

    try:
        trusted_root = _mapping(payload.get("trusted_root"))
        cosign = _mapping(payload.get("cosign"))
        if set(payload) != _POLICY_FIELDS:
            raise ValueError
        if set(trusted_root) != {"path", "sha256"}:
            raise ValueError
        if set(cosign) != {"minimum_version", "maximum_version_exclusive", "timeout_seconds"}:
            raise ValueError
        identity = _required_text(payload.get("certificate_identity"))
        issuer = _required_text(payload.get("certificate_oidc_issuer"))
        if payload.get("schema") != POLICY_SCHEMA or payload.get("backend") != "cosign_keyless_v1":
            raise ValueError
        timeout = _bounded_int(cosign.get("timeout_seconds"), minimum=1, maximum=120)
        max_validity = _bounded_int(payload.get("max_validity_seconds"), minimum=1, maximum=604800)
        skew = _bounded_int(payload.get("clock_skew_seconds"), minimum=0, maximum=3600)
        minimum_level = _required_text(payload.get("minimum_certification_level"))
        if minimum_level != "certified":
            raise ValueError
        minimum_version = _supported_minimum_version(cosign.get("minimum_version"))
        maximum_version = _required_text(cosign.get("maximum_version_exclusive"))
        root_digest = _required_text(trusted_root.get("sha256"))
        if maximum_version != "4.0.0" or not is_canonical_sha256_digest(root_digest):
            raise ValueError
        return RouteAttestationPolicy(
            adapter=CosignVerificationPolicy(
                certificate_identity=identity,
                certificate_oidc_issuer=issuer,
                minimum_version=minimum_version,
                maximum_version_exclusive=maximum_version,
                timeout_seconds=timeout,
            ),
            fingerprint=canonical_fingerprint(dict(payload)),
            trusted_root_sha256=root_digest,
            environments=_text_set(payload.get("allowed_environments")),
            authorization_profiles=_text_set(payload.get("allowed_authorization_profiles")),
            certification_profiles=_text_set(payload.get("allowed_certification_profiles")),
            minimum_certification_level=minimum_level,
            max_validity_seconds=max_validity,
            clock_skew_seconds=skew,
            revoked_attestations=_text_set(payload.get("revoked_attestation_ids"), allow_empty=True),
            revoked_signers=_text_set(payload.get("revoked_certificate_identities"), allow_empty=True),
        )
    except (TypeError, ValueError):
        return (
            "DPONE_ROUTE_ATTESTATION_POLICY_INVALID",
            "The route-attestation policy is incomplete or outside supported bounds.",
        )


def route_attestation_policy_error(
    attestation: Mapping[str, Any],
    certification: Mapping[str, Any],
    policy: RouteAttestationPolicy,
    *,
    expected: RouteAttestationExpectedSubject,
    now: datetime,
) -> tuple[str, str] | None:
    """Return the first deterministic subject/policy violation."""

    claims = _mapping(attestation.get("claims"))
    route = _mapping(claims.get("route"))
    cert_claim = _mapping(claims.get("certification"))
    subject = _mapping(claims.get("subject"))
    validity = _mapping(claims.get("validity"))
    if _route_claim_error(route, certification) is not None:
        return "DPONE_ROUTE_ATTESTATION_ROUTE_MISMATCH", "Signed route claims do not match certified catalog facts."
    expected_subject = expected.to_dict()
    actual_subject = {key: str(subject.get(key) or "") for key in expected_subject if key != "route_id"}
    if str(route.get("route_id") or "") != expected.route_id or actual_subject != {
        key: value for key, value in expected_subject.items() if key != "route_id"
    }:
        return "DPONE_ROUTE_ATTESTATION_SUBJECT_MISMATCH", "Signed claims target a different runtime subject."
    attestation_id = str(attestation.get("attestation_id") or "")
    if attestation_id in policy.revoked_attestations or policy.adapter.certificate_identity in policy.revoked_signers:
        return "DPONE_ROUTE_ATTESTATION_REVOKED", "Route attestation or signer is revoked by policy."
    certification_profile = str(cert_claim.get("profile") or "")
    if (
        subject.get("environment") not in policy.environments
        or subject.get("authorization_profile") not in policy.authorization_profiles
        or certification_profile not in policy.certification_profiles
        or cert_claim.get("level") != policy.minimum_certification_level
        or not certification_trust(certification).passed
        or certification.get("level") != policy.minimum_certification_level
        or certification.get("profile") != certification_profile
    ):
        return "DPONE_ROUTE_ATTESTATION_POLICY_DENIED", "Route attestation is outside the allowed policy profiles."
    return _validity_error(validity, policy, now=now)


def certified_route_id_for_key(key: tuple[str, str, str]) -> str | None:
    """Resolve one normalized source/sink/strategy tuple from the certified catalog."""

    candidate = next((route for route in certified_sampling_route_catalog() if route.key == key), None)
    return candidate.certification_id if candidate is not None else None


def _route_claim_error(route: Mapping[str, Any], certification: Mapping[str, Any]) -> str | None:
    route_id = str(route.get("route_id") or "")
    candidate = next(
        (item for item in certified_sampling_route_catalog() if item.certification_id == route_id),
        None,
    )
    if candidate is None:
        return "unknown"
    expected = {
        "route_id": candidate.certification_id,
        "source": candidate.source,
        "sink": candidate.sink,
        "strategy": candidate.strategy,
        "transport": candidate.transport,
        "schema_evolution": candidate.schema_evolution,
        "airflow_runtime_mode": candidate.airflow_runtime_mode,
        "sampling_mode": candidate.sampling_mode,
    }
    if dict(route) != expected:
        return "catalog_mismatch"
    certified_route = certification.get("route")
    if not isinstance(certified_route, Mapping):
        return "certification_route_missing"
    for field in ("source", "sink", "strategy"):
        if str(certified_route.get(field) or "") != str(route.get(field) or ""):
            return "certification_route_mismatch"
    return None


def _validity_error(
    validity: Mapping[str, Any], policy: RouteAttestationPolicy, *, now: datetime
) -> tuple[str, str] | None:
    issued = parse_aware_datetime(validity.get("issued_at"))
    start = parse_aware_datetime(validity.get("not_before"))
    end = parse_aware_datetime(validity.get("expires_at"))
    if issued is None or start is None or end is None or issued > start or start >= end:
        return "DPONE_ROUTE_ATTESTATION_VALIDITY_INVALID", "Route-attestation validity is malformed."
    if (end - start).total_seconds() > policy.max_validity_seconds:
        return "DPONE_ROUTE_ATTESTATION_VALIDITY_INVALID", "Route-attestation validity exceeds policy."
    skew = timedelta(seconds=policy.clock_skew_seconds)
    if now + skew < start:
        return "DPONE_ROUTE_ATTESTATION_NOT_YET_VALID", "Route attestation is not yet valid."
    if now - skew >= end:
        return "DPONE_ROUTE_ATTESTATION_EXPIRED", "Route attestation has expired."
    return None


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("mapping required")
    return value


def _required_text(value: object) -> str:
    text = str(value or "").strip()
    if not text or len(text) > 1024:
        raise ValueError("text required")
    return text


def _text_set(value: object, *, allow_empty: bool = False) -> frozenset[str]:
    if (
        not isinstance(value, list)
        or len(value) > 1024
        or any(not isinstance(item, str) or not item or len(item) > 1024 for item in value)
    ):
        raise ValueError("string list required")
    result = frozenset(value)
    if len(result) != len(value) or (not result and not allow_empty):
        raise ValueError("unique non-empty string list required")
    return result


def _bounded_int(value: object, *, minimum: int, maximum: int) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
        raise ValueError("bounded integer required")
    return value


def _supported_minimum_version(value: object) -> str:
    text = _required_text(value)
    match = _VERSION_PATTERN.fullmatch(text)
    if match is None or (3, int(match.group(1)), int(match.group(2))) < (3, 0, 4):
        raise ValueError("unsupported minimum version")
    return text


__all__ = [
    "RouteAttestationPolicy",
    "certified_route_id_for_key",
    "parse_route_attestation_policy",
    "route_attestation_policy_error",
]
