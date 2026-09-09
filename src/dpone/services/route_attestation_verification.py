"""Orchestrate signed route-attestation verification."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from datetime import UTC, datetime  # type: ignore[attr-defined]
from typing import Any

from dpone.contracts.route_attestation import (
    ATTESTATION_SCHEMA,
    RouteAttestationExpectedSubject,
    RouteAttestationVerification,
    canonical_fingerprint,
    is_canonical_sha256_digest,
    route_attestation_id,
    sha256_bytes,
)
from dpone.ports.route_attestation import RouteAttestationSignatureVerifier
from dpone.services.route_attestation_policy import (
    RouteAttestationPolicy,
    certified_route_id_for_key,
    parse_route_attestation_policy,
    route_attestation_policy_error,
)


class RouteAttestationVerificationService:
    """Verify local trust bytes before runtime authorization or external I/O."""

    def __init__(
        self,
        *,
        signature_verifier: RouteAttestationSignatureVerifier,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self._signature_verifier = signature_verifier
        self._clock = clock or (lambda: datetime.now(UTC))

    def verify(
        self,
        *,
        attestation: bytes,
        sigstore_bundle: bytes,
        certification_bundle: bytes,
        policy: Mapping[str, Any],
        trusted_root: bytes,
        expected: RouteAttestationExpectedSubject,
        trusted_root_sha256: str | None = None,
    ) -> RouteAttestationVerification:
        now = self._aware_now()
        parsed_policy = parse_route_attestation_policy(policy)
        if isinstance(parsed_policy, tuple):
            return _empty_receipt(
                decision="invalid",
                code=parsed_policy[0],
                message=parsed_policy[1],
                now=now,
                attestation=attestation,
                certification=certification_bundle,
                policy=policy,
            )
        root_digest = sha256_bytes(trusted_root)
        if root_digest != parsed_policy.trusted_root_sha256 or (
            trusted_root_sha256 is not None and trusted_root_sha256 != root_digest
        ):
            return _empty_receipt(
                decision="unverified",
                code="DPONE_ROUTE_ATTESTATION_TRUST_ROOT_UNAVAILABLE",
                message="The local trusted root does not match its pinned digest.",
                now=now,
                attestation=attestation,
                certification=certification_bundle,
                policy=policy,
            )
        parsed_attestation = _strict_json_mapping(attestation)
        certification = _strict_json_mapping(certification_bundle)
        if parsed_attestation is None or certification is None:
            return _empty_receipt(
                decision="invalid",
                code="DPONE_ROUTE_ATTESTATION_INPUT_INVALID",
                message="Route-attestation or certification JSON is invalid or has duplicate keys.",
                now=now,
                attestation=attestation,
                certification=certification_bundle,
                policy=policy,
            )
        local_error = _local_integrity_error(parsed_attestation, certification, certification_bundle)
        if local_error is not None:
            return _payload_receipt(
                parsed_attestation,
                parsed_policy,
                decision="invalid",
                code=local_error[0],
                message=local_error[1],
                now=now,
                attestation=attestation,
                certification=certification_bundle,
            )
        signature = self._signature_verifier.verify_blob(
            blob=attestation,
            sigstore_bundle=sigstore_bundle,
            trusted_root=trusted_root,
            policy=parsed_policy.adapter,
        )
        if signature.status != "verified":
            return _payload_receipt(
                parsed_attestation,
                parsed_policy,
                decision=signature.status,
                code=signature.code,
                message=signature.message,
                now=now,
                attestation=attestation,
                certification=certification_bundle,
                verifier_version=signature.verifier_version,
            )
        policy_error = route_attestation_policy_error(
            parsed_attestation,
            certification,
            parsed_policy,
            expected=expected,
            now=now,
        )
        if policy_error is not None:
            return _payload_receipt(
                parsed_attestation,
                parsed_policy,
                decision="invalid",
                code=policy_error[0],
                message=policy_error[1],
                now=now,
                attestation=attestation,
                certification=certification_bundle,
                verifier_version=signature.verifier_version,
            )
        return _payload_receipt(
            parsed_attestation,
            parsed_policy,
            decision="verified",
            code="DPONE_ROUTE_ATTESTATION_VERIFIED",
            message="Route attestation is valid for this deployment.",
            now=now,
            attestation=attestation,
            certification=certification_bundle,
            verifier_version=signature.verifier_version,
        )

    def _aware_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("route-attestation clock must return an offset-aware datetime")
        return value.astimezone(UTC)


def _local_integrity_error(
    attestation: Mapping[str, Any], certification: Mapping[str, Any], certification_bytes: bytes
) -> tuple[str, str] | None:
    if not _attestation_shape_valid(attestation) or certification.get("schema_version") != (
        "dpone.route_certification_bundle.v1"
    ):
        return "DPONE_ROUTE_ATTESTATION_INPUT_INVALID", "Route-attestation or certification schema is invalid."
    claims = attestation.get("claims")
    if not isinstance(claims, dict) or attestation.get("attestation_id") != route_attestation_id(claims):
        return "DPONE_ROUTE_ATTESTATION_DIGEST_MISMATCH", "Route-attestation claims do not match their identity."
    certification_claim = claims.get("certification")
    if not isinstance(certification_claim, Mapping) or certification_claim.get("bundle_sha256") != sha256_bytes(
        certification_bytes
    ):
        return (
            "DPONE_ROUTE_ATTESTATION_CERTIFICATION_DIGEST_MISMATCH",
            "Route-certification bundle bytes do not match the signed digest.",
        )
    return None


def _attestation_shape_valid(attestation: Mapping[str, Any]) -> bool:
    if set(attestation) != {"schema", "attestation_id", "claims"} or attestation.get("schema") != ATTESTATION_SCHEMA:
        return False
    if not is_canonical_sha256_digest(attestation.get("attestation_id")):
        return False
    claims = attestation.get("claims")
    if not isinstance(claims, Mapping) or set(claims) != {"route", "certification", "subject", "validity"}:
        return False
    route_fields = {
        "route_id",
        "source",
        "sink",
        "strategy",
        "transport",
        "schema_evolution",
        "airflow_runtime_mode",
        "sampling_mode",
    }
    subject_fields = {
        "release_id",
        "deployment_id",
        "environment",
        "runtime_image_digest",
        "authorization_profile",
    }
    route = _exact_text_mapping(claims.get("route"), route_fields)
    certification = _exact_text_mapping(claims.get("certification"), {"bundle_sha256", "profile", "level"})
    subject = _exact_text_mapping(claims.get("subject"), subject_fields)
    validity = _exact_text_mapping(claims.get("validity"), {"issued_at", "not_before", "expires_at"})
    if route is None or certification is None or subject is None or validity is None:
        return False
    return (
        all(
            is_canonical_sha256_digest(value)
            for value in (
                certification["bundle_sha256"],
                subject["release_id"],
                subject["deployment_id"],
                subject["runtime_image_digest"],
            )
        )
        and certification["level"] == "certified"
    )


def _exact_text_mapping(value: object, fields: set[str]) -> dict[str, str] | None:
    if not isinstance(value, Mapping) or set(value) != fields:
        return None
    result = {field: value.get(field) for field in fields}
    if any(not isinstance(item, str) or not item or len(item) > 1024 for item in result.values()):
        return None
    return {key: str(item) for key, item in result.items()}


def _payload_receipt(
    payload: Mapping[str, Any],
    policy: RouteAttestationPolicy,
    *,
    decision: str,
    code: str,
    message: str,
    now: datetime,
    attestation: bytes,
    certification: bytes,
    verifier_version: str | None = None,
) -> RouteAttestationVerification:
    claims = _mapping_or_empty(payload.get("claims"))
    route = _mapping_or_empty(claims.get("route"))
    subject = _mapping_or_empty(claims.get("subject"))
    validity = _mapping_or_empty(claims.get("validity"))
    return RouteAttestationVerification(
        decision=decision,
        code=code,
        message=message,
        attestation_id=_optional_text(payload.get("attestation_id")),
        attestation_sha256=sha256_bytes(attestation),
        certification_bundle_sha256=sha256_bytes(certification),
        policy_fingerprint=policy.fingerprint,
        route_id=_optional_text(route.get("route_id")),
        release_id=_optional_text(subject.get("release_id")),
        deployment_id=_optional_text(subject.get("deployment_id")),
        environment=_optional_text(subject.get("environment")),
        authorization_profile=_optional_text(subject.get("authorization_profile")),
        signer={
            "backend": "cosign_keyless_v1",
            "certificate_identity": policy.adapter.certificate_identity,
            "certificate_oidc_issuer": policy.adapter.certificate_oidc_issuer,
            "verifier_version": verifier_version,
        },
        validity={key: validity.get(key) for key in ("not_before", "expires_at")},
        verified_at=_utc_text(now),
    )


def _empty_receipt(
    *,
    decision: str,
    code: str,
    message: str,
    now: datetime,
    attestation: bytes,
    certification: bytes,
    policy: Mapping[str, Any],
) -> RouteAttestationVerification:
    return RouteAttestationVerification(
        decision=decision,
        code=code,
        message=message,
        attestation_id=None,
        attestation_sha256=sha256_bytes(attestation),
        certification_bundle_sha256=sha256_bytes(certification),
        policy_fingerprint=canonical_fingerprint(dict(policy)),
        route_id=None,
        release_id=None,
        deployment_id=None,
        environment=None,
        authorization_profile=None,
        signer={},
        validity={},
        verified_at=_utc_text(now),
    )


def _strict_json_mapping(raw: bytes) -> dict[str, Any] | None:
    def pairs(items: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    try:
        payload = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError):
        return None
    return payload if isinstance(payload, dict) else None


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"unsupported JSON constant: {value}")


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _mapping_or_empty(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


__all__ = [
    "RouteAttestationExpectedSubject",
    "RouteAttestationSignatureVerifier",
    "RouteAttestationVerification",
    "RouteAttestationVerificationService",
    "certified_route_id_for_key",
]
