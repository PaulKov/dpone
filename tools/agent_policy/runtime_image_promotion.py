"""Validate runtime-image evidence and promote certified GHCR aliases."""

from __future__ import annotations

import importlib
import re
import sys
from collections.abc import Mapping
from copy import deepcopy
from typing import Any

if __package__:
    from .runtime_image_promotion_models import (
        AliasDecision,
        Certification,
        CertificationIdentity,
        ManifestObservation,
        ManifestState,
        PublicationReceipt,
        TagDecision,
    )
else:
    from runtime_image_promotion_models import (  # type: ignore[no-redef]
        AliasDecision,
        Certification,
        CertificationIdentity,
        ManifestObservation,
        ManifestState,
        PublicationReceipt,
        TagDecision,
    )

if __package__:
    from .runtime_image_publication_journal import validate_publication as _validate_publication
else:
    from runtime_image_publication_journal import (  # type: ignore[no-redef]
        validate_publication as _validate_publication,
    )

CERTIFICATION_SCHEMA_V1 = "dpone.runtime-image-certification.v1"
CERTIFICATION_SCHEMA_V2 = "dpone.runtime-image-certification.v2"
CERTIFICATION_SCHEMA = CERTIFICATION_SCHEMA_V2
PUBLICATION_SCHEMA = "dpone.runtime-image-publication.v1"
CANONICAL_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
STABLE_SEMVER = re.compile(r"^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)$")
FULL_SHA = re.compile(r"^[0-9a-f]{40}$")
IMAGE_NAME = re.compile(r"^ghcr\.io/[a-z0-9]+(?:[._/-][a-z0-9]+)*$")
SAFE_CODE = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
LEGACY_REQUIRED_CHECK_IDS = tuple(
    "installed-dpone-version dpone-version pip-check bcp sqlcmd clickhouse-client native-accel-doctor "
    "airflow-runtime-init-fetch-help airflow-runtime-pack-exec-help".split()
)
REQUIRED_CHECK_IDS = (*LEGACY_REQUIRED_CHECK_IDS, "cosign")


class ContractError(ValueError):
    """Sanitized evidence or policy contract violation."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


def _strict(value: Any, required: tuple[str, ...], optional: tuple[str, ...] = ()) -> dict[str, Any]:
    if type(value) is not dict or not set(required) <= value.keys() or not value.keys() <= set(required + optional):
        raise ContractError("EVIDENCE_SHAPE_INVALID")
    return value


def _text(value: Any, pattern: re.Pattern[str] | None = None) -> str:
    if not isinstance(value, str) or not value or (pattern is not None and pattern.fullmatch(value) is None):
        raise ContractError("EVIDENCE_VALUE_INVALID")
    return value


def _version(value: object) -> tuple[int, int, int]:
    text = str(value)
    if STABLE_SEMVER.fullmatch(text) is None:
        raise ContractError("SEMVER_INVALID")
    return tuple(int(part) for part in text.split("."))  # type: ignore[return-value]


def _safe_code(value: Any, fallback: str) -> str:
    return value if isinstance(value, str) and SAFE_CODE.fullmatch(value) else fallback


def classify_manifest_lookup(result: Any) -> ManifestObservation:
    """Map one sanitized HTTP result to PRESENT, authenticated ABSENT, or ERROR."""

    status = getattr(result, "status_code", None)
    authenticated = getattr(result, "authenticated", False) is True
    error = getattr(result, "error_code", None)
    version = getattr(result, "version", None)
    if error is not None:
        return ManifestObservation(ManifestState.ERROR, error_code=_safe_code(error, "LOOKUP_ERROR"))
    headers = getattr(result, "headers", {})
    normalized = (
        {str(key).lower(): str(value).strip() for key, value in headers.items()} if isinstance(headers, Mapping) else {}
    )
    if status == 200 and authenticated:
        digest = normalized.get("docker-content-digest")
        if digest is not None and CANONICAL_DIGEST.fullmatch(digest):
            return ManifestObservation(
                ManifestState.PRESENT, digest=digest, version=version if isinstance(version, str) else None
            )
        return ManifestObservation(ManifestState.ERROR, error_code="LOOKUP_DIGEST_INVALID")
    if status == 404 and authenticated:
        return ManifestObservation(ManifestState.ABSENT)
    if not authenticated:
        return ManifestObservation(ManifestState.ERROR, error_code="LOOKUP_UNAUTHENTICATED")
    code = f"LOOKUP_HTTP_{status}" if isinstance(status, int) and 100 <= status <= 599 else "LOOKUP_ERROR"
    return ManifestObservation(ManifestState.ERROR, error_code=code)


def decide_fixed_tag(*, expected_digest: str, observed: ManifestObservation) -> TagDecision:
    """Apply fixed-alias create-or-compare semantics."""

    _text(expected_digest, CANONICAL_DIGEST)
    if observed.state is ManifestState.ABSENT:
        return TagDecision(AliasDecision.CREATED, True)
    if observed.state is ManifestState.PRESENT and observed.digest == expected_digest:
        return TagDecision(AliasDecision.NOOP_SAME, False)
    blocker = observed.error_code if observed.state is ManifestState.ERROR else "FIXED_ALIAS_DIGEST_CONFLICT"
    return TagDecision(AliasDecision.BLOCKED, False, _safe_code(blocker, "FIXED_ALIAS_INVALID"))


def decide_latest(
    *,
    candidate_version: object,
    candidate_digest: str,
    observed: ManifestObservation,
    observed_version: object | None,
    candidate_image: str | None = None,
    trusted_latest_certification: Certification | None = None,
) -> TagDecision:
    """Advance latest monotonically by stable SemVer, or fail closed."""

    candidate = _version(candidate_version)
    _text(candidate_digest, CANONICAL_DIGEST)
    if observed.state is ManifestState.ABSENT:
        return TagDecision(AliasDecision.CREATED, True)
    if (
        observed.state is not ManifestState.PRESENT
        or observed.digest is None
        or CANONICAL_DIGEST.fullmatch(observed.digest) is None
    ):
        return TagDecision(AliasDecision.BLOCKED, False, _safe_code(observed.error_code, "LATEST_LOOKUP_INVALID"))
    try:
        current = _version(observed_version)
    except ContractError:
        return TagDecision(AliasDecision.BLOCKED, False, "LATEST_VERSION_INVALID")
    if observed.digest == candidate_digest:
        if current == candidate:
            return TagDecision(AliasDecision.NOOP_SAME, False)
        return TagDecision(AliasDecision.BLOCKED, False, "LATEST_VERSION_DIGEST_CONFLICT")
    if current < candidate:
        return TagDecision(AliasDecision.ADVANCED, True)
    if current > candidate:
        if _certification_binds(
            trusted_latest_certification,
            image=candidate_image,
            digest=observed.digest,
            version=str(observed_version),
        ):
            return TagDecision(AliasDecision.PRESERVED_NEWER, False)
        return TagDecision(AliasDecision.BLOCKED, False, "LATEST_CERTIFICATION_UNPROVEN")
    return TagDecision(AliasDecision.BLOCKED, False, "LATEST_VERSION_DIGEST_CONFLICT")


def _validate_identity(identity: CertificationIdentity) -> None:
    _text(identity.image, IMAGE_NAME)
    _version(identity.version)
    _text(identity.digest, CANONICAL_DIGEST)
    _text(identity.source_commit_sha, FULL_SHA)
    if identity.source_ref != f"refs/tags/v{identity.version}":
        raise ContractError("SOURCE_REF_INVALID")


def validate_certification(
    payload: Any,
    *,
    expected: CertificationIdentity | None = None,
) -> Certification:
    """Reject incomplete, skipped, mismatched, or non-canonical PASS evidence."""

    root = _strict(
        payload,
        ("schema", "status", "idempotency_key", "release", "subject", "build", "checks", "attestations"),
    )
    release = _strict(root["release"], ("version", "tag", "source_commit_sha", "source_ref"))
    subject = _strict(root["subject"], ("name", "digest", "platform"))
    build = _strict(
        root["build"],
        ("run_id", "run_attempt", "dockerfile_sha256", "dockerignore_sha256", "context_manifest_sha256"),
    )
    identity = CertificationIdentity(
        _text(subject["name"], IMAGE_NAME),
        _text(release["version"], STABLE_SEMVER),
        _text(subject["digest"], CANONICAL_DIGEST),
        _text(release["source_commit_sha"], FULL_SHA),
        _text(release["source_ref"]),
    )
    _validate_identity(identity)
    schema = root["schema"]
    required_check_ids = {
        CERTIFICATION_SCHEMA_V1: LEGACY_REQUIRED_CHECK_IDS,
        CERTIFICATION_SCHEMA_V2: REQUIRED_CHECK_IDS,
    }.get(schema)
    if required_check_ids is None:
        raise ContractError("CERTIFICATION_SCHEMA_UNSUPPORTED")
    if (
        root["status"] != "PASS"
        or release["tag"] != f"v{identity.version}"
        or subject["platform"] != "linux/amd64"
        or root["idempotency_key"] != f"{identity.image}|{identity.version}|{identity.digest}"
    ):
        raise ContractError("CERTIFICATION_IDENTITY_INVALID")
    for key in ("run_id", "run_attempt"):
        _text(build[key], re.compile(r"^[1-9]\d*$"))
    for key in ("dockerfile_sha256", "dockerignore_sha256", "context_manifest_sha256"):
        _text(build[key], CANONICAL_DIGEST)
    checks = root["checks"]
    if not isinstance(checks, list) or len(checks) != len(required_check_ids):
        raise ContractError("CERTIFICATION_CHECKS_INCOMPLETE")
    normalized_checks: dict[str, dict[str, Any]] = {}
    for item in checks:
        check = _strict(item, ("id", "status", "subject_digest"))
        check_id = _text(check["id"])
        if check_id in normalized_checks or check["status"] != "PASS" or check["subject_digest"] != identity.digest:
            raise ContractError("CERTIFICATION_CHECK_INVALID")
        normalized_checks[check_id] = check
    if set(normalized_checks) != set(required_check_ids):
        raise ContractError("CERTIFICATION_CHECKS_INCOMPLETE")
    attestations = _strict(root["attestations"], ("provenance", "sbom"))
    for kind in ("provenance", "sbom"):
        attestation = _strict(attestations[kind], ("status", "subject_digest"))
        if attestation["status"] != "PASS" or attestation["subject_digest"] != identity.digest:
            raise ContractError("CERTIFICATION_ATTESTATION_INVALID")
    if expected is not None:
        _validate_identity(expected)
        if identity != expected:
            raise ContractError("CERTIFICATION_IDENTITY_MISMATCH")
    normalized = deepcopy(root)
    normalized["checks"] = [deepcopy(normalized_checks[check_id]) for check_id in required_check_ids]
    return Certification(normalized, identity.image, identity.digest, identity.version, identity.source_commit_sha)


def require_promotable_certification(certification: Certification) -> None:
    """Allow registry mutations only from the current complete evidence schema."""

    if certification.schema != CERTIFICATION_SCHEMA_V2:
        raise ContractError("CERTIFICATION_SCHEMA_NOT_PROMOTABLE")


def _certification_binds(
    certification: Certification | None,
    *,
    image: str | None,
    digest: str,
    version: str,
) -> bool:
    if certification is None or image is None:
        return False
    try:
        validated = validate_certification(certification.to_payload())
    except (AttributeError, ContractError, TypeError):
        return False
    return validated.image == image and validated.digest == digest and validated.version == version


def _validate_publication_semantics(
    root: dict[str, Any],
    *,
    digest: str,
    image: str,
    version: str,
    roles: list[str],
    trusted_latest_certification: Certification | None,
) -> None:
    transitions = root["transitions"]
    for fixed in transitions[:2]:
        if fixed["status"] != "PASS":
            continue
        before, after = fixed["before"], fixed["after"]
        created = fixed["decision"] == "CREATED"
        noop = before.get("state") == "PRESENT" and before.get("digest") == digest and before == after
        if (
            after.get("digest") != digest
            or fixed["decision"] not in ("CREATED", "NOOP_SAME")
            or (created and before["state"] != "ABSENT")
            or (not created and not noop)
        ):
            raise ContractError("PUBLICATION_FIXED_ALIAS_INVALID")
    if root["status"] != "PASS":
        failed = any(item["status"] == "FAIL" for item in transitions)
        partial = any(item["decision"] != "BLOCKED" for item in transitions)
        outcome = "PARTIAL_ALIAS_PENDING" if partial else "BLOCKED"
        if root["status"] != "FAIL" or not failed or root["outcome"] != outcome:
            raise ContractError("PUBLICATION_FAILURE_INVALID")
        return
    if root["outcome"] != "PUBLISHED" or roles != ["version", "sha", "latest"]:
        raise ContractError("PUBLICATION_FALSE_PASS")
    if any(item["status"] != "PASS" for item in transitions):
        raise ContractError("PUBLICATION_FALSE_PASS")
    before, after = transitions[2]["before"], transitions[2]["after"]
    decision = transitions[2]["decision"]
    if decision == "PRESERVED_NEWER":
        certified = _certification_binds(
            trusted_latest_certification,
            image=image,
            digest=after.get("digest"),
            version=after.get("version"),
        )
        if before != after or not certified or _version(after.get("version")) <= _version(version):
            raise ContractError("LATEST_CERTIFICATION_UNPROVEN")
    elif decision == "NOOP_SAME":
        if before != after or after.get("digest") != digest or after.get("version") != version:
            raise ContractError("PUBLICATION_LATEST_INVALID")
    elif decision in ("CREATED", "ADVANCED"):
        valid_before = (
            before["state"] == "ABSENT"
            if decision == "CREATED"
            else _version(before.get("version")) < _version(version)
        )
        if not valid_before or after.get("digest") != digest or after.get("version") != version:
            raise ContractError("PUBLICATION_LATEST_INVALID")
    else:
        raise ContractError("PUBLICATION_LATEST_INVALID")


def validate_publication(
    payload: Any,
    *,
    trusted_latest_certification: Certification | None = None,
) -> PublicationReceipt:
    """Validate truthful per-alias publication evidence."""

    return _validate_publication(
        payload,
        policy=sys.modules[__name__],
        trusted_latest_certification=trusted_latest_certification,
    )


def promote_certified_image(
    *,
    certification: Certification,
    certification_sha256: str,
    registry: Any,
    journal: Any | None = None,
    trusted_latest_certification: Certification | None = None,
) -> PublicationReceipt:
    module = f"{__package__}.runtime_image_registry" if __package__ else "runtime_image_registry"
    reconcile = importlib.import_module(module).reconcile_certified_image
    return reconcile(
        policy=sys.modules[__name__],
        certification=certification,
        certification_sha256=certification_sha256,
        registry=registry,
        journal=journal,
        trusted_latest_certification=trusted_latest_certification,
    )


def main(argv: list[str] | None = None) -> int:
    """Run the side-effect adapter while retaining this stable CLI path."""

    module = f"{__package__}.runtime_image_registry" if __package__ else "runtime_image_registry"
    return importlib.import_module(module).run_cli(sys.modules[__name__], argv)


if __name__ == "__main__":
    raise SystemExit(main())
