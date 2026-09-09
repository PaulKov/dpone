"""Provider-neutral trust evidence for schema migration bundles."""

from __future__ import annotations

import fnmatch
from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness.schema_migration_trust_models import (
    PRODUCER,
    PROVENANCE_SCHEMA,
    TRUST_POLICY_SCHEMA,
    TRUST_VERIFICATION_SCHEMA,
    ArtifactSignatureProvider,
    ExternalAttestationEvidence,
    LocalHmacSignatureProvider,
    MigrationProvenanceBuilder,
    MigrationProvenanceStatement,
    MigrationTrustPolicy,
    MigrationTrustSubject,
    trust_fingerprint,
)


class MigrationTrustAttestor:
    """Creates signed migration-bundle provenance without SCM or DB calls."""

    def __init__(
        self,
        *,
        subject_builder: MigrationTrustSubject | None = None,
        provenance_builder: MigrationProvenanceBuilder | None = None,
        signer: ArtifactSignatureProvider | None = None,
    ) -> None:
        self._subject_builder = subject_builder or MigrationTrustSubject()
        self._provenance_builder = provenance_builder or MigrationProvenanceBuilder()
        self._signer = signer or LocalHmacSignatureProvider()

    def attest(
        self,
        *,
        bundle: Mapping[str, Any],
        verification: Mapping[str, Any],
        provenance_source: str,
        repository: str,
        commit_sha: str,
        ref: str,
        run_id: str | None = None,
        workflow: str | None = None,
        actor: str | None = None,
        run_url: str | None = None,
        builder_id: str = "dpone.local",
        protected_ref: bool = False,
        signing_key: str | None = None,
        signing_key_id: str = "local-hmac",
        finished_at: str | None = None,
    ) -> dict[str, Any]:
        subject = self._subject_builder.from_bundle(bundle)
        blockers = [str(item) for item in verification.get("blockers", []) if str(item)]
        if verification.get("status") == "blocked":
            blockers.append("migration_trust.bundle_verification_blocked")
        statement = self._provenance_builder.build(
            subject=subject,
            provenance_source=provenance_source,
            repository=repository,
            commit_sha=commit_sha,
            ref=ref,
            run_id=run_id,
            workflow=workflow,
            actor=actor,
            run_url=run_url,
            builder_id=builder_id,
            protected_ref=protected_ref,
            finished_at=finished_at,
        )
        unsigned = {
            "schema_version": PROVENANCE_SCHEMA,
            "status": "blocked" if blockers else "attested",
            "producer": PRODUCER,
            "bundle_id": bundle.get("bundle_id"),
            "pack_id": bundle.get("pack_id"),
            "subject": subject,
            "statement": statement,
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": [],
        }
        provenance_id = trust_fingerprint(unsigned)
        payload = {**unsigned, "provenance_id": provenance_id}
        if signing_key:
            payload["signature"] = self._signer.sign(
                signed_sha256=provenance_id,
                key=signing_key,
                key_id=signing_key_id,
            )
        return payload


class MigrationTrustVerifier:
    """Validates provenance, signatures, external receipts and policy."""

    def __init__(self, *, subject_builder: MigrationTrustSubject | None = None) -> None:
        self._subject_builder = subject_builder or MigrationTrustSubject()

    def verify(
        self,
        *,
        bundle: Mapping[str, Any],
        verification: Mapping[str, Any],
        provenance: Mapping[str, Any],
        policy: MigrationTrustPolicy,
        signing_key: str | None = None,
        external_attestations: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        blockers: list[str] = []
        warnings: list[str] = []
        checks: list[dict[str, Any]] = []
        expected_subject = self._subject_builder.from_bundle(bundle)
        bundle_blockers = _verification_blockers(verification)
        schema_blockers = _schema_blockers(provenance)
        subject_blockers = _subject_blockers(provenance, expected_subject)
        policy_blockers = _policy_blockers(provenance, policy)
        signature_blockers = _signature_blockers(provenance, policy, signing_key)
        external_blockers = _external_blockers(provenance, expected_subject, policy, external_attestations)
        blockers.extend(bundle_blockers)
        blockers.extend(schema_blockers)
        blockers.extend(subject_blockers)
        blockers.extend(policy_blockers)
        blockers.extend(signature_blockers)
        blockers.extend(external_blockers)
        checks.extend(
            [
                _check("bundle_integrity", "blocked" if bundle_blockers else "passed", bundle_blockers),
                _check("schema", "blocked" if schema_blockers else "passed", schema_blockers),
                _check("subject", "blocked" if subject_blockers else "passed", subject_blockers),
                _check("policy", "blocked" if policy_blockers else "passed", policy_blockers),
                _check("signature", "blocked" if signature_blockers else "passed", signature_blockers),
                _check("external_attestations", "blocked" if external_blockers else "passed", external_blockers),
            ]
        )
        blockers = list(dict.fromkeys(blockers))
        status = "blocked" if blockers else "warning" if warnings else "trusted"
        payload: dict[str, Any] = {
            "schema_version": TRUST_VERIFICATION_SCHEMA,
            "status": status,
            "bundle_id": bundle.get("bundle_id"),
            "pack_id": bundle.get("pack_id"),
            "provenance_id": provenance.get("provenance_id"),
            "checks": checks,
            "blockers": blockers,
            "warnings": warnings,
            "recommendations": _recommendations(status),
        }
        payload["trust_verification_id"] = trust_fingerprint(
            {
                "bundle_id": payload["bundle_id"],
                "pack_id": payload["pack_id"],
                "provenance_id": payload["provenance_id"],
                "policy": policy.profile,
                "blockers": blockers,
                "warnings": warnings,
            }
        )
        return payload


def _verification_blockers(verification: Mapping[str, Any]) -> tuple[str, ...]:
    blockers = [str(item) for item in verification.get("blockers", []) if str(item)]
    if verification.get("status") == "blocked":
        blockers.append("migration_trust.bundle_verification_blocked")
    return tuple(blockers)


def _schema_blockers(provenance: Mapping[str, Any]) -> tuple[str, ...]:
    blockers: list[str] = []
    if provenance.get("schema_version") != PROVENANCE_SCHEMA:
        blockers.append("migration_trust.provenance_schema_invalid")
    if provenance.get("producer") != PRODUCER:
        blockers.append("migration_trust.producer_invalid")
    return tuple(blockers)


def _subject_blockers(provenance: Mapping[str, Any], expected: Mapping[str, Any]) -> tuple[str, ...]:
    subject = provenance.get("subject", {})
    return (
        ("migration_trust.subject_mismatch",)
        if not isinstance(subject, Mapping) or dict(subject) != dict(expected)
        else ()
    )


def _policy_blockers(provenance: Mapping[str, Any], policy: MigrationTrustPolicy) -> tuple[str, ...]:
    producer = str(provenance.get("producer", ""))
    params, metadata = _params_and_metadata(provenance)
    blockers: list[str] = []
    if policy.allowed_producers and producer not in policy.allowed_producers:
        blockers.append("migration_trust.producer_not_allowed")
    if policy.allowed_repositories and params.get("repository") not in policy.allowed_repositories:
        blockers.append("migration_trust.repository_not_allowed")
    if policy.allowed_refs and not any(
        fnmatch.fnmatchcase(str(params.get("ref", "")), item) for item in policy.allowed_refs
    ):
        blockers.append("migration_trust.ref_not_allowed")
    if policy.require_commit_sha and not str(params.get("commit", "")).strip():
        blockers.append("migration_trust.commit_sha_required")
    if policy.require_ci_run and not str(metadata.get("run_id", "")).strip():
        blockers.append("migration_trust.ci_run_required")
    if policy.require_protected_ref and metadata.get("protected_ref") is not True:
        blockers.append("migration_trust.protected_ref_required")
    return tuple(blockers)


def _signature_blockers(
    provenance: Mapping[str, Any], policy: MigrationTrustPolicy, signing_key: str | None
) -> tuple[str, ...]:
    signature = provenance.get("signature")
    if not isinstance(signature, Mapping):
        return ("migration_trust.signature_required",) if policy.require_signed_provenance else ()
    blockers: list[str] = []
    algorithm = str(signature.get("algorithm", ""))
    key_id = str(signature.get("key_id", ""))
    provenance_id = _expected_provenance_id(provenance)
    if algorithm not in policy.signature.allowed_algorithms:
        blockers.append("migration_trust.signature_algorithm_not_allowed")
    if policy.signature.allowed_key_ids and key_id not in policy.signature.allowed_key_ids:
        blockers.append("migration_trust.signature_key_not_allowed")
    if signature.get("signed_sha256") != provenance_id:
        blockers.append("migration_trust.signature_subject_mismatch")
    if policy.require_signed_provenance and not signing_key:
        blockers.append("migration_trust.signature_key_missing")
    if signing_key and not LocalHmacSignatureProvider().verify(
        signature=signature, signed_sha256=provenance_id, key=signing_key
    ):
        blockers.append("migration_trust.signature_mismatch")
    return tuple(blockers)


def _external_blockers(
    provenance: Mapping[str, Any],
    expected_subject: Mapping[str, Any],
    policy: MigrationTrustPolicy,
    external_attestations: Sequence[Mapping[str, Any]],
) -> tuple[str, ...]:
    if policy.external_attestations.required and not external_attestations:
        return ("migration_trust.external_attestation_required",)
    blockers: list[str] = []
    valid_statuses = {"verified", "trusted", "passed", "success"}
    acceptable_subjects = {
        str(expected_subject.get("subject_digest")),
        str(expected_subject.get("bundle_id")),
        str(expected_subject.get("bundle_digest")),
    }
    for item in external_attestations:
        kind = str(item.get("kind") or item.get("schema_version") or "unknown")
        if policy.external_attestations.allowed_kinds and kind not in policy.external_attestations.allowed_kinds:
            blockers.append(f"migration_trust.external_kind_not_allowed:{kind}")
        if str(item.get("status", "")).lower() not in valid_statuses:
            blockers.append(f"migration_trust.external_status_not_trusted:{kind}")
        subject = str(item.get("subject_digest") or item.get("bundle_id") or item.get("bundle_digest") or "")
        if subject not in acceptable_subjects:
            blockers.append(f"migration_trust.external_subject_mismatch:{kind}")
    return tuple(blockers)


def _expected_provenance_id(provenance: Mapping[str, Any]) -> str:
    raw = {key: value for key, value in provenance.items() if key not in {"provenance_id", "signature"}}
    return trust_fingerprint(raw)


def _params_and_metadata(provenance: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any]]:
    statement = provenance.get("statement", {})
    predicate = statement.get("predicate", {}) if isinstance(statement, Mapping) else {}
    build = predicate.get("buildDefinition", {}) if isinstance(predicate, Mapping) else {}
    run = predicate.get("runDetails", {}) if isinstance(predicate, Mapping) else {}
    params = build.get("externalParameters", {}) if isinstance(build, Mapping) else {}
    metadata = run.get("metadata", {}) if isinstance(run, Mapping) else {}
    return (params if isinstance(params, Mapping) else {}, metadata if isinstance(metadata, Mapping) else {})


def _check(name: str, status: str, details: tuple[str, ...]) -> dict[str, Any]:
    return {"name": name, "status": status, "details": list(details)}


def _recommendations(status: str) -> list[str]:
    if status == "blocked":
        return ["Regenerate or re-sign migration provenance before using this bundle in protected deploys."]
    if status == "warning":
        return ["Review non-blocking trust warnings before promotion."]
    return ["Use this trust receipt with bundle gate in protected CI/CD flows."]


__all__ = [
    "PROVENANCE_SCHEMA",
    "TRUST_POLICY_SCHEMA",
    "TRUST_VERIFICATION_SCHEMA",
    "ArtifactSignatureProvider",
    "ExternalAttestationEvidence",
    "LocalHmacSignatureProvider",
    "MigrationProvenanceBuilder",
    "MigrationProvenanceStatement",
    "MigrationTrustAttestor",
    "MigrationTrustPolicy",
    "MigrationTrustSubject",
    "MigrationTrustVerifier",
]
