"""Core models and signing primitives for schema migration trust evidence."""

from __future__ import annotations

import hmac
from collections.abc import Mapping
from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Protocol

from dpone.readiness.migration_control import stable_fingerprint

TRUST_POLICY_SCHEMA = "dpone.schema_migration_trust_policy.v1"
PROVENANCE_SCHEMA = "dpone.schema_migration_provenance.v1"
TRUST_VERIFICATION_SCHEMA = "dpone.schema_migration_trust_verification.v1"
PRODUCER = "dpone schema migration bundle attest"
INTOTO_STATEMENT = "https://in-toto.io/Statement/v1"
SLSA_PROVENANCE = "https://slsa.dev/provenance/v1"
MigrationProvenanceStatement = dict[str, Any]
ExternalAttestationEvidence = Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class MigrationTrustSignaturePolicy:
    allowed_algorithms: tuple[str, ...] = ("HMAC-SHA256",)
    allowed_key_ids: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> MigrationTrustSignaturePolicy:
        if raw is None:
            return cls()
        return cls(
            allowed_algorithms=_strings(raw.get("allowed_algorithms")) or ("HMAC-SHA256",),
            allowed_key_ids=_strings(raw.get("allowed_key_ids")),
        )


@dataclass(frozen=True, slots=True)
class MigrationExternalAttestationPolicy:
    required: bool = False
    allowed_kinds: tuple[str, ...] = ()

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> MigrationExternalAttestationPolicy:
        if raw is None:
            return cls()
        return cls(required=bool(raw.get("required", False)), allowed_kinds=_strings(raw.get("allowed_kinds")))


@dataclass(frozen=True, slots=True)
class MigrationTrustPolicy:
    profile: str = "advisory"
    require_signed_provenance: bool = False
    allowed_producers: tuple[str, ...] = ()
    allowed_repositories: tuple[str, ...] = ()
    allowed_refs: tuple[str, ...] = ()
    require_protected_ref: bool = False
    require_commit_sha: bool = False
    require_ci_run: bool = False
    signature: MigrationTrustSignaturePolicy = MigrationTrustSignaturePolicy()
    external_attestations: MigrationExternalAttestationPolicy = MigrationExternalAttestationPolicy()

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any] | None) -> MigrationTrustPolicy:
        if raw is None:
            return cls()
        if raw.get("schema_version") not in {None, TRUST_POLICY_SCHEMA}:
            raise ValueError(f"unsupported trust policy schema: {raw.get('schema_version')}")
        signature = raw.get("signature")
        external = raw.get("external_attestations")
        if signature is not None and not isinstance(signature, Mapping):
            raise ValueError("signature policy must be an object")
        if external is not None and not isinstance(external, Mapping):
            raise ValueError("external_attestations policy must be an object")
        return cls(
            profile=str(raw.get("profile", "advisory")),
            require_signed_provenance=bool(raw.get("require_signed_provenance", False)),
            allowed_producers=_strings(raw.get("allowed_producers")),
            allowed_repositories=_strings(raw.get("allowed_repositories")),
            allowed_refs=_strings(raw.get("allowed_refs")),
            require_protected_ref=bool(raw.get("require_protected_ref", False)),
            require_commit_sha=bool(raw.get("require_commit_sha", False)),
            require_ci_run=bool(raw.get("require_ci_run", False)),
            signature=MigrationTrustSignaturePolicy.from_mapping(signature),
            external_attestations=MigrationExternalAttestationPolicy.from_mapping(external),
        )


class MigrationTrustSubject:
    """Builds the stable subject bound to one migration bundle."""

    def from_bundle(self, bundle: Mapping[str, Any]) -> dict[str, Any]:
        attestation = bundle.get("attestation", {}) if isinstance(bundle.get("attestation"), Mapping) else {}
        subject = {
            "bundle_id": bundle.get("bundle_id"),
            "pack_id": bundle.get("pack_id"),
            "target": dict(bundle.get("target", {})) if isinstance(bundle.get("target"), Mapping) else {},
            "bundle_digest": attestation.get("bundle_digest"),
            "artifact_digests": _artifact_digests(bundle),
        }
        return {**subject, "subject_digest": stable_fingerprint(subject)}


class ArtifactSignatureProvider(Protocol):
    """Thin port for signing and verifying provenance digests."""

    def sign(self, *, signed_sha256: str, key: str, key_id: str) -> dict[str, str]:
        raise NotImplementedError

    def verify(self, *, signature: Mapping[str, Any], signed_sha256: str, key: str) -> bool:
        raise NotImplementedError


class LocalHmacSignatureProvider:
    """HMAC provider with the same local-dev semantics as supply-chain signing."""

    algorithm = "HMAC-SHA256"

    def sign(self, *, signed_sha256: str, key: str, key_id: str) -> dict[str, str]:
        return {
            "algorithm": self.algorithm,
            "key_id": key_id,
            "signed_sha256": signed_sha256,
            "signature": _hmac_signature(key=key, signed_sha256=signed_sha256),
        }

    def verify(self, *, signature: Mapping[str, Any], signed_sha256: str, key: str) -> bool:
        expected = _hmac_signature(key=key, signed_sha256=signed_sha256)
        return hmac.compare_digest(expected, str(signature.get("signature", "")))


class MigrationProvenanceBuilder:
    """Builds SLSA/in-toto-style provenance statements."""

    def build(
        self,
        *,
        subject: Mapping[str, Any],
        provenance_source: str,
        repository: str,
        commit_sha: str,
        ref: str,
        run_id: str | None,
        workflow: str | None,
        actor: str | None,
        run_url: str | None,
        builder_id: str,
        protected_ref: bool,
        finished_at: str | None,
    ) -> MigrationProvenanceStatement:
        return {
            "_type": INTOTO_STATEMENT,
            "predicateType": SLSA_PROVENANCE,
            "subject": [{"name": "schema-migration-bundle", "digest": {"sha256": subject["subject_digest"]}}],
            "predicate": {
                "buildDefinition": {
                    "buildType": "https://dpone.dev/schema-migration/bundle-attest/v1",
                    "externalParameters": {
                        "provenance_source": provenance_source,
                        "repository": repository,
                        "commit": commit_sha,
                        "ref": ref,
                        "workflow": workflow,
                    },
                },
                "runDetails": {
                    "builder": {"id": builder_id},
                    "metadata": {
                        "run_id": run_id,
                        "run_url": run_url,
                        "actor": actor,
                        "protected_ref": protected_ref,
                        "finishedOn": finished_at,
                    },
                },
            },
        }


def _artifact_digests(bundle: Mapping[str, Any]) -> list[dict[str, Any]]:
    artifacts = bundle.get("artifacts", [])
    if not isinstance(artifacts, list):
        return []
    return [
        {"kind": item.get("kind"), "path": item.get("path"), "sha256": item.get("sha256")}
        for item in artifacts
        if isinstance(item, Mapping)
    ]


def _hmac_signature(*, key: str, signed_sha256: str) -> str:
    return hmac.new(key.encode("utf-8"), signed_sha256.encode("utf-8"), sha256).hexdigest()


def _strings(raw: object) -> tuple[str, ...]:
    return tuple(str(item) for item in raw if str(item)) if isinstance(raw, list) else ()


def trust_fingerprint(payload: object) -> str:
    return stable_fingerprint(payload)


__all__ = [
    "INTOTO_STATEMENT",
    "PRODUCER",
    "PROVENANCE_SCHEMA",
    "SLSA_PROVENANCE",
    "TRUST_POLICY_SCHEMA",
    "TRUST_VERIFICATION_SCHEMA",
    "ArtifactSignatureProvider",
    "ExternalAttestationEvidence",
    "LocalHmacSignatureProvider",
    "MigrationProvenanceBuilder",
    "MigrationProvenanceStatement",
    "MigrationTrustPolicy",
    "MigrationTrustSubject",
    "trust_fingerprint",
]
