"""Detached evidence signatures for data product authority gates."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
from collections.abc import Mapping
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

EVIDENCE_SIGNATURE_SCHEMA = "dpone.data_product_evidence_signature.v1"


class EvidenceSigner:
    """Sign dpone evidence artifacts without storing secrets in receipts."""

    def sign(self, *, registry: Mapping[str, Any], artifact: Mapping[str, Any], actor: str) -> dict[str, Any]:
        signing = _signing_options(registry)
        algorithm = str(signing.get("algorithm") or "digest_only")
        blockers: list[str] = []
        warnings: list[str] = []
        if not _actor_known(registry, actor):
            blockers.append(f"data_product_authority.signature_unknown_actor:{actor}")
        artifact_sha = _artifact_sha256(artifact)
        signature_value: str | None = None
        signature_payload_sha: str | None = None
        if algorithm == "hmac_sha256":
            secret = os.getenv(str(signing.get("key_env") or ""))
            if not secret:
                blockers.append("data_product_authority.signature_key_missing")
            else:
                signed = _signature_payload(
                    registry=registry, actor=actor, artifact_sha=artifact_sha, algorithm=algorithm
                )
                signature_payload_sha = _sha256(_canonical_json(signed))
                signature_value = (
                    "hmac_sha256:"
                    + hmac.new(
                        secret.encode("utf-8"),
                        _canonical_json(signed).encode("utf-8"),
                        hashlib.sha256,
                    ).hexdigest()
                )
        elif algorithm == "digest_only":
            warnings.append("data_product_authority.signature_digest_only")
        elif algorithm == "none":
            warnings.append("data_product_authority.signature_disabled")
        else:
            blockers.append(f"data_product_authority.signature_algorithm_unsupported:{algorithm}")
        payload = _signature_receipt(
            status="blocked" if blockers else "warning" if warnings else "signed",
            registry=registry,
            artifact=artifact,
            actor=actor,
            algorithm=algorithm,
            artifact_sha=artifact_sha,
            signature_value=signature_value,
            signature_payload_sha=signature_payload_sha,
            blockers=blockers,
            warnings=warnings,
        )
        payload["evidence_signature_id"] = stable_fingerprint(payload)
        return payload


class EvidenceSignatureVerifier:
    """Verify detached digest/HMAC evidence signatures."""

    def verify(
        self,
        *,
        registry: Mapping[str, Any],
        artifact: Mapping[str, Any],
        signature: Mapping[str, Any],
    ) -> dict[str, Any]:
        blockers: list[str] = []
        warnings: list[str] = []
        algorithm = str(signature.get("algorithm") or "")
        actor = str(signature.get("actor") or "")
        if actor != signature.get("signed_by"):
            blockers.append("data_product_authority.signature_actor_mismatch")
        if not _actor_known(registry, actor):
            blockers.append(f"data_product_authority.signature_unknown_actor:{actor}")
        artifact_sha = _artifact_sha256(artifact)
        if artifact_sha != signature.get("artifact_sha256"):
            blockers.append("data_product_authority.signature_artifact_hash_mismatch")
        if algorithm == "hmac_sha256":
            secret = os.getenv(str(_signing_options(registry).get("key_env") or ""))
            if not secret:
                blockers.append("data_product_authority.signature_key_missing")
            else:
                signed = _signature_payload(
                    registry=registry, actor=actor, artifact_sha=artifact_sha, algorithm=algorithm
                )
                expected = (
                    "hmac_sha256:"
                    + hmac.new(
                        secret.encode("utf-8"),
                        _canonical_json(signed).encode("utf-8"),
                        hashlib.sha256,
                    ).hexdigest()
                )
                if expected != signature.get("signature"):
                    blockers.append("data_product_authority.signature_value_mismatch")
        elif algorithm == "digest_only":
            warnings.append("data_product_authority.signature_digest_only")
        else:
            blockers.append(f"data_product_authority.signature_algorithm_unsupported:{algorithm}")
        payload = _signature_receipt(
            status="blocked" if blockers else "warning" if warnings else "verified",
            registry=registry,
            artifact=artifact,
            actor=actor,
            algorithm=algorithm,
            artifact_sha=artifact_sha,
            signature_value=str(signature.get("signature")) if signature.get("signature") else None,
            signature_payload_sha=str(signature.get("signature_payload_sha256"))
            if signature.get("signature_payload_sha256")
            else None,
            blockers=blockers,
            warnings=warnings,
        )
        payload["source_evidence_signature_id"] = signature.get("evidence_signature_id")
        payload["evidence_signature_id"] = stable_fingerprint(payload)
        return payload


def _signature_receipt(
    *,
    status: str,
    registry: Mapping[str, Any],
    artifact: Mapping[str, Any],
    actor: str,
    algorithm: str,
    artifact_sha: str,
    signature_value: str | None,
    signature_payload_sha: str | None,
    blockers: list[str],
    warnings: list[str],
) -> dict[str, Any]:
    return {
        "schema_version": EVIDENCE_SIGNATURE_SCHEMA,
        "status": status,
        "authority_registry_id": registry.get("authority_registry_id"),
        "actor": actor,
        "signed_by": actor,
        "algorithm": algorithm,
        "artifact_schema_version": artifact.get("schema_version"),
        "artifact_id": _artifact_id(artifact),
        "artifact_sha256": artifact_sha,
        "signature": signature_value,
        "signature_payload_sha256": signature_payload_sha,
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }


def _signature_payload(*, registry: Mapping[str, Any], actor: str, artifact_sha: str, algorithm: str) -> dict[str, Any]:
    return {
        "authority_registry_id": registry.get("authority_registry_id"),
        "actor": actor,
        "algorithm": algorithm,
        "artifact_sha256": artifact_sha,
    }


def _artifact_id(artifact: Mapping[str, Any]) -> str | None:
    for key, value in artifact.items():
        if str(key).endswith("_id") and value:
            return str(value)
    return None


def _artifact_sha256(artifact: Mapping[str, Any]) -> str:
    return _sha256(_canonical_json(artifact))


def _sha256(value: str) -> str:
    return "sha256:" + hashlib.sha256(value.encode("utf-8")).hexdigest()


def _canonical_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _signing_options(registry: Mapping[str, Any]) -> Mapping[str, Any]:
    signing = registry.get("signing")
    return signing if isinstance(signing, Mapping) else {}


def _actor_known(registry: Mapping[str, Any], actor: str) -> bool:
    identities = registry.get("identities")
    return any(isinstance(item, Mapping) and item.get("id") == actor for item in identities or [])


__all__ = ["EVIDENCE_SIGNATURE_SCHEMA", "EvidenceSignatureVerifier", "EvidenceSigner"]
