"""Deterministic identity for one runtime artifact-attestation decision."""

from __future__ import annotations

import hashlib
import json
from typing import Any


def attestation_decision_sha256(
    *,
    plan_sha256: str,
    trust_policy_sha256: str | None,
    effective_requirement: str,
    attestation_status: str,
    artifact_attestation_subject_kind: str | None = None,
    artifact_attestation_backend: str | None = None,
    artifact_attestation_id: str | None = None,
    artifact_attestation_verification_sha256: str | None = None,
    artifact_attestation_observed_claims: tuple[str, ...] = (),
    artifact_attestation_unobserved_claims: tuple[str, ...] = (),
) -> str:
    """Bind one init-only policy decision to its pinned plan and policy snapshot."""

    version = 2 if artifact_attestation_id is not None else 1
    body: dict[str, Any] = {
        "schema": f"dpone.runtime-fetch-ready-attestation-decision.v{version}",
        "plan_sha256": plan_sha256,
        "trust_policy_sha256": trust_policy_sha256,
        "effective_requirement": effective_requirement,
        "attestation_status": attestation_status,
    }
    if version == 2:
        body["artifact_attestation"] = {
            "subject_kind": artifact_attestation_subject_kind,
            "backend": artifact_attestation_backend,
            "attestation_id": artifact_attestation_id,
            "verification_sha256": artifact_attestation_verification_sha256,
            "observed_claims": list(artifact_attestation_observed_claims),
            "unobserved_claims": list(artifact_attestation_unobserved_claims),
        }
    payload = json.dumps(
        body,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(payload).hexdigest()


__all__ = ["attestation_decision_sha256"]
