"""Closed GitHub authentication of original nonproduction grant bytes.

This bridge reuses the actual offline GitHub verifier and existing external
runtime-artifact-trust-policy.v2. It neither signs nor discovers trusted roots.
The NP identity is a composite workflow URL@commit selector; it is not the
certificate SAN spelling. Cosign public-key labels are explicitly unsupported.
"""

from __future__ import annotations

import os
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Protocol

from dpone.adapters.github_artifact_attestation import GitHubAttestationVerification
from dpone.contracts.nonproduction_authority import NonproductionAuthorityPolicy, NonproductionSignatureSubject
from dpone.contracts.nonproduction_grants import parse_nonproduction_grant
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError, parse_document
from dpone.contracts.runtime_artifact_attestation import (
    GITHUB_ATTESTATION_BACKEND,
    MAX_ATTESTATION_BUNDLE_BYTES,
    RUNTIME_ARTIFACT_TRUST_POLICY_V2,
    GitHubArtifactAttestationPolicy,
    github_cli_version_supported,
    parse_runtime_artifact_trust_policy,
)
from dpone.ports.nonproduction_authentication import NonproductionTrustSnapshot


class _GitHubFileVerifier(Protocol):
    """The existing real file verifier is injected only by trusted app roots."""

    def verify_files(
        self,
        *,
        subject_path: Path,
        bundle_path: Path,
        policy: GitHubArtifactAttestationPolicy,
        expected_subject_sha256: str | None = None,
    ) -> GitHubAttestationVerification: ...


class GitHubNonproductionGrantSignatureVerifier:
    """Exact original-byte verification, with no report or backend fallback."""

    def __init__(self, *, verifier: _GitHubFileVerifier) -> None:
        self._verifier = verifier

    def require_trust(self, *, trust: NonproductionTrustSnapshot, now: datetime) -> None:
        """Strictly reparse existing policy and public root freshness each time."""
        self._policies(trust, now)

    def verify(
        self,
        *,
        grant_bytes: bytes,
        sigstore_bundle: bytes,
        trust: NonproductionTrustSnapshot,
        now: datetime,
    ) -> NonproductionSignatureSubject:
        """Return a subject only after the injected real verifier has succeeded.

        The caller must still refresh independent policy/revocation/time and
        compare phase subjects; the runtime authenticator owns those decisions.
        """
        grant = parse_nonproduction_grant(grant_bytes)
        policy, github = self._policies(trust, now)
        policy.require_scope(grant.scope)
        if type(sigstore_bundle) is not bytes or not 1 <= len(sigstore_bundle) <= MAX_ATTESTATION_BUNDLE_BYTES:
            raise NonproductionAuthorityError("signature_bundle_budget") from None
        try:
            with tempfile.TemporaryDirectory(prefix="dpone-nonproduction-grant-") as directory:
                root = Path(directory)
                subject_path = _private_file(root / "grant.json", grant_bytes)
                bundle_path = _private_file(root / "signature.jsonl", sigstore_bundle)
                result = self._verifier.verify_files(
                    subject_path=subject_path,
                    bundle_path=bundle_path,
                    policy=github,
                    expected_subject_sha256=grant.grant_sha256,
                )
            if (
                type(result) is not GitHubAttestationVerification
                or result.subject_sha256 != grant.grant_sha256
                or type(result.verified_attestations) is not int
                or result.verified_attestations < 1
                or type(result.verifier_version) is not str
                or not github_cli_version_supported(result.verifier_version, github.gh)
            ):
                raise ValueError
        except Exception:
            raise NonproductionAuthorityError("signature_verification") from None
        return grant.signature_subject(policy)

    @staticmethod
    def _policies(
        trust: NonproductionTrustSnapshot, now: datetime
    ) -> tuple[NonproductionAuthorityPolicy, GitHubArtifactAttestationPolicy]:
        try:
            if type(trust) is not NonproductionTrustSnapshot:
                raise NonproductionAuthorityError("trust_snapshot")
            trust.require_integrity()
            policy = NonproductionAuthorityPolicy.from_bytes(trust.policy_bytes, expected_sha256=trust.policy_sha256)
            policy.require_current(now=now, current_revocation_epoch=trust.current_revocation_epoch)
            if policy.signer.backend != GITHUB_ATTESTATION_BACKEND:
                raise NonproductionAuthorityError("signer_backend_unsupported")
            parsed = parse_runtime_artifact_trust_policy(
                parse_document(trust.verifier_policy_bytes, RUNTIME_ARTIFACT_TRUST_POLICY_V2), now=now
            )
            github = parsed.verifier
            if parsed.trust_tier != "non_production" or parsed.attestations != "required_for_prod" or github is None:
                raise NonproductionAuthorityError("signer_trust_policy")
            if (
                policy.signer.issuer != github.cert_oidc_issuer
                or policy.signer.identity != f"https://github.com/{github.signer_workflow}@{github.signer_digest}"
                or policy.signer.trust_root_sha256 != github.trusted_root_sha256
                or policy.source_repository != "https://github.com/" + github.repository
            ):
                raise NonproductionAuthorityError("signer_policy_binding")
            return policy, github
        except NonproductionAuthorityError as error:
            raise NonproductionAuthorityError(error.reason) from None
        except Exception:
            raise NonproductionAuthorityError("signer_trust_policy") from None


def _private_file(path: Path, data: bytes) -> Path:
    """Create only a bounded public-artifact copy inside our private directory."""
    with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), "wb") as stream:
        stream.write(data)
    return path
