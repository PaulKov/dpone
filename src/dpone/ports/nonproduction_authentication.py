"""Injected trust and original-byte signature boundaries for synthetic grants.

The provider is selected by the protected composition root, never by grant or
artifact fields. Snapshots carry independently pinned existing policy documents;
they introduce no new wire schema. Integrity and returned subject values alone
are not authentication, enrollment, consumption or permission to issue writers.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from typing import Protocol

from dpone.contracts.nonproduction_authority import NonproductionSignatureSubject, require_epoch
from dpone.contracts.nonproduction_scope import MAX_DOCUMENT_BYTES, NonproductionAuthorityError, digest


@dataclass(frozen=True, slots=True)
class NonproductionTrustSnapshot:
    """One immutable observation from an independent policy/revocation source.

    Both digests are external pins; computing them from an artifact's supplied
    policy does not establish trust. The verifier document is the existing
    runtime-artifact-trust-policy.v2, with its original public root bytes.
    """

    policy_bytes: bytes = field(repr=False)
    policy_sha256: str
    verifier_policy_bytes: bytes = field(repr=False)
    verifier_policy_sha256: str
    current_revocation_epoch: int

    def require_integrity(self) -> None:
        """Check immutable bounded transport bytes against both external pins."""
        for raw, expected, reason in (
            (self.policy_bytes, self.policy_sha256, "policy_pin"),
            (self.verifier_policy_bytes, self.verifier_policy_sha256, "verifier_policy_pin"),
        ):
            digest(expected)
            if type(raw) is not bytes or not 1 <= len(raw) <= MAX_DOCUMENT_BYTES:
                raise NonproductionAuthorityError("trust_document_budget") from None
            if "sha256:" + sha256(raw).hexdigest() != expected:
                raise NonproductionAuthorityError(reason) from None
        require_epoch(self.current_revocation_epoch)


class NonproductionTrustProvider(Protocol):
    """Reopen current independent policy, pins and revocation on every read."""

    def read(self) -> NonproductionTrustSnapshot: ...


class NonproductionGrantSignatureVerifier(Protocol):
    """Trusted capability, injected independently of untrusted grant contents."""

    def require_trust(self, *, trust: NonproductionTrustSnapshot, now: datetime) -> None:
        """Revalidate signer configuration and root freshness without signing."""
        ...

    def verify(
        self,
        *,
        grant_bytes: bytes,
        sigstore_bundle: bytes,
        trust: NonproductionTrustSnapshot,
        now: datetime,
    ) -> NonproductionSignatureSubject:
        """Verify original bytes; never accept an artifact's verification report."""
        ...
