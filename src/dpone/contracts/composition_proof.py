"""Exact scope of protected attempt proofs, independent of receipt authenticity.

Only trusted backend producers may insert these records. The store must compare
their authority set with its immutable issuance journal; a caller-supplied
document or digest is never closure, quiescence or business-outcome evidence.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from uuid import UUID

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_activation import CompositionAdmissionError, require_digest
from dpone.contracts.composition_attempt import CompositionAttemptIdentity


@dataclass(frozen=True, order=True, slots=True)
class CompositionProofAuthority:
    """One independently journaled principal on one protected backend service."""

    connector: str
    service_id: str
    principal_id: str

    def __post_init__(self) -> None:
        try:
            valid_service = str(UUID(self.service_id)) == self.service_id
            if self.connector == "mssql":
                valid_principal = re.fullmatch(r"mssql-sid:[0-9a-f]{32}", self.principal_id) is not None
            elif self.connector == "clickhouse":
                prefix, _, principal = self.principal_id.partition(":")
                valid_principal = prefix == "clickhouse-user" and str(UUID(principal)) == principal
            else:
                valid_principal = False
        except (ValueError, TypeError, AttributeError):
            valid_service = valid_principal = False
        if not valid_service or not valid_principal:
            raise CompositionAdmissionError("proof_authority")


def composition_attempt_epoch_subject(attempt: CompositionAttemptIdentity) -> str:
    """Bind the complete ordered attempt guard closure without changing its hash."""
    attempt.__post_init__()
    return canonical_fingerprint({"guard_epochs": attempt.guard_epochs})


@dataclass(frozen=True, slots=True)
class CompositionAttemptProof:
    """Canonical scope and producer-evidence subject for one protected proof."""

    kind: str
    attempt_sha256: str
    activation_request_sha256: str
    guard_epochs_sha256: str
    authorities: tuple[CompositionProofAuthority, ...]
    evidence_sha256: str
    outcome_state: str | None = None

    def __post_init__(self) -> None:
        if self.kind not in {"CLOSED_GATES", "QUIESCENCE", "OUTCOME"}:
            raise CompositionAdmissionError("proof_kind")
        if (self.kind == "OUTCOME" and self.outcome_state not in {"SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"}) or (
            self.kind != "OUTCOME" and self.outcome_state is not None
        ):
            raise CompositionAdmissionError("proof_outcome_state")
        for value in (
            self.attempt_sha256,
            self.activation_request_sha256,
            self.guard_epochs_sha256,
            self.evidence_sha256,
        ):
            require_digest(value)
        if (
            type(self.authorities) is not tuple
            or not self.authorities
            or len(self.authorities) > 8192
            or any(type(value) is not CompositionProofAuthority for value in self.authorities)
        ):
            raise CompositionAdmissionError("proof_authority_closure")
        for authority in self.authorities:
            authority.__post_init__()
        if self.authorities != tuple(sorted(set(self.authorities))):
            raise CompositionAdmissionError("proof_authority_closure")

    def to_dict(self) -> dict[str, object]:
        """Return detached scope bytes for the protected proof producer."""
        return {"schema": "dpone.composition-attempt-proof.v1", **asdict(self)}

    @property
    def proof_sha256(self) -> str:
        return canonical_fingerprint(self.to_dict())

    def require_attempt(self, attempt: CompositionAttemptIdentity) -> CompositionAttemptProof:
        """Require exact attempt, parent and epoch closure before issuer comparison."""
        self.__post_init__()
        attempt.__post_init__()
        if (self.attempt_sha256, self.activation_request_sha256, self.guard_epochs_sha256) != (
            attempt.attempt_sha256,
            attempt.activation_request_sha256,
            composition_attempt_epoch_subject(attempt),
        ):
            raise CompositionAdmissionError("proof_attempt")
        return self
