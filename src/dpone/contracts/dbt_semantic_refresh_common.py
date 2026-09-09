"""Shared, dependency-light proof primitives for semantic refresh V2."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, Protocol

ProofStatus = Literal["PROVEN", "NONCONFORMANT", "UNVERIFIED"]


@dataclass(frozen=True, slots=True)
class SemanticRefreshProofIssue:
    """One deterministic fail-closed proof issue without sensitive values."""

    code: str
    field: str
    message: str
    status: Literal["NONCONFORMANT", "UNVERIFIED"]
    unique_id: str | None = None

    def to_jsonable(self) -> dict[str, str | None]:
        """Return the stable public projection of the issue."""

        return {
            "code": self.code,
            "field": self.field,
            "message": self.message,
            "status": self.status,
            "unique_id": self.unique_id,
        }


@dataclass(frozen=True, slots=True)
class TargetIndependentSqlProof:
    """Structural projection returned by the injected SQL parser adapter."""

    status: str
    compiled_sql_sha256: str | None
    read_relations: tuple[tuple[str, str, str], ...]
    issues: tuple[SemanticRefreshProofIssue, ...]


class TargetIndependentSqlProofPort(Protocol):
    """Prove one immutable query across at least two certified targets."""

    def prove(
        self,
        compiled_sql_by_target: dict[str, str],
        *,
        forbidden_relations: tuple[tuple[str, str, str], ...],
    ) -> TargetIndependentSqlProof: ...


def proof_status(issues: tuple[SemanticRefreshProofIssue, ...]) -> ProofStatus:
    """Reduce issues with known nonconformance taking precedence over doubt."""

    if any(issue.status == "NONCONFORMANT" for issue in issues):
        return "NONCONFORMANT"
    if issues:
        return "UNVERIFIED"
    return "PROVEN"


def nonconformant(
    code: str,
    field: str,
    message: str,
    *,
    unique_id: str | None = None,
) -> SemanticRefreshProofIssue:
    """Build one nonconformance issue."""

    return SemanticRefreshProofIssue(code, field, message, "NONCONFORMANT", unique_id)


def unverified(
    code: str,
    field: str,
    message: str,
    *,
    unique_id: str | None = None,
) -> SemanticRefreshProofIssue:
    """Build one missing-authority issue."""

    return SemanticRefreshProofIssue(code, field, message, "UNVERIFIED", unique_id)


__all__ = [
    "ProofStatus",
    "SemanticRefreshProofIssue",
    "TargetIndependentSqlProof",
    "TargetIndependentSqlProofPort",
    "nonconformant",
    "proof_status",
    "unverified",
]
