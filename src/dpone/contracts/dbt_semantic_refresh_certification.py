"""Protected exact-coordinate certification boundary for dbt semantic refresh."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Protocol

from dpone.contracts.dbt_contract_validation import canonical_fingerprint, require_digest


@dataclass(frozen=True, slots=True)
class SemanticRefreshCertificationRequest:
    """Exact immutable subject that a protected verifier must certify."""

    certification_coordinate_sha256: str
    manifest_sha256: str
    profile_sha256: str
    toolchain_sha256: str
    verification_time: str
    request_sha256: str

    @classmethod
    def build(
        cls,
        *,
        certification_coordinate_sha256: str,
        manifest_sha256: str,
        profile_sha256: str,
        toolchain_sha256: str,
        verification_time: str,
    ) -> SemanticRefreshCertificationRequest:
        values = {
            "certification_coordinate_sha256": _digest(
                certification_coordinate_sha256,
                "certification_coordinate_sha256",
            ),
            "manifest_sha256": _digest(manifest_sha256, "manifest_sha256"),
            "profile_sha256": _digest(profile_sha256, "profile_sha256"),
            "schema": "dpone.dbt-semantic-refresh-certification-request.v1",
            "toolchain_sha256": _digest(toolchain_sha256, "toolchain_sha256"),
            "verification_time": _utc(verification_time, "verification_time"),
        }
        return cls(
            values["certification_coordinate_sha256"],
            values["manifest_sha256"],
            values["profile_sha256"],
            values["toolchain_sha256"],
            values["verification_time"],
            canonical_fingerprint(values),
        )


@dataclass(frozen=True, slots=True)
class SemanticRefreshCertificationDecision:
    """Verifier result bound to one exact request and protected live receipt."""

    request_sha256: str
    status: str
    receipt_sha256: str | None = None
    certified_at: str | None = None
    expires_at: str | None = None
    revoked: bool = False

    def __post_init__(self) -> None:
        _digest(self.request_sha256, "request_sha256")
        if self.status not in {"CERTIFIED", "UNVERIFIED"}:
            raise ValueError("semantic refresh certification status is unsupported")
        if self.status == "CERTIFIED":
            _digest(self.receipt_sha256, "receipt_sha256")
            certified = _utc(self.certified_at, "certified_at")
            expires = _utc(self.expires_at, "expires_at")
            if certified >= expires:
                raise ValueError("certification expiry must follow certification time")
            if self.revoked:
                raise ValueError("a revoked certification cannot be CERTIFIED")
        elif self.receipt_sha256 is not None:
            raise ValueError("an UNVERIFIED decision cannot claim a receipt")
        if not isinstance(self.revoked, bool):
            raise ValueError("revoked must be boolean")

    def valid_at(self, verification_time: str) -> bool:
        """Return whether the receipt covers one explicit compile instant."""

        if self.status != "CERTIFIED" or self.revoked:
            return False
        observed = _utc(verification_time, "verification_time")
        certified = _utc(self.certified_at, "certified_at")
        expires = _utc(self.expires_at, "expires_at")
        return certified <= observed < expires


class SemanticRefreshCertificationVerifierPort(Protocol):
    """Verify a protected live receipt for one immutable coordinate."""

    def verify(
        self,
        request: SemanticRefreshCertificationRequest,
    ) -> SemanticRefreshCertificationDecision:
        """Return a request-bound decision or fail closed."""


def _digest(value: object, field: str) -> str:
    return require_digest(value, field, "DPONE_DBT_V2_CERTIFICATION_INVALID")


def _utc(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise ValueError(f"{field} must be a canonical UTC timestamp")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise ValueError(f"{field} must be a canonical UTC timestamp") from exc
    if parsed.isoformat().replace("+00:00", "Z") != value:
        raise ValueError(f"{field} must be a canonical UTC timestamp")
    return value


__all__ = [
    "SemanticRefreshCertificationDecision",
    "SemanticRefreshCertificationRequest",
    "SemanticRefreshCertificationVerifierPort",
]
