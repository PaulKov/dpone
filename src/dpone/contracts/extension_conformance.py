"""Closed extension conformance profiles and report identities."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from dpone.contracts.airflow_deployment import canonical_fingerprint

CHECK_RECEIPT_SCHEMA = "dpone.extension-check-receipt.v1"
REQUEST_SCHEMA = "dpone.extension-conformance-request.v1"
REPORT_SCHEMA = "dpone.extension-conformance.v1"

PROFILE_CHECKS: dict[str, tuple[str, ...]] = {
    "airflow_provider": (
        "public_contract",
        "provider_discovery",
        "airflow_matrix",
        "package_smoke",
        "parse_budget",
    ),
    "recipe_catalog": (
        "bundle_verification",
        "catalog_validation",
        "scaffold_fixture",
        "semantic_fingerprint",
    ),
    "credential_resolver": (
        "contract",
        "recovery",
        "secret_redaction",
        "runtime_isolation",
        "live",
    ),
    "route": (
        "route_contract",
        "reconciliation",
        "recovery",
        "state_evidence_ordering",
        "live",
    ),
}


class ExtensionConformanceError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class ExtensionCheckResult:
    check: str
    status: str
    code: str
    artifact_ref: str | None
    sha256: str | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "check": self.check,
            "status": self.status,
            "code": self.code,
            "artifact_ref": self.artifact_ref,
            "sha256": self.sha256,
        }


@dataclass(frozen=True, slots=True)
class ExtensionConformanceReport:
    conformance_id: str
    profile: str
    subject: dict[str, str]
    status: str
    checks: tuple[ExtensionCheckResult, ...]
    evaluated_at: str

    @property
    def passed(self) -> bool:
        return self.status == "PASS"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": REPORT_SCHEMA,
            "conformance_id": self.conformance_id,
            "profile": self.profile,
            "subject": dict(self.subject),
            "status": self.status,
            "checks": [item.to_dict() for item in self.checks],
            "evaluated_at": self.evaluated_at,
        }

    def to_markdown(self) -> str:
        rows = "\n".join(f"| `{item.check}` | `{item.status}` | `{item.code}` |" for item in self.checks)
        return (
            "# dpone Extension Conformance\n\n"
            f"- profile: `{self.profile}`\n"
            f"- subject: `{self.subject['id']}@{self.subject['version']}`\n"
            f"- digest: `{self.subject['digest']}`\n"
            f"- status: `{self.status}`\n"
            f"- conformance_id: `{self.conformance_id}`\n\n"
            "| Check | Status | Code |\n|---|---|---|\n"
            f"{rows}\n"
        )


def conformance_id(*, profile: str, subject: Mapping[str, str], evidence: list[Mapping[str, Any]]) -> str:
    return canonical_fingerprint(
        {
            "profile": profile,
            "subject": dict(subject),
            "evidence": sorted(
                ({"check": item.get("check"), "sha256": item.get("sha256")} for item in evidence),
                key=lambda item: str(item["check"]),
            ),
        }
    )


__all__ = [
    "CHECK_RECEIPT_SCHEMA",
    "ExtensionCheckResult",
    "ExtensionConformanceError",
    "ExtensionConformanceReport",
    "PROFILE_CHECKS",
    "REPORT_SCHEMA",
    "REQUEST_SCHEMA",
    "conformance_id",
]
