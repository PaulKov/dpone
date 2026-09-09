"""Canonical document validation for MSSQL prerequisite authority rows."""

from __future__ import annotations

import json

from dpone.contracts.semantic_refresh_route_certification import (
    LiveCertificationStatus,
    SemanticRefreshRouteLiveCertificationReceipt,
)
from dpone.contracts.semantic_refresh_runtime_assurance import (
    RuntimeAssuranceStatus,
    SemanticRefreshRuntimeAssuranceReceipt,
)


def validate_route_authority_json(value: str, expected_digest: str) -> None:
    """Recompute one closed route receipt and require its protected PASS digest."""

    try:
        receipt = SemanticRefreshRouteLiveCertificationReceipt.from_mapping(json.loads(value))
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("route authority document is invalid") from exc
    if (
        receipt.route_certification_receipt_sha256 != expected_digest
        or receipt.status is not LiveCertificationStatus.PASS
    ):
        raise ValueError("route authority document differs from protected PASS receipt")


def validate_runtime_authority_json(
    value: str,
    *,
    expected_digest: str,
    expected_subject_json: str,
) -> None:
    """Recompute one closed runtime receipt and require its exact subject/digest."""

    try:
        receipt = SemanticRefreshRuntimeAssuranceReceipt.from_mapping(json.loads(value))
        actual_subject = json.dumps(
            receipt.subject.to_dict(),
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
    except (TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ValueError("runtime assurance authority document is invalid") from exc
    if (
        receipt.runtime_assurance_receipt_sha256 != expected_digest
        or receipt.status is not RuntimeAssuranceStatus.CERTIFIED
        or actual_subject != expected_subject_json
    ):
        raise ValueError("runtime assurance authority differs from protected receipt")


__all__ = ["validate_route_authority_json", "validate_runtime_authority_json"]
