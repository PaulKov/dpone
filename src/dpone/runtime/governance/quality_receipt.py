"""Policy-bound producer receipts for runtime quality-gate evaluation."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.quality_failure import QualityFailureBoundary
    from dpone.governance.quality import QualityGatePolicy


from collections.abc import Mapping
from dataclasses import dataclass, field, replace

from dpone.contracts.quality_failure import (
    QualityGateFailure,
    QualityGateReceiptInvalid,
    QualityGateReceiptMismatch,
    QualityGateReceiptRequired,
)
from dpone.governance.quality import (
    GATE_STATUSES,
    QualityGateReport,
    QualityGateResult,
    quality_gate_contract,
    quality_gate_policy_fingerprint,
)
from dpone.runtime.native_transfer_quality_evidence import (
    NativeTransferQualityScopeEvidenceError,
    normalize_native_quality_scope_summary,
)

_REPORT_KIND = "dpone.data_quality.gates.v1"
_RECEIPT_BOUNDARIES = frozenset({"pre_commit", "post_commit", "resume_validation"})
_GATE_CONTRACT_FIELDS = frozenset({"gate_id", "type", "severity"})


@dataclass(frozen=True, slots=True)
class QualityGateReceipt:
    """Producer-owned proof that one coordinator completed quality evaluation."""

    report: QualityGateReport
    boundary: QualityFailureBoundary = "pre_commit"
    scope_summary: Mapping[str, object] | None = None
    policy_snapshot_id: str | None = None
    run_id: str | None = None
    load_id: str | None = None
    _authority: object | None = field(default=None, repr=False, compare=False)


StagedQualityGateReceipt = QualityGateReceipt
"""Compatibility alias for the original staged-only receipt name."""


def validate_quality_gate_receipt(
    receipt: object,
    *,
    policy: QualityGatePolicy,
) -> QualityGateReceipt | None:
    """Validate a receipt against the exact current normalized policy."""

    validated = validate_quality_gate_receipt_contract(receipt, policy=policy)
    if validated is not None and not validated.report.passed:
        raise QualityGateFailure(validated.report)
    return validated


def validate_quality_gate_receipt_contract(
    receipt: object,
    *,
    policy: QualityGatePolicy,
) -> QualityGateReceipt | None:
    """Validate receipt structure and policy binding without requiring success."""

    if receipt is None:
        if policy.gates:
            raise QualityGateReceiptRequired
        return None

    validated = _normalize_receipt(receipt, invalid_message="quality gate receipt is invalid")
    report = validated.report
    contract = report.gate_contract
    fingerprint = report.policy_fingerprint
    expected_contract = quality_gate_contract(policy)

    if fingerprint is None:
        if contract:
            raise QualityGateReceiptInvalid
        if policy.gates:
            raise QualityGateReceiptRequired
        if report.results:
            raise QualityGateReceiptMismatch
    else:
        if fingerprint != quality_gate_policy_fingerprint(policy) or contract != expected_contract:
            raise QualityGateReceiptMismatch

    result_contract = tuple(
        {
            "gate_id": result.gate_id,
            "type": result.type,
            "severity": result.severity,
        }
        for result in report.results
    )
    if result_contract != expected_contract:
        raise QualityGateReceiptMismatch
    return validated


def report_from_quality_gate_receipt(
    receipt: object,
    policy: QualityGatePolicy | None = None,
) -> QualityGateReport | None:
    """Return a trusted report, optionally bound to the supplied current policy."""

    if policy is not None:
        validated = validate_quality_gate_receipt(receipt, policy=policy)
        return validated.report if validated is not None else None
    return _legacy_report(
        receipt,
        invalid_message="quality gate receipt is invalid",
    )


def report_from_staged_quality_receipt(
    receipt: object,
    policy: QualityGatePolicy | None = None,
) -> QualityGateReport | None:
    """Compatibility facade retaining the original staged receipt API."""

    if policy is not None:
        try:
            validated = validate_quality_gate_receipt(receipt, policy=policy)
        except QualityGateReceiptInvalid as exc:
            raise QualityGateReceiptInvalid(
                "staged quality gate receipt is invalid",
                outcome=exc.outcome,
            ) from None
        return validated.report if validated is not None else None
    return _legacy_report(
        receipt,
        invalid_message="staged quality gate receipt is invalid",
    )


def _legacy_report(
    receipt: object,
    *,
    invalid_message: str,
) -> QualityGateReport | None:
    if receipt is None:
        return None
    validated = _normalize_receipt(receipt, invalid_message=invalid_message)
    if not validated.report.passed:
        raise QualityGateFailure(validated.report)
    assert isinstance(receipt, QualityGateReceipt)
    return receipt.report


def _normalize_receipt(
    receipt: object,
    *,
    invalid_message: str,
) -> QualityGateReceipt:
    if (
        not isinstance(receipt, QualityGateReceipt)
        or not isinstance(receipt.report, QualityGateReport)
        or not isinstance(receipt.boundary, str)
        or receipt.boundary not in _RECEIPT_BOUNDARIES
    ):
        raise QualityGateReceiptInvalid(invalid_message)
    report = _normalize_report(receipt.report, invalid_message=invalid_message)
    try:
        scope_summary = normalize_native_quality_scope_summary(receipt.scope_summary)
    except NativeTransferQualityScopeEvidenceError:
        raise QualityGateReceiptInvalid(invalid_message) from None
    _validate_execution_binding(receipt, invalid_message=invalid_message)
    return replace(receipt, report=report, scope_summary=scope_summary)


def _normalize_report(
    report: QualityGateReport,
    *,
    invalid_message: str,
) -> QualityGateReport:
    if report.kind != _REPORT_KIND or not isinstance(report.results, tuple):
        raise QualityGateReceiptInvalid(invalid_message)
    fingerprint = report.policy_fingerprint
    if fingerprint is not None and not _is_canonical_fingerprint(fingerprint):
        raise QualityGateReceiptInvalid(invalid_message)
    contract = _normalize_gate_contract(report.gate_contract, invalid_message=invalid_message)
    for result in report.results:
        _validate_result(result, invalid_message=invalid_message)
    return replace(report, gate_contract=contract)


def _normalize_gate_contract(
    value: object,
    *,
    invalid_message: str,
) -> tuple[dict[str, str], ...]:
    if not isinstance(value, tuple):
        raise QualityGateReceiptInvalid(invalid_message)
    normalized: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, Mapping) or frozenset(item) != _GATE_CONTRACT_FIELDS:
            raise QualityGateReceiptInvalid(invalid_message)
        if any(not isinstance(item.get(field), str) for field in _GATE_CONTRACT_FIELDS):
            raise QualityGateReceiptInvalid(invalid_message)
        normalized.append(
            {
                "gate_id": item["gate_id"],
                "type": item["type"],
                "severity": item["severity"],
            }
        )
    return tuple(normalized)


def _validate_result(
    result: object,
    *,
    invalid_message: str,
) -> None:
    if not isinstance(result, QualityGateResult):
        raise QualityGateReceiptInvalid(invalid_message)
    if (
        not isinstance(result.gate_id, str)
        or not isinstance(result.type, str)
        or not isinstance(result.status, str)
        or result.status not in GATE_STATUSES
        or not isinstance(result.severity, str)
        or not isinstance(result.metrics, Mapping)
        or not isinstance(result.message, str)
    ):
        raise QualityGateReceiptInvalid(invalid_message)


def _is_canonical_fingerprint(value: object) -> bool:
    if not isinstance(value, str) or not value.startswith("sha256:"):
        return False
    suffix = value.removeprefix("sha256:")
    return len(suffix) == 64 and all(character in "0123456789abcdef" for character in suffix)


def _validate_execution_binding(
    receipt: QualityGateReceipt,
    *,
    invalid_message: str,
) -> None:
    binding = (receipt.policy_snapshot_id, receipt.run_id, receipt.load_id)
    if all(value is None for value in binding):
        return
    if (
        not _is_canonical_fingerprint(receipt.policy_snapshot_id)
        or not _is_bounded_identity(receipt.run_id)
        or not _is_bounded_identity(receipt.load_id)
    ):
        raise QualityGateReceiptInvalid(invalid_message)


def _is_bounded_identity(value: object) -> bool:
    return isinstance(value, str) and 0 < len(value) <= 256


__all__ = [
    "QualityGateReceipt",
    "StagedQualityGateReceipt",
    "report_from_quality_gate_receipt",
    "report_from_staged_quality_receipt",
    "validate_quality_gate_receipt",
    "validate_quality_gate_receipt_contract",
]
