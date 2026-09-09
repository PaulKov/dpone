"""Immutable quality policy snapshots and process-local execution authority."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.quality_failure import QualityFailureBoundary


from collections.abc import Mapping
from dataclasses import dataclass
from threading import Lock
from typing import Any

from dpone.contracts.quality_failure import QualityGateFailure, QualityGateReceiptInvalid, QualityGateReceiptMismatch
from dpone.governance.quality import (
    QualityGatePolicy,
    QualityGateRunner,
    QualityProbeSnapshot,
)
from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricPolicy
from dpone.runtime.governance.quality_execution_support import (
    _immutable_gate_policy,
    _immutable_safe_report,
    _immutable_scope,
    _is_bounded_identity,
    _policy_snapshot_id,
    _quality_boundary,
    _quality_config,
    quality_gate_report_evidence,
)
from dpone.runtime.governance.quality_receipt import (
    QualityGateReceipt,
    validate_quality_gate_receipt_contract,
)


@dataclass(frozen=True, slots=True)
class QualityExecutionSnapshot:
    """Deeply immutable normalized policies captured before runtime side effects."""

    gate_policy: QualityGatePolicy
    acceptance_policy: AcceptanceMetricPolicy
    policy_snapshot_id: str

    def __post_init__(self) -> None:
        gate_policy = _immutable_gate_policy(self.gate_policy)
        expected_id = _policy_snapshot_id(gate_policy, self.acceptance_policy)
        if self.policy_snapshot_id != expected_id:
            raise ValueError("quality execution snapshot identity is invalid")
        object.__setattr__(self, "gate_policy", gate_policy)

    @classmethod
    def from_load_config(cls, load_config: Any) -> QualityExecutionSnapshot:
        """Resolve and freeze gate plus acceptance policy from one load config."""

        gate_policy = _immutable_gate_policy(QualityGatePolicy.from_config(_quality_config(load_config)))
        acceptance_policy = AcceptanceMetricPolicy.from_load_config(load_config)
        return cls(
            gate_policy=gate_policy,
            acceptance_policy=acceptance_policy,
            policy_snapshot_id=_policy_snapshot_id(gate_policy, acceptance_policy),
        )

    def is_inert(self) -> bool:
        return not self.gate_policy.gates and not self.acceptance_policy.enabled


class QualityGateExecution:
    """Locked one-consumer authority for one run/load quality evaluation."""

    def __init__(
        self,
        snapshot: QualityExecutionSnapshot,
        *,
        run_id: str,
        load_id: str,
        runner: object | None = None,
    ) -> None:
        if not isinstance(snapshot, QualityExecutionSnapshot):
            raise QualityGateReceiptInvalid
        if not _is_bounded_identity(run_id) or not _is_bounded_identity(load_id):
            raise QualityGateReceiptInvalid
        self.snapshot = snapshot
        self.run_id = run_id
        self.load_id = load_id
        self._runner = runner if runner is not None else QualityGateRunner()
        self._authority = object()
        self._lock = Lock()
        self._state = "OPEN"
        self._boundary: QualityFailureBoundary | None = None
        self._issued_receipt: QualityGateReceipt | None = None

    def select_boundary(self, boundary: str, *, load_config: Any) -> None:
        """Bind the path-owned mutation boundary before quality evaluation."""

        selected = _quality_boundary(boundary)
        with self._lock:
            self._assert_current_locked(load_config)
            if self._state != "OPEN":
                raise QualityGateReceiptInvalid
            if self._boundary is not None and self._boundary != selected:
                raise QualityGateReceiptInvalid
            self._boundary = selected

    def assert_current(self, *, load_config: Any) -> None:
        """Fail closed when mutable authoring no longer matches the snapshot."""

        with self._lock:
            self._assert_current_locked(load_config)

    def evaluate(
        self,
        *,
        load_config: Any,
        boundary: str,
        source_snapshot: QualityProbeSnapshot,
        target_snapshot: QualityProbeSnapshot,
        scope_summary: Mapping[str, object] | None = None,
    ) -> QualityGateReceipt:
        """Evaluate once, validate the report, and issue one authoritative receipt."""

        selected = _quality_boundary(boundary)
        with self._lock:
            self._assert_current_locked(load_config)
            if (
                self._state != "OPEN"
                or self._boundary != selected
                or not isinstance(source_snapshot, QualityProbeSnapshot)
                or not isinstance(target_snapshot, QualityProbeSnapshot)
            ):
                raise QualityGateReceiptInvalid
            self._state = "EVALUATING"
            try:
                run = getattr(self._runner, "run", None)
                if not callable(run):
                    raise QualityGateReceiptInvalid
                raw_report = run(
                    self.snapshot.gate_policy,
                    source=source_snapshot,
                    target=target_snapshot,
                )
                raw_receipt = QualityGateReceipt(
                    report=raw_report,
                    boundary=selected,
                    scope_summary=scope_summary,
                )
                validated = validate_quality_gate_receipt_contract(
                    raw_receipt,
                    policy=self.snapshot.gate_policy,
                )
                assert validated is not None
                safe_report = _immutable_safe_report(validated.report, self.snapshot.gate_policy)
                receipt = QualityGateReceipt(
                    report=safe_report,
                    boundary=selected,
                    scope_summary=_immutable_scope(validated.scope_summary),
                    policy_snapshot_id=self.snapshot.policy_snapshot_id,
                    run_id=self.run_id,
                    load_id=self.load_id,
                    _authority=self._authority,
                )
                validate_quality_gate_receipt_contract(receipt, policy=self.snapshot.gate_policy)
                if not safe_report.passed:
                    self._state = "FAILED"
                    raise QualityGateFailure(safe_report)
                self._issued_receipt = receipt
                self._state = "ISSUED"
                return receipt
            except QualityGateFailure:
                raise
            except Exception:
                self._state = "INVALID"
                raise

    def evidence_projection(
        self,
        receipt: object,
        *,
        load_config: Any,
    ) -> dict[str, object]:
        """Return a fresh bounded projection after authority and drift validation."""

        with self._lock:
            authoritative = self._require_receipt_locked(
                receipt,
                load_config=load_config,
                expected_state="ISSUED",
            )
            return quality_gate_report_evidence(authoritative.report, self.snapshot.gate_policy)

    def receipt_for_report(
        self,
        report: object,
        *,
        load_config: Any,
    ) -> QualityGateReceipt:
        """Return the exact issued receipt for its immutable report object."""

        with self._lock:
            self._assert_current_locked(load_config)
            if self._state != "ISSUED" or self._issued_receipt is None or report is not self._issued_receipt.report:
                raise QualityGateReceiptInvalid
            return self._issued_receipt

    def accept_payload(
        self,
        receipt: object,
        *,
        load_config: Any,
    ) -> QualityGateReceipt | None:
        """Consume the exact issued receipt once at the payload boundary."""

        with self._lock:
            if self._state == "OPEN" and self.snapshot.is_inert():
                return self._empty_compatibility_locked(receipt, load_config=load_config)
            authoritative = self._require_receipt_locked(
                receipt,
                load_config=load_config,
                expected_state="ISSUED",
            )
            self._state = "PAYLOAD_ACCEPTED"
            return authoritative

    def accept_state(
        self,
        receipt: object,
        *,
        load_config: Any,
    ) -> QualityGateReceipt | None:
        """Consume the payload-accepted receipt once before source state."""

        with self._lock:
            if self._state == "OPEN" and self.snapshot.is_inert():
                return self._empty_compatibility_locked(receipt, load_config=load_config)
            authoritative = self._require_receipt_locked(
                receipt,
                load_config=load_config,
                expected_state="PAYLOAD_ACCEPTED",
            )
            self._state = "STATE_ACCEPTED"
            return authoritative

    def _assert_current_locked(self, load_config: Any) -> None:
        try:
            current = QualityExecutionSnapshot.from_load_config(load_config)
        except Exception:
            raise QualityGateReceiptMismatch from None
        if current.policy_snapshot_id != self.snapshot.policy_snapshot_id:
            raise QualityGateReceiptMismatch

    def _require_receipt_locked(
        self,
        receipt: object,
        *,
        load_config: Any,
        expected_state: str,
    ) -> QualityGateReceipt:
        self._assert_current_locked(load_config)
        if (
            self._state != expected_state
            or not isinstance(receipt, QualityGateReceipt)
            or receipt is not self._issued_receipt
            or receipt._authority is not self._authority
            or receipt.boundary != self._boundary
            or receipt.policy_snapshot_id != self.snapshot.policy_snapshot_id
            or receipt.run_id != self.run_id
            or receipt.load_id != self.load_id
        ):
            raise QualityGateReceiptInvalid
        validate_quality_gate_receipt_contract(receipt, policy=self.snapshot.gate_policy)
        if not receipt.report.passed:
            raise QualityGateFailure(receipt.report)
        return receipt

    def _empty_compatibility_locked(
        self,
        receipt: object,
        *,
        load_config: Any,
    ) -> QualityGateReceipt | None:
        self._assert_current_locked(load_config)
        validated = validate_quality_gate_receipt_contract(
            receipt,
            policy=self.snapshot.gate_policy,
        )
        if validated is None:
            return None
        if (
            validated.report.policy_fingerprint is not None
            or validated.report.gate_contract
            or validated.report.results
            or validated.policy_snapshot_id is not None
            or validated.run_id is not None
            or validated.load_id is not None
            or validated._authority is not None
        ):
            raise QualityGateReceiptInvalid
        return validated
