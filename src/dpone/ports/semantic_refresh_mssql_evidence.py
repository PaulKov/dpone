"""Immutable MSSQL producer evidence and its capability port."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

from dpone._compat import StrEnum

_SHA256_PREFIX = "sha256:"


class MssqlTransactionDisposition(StrEnum):
    """Positive engine evidence available during producer reconciliation."""

    NOT_OPENED = "NOT_OPENED"
    ROLLED_BACK = "ROLLED_BACK"
    LIVE = "LIVE"
    UNKNOWN = "UNKNOWN"


def mssql_session_evidence_sha256(
    *,
    operation_id: str,
    operation_plan_sha256: str,
    attempt_binding_sha256: str,
    fencing_epoch: int,
    transaction_disposition: MssqlTransactionDisposition,
    controller_proves_not_invoked: bool,
) -> str:
    """Digest one durable engine/controller session-outcome record."""

    _require_text(operation_id, "operation_id")
    _require_digest(operation_plan_sha256, "operation_plan_sha256")
    _require_digest(attempt_binding_sha256, "attempt_binding_sha256")
    _require_positive(fencing_epoch, "fencing_epoch")
    if not isinstance(transaction_disposition, MssqlTransactionDisposition):
        raise ValueError("transaction_disposition is unsupported")
    if not isinstance(controller_proves_not_invoked, bool):
        raise ValueError("controller_proves_not_invoked must be boolean")
    payload = {
        "attempt_binding_sha256": attempt_binding_sha256,
        "controller_proves_not_invoked": controller_proves_not_invoked,
        "fencing_epoch": fencing_epoch,
        "operation_id": operation_id,
        "operation_plan_sha256": operation_plan_sha256,
        "schema": "dpone.semantic-refresh-mssql-session-evidence.v1",
        "transaction_disposition": transaction_disposition.value,
    }
    return _sha256(payload)


@dataclass(frozen=True, slots=True)
class MssqlImageEvidence:
    """Identity and digest for one durable full-scope SQL Server image."""

    operation_id: str
    operation_plan_sha256: str
    attempt_binding_sha256: str
    fencing_epoch: int
    image_role: str
    image_sha256: str
    row_count: int
    committed: bool

    def __post_init__(self) -> None:
        _require_text(self.operation_id, "operation_id")
        _require_digest(self.operation_plan_sha256, "operation_plan_sha256")
        _require_digest(self.attempt_binding_sha256, "attempt_binding_sha256")
        _require_positive(self.fencing_epoch, "fencing_epoch")
        if self.image_role not in {"BEFORE", "AFTER"}:
            raise ValueError("image_role must equal BEFORE or AFTER")
        _require_digest(self.image_sha256, "image_sha256")
        _require_non_negative(self.row_count, "row_count")
        if not isinstance(self.committed, bool):
            raise ValueError("committed must be boolean")


@dataclass(frozen=True, slots=True)
class MssqlBuildReceiptEvidence:
    """Immutable receipt inserted in the same transaction as target DML/images."""

    operation_id: str
    operation_plan_sha256: str
    attempt_binding_sha256: str
    fencing_epoch: int
    before_image_sha256: str
    after_image_sha256: str
    inserted_count: int
    updated_count: int
    model_unique_id: str | None = None
    strategy_authority_sha256: str | None = None
    before_image_relation: str | None = None
    after_image_relation: str | None = None
    build_receipt_sha256: str | None = None

    @classmethod
    def build_exact(
        cls,
        *,
        operation_id: str,
        operation_plan_sha256: str,
        attempt_binding_sha256: str,
        fencing_epoch: int,
        before_image_sha256: str,
        after_image_sha256: str,
        inserted_count: int,
        updated_count: int,
        model_unique_id: str,
        strategy_authority_sha256: str,
        before_image_relation: str,
        after_image_relation: str,
    ) -> MssqlBuildReceiptEvidence:
        """Build exact SQL Server receipt evidence and derive its digest."""

        payload: dict[str, object] = {
            "after_image_relation": after_image_relation,
            "after_image_sha256": after_image_sha256,
            "attempt_binding_sha256": attempt_binding_sha256,
            "before_image_relation": before_image_relation,
            "before_image_sha256": before_image_sha256,
            "fencing_epoch": fencing_epoch,
            "inserted_count": inserted_count,
            "model_unique_id": model_unique_id,
            "operation_id": operation_id,
            "operation_plan_sha256": operation_plan_sha256,
            "schema": "dpone.semantic-refresh-model-build-receipt.v1",
            "strategy_authority_sha256": strategy_authority_sha256,
            "updated_count": updated_count,
        }
        return cls(
            operation_id=operation_id,
            operation_plan_sha256=operation_plan_sha256,
            attempt_binding_sha256=attempt_binding_sha256,
            fencing_epoch=fencing_epoch,
            before_image_sha256=before_image_sha256,
            after_image_sha256=after_image_sha256,
            inserted_count=inserted_count,
            updated_count=updated_count,
            model_unique_id=model_unique_id,
            strategy_authority_sha256=strategy_authority_sha256,
            before_image_relation=before_image_relation,
            after_image_relation=after_image_relation,
            build_receipt_sha256=_build_receipt_sha256(payload),
        )

    def __post_init__(self) -> None:
        _require_text(self.operation_id, "operation_id")
        _require_digest(self.operation_plan_sha256, "operation_plan_sha256")
        _require_digest(self.attempt_binding_sha256, "attempt_binding_sha256")
        _require_positive(self.fencing_epoch, "fencing_epoch")
        _require_digest(self.before_image_sha256, "before_image_sha256")
        _require_digest(self.after_image_sha256, "after_image_sha256")
        _require_non_negative(self.inserted_count, "inserted_count")
        _require_non_negative(self.updated_count, "updated_count")
        extended = (
            self.model_unique_id,
            self.strategy_authority_sha256,
            self.before_image_relation,
            self.after_image_relation,
            self.build_receipt_sha256,
        )
        if any(value is not None for value in extended):
            if not all(value is not None for value in extended):
                raise ValueError("build receipt authority fields must be complete")
            assert self.model_unique_id is not None
            assert self.strategy_authority_sha256 is not None
            assert self.before_image_relation is not None
            assert self.after_image_relation is not None
            assert self.build_receipt_sha256 is not None
            _require_text(self.model_unique_id, "model_unique_id")
            _require_digest(self.strategy_authority_sha256, "strategy_authority_sha256")
            _require_text(self.before_image_relation, "before_image_relation")
            _require_text(self.after_image_relation, "after_image_relation")
            _require_digest(self.build_receipt_sha256, "build_receipt_sha256")
            if mssql_build_receipt_sha256(self) != self.build_receipt_sha256:
                raise ValueError("build receipt digest differs from exact receipt content")


def mssql_build_receipt_sha256(receipt: MssqlBuildReceiptEvidence) -> str:
    """Recompute the exact SQL Server NVARCHAR model-build receipt digest."""

    if any(
        value is None
        for value in (
            receipt.model_unique_id,
            receipt.strategy_authority_sha256,
            receipt.before_image_relation,
            receipt.after_image_relation,
        )
    ):
        raise ValueError("build receipt authority fields are incomplete")
    payload = {
        "after_image_relation": receipt.after_image_relation,
        "after_image_sha256": receipt.after_image_sha256,
        "attempt_binding_sha256": receipt.attempt_binding_sha256,
        "before_image_relation": receipt.before_image_relation,
        "before_image_sha256": receipt.before_image_sha256,
        "fencing_epoch": receipt.fencing_epoch,
        "inserted_count": receipt.inserted_count,
        "model_unique_id": receipt.model_unique_id,
        "operation_id": receipt.operation_id,
        "operation_plan_sha256": receipt.operation_plan_sha256,
        "schema": "dpone.semantic-refresh-model-build-receipt.v1",
        "strategy_authority_sha256": receipt.strategy_authority_sha256,
        "updated_count": receipt.updated_count,
    }
    return _build_receipt_sha256(payload)


def _build_receipt_sha256(payload: Mapping[str, object]) -> str:
    raw = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    return _SHA256_PREFIX + hashlib.sha256(raw.encode("utf-16le")).hexdigest()


@dataclass(frozen=True, slots=True)
class MssqlOperationEvidence:
    """One bounded authoritative snapshot used to reconcile a model outcome."""

    operation_id: str
    operation_plan_sha256: str
    attempt_binding_sha256: str
    fencing_epoch: int
    database_available: bool
    controller_proves_not_invoked: bool
    transaction_disposition: MssqlTransactionDisposition
    receipt: MssqlBuildReceiptEvidence | None
    before_image: MssqlImageEvidence | None
    after_image: MssqlImageEvidence | None

    def __post_init__(self) -> None:
        _require_text(self.operation_id, "operation_id")
        _require_digest(self.operation_plan_sha256, "operation_plan_sha256")
        _require_digest(self.attempt_binding_sha256, "attempt_binding_sha256")
        _require_positive(self.fencing_epoch, "fencing_epoch")
        for field_name in ("database_available", "controller_proves_not_invoked"):
            if not isinstance(getattr(self, field_name), bool):
                raise ValueError(f"{field_name} must be boolean")
        if not isinstance(self.transaction_disposition, MssqlTransactionDisposition):
            raise ValueError("transaction_disposition is unsupported")


def mssql_operation_evidence_sha256(evidence: MssqlOperationEvidence) -> str:
    """Digest the complete durable evidence snapshot used by outcome policy."""

    def image(item: MssqlImageEvidence | None) -> dict[str, object] | None:
        if item is None:
            return None
        return {
            "attempt_binding_sha256": item.attempt_binding_sha256,
            "committed": item.committed,
            "fencing_epoch": item.fencing_epoch,
            "image_role": item.image_role,
            "image_sha256": item.image_sha256,
            "operation_id": item.operation_id,
            "operation_plan_sha256": item.operation_plan_sha256,
            "row_count": item.row_count,
        }

    receipt = evidence.receipt
    payload = {
        "after_image": image(evidence.after_image),
        "attempt_binding_sha256": evidence.attempt_binding_sha256,
        "before_image": image(evidence.before_image),
        "controller_proves_not_invoked": evidence.controller_proves_not_invoked,
        "database_available": evidence.database_available,
        "fencing_epoch": evidence.fencing_epoch,
        "operation_id": evidence.operation_id,
        "operation_plan_sha256": evidence.operation_plan_sha256,
        "receipt": (
            None
            if receipt is None
            else {
                "after_image_sha256": receipt.after_image_sha256,
                "attempt_binding_sha256": receipt.attempt_binding_sha256,
                "before_image_sha256": receipt.before_image_sha256,
                "fencing_epoch": receipt.fencing_epoch,
                "inserted_count": receipt.inserted_count,
                "operation_id": receipt.operation_id,
                "operation_plan_sha256": receipt.operation_plan_sha256,
                "updated_count": receipt.updated_count,
                "build_receipt_sha256": receipt.build_receipt_sha256,
                "model_unique_id": receipt.model_unique_id,
                "strategy_authority_sha256": receipt.strategy_authority_sha256,
                "before_image_relation": receipt.before_image_relation,
                "after_image_relation": receipt.after_image_relation,
            }
        ),
        "schema": "dpone.semantic-refresh-mssql-operation-evidence.v1",
        "transaction_disposition": evidence.transaction_disposition.value,
    }
    return _sha256(payload)


class SemanticRefreshMssqlEvidencePort(Protocol):
    """Read one transactionally consistent producer-evidence snapshot."""

    def read_operation_evidence(
        self,
        *,
        operation_id: str,
        attempt_binding_sha256: str,
    ) -> MssqlOperationEvidence:
        """Return exact operation/attempt evidence without outcome inference."""


def _sha256(payload: Mapping[str, object]) -> str:
    raw = json.dumps(payload, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return _SHA256_PREFIX + hashlib.sha256(raw).hexdigest()


def _require_text(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")


def _require_digest(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not value.startswith(_SHA256_PREFIX)
        or len(value) != len(_SHA256_PREFIX) + 64
        or any(character not in "0123456789abcdef" for character in value[len(_SHA256_PREFIX) :])
    ):
        raise ValueError(f"{field_name} must be a lowercase sha256 digest")


def _require_non_negative(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")


def _require_positive(value: int, field_name: str) -> None:
    _require_non_negative(value, field_name)
    if value == 0:
        raise ValueError(f"{field_name} must be positive")


__all__ = [
    "MssqlBuildReceiptEvidence",
    "MssqlImageEvidence",
    "MssqlOperationEvidence",
    "MssqlTransactionDisposition",
    "SemanticRefreshMssqlEvidencePort",
    "mssql_operation_evidence_sha256",
    "mssql_build_receipt_sha256",
    "mssql_session_evidence_sha256",
]
