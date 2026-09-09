"""Closed identity and semantic rules for retention transaction occurrences."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.deployment_cache_retention_state_digest import (
    RetentionStateIdentityError,
    canonical_digest,
    require_digest,
)

MAX_CONTROL_PATH = 4_096
RETENTION_TRANSACTION_PHASES = frozenset(
    {
        "prepared",
        "detached",
        "deletion_started",
        "deployment_deleted",
        "activation_deleted",
        "committed",
        "restored",
        "blocked",
    }
)
TERMINAL_RETENTION_PHASES = frozenset({"committed", "restored"})


class RetentionTransactionContractError(ValueError):
    """One transaction occurrence violates its closed state contract."""


def retention_transaction_id(value: Mapping[str, object]) -> str:
    """Derive an immutable identity for one detach/delete occurrence."""

    body = {
        "schema": "dpone.deployment-cache-retention-transaction.v1",
        "deployment_id": _digest(value.get("deployment_id"), "transaction deployment id"),
        "environment": _text(value.get("environment"), "transaction environment", 128),
        "original_path": _text(value.get("original_path"), "original path", MAX_CONTROL_PATH),
        "detached_path": _text(value.get("detached_path"), "detached path", MAX_CONTROL_PATH),
        "activation_path": _text(value.get("activation_path"), "activation path", MAX_CONTROL_PATH),
        "expected_device": _non_negative_int(value.get("expected_device"), "expected device"),
        "expected_inode": _positive_int(value.get("expected_inode"), "expected inode"),
        "operation_id": _optional_digest(value.get("operation_id"), "transaction operation id"),
    }
    return canonical_digest(body)


def parse_retention_transaction(
    value: object,
    *,
    key: str,
    occurrence_bound: bool,
) -> dict[str, Any]:
    """Validate a legacy deployment-keyed or occurrence-keyed transaction."""

    transaction = _mapping(value)
    fields = {
        "deployment_id",
        "environment",
        "phase",
        "original_path",
        "detached_path",
        "activation_path",
        "expected_device",
        "expected_inode",
    }
    if occurrence_bound:
        fields.update({"operation_id", "transaction_id"})
    if set(transaction) != fields:
        raise RetentionTransactionContractError("retention transaction fields are invalid")
    deployment_id = _digest(transaction.get("deployment_id"), "transaction deployment id")
    if not occurrence_bound and deployment_id != key:
        raise RetentionTransactionContractError("transaction key and deployment identity disagree")
    normalized: dict[str, Any] = {
        "deployment_id": deployment_id,
        "environment": _text(transaction.get("environment"), "transaction environment", 128),
        "phase": _enum(transaction.get("phase"), RETENTION_TRANSACTION_PHASES, "transaction phase"),
        "original_path": _text(transaction.get("original_path"), "original path", MAX_CONTROL_PATH),
        "detached_path": _text(transaction.get("detached_path"), "detached path", MAX_CONTROL_PATH),
        "activation_path": _text(transaction.get("activation_path"), "activation path", MAX_CONTROL_PATH),
        "expected_device": _non_negative_int(transaction.get("expected_device"), "expected device"),
        "expected_inode": _positive_int(transaction.get("expected_inode"), "expected inode"),
    }
    if occurrence_bound:
        normalized["operation_id"] = _optional_digest(transaction.get("operation_id"), "transaction operation id")
        transaction_id = _digest(transaction.get("transaction_id"), "transaction id")
        normalized["transaction_id"] = transaction_id
        if transaction_id != key or transaction_id != retention_transaction_id(normalized):
            raise RetentionTransactionContractError("transaction occurrence identity does not match its body")
    return normalized


def bind_retention_transaction(value: Mapping[str, object]) -> dict[str, Any]:
    """Add and verify the immutable occurrence identity used by recovery v2."""

    candidate = {**dict(value), "transaction_id": retention_transaction_id(value)}
    return parse_retention_transaction(candidate, key=str(candidate["transaction_id"]), occurrence_bound=True)


def validate_recovery_transaction_semantics(
    *,
    status: str,
    restored_deployment_ids: tuple[str, ...],
    pending_deployment_ids: tuple[str, ...],
    transactions: Mapping[str, Mapping[str, object]],
    quarantined_paths: tuple[str, ...],
) -> None:
    pending = tuple(
        sorted(
            {
                str(value["deployment_id"])
                for value in transactions.values()
                if value["phase"] not in TERMINAL_RETENTION_PHASES
            }
        )
    )
    restored = tuple(
        sorted({str(value["deployment_id"]) for value in transactions.values() if value["phase"] == "restored"})
    )
    quarantined = tuple(
        sorted(str(value["detached_path"]) for value in transactions.values() if value["phase"] == "blocked")
    )
    if pending != tuple(sorted(pending_deployment_ids)):
        raise RetentionTransactionContractError("pending ids do not match non-terminal transactions")
    if restored != tuple(sorted(restored_deployment_ids)):
        raise RetentionTransactionContractError("restored ids do not match restored transactions")
    if quarantined != tuple(sorted(quarantined_paths)):
        raise RetentionTransactionContractError("quarantined paths do not match blocked transactions")
    expected_status = "blocked" if quarantined else "recovering" if pending else "recovered"
    if status != expected_status:
        raise RetentionTransactionContractError("recovery status does not match transaction state")


def _mapping(value: object) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise RetentionTransactionContractError("retention transaction must be an object with string keys")
    return value


def _optional_digest(value: object, label: str) -> str | None:
    return None if value is None else _digest(value, label)


def _digest(value: object, label: str) -> str:
    try:
        return require_digest(value, label)
    except RetentionStateIdentityError as exc:
        raise RetentionTransactionContractError(str(exc)) from exc


def _text(value: object, label: str, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise RetentionTransactionContractError(f"{label} must be bounded non-empty text")
    return value


def _enum(value: object, allowed: frozenset[str], label: str) -> str:
    text = _text(value, label, 128)
    if text not in allowed:
        raise RetentionTransactionContractError(f"{label} is invalid")
    return text


def _positive_int(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
        raise RetentionTransactionContractError(f"{label} must be a positive integer")
    return value


def _non_negative_int(value: object, label: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RetentionTransactionContractError(f"{label} must be a non-negative integer")
    return value


__all__ = [
    "MAX_CONTROL_PATH",
    "RETENTION_TRANSACTION_PHASES",
    "RetentionStateIdentityError",
    "RetentionTransactionContractError",
    "bind_retention_transaction",
    "canonical_digest",
    "parse_retention_transaction",
    "require_digest",
    "retention_transaction_id",
    "validate_recovery_transaction_semantics",
]
