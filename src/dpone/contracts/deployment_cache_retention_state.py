"""Canonical contracts for destructive Airflow cache retention state."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime
from typing import Any
from uuid import UUID

from dpone.contracts.deployment_cache_retention_transaction import (
    MAX_CONTROL_PATH,
    RETENTION_TRANSACTION_PHASES,
    RetentionStateIdentityError,
    RetentionTransactionContractError,
    bind_retention_transaction,
    parse_retention_transaction,
    validate_recovery_transaction_semantics,
)
from dpone.contracts.deployment_cache_retention_transaction import (
    canonical_digest as _canonical_digest,
)
from dpone.contracts.deployment_cache_retention_transaction import (
    require_digest as _require_digest,
)

ACTIVATION_HISTORY_SCHEMA = "dpone.deployment-cache-activation-history.v2"
RETENTION_RECOVERY_SCHEMA_V1 = "dpone.deployment-cache-retention-recovery.v1"
RETENTION_RECOVERY_SCHEMA = "dpone.deployment-cache-retention-recovery.v2"
MAX_ACTIVATION_HISTORY_ENTRIES = 10_000
MAX_DAG_IDS = 5_000
MAX_RECOVERY_TRANSACTIONS = 10_000


class DeploymentCacheRetentionStateError(ValueError):
    """One retention state document violates its published closed contract."""


def retention_operation_id(
    *,
    environment: str,
    reviewed_plan_sha256: str,
    review_id: str | None = None,
) -> str:
    """Derive one stable replay identity from a reviewed destructive attempt."""

    _bounded_text(environment, "environment", maximum=128)
    require_digest(reviewed_plan_sha256, "reviewed plan sha256")
    body = {
        "schema": "dpone.deployment-cache-retention-operation.v1",
        "environment": environment,
        "reviewed_plan_sha256": reviewed_plan_sha256,
    }
    if review_id is not None:
        body["schema"] = "dpone.deployment-cache-retention-operation.v2"
        body["review_id"] = canonical_uuid4(review_id)
    return canonical_digest(body)


def parse_activation_history(value: Mapping[str, object]) -> dict[str, Any]:
    _require_exact_keys(value, {"schema", "revision", "entries", "legacy_v1_diagnostics"}, "history")
    if value.get("schema") != ACTIVATION_HISTORY_SCHEMA:
        raise DeploymentCacheRetentionStateError("activation history schema is invalid")
    entries = _mapping(value.get("entries"), "activation history entries")
    if len(entries) > MAX_ACTIVATION_HISTORY_ENTRIES:
        raise DeploymentCacheRetentionStateError("activation history has too many entries")
    normalized_entries: dict[str, dict[str, Any]] = {}
    for activation_id, raw_entry in entries.items():
        canonical_id = canonical_uuid4(activation_id)
        if canonical_id in normalized_entries:
            raise DeploymentCacheRetentionStateError("activation history contains duplicate occurrences")
        normalized_entries[canonical_id] = _parse_activation_entry(raw_entry, activation_id=canonical_id)
    diagnostics = _parse_legacy_diagnostics(value.get("legacy_v1_diagnostics"))
    body = {
        "schema": ACTIVATION_HISTORY_SCHEMA,
        "entries": {key: normalized_entries[key] for key in sorted(normalized_entries)},
        "legacy_v1_diagnostics": diagnostics,
    }
    if value.get("revision") != canonical_digest(body):
        raise DeploymentCacheRetentionStateError("activation history revision does not match its body")
    return {**body, "revision": str(value["revision"])}


def build_activation_history(
    entries: Mapping[str, Mapping[str, object]],
    legacy_v1_diagnostics: Sequence[Mapping[str, object]],
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "schema": ACTIVATION_HISTORY_SCHEMA,
        "entries": {str(key): dict(value) for key, value in sorted(entries.items())},
        "legacy_v1_diagnostics": [dict(value) for value in legacy_v1_diagnostics],
    }
    candidate = {**body, "revision": canonical_digest(body)}
    return parse_activation_history(candidate)


def parse_retention_recovery(value: Mapping[str, object]) -> dict[str, Any]:
    required = {
        "schema",
        "revision",
        "status",
        "restored_deployment_ids",
        "pending_deployment_ids",
        "transactions",
    }
    _require_allowed_keys(value, required, required | {"quarantined_paths"}, "recovery journal")
    schema = value.get("schema")
    if schema not in {RETENTION_RECOVERY_SCHEMA_V1, RETENTION_RECOVERY_SCHEMA}:
        raise DeploymentCacheRetentionStateError("retention recovery schema is invalid")
    status = _enum_text(value.get("status"), {"recovering", "blocked", "recovered"}, "recovery status")
    restored = _deployment_ids(value.get("restored_deployment_ids"), "restored deployment ids")
    pending = _deployment_ids(value.get("pending_deployment_ids"), "pending deployment ids")
    if schema == RETENTION_RECOVERY_SCHEMA_V1 and set(restored).intersection(pending):
        raise DeploymentCacheRetentionStateError("restored and pending deployment ids overlap")
    transactions_raw = _mapping(value.get("transactions"), "recovery transactions")
    if len(transactions_raw) > MAX_RECOVERY_TRANSACTIONS:
        raise DeploymentCacheRetentionStateError("recovery journal has too many transactions")
    transactions: dict[str, dict[str, Any]] = {}
    occurrence_bound = schema == RETENTION_RECOVERY_SCHEMA
    for transaction_key, raw_transaction in transactions_raw.items():
        try:
            canonical_key = require_digest(transaction_key, "transaction identity")
            transactions[canonical_key] = parse_retention_transaction(
                raw_transaction,
                key=canonical_key,
                occurrence_bound=occurrence_bound,
            )
        except RetentionTransactionContractError as exc:
            raise DeploymentCacheRetentionStateError(str(exc)) from exc
    quarantined = _bounded_unique_texts(
        value.get("quarantined_paths", []),
        label="quarantined paths",
        maximum=MAX_RECOVERY_TRANSACTIONS,
        item_maximum=MAX_CONTROL_PATH,
    )
    try:
        validate_recovery_transaction_semantics(
            status=status,
            restored_deployment_ids=restored,
            pending_deployment_ids=pending,
            transactions=transactions,
            quarantined_paths=quarantined,
        )
    except RetentionTransactionContractError as exc:
        raise DeploymentCacheRetentionStateError(str(exc)) from exc
    body: dict[str, Any] = {
        "schema": schema,
        "status": status,
        "restored_deployment_ids": list(restored),
        "pending_deployment_ids": list(pending),
        "transactions": {key: transactions[key] for key in sorted(transactions)},
    }
    if quarantined:
        body["quarantined_paths"] = list(quarantined)
    if value.get("revision") != canonical_digest(body):
        raise DeploymentCacheRetentionStateError("retention recovery revision does not match its body")
    return {**body, "revision": str(value["revision"])}


def build_retention_recovery(
    *,
    status: str,
    restored_deployment_ids: Sequence[str],
    pending_deployment_ids: Sequence[str],
    transactions: Mapping[str, Mapping[str, object]],
    quarantined_paths: Sequence[str] = (),
) -> dict[str, Any]:
    operation_bound_transactions: dict[str, dict[str, Any]] = {}
    for value in transactions.values():
        try:
            bound = bind_retention_transaction({**dict(value), "operation_id": value.get("operation_id")})
        except RetentionTransactionContractError as exc:
            raise DeploymentCacheRetentionStateError(str(exc)) from exc
        operation_bound_transactions[str(bound["transaction_id"])] = bound
    body: dict[str, Any] = {
        "schema": RETENTION_RECOVERY_SCHEMA,
        "status": status,
        "restored_deployment_ids": list(restored_deployment_ids),
        "pending_deployment_ids": list(pending_deployment_ids),
        "transactions": operation_bound_transactions,
    }
    if quarantined_paths:
        body["quarantined_paths"] = list(quarantined_paths)
    return parse_retention_recovery({**body, "revision": canonical_digest(body)})


def canonical_digest(value: Mapping[str, object]) -> str:
    return _canonical_digest(value)


def canonical_uuid4(value: object) -> str:
    try:
        parsed = UUID(str(value))
    except (ValueError, TypeError, AttributeError) as exc:
        raise DeploymentCacheRetentionStateError("value must be a canonical UUIDv4") from exc
    canonical = str(parsed)
    if parsed.version != 4 or canonical != value:
        raise DeploymentCacheRetentionStateError("value must be a canonical UUIDv4")
    return canonical


def require_digest(value: object, label: str) -> str:
    try:
        return _require_digest(value, label)
    except RetentionStateIdentityError as exc:
        raise DeploymentCacheRetentionStateError(str(exc)) from exc


def _parse_activation_entry(value: object, *, activation_id: str) -> dict[str, Any]:
    entry = _mapping(value, "activation history entry")
    keys = {
        "activation_id",
        "release_id",
        "deployment_id",
        "airflow_index_sha256",
        "loaded_dag_ids",
        "verified_at",
    }
    _require_exact_keys(entry, keys, "activation history entry")
    if canonical_uuid4(entry.get("activation_id")) != activation_id:
        raise DeploymentCacheRetentionStateError("activation history key and entry identity disagree")
    loaded = _bounded_unique_texts(
        entry.get("loaded_dag_ids"),
        label="loaded DAG ids",
        minimum=1,
        maximum=MAX_DAG_IDS,
        item_maximum=250,
    )
    verified_at = _timestamp(entry.get("verified_at"))
    return {
        "activation_id": activation_id,
        "release_id": require_digest(entry.get("release_id"), "release id"),
        "deployment_id": require_digest(entry.get("deployment_id"), "deployment id"),
        "airflow_index_sha256": require_digest(entry.get("airflow_index_sha256"), "airflow index sha256"),
        "loaded_dag_ids": list(loaded),
        "verified_at": verified_at,
    }


def _parse_legacy_diagnostics(value: object) -> list[dict[str, str]]:
    items = _sequence(value, "legacy diagnostics", maximum=MAX_ACTIVATION_HISTORY_ENTRIES)
    result: list[dict[str, str]] = []
    for raw in items:
        item = _mapping(raw, "legacy diagnostic")
        _require_exact_keys(item, {"entry_sha256"}, "legacy diagnostic")
        result.append({"entry_sha256": require_digest(item.get("entry_sha256"), "legacy entry sha256")})
    return result


def _deployment_ids(value: object, label: str) -> tuple[str, ...]:
    values = tuple(require_digest(item, label) for item in _sequence(value, label, maximum=MAX_RECOVERY_TRANSACTIONS))
    if len(values) != len(set(values)):
        raise DeploymentCacheRetentionStateError(f"{label} must be unique")
    return values


def _bounded_unique_texts(
    value: object,
    *,
    label: str,
    maximum: int,
    item_maximum: int,
    minimum: int = 0,
) -> tuple[str, ...]:
    items = _sequence(value, label, maximum=maximum)
    if len(items) < minimum:
        raise DeploymentCacheRetentionStateError(f"{label} has too few entries")
    result = tuple(_bounded_text(item, label, maximum=item_maximum) for item in items)
    if len(result) != len(set(result)):
        raise DeploymentCacheRetentionStateError(f"{label} must be unique")
    return result


def _timestamp(value: object) -> str:
    text = _bounded_text(value, "verified timestamp", maximum=128)
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DeploymentCacheRetentionStateError("verified timestamp must be ISO 8601") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise DeploymentCacheRetentionStateError("verified timestamp must include an offset")
    return text


def _mapping(value: object, label: str) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or any(not isinstance(key, str) for key in value):
        raise DeploymentCacheRetentionStateError(f"{label} must be an object with string keys")
    return value


def _sequence(value: object, label: str, *, maximum: int) -> Sequence[object]:
    if not isinstance(value, list) or len(value) > maximum:
        raise DeploymentCacheRetentionStateError(f"{label} must be a bounded array")
    return value


def _require_exact_keys(value: Mapping[str, object], keys: set[str], label: str) -> None:
    if set(value) != keys:
        raise DeploymentCacheRetentionStateError(f"{label} fields are invalid")


def _require_allowed_keys(
    value: Mapping[str, object],
    required: set[str],
    allowed: set[str],
    label: str,
) -> None:
    if not required <= set(value) <= allowed:
        raise DeploymentCacheRetentionStateError(f"{label} fields are invalid")


def _enum_text(value: object, allowed: set[str] | frozenset[str], label: str) -> str:
    text = _bounded_text(value, label, maximum=128)
    if text not in allowed:
        raise DeploymentCacheRetentionStateError(f"{label} is invalid")
    return text


def _bounded_text(value: object, label: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise DeploymentCacheRetentionStateError(f"{label} must be bounded non-empty text")
    return value


__all__ = [
    "ACTIVATION_HISTORY_SCHEMA",
    "DeploymentCacheRetentionStateError",
    "RetentionTransactionContractError",
    "MAX_CONTROL_PATH",
    "RETENTION_RECOVERY_SCHEMA",
    "RETENTION_RECOVERY_SCHEMA_V1",
    "RETENTION_TRANSACTION_PHASES",
    "build_activation_history",
    "build_retention_recovery",
    "bind_retention_transaction",
    "canonical_digest",
    "canonical_uuid4",
    "parse_activation_history",
    "parse_retention_recovery",
    "retention_operation_id",
    "require_digest",
]
