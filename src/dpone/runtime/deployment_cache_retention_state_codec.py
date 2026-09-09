"""Runtime boundary for canonical deployment-cache retention state documents."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.contracts.deployment_cache_retention_state import (
    ACTIVATION_HISTORY_SCHEMA,
    RETENTION_RECOVERY_SCHEMA,
    RETENTION_RECOVERY_SCHEMA_V1,
    DeploymentCacheRetentionStateError,
    RetentionTransactionContractError,
    bind_retention_transaction,
    build_activation_history,
    build_retention_recovery,
    canonical_digest,
    canonical_uuid4,
    parse_activation_history,
    parse_retention_recovery,
    require_digest,
    retention_operation_id,
)

RETENTION_RECOVERY_ACK_SCHEMA_V1 = "dpone.deployment-cache-retention-recovery-ack.v1"
RETENTION_RECOVERY_ACK_SCHEMA = "dpone.deployment-cache-retention-recovery-ack.v2"
MAX_ACKNOWLEDGED_DEPLOYMENTS = 10_000


def migrate_retention_recovery(value: Mapping[str, object]) -> dict[str, Any]:
    """Validate recovery state and upgrade v1 WAL entries without guessing ownership."""

    normalized = parse_retention_recovery(value)
    if normalized["schema"] == RETENTION_RECOVERY_SCHEMA:
        return normalized
    if normalized["schema"] != RETENTION_RECOVERY_SCHEMA_V1:
        raise DeploymentCacheRetentionStateError("retention recovery schema is invalid")
    transactions = {}
    for transaction in normalized["transactions"].values():
        try:
            bound = bind_retention_transaction({**dict(transaction), "operation_id": None})
        except RetentionTransactionContractError as exc:
            raise DeploymentCacheRetentionStateError("legacy retention transaction is invalid") from exc
        transactions[str(bound["transaction_id"])] = bound
    return build_retention_recovery(
        status=str(normalized["status"]),
        restored_deployment_ids=tuple(normalized["restored_deployment_ids"]),
        pending_deployment_ids=tuple(normalized["pending_deployment_ids"]),
        transactions=transactions,
        quarantined_paths=tuple(normalized.get("quarantined_paths", ())),
    )


class DeploymentCacheRetentionRecoveryAckError(ValueError):
    """One local recovery acknowledgement violates its closed contract."""


def build_retention_recovery_ack(
    *,
    environment: str,
    acknowledged_transaction_ids: Sequence[str],
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "schema": RETENTION_RECOVERY_ACK_SCHEMA,
        "environment": environment,
        "acknowledged_transaction_ids": sorted(acknowledged_transaction_ids),
    }
    return parse_retention_recovery_ack({**body, "revision": canonical_digest(body)})


def parse_retention_recovery_ack(value: Mapping[str, object]) -> dict[str, Any]:
    if value.get("schema") == RETENTION_RECOVERY_ACK_SCHEMA_V1:
        return _parse_retention_recovery_ack_v1(value)
    required = {
        "schema",
        "revision",
        "environment",
        "acknowledged_transaction_ids",
    }
    if set(value) != required:
        raise DeploymentCacheRetentionRecoveryAckError("recovery acknowledgement fields are invalid")
    if value.get("schema") != RETENTION_RECOVERY_ACK_SCHEMA:
        raise DeploymentCacheRetentionRecoveryAckError("recovery acknowledgement schema is invalid")
    environment = value.get("environment")
    if not isinstance(environment, str) or not environment or len(environment) > 128:
        raise DeploymentCacheRetentionRecoveryAckError("recovery acknowledgement environment is invalid")
    raw_ids = value.get("acknowledged_transaction_ids")
    if not isinstance(raw_ids, list) or len(raw_ids) > MAX_ACKNOWLEDGED_DEPLOYMENTS:
        raise DeploymentCacheRetentionRecoveryAckError("acknowledged transaction ids are invalid")
    acknowledged = tuple(_digest(item, "acknowledged transaction id") for item in raw_ids)
    if tuple(sorted(set(acknowledged))) != acknowledged:
        raise DeploymentCacheRetentionRecoveryAckError("acknowledged transaction ids must be sorted and unique")
    body: dict[str, Any] = {
        "schema": RETENTION_RECOVERY_ACK_SCHEMA,
        "environment": environment,
        "acknowledged_transaction_ids": list(acknowledged),
    }
    if value.get("revision") != canonical_digest(body):
        raise DeploymentCacheRetentionRecoveryAckError("recovery acknowledgement revision is invalid")
    return {**body, "revision": str(value["revision"])}


def _parse_retention_recovery_ack_v1(value: Mapping[str, object]) -> dict[str, Any]:
    fields = {"schema", "revision", "environment", "journal_revision", "restored_deployment_ids"}
    if set(value) != fields:
        raise DeploymentCacheRetentionRecoveryAckError("recovery acknowledgement fields are invalid")
    environment = value.get("environment")
    if not isinstance(environment, str) or not environment or len(environment) > 128:
        raise DeploymentCacheRetentionRecoveryAckError("recovery acknowledgement environment is invalid")
    journal_revision = _digest(value.get("journal_revision"), "journal revision")
    raw_ids = value.get("restored_deployment_ids")
    if not isinstance(raw_ids, list) or len(raw_ids) > MAX_ACKNOWLEDGED_DEPLOYMENTS:
        raise DeploymentCacheRetentionRecoveryAckError("restored deployment ids are invalid")
    restored = tuple(_digest(item, "restored deployment id") for item in raw_ids)
    if tuple(sorted(set(restored))) != restored:
        raise DeploymentCacheRetentionRecoveryAckError("restored deployment ids must be sorted and unique")
    body = {
        "schema": RETENTION_RECOVERY_ACK_SCHEMA_V1,
        "environment": environment,
        "journal_revision": journal_revision,
        "restored_deployment_ids": list(restored),
    }
    if value.get("revision") != canonical_digest(body):
        raise DeploymentCacheRetentionRecoveryAckError("recovery acknowledgement revision is invalid")
    return {**body, "revision": str(value["revision"])}


def _digest(value: object, label: str) -> str:
    try:
        return require_digest(value, label)
    except ValueError as exc:
        raise DeploymentCacheRetentionRecoveryAckError(str(exc)) from exc


__all__ = [
    "ACTIVATION_HISTORY_SCHEMA",
    "DeploymentCacheRetentionRecoveryAckError",
    "DeploymentCacheRetentionStateError",
    "RETENTION_RECOVERY_ACK_SCHEMA",
    "RETENTION_RECOVERY_ACK_SCHEMA_V1",
    "build_activation_history",
    "bind_retention_transaction",
    "build_retention_recovery_ack",
    "build_retention_recovery",
    "canonical_digest",
    "canonical_uuid4",
    "migrate_retention_recovery",
    "parse_activation_history",
    "parse_retention_recovery_ack",
    "parse_retention_recovery",
    "retention_operation_id",
    "require_digest",
]
