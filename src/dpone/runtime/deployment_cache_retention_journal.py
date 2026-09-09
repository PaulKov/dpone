"""Durable WAL reducer for deployment-cache retention transactions."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.runtime.deployment_cache_common import (
    DeploymentCacheError,
    atomic_write_json,
    read_regular_json_object,
)
from dpone.runtime.deployment_cache_retention_contracts import (
    DeploymentCacheRetentionApplyError,
    RetentionTransactionOccurrence,
)
from dpone.runtime.deployment_cache_retention_state_codec import (
    DeploymentCacheRetentionRecoveryAckError,
    DeploymentCacheRetentionStateError,
    RetentionTransactionContractError,
    bind_retention_transaction,
    build_retention_recovery,
    build_retention_recovery_ack,
    migrate_retention_recovery,
    parse_retention_recovery,
    parse_retention_recovery_ack,
)

_JOURNAL_NAME = ".retention-recovery.json"
_ACK_NAME = ".retention-recovery-ack.json"
MAX_RETENTION_JOURNAL_BYTES = 128 * 1024 * 1024
MAX_RETENTION_RECOVERY_ACK_BYTES = 2 * 1024 * 1024


@dataclass(frozen=True)
class _RetentionRecoverySnapshot:
    normalized: dict[str, Any]
    source_schema: str
    source_revision: str
    source_restored_deployment_ids: tuple[str, ...]


class DeploymentCacheRetentionJournal:
    """Persist validated retention transitions before mutating live state."""

    def __init__(self, cache_root: Path) -> None:
        self._root = cache_root
        self._path = cache_root / _JOURNAL_NAME
        self._ack_path = cache_root / _ACK_NAME

    def read_transactions(self) -> dict[str, dict[str, object]]:
        normalized = self._read_state()
        if normalized is None:
            return {}
        return {str(key): dict(value) for key, value in normalized["transactions"].items()}

    def unacknowledged_restored_deployment_ids(self, *, environment: str) -> tuple[str, ...]:
        """Compatibility projection of unacknowledged restored occurrences."""

        return tuple(sorted({item.deployment_id for item in self.unacknowledged_restored(environment=environment)}))

    def unacknowledged_restored(self, *, environment: str) -> tuple[RetentionTransactionOccurrence, ...]:
        """Return exact restored occurrences until reconciliation is durable."""

        snapshot = self._read_state_snapshot()
        if snapshot is None:
            return ()
        normalized = snapshot.normalized
        restored = tuple(
            _occurrence(transaction)
            for transaction in normalized["transactions"].values()
            if transaction["phase"] == "restored" and transaction["environment"] == environment
        )
        if not restored:
            return ()
        acknowledgement = self._effective_ack(snapshot, environment=environment)
        acknowledged = (
            frozenset(acknowledgement["acknowledged_transaction_ids"])
            if acknowledgement is not None and "acknowledged_transaction_ids" in acknowledgement
            else frozenset()
        )
        return tuple(
            sorted((item for item in restored if item.transaction_id not in acknowledged), key=_occurrence_key)
        )

    def acknowledge_restored(self, *, environment: str, transaction_ids: tuple[str, ...]) -> None:
        """Acknowledge recovery only after the matching apply receipt is aborted."""

        snapshot = self._read_state_snapshot()
        if snapshot is None or snapshot.normalized["status"] != "recovered":
            raise self.error("retention recovery cannot be acknowledged before it is complete")
        normalized = snapshot.normalized
        restored_ids = {
            str(transaction["transaction_id"])
            for transaction in normalized["transactions"].values()
            if transaction["phase"] == "restored" and transaction["environment"] == environment
        }
        requested = frozenset(transaction_ids)
        if not requested or not requested <= restored_ids:
            raise self.error("retention recovery acknowledgement does not match the journal")
        acknowledgement = self._effective_ack(snapshot, environment=environment)
        acknowledged = set(
            acknowledgement.get("acknowledged_transaction_ids", ()) if acknowledgement is not None else ()
        )
        acknowledged.update(requested)
        payload = build_retention_recovery_ack(
            environment=environment,
            acknowledged_transaction_ids=tuple(sorted(acknowledged)),
        )
        try:
            atomic_write_json(self._ack_path, payload)
        except OSError as exc:
            raise self.error("retention recovery acknowledgement could not be committed durably") from exc

    def _read_state(self) -> dict[str, Any] | None:
        snapshot = self._read_state_snapshot()
        return snapshot.normalized if snapshot is not None else None

    def _read_state_snapshot(self) -> _RetentionRecoverySnapshot | None:
        if not self._path.exists():
            return None
        payload = read_regular_json_object(
            self._path,
            missing_code="DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_INVALID",
            invalid_code="DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_INVALID",
            label="deployment cache retention recovery journal",
            root=self._root,
            max_bytes=MAX_RETENTION_JOURNAL_BYTES,
        )
        try:
            source = parse_retention_recovery(payload)
            normalized = migrate_retention_recovery(payload)
        except DeploymentCacheRetentionStateError as exc:
            raise self.error("deployment cache retention recovery journal is invalid") from exc
        return _RetentionRecoverySnapshot(
            normalized=normalized,
            source_schema=str(source["schema"]),
            source_revision=str(source["revision"]),
            source_restored_deployment_ids=tuple(str(item) for item in source["restored_deployment_ids"]),
        )

    def _read_ack(self) -> dict[str, Any] | None:
        if not self._ack_path.exists():
            return None
        payload = read_regular_json_object(
            self._ack_path,
            missing_code="DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_INVALID",
            invalid_code="DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_INVALID",
            label="deployment cache retention recovery acknowledgement",
            root=self._root,
            max_bytes=MAX_RETENTION_RECOVERY_ACK_BYTES,
        )
        try:
            return parse_retention_recovery_ack(payload)
        except DeploymentCacheRetentionRecoveryAckError as exc:
            raise self.error("deployment cache retention recovery acknowledgement is invalid") from exc

    def advance(self, transaction: dict[str, object], phase: str) -> None:
        candidate = dict(transaction)
        candidate["phase"] = phase
        self.commit(candidate)
        transaction["phase"] = phase

    def commit(self, transaction: Mapping[str, object]) -> None:
        try:
            bound = (
                dict(transaction)
                if "transaction_id" in transaction
                else bind_retention_transaction({**dict(transaction), "operation_id": transaction.get("operation_id")})
            )
        except RetentionTransactionContractError as exc:
            raise self.error("retention transaction identity is invalid") from exc
        transaction_id = str(bound["transaction_id"])
        transactions = self.read_transactions()
        transactions[transaction_id] = bound
        self._write_transactions(transactions)

    def prune_committed(self, *, operation_id: str, deployment_ids: tuple[str, ...]) -> None:
        """Drop terminal success WAL only after its apply receipt is durable."""

        transactions = self.read_transactions()
        selected = set(deployment_ids)
        for transaction_id, transaction in tuple(transactions.items()):
            if transaction["deployment_id"] not in selected or transaction.get("operation_id") != operation_id:
                continue
            if transaction["phase"] != "committed":
                raise self.error("retention success WAL cannot be pruned before commit")
            transactions.pop(transaction_id)
        self._write_transactions(transactions)

    def _write_transactions(self, transactions: Mapping[str, Mapping[str, object]]) -> None:
        self._upgrade_legacy_ack_before_mutation()
        restored = sorted(
            {str(value["deployment_id"]) for value in transactions.values() if value["phase"] == "restored"}
        )
        pending = sorted(
            {
                str(value["deployment_id"])
                for value in transactions.values()
                if value["phase"] not in {"committed", "restored"}
            }
        )
        quarantined = sorted(
            str(value["detached_path"]) for value in transactions.values() if value["phase"] == "blocked"
        )
        status = "recovered" if not pending else ("blocked" if quarantined else "recovering")
        payload = build_retention_recovery(
            status=status,
            restored_deployment_ids=restored,
            pending_deployment_ids=pending,
            transactions=transactions,
            quarantined_paths=quarantined,
        )
        try:
            atomic_write_json(self._path, payload)
        except OSError as exc:
            raise DeploymentCacheRetentionApplyError(
                "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_JOURNAL_FAILED",
                "deployment cache retention transaction could not be committed durably",
                path=self._path.as_posix(),
                details={"state_may_have_changed": True},
            ) from exc

    def block(self, transaction: dict[str, object]) -> bool:
        self.advance(transaction, "blocked")
        return False

    def error(self, message: str) -> DeploymentCacheRetentionApplyError:
        details: dict[str, object] = {"state_may_have_changed": True}
        try:
            if self._path.exists():
                payload = read_regular_json_object(
                    self._path,
                    missing_code="DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_INVALID",
                    invalid_code="DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_INVALID",
                    label="deployment cache retention recovery journal",
                    root=self._root,
                    max_bytes=MAX_RETENTION_JOURNAL_BYTES,
                )
                normalized = migrate_retention_recovery(payload)
                quarantined = list(normalized.get("quarantined_paths", ()))
                details.update(
                    restored_deployment_ids=list(normalized["restored_deployment_ids"]),
                    pending_deployment_ids=list(normalized["pending_deployment_ids"]),
                    quarantined_paths=quarantined,
                    quarantined_path=next(iter(quarantined), None),
                )
        except (DeploymentCacheError, DeploymentCacheRetentionStateError, OSError):
            pass
        return DeploymentCacheRetentionApplyError(
            "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED",
            message,
            path=self._path.as_posix(),
            details=details,
        )

    def _effective_ack(
        self,
        snapshot: _RetentionRecoverySnapshot,
        *,
        environment: str,
    ) -> dict[str, Any] | None:
        acknowledgement = self._read_ack()
        if acknowledgement is None or acknowledgement["environment"] != environment:
            return None
        if "acknowledged_transaction_ids" in acknowledgement:
            return acknowledgement
        restored_ids = snapshot.source_restored_deployment_ids
        if (
            acknowledgement["journal_revision"] != snapshot.source_revision
            or tuple(acknowledgement["restored_deployment_ids"]) != restored_ids
        ):
            return None
        state = snapshot.normalized
        transaction_ids = tuple(
            sorted(
                str(transaction["transaction_id"])
                for transaction in state["transactions"].values()
                if transaction["phase"] == "restored"
                and transaction["environment"] == environment
                and transaction["deployment_id"] in restored_ids
            )
        )
        acknowledged_deployments = {
            str(transaction["deployment_id"])
            for transaction in state["transactions"].values()
            if transaction["phase"] == "restored"
            and transaction["environment"] == environment
            and transaction["transaction_id"] in transaction_ids
        }
        if acknowledged_deployments != set(restored_ids):
            return None
        upgraded = build_retention_recovery_ack(
            environment=environment,
            acknowledged_transaction_ids=transaction_ids,
        )
        try:
            atomic_write_json(self._ack_path, upgraded)
        except OSError as exc:
            raise self.error("legacy recovery acknowledgement could not be upgraded durably") from exc
        return upgraded

    def _upgrade_legacy_ack_before_mutation(self) -> None:
        snapshot = self._read_state_snapshot()
        if snapshot is None or not self._ack_path.exists():
            return
        acknowledgement = self._read_ack()
        if acknowledgement is None or "acknowledged_transaction_ids" in acknowledgement:
            return
        self._effective_ack(snapshot, environment=str(acknowledgement["environment"]))


__all__ = [
    "MAX_RETENTION_JOURNAL_BYTES",
    "MAX_RETENTION_RECOVERY_ACK_BYTES",
    "DeploymentCacheRetentionJournal",
]


def _occurrence(transaction: Mapping[str, object]) -> RetentionTransactionOccurrence:
    return RetentionTransactionOccurrence(
        transaction_id=str(transaction["transaction_id"]),
        deployment_id=str(transaction["deployment_id"]),
        operation_id=(str(transaction["operation_id"]) if transaction.get("operation_id") is not None else None),
        original_path=str(transaction["original_path"]),
    )


def _occurrence_key(value: RetentionTransactionOccurrence) -> tuple[str, str]:
    return value.deployment_id, value.transaction_id
