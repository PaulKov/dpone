"""Replicated authority adapter with a one-shot mutation boundary."""

from __future__ import annotations

import json
from typing import Any

from dpone.ports.clickhouse_cluster_publication import contracts

AUTHORITY_TABLE = contracts.AUTHORITY_TABLE
_INSERT_COLUMNS = "target_key, operation_id, fence_token, phase, dispatch_epoch, payload, payload_sha256, version"


class ClickHouseKeeperMapAuthority:
    """Write one authority version per call.

    The table is ReplicatedReplacingMergeTree. KeeperMap would be a tighter
    compare-and-swap, but it stays disabled until the server defines
    keeper_map_path_prefix, and that setting is not a query parameter.
    """

    def __init__(self, connector: Any, database: str, *, table: str = AUTHORITY_TABLE) -> None:
        self._connector = connector
        self._database = database
        self._table = table

    def read_versioned(self, target_key: str) -> contracts.VersionedAuthorityRecord | None:
        rows = self._connector.get_records(
            f"SELECT operation_id, fence_token, phase, dispatch_epoch, payload, payload_sha256, version "
            f"FROM {self._qualified} FINAL WHERE target_key = %(target_key)s",
            {"target_key": target_key},
        )
        if not rows:
            return None
        if len(rows) != 1:
            return None
        row = rows[0]
        record = _decode_record(_text(row[4]))
        if (
            record.target_key != target_key
            or record.operation_id != str(row[0])
            or record.fence_token != str(row[1])
            or record.phase.value != str(row[2])
            or record.dispatch_epoch != int(row[3])
            or record.payload_sha256 != _text(row[5])
        ):
            return None
        return contracts.VersionedAuthorityRecord(record=record, version=int(row[6]))

    def create_if_absent(self, record: contracts.AuthorityRecord) -> contracts.AuthorityMutationResult:
        sql = (
            f"INSERT INTO {self._qualified} ({_INSERT_COLUMNS}) "
            "SELECT %(target_key)s, %(operation_id)s, %(fence_token)s, %(phase)s, %(dispatch_epoch)s, "
            "%(payload)s, %(payload_sha256)s, 1 "
            f"WHERE (SELECT count() FROM {self._qualified} FINAL WHERE target_key = %(target_key)s) = 0"
        )
        values = {
            "target_key": record.target_key,
            "operation_id": record.operation_id,
            "fence_token": record.fence_token,
            "phase": record.phase.value,
            "dispatch_epoch": record.dispatch_epoch,
            "payload": record.payload,
            "payload_sha256": record.payload_sha256,
        }
        return self._mutate_once(sql, values, record, prior_version=None)

    def compare_and_swap(
        self, current: contracts.VersionedAuthorityRecord, desired: contracts.AuthorityRecord
    ) -> contracts.AuthorityMutationResult:
        before = current.record
        sql = (
            f"INSERT INTO {self._qualified} ({_INSERT_COLUMNS}) "
            "SELECT %(target_key)s, %(operation_id)s, %(fence_token)s, %(new_phase)s, %(new_epoch)s, "
            "%(payload)s, %(payload_sha256)s, %(version)s + 1 "
            f"WHERE (SELECT (version, operation_id, fence_token, phase) FROM {self._qualified} FINAL "
            "WHERE target_key = %(target_key)s) = "
            "(%(version)s, %(expected_operation)s, %(expected_fence)s, %(expected_phase)s)"
        )
        params = {
            "operation_id": desired.operation_id,
            "fence_token": desired.fence_token,
            "new_phase": desired.phase.value,
            "new_epoch": desired.dispatch_epoch,
            "payload": desired.payload,
            "payload_sha256": desired.payload_sha256,
            "target_key": before.target_key,
            "version": current.version,
            "expected_operation": before.operation_id,
            "expected_fence": before.fence_token,
            "expected_phase": before.phase.value,
        }
        return self._mutate_once(sql, params, desired, prior_version=current.version)

    def _mutate_once(
        self, sql: str, params: Any, desired: contracts.AuthorityRecord, *, prior_version: int | None
    ) -> contracts.AuthorityMutationResult:
        query_id = f"dpone-authority-{desired.operation_id[:24]}-{desired.dispatch_epoch}"
        try:
            self._connector.connection.execute(sql, params, query_id=query_id)
        except Exception:
            return contracts.AuthorityMutationResult(contracts.AuthorityMutationStatus.OUTCOME_UNKNOWN)
        observed = self.read_versioned(desired.target_key)
        expected_version = 1 if prior_version is None else prior_version + 1
        if observed is None:
            return contracts.AuthorityMutationResult(contracts.AuthorityMutationStatus.OUTCOME_UNKNOWN)
        if observed.record != desired or observed.version != expected_version:
            return contracts.AuthorityMutationResult(contracts.AuthorityMutationStatus.CONFLICT, observed=observed)
        permit = None
        if desired.phase in {contracts.AuthorityPhase.DISPATCHING, contracts.AuthorityPhase.CLEANUP_DISPATCHING}:
            permit = contracts.DispatchPermit(
                target_key=desired.target_key,
                operation_id=desired.operation_id,
                fence_token=desired.fence_token,
                dispatch_epoch=desired.dispatch_epoch,
            )
        return contracts.AuthorityMutationResult(
            contracts.AuthorityMutationStatus.VERIFIED, observed=observed, permit=permit
        )

    @property
    def _qualified(self) -> str:
        return f"{_quote(self._database)}.{_quote(self._table)}"


def _decode_record(raw: str) -> contracts.AuthorityRecord:
    payload = json.loads(raw)
    payload["phase"] = contracts.AuthorityPhase(payload["phase"])
    payload["desired"] = contracts.GenerationIdentity(**payload["desired"])
    if payload.get("predecessor") is not None:
        payload["predecessor"] = contracts.GenerationIdentity(**payload["predecessor"])
    return contracts.AuthorityRecord(**payload)


def _quote(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
