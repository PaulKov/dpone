"""Strict, externally provisioned KeeperMap authority for durable quality replay.

The platform must provision identical KeeperMap facades against the same Keeper
service. ``require_ready`` only inspects their engine, literal path, primary key
and columns; it never creates, replaces or migrates a table. Composition must use
this object for both bootstrap and authority, excluding the legacy bootstrap.

Writes bypass connector retry wrappers. An acknowledged write and exact canonical
payload/version readback are both required before a dispatch permit is returned.
A lost acknowledgement is unknown even if a later read could observe the write.
"""

from __future__ import annotations

import hashlib
import json
import re
import secrets
from collections.abc import Sequence
from dataclasses import replace
from typing import Any, NoReturn

from dpone.ports.clickhouse_cluster_publication import contracts

_COLUMNS = (
    ("target_key", "String"),
    ("operation_id", "String"),
    ("fence_token", "String"),
    ("phase", "String"),
    ("dispatch_epoch", "UInt64"),
    ("payload", "String"),
    ("payload_sha256", "FixedString(64)"),
)
_ENGINE = re.compile(r"KeeperMap\(\s*'(/[A-Za-z0-9_./-]+)'\s*(?:,\s*[0-9]+\s*)?\)")
_MUTATION_SETTINGS = {"keeper_map_strict_mode": 1, "insert_keeper_max_retries": 0}
_MAX_PAYLOAD_BYTES = 1024 * 1024


class ClickHouseQualityKeeperMapAuthority:
    """Provide strict CAS after read-only admission of operator-managed storage."""

    def __init__(self, connector: Any, database: str, *, table: str = contracts.AUTHORITY_TABLE) -> None:
        self._connector = connector
        self._database = database
        self._table = table
        self._ready = False

    def supports_linearizable_dispatch_permit(self) -> bool:
        """Only an admitted shared KeeperMap may issue a recovery permit."""
        return self._ready

    def require_ready(self, cluster: str, database: str, hosts: Sequence[str]) -> None:
        """Reject absent, legacy, inconsistent or unverified storage without DDL.

        Literal absolute Keeper paths exclude replica macros. The platform's
        common Keeper-service and writer-permission boundary remains a deployment
        requirement: SQL metadata alone cannot authenticate that boundary.
        """
        self._ready = False
        if database != self._database or not cluster or not hosts or len(set(hosts)) != len(hosts):
            _reject("UNSUPPORTED", "authority inventory or database is invalid")
        params = {"cluster": cluster, "database": database, "table": self._table}
        try:
            tables = self._connector.get_records(
                "SELECT hostName(), engine_full, primary_key "
                "FROM clusterAllReplicas(%(cluster)s, system.tables) "
                "WHERE database = %(database)s AND name = %(table)s ORDER BY hostName()",
                params,
            )
            columns = self._connector.get_records(
                "SELECT hostName(), name, type, position, default_kind, default_expression "
                "FROM clusterAllReplicas(%(cluster)s, system.columns) "
                "WHERE database = %(database)s AND table = %(table)s ORDER BY hostName(), position",
                params,
            )
            _validate_facades(tables, columns, hosts)
        except contracts.ClusterPublicationError:
            raise
        except Exception:
            _reject("UNSUPPORTED", "authority storage could not be verified")
        self._ready = True

    def ensure(self, cluster: str, database: str, hosts: Sequence[str]) -> None:
        """Satisfy the bootstrap port using only externally provisioned storage."""
        self.require_ready(cluster, database, hosts)

    def read_versioned(self, target_key: str) -> contracts.VersionedAuthorityRecord | None:
        """Read an exact canonical envelope; malformed or ambiguous rows fail closed."""
        self._require_ready()
        rows = self._connector.get_records(
            "SELECT operation_id, fence_token, phase, dispatch_epoch, payload, payload_sha256, _version "
            f"FROM {self._qualified} WHERE target_key = %(target_key)s",
            {"target_key": target_key},
        )
        if not rows:
            return None
        if len(rows) != 1:
            _reject("INVALID", "authority row is ambiguous")
        try:
            row = rows[0]
            if len(row) != 7 or type(row[6]) is not int or row[6] < 0:
                _reject("INVALID", "authority version is invalid")
            raw = _text(row[4])
            record = _decode_record(raw)
            if (
                contracts.canonical_json(json.loads(raw)) != raw
                or record.target_key != target_key
                or record.operation_id != _text(row[0])
                or record.fence_token != _text(row[1])
                or record.phase.value != _text(row[2])
                or type(row[3]) is not int
                or record.dispatch_epoch != row[3]
                or hashlib.sha256(raw.encode()).hexdigest() != _text(row[5])
            ):
                _reject("INVALID", "authority envelope identity or digest differs")
            return contracts.VersionedAuthorityRecord(record=record, version=row[6])
        except contracts.ClusterPublicationError:
            raise
        except (ValueError, TypeError, KeyError, IndexError, UnicodeError):
            _reject("INVALID", "authority envelope is malformed")

    def create_if_absent(self, record: contracts.AuthorityRecord) -> contracts.AuthorityMutationResult:
        """Strict insert once: existing keys cannot be silently replaced."""
        self._require_ready()
        sql = (
            f"INSERT INTO {self._qualified} "
            "(target_key, operation_id, fence_token, phase, dispatch_epoch, payload, payload_sha256) "
            "VALUES (%(target_key)s, %(operation_id)s, %(fence_token)s, %(phase)s, %(dispatch_epoch)s, "
            "%(payload)s, %(payload_sha256)s)"
        )
        return self._mutate_once(sql, _record_params(record), record, prior_version=None)

    def compare_and_swap(
        self, current: contracts.VersionedAuthorityRecord, desired: contracts.AuthorityRecord
    ) -> contracts.AuthorityMutationResult:
        """Update only the exact observed target, Keeper version and publication fence."""
        self._require_ready()
        before = current.record
        if desired.target_key != before.target_key or type(current.version) is not int or current.version < 0:
            _reject("INVALID", "authority CAS identity or version is invalid")
        sql = (
            f"ALTER TABLE {self._qualified} UPDATE operation_id = %(operation_id)s, "
            "fence_token = %(fence_token)s, phase = %(phase)s, dispatch_epoch = %(dispatch_epoch)s, "
            "payload = %(payload)s, payload_sha256 = %(payload_sha256)s "
            "WHERE target_key = %(target_key)s AND _version = %(version)s "
            "AND operation_id = %(expected_operation)s AND fence_token = %(expected_fence)s "
            "AND phase = %(expected_phase)s"
        )
        params = {
            **_record_params(desired),
            "version": current.version,
            "expected_operation": before.operation_id,
            "expected_fence": before.fence_token,
            "expected_phase": before.phase.value,
        }
        return self._mutate_once(sql, params, desired, prior_version=current.version)

    def _mutate_once(
        self, sql: str, params: dict[str, Any], desired: contracts.AuthorityRecord, *, prior_version: int | None
    ) -> contracts.AuthorityMutationResult:
        # Bind the acknowledged write to this particular caller. A filtered
        # zero-row UPDATE cannot impersonate another caller's identical desired
        # record, even when both started from the same authority version.
        desired = replace(desired, authority_write_id=secrets.token_hex(16))
        params = {**params, **_record_params(desired)}
        if _decode_record(desired.payload).payload != desired.payload:
            _reject("INVALID", "authority write envelope is invalid")
        try:
            self._connector.connection.execute(
                sql,
                params,
                settings=dict(_MUTATION_SETTINGS),
                query_id=f"dpone-quality-authority-{desired.operation_id[:24]}-{desired.dispatch_epoch}",
            )
        except Exception:
            return contracts.AuthorityMutationResult(contracts.AuthorityMutationStatus.OUTCOME_UNKNOWN)
        try:
            observed = self.read_versioned(desired.target_key)
        except Exception:
            return contracts.AuthorityMutationResult(contracts.AuthorityMutationStatus.OUTCOME_UNKNOWN)
        expected_version = 0 if prior_version is None else prior_version + 1
        if observed is None:
            return contracts.AuthorityMutationResult(contracts.AuthorityMutationStatus.OUTCOME_UNKNOWN)
        if observed.record.payload != desired.payload or observed.version != expected_version:
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

    def _require_ready(self) -> None:
        if not self._ready:
            _reject("UNSUPPORTED", "authority storage requires read-only admission")

    @property
    def _qualified(self) -> str:
        return f"{_quote(self._database)}.{_quote(self._table)}"


def _validate_facades(tables: Sequence[Any], columns: Sequence[Any], hosts: Sequence[str]) -> None:
    if sorted(row[0] for row in tables) != sorted(hosts):
        _reject("UNSUPPORTED", "authority facades do not cover the exact inventory")
    engines = {_text(row[1]) for row in tables}
    if len(engines) != 1 or not _ENGINE.fullmatch(next(iter(engines))):
        _reject("UNSUPPORTED", "authority requires a shared literal KeeperMap path")
    if any(_text(row[2]) not in {"target_key", "`target_key`"} for row in tables):
        _reject("UNSUPPORTED", "authority primary key differs")
    expected = sorted(
        (host, name, kind, position, "", "") for host in hosts for position, (name, kind) in enumerate(_COLUMNS, 1)
    )
    if sorted(tuple(row) for row in columns) != expected:
        _reject("UNSUPPORTED", "authority columns differ from the required schema")


def _decode_record(raw: str) -> contracts.AuthorityRecord:
    if len(raw.encode()) > _MAX_PAYLOAD_BYTES:
        _reject("INVALID", "authority envelope is oversized")
    payload = json.loads(raw)
    payload["phase"] = contracts.AuthorityPhase(payload["phase"])
    payload["desired"] = contracts.GenerationIdentity(**payload["desired"])
    if payload.get("predecessor") is not None:
        payload["predecessor"] = contracts.GenerationIdentity(**payload["predecessor"])
    record = contracts.AuthorityRecord(**payload)
    if record.schema_version not in {contracts.SCHEMA_VERSION, contracts.QUALITY_SCHEMA_VERSION}:
        _reject("INVALID", "authority schema version is unsupported")
    if (record.schema_version == contracts.QUALITY_SCHEMA_VERSION) != (record.quality_evidence is not None):
        _reject("INVALID", "authority schema version and quality evidence differ")
    return record


def _record_params(record: contracts.AuthorityRecord) -> dict[str, Any]:
    return {
        "target_key": record.target_key,
        "operation_id": record.operation_id,
        "fence_token": record.fence_token,
        "phase": record.phase.value,
        "dispatch_epoch": record.dispatch_epoch,
        "payload": record.payload,
        "payload_sha256": record.payload_sha256,
    }


def _quote(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _reject(reason: str, detail: str) -> NoReturn:
    raise contracts.ClusterPublicationError(f"DPONE_REPLAY_QUALITY_EVIDENCE_{reason}", detail)
