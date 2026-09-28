"""Strict authority-backed quality storage for managed cluster generations.

This adapter is explicitly composed with a KeeperMap authority. It does not
bootstrap or migrate tables. A durable read guard fences managed slot reuse;
unmanaged writes remain outside the managed-publication contract.
"""

from __future__ import annotations

import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, replace
from threading import Lock
from typing import Any

from dpone.ports.clickhouse_cluster_publication import contracts, require_verified_mutation
from dpone.runtime.governance.quality_execution import QualityExecutionSnapshot
from dpone.runtime.governance.quality_replay_identity import admission_digest
from dpone.runtime.quality_replay_contracts import QualityReplayStore
from dpone.runtime.quality_replay_contracts import contracts as quality_contracts
from dpone.runtime.sinks.clickhouse_cluster_publication_identity import cluster_name, operation_id

QualityReplayCapsule = quality_contracts.QualityReplayCapsule
ReplayQualityEvidenceError = quality_contracts.ReplayQualityEvidenceError
canonical_quality_json = quality_contracts.canonical_quality_json


class ClickHouseReplayQualityStore(QualityReplayStore):
    """Bind producer evidence to one exact candidate and publication record."""

    def __init__(self, catalog: Any, authority_factory: Any) -> None:
        self._catalog = catalog
        self._authority_factory = authority_factory
        self._pending: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = Lock()
        self._guards: dict[str, str] = {}

    def require_ready(self, load_config: Any) -> None:
        cluster = cluster_name(load_config)
        inventory = self._catalog.inventory(cluster)
        authority = self._authority_factory(str(load_config.target_schema))
        authority.require_ready(cluster, str(load_config.target_schema), inventory.hosts)

    def stage(self, load_config: Any, handle: Any, core: dict[str, Any]) -> None:
        candidate = handle.finalization_config or handle.staging_config
        key = (str(candidate.target_schema), str(candidate.target_table))
        if key[0] != str(load_config.target_schema):
            raise ReplayQualityEvidenceError("MISMATCH")
        # Round-trip now: reject oversized/unserializable producer output before
        # the finalizer can reach any irreversible boundary.
        prepared = QualityReplayCapsule.prepare(core)
        with self._lock:
            if key in self._pending:
                raise ReplayQualityEvidenceError("INVALID")
            self._pending[key] = prepared.core

    def seal(self, record: contracts.AuthorityRecord, load_config: Any) -> contracts.AuthorityRecord:
        """Called by the publication owner before acquiring a dispatch permit."""
        if QualityExecutionSnapshot.from_load_config(load_config).is_inert():
            return record
        key = (record.database, record.candidate)
        with self._lock:
            core = self._pending.pop(key, None)
        if core is None:
            raise ReplayQualityEvidenceError("REQUIRED")
        if core["policy_snapshot_id"] != QualityExecutionSnapshot.from_load_config(
            load_config
        ).policy_snapshot_id or core["effective_plan"].get("effective_config_digest") != admission_digest(load_config):
            raise ReplayQualityEvidenceError("MISMATCH")
        core["binding"] = _binding(record)
        capsule = QualityReplayCapsule.prepare(core)
        return replace(record, quality_evidence=capsule.payload, schema_version=contracts.QUALITY_SCHEMA_VERSION)

    @contextmanager
    def committed(self, load_config: Any) -> Iterator[QualityReplayCapsule]:
        """Keep every store/guard failure blocking after the publication commit."""
        try:
            with self._committed(load_config) as capsule:
                yield capsule
        except ReplayQualityEvidenceError:
            raise
        except Exception as error:
            raise ReplayQualityEvidenceError("INCOMPLETE") from error

    @contextmanager
    def _committed(self, load_config: Any) -> Iterator[QualityReplayCapsule]:
        authority, current = self._read(load_config)
        capsule = self._capsule(current)
        if current.record.quality_reader is not None:
            raise ReplayQualityEvidenceError("INCOMPLETE")
        token = secrets.token_hex(16)
        locked = require_verified_mutation(
            authority.compare_and_swap(current, replace(current.record, quality_reader=token)),
            permit=False,
        )
        key = current.record.target_key
        with self._lock:
            self._guards[key] = token
        try:
            self._require_generation(load_config, locked.record)
            yield capsule
            _, after = self._read(load_config)
            if after.record.quality_reader != token:
                raise ReplayQualityEvidenceError("MISMATCH")
            self._require_generation(load_config, after.record)
        finally:
            with self._lock:
                self._guards.pop(key, None)
            # An unknown release must remain blocking; it must never silently
            # relabel the operation as successful or clear somebody else's guard.
            observed = authority.read_versioned(key)
            if observed is None or observed.record.quality_reader != token:
                raise ReplayQualityEvidenceError("MISMATCH")
            require_verified_mutation(
                authority.compare_and_swap(observed, replace(observed.record, quality_reader=None)),
                permit=False,
            )

    def complete(self, load_config: Any, capsule: QualityReplayCapsule) -> QualityReplayCapsule:
        authority, current = self._read(load_config)
        with self._lock:
            token = self._guards.get(current.record.target_key)
        if token is None or current.record.quality_reader != token:
            raise ReplayQualityEvidenceError("INVALID")
        actual = self._capsule(current)
        if actual != capsule:
            raise ReplayQualityEvidenceError("MISMATCH")
        complete = capsule.advance("COMPLETE", authority_version=current.version + 1)
        result = require_verified_mutation(
            authority.compare_and_swap(current, replace(current.record, quality_evidence=complete.payload)),
            permit=False,
        )
        return self._capsule(result)

    def _read(self, config: Any) -> tuple[Any, contracts.VersionedAuthorityRecord]:
        cluster = cluster_name(config)
        database, target = str(config.target_schema), str(config.target_table)
        key = contracts.digest_payload({"cluster": cluster, "database": database, "target": target})
        authority = self._authority_factory(database)
        current = authority.read_versioned(key)
        if current is None:
            raise ReplayQualityEvidenceError("REQUIRED")
        if current.record.operation_id != operation_id(config):
            raise ReplayQualityEvidenceError("MISMATCH")
        if current.record.phase not in {
            contracts.AuthorityPhase.COMMITTED,
            contracts.AuthorityPhase.CLEANUP_DISPATCHING,
            contracts.AuthorityPhase.COMPLETED,
        }:
            raise ReplayQualityEvidenceError("INCOMPLETE")
        return authority, current

    @staticmethod
    def _capsule(current: contracts.VersionedAuthorityRecord) -> QualityReplayCapsule:
        record = current.record
        if record.quality_evidence is None:
            raise ReplayQualityEvidenceError("REQUIRED")
        capsule = QualityReplayCapsule.parse(record.quality_evidence)
        version = capsule.completion_authority_version
        if version is not None and version > current.version:
            raise ReplayQualityEvidenceError("MISMATCH")
        if capsule.core["binding"] != _binding(record):
            raise ReplayQualityEvidenceError("MISMATCH")
        return capsule

    def _require_generation(self, config: Any, record: contracts.AuthorityRecord) -> None:
        cluster = cluster_name(config)
        inventory = self._catalog.inventory(cluster)
        contracts.require_inventory(record, inventory)
        facts = self._catalog.generations(
            cluster,
            record.database,
            record.target,
            record.candidate,
            inventory.hosts,
        )
        if tuple(sorted(fact.host for fact in facts)) != inventory.hosts or any(
            fact.target != record.desired or not fact.target_healthy for fact in facts
        ):
            raise ReplayQualityEvidenceError("MISMATCH")


def _binding(record: contracts.AuthorityRecord) -> dict[str, Any]:
    value = {
        "target_key": record.target_key,
        "operation_id": record.operation_id,
        "fence_token": record.fence_token,
        "inventory_digest": record.inventory_digest,
        "plan_digest": record.plan_digest,
        "desired": asdict(record.desired),
        "staged_rows": record.staged_rows,
    }
    canonical_quality_json(value)
    return value
