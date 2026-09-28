"""Strict authority-backed quality storage for managed cluster generations.

This adapter is explicitly composed with a KeeperMap authority. It does not
bootstrap or migrate tables. A durable read guard fences managed slot reuse;
unmanaged writes remain outside the managed-publication contract.
"""

from __future__ import annotations

import json
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import asdict, replace
from threading import Lock
from time import monotonic
from typing import Any

from dpone.ports.clickhouse_cluster_publication import contracts, require_verified_mutation
from dpone.runtime.governance.quality_execution import QualityExecutionSnapshot
from dpone.runtime.governance.quality_replay_identity import admission_digest
from dpone.runtime.governance.quality_target_plan import target_request
from dpone.runtime.quality_replay_contracts import (
    MAX_FRAME_BYTES,
    MAX_UINT64,
    QualityReplayStore,
    unavailable_observation,
)
from dpone.runtime.quality_replay_contracts import contracts as quality_contracts
from dpone.runtime.sinks.clickhouse_cluster_publication_identity import cluster_name, operation_id

QualityReplayCapsule = quality_contracts.QualityReplayCapsule
ReplayQualityEvidenceError = quality_contracts.ReplayQualityEvidenceError
canonical_quality_json = quality_contracts.canonical_quality_json


class ClickHouseReplayQualityStore(QualityReplayStore):
    """Bind producer evidence to one exact candidate and publication record."""

    def __init__(self, catalog: Any, authority_factory: Any, *, target_acceptance_reader: Any = None) -> None:
        self._target_reader = target_acceptance_reader
        self._retained: set[str] = set()
        self._catalog = catalog
        self._authority_factory = authority_factory
        self._pending: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = Lock()
        self._guards: dict[str, str] = {}

    @property
    def target_reader(self) -> Any:
        return self._target_reader

    def reader_token(self, load_config: Any) -> str:
        _, current = self._read(load_config)
        with self._lock:
            token = self._guards.get(current.record.target_key)
        if token is None or current.record.quality_reader != token:
            raise ReplayQualityEvidenceError("MISMATCH")
        return token

    def retain_guard(self, load_config: Any) -> None:
        # Do not attempt further authority I/O to decide whether to retain a
        # guard after an unknown outcome. The local owner is sufficient.
        with self._lock:
            key = _target_key(load_config)
            if key in self._guards:
                self._retained.add(key)

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
        if "target_plan" in core:
            seal_target_plan(core, self._catalog.inventory(cluster_name(load_config)).hosts)
        capsule = QualityReplayCapsule.prepare(core)
        if capsule.version == quality_contracts.TARGET_VERSION:
            reserve_target_completion(capsule, self._catalog.inventory(cluster_name(load_config)).hosts)
            request = target_request(capsule.core, capsule.core_digest, "f" * 32)
            self.target_reader.validate_plan(request)
            deadline = monotonic() + 60.0
            self.target_reader.verify_generation(replace(request, table=record.candidate), deadline=deadline)
            if monotonic() >= deadline:
                raise ReplayQualityEvidenceError("INCOMPLETE")
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
            if getattr(error, "blocks_committed_success", False):
                raise
            classified = ReplayQualityEvidenceError("INCOMPLETE")
            classified.replay_details = getattr(error, "replay_details", {})
            raise classified from error

    @contextmanager
    def _committed(self, load_config: Any) -> Iterator[QualityReplayCapsule]:
        authority, current = self._read(load_config)
        try:
            capsule = self._capsule(current)
        except ReplayQualityEvidenceError as error:
            error.replay_details = {"target_commit": "proven"}
            raise
        token = secrets.token_hex(16)
        try:
            if current.record.quality_reader is not None:
                raise ReplayQualityEvidenceError("INCOMPLETE")
            locked = require_verified_mutation(
                authority.compare_and_swap(current, replace(current.record, quality_reader=token)),
                permit=False,
            )
        except BaseException as error:
            setattr(error, "replay_details", {"target_commit": "proven"})
            raise
        key = current.record.target_key
        with self._lock:
            self._guards[key] = token
        try:
            if capsule.version == quality_contracts.VERSION:
                self._require_generation(load_config, locked.record)
            yield capsule
            with self._lock:
                if key in self._retained:
                    raise ReplayQualityEvidenceError("INCOMPLETE")
            _, after = self._read(load_config)
            if after.record.quality_reader != token:
                raise ReplayQualityEvidenceError("MISMATCH")
            if capsule.version == quality_contracts.VERSION:
                self._require_generation(load_config, after.record)
        finally:
            with self._lock:
                self._guards.pop(key, None)
                retained = key in self._retained
                self._retained.discard(key)
            if not retained:
                # Never retry an uncertain release or clear another owner.
                observed = authority.read_versioned(key)
                if observed is None or observed.record.quality_reader != token:
                    raise ReplayQualityEvidenceError("MISMATCH")
                require_verified_mutation(
                    authority.compare_and_swap(observed, replace(observed.record, quality_reader=None)),
                    permit=False,
                )

    def complete(self, load_config: Any, capsule: QualityReplayCapsule) -> QualityReplayCapsule:
        return self.transition(load_config, capsule, "COMPLETE")

    def transition(
        self, load_config: Any, capsule: QualityReplayCapsule, state: str, *, target: dict[str, Any] | None = None
    ) -> QualityReplayCapsule:
        authority, current = self._read(load_config)
        self.reader_token(load_config)
        actual = self._capsule(current)
        if actual != capsule:
            raise ReplayQualityEvidenceError("MISMATCH")
        advanced = capsule.advance(state, authority_version=current.version + 1, target=target)
        try:
            result = require_verified_mutation(
                authority.compare_and_swap(current, replace(current.record, quality_evidence=advanced.payload)),
                permit=False,
            )
            verified = self._capsule(result)
            if verified != advanced:
                raise ReplayQualityEvidenceError("MISMATCH")
            return verified
        except BaseException:
            self.retain_guard(load_config)
            raise

    def _read(self, config: Any) -> tuple[Any, contracts.VersionedAuthorityRecord]:
        key = _target_key(config)
        authority = self._authority_factory(str(config.target_schema))
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


def _target_key(config: Any) -> str:
    return contracts.digest_payload(
        {"cluster": cluster_name(config), "database": str(config.target_schema), "target": str(config.target_table)}
    )


def seal_target_plan(core: dict[str, Any], hosts: tuple[str, ...]) -> None:
    """Bind the original admitted replicas before the immutable core is sealed."""
    core["target_plan"]["replicas"] = list(sorted(hosts))


def reserve_target_completion(capsule: quality_contracts.QualityReplayCapsule, hosts: tuple[str, ...]) -> None:
    """Fit both transitions, longest replica and all worst-case UInt64 counts.

    The fixed framing allowance includes nonce, envelope keys, transport status,
    and bound lifecycle metadata. Every variable request/observation string is
    included in full; no truncation or selector reduction is allowed.
    """
    request = target_request(capsule.core, capsule.core_digest, "f" * 32)
    replica = max(hosts, key=lambda host: len(json.dumps(host).encode("utf-8")))
    warning = unavailable_observation(request, replica=replica, attempt_id="f" * 32)
    successful = {
        **warning,
        "warnings": [],
        "row_count": MAX_UINT64 if request.row_count else None,
        "null_counts": dict.fromkeys(request.null_columns, MAX_UINT64),
        "distinct_counts": dict.fromkeys(request.distinct_columns, MAX_UINT64),
    }
    variants = (warning, successful) if capsule.core["target_plan"]["mode"] == "warn_only" else (successful,)
    for observation in variants:
        pending = capsule.advance("TARGET_PENDING", authority_version=MAX_UINT64 - 1)
        pending.advance("COMPLETE", authority_version=MAX_UINT64, target=observation)
        encoded = quality_contracts.canonical_quality_json(observation).encode("utf-8")
        if len(encoded) + 4096 > MAX_FRAME_BYTES:
            raise quality_contracts.ReplayQualityEvidenceError("INVALID")
    # The worker receives its own framed request as well as returning evidence.
    request_payload = replace(request, binding=dict(request.binding))
    if len(quality_contracts.canonical_quality_json(asdict(request_payload)).encode("utf-8")) + 4096 > MAX_FRAME_BYTES:
        raise quality_contracts.ReplayQualityEvidenceError("INVALID")
