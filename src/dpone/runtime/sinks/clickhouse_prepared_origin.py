"""Store-produced preparation provenance for reused strict authority slots.

The digest is not a signature. Its trust comes from exclusive admitted writers
and strict, payload-bound CAS. It must never authenticate a legacy import.
"""

from __future__ import annotations

import json
from dataclasses import replace
from typing import NoReturn

from dpone.ports.clickhouse_cluster_publication import contracts as c
from dpone.runtime.quality_replay_contracts import contracts as quality_contracts

_KIND = "strict-prepared-v1"
_MAX_COUNTER = (1 << 63) - 1
_TRANSITIONS = {
    (c.AuthorityPhase.PREPARED, c.AuthorityPhase.DISPATCHING): 1,
    (c.AuthorityPhase.DISPATCHING, c.AuthorityPhase.COMMITTED): 0,
    (c.AuthorityPhase.COMMITTED, c.AuthorityPhase.CLEANUP_DISPATCHING): 1,
    (c.AuthorityPhase.COMMITTED, c.AuthorityPhase.COMPLETED): 0,
    (c.AuthorityPhase.CLEANUP_DISPATCHING, c.AuthorityPhase.COMPLETED): 0,
}
_GOVERNANCE_PHASES = {
    c.AuthorityPhase.COMMITTED,
    c.AuthorityPhase.CLEANUP_DISPATCHING,
    c.AuthorityPhase.COMPLETED,
}


def prepare_strict_write(
    current: c.VersionedAuthorityRecord | None,
    desired: c.AuthorityRecord,
) -> c.AuthorityRecord:
    """Stamp only acquisition; preserve original provenance on later writes."""
    if current is None:
        return _stamp(desired, version=0, epoch=0)
    before = current.record
    if not _counter(current.version) or not _counter(before.dispatch_epoch):
        _invalid("authority counter is invalid or exhausted")
    if before.target_key != desired.target_key:
        _invalid("authority target changed")
    if before.operation_id != desired.operation_id:
        if before.phase is not c.AuthorityPhase.COMPLETED or before.fence_token == desired.fence_token:
            _invalid("only a completed slot may admit a new fenced operation")
        quality_contracts.require_quality_retired(
            before.quality_evidence, before.quality_reader, authority_version=current.version
        )
        return _stamp(desired, version=current.version + 1, epoch=before.dispatch_epoch + 1)
    if desired.prepared_origin != before.prepared_origin:
        _invalid("original preparation provenance changed")
    for field in (
        "fence_token",
        "inventory_digest",
        "plan_digest",
        "database",
        "target",
        "candidate",
        "desired",
        "predecessor",
        "staged_rows",
        "schema_version",
    ):
        if getattr(desired, field) != getattr(before, field):
            _invalid("immutable publication identity changed")
    step = _TRANSITIONS.get((before.phase, desired.phase))
    if before.phase == desired.phase and before.phase in _GOVERNANCE_PHASES:
        step = 0
    if step is None or desired.dispatch_epoch != before.dispatch_epoch + step:
        _invalid("publication phase or epoch transition is invalid")
    return desired


def require_prepared_origin(current: c.VersionedAuthorityRecord) -> None:
    """Require the exact unadvanced PREPARED acquisition of this operation."""
    record = current.record
    if not record.authority_write_id:
        _unsafe()
    if record.prepared_origin is None:
        # Compatibility for original strict inserts, never for imported legacy
        # records or reused slots without operation-scoped provenance.
        if current.version != 0 or record.dispatch_epoch != 0:
            _unsafe()
        return
    try:
        origin = json.loads(record.prepared_origin)
        if (
            not isinstance(origin, dict)
            or set(origin) != {"kind", "prepared_version", "prepared_epoch", "prepared_payload_sha256"}
            or origin["kind"] != _KIND
            or not _counter(origin["prepared_version"])
            or not _counter(origin["prepared_epoch"])
            or origin["prepared_version"] != current.version
            or origin["prepared_epoch"] != record.dispatch_epoch
            or origin["prepared_payload_sha256"] != _unsigned(record).payload_sha256
            or c.canonical_json(origin) != record.prepared_origin
        ):
            _unsafe()
    except (ValueError, TypeError):
        _unsafe()


def _stamp(record: c.AuthorityRecord, *, version: int, epoch: int) -> c.AuthorityRecord:
    if (
        record.phase is not c.AuthorityPhase.PREPARED
        or record.prepared_origin is not None
        or record.authority_write_id is not None
        or record.quality_reader is not None
        or not _counter(version)
        or not _counter(epoch)
        or type(record.dispatch_epoch) is not int
        or record.dispatch_epoch != epoch
        or any(
            (
                record.ddl_entry,
                record.ddl_correlation_token,
                record.ddl_query_digest,
                record.cleanup_entry,
                record.cleanup_correlation_token,
                record.cleanup_query_digest,
            )
        )
    ):
        _invalid("new preparation must be pristine and store-stamped")
    origin = c.canonical_json(
        {
            "kind": _KIND,
            "prepared_version": version,
            "prepared_epoch": epoch,
            "prepared_payload_sha256": _unsigned(record).payload_sha256,
        }
    )
    return replace(record, prepared_origin=origin)


def _unsigned(record: c.AuthorityRecord) -> c.AuthorityRecord:
    return replace(record, prepared_origin=None, authority_write_id=None)


def _counter(value: object) -> bool:
    return type(value) is int and 0 <= value < _MAX_COUNTER


def _invalid(detail: str) -> NoReturn:
    raise c.ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_INVALID", detail)


def _unsafe() -> NoReturn:
    raise c.ClusterPublicationError(
        "DPONE_CLICKHOUSE_CLUSTER_AUTHORITY_UNSAFE",
        "exact strict preparation origin is not proven",
    )
