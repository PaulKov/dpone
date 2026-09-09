"""Fail-closed lifecycle policy for immutable incremental snapshots.

An :class:`IncrementalSnapshotEnvelope` already contains the source-owned
delta and complete-key schemas captured under one PostgreSQL MVCC snapshot.
The generic payload lifecycle is intentionally mutable: it may project
columns, evolve a target, or apply physical DDL.  Those operations are not a
valid preparation step for an envelope because they can detach the staged
data from its integrity receipts or publish source-only columns such as
``__dpone__xmin``.

This small policy is the single boundary that distinguishes both lifecycles.
It validates the immutable evidence and returns a normal runtime lifecycle
context, allowing the rest of the payload loader to remain shared.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.runtime.etl.lifecycle import RuntimeLifecycleContext
from dpone.runtime.incremental_snapshot import (
    IncrementalSnapshotEnvelope,
    KeySnapshotReconciliationPolicy,
)
from dpone.runtime.sinks.load_payload import LoadPayload


class SnapshotEnvelopeLifecycleError(RuntimeError):
    """Raised before target mutation when immutable envelope evidence drifts."""


class SnapshotEnvelopeLifecyclePolicy:
    """Select and validate the immutable snapshot-envelope lifecycle."""

    def prepare(
        self,
        *,
        load_config: Any,
        payload: LoadPayload,
    ) -> RuntimeLifecycleContext | None:
        """Return an immutable context, or ``None`` for a regular payload."""

        envelope = payload.artifact
        if not isinstance(envelope, IncrementalSnapshotEnvelope):
            return None

        options = _mapping(getattr(load_config, "options", None))
        reconciliation = KeySnapshotReconciliationPolicy.from_runtime(
            options,
            legacy_enabled=bool(getattr(load_config, "reconciliation", False)),
        )
        if not reconciliation.key_snapshot_enabled:
            raise SnapshotEnvelopeLifecycleError("incremental_snapshot_requires_key_snapshot_reconciliation")
        if str(options.get("source_custom_predicate") or "").strip():
            raise SnapshotEnvelopeLifecycleError("incremental_snapshot_scoped_predicate_is_unsupported")

        physical_design = _mapping(options.get("physical_design"))
        if physical_design.get("apply_runtime") is not False:
            raise SnapshotEnvelopeLifecycleError("incremental_snapshot_requires_physical_design_apply_runtime_false")

        delta_schema = _normalized_schema(envelope.delta_schema, label="delta")
        payload_schema = _normalized_schema(payload.schema, label="payload")
        if payload_schema != delta_schema:
            raise SnapshotEnvelopeLifecycleError("incremental_snapshot_payload_schema_does_not_match_envelope")

        delta_receipt = envelope.delta_receipt
        key_receipt = envelope.key_receipt
        if not delta_receipt.complete:
            raise SnapshotEnvelopeLifecycleError("incremental_snapshot_delta_is_incomplete")
        if not key_receipt.complete:
            raise SnapshotEnvelopeLifecycleError("incremental_snapshot_keys_are_incomplete")
        if tuple(delta_receipt.columns) != tuple(name for name, _dtype in delta_schema):
            raise SnapshotEnvelopeLifecycleError("incremental_snapshot_delta_columns_do_not_match_schema")
        key_names = tuple(name for name, _dtype in envelope.key_schema)
        if tuple(key_receipt.key_columns) != tuple(name for name in key_names if not name.startswith("__dpone__")):
            raise SnapshotEnvelopeLifecycleError("incremental_snapshot_key_columns_do_not_match_schema")

        immutable_payload = payload.rebind(
            artifact=envelope,
            schema=delta_schema,
        )
        return RuntimeLifecycleContext(payload=immutable_payload)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _normalized_schema(
    schema: Sequence[tuple[str, str]],
    *,
    label: str,
) -> tuple[tuple[str, str], ...]:
    normalized = tuple((str(name), str(dtype)) for name, dtype in schema)
    names = tuple(name for name, _dtype in normalized)
    if not normalized or any(not name or not dtype for name, dtype in normalized):
        raise SnapshotEnvelopeLifecycleError(f"incremental_snapshot_{label}_schema_is_invalid")
    if len(set(names)) != len(names):
        raise SnapshotEnvelopeLifecycleError(f"incremental_snapshot_{label}_schema_has_duplicates")
    return normalized


__all__ = [
    "SnapshotEnvelopeLifecycleError",
    "SnapshotEnvelopeLifecyclePolicy",
]
