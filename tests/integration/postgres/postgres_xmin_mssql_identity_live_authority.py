"""Operator-authority helpers for the PostgreSQL XMin -> MSSQL live proof.

The module provisions only immutable, one-shot approval rows.  All repair
admission, target mutation, ownership transfer, checkpoint CAS, receipt and
authority consumption still execute through the public ``ETLProcessor``
route.  Helpers intentionally return sanitized facts suitable for committed
certification evidence.
"""

from __future__ import annotations

import uuid
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Any

from dpone.contracts.repair_authority import (
    ExpectedCheckpoint,
    RepairAllowance,
    RepairAuthority,
    TargetAuthorityTransfer,
    repair_authority_digest,
)
from dpone.ports.source_state_storage import SourceStateKey
from dpone.runtime.state.xmin_storage import XMinState

_UTC = timezone.utc  # noqa: UP017 - mypy uses the supported Python 3.10 stubs.


def clone_config(
    route: Any,
    *,
    process: str | None = None,
    repair_authority_ref: str | None = None,
) -> Any:
    """Return an isolated workload config with optional runtime-only bindings."""

    options = deepcopy(route.load_config.options)
    if process is not None:
        identity = dict(options["state_identity"])
        identity["process"] = process
        options["state_identity"] = identity
    return replace(
        route.load_config,
        options=options,
        repair_authority_ref=repair_authority_ref,
    )


def resolve_key(route: Any, config: Any) -> tuple[SourceStateKey, XMinState | None]:
    """Resolve the production state identity using the normal source preflight."""

    state = route.processor.source.get_incremental_state(config)
    strategy = route.processor.source._xmin_extract
    key = getattr(strategy, "_loaded_state_key", None)
    if not isinstance(key, SourceStateKey):
        raise AssertionError("standard XMin source did not freeze a SourceStateKey")
    return key, state


def checkpoint(route: Any, key: SourceStateKey) -> XMinState | None:
    """Read one active checkpoint through the production MSSQL state store."""

    return route.processor.source.state_storage.load_state_by_key(key)


def provision_authority(
    route: Any,
    *,
    key: SourceStateKey,
    expected: XMinState | None,
    full_baseline: bool,
    max_delete_rows: int | None = None,
    max_delete_ratio: float | None = None,
    transfer_from: TargetAuthorityTransfer | None = None,
    expires_delta: timedelta = timedelta(hours=1),
    authority_id: str | None = None,
    reason: str = "work-item disposable vendor-live repair certification",
) -> RepairAuthority:
    """Insert one exact immutable approval using the external-owner contract."""

    authority_id = authority_id or f"live-{uuid.uuid4().hex}"
    expected_checkpoint = ExpectedCheckpoint(
        absent=expected is None,
        xmin=expected.xmin_value if expected is not None else None,
        revision=expected.revision if expected is not None else None,
    )
    allowance = RepairAllowance(
        full_baseline=full_baseline,
        max_delete_rows=max_delete_rows,
        max_delete_ratio=max_delete_ratio,
    )
    expires_at = datetime.now(_UTC) + expires_delta
    digest = repair_authority_digest(
        authority_id=authority_id,
        state_key=key.digest,
        expected_checkpoint=expected_checkpoint,
        scope_hash=key.scope_hash,
        reason=reason,
        expires_at_utc=expires_at,
        allow=allowance,
        transfer_from=transfer_from,
    )
    authority = RepairAuthority(
        authority_id=authority_id,
        state_key=key.digest,
        expected_checkpoint=expected_checkpoint,
        scope_hash=key.scope_hash,
        reason=reason,
        expires_at_utc=expires_at,
        allow=allowance,
        authority_digest=digest,
        transfer_from=transfer_from,
    )
    route.state.execute_query(
        f"""
        INSERT INTO [{route.state_database}].[system].[dpone_repair_authority] (
            authority_id, authority_digest, state_key,
            transfer_from_state_key, transfer_from_xmin, transfer_from_revision,
            expected_checkpoint_absent, expected_xmin, expected_revision,
            scope_hash, reason, expires_at_utc, allow_full_baseline,
            allow_max_delete_rows, allow_max_delete_ratio
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            authority.authority_id,
            authority.authority_digest,
            authority.state_key,
            transfer_from.state_key if transfer_from is not None else None,
            transfer_from.xmin if transfer_from is not None else None,
            transfer_from.revision if transfer_from is not None else None,
            int(expected_checkpoint.absent),
            expected_checkpoint.xmin,
            expected_checkpoint.revision,
            authority.scope_hash,
            authority.reason,
            authority.expires_at_utc.replace(tzinfo=None),
            int(authority.allow.full_baseline),
            authority.allow.max_delete_rows,
            authority.allow.max_delete_ratio,
        ),
    )
    return authority


def authority_consumptions(route: Any, authority_id: str) -> list[dict[str, Any]]:
    """Return exact, non-secret one-shot consumption evidence."""

    return route.state.get_records(
        f"""
        SELECT authority_id, authority_digest, state_key, load_id, receipt_id,
               used_full_baseline, observed_delete_rows, observed_delete_ratio,
               consumed_at_utc
        FROM [{route.state_database}].[system].[dpone_repair_authority_consumption]
        WHERE authority_id = ?
        """,
        (authority_id,),
        as_dict=True,
    )


def ownership_ledger(route: Any) -> list[dict[str, Any]]:
    """Read all owners without collation-sensitive coordinate predicates."""

    return route.state.get_records(
        f"""
        SELECT state_key, target_identity, process_name, target_database,
               target_schema, target_table, xmin_value, state_revision,
               superseded_at_utc, superseded_by_state_key
        FROM [{route.state_database}].[system].[dpone_source_state]
        ORDER BY __dpone__loaded_at, state_key
        """,
        as_dict=True,
    )


def receipt_count(route: Any) -> int:
    """Return the durable commit-receipt cardinality."""

    rows = route.state.get_records(
        f"SELECT COUNT_BIG(*) AS n FROM [{route.state_database}].[system].[dpone_commit_receipt]",
        as_dict=True,
    )
    return int(rows[0]["n"])


def consumption_count(route: Any) -> int:
    """Return the durable one-shot-consumption cardinality."""

    rows = route.state.get_records(
        f"SELECT COUNT_BIG(*) AS n FROM [{route.state_database}].[system].[dpone_repair_authority_consumption]",
        as_dict=True,
    )
    return int(rows[0]["n"])


def force_checkpoint(
    route: Any,
    key: SourceStateKey,
    *,
    xmin: int,
    wraparound_detected: bool = False,
) -> XMinState:
    """Arrange an unsafe checkpoint before a real-vendor repair attempt."""

    route.state.execute_query(
        f"""
        UPDATE [{route.state_database}].[system].[dpone_source_state]
        SET xmin_value = ?, wraparound_detected = ?, frozen_xid = NULL,
            __dpone__updated_at = SYSUTCDATETIME()
        WHERE state_key = ? AND target_identity = ? AND superseded_at_utc IS NULL
        """,
        (xmin, int(wraparound_detected), key.digest, key.target_identity),
    )
    result = checkpoint(route, key)
    if result is None or result.xmin_value != xmin:
        raise AssertionError("unsafe checkpoint arrangement failed")
    return result


def target_before_image(route: Any) -> tuple[tuple[Any, ...], ...]:
    """Canonicalize target business/technical state for rollback assertions."""

    return tuple(tuple(row[name] for name in sorted(row)) for row in route.target_rows())


def hex_digest(value: bytes | bytearray | memoryview) -> str:
    """Render only public binary identities in evidence."""

    return bytes(value).hex()


__all__ = [
    "authority_consumptions",
    "checkpoint",
    "clone_config",
    "consumption_count",
    "force_checkpoint",
    "hex_digest",
    "ownership_ledger",
    "provision_authority",
    "receipt_count",
    "resolve_key",
    "target_before_image",
]
