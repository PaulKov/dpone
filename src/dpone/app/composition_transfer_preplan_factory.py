"""Bind trusted transfer preplanning to verified parent and signed connections."""

from __future__ import annotations

from functools import partial
from typing import Any

from dpone.app.composition_transfer_observation_factory import _ReadSession, require_transfer_target_identity
from dpone.config.state import resolve_mssql_state_location
from dpone.runtime.composition_transfer_preplan import (
    CompositionTransferPreplanService,
    verify_retained_transfer_commit,
)
from dpone.runtime.composition_transfer_preplan_store import CompositionTransferPreplanStore


def build_transfer_preplan_factory(
    *,
    control: Any,
    verified_manifest: Any,
    source_target: Any,
    sink_target: Any,
    state_target: Any,
    state_config: Any,
    parent_context: Any,
    payload_root: Any,
    read_plan: Any,
) -> Any:
    """Return an attempt-specific verifier factory; construction creates no journal."""
    location = resolve_mssql_state_location(state_config, state_target)

    def build(attempt: Any, write: Any) -> CompositionTransferPreplanService:
        plan = read_plan(attempt)
        return CompositionTransferPreplanService(
            verified_manifest=verified_manifest,
            source_target=source_target,
            sink_target=sink_target,
            state_target=state_target,
            parent_context=parent_context,
            attempt=attempt,
            write=write,
            plan_sha256=plan.sources.subject_sha256,
            journal=CompositionTransferPreplanStore(payload_root),
            require_target=partial(
                require_transfer_target_identity,
                write=write,
                control=control,
                target=sink_target,
                state=state_target,
                state_database=location.location.database,
            ),
        )

    return build


def build_transfer_commit_verifier(payload_root: Any) -> Any:
    """Reopen the SQL-pinned envelope on each same-transaction observation."""

    def verify(connection: Any, bound: Any, receipt: Any, payload: Any) -> None:
        original = bound.require_preplan()
        retained = CompositionTransferPreplanStore(payload_root).load(bound.attempt, bytes.fromhex(original[7:]))
        verify_retained_transfer_commit(
            retained, binding=bound, receipt=receipt, payload=payload, connector=_ReadSession(connection)
        )

    return verify
