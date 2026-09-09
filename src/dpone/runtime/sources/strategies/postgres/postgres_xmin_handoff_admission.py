"""Pre-payload admission for explicit PostgreSQL XMin incremental mode."""

from __future__ import annotations

from typing import Any

from dpone.ports.source_state_storage import SourceStateKey
from dpone.runtime.postgres_xmin_execution import (
    PostgresXminExecutionMode,
    PostgresXminExecutionPolicy,
    xmin_handoff_seed_load_id,
)
from dpone.runtime.state.xmin_storage import XMinState


def require_committed_xmin_handoff(
    *,
    state_storage: Any,
    state_key: SourceStateKey,
    state: XMinState | None,
    policy: PostgresXminExecutionPolicy,
) -> XMinState | None:
    """Require the deterministic initial seed receipt before incremental I/O."""

    if policy.mode is not PostgresXminExecutionMode.INCREMENTAL:
        return state
    if state is None or policy.handoff_id is None:
        raise RuntimeError("postgres_xmin_handoff.not_committed")
    probe = getattr(state_storage, "probe_receipt", None)
    if not callable(probe):
        raise RuntimeError("postgres_xmin_handoff.receipt_probe_required")
    receipt = probe(
        key=state_key,
        load_id=xmin_handoff_seed_load_id(
            handoff_id=policy.handoff_id,
            state_key_digest=state_key.digest,
        ),
    )
    exact = (
        receipt is not None
        and receipt.candidate_revision == 1
        and state.revision >= 1
        and state.xmin_value >= receipt.candidate_xmin
    )
    if not exact:
        raise RuntimeError("postgres_xmin_handoff.not_committed")
    return state


__all__ = ["require_committed_xmin_handoff"]
