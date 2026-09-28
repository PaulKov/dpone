"""Bridge authoritative governance quality receipts into range evidence."""

from __future__ import annotations

from typing import Any


def advance_range_governed_quality(handle: Any, receipt: Any, evidence: Any) -> None:
    """Project the validated, public receipt fields without retaining its authority token."""

    transition = getattr(handle.sink_state, "mark_range_governed_quality_passed", None)
    if callable(transition):
        transition(
            config=handle.finalization_config or handle.staging_config,
            quality_receipt={
                "boundary": receipt.boundary,
                "policy_snapshot_id": receipt.policy_snapshot_id,
                "run_id": receipt.run_id,
                "load_id": receipt.load_id,
                "report": evidence,
            },
        )
