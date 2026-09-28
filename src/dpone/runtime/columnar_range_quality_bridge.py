"""Bridge authoritative governance quality receipts into range evidence."""

from __future__ import annotations

from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from dpone.runtime.governance.finalization_support import load_result_details
from dpone.runtime.governance.ports import staged_load_range_evidence_details


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


def range_load_result_details(load_result: Any, handle: Any) -> dict[str, Any]:
    """Attach the current publication state to the ordinary result evidence."""

    return {**load_result_details(load_result), **staged_load_range_evidence_details(handle)}


def record_terminal_range_evidence(record: Callable[..., None] | None, load_record: Any, handle: Any) -> None:
    """Persist cleanup-complete evidence without emitting empty non-range steps."""

    details = staged_load_range_evidence_details(handle)
    if details and record is not None:
        record(
            load_record,
            "range_evidence_terminal",
            "succeeded",
            started_at=datetime.now(UTC),
            details=details,
        )
