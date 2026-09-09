"""Pure helper functions for the backfill command service."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.backfill.progress import backfill_progress


def advisor_profile(backfill_options: Mapping[str, Any]) -> str:
    """Resolve the self-service advisor profile from manifest options."""

    advisor_options = backfill_options.get("advisor")
    if not isinstance(advisor_options, Mapping):
        return "balanced"
    return str(advisor_options.get("optimize_for") or "balanced")


def backfill_status_projection(ledger: Any | None) -> dict[str, Any]:
    """Project optional durable progress and publication evidence."""

    if ledger is None:
        return {"progress": None, "publication": None}
    publication = ledger.publication
    return {
        "progress": backfill_progress(ledger),
        "publication": publication.to_jsonable() if publication is not None else None,
    }


def backfill_operation_status(payload: Mapping[str, Any]) -> str | None:
    result = payload.get("result")
    details = result.get("details") if isinstance(result, Mapping) else None
    backfill = details.get("backfill") if isinstance(details, Mapping) else None
    status = backfill.get("operation_status") if isinstance(backfill, Mapping) else None
    return str(status) if status else None


def doctor_next_actions(manifest: str, counts: Mapping[str, int]) -> list[str]:
    if counts.get("running"):
        return [f"dpone backfill status {manifest} --format json"]
    if counts.get("failed"):
        return [
            f"dpone backfill retry-failed {manifest} --execute",
            f"dpone backfill status {manifest} --format json",
        ]
    if counts.get("pending"):
        return [f"dpone backfill resume {manifest}"]
    return []


def sink_type_from_options(options: Mapping[str, Any] | None) -> str:
    if not isinstance(options, Mapping):
        return ""
    return str(options.get("sink_type") or "")


def state_backend(backfill_options: Mapping[str, Any]) -> str:
    state = backfill_options.get("state") if isinstance(backfill_options.get("state"), Mapping) else {}
    return str((state or {}).get("backend") or "local_file")


__all__ = [
    "advisor_profile",
    "backfill_operation_status",
    "backfill_status_projection",
    "doctor_next_actions",
    "sink_type_from_options",
    "state_backend",
]
