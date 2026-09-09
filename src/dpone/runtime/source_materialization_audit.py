"""Runtime audit helpers for source-side materialization lifecycle events."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.runtime.decision_audit import publish_runtime_decision
from dpone.runtime.source_materialization_cleanup import cleanup_result_to_dict


def publish_source_materialization_cleanup(
    *,
    provider: str | None,
    cleanup_policy: str,
    result: Any,
) -> dict[str, Any] | None:
    """Normalize and publish source cleanup result as runtime decision evidence."""

    normalized = cleanup_result_to_dict(result)
    if normalized is None:
        return None
    status = str(normalized.get("status") or "unknown")
    reason = normalized.get("reason")
    warning = status not in {"deleted", "skipped"}
    publish_runtime_decision(
        {
            "requested_backend": cleanup_policy,
            "selected_backend": status,
            "fallback_reason": reason if warning else None,
            "release_gate": "warning" if warning else "green",
            "warnings": [str(reason)] if warning and reason else [],
            "blockers": ["source_materialization_cleanup_failed"] if status == "failed" else [],
        },
        decision_id="source_materialization.cleanup",
        phase="cleanup",
        component="source_materialization",
        category="managed_artifact_cleanup",
        fallback_allowed=status == "deferred",
        provider=provider,
        details={"cleanup_result": _plain_dict(normalized)},
    )
    return normalized


def _plain_dict(value: Mapping[str, Any]) -> dict[str, Any]:
    return dict(value)


__all__ = ["publish_source_materialization_cleanup"]
