"""Safe state inspection and destructive-operation preview service."""

from __future__ import annotations

from typing import Any


class StateInspectorService:
    """Safe state-management UX facade for state backends."""

    def inspect(self, backend: str, state_type: str, identity: str) -> dict[str, Any]:
        return {
            "operation": "inspect",
            "backend": backend,
            "state_type": state_type,
            "identity": identity,
            "status": "plan_only",
        }

    def reset(self, backend: str, state_type: str, identity: str, *, yes: bool = False) -> dict[str, Any]:
        return {
            "operation": "reset",
            "backend": backend,
            "state_type": state_type,
            "identity": identity,
            "mode": "execute" if yes else "preview",
            "requires_yes": not yes,
        }

    def export(self, backend: str, state_type: str, identity: str) -> dict[str, Any]:
        return {
            "operation": "export",
            "backend": backend,
            "state_type": state_type,
            "identity": identity,
            "records": [],
        }

    def replay_from(
        self, backend: str, state_type: str, identity: str, offset: str, *, yes: bool = False
    ) -> dict[str, Any]:
        return {
            "operation": "replay_from",
            "backend": backend,
            "state_type": state_type,
            "identity": identity,
            "offset": offset,
            "mode": "execute" if yes else "preview",
            "requires_yes": not yes,
        }

    def rewind(
        self,
        backend: str,
        state_type: str,
        identity: str,
        *,
        to: str,
        reason: str = "",
        yes: bool = False,
        approved_by: str = "",
    ) -> dict[str, Any]:
        """Preview or execute a watermark rewind with an explicit approval gate.

        Rewinding a watermark makes the next incremental run re-read history,
        so execution requires both ``--yes`` and ``--approved-by``. The
        returned payload doubles as reviewable evidence (who approved what
        rewind, to which watermark, and why).
        """

        approval_granted = bool(yes and approved_by.strip())
        missing: list[str] = []
        if not yes:
            missing.append("--yes")
        if not approved_by.strip():
            missing.append("--approved-by")
        return {
            "operation": "rewind",
            "backend": backend,
            "state_type": state_type,
            "identity": identity,
            "target_watermark": to,
            "reason": reason or "unspecified",
            "mode": "execute" if approval_granted else "preview",
            "approval": {
                "required": True,
                "granted": approval_granted,
                "approved_by": approved_by.strip() or None,
                "missing": missing,
            },
            "next_actions": (
                [] if approval_granted else [f"re-run with {' and '.join(missing)} to execute the rewind"]
            ),
        }

    def compare(self, backend: str, state_type: str, left: str, right: str) -> dict[str, Any]:
        return {
            "operation": "compare",
            "backend": backend,
            "state_type": state_type,
            "left": left,
            "right": right,
            "differences": [],
        }


__all__ = ["StateInspectorService"]
