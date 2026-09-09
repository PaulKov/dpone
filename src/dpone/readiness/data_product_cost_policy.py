"""Profile policy helpers for data product cost governance."""

from __future__ import annotations

from collections.abc import Sequence


def status(blockers: Sequence[str], warnings: Sequence[str], success: str) -> str:
    return "blocked" if blockers else "warning" if warnings else success


def apply_profile(
    *,
    blockers: Sequence[str],
    warnings: Sequence[str],
    profile: str,
    mode: str = "gate",
) -> tuple[list[str], list[str]]:
    unique_blockers = list(dict.fromkeys(str(item) for item in blockers if str(item)))
    unique_warnings = list(dict.fromkeys(str(item) for item in warnings if str(item)))
    if mode == "observe" or profile == "advisory":
        return [], [*unique_warnings, *unique_blockers]
    return unique_blockers, unique_warnings


__all__ = ["apply_profile", "status"]
