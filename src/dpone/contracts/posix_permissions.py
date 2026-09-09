"""Pure POSIX access-bit predicates shared by filesystem safety adapters."""

from __future__ import annotations

_ACCESS_BITS = 0o777


def has_posix_access_mode(mode: int, expected: int) -> bool:
    """Compare owner/group/world access bits while ignoring inherited special bits."""

    if expected < 0 or expected & ~_ACCESS_BITS:
        raise ValueError("expected POSIX access mode must contain only owner/group/world bits")
    return mode & _ACCESS_BITS == expected


__all__ = ["has_posix_access_mode"]
