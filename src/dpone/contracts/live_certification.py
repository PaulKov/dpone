"""Shared live-certification public profile contract."""

from __future__ import annotations

LIVE_CERTIFICATION_PROFILE_CHOICES: tuple[str, ...] = (
    "local_live",
    "real_local",
    "type_matrix_certification",
    "native_transfer",
    "vendor_live",
)

LOCAL_LIVE_CERTIFICATION_PROFILES: frozenset[str] = frozenset(
    {
        "local_live",
        "real_local",
        "type_matrix_certification",
        "native_transfer",
    }
)

__all__ = ["LIVE_CERTIFICATION_PROFILE_CHOICES", "LOCAL_LIVE_CERTIFICATION_PROFILES"]
