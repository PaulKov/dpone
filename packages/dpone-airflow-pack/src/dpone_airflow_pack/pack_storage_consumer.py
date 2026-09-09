"""Scheduler-side pack storage mode (mirrors ``dpone.gitops.pack_storage_mode`` without importing dpone)."""

from __future__ import annotations

import os

PACK_STORAGE_MODES = frozenset({"gitops", "remote", "hybrid"})
STORAGE_MODE_ENV = "DPONE_AIRFLOW_PACK_STORAGE_MODE"
_MODE_ALIASES = {
    "object_storage": "remote",
    "object-storage": "remote",
    "s3": "remote",
}


def normalize_pack_storage_mode(raw: object) -> str:
    """Normalize authored mode; default ``gitops``; alias ``object_storage``→``remote``."""

    if raw is None or raw == "":
        return "gitops"
    text = str(raw).strip().lower()
    text = _MODE_ALIASES.get(text, text)
    if text not in PACK_STORAGE_MODES:
        allowed = ", ".join(sorted(PACK_STORAGE_MODES))
        raise ValueError(f"{STORAGE_MODE_ENV} must be one of: {allowed}")
    return text


def pack_storage_mode_from_environ() -> str:
    return normalize_pack_storage_mode(os.environ.get(STORAGE_MODE_ENV))


def allows_git_fallback_on_cache_miss() -> bool:
    """Whether scheduler may read ``.dpone/gitops`` when the remote cache is unusable.

    Aligns with ``PackStoragePolicy.allows_git_fallback`` for ``remote`` (deny) and
    ``hybrid`` (allow). ``gitops`` also allows the local tree because it is the
    primary artifact source, not a cache fallback.
    """

    return pack_storage_mode_from_environ() != "remote"


class DagSpecCacheMissingError(RuntimeError):
    """Remote storage mode cannot load dag-specs from the configured cache."""

    code = "dag_spec_cache_missing"

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.code = DagSpecCacheMissingError.code


__all__ = [
    "PACK_STORAGE_MODES",
    "STORAGE_MODE_ENV",
    "DagSpecCacheMissingError",
    "allows_git_fallback_on_cache_miss",
    "normalize_pack_storage_mode",
    "pack_storage_mode_from_environ",
]
