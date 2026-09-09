"""Runtime process error contracts."""

from __future__ import annotations


class ETLProcessError(Exception):
    """Raised when an ETL process fails during runtime execution."""


__all__ = ["ETLProcessError"]
