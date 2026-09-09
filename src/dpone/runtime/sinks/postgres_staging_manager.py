"""Compatibility shim for PostgreSQL staging manager.

Canonical implementation lives in
``dpone.runtime.sinks.staging_managers.postgres``.
"""

from __future__ import annotations

from dpone.runtime.sinks.staging_managers.postgres import PostgresStagingManager

__all__ = ["PostgresStagingManager"]
