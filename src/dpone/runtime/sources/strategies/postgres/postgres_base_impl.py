"""Deprecated compatibility shim for the PostgreSQL base source strategy.

Use ``dpone.runtime.sources.strategies.postgres.postgres_base`` for public imports or
``dpone.runtime.sources.strategies.postgres.postgres_base_strategy`` for internal implementation imports.
"""

from __future__ import annotations

from dpone.runtime.sources.strategies.postgres.postgres_base_strategy import PostgresBaseStrategy

__all__ = ["PostgresBaseStrategy"]
