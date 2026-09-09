"""Typed ClickHouse CDC materialization contracts."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol


class ClickHouseCdcTypedColumnPort(Protocol):
    """Column shape required by typed CDC SQL renderers."""

    name: str
    clickhouse_type: str
    required: bool

    @property
    def resolved_payload_key(self) -> str:
        """Return the JSON payload key backing this typed column."""

    @property
    def ddl_fragment(self) -> str:
        """Return this column's ClickHouse DDL fragment."""


class ClickHouseCdcTypedPlanPort(Protocol):
    """Plan shape required by typed CDC SQL renderers."""

    cdc_database: str
    cdc_table: str
    target_database: str
    target_table: str
    unique_key: tuple[str, ...]
    columns: Sequence[ClickHouseCdcTypedColumnPort]

    @property
    def qualified_cdc_table(self) -> str:
        """Return the qualified CDC log table."""

    @property
    def qualified_shadow_table(self) -> str:
        """Return the qualified shadow serving table."""


class ClickHouseCdcTypedPolicyPort(Protocol):
    """Policy shape required by typed CDC SQL renderers."""

    delete_mode: str


__all__ = [
    "ClickHouseCdcTypedColumnPort",
    "ClickHouseCdcTypedPlanPort",
    "ClickHouseCdcTypedPolicyPort",
]
