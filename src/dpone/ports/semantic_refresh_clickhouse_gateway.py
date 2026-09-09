"""Primitive ClickHouse publication gateway capability."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol


class SemanticRefreshClickHouseGateway(Protocol):
    """Primitive-mapping boundary implemented by a thin ClickHouse adapter."""

    def prepare(
        self,
        plan: Mapping[str, object],
        sql: Mapping[str, str],
    ) -> Mapping[str, object]:
        """Build/load/prove one shadow and return exact observations."""

    def inspect_uuid_map(self, request: Mapping[str, object]) -> Mapping[str, object]:
        """Read target and shadow UUID ownership from ClickHouse authority."""

    def inspect_target_authority(self, plan: Mapping[str, object]) -> Mapping[str, object]:
        """Read the full post-exchange target physical authority."""

    def inspect_empty_scope(self, plan: Mapping[str, object]) -> Mapping[str, object]:
        """Read immutable empty-manifest and current target-scope evidence."""

    def revalidate(
        self,
        plan: Mapping[str, object],
        sql: Mapping[str, str],
    ) -> Mapping[str, object]:
        """Rerun exact PREPARE invariants immediately before sequential COMMIT."""

    def exchange(self, request: Mapping[str, object]) -> None:
        """Execute the guarded atomic EXCHANGE TABLES statement."""

    def cleanup_retained(self, request: Mapping[str, object]) -> None:
        """Drop only exact attempt-local relations after durable COMPLETE."""
