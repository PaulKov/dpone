"""Physical resource gates around bounded ClickHouse publication mutations."""

from __future__ import annotations

from collections.abc import Callable, Mapping

from dpone.adapters.semantic_refresh_clickhouse_http_queries import (
    ClickHouseHttpGatewayError,
    PlanNames,
    relation_bytes_query,
    retained_backup_bytes_query,
    scope_count_query,
)


class ClickHousePublicationResourceGuard:
    """Verify protected per-relation and aggregate transient-byte limits."""

    def __init__(self, scalar: Callable[[str], int]) -> None:
        self._scalar = scalar

    def assert_pre_mutation(self, plan: Mapping[str, object], names: PlanNames) -> None:
        """Reject current drift and an impossible protected peak before CREATE."""

        artifact_rows = _non_negative_count(plan, "artifact_total_rows")
        if artifact_rows > _positive_limit(plan, "max_staging_rows"):
            raise ClickHouseHttpGatewayError(
                "ClickHouse sealed artifact row count exceeds staging row budget before rebuild"
            )
        target_bytes = self._scalar(relation_bytes_query(names.database, names.target))
        if target_bytes > _positive_limit(plan, "max_shadow_bytes"):
            raise ClickHouseHttpGatewayError("ClickHouse current target bytes exceed shadow byte budget before rebuild")
        _, _, retained_bytes = self.assert_current(plan, names, boundary="before rebuild")
        staging_limit = _positive_limit(plan, "max_staging_bytes")
        shadow_limit = _positive_limit(plan, "max_shadow_bytes")
        total_limit = _positive_limit(plan, "max_total_transient_bytes")
        if retained_bytes + staging_limit + shadow_limit > total_limit:
            raise ClickHouseHttpGatewayError(
                "ClickHouse protected staging/shadow peak exceeds total transient byte budget before rebuild"
            )

    def assert_current(
        self,
        plan: Mapping[str, object],
        names: PlanNames,
        *,
        boundary: str,
    ) -> tuple[int, int, int]:
        """Observe and enforce exact current bytes and target-scope rows."""

        staging_bytes, shadow_bytes, retained_bytes = self.resource_bytes(names)
        target_scope_rows = self._scalar(
            scope_count_query(
                names.database,
                names.target,
                str(plan["event_time_column"]),
                str(plan["scope_start"]),
                str(plan["scope_end"]),
            )
        )
        checks = (
            (target_scope_rows, _positive_limit(plan, "max_target_scope_rows"), "target scope row"),
            (staging_bytes, _positive_limit(plan, "max_staging_bytes"), "staging byte"),
            (shadow_bytes, _positive_limit(plan, "max_shadow_bytes"), "shadow byte"),
            (retained_bytes, _positive_limit(plan, "max_retained_backup_bytes"), "retained backup byte"),
            (
                staging_bytes + shadow_bytes + retained_bytes,
                _positive_limit(plan, "max_total_transient_bytes"),
                "total transient byte",
            ),
        )
        for observed, maximum, label in checks:
            if observed > maximum:
                raise ClickHouseHttpGatewayError(f"ClickHouse {label} budget exceeded {boundary}")
        return staging_bytes, shadow_bytes, retained_bytes

    def resource_bytes(self, names: PlanNames) -> tuple[int, int, int]:
        """Read exact current staging, shadow, and retained inventory bytes."""

        staging_bytes = self._scalar(relation_bytes_query(names.database, names.staging))
        shadow_bytes = self._scalar(relation_bytes_query(names.database, names.shadow))
        retained_bytes = self._scalar(relation_bytes_query(names.database, names.target)) + self._scalar(
            retained_backup_bytes_query(names)
        )
        return staging_bytes, shadow_bytes, retained_bytes


def _positive_limit(plan: Mapping[str, object], field: str) -> int:
    value = plan.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ClickHouseHttpGatewayError(f"ClickHouse protected {field} is invalid")
    return value


def _non_negative_count(plan: Mapping[str, object], field: str) -> int:
    value = plan.get(field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ClickHouseHttpGatewayError(f"ClickHouse protected {field} is invalid")
    return value


__all__ = ["ClickHousePublicationResourceGuard"]
