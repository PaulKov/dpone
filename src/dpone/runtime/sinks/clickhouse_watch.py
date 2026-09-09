"""ClickHouse release-watch probe."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.runtime.sinks.clickhouse_post_apply import ClickHousePostApplyVerifier


@dataclass(slots=True)
class ClickHouseWatchProbe:
    """Read-only ClickHouse probe for release watch samples."""

    post_apply: ClickHousePostApplyVerifier

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any],
        connector_factory: Callable[[Mapping[str, Any]], Any] | None = None,
    ) -> ClickHouseWatchProbe:
        return cls(ClickHousePostApplyVerifier.from_config(config, connector_factory=connector_factory))

    def inspect(self, plan: dict[str, Any]) -> Any:
        return self.post_apply.inspect(plan)

    def execute(self, canary: dict[str, Any]) -> Sequence[Mapping[str, Any]]:
        return self.post_apply.execute(canary)

    def profile(self, plan: dict[str, Any]) -> Mapping[str, Any]:
        return self.post_apply.profile(plan)

    def query_health(self, plan: dict[str, Any]) -> Mapping[str, Any]:
        if not _checks(plan).get("query_health", False):
            return {"checks": [], "blockers": [], "warnings": []}
        query_health = plan.get("query_health", {})
        query_health = query_health if isinstance(query_health, Mapping) else {}
        try:
            row = _row(self.post_apply.connector, _query_log_sql(plan))
        except Exception:  # noqa: BLE001 - query_log availability differs by ClickHouse setup.
            code = "schema_migration_watch.query_log_unavailable"
            if str(plan.get("profile")) in {"prod_strict", "regulated"}:
                return {"checks": [{"name": "query_health", "status": "failed"}], "blockers": [code], "warnings": []}
            return {"checks": [{"name": "query_health", "status": "warning"}], "blockers": [], "warnings": [code]}
        metrics = {
            "query_count": int(row.get("query_count", 0) or 0),
            "error_count": int(row.get("error_count", 0) or 0),
            "p95_ms": int(float(row.get("p95_ms", 0) or 0)),
            "max_read_rows": int(row.get("max_read_rows", 0) or 0),
            "max_memory_usage": int(row.get("max_memory_usage", 0) or 0),
        }
        blockers = _budget_blockers(metrics, query_health)
        return {
            "checks": [{"name": "query_health", "status": "failed" if blockers else "passed", "metrics": metrics}],
            "blockers": blockers,
            "warnings": [],
            "metrics": metrics,
        }

    def close(self) -> None:
        self.post_apply.close()


def _query_log_sql(plan: Mapping[str, Any]) -> str:
    target = plan.get("target", {})
    table = str(target.get("table") if isinstance(target, Mapping) else "")
    schema, name = table.split(".", 1) if "." in table else ("default", table)
    query_health = plan.get("query_health", {})
    lookback = int(query_health.get("lookback_seconds", 900) if isinstance(query_health, Mapping) else 900)
    literal = _quote_literal(f"{schema}.{name}")
    quoted = _quote_literal(f"`{schema}`.`{name}`")
    return (
        "SELECT count() AS query_count, "
        "sum(if(exception_code != 0 OR type = 'ExceptionWhileProcessing', 1, 0)) AS error_count, "
        "quantileExact(0.95)(query_duration_ms) AS p95_ms, "
        "max(read_rows) AS max_read_rows, max(memory_usage) AS max_memory_usage "
        "FROM system.query_log "
        f"WHERE event_time >= now() - INTERVAL {max(0, lookback)} SECOND "
        f"AND (position(query, {literal}) > 0 OR position(query, {quoted}) > 0)"
    )


def _budget_blockers(metrics: Mapping[str, int], budget: Mapping[str, Any]) -> list[str]:
    blockers: list[str] = []
    if _exceeds(metrics, budget, "error_count", "max_error_count"):
        blockers.append("schema_migration_watch.query_health_blocked:error_count")
    if _exceeds(metrics, budget, "p95_ms", "max_p95_ms"):
        blockers.append("schema_migration_watch.query_health_blocked:p95_ms")
    if _exceeds(metrics, budget, "max_read_rows", "max_read_rows"):
        blockers.append("schema_migration_watch.query_health_blocked:max_read_rows")
    return blockers


def _exceeds(metrics: Mapping[str, int], budget: Mapping[str, Any], metric_key: str, budget_key: str) -> bool:
    limit = int(budget.get(budget_key, 0) or 0)
    return limit > 0 and int(metrics.get(metric_key, 0) or 0) > limit


def _checks(plan: Mapping[str, Any]) -> Mapping[str, bool]:
    checks = plan.get("checks", {})
    return checks if isinstance(checks, Mapping) else {}


def _row(connector: Any, query: str) -> Mapping[str, Any]:
    rows = connector.get_records(query, as_dict=True)
    first = rows[0] if rows else {}
    return first if isinstance(first, Mapping) else {}


def _quote_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


__all__ = ["ClickHouseWatchProbe"]
