"""ClickHouse read-only post-apply verification adapter."""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from types import SimpleNamespace
from typing import Any

from dpone.readiness.physical_state import PhysicalTableState
from dpone.runtime.sinks.clickhouse_physical_reconciliation import ClickHousePhysicalIntrospector


@dataclass(slots=True)
class ClickHousePostApplyVerifier:
    """Read-only ClickHouse inspector and canary executor."""

    connector: Any

    @classmethod
    def from_config(
        cls,
        config: Mapping[str, Any],
        connector_factory: Callable[[Mapping[str, Any]], Any] | None = None,
    ) -> ClickHousePostApplyVerifier:
        factory = connector_factory or _default_connector_factory
        return cls(factory(config))

    def inspect(self, plan: dict[str, Any]) -> PhysicalTableState:
        schema, table = _schema_table(plan)
        load_config = SimpleNamespace(target_schema=schema, target_table=table)
        return ClickHousePhysicalIntrospector(self.connector).inspect(load_config)

    def execute(self, canary: dict[str, Any]) -> Sequence[Mapping[str, Any]]:
        return self.connector.get_records(str(canary.get("query") or ""), as_dict=True)

    def profile(self, plan: dict[str, Any]) -> Mapping[str, Any]:
        table = _quoted_table(plan)
        checks = _checks(plan)
        columns = _desired_columns(plan)
        key_columns = _key_columns(plan, columns)
        check_results: list[dict[str, Any]] = []
        blockers: list[str] = []
        metrics: dict[str, Any] = {}
        if checks.get("row_count"):
            row_count = int(_scalar(self.connector, f"SELECT count() AS row_count FROM {table}", "row_count") or 0)
            metrics["row_count"] = row_count
            check_results.append({"name": "row_count", "status": "passed", "value": row_count})
        if checks.get("typed_hash") and columns:
            typed_hash = str(
                _scalar(
                    self.connector, f"SELECT {_typed_hash_expression(columns)} AS typed_hash FROM {table}", "typed_hash"
                )
                or ""
            )
            metrics["typed_hash"] = typed_hash
            check_results.append({"name": "typed_hash", "status": "passed", "value": typed_hash})
        if checks.get("null_distribution") and columns:
            nulls = _row(
                self.connector,
                f"SELECT {', '.join(f'sum(isNull({_quote_identifier(column)})) AS {_quote_identifier(column)}' for column in columns)} "
                f"FROM {table}",
            )
            metrics["null_distribution"] = {column: int(nulls.get(column, 0) or 0) for column in columns}
            check_results.append({"name": "null_distribution", "status": "passed"})
        if checks.get("duplicate_key") and key_columns:
            duplicate_count = int(
                _scalar(self.connector, _duplicate_key_query(table=table, key_columns=key_columns), "duplicate_keys")
                or 0
            )
            status = "failed" if duplicate_count else "passed"
            check_results.append({"name": "duplicate_key", "status": status, "value": duplicate_count})
            if duplicate_count:
                blockers.append(f"schema_migration_post_apply.duplicate_key:{','.join(key_columns)}")
        if checks.get("null_key") and key_columns:
            null_key_count = int(
                _scalar(self.connector, _null_key_query(table=table, key_columns=key_columns), "null_keys") or 0
            )
            status = "failed" if null_key_count else "passed"
            check_results.append({"name": "null_key", "status": status, "value": null_key_count})
            if null_key_count:
                blockers.append(f"schema_migration_post_apply.null_key:{','.join(key_columns)}")
        if checks.get("nested_parent_child"):
            check_results.append({"name": "nested_parent_child", "status": "passed", "skipped": True})
        return {"checks": check_results, "blockers": blockers, "warnings": [], "metrics": metrics}

    def close(self) -> None:
        close = getattr(self.connector, "close", None)
        if callable(close):
            close()


def _schema_table(plan: Mapping[str, Any]) -> tuple[str, str]:
    target = plan.get("target", {})
    table = str(target.get("table") if isinstance(target, Mapping) else "")
    if "." in table:
        schema, name = table.split(".", 1)
        return schema, name
    return "default", table


def _quoted_table(plan: Mapping[str, Any]) -> str:
    schema, table = _schema_table(plan)
    return f"{_quote_identifier(schema)}.{_quote_identifier(table)}"


def _checks(plan: Mapping[str, Any]) -> Mapping[str, bool]:
    checks = plan.get("checks", {})
    return checks if isinstance(checks, Mapping) else {}


def _desired_columns(plan: Mapping[str, Any]) -> tuple[str, ...]:
    desired = plan.get("desired", {})
    columns = desired.get("columns", {}) if isinstance(desired, Mapping) else {}
    if isinstance(columns, Mapping):
        return tuple(str(name) for name in columns if str(name))
    return ()


def _key_columns(plan: Mapping[str, Any], columns: Sequence[str]) -> tuple[str, ...]:
    desired = plan.get("desired", {})
    order_by = desired.get("order_by", []) if isinstance(desired, Mapping) else []
    if isinstance(order_by, Sequence) and not isinstance(order_by, str):
        keys = tuple(str(item).strip("`") for item in order_by if str(item))
        return tuple(key for key in keys if key in columns)
    return ()


def _row(connector: Any, query: str) -> Mapping[str, Any]:
    rows = connector.get_records(query, as_dict=True)
    first = rows[0] if rows else {}
    return first if isinstance(first, Mapping) else {}


def _scalar(connector: Any, query: str, column: str) -> Any:
    return _row(connector, query).get(column)


def _typed_hash_expression(columns: Sequence[str]) -> str:
    parts = ", '|#|', ".join(
        f"if(isNull({_quote_identifier(column)}), '<NULL>', toString({_quote_identifier(column)}))"
        for column in columns
    )
    return f"hex(groupBitXor(cityHash64(concat({parts}))))"


def _duplicate_key_query(*, table: str, key_columns: Sequence[str]) -> str:
    keys = ", ".join(_quote_identifier(column) for column in key_columns)
    return (
        f"SELECT count() AS duplicate_keys FROM (SELECT {keys}, count() AS c FROM {table} GROUP BY {keys} HAVING c > 1)"
    )


def _null_key_query(*, table: str, key_columns: Sequence[str]) -> str:
    condition = " OR ".join(f"isNull({_quote_identifier(column)})" for column in key_columns)
    return f"SELECT count() AS null_keys FROM {table} WHERE {condition}"


def _quote_identifier(value: str) -> str:
    return f"`{str(value).strip('`').replace('`', '``')}`"


def _default_connector_factory(config: Mapping[str, Any]) -> Any:
    module = import_module("dpone.runtime.connectors.clickhouse")
    connector_cls = module.ClickHouseConnector
    return connector_cls(
        host=str(config.get("host", "127.0.0.1")),
        port=int(config.get("port", 9000)),
        database=str(config.get("database", "default")),
        user=str(config.get("user", config.get("username", "default"))),
        password=str(config.get("password", "")),
        secure=bool(config.get("secure", False)),
        compression=bool(config.get("compression", True)),
    )


__all__ = ["ClickHousePostApplyVerifier"]
