"""File-IO facade for data product assertion commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.data_product_assertion_sql import safe_select_sql
from dpone.readiness.migration_control import stable_fingerprint


class DataProductAssertionFacade:
    """Thin CLI facade; assertion policy lives in readiness modules."""

    def plan(self, *, manifest_path: str) -> dict[str, Any]:
        payload = _assertions().DataProductAssertionPlanner().plan(manifest=_read_mapping(manifest_path))
        return _resolve_import_paths(payload, Path(manifest_path).parent)

    def evaluate(
        self,
        *,
        plan_path: str,
        runtime_artifact_path: str | None = None,
        target_connection_path: str | None = None,
        target_evidence_path: str | None = None,
    ) -> dict[str, Any]:
        plan = _read_mapping(plan_path)
        target = _read_optional(target_evidence_path) or _target_evidence(plan, target_connection_path)
        return (
            _assertions()
            .DataProductAssertionEvaluator()
            .evaluate(
                plan=plan,
                runtime_artifacts=_runtime_artifacts(runtime_artifact_path),
                target_evidence=target,
                imported_evidence=_imported_evidence(plan),
            )
        )

    def gate(self, *, evaluation_path: str, profile: str) -> dict[str, Any]:
        return (
            _assertions()
            .DataProductAssertionGate()
            .evaluate(evaluation=_read_mapping(evaluation_path), profile=profile)
        )

    def report(self, *, evaluation_path: str) -> dict[str, Any]:
        return _assertions().DataProductAssertionGate().report(evaluation=_read_mapping(evaluation_path))


def _resolve_import_paths(payload: dict[str, Any], base: Path) -> dict[str, Any]:
    imports = payload.get("imports")
    if not isinstance(imports, Mapping):
        return payload
    resolved = {
        str(key): str(path if (path := Path(str(value))).is_absolute() else base / path)
        for key, value in imports.items()
    }
    payload = {**payload, "imports": resolved}
    payload["assertion_plan_id"] = stable_fingerprint(
        {key: value for key, value in payload.items() if key != "assertion_plan_id"}
    )
    return payload


def _runtime_artifacts(path: str | None) -> tuple[dict[str, Any], ...]:
    if not path:
        return ()
    payload = _read_mapping(path)
    if isinstance(payload.get("runs"), list):
        return tuple(dict(item) for item in payload["runs"] if isinstance(item, Mapping))
    return (payload,)


def _imported_evidence(plan: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    payloads: list[dict[str, Any]] = []
    imports = plan.get("imports")
    if not isinstance(imports, Mapping):
        return ()
    for path in imports.values():
        try:
            payloads.append(_read_mapping(str(path)))
        except FileNotFoundError:
            continue
    return tuple(payloads)


def _target_evidence(plan: Mapping[str, Any], path: str | None) -> dict[str, Any] | None:
    if not path:
        return None
    connection = _read_mapping(path)
    if str(connection.get("type") or connection.get("sink_type") or "").lower() != "clickhouse":
        return {"warnings": [f"data_product_assertions.unsupported_target:{connection.get('type') or 'unknown'}"]}
    return _clickhouse_probe(plan, connection)


def _clickhouse_probe(plan: Mapping[str, Any], connection: Mapping[str, Any]) -> dict[str, Any]:
    try:
        import clickhouse_connect
    except ModuleNotFoundError:
        return {"warnings": ["data_product_assertions.clickhouse_probe_unavailable"]}
    target = plan.get("target", {})
    table = target.get("table") if isinstance(target, Mapping) else None
    if not table:
        return {"warnings": ["data_product_assertions.target_table_missing"]}
    client = clickhouse_connect.get_client(
        host=str(connection.get("host") or "localhost"),
        port=int(connection.get("port") or 8123),
        username=str(connection.get("username") or connection.get("user") or "default"),
        password=str(connection.get("password") or ""),
        database=str(connection.get("database") or "default"),
    )
    table_sql = _quote_path(str(table))
    evidence: dict[str, Any] = {"row_count": _scalar(client, f"SELECT count() FROM {table_sql}")}
    sql_results: dict[str, dict[str, Any]] = {}
    for assertion in _assertions_from_plan(plan):
        assertion_type = str(assertion.get("type") or "")
        if assertion_type == "null_key":
            for column in _columns(assertion):
                _record_count(evidence, "null_key_failures", column, _null_count(client, table_sql, column))
        elif assertion_type == "duplicate_key":
            for column in _columns(assertion):
                _record_count(evidence, "duplicate_key_failures", column, _duplicate_count(client, table_sql, column))
        elif assertion_type == "accepted_values":
            column = str(assertion.get("column") or "")
            _record_count(
                evidence, "accepted_values_failures", column, _accepted_values_count(client, table_sql, assertion)
            )
        elif assertion_type == "regex":
            column = str(assertion.get("column") or "")
            _record_count(evidence, "regex_failures", column, _regex_count(client, table_sql, assertion))
        elif assertion_type == "range":
            column = str(assertion.get("column") or "")
            _record_count(evidence, "range_failures", column, _range_count(client, table_sql, assertion))
        elif assertion_type == "sql" and safe_select_sql(str(assertion.get("query") or "")):
            value = _scalar(client, str(assertion.get("query") or "SELECT 0"))
            column = str(_mapping(assertion.get("expect")).get("column") or "value")
            sql_results[str(assertion.get("id"))] = {column: value}
    if sql_results:
        evidence["sql_results"] = sql_results
    return evidence


def _assertions_from_plan(plan: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    found: list[Mapping[str, Any]] = []
    for suite in plan.get("suites", []) if isinstance(plan.get("suites"), list) else ():
        if isinstance(suite, Mapping) and isinstance(suite.get("assertions"), list):
            found.extend(item for item in suite["assertions"] if isinstance(item, Mapping))
    return tuple(found)


def _null_count(client: Any, table_sql: str, column: str) -> Any:
    return _scalar(client, f"SELECT count() FROM {table_sql} WHERE {_quote_path(column)} IS NULL")


def _duplicate_count(client: Any, table_sql: str, column: str) -> Any:
    column_sql = _quote_path(column)
    return _scalar(
        client,
        f"SELECT count() FROM (SELECT {column_sql}, count() AS c FROM {table_sql} GROUP BY {column_sql} HAVING c > 1)",
    )


def _accepted_values_count(client: Any, table_sql: str, assertion: Mapping[str, Any]) -> Any:
    column = str(assertion.get("column") or "")
    values = assertion.get("values")
    if not column or not isinstance(values, list):
        return None
    allowed = ", ".join(_literal(value) for value in values)
    return _scalar(client, f"SELECT count() FROM {table_sql} WHERE {_quote_path(column)} NOT IN ({allowed})")


def _regex_count(client: Any, table_sql: str, assertion: Mapping[str, Any]) -> Any:
    column = str(assertion.get("column") or "")
    pattern = str(assertion.get("pattern") or "")
    if not column or not pattern:
        return None
    return _scalar(
        client,
        f"SELECT count() FROM {table_sql} WHERE NOT match(toString({_quote_path(column)}), {_literal(pattern)})",
    )


def _range_count(client: Any, table_sql: str, assertion: Mapping[str, Any]) -> Any:
    column = str(assertion.get("column") or "")
    if not column:
        return None
    checks: list[str] = []
    if assertion.get("min") is not None:
        checks.append(f"{_quote_path(column)} < {_literal(assertion.get('min'))}")
    if assertion.get("max") is not None:
        checks.append(f"{_quote_path(column)} > {_literal(assertion.get('max'))}")
    if not checks:
        return None
    return _scalar(client, f"SELECT count() FROM {table_sql} WHERE {' OR '.join(checks)}")


def _record_count(evidence: dict[str, Any], key: str, column: str, value: Any) -> None:
    if not column or value is None:
        return
    bucket = evidence.setdefault(key, {})
    if isinstance(bucket, dict):
        bucket[column] = value


def _columns(assertion: Mapping[str, Any]) -> tuple[str, ...]:
    raw = assertion.get("columns") or ([assertion.get("column")] if assertion.get("column") else [])
    return tuple(str(item) for item in raw if str(item)) if isinstance(raw, list) else ()


def _quote_path(value: str) -> str:
    return ".".join(f"`{part.replace('`', '``')}`" for part in value.split(".") if part)


def _literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, int | float):
        return str(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _scalar(client: Any, query: str) -> Any:
    result = client.query(query)
    return result.result_rows[0][0] if result.result_rows else None


def _read_optional(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(path) if path else None


def _read_mapping(path: str) -> dict[str, Any]:
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _assertions() -> Any:
    return import_module("dpone.readiness.data_product_assertions")


__all__ = ["DataProductAssertionFacade"]
