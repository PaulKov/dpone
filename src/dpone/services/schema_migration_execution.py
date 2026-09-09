"""Target execution helpers for schema migration packs."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, Protocol


class MigrationOperationExecutor(Protocol):
    """DI port for executing one target-side migration operation."""

    def execute(self, operation: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class MigrationExecutionReport:
    """Execution result for one phase or one flat DDL pack."""

    operations: tuple[dict[str, Any], ...]
    blockers: tuple[str, ...] = ()


def load_migration_operation_executor(
    *,
    target: Any,
    target_connection_path: str | None,
) -> MigrationOperationExecutor | None:
    """Build a target executor from an explicit connection JSON artifact."""

    if not target_connection_path:
        return None
    config = _read_json_object(target_connection_path)
    target_sink_type = _target_sink_type(target)
    sink_type = str(config.get("type", config.get("sink_type", target_sink_type))).lower()
    if sink_type != target_sink_type.lower():
        raise ValueError(f"target connection type {sink_type!r} does not match pack target {target_sink_type!r}")
    if sink_type == "clickhouse":
        module = import_module("dpone.runtime.sinks.clickhouse_migration_execution")
        return module.ClickHouseMigrationOperationExecutor.from_config(config)
    raise ValueError(f"target execution is implemented for clickhouse in v1, got {sink_type!r}")


def execute_migration_operations(
    *,
    operations: Sequence[dict[str, Any]],
    executor: MigrationOperationExecutor | None,
) -> MigrationExecutionReport:
    """Execute operations and fail closed on target or validation errors."""

    if not operations:
        return MigrationExecutionReport(operations=())
    if executor is None:
        return MigrationExecutionReport(operations=(), blockers=("migration.target_executor_required",))
    executed: list[dict[str, Any]] = []
    blockers: list[str] = []
    for operation in operations:
        try:
            result = _normalize_result(operation, executor.execute(operation))
        except Exception as exc:  # pragma: no cover - target-specific driver surface
            result = _operation_result(operation, status="failed", error=str(exc))
        executed.append(result)
        if result["status"] == "failed":
            blockers.append(f"migration.operation_failed:{result['name']}")
    blockers.extend(_validation_blockers(executed))
    return MigrationExecutionReport(
        operations=tuple(_public_result(item) for item in executed),
        blockers=tuple(dict.fromkeys(blockers)),
    )


def phase_operations(phase_payload: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    """Return executable operations in stable phase order."""

    return tuple(
        dict(item)
        for key in ("operations", "validations")
        for item in phase_payload.get(key, [])
        if isinstance(item, dict)
    )


def ddl_operations(ddl: Iterable[str]) -> tuple[dict[str, Any], ...]:
    """Wrap flat DDL statements as migration operations."""

    return tuple(
        {"name": f"ddl_{index}", "operation_type": "sql", "sql": statement}
        for index, statement in enumerate(ddl, start=1)
    )


def _validation_blockers(results: Sequence[dict[str, Any]]) -> list[str]:
    validations = [
        item for item in results if item.get("operation_type") == "validation" and item["status"] != "failed"
    ]
    blockers: list[str] = []
    index = 0
    while index + 1 < len(validations):
        left = validations[index]
        right = validations[index + 1]
        if _canonical_rows(left.get("_comparison_rows", [])) != _canonical_rows(right.get("_comparison_rows", [])):
            blockers.append(f"migration.validation_mismatch:{left['name']}:{right['name']}")
        index += 2
    if index < len(validations) and _jsonable_rows(validations[index].get("_comparison_rows", [])):
        blockers.append(f"migration.validation_not_empty:{validations[index]['name']}")
    return blockers


def _normalize_result(operation: Mapping[str, Any], result: Mapping[str, Any]) -> dict[str, Any]:
    return _operation_result(
        operation,
        status=str(result.get("status", "executed")),
        result_rows=result.get("result_rows"),
        error=str(result["error"]) if result.get("error") else None,
    )


def _operation_result(
    operation: Mapping[str, Any],
    *,
    status: str,
    result_rows: object | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    rows = _jsonable_rows(result_rows or [])
    payload: dict[str, Any] = {
        "name": str(operation.get("name", "operation")),
        "operation_type": str(operation.get("operation_type", "sql")),
        "status": status,
        "sql": str(operation.get("sql", "")),
        "row_count": len(rows),
        "result_fingerprint": _fingerprint(rows),
        "_comparison_rows": rows,
    }
    if rows and len(rows) <= 20:
        payload["result_rows"] = rows
    elif rows:
        payload["result_truncated"] = True
    if error:
        payload["error"] = error
    return payload


def _public_result(result: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): value for key, value in result.items() if not str(key).startswith("_")}


def _canonical_rows(rows: object) -> str:
    return _fingerprint(_jsonable_rows(rows))


def _jsonable_rows(rows: object) -> list[Any]:
    if rows is None:
        return []
    if not isinstance(rows, list | tuple):
        return [_jsonable(rows)]
    return [_jsonable(row) for row in rows]


def _jsonable(value: object) -> Any:
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if value is None or isinstance(value, str | int | float | bool):
        return value
    return str(value)


def _read_json_object(path: str | Path) -> dict[str, Any]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain a JSON object")
    return raw


def _target_sink_type(target: Any) -> str:
    if isinstance(target, Mapping):
        return str(target.get("sink_type", ""))
    return str(getattr(target, "sink_type", ""))


def _fingerprint(payload: Any) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


__all__ = [
    "MigrationExecutionReport",
    "MigrationOperationExecutor",
    "ddl_operations",
    "execute_migration_operations",
    "load_migration_operation_executor",
    "phase_operations",
]
