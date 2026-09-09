"""Execution evidence helpers for schema migration rehearsal runs."""

from __future__ import annotations

import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.readiness.migration_control import stable_fingerprint

REHEARSAL_RUN_SCHEMA = "dpone.schema_migration_rehearsal_run.v1"


class RehearsalOperationExecutor(Protocol):
    def execute(self, operation: dict[str, Any]) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class MigrationRehearsalRunner:
    """Runs rehearsal operations through an injected target execution port."""

    def run(
        self,
        *,
        plan: Mapping[str, Any],
        executor: RehearsalOperationExecutor | None,
        execute: bool = False,
    ) -> dict[str, Any]:
        if plan.get("status") == "blocked":
            return _run_payload(plan, status="blocked", execute=execute, blockers=_strings(plan.get("blockers", [])))
        if not execute:
            return _run_payload(
                plan,
                status="dry_run",
                execute=False,
                warnings=("schema_migration_rehearsal.not_executed",),
            )
        if executor is None:
            return _run_payload(
                plan,
                status="blocked",
                execute=True,
                blockers=("schema_migration_rehearsal.target_executor_required",),
            )
        start = time.perf_counter()
        operations = _execute_operations(_operation_list(plan.get("operations", [])), executor)
        rollback_operations = _execute_operations(_operation_list(plan.get("rollback_operations", [])), executor)
        blockers = _execution_blockers(operations + rollback_operations)
        return _run_payload(
            plan,
            status="blocked" if blockers else "passed",
            execute=True,
            blockers=blockers,
            operations=operations,
            rollback_operations=rollback_operations,
            duration_ms=int((time.perf_counter() - start) * 1000),
        )


def _execute_operations(
    operations: Sequence[dict[str, Any]],
    executor: RehearsalOperationExecutor,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for operation in operations:
        try:
            result = executor.execute(dict(operation))
            rows = _jsonable_rows(result.get("result_rows", []))
            payload = _operation_result(operation, status=str(result.get("status", "executed")), rows=rows)
        except Exception as exc:  # pragma: no cover - driver-specific surface
            payload = _operation_result(operation, status="failed", rows=[], error=str(exc))
        results.append(payload)
    return results


def _execution_blockers(results: Sequence[Mapping[str, Any]]) -> tuple[str, ...]:
    blockers = [f"migration.operation_failed:{item.get('name')}" for item in results if item.get("status") == "failed"]
    validations = [
        item for item in results if item.get("operation_type") == "validation" and item.get("status") != "failed"
    ]
    index = 0
    while index + 1 < len(validations):
        left = validations[index]
        right = validations[index + 1]
        if left.get("result_fingerprint") != right.get("result_fingerprint"):
            blockers.append(f"migration.validation_mismatch:{left.get('name')}:{right.get('name')}")
        index += 2
    if index < len(validations) and validations[index].get("row_count"):
        blockers.append(f"migration.validation_not_empty:{validations[index].get('name')}")
    return tuple(dict.fromkeys(blockers))


def _run_payload(
    plan: Mapping[str, Any],
    *,
    status: str,
    execute: bool,
    blockers: Sequence[str] = (),
    warnings: Sequence[str] = (),
    operations: Sequence[Mapping[str, Any]] = (),
    rollback_operations: Sequence[Mapping[str, Any]] = (),
    duration_ms: int = 0,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": REHEARSAL_RUN_SCHEMA,
        "status": status,
        "executed": execute,
        "rehearsal_plan_id": plan.get("rehearsal_plan_id"),
        "pack_id": plan.get("pack_id"),
        "bundle_id": plan.get("bundle_id"),
        "environment": plan.get("environment"),
        "target": dict(plan.get("target", {})) if isinstance(plan.get("target"), Mapping) else {},
        "operations": list(operations),
        "rollback_operations": list(rollback_operations),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "metrics": {
            "duration_ms": duration_ms,
            "rows_validated": sum(
                int(item.get("row_count", 0)) for item in operations if item.get("operation_type") == "validation"
            ),
            "operations_executed": len(operations) + len(rollback_operations),
        },
    }
    payload["rehearsal_run_id"] = stable_fingerprint(
        {
            "plan_id": payload["rehearsal_plan_id"],
            "status": status,
            "executed": execute,
            "operations": list(operations),
            "rollback_operations": list(rollback_operations),
            "blockers": payload["blockers"],
            "warnings": payload["warnings"],
        }
    )
    return payload


def _operation_list(raw: object) -> list[dict[str, Any]]:
    return [dict(item) for item in raw if isinstance(item, Mapping)] if isinstance(raw, list) else []


def _operation_result(
    operation: Mapping[str, Any],
    *,
    status: str,
    rows: list[Any],
    error: str | None = None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "name": str(operation.get("name", "operation")),
        "phase": operation.get("phase"),
        "operation_type": str(operation.get("operation_type", "sql")),
        "status": status,
        "sql": str(operation.get("sql", "")),
        "row_count": len(rows),
        "result_fingerprint": _fingerprint(rows),
    }
    if rows and len(rows) <= 20:
        payload["result_rows"] = rows
    elif rows:
        payload["result_truncated"] = True
    if error:
        payload["error"] = error
    return {key: value for key, value in payload.items() if value is not None}


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


def _strings(raw: object) -> list[str]:
    return [str(item) for item in raw if str(item)] if isinstance(raw, list | tuple) else []


def _fingerprint(payload: Any) -> str:
    canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


__all__ = ["MigrationRehearsalRunner", "REHEARSAL_RUN_SCHEMA", "RehearsalOperationExecutor"]
