"""Restore planning, execution, and certification for recovery points."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.readiness.migration_control import stable_fingerprint
from dpone.readiness.schema_migration_recovery import (
    PROFILES,
    RESTORE_CERTIFICATE_SCHEMA,
    RESTORE_PLAN_SCHEMA,
    RESTORE_RUN_SCHEMA,
)


class RecoveryOperationExecutor(Protocol):
    def execute(self, operation: dict[str, Any]) -> dict[str, Any]: ...


class TargetRecoveryDialect(Protocol):
    def render_restore_table(
        self,
        *,
        source_table: str,
        restored_table: str,
        destination: str,
        settings: Mapping[str, Any],
    ) -> str: ...


@dataclass(frozen=True, slots=True)
class RecoveryRestorePlanner:
    dialect: TargetRecoveryDialect | None = None

    def plan(
        self,
        *,
        restore_point: Mapping[str, Any],
        chain_verification: Mapping[str, Any],
        target_connection: Mapping[str, Any],
        environment: str,
    ) -> dict[str, Any]:
        blockers = _strings(chain_verification.get("blockers", []))
        if chain_verification.get("status") != "verified":
            blockers.append("schema_migration_recovery.chain_not_verified")
        if str(environment).lower() in {"prod", "production"}:
            blockers.append("schema_migration_recovery.restore_prod_blocked")
        sink = str(target_connection.get("type") or target_connection.get("sink_type") or "").lower()
        if sink != "clickhouse":
            blockers.append(f"schema_migration_recovery.unsupported_target:{sink}")
        if self.dialect is None:
            blockers.append("schema_migration_recovery.dialect_required")
        target = _mapping(restore_point.get("target"))
        source_table = str(target.get("table") or "")
        restored_table = _restore_table_name(source_table, str(restore_point.get("restore_point_id") or ""))
        operations = [] if blockers else [_restore_operation(source_table, restored_table, restore_point, self.dialect)]
        payload: dict[str, Any] = {
            "schema_version": RESTORE_PLAN_SCHEMA,
            "status": "blocked" if blockers else "planned",
            "restore_point_id": restore_point.get("restore_point_id"),
            "chain_verification_id": chain_verification.get("chain_verification_id"),
            "pack_id": restore_point.get("pack_id"),
            "environment": environment,
            "target": target,
            "target_connection": _public_connection(target_connection),
            "restore_table": restored_table,
            "operations": operations,
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": [],
        }
        payload["restore_plan_id"] = stable_fingerprint(payload)
        return payload


@dataclass(frozen=True, slots=True)
class RecoveryRestoreRunner:
    def run(
        self,
        *,
        plan: Mapping[str, Any],
        executor: RecoveryOperationExecutor | None,
        execute: bool = False,
    ) -> dict[str, Any]:
        if plan.get("status") == "blocked":
            return _run_payload(plan, "blocked", execute, blockers=_strings(plan.get("blockers", [])))
        if not execute:
            return _run_payload(plan, "dry_run", False, warnings=("schema_migration_recovery.restore_not_executed",))
        if executor is None:
            return _run_payload(
                plan,
                "blocked",
                True,
                blockers=("schema_migration_recovery.target_executor_required",),
            )
        start = time.perf_counter()
        operations = _execute_operations(_operations(plan), executor)
        blockers = [
            f"schema_migration_recovery.operation_failed:{operation.get('name')}"
            for operation in operations
            if operation.get("status") not in {"executed", "ok", "success"}
        ]
        return _run_payload(
            plan,
            "failed" if blockers else "restored",
            True,
            operations=operations,
            blockers=blockers,
            duration_ms=int((time.perf_counter() - start) * 1000),
        )


@dataclass(frozen=True, slots=True)
class RecoveryRestoreCertifier:
    def certify(self, *, restore_run: Mapping[str, Any], profile: str = "stage") -> dict[str, Any]:
        selected_profile = profile if profile in PROFILES else "stage"
        blockers = _strings(restore_run.get("blockers", []))
        warnings = _strings(restore_run.get("warnings", []))
        if restore_run.get("status") == "dry_run" and selected_profile != "advisory":
            blockers.append("schema_migration_recovery.restore_execution_required")
        if restore_run.get("status") in {"blocked", "failed"}:
            blockers.append("schema_migration_recovery.restore_run_blocked")
        status = "blocked" if blockers else "warning" if warnings else "certified"
        payload: dict[str, Any] = {
            "schema_version": RESTORE_CERTIFICATE_SCHEMA,
            "status": status,
            "profile": selected_profile,
            "pack_id": restore_run.get("pack_id"),
            "restore_point_id": restore_run.get("restore_point_id"),
            "restore_run_id": restore_run.get("restore_run_id"),
            "environment": restore_run.get("environment"),
            "target": _mapping(restore_run.get("target")),
            "checks": [{"name": "restore_run", "status": "passed" if status == "certified" else status}],
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "metrics": _mapping(restore_run.get("metrics")),
        }
        payload["certificate_id"] = stable_fingerprint(payload)
        return payload


def _restore_operation(
    source_table: str,
    restored_table: str,
    restore_point: Mapping[str, Any],
    dialect: TargetRecoveryDialect | None,
) -> dict[str, Any]:
    sql = (
        dialect.render_restore_table(
            source_table=source_table,
            restored_table=restored_table,
            destination=str(restore_point.get("destination") or ""),
            settings={"allow_non_empty_tables": 0},
        )
        if dialect
        else ""
    )
    return {"name": "restore_point", "operation_type": "sql", "sql": sql}


def _restore_table_name(table: str, restore_point_id: str) -> str:
    parts = [part for part in table.split(".") if part]
    name = parts[-1] if parts else "table"
    prefix = ".".join(parts[:-1])
    short = restore_point_id.split(":", 1)[-1][:12]
    restored = f"__dpone_recovery_{name}_{short}"
    return f"{prefix}.{restored}" if prefix else restored


def _run_payload(
    plan: Mapping[str, Any],
    status: str,
    execute: bool,
    *,
    operations: list[dict[str, Any]] | None = None,
    blockers: list[str] | tuple[str, ...] = (),
    warnings: list[str] | tuple[str, ...] = (),
    duration_ms: int = 0,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": RESTORE_RUN_SCHEMA,
        "status": status,
        "executed": execute,
        "restore_plan_id": plan.get("restore_plan_id"),
        "restore_point_id": plan.get("restore_point_id"),
        "pack_id": plan.get("pack_id"),
        "environment": plan.get("environment"),
        "target": _mapping(plan.get("target")),
        "operations": operations if operations is not None else _operations(plan),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "metrics": {"duration_ms": duration_ms, "operations_executed": len(operations or [])},
    }
    payload["restore_run_id"] = stable_fingerprint(payload)
    return payload


def _execute_operations(operations: list[dict[str, Any]], executor: RecoveryOperationExecutor) -> list[dict[str, Any]]:
    executed: list[dict[str, Any]] = []
    for operation in operations:
        result = executor.execute(operation)
        executed.append({**operation, "status": str(result.get("status") or "executed"), "result": dict(result)})
    return executed


def _operations(plan: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = plan.get("operations", [])
    return [dict(item) for item in raw if isinstance(item, Mapping)] if isinstance(raw, list) else []


def _public_connection(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        str(key): value
        for key, value in raw.items()
        if key not in {"password", "token", "secret"} and isinstance(value, str | int | float | bool)
    }


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _strings(raw: object) -> list[str]:
    return [str(item) for item in raw if str(item)] if isinstance(raw, list | tuple) else []


__all__ = [
    "RecoveryOperationExecutor",
    "RecoveryRestoreCertifier",
    "RecoveryRestorePlanner",
    "RecoveryRestoreRunner",
    "TargetRecoveryDialect",
]
