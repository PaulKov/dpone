"""Execution and certification services for migration backup contracts."""

from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.readiness.migration_control import MigrationPack, stable_fingerprint
from dpone.readiness.schema_migration_backup_ports import TargetBackupDialect, restore_operation
from dpone.readiness.schema_migration_backup_support import (
    BACKUP_CERTIFICATE_SCHEMA,
    BACKUP_RUN_SCHEMA,
    PROFILES,
    RESTORE_PLAN_SCHEMA,
    RESTORE_RUN_SCHEMA,
    approval_blockers,
    mapping,
    restore_table_name,
    strings,
    target_connection_public,
)


class BackupOperationExecutor(Protocol):
    def execute(self, operation: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class MigrationBackupRunner:
    def run(
        self,
        *,
        plan: Mapping[str, Any],
        executor: BackupOperationExecutor | None,
        execute: bool = False,
        approval: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        if plan.get("status") == "blocked":
            return _backup_run(plan=plan, status="blocked", execute=execute, blockers=strings(plan.get("blockers", [])))
        if not execute:
            return _backup_run(
                plan=plan, status="dry_run", execute=False, warnings=("schema_migration_backup.not_executed",)
            )
        pack = _pack_from_plan(plan)
        blockers = list(approval_blockers(pack, plan, approval))
        if executor is None:
            blockers.append("schema_migration_backup.target_executor_required")
        if blockers:
            return _backup_run(plan=plan, status="blocked", execute=True, blockers=blockers)
        start = time.perf_counter()
        operations = _execute_operations(_operations(plan), executor)
        blockers = _execution_blockers(operations)
        return _backup_run(
            plan=plan,
            status="failed" if blockers else "backed_up",
            execute=True,
            operations=operations,
            blockers=blockers,
            duration_ms=int((time.perf_counter() - start) * 1000),
        )


@dataclass(frozen=True, slots=True)
class MigrationRestorePlanner:
    dialect: TargetBackupDialect | None = None

    def plan(
        self,
        *,
        backup_run: Mapping[str, Any],
        target_connection: Mapping[str, Any],
        environment: str,
    ) -> dict[str, Any]:
        blockers = list(strings(backup_run.get("blockers", [])))
        if backup_run.get("status") not in {"backed_up", "dry_run"}:
            blockers.append("schema_migration_backup.backup_run_not_usable")
        if str(environment).lower() in {"prod", "production"}:
            blockers.append("schema_migration_backup.restore_prod_blocked")
        target = mapping(backup_run.get("target"))
        sink = str(
            target_connection.get("type") or target_connection.get("sink_type") or target.get("sink_type")
        ).lower()
        if sink != "clickhouse":
            blockers.append("schema_migration_backup.restore_unsupported_target")
        if self.dialect is None:
            blockers.append("schema_migration_backup.dialect_required")
        source_table = str(target.get("table") or "")
        restored_table = str(
            backup_run.get("restore_table") or restore_table_name(source_table, str(backup_run.get("pack_id")))
        )
        destination_value = str(backup_run.get("backup_destination") or "")
        operations = (
            [] if blockers else [restore_operation(source_table, restored_table, destination_value, self.dialect)]
        )
        payload: dict[str, Any] = {
            "schema_version": RESTORE_PLAN_SCHEMA,
            "status": "blocked" if blockers else "planned",
            "pack_id": backup_run.get("pack_id"),
            "backup_run_id": backup_run.get("backup_run_id"),
            "environment": environment,
            "target": target,
            "target_connection": target_connection_public(target_connection),
            "backup_destination": destination_value,
            "restore_table": restored_table,
            "operations": operations,
            "preconditions": {
                "require_clean_target": mapping(backup_run.get("restore_rehearsal")).get("require_clean_target", True)
            },
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": [],
        }
        payload["restore_plan_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "backup_run_id": payload["backup_run_id"],
                "environment": environment,
                "restore_table": restored_table,
                "operations": operations,
                "blockers": payload["blockers"],
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class MigrationRestoreRunner:
    def run(
        self,
        *,
        plan: Mapping[str, Any],
        executor: BackupOperationExecutor | None,
        execute: bool = False,
    ) -> dict[str, Any]:
        if plan.get("status") == "blocked":
            return _restore_run(
                plan=plan, status="blocked", execute=execute, blockers=strings(plan.get("blockers", []))
            )
        if not execute:
            return _restore_run(
                plan=plan, status="dry_run", execute=False, warnings=("schema_migration_backup.restore_not_executed",)
            )
        if executor is None:
            return _restore_run(
                plan=plan,
                status="blocked",
                execute=True,
                blockers=("schema_migration_backup.target_executor_required",),
            )
        start = time.perf_counter()
        operations = _execute_operations(_operations(plan), executor)
        blockers = _execution_blockers(operations)
        return _restore_run(
            plan=plan,
            status="failed" if blockers else "restored",
            execute=True,
            operations=operations,
            blockers=blockers,
            duration_ms=int((time.perf_counter() - start) * 1000),
        )


@dataclass(frozen=True, slots=True)
class MigrationBackupCertifier:
    def certify(
        self,
        *,
        backup_run: Mapping[str, Any],
        restore_run: Mapping[str, Any] | None = None,
        profile: str = "stage",
    ) -> dict[str, Any]:
        selected_profile = profile if profile in PROFILES else "stage"
        blockers = list(strings(backup_run.get("blockers", [])))
        warnings = list(strings(backup_run.get("warnings", [])))
        if backup_run.get("status") == "dry_run":
            if selected_profile == "advisory":
                warnings.append("schema_migration_backup.dry_run_certificate")
            else:
                blockers.append("schema_migration_backup.execution_required")
        if backup_run.get("status") in {"blocked", "failed"}:
            blockers.append("schema_migration_backup.backup_run_blocked")
        restore = restore_run or {}
        if selected_profile in {"prod_strict", "regulated"} and restore.get("status") != "restored":
            blockers.append("schema_migration_backup.restore_rehearsal_required")
        if restore:
            warnings.extend(strings(restore.get("warnings", [])))
            blockers.extend(strings(restore.get("blockers", [])))
        status = "blocked" if blockers else "warning" if warnings else "certified"
        payload: dict[str, Any] = {
            "schema_version": BACKUP_CERTIFICATE_SCHEMA,
            "status": status,
            "profile": selected_profile,
            "pack_id": backup_run.get("pack_id"),
            "environment": backup_run.get("environment"),
            "target": mapping(backup_run.get("target")),
            "backup_run_id": backup_run.get("backup_run_id"),
            "restore_run_id": restore.get("restore_run_id") if restore else None,
            "backup_destination": backup_run.get("backup_destination"),
            "backup_kind": backup_run.get("backup_kind", "full"),
            "base_restore_point_id": backup_run.get("base_restore_point_id"),
            "base_backup_destination": backup_run.get("base_backup_destination"),
            "chain_id": backup_run.get("chain_id"),
            "chain_depth": backup_run.get("chain_depth", 0),
            "rpo_seconds": backup_run.get("rpo_seconds", 0),
            "created_at": backup_run.get("created_at"),
            "valid_until": backup_run.get("valid_until"),
            "retention": mapping(backup_run.get("retention")),
            "restore_rehearsal": mapping(backup_run.get("restore_rehearsal")),
            "restore_table": backup_run.get("restore_table"),
            "remediation": mapping(backup_run.get("remediation")),
            "checks": _certificate_checks(backup_run, restore, status),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "metrics": _certificate_metrics(backup_run, restore),
        }
        payload["certificate_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "backup_run_id": payload["backup_run_id"],
                "restore_run_id": payload["restore_run_id"],
                "profile": selected_profile,
                "status": status,
                "blockers": payload["blockers"],
                "warnings": payload["warnings"],
            }
        )
        return payload


def _pack_from_plan(plan: Mapping[str, Any]) -> MigrationPack:
    target = mapping(plan.get("target"))
    return MigrationPack.from_mapping(
        {
            "pack_id": plan.get("pack_id"),
            "target": target,
            "desired_fingerprint": "sha256:" + "0" * 64,
            "desired": target,
        }
    )


def _operations(payload: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    raw = payload.get("operations", [])
    return tuple(dict(item) for item in raw if isinstance(item, Mapping)) if isinstance(raw, list) else ()


def _execute_operations(
    operations: tuple[dict[str, Any], ...], executor: BackupOperationExecutor | None
) -> list[dict[str, Any]]:
    executed: list[dict[str, Any]] = []
    for operation in operations:
        try:
            result = executor.execute(operation) if executor else {"status": "blocked"}
            status = str(result.get("status") or "executed") if isinstance(result, Mapping) else "executed"
            executed.append(
                {**operation, "status": status, "result": dict(result) if isinstance(result, Mapping) else {}}
            )
        except Exception as exc:  # pragma: no cover - defensive executor boundary
            executed.append({**operation, "status": "failed", "error": str(exc)})
    return executed


def _execution_blockers(operations: list[dict[str, Any]]) -> list[str]:
    return [
        f"schema_migration_backup.operation_failed:{operation.get('name')}"
        for operation in operations
        if operation.get("status") not in {"executed", "ok", "success"}
    ]


def _backup_run(
    *,
    plan: Mapping[str, Any],
    status: str,
    execute: bool,
    operations: list[dict[str, Any]] | None = None,
    blockers: list[str] | tuple[str, ...] = (),
    warnings: list[str] | tuple[str, ...] = (),
    duration_ms: int = 0,
) -> dict[str, Any]:
    payload = _run_base(BACKUP_RUN_SCHEMA, plan, status, execute, operations, blockers, warnings, duration_ms)
    payload["backup_destination"] = plan.get("backup_destination")
    payload["backup_kind"] = plan.get("backup_kind", "full")
    payload["base_restore_point_id"] = plan.get("base_restore_point_id")
    payload["base_backup_destination"] = plan.get("base_backup_destination")
    payload["chain_id"] = plan.get("chain_id")
    payload["chain_depth"] = plan.get("chain_depth", 0)
    payload["rpo_seconds"] = plan.get("rpo_seconds", 0)
    payload["retention"] = mapping(plan.get("retention"))
    payload["restore_table"] = plan.get("restore_table")
    payload["restore_rehearsal"] = mapping(plan.get("restore_rehearsal"))
    payload["remediation"] = {
        "capability": "restore_from_backup",
        "operations": list(plan.get("remediation_operations", [])),
    }
    payload["backup_run_id"] = stable_fingerprint(payload)
    return payload


def _restore_run(**kwargs: Any) -> dict[str, Any]:
    payload = _run_base(
        RESTORE_RUN_SCHEMA,
        kwargs["plan"],
        kwargs["status"],
        kwargs["execute"],
        kwargs.get("operations"),
        kwargs.get("blockers", ()),
        kwargs.get("warnings", ()),
        kwargs.get("duration_ms", 0),
    )
    payload["backup_run_id"] = kwargs["plan"].get("backup_run_id")
    payload["backup_destination"] = kwargs["plan"].get("backup_destination")
    payload["restore_table"] = kwargs["plan"].get("restore_table")
    payload["restore_run_id"] = stable_fingerprint(payload)
    return payload


def _run_base(
    schema: str,
    plan: Mapping[str, Any],
    status: str,
    execute: bool,
    operations: list[dict[str, Any]] | None,
    blockers: list[str] | tuple[str, ...],
    warnings: list[str] | tuple[str, ...],
    duration_ms: int,
) -> dict[str, Any]:
    return {
        "schema_version": schema,
        "status": status,
        "pack_id": plan.get("pack_id"),
        "environment": plan.get("environment"),
        "target": mapping(plan.get("target")),
        "executed": execute,
        "operations": operations if operations is not None else list(plan.get("operations", [])),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "metrics": {"duration_ms": duration_ms, "operations_executed": len(operations or [])},
        "backup_plan_id": plan.get("backup_plan_id"),
        "restore_plan_id": plan.get("restore_plan_id"),
    }


def _certificate_checks(
    backup_run: Mapping[str, Any], restore_run: Mapping[str, Any], status: str
) -> list[dict[str, Any]]:
    return [
        {"name": "backup_run", "status": "passed" if backup_run.get("status") == "backed_up" else status},
        {"name": "restore_rehearsal", "status": "passed" if restore_run.get("status") == "restored" else "warning"},
    ]


def _certificate_metrics(backup_run: Mapping[str, Any], restore_run: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "backup_duration_ms": mapping(backup_run.get("metrics")).get("duration_ms", 0),
        "restore_duration_ms": mapping(restore_run.get("metrics")).get("duration_ms", 0) if restore_run else 0,
    }


__all__ = [
    "BackupOperationExecutor",
    "MigrationBackupCertifier",
    "MigrationBackupRunner",
    "MigrationRestorePlanner",
    "MigrationRestoreRunner",
]
