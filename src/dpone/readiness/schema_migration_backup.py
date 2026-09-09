"""Provider-neutral backup, restore rehearsal, and certification contracts."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from dpone.readiness.migration_control import MigrationPack, stable_fingerprint
from dpone.readiness.schema_migration_backup_ports import TargetBackupDialect, restore_operation
from dpone.readiness.schema_migration_backup_support import (
    BACKUP_PLAN_SCHEMA,
    backup_options,
    backup_recovery_metadata,
    destination,
    preconditions,
    profile,
    require_for,
    requirement_reasons,
    restore_rehearsal_options,
    restore_table_name,
    retention_options,
    sink_type,
    strategy,
    target_blockers,
    target_connection_public,
)


@dataclass(frozen=True, slots=True)
class MigrationBackupPlanner:
    dialect: TargetBackupDialect | None = None

    def plan(
        self,
        *,
        pack: Mapping[str, Any],
        manifest: Mapping[str, Any] | None,
        target_connection: Mapping[str, Any],
        environment: str,
        base_restore_point: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        migration_pack = MigrationPack.from_mapping(dict(pack))
        options = backup_options(manifest)
        if not bool(options.get("enabled", False)):
            return _disabled_plan(migration_pack, target_connection, environment)
        reasons = list(requirement_reasons(migration_pack, require_for(options)))
        blockers = [
            *(["schema_migration_backup.pack_blocked"] if migration_pack.blockers else []),
            *target_blockers(migration_pack, target_connection),
            *(
                ["schema_migration_backup.dialect_required"]
                if sink_type(migration_pack, target_connection) == "clickhouse" and self.dialect is None
                else []
            ),
        ]
        selected_strategy = strategy(options)
        required = bool(reasons)
        recovery = backup_recovery_metadata(options=options, base_restore_point=base_restore_point)
        if required and selected_strategy in {"none", "manual"} and str(options.get("mode") or "gate") == "gate":
            blockers.append(f"schema_migration_backup.strategy_not_executable:{selected_strategy}")
        backup_destination = destination(options, migration_pack)
        operations = (
            []
            if blockers or selected_strategy != "target_native"
            else [_backup_operation(migration_pack, backup_destination, self.dialect, recovery)]
        )
        restore_table = restore_table_name(migration_pack.target.table, migration_pack.pack_id)
        remediation_operations = (
            []
            if blockers or selected_strategy != "target_native"
            else _restore_operations(migration_pack, backup_destination, restore_table, self.dialect)
        )
        payload: dict[str, Any] = {
            "schema_version": BACKUP_PLAN_SCHEMA,
            "status": "blocked" if blockers else "planned",
            "pack_id": migration_pack.pack_id,
            "environment": environment,
            "target": migration_pack.target.to_dict(),
            "target_connection": target_connection_public(target_connection),
            "profile": profile(options),
            "mode": str(options.get("mode") or "gate"),
            "strategy": selected_strategy,
            "required": required,
            "requirement_reasons": reasons,
            "backup_destination": backup_destination,
            "backup_kind": recovery["backup_kind"],
            "base_restore_point_id": recovery["base_restore_point_id"],
            "base_backup_destination": recovery["base_backup_destination"],
            "chain_id": recovery["chain_id"],
            "chain_depth": recovery["chain_depth"],
            "rpo_seconds": recovery["rpo_seconds"],
            "restore_table": restore_table,
            "operations": operations,
            "remediation_operations": remediation_operations,
            "preconditions": preconditions(options),
            "retention": retention_options(options),
            "restore_rehearsal": restore_rehearsal_options(options),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(migration_pack.warnings),
        }
        payload["backup_plan_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "environment": environment,
                "strategy": selected_strategy,
                "required": required,
                "reasons": reasons,
                "destination": backup_destination,
                "recovery": recovery,
                "operations": operations,
                "blockers": payload["blockers"],
            }
        )
        return payload


def _disabled_plan(pack: MigrationPack, target_connection: Mapping[str, Any], environment: str) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": BACKUP_PLAN_SCHEMA,
        "status": "disabled",
        "pack_id": pack.pack_id,
        "environment": environment,
        "target": pack.target.to_dict(),
        "target_connection": target_connection_public(target_connection),
        "required": False,
        "requirement_reasons": [],
        "operations": [],
        "remediation_operations": [],
        "preconditions": {},
        "blockers": [],
        "warnings": ["schema_migration_backup.disabled"],
    }
    payload["backup_plan_id"] = stable_fingerprint(payload)
    return payload


def _backup_operation(
    pack: MigrationPack,
    backup_destination: str,
    dialect: TargetBackupDialect | None,
    recovery: Mapping[str, Any],
) -> dict[str, Any]:
    settings = {}
    if recovery.get("backup_kind") == "incremental" and recovery.get("base_backup_destination"):
        settings["base_backup"] = {"raw": recovery["base_backup_destination"]}
    sql = (
        dialect.render_backup_table(table=pack.target.table, destination=backup_destination, settings=settings)
        if dialect
        else ""
    )
    return {"name": "backup_table", "operation_type": "sql", "sql": sql, "backup_destination": backup_destination}


def _restore_operations(
    pack: MigrationPack,
    backup_destination: str,
    restored_table: str,
    dialect: TargetBackupDialect | None,
) -> list[dict[str, Any]]:
    return [
        restore_operation(pack.target.table, restored_table, backup_destination, dialect),
        {
            "name": "exchange_restored_table",
            "operation_type": "sql",
            "sql": f"EXCHANGE TABLES {pack.target.table} AND {restored_table}",
        },
    ]


_EXECUTION_EXPORTS = (
    "BackupOperationExecutor",
    "MigrationBackupCertifier",
    "MigrationBackupRunner",
    "MigrationRestorePlanner",
    "MigrationRestoreRunner",
)


def __getattr__(name: str) -> Any:
    if name in _EXECUTION_EXPORTS:
        module = import_module("dpone.readiness.schema_migration_backup_execution")
        return getattr(module, name)
    raise AttributeError(name)


__all__ = [
    *_EXECUTION_EXPORTS,
    "MigrationBackupPlanner",
    "TargetBackupDialect",
]
