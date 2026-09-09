"""File-IO facade for schema migration recovery catalog commands."""

from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml

from dpone.services.schema_migration_execution import load_migration_operation_executor


class MigrationRecoveryFacade:
    def point_record(
        self,
        *,
        backup_certificate_path: str,
        environment: str,
        store_backend: str,
        store_uri: str | None,
        mode: str = "gate",
    ) -> dict[str, Any]:
        recovery = _recovery_core()
        point = recovery.RecoveryPointRecorder().record(
            backup_certificate=_read_mapping(backup_certificate_path),
            environment=environment,
            mode=mode,
        )
        _store(store_backend, store_uri).append(point)
        return point

    def point_latest(
        self,
        *,
        target: str,
        environment: str,
        profile: str,
        store_backend: str,
        store_uri: str | None,
    ) -> dict[str, Any]:
        return (
            _recovery_core()
            .RecoveryPointSelector(_store(store_backend, store_uri))
            .latest(
                target=target,
                environment=environment,
                profile=profile,
            )
        )

    def point_list(
        self,
        *,
        target: str | None,
        environment: str | None,
        store_backend: str,
        store_uri: str | None,
    ) -> dict[str, Any]:
        points = _store(store_backend, store_uri).query(target=target, environment=environment)
        return {"schema_version": "dpone.schema_migration_recovery_catalog.v1", "restore_points": list(points)}

    def chain_verify(
        self,
        *,
        restore_point_id: str,
        store_backend: str,
        store_uri: str | None,
        require_restore_rehearsal: bool = False,
    ) -> dict[str, Any]:
        return (
            _recovery_core()
            .RecoveryChainVerifier(_store(store_backend, store_uri))
            .verify(
                restore_point_id=restore_point_id,
                require_restore_rehearsal=require_restore_rehearsal,
            )
        )

    def restore_plan(
        self,
        *,
        restore_point_id: str,
        target_connection_path: str,
        environment: str,
        store_backend: str,
        store_uri: str | None,
    ) -> dict[str, Any]:
        store = _store(store_backend, store_uri)
        point = store.get(restore_point_id)
        if point is None:
            return _missing_point(restore_point_id, environment)
        recovery = _recovery_core()
        chain = recovery.RecoveryChainVerifier(store).verify(restore_point_id=restore_point_id)
        connection = _read_mapping(target_connection_path)
        connection["path"] = target_connection_path
        return recovery.RecoveryRestorePlanner(dialect=_recovery_dialect(connection)).plan(
            restore_point=point,
            chain_verification=chain,
            target_connection=connection,
            environment=environment,
        )

    def restore_run(self, *, plan_path: str, execute: bool = False) -> dict[str, Any]:
        plan = _read_mapping(plan_path)
        executor = _executor(plan, execute)
        try:
            return _recovery_core().RecoveryRestoreRunner().run(plan=plan, executor=executor, execute=execute)
        finally:
            _close(executor)

    def restore_certify(self, *, restore_run_path: str, profile: str) -> dict[str, Any]:
        return (
            _recovery_core()
            .RecoveryRestoreCertifier()
            .certify(
                restore_run=_read_mapping(restore_run_path),
                profile=profile,
            )
        )

    def restore_report(self, *, certificate_path: str) -> dict[str, Any]:
        return _read_mapping(certificate_path)

    def retention_plan(
        self,
        *,
        target: str,
        environment: str,
        store_backend: str,
        store_uri: str | None,
    ) -> dict[str, Any]:
        return (
            _recovery_core()
            .RecoveryRetentionPlanner(_store(store_backend, store_uri))
            .plan(
                target=target,
                environment=environment,
            )
        )


def _store(backend: str, uri: str | None) -> Any:
    path = Path(uri or ".dpone/schema-migration/recovery/registry.json")
    if backend == "sqlite":
        return import_module("dpone.readiness.schema_migration_recovery_sqlite").SqliteRecoveryCatalogStore(path)
    return import_module("dpone.readiness.schema_migration_recovery_store").LocalJsonRecoveryCatalogStore(path)


def _recovery_core() -> Any:
    return import_module("dpone.readiness.schema_migration_recovery")


def _executor(plan: dict[str, Any], execute: bool) -> Any:
    if not execute:
        return None
    return load_migration_operation_executor(
        target=plan.get("target", {}),
        target_connection_path=_connection_path(plan),
    )


def _recovery_dialect(connection: dict[str, Any]) -> Any:
    sink_type = str(connection.get("type") or connection.get("sink_type") or "").lower()
    if sink_type != "clickhouse":
        return None
    module = import_module("dpone.runtime.sinks.clickhouse_recovery")
    return module.ClickHouseRecoveryDialect()


def _connection_path(plan: dict[str, Any]) -> str | None:
    connection = plan.get("target_connection", {})
    if isinstance(connection, dict) and connection.get("path"):
        return str(connection["path"])
    return None


def _missing_point(restore_point_id: str, environment: str) -> dict[str, Any]:
    return {
        "schema_version": "dpone.schema_migration_recovery_restore_plan.v1",
        "status": "blocked",
        "restore_point_id": restore_point_id,
        "pack_id": None,
        "environment": environment,
        "target": {},
        "operations": [],
        "blockers": ["schema_migration_recovery.restore_point_not_found"],
        "warnings": [],
    }


def _close(executor: Any) -> None:
    close = getattr(executor, "close", None)
    if callable(close):
        close()


def _read_mapping(path: str | Path) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    raw = json.loads(text) if str(path).lower().endswith(".json") else yaml.safe_load(text)
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


__all__ = ["MigrationRecoveryFacade"]
