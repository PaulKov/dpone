"""File-IO facade for schema migration backup and restore commands."""

from __future__ import annotations

import json
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.schema_migration_backup import (
    MigrationBackupCertifier,
    MigrationBackupPlanner,
    MigrationBackupRunner,
    MigrationRestorePlanner,
    MigrationRestoreRunner,
)
from dpone.readiness.schema_migration_backup_rendering import render_backup_markdown
from dpone.readiness.schema_migration_backup_support import recovery_catalog_options
from dpone.services.schema_migration_execution import load_migration_operation_executor


class MigrationBackupFacade:
    """Thin application facade used by CLI and CI jobs."""

    def plan(
        self,
        *,
        pack_path: str,
        manifest_path: str,
        target_connection_path: str,
        environment: str,
    ) -> dict[str, Any]:
        connection = _read_mapping(target_connection_path)
        connection["path"] = target_connection_path
        manifest = _read_mapping(manifest_path)
        return MigrationBackupPlanner(dialect=_backup_dialect(connection)).plan(
            pack=_read_mapping(pack_path),
            manifest=manifest,
            target_connection=connection,
            environment=environment,
            base_restore_point=_latest_base_restore_point(manifest, environment),
        )

    def create(self, *, plan_path: str, approval_path: str | None = None, execute: bool = False) -> dict[str, Any]:
        plan = _read_mapping(plan_path)
        executor = _executor(plan, execute)
        try:
            return MigrationBackupRunner().run(
                plan=plan,
                executor=executor,
                execute=execute,
                approval=_read_optional(approval_path),
            )
        finally:
            _close(executor)

    def restore_plan(
        self,
        *,
        backup_run_path: str,
        target_connection_path: str,
        environment: str,
    ) -> dict[str, Any]:
        connection = _read_mapping(target_connection_path)
        connection["path"] = target_connection_path
        return MigrationRestorePlanner(dialect=_backup_dialect(connection)).plan(
            backup_run=_read_mapping(backup_run_path),
            target_connection=connection,
            environment=environment,
        )

    def restore_run(self, *, plan_path: str, execute: bool = False) -> dict[str, Any]:
        plan = _read_mapping(plan_path)
        executor = _executor(plan, execute)
        try:
            return MigrationRestoreRunner().run(plan=plan, executor=executor, execute=execute)
        finally:
            _close(executor)

    def certify(
        self,
        *,
        backup_run_path: str,
        restore_run_path: str | None = None,
        profile: str = "stage",
    ) -> dict[str, Any]:
        return MigrationBackupCertifier().certify(
            backup_run=_read_mapping(backup_run_path),
            restore_run=_read_optional(restore_run_path),
            profile=profile,
        )

    def report(self, *, certificate_path: str) -> dict[str, Any]:
        certificate = _read_mapping(certificate_path)
        return {**certificate, "markdown": render_backup_markdown(certificate)}


def _executor(plan: dict[str, Any], execute: bool) -> Any:
    if not execute:
        return None
    return load_migration_operation_executor(
        target=plan.get("target", {}),
        target_connection_path=_connection_path(plan),
    )


def _backup_dialect(connection: dict[str, Any]) -> Any:
    sink_type = str(connection.get("type") or connection.get("sink_type") or "").lower()
    if sink_type != "clickhouse":
        return None
    module = import_module("dpone.runtime.sinks.clickhouse_backup")
    return module.ClickHouseBackupDialect()


def _latest_base_restore_point(manifest: dict[str, Any], environment: str) -> dict[str, Any] | None:
    recovery = recovery_catalog_options(manifest)
    incremental = recovery["incremental"]
    if not bool(incremental.get("enabled", False)):
        return None
    target = _manifest_target_key(manifest)
    if not target:
        return None
    catalog = recovery["catalog"]
    store_uri = catalog.get("store_uri")
    if not store_uri:
        return None
    backend = str(catalog.get("store_backend") or "local_json")
    if backend == "sqlite":
        store_module = import_module("dpone.readiness.schema_migration_recovery_sqlite")
        store = store_module.SqliteRecoveryCatalogStore(Path(store_uri))
    else:
        store_module = import_module("dpone.readiness.schema_migration_recovery_store")
        store = store_module.LocalJsonRecoveryCatalogStore(Path(store_uri))
    selector_module = import_module("dpone.readiness.schema_migration_recovery")
    latest = selector_module.RecoveryPointSelector(store).latest(
        target=target,
        environment=environment,
        profile=str(recovery.get("profile") or "stage"),
    )
    return latest if latest.get("status") == "usable" else None


def _manifest_target_key(manifest: dict[str, Any]) -> str | None:
    sink = manifest.get("sink", {})
    source = manifest.get("source", {})
    if not isinstance(sink, dict):
        return None
    sink_type = str(sink.get("type") or sink.get("sink_type") or "clickhouse").lower()
    table = (
        sink.get("table")
        or sink.get("target_table")
        or (sink.get("options", {}) if isinstance(sink.get("options"), dict) else {}).get("table")
        or (source.get("table") if isinstance(source, dict) else None)
    )
    return f"{sink_type}.{table}" if table else None


def _connection_path(plan: dict[str, Any]) -> str | None:
    connection = plan.get("target_connection", {})
    if isinstance(connection, dict) and connection.get("path"):
        return str(connection["path"])
    return None


def _close(executor: Any) -> None:
    close = getattr(executor, "close", None)
    if callable(close):
        close()


def _read_optional(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(path) if path else None


def _read_mapping(path: str | Path) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    raw = json.loads(text) if str(path).lower().endswith(".json") else yaml.safe_load(text)
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


__all__ = ["MigrationBackupFacade"]
