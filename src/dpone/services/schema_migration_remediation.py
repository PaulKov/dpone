"""File-IO facade for schema migration remediation commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import yaml

from dpone.readiness.schema_migration_remediation import (
    MigrationRemediationCertifier,
    MigrationRemediationPlanner,
    MigrationRemediationRunner,
)
from dpone.readiness.schema_migration_remediation_rendering import render_remediation_markdown
from dpone.services.schema_migration_execution import load_migration_operation_executor


class MigrationRemediationFacade:
    """Thin application facade used by CLI and CI jobs."""

    def plan(
        self,
        *,
        pack_path: str,
        watch_certificate_path: str,
        ledger_path: str,
        manifest_path: str,
        target_connection_path: str,
        environment: str,
        backup_certificate_path: str | None = None,
    ) -> dict[str, Any]:
        connection = _read_mapping(target_connection_path)
        connection["path"] = target_connection_path
        payload = MigrationRemediationPlanner().plan(
            pack=_read_mapping(pack_path),
            watch_certificate=_read_mapping(watch_certificate_path),
            ledger=_read_mapping(ledger_path),
            manifest=_read_mapping(manifest_path),
            target_connection=connection,
            environment=environment,
            backup_certificate=_read_optional(backup_certificate_path),
        )
        payload["ledger_path"] = ledger_path
        return payload

    def apply(self, *, plan_path: str, approval_path: str | None = None, execute: bool = False) -> dict[str, Any]:
        plan = _read_mapping(plan_path)
        executor = (
            load_migration_operation_executor(
                target=plan.get("target", {}),
                target_connection_path=_connection_path(plan),
            )
            if execute
            else None
        )
        try:
            return MigrationRemediationRunner().run(
                plan=plan,
                executor=executor,
                execute=execute,
                approval=_read_optional(approval_path),
            )
        finally:
            close = getattr(executor, "close", None)
            if callable(close):
                close()

    def certify(
        self, *, run_path: str, target_connection_path: str | None = None, profile: str = "stage"
    ) -> dict[str, Any]:
        del target_connection_path
        return MigrationRemediationCertifier().certify(run=_read_mapping(run_path), profile=profile)

    def report(self, *, certificate_path: str) -> dict[str, Any]:
        certificate = _read_mapping(certificate_path)
        return {**certificate, "markdown": render_remediation_markdown(certificate)}


def _connection_path(plan: dict[str, Any]) -> str | None:
    connection = plan.get("target_connection", {})
    if isinstance(connection, dict) and connection.get("path"):
        return str(connection["path"])
    return None


def _read_optional(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(path) if path else None


def _read_mapping(path: str | Path) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    raw = json.loads(text) if str(path).lower().endswith(".json") else yaml.safe_load(text)
    if not isinstance(raw, dict):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


__all__ = ["MigrationRemediationFacade"]
