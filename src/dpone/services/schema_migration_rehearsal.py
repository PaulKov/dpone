"""File-IO facade for schema migration rehearsal commands."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from dpone.readiness.migration_control import read_json_object
from dpone.readiness.schema_migration_rehearsal import (
    MigrationRehearsalCertifier,
    MigrationRehearsalPlanner,
    MigrationRehearsalRunner,
    render_rehearsal_markdown,
)
from dpone.services.schema_migration_execution import load_migration_operation_executor


class MigrationRehearsalFacade:
    """Application facade for migration rehearsal artifacts."""

    def plan(
        self,
        *,
        pack_path: str,
        environment: str,
        target_connection_path: str,
        output_dir: str,
        bundle_path: str | None = None,
    ) -> dict[str, Any]:
        target_connection = read_json_object(target_connection_path)
        target_connection["path"] = target_connection_path
        payload = MigrationRehearsalPlanner().plan(
            pack=read_json_object(pack_path),
            bundle=read_json_object(bundle_path) if bundle_path else None,
            environment=environment,
            target_connection=target_connection,
        )
        out_dir = Path(output_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        _write_json(out_dir / "rehearsal-plan.json", payload)
        return payload

    def run(
        self,
        *,
        plan_path: str,
        execute: bool = False,
    ) -> dict[str, Any]:
        plan = read_json_object(plan_path)
        executor = None
        if execute:
            executor = load_migration_operation_executor(
                target=plan.get("target", {}),
                target_connection_path=_connection_path(plan),
            )
        return MigrationRehearsalRunner().run(plan=plan, executor=executor, execute=execute)

    def certify(
        self,
        *,
        run_path: str,
        profile: str = "stage",
        fixture_build_path: str | None = None,
        before_profile_path: str | None = None,
        after_profile_path: str | None = None,
    ) -> dict[str, Any]:
        return MigrationRehearsalCertifier().certify(
            run=read_json_object(run_path),
            profile=profile,
            fixture_build=read_json_object(fixture_build_path) if fixture_build_path else None,
            before_profile=read_json_object(before_profile_path) if before_profile_path else None,
            after_profile=read_json_object(after_profile_path) if after_profile_path else None,
        )

    def report(self, *, certificate_path: str) -> dict[str, Any]:
        certificate = read_json_object(certificate_path)
        return {**certificate, "markdown": render_rehearsal_markdown(certificate)}


def _connection_path(plan: dict[str, Any]) -> str | None:
    target_connection = plan.get("target_connection", {})
    if isinstance(target_connection, dict):
        path = target_connection.get("path")
        return str(path) if path else None
    return None


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


__all__ = ["MigrationRehearsalFacade"]
