"""Safe authoring-source fixes for beginner Airflow self-service migrations."""

from __future__ import annotations

import copy
import re
from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.authoring_migration_io import (
    AuthoringMigrationFileSystem,
    AuthoringMigrationIoError,
)
from dpone.readiness.airflow_authoring_migration_plan import airflow_authoring_migration_plan
from dpone.readiness.airflow_pipeline_source import (
    bounded_redacted_diff,
)
from dpone.readiness.airflow_pipeline_source_reader import PipelineSourcePathError, resolve_pipeline_path
from dpone.readiness.airflow_self_service_models import Change, SelfServiceResult, dpone_error

_SECTION_PATH_RE = re.compile(r"^processes\[(?P<index>\d+)]\.(?P<side>source|sink)$")


class AirflowAuthoringFixService:
    """Apply only safe, source-owned Airflow authoring migrations."""

    def __init__(
        self,
        *,
        root: str | Path = ".",
        filesystem: AuthoringMigrationFileSystem | None = None,
    ) -> None:
        self._filesystem = filesystem or AuthoringMigrationFileSystem(root)
        self._root = self._filesystem.root

    def fix(self, target: str | Path, *, apply: bool) -> SelfServiceResult:
        try:
            snapshot = self._filesystem.read_source(target)
        except AuthoringMigrationIoError as exc:
            return _source_read_failure(
                exc,
                apply=apply,
                target=target,
                root=self._root,
            )
        source_path = snapshot.path
        payload = copy.deepcopy(snapshot.payload)

        plan = airflow_authoring_migration_plan(payload=payload, source_path=source_path, root=self._root)
        if plan is None:
            return SelfServiceResult(
                passed=True,
                changes=(),
                details=_fix_details(
                    apply=apply,
                    applied=False,
                    source_path=source_path,
                    root=self._root,
                    legacy_sections=0,
                    applied_sections=0,
                    plan=None,
                ),
            )

        plan = copy.deepcopy(plan)
        plan["summary"]["apply_supported"] = True
        _bound_plan_diffs(plan)
        changes = _planned_changes(plan)
        applied_sections = 0
        if apply:
            applied_sections = _apply_plan(payload, plan)
            try:
                self._filesystem.apply(
                    source=snapshot,
                    desired_source=_yaml_bytes(payload),
                    fragment_relative=None,
                    desired_fragment=None,
                )
            except AuthoringMigrationIoError as exc:
                return SelfServiceResult(
                    passed=False,
                    changes=changes,
                    errors=(_fix_io_error(exc, target=target),),
                    details=_fix_details(
                        apply=True,
                        applied=False,
                        source_path=source_path,
                        root=self._root,
                        legacy_sections=len(plan["changes"]),
                        applied_sections=0,
                        plan=plan,
                    ),
                    exit_code=4,
                )
            changes = tuple(
                Change(
                    action="modify",
                    path=change.path,
                    message="Applied legacy Airflow authoring migration to the pipeline source.",
                    diff=change.diff,
                )
                for change in changes
            )

        return SelfServiceResult(
            passed=True,
            changes=changes,
            details=_fix_details(
                apply=apply,
                applied=apply and applied_sections > 0,
                source_path=source_path,
                root=self._root,
                legacy_sections=len(plan["changes"]),
                applied_sections=applied_sections,
                plan=plan,
            ),
        )


def _source_read_failure(
    error: AuthoringMigrationIoError,
    *,
    apply: bool,
    target: str | Path,
    root: Path,
) -> SelfServiceResult:
    if error.code == "DPONE_AUTHORING_MIGRATION_PATH_INVALID" and _is_missing_target(
        root,
        target,
    ):
        return SelfServiceResult(
            passed=False,
            errors=(
                dpone_error(
                    "DPONE_PIPELINE_SOURCE_NOT_FOUND",
                    "Pipeline source was not found",
                    stage="fix",
                    entity={"kind": "pipeline", "id": str(target)},
                ),
            ),
        )
    return SelfServiceResult(
        passed=False,
        errors=(_fix_io_error(error, target=target),),
        details={
            "kind": "dpone.airflow-authoring-fix.v1",
            "schema": "dpone.airflow-authoring-fix.v1",
            "mode": "apply" if apply else "plan",
            "apply": apply,
            "applied": False,
        },
        exit_code=4,
    )


def _fix_io_error(
    error: AuthoringMigrationIoError,
    *,
    target: str | Path,
) -> dict[str, Any]:
    extra = {"recovery_path": error.recovery_path} if error.recovery_path is not None else None
    return dpone_error(
        error.code,
        str(error),
        stage="fix",
        entity={"kind": "pipeline", "id": Path(target).name or "pipeline"},
        extra=extra,
    )


def _is_missing_target(root: Path, target: str | Path) -> bool:
    try:
        resolved = resolve_pipeline_path(root, target)
    except PipelineSourcePathError:
        return False
    try:
        resolved.relative_to(root)
    except ValueError:
        return False
    return not resolved.exists() and not resolved.is_symlink()


def _base_details(apply: bool, source_path: Path, root: Path) -> dict[str, Any]:
    return {
        "kind": "dpone.airflow-authoring-fix.v1",
        "schema": "dpone.airflow-authoring-fix.v1",
        "mode": "apply" if apply else "plan",
        "apply": apply,
        "applied": False,
        "target": _relative(source_path, root),
        "summary": {
            "legacy_sections": 0,
            "applied_sections": 0,
            "no_op": True,
        },
    }


def _fix_details(
    *,
    apply: bool,
    applied: bool,
    source_path: Path,
    root: Path,
    legacy_sections: int,
    applied_sections: int,
    plan: dict[str, Any] | None,
) -> dict[str, Any]:
    details = _base_details(apply, source_path, root)
    details["applied"] = applied
    details["summary"] = {
        "legacy_sections": legacy_sections,
        "applied_sections": applied_sections,
        "no_op": legacy_sections == 0,
    }
    if plan is not None:
        details["migration_plan"] = plan
    return details


def _bound_plan_diffs(plan: dict[str, Any]) -> None:
    for change in plan.get("changes", ()):
        if isinstance(change, dict):
            change["unified_diff"] = bounded_redacted_diff(str(change.get("unified_diff", "")))


def _planned_changes(plan: dict[str, Any]) -> tuple[Change, ...]:
    return tuple(
        Change(
            action="plan_modify",
            path=str(change["path"]),
            message="Replace legacy Airflow connection fields with connection_ref.",
            diff=bounded_redacted_diff(str(change.get("unified_diff", ""))),
        )
        for change in plan["changes"]
    )


def _apply_plan(payload: dict[str, Any], plan: dict[str, Any]) -> int:
    applied = 0
    for change in plan["changes"]:
        _replace_section(payload, str(change["path"]), change["proposed_section"])
        applied += 1
    return applied


def _replace_section(payload: dict[str, Any], section_path: str, proposed_section: object) -> None:
    match = _SECTION_PATH_RE.fullmatch(section_path)
    if match is None or not isinstance(proposed_section, dict):
        raise ValueError(f"Unsupported Airflow authoring fix path: {section_path}")
    processes = payload.get("processes")
    if not isinstance(processes, list):
        raise ValueError("Pipeline source no longer contains a processes list")
    index = int(match.group("index"))
    if index >= len(processes) or not isinstance(processes[index], dict):
        raise ValueError(f"Pipeline process does not exist for fix path: {section_path}")
    processes[index][match.group("side")] = copy.deepcopy(proposed_section)


def _yaml_bytes(payload: dict[str, Any]) -> bytes:
    return yaml.safe_dump(payload, sort_keys=False).encode("utf-8")


def _relative(path: Path, root: Path) -> str:
    try:
        return path.relative_to(root).as_posix()
    except ValueError:
        return path.as_posix()


__all__ = ["AirflowAuthoringFixService"]
