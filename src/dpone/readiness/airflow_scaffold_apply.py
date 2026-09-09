"""Confined, compare-and-create helpers for Airflow self-service scaffold."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

import yaml

from dpone.manifest.project_root import ProjectRootIdentity
from dpone.readiness.airflow_pipeline_source import (
    ConcurrentAuthoringCreate,
    ConfinedAuthoringFileSystem,
    ConfinedAuthoringPathError,
    ConfinedFileContent,
    ConfinedFileCreation,
)
from dpone.readiness.airflow_scaffold_diff import scaffold_diff
from dpone.readiness.airflow_self_service_models import Change
from dpone.readiness.workload_init_scaffold_rollback import (
    rollback_scaffold_files,
    scaffold_directory_recovery_entries,
    scaffold_directory_recovery_paths,
    scaffold_rollback_journal,
)

if TYPE_CHECKING:
    from dpone.readiness.airflow_pipeline_source import ConfinedFileRollbackOutcome

ScaffoldPathError = ConfinedAuthoringPathError
ScaffoldConcurrentWrite = ConcurrentAuthoringCreate
_FileContent = ConfinedFileContent
_CreatedFile = ConfinedFileCreation


@dataclass(frozen=True)
class ScaffoldFile:
    path: Path
    text: str

    @classmethod
    def yaml(cls, path: Path, payload: dict[str, Any]) -> ScaffoldFile:
        return cls(path=path, text=yaml.safe_dump(payload, sort_keys=False, allow_unicode=False))

    @property
    def sha256(self) -> str:
        return "sha256:" + hashlib.sha256(self.text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class ScaffoldApplyPlan:
    changes: tuple[Change, ...]
    rollback_journal: dict[str, Any]
    rollback_issues: tuple[str, ...] = ()
    recovery_artifacts: tuple[str, ...] = ()
    apply_failed: bool = False
    created_files: tuple[tuple[int, _CreatedFile], ...] = ()

    @property
    def has_conflict(self) -> bool:
        return any(change.action == "conflict" for change in self.changes)

    @property
    def recovery_required(self) -> bool:
        return bool(self.rollback_issues or self.recovery_artifacts)


class ScaffoldFileSystem:
    def __init__(
        self,
        root: str | Path,
        *,
        root_identity: ProjectRootIdentity | None = None,
    ) -> None:
        self._filesystem = ConfinedAuthoringFileSystem(
            root,
            root_identity=root_identity,
        )

    def read(self, path: Path) -> _FileContent | None:
        return self._filesystem.read(path)

    def create(self, file: ScaffoldFile) -> _CreatedFile | None:
        return self._filesystem.create(file.path, file.text.encode("utf-8"))

    def rollback(self, created: _CreatedFile) -> ConfinedFileRollbackOutcome:
        return self._filesystem.rollback(created)


class ScaffoldApplier:
    """Plan and create generated files without following or overwriting user paths."""

    def __init__(
        self,
        root: str | Path,
        *,
        filesystem: ScaffoldFileSystem | None = None,
        root_identity: ProjectRootIdentity | None = None,
    ) -> None:
        self._filesystem = filesystem or ScaffoldFileSystem(
            root,
            root_identity=root_identity,
        )

    def plan(self, files: tuple[ScaffoldFile, ...]) -> ScaffoldApplyPlan:
        changes = tuple(self._plan_change(file) for file in files)
        return ScaffoldApplyPlan(changes=changes, rollback_journal=scaffold_rollback_journal(()))

    def apply(
        self,
        files: tuple[ScaffoldFile, ...],
        *,
        precondition: Callable[[], bool] | None = None,
        postcondition: Callable[[], bool] | None = None,
    ) -> ScaffoldApplyPlan:
        plan = self.plan(files)
        if plan.has_conflict:
            return ScaffoldApplyPlan(
                changes=_mark_not_applied(plan.changes),
                rollback_journal=plan.rollback_journal,
            )

        created: list[tuple[int, _CreatedFile]] = []
        changes = list(plan.changes)
        if not _condition_passes(precondition):
            return self._condition_failure(changes=changes, created=created)
        for index, (file, change) in enumerate(zip(files, plan.changes, strict=True)):
            if change.action != "create":
                continue
            try:
                outcome = self._filesystem.create(file)
            except ScaffoldConcurrentWrite as exc:
                return self._failure_receipt(
                    changes=changes,
                    created=created,
                    failed_index=index,
                    failure=exc,
                    failed_change=_conflict_change(
                        file,
                        exc.existing,
                        message=str(exc),
                    ),
                )
            except ScaffoldPathError as exc:
                return self._failure_receipt(
                    changes=changes,
                    created=created,
                    failed_index=index,
                    failure=exc,
                    failed_change=_unsafe_change(file, str(exc)),
                )
            except Exception as exc:  # noqa: BLE001 - every compensation must be attempted
                receipt = self._failure_receipt(
                    changes=changes,
                    created=created,
                    failed_index=index,
                    failure=exc,
                    failed_change=Change(
                        "failed",
                        file.path.as_posix(),
                        "Scaffold file could not be created safely.",
                    ),
                )
                setattr(exc, "scaffold_receipt", receipt)
                raise
            if outcome is None:
                changes[index] = Change("no_op", file.path.as_posix())
            else:
                created.append((index, outcome))
        if not _condition_passes(postcondition) or not _condition_passes(lambda: self._planned_files_match(files)):
            return self._condition_failure(changes=changes, created=created)
        return ScaffoldApplyPlan(
            changes=tuple(changes),
            rollback_journal=scaffold_rollback_journal([item.path.as_posix() for _, item in created]),
            created_files=tuple(created),
        )

    def compensate(self, plan: ScaffoldApplyPlan) -> ScaffoldApplyPlan:
        """Roll back files created by a successful apply."""

        if not plan.created_files:
            return plan
        changes = list(plan.changes)
        rollback = rollback_scaffold_files(
            plan.created_files,
            rollback=self._filesystem.rollback,
        )
        for index, change in rollback.receipt_changes:
            if 0 <= index < len(changes):
                changes[index] = change
        return ScaffoldApplyPlan(
            changes=(*changes, *rollback.recovery_changes),
            rollback_journal=scaffold_rollback_journal(
                rollback.unresolved_paths,
                recovery_entries=rollback.recovery_entries,
            ),
            rollback_issues=(*plan.rollback_issues, *rollback.issues),
            recovery_artifacts=tuple(dict.fromkeys((*plan.recovery_artifacts, *rollback.recovery_artifacts))),
            apply_failed=True,
            created_files=(),
        )

    def _planned_files_match(self, files: tuple[ScaffoldFile, ...]) -> bool:
        try:
            return all(
                (current := self._filesystem.read(file.path)) is not None
                and current.complete
                and current.content == file.text.encode("utf-8")
                for file in files
            )
        except ScaffoldPathError:
            return False

    def _plan_change(self, file: ScaffoldFile) -> Change:
        try:
            existing = self._filesystem.read(file.path)
        except ScaffoldPathError as exc:
            return _unsafe_change(file, str(exc))
        if existing is None:
            return Change(
                "create",
                file.path.as_posix(),
                diff=scaffold_diff(
                    "",
                    file.text,
                    path=file.path,
                    existing_complete=True,
                    fromfile="/dev/null",
                    tofile=f"desired/{file.path.as_posix()}",
                ),
            )
        if existing.complete and existing.content == file.text.encode("utf-8"):
            return Change("no_op", file.path.as_posix())
        return _conflict_change(
            file,
            existing,
            message="file exists with different content",
        )

    def _failure_receipt(
        self,
        *,
        changes: list[Change],
        created: list[tuple[int, _CreatedFile]],
        failed_index: int,
        failure: BaseException | None,
        failed_change: Change,
    ) -> ScaffoldApplyPlan:
        changes[failed_index] = failed_change
        rollback = rollback_scaffold_files(
            created,
            rollback=self._filesystem.rollback,
        )
        for index, change in rollback.receipt_changes:
            changes[index] = change
        receipt_changes = _mark_not_applied(tuple(changes), after=failed_index)
        directory_recovery_paths = scaffold_directory_recovery_paths(failure)
        directory_recovery_entries = scaffold_directory_recovery_entries(failure)
        directory_issues = tuple(
            f"{path}: transaction-created directory requires manual recovery verification"
            for path in directory_recovery_paths
        )
        directory_changes = tuple(
            Change(
                "recovery",
                path,
                "A transaction-created directory requires manual recovery verification.",
            )
            for path in directory_recovery_paths
        )
        return ScaffoldApplyPlan(
            changes=(*receipt_changes, *rollback.recovery_changes, *directory_changes),
            rollback_journal=scaffold_rollback_journal(
                rollback.unresolved_paths,
                recovery_entries=(*rollback.recovery_entries, *directory_recovery_entries),
            ),
            rollback_issues=(*rollback.issues, *directory_issues),
            recovery_artifacts=tuple(dict.fromkeys((*rollback.recovery_artifacts, *directory_recovery_paths))),
            apply_failed=True,
        )

    def _condition_failure(
        self,
        *,
        changes: list[Change],
        created: list[tuple[int, _CreatedFile]],
    ) -> ScaffoldApplyPlan:
        rollback = rollback_scaffold_files(
            created,
            rollback=self._filesystem.rollback,
        )
        for index, change in rollback.receipt_changes:
            changes[index] = change
        guarded = _mark_not_applied(tuple(changes))
        conflict = Change(
            "conflict",
            "project-authority",
            "Project authoring authority changed while the scaffold was being applied.",
        )
        return ScaffoldApplyPlan(
            changes=(*guarded, conflict, *rollback.recovery_changes),
            rollback_journal=scaffold_rollback_journal(
                rollback.unresolved_paths,
                recovery_entries=rollback.recovery_entries,
            ),
            rollback_issues=rollback.issues,
            recovery_artifacts=rollback.recovery_artifacts,
            apply_failed=True,
        )


def _mark_not_applied(
    changes: tuple[Change, ...],
    *,
    after: int | None = None,
) -> tuple[Change, ...]:
    return tuple(
        (
            Change(
                "not_applied",
                change.path,
                "Not applied because another scaffold change blocked the operation.",
                diff=change.diff,
            )
            if change.action == "create" and (after is None or index > after)
            else change
        )
        for index, change in enumerate(changes)
    )


def _condition_passes(condition: Callable[[], bool] | None) -> bool:
    if condition is None:
        return True
    try:
        return condition()
    except Exception:  # noqa: BLE001 - guard failures must fail closed
        return False


def _unsafe_change(file: ScaffoldFile, message: str) -> Change:
    return Change(
        "conflict",
        file.path.as_posix(),
        message or "scaffold target path is unsafe",
    )


def _conflict_change(
    file: ScaffoldFile,
    existing: _FileContent | None,
    *,
    message: str,
) -> Change:
    existing_text = existing.content.decode("utf-8", errors="replace") if existing is not None else ""
    return Change(
        "conflict",
        file.path.as_posix(),
        message,
        diff=scaffold_diff(
            existing_text,
            file.text,
            path=file.path,
            existing_complete=existing.complete if existing is not None else True,
            fromfile=f"existing/{file.path.as_posix()}",
            tofile=f"desired/{file.path.as_posix()}",
        ),
    )


__all__ = [
    "ScaffoldApplier",
    "ScaffoldApplyPlan",
    "ScaffoldFile",
]
