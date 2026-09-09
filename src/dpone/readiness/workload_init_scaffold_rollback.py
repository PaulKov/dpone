"""Exhaustive rollback receipt aggregation for workload-init scaffolds."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from dpone.readiness.airflow_pipeline_source import ConfinedFileCreation, ConfinedFileRollbackOutcome


from collections.abc import Callable, Sequence
from dataclasses import dataclass
from pathlib import Path

from dpone.readiness.airflow_self_service_models import Change
from dpone.security_redaction import redact_text

_JOURNAL_SCHEMA = "dpone.scaffold-rollback-journal.v1"


@dataclass(frozen=True)
class ScaffoldRollbackReport:
    """Every terminal change and unresolved receipt from one rollback pass."""

    receipt_changes: tuple[tuple[int, Change], ...]
    recovery_changes: tuple[Change, ...]
    issues: tuple[str, ...]
    recovery_artifacts: tuple[str, ...]
    unresolved_paths: tuple[str, ...]
    recovery_entries: tuple[dict[str, Any], ...]


def rollback_scaffold_files(
    created: Sequence[tuple[int, ConfinedFileCreation]],
    *,
    rollback: Callable[[ConfinedFileCreation], ConfinedFileRollbackOutcome],
) -> ScaffoldRollbackReport:
    """Compensate every creation in reverse order without hiding failures."""

    receipt_changes: list[tuple[int, Change]] = []
    recovery_changes: list[Change] = []
    issues: list[str] = []
    recovery_artifacts: list[str] = []
    unresolved_paths: list[str] = []
    recovery_entries: list[dict[str, Any]] = []
    for index, item in reversed(created):
        path = item.path.as_posix()
        try:
            outcome = rollback(item)
        except Exception as exc:  # noqa: BLE001 - every compensation must be attempted
            issues.append(f"{path}: {redact_text(str(exc)) or 'rollback failed'}")
            unresolved_paths.append(path)
            recovery_entries.append(_owned_file_recovery_entry(item))
            receipt_changes.append(
                (
                    index,
                    Change(
                        "recovery_required",
                        path,
                        "Scaffold rollback could not prove that operation-owned bytes were removed.",
                    ),
                )
            )
            for recovery_path in _error_recovery_paths(exc, source_path=item.path):
                recovery_artifacts.append(recovery_path)
                recovery_changes.append(_error_recovery_change(recovery_path, source_path=item.path))
            continue
        receipt_changes.append((index, _rollback_change(outcome)))
        if outcome.recovery_path is not None:
            recovery_artifacts.append(outcome.recovery_path.as_posix())
            recovery_changes.append(_recovery_change(outcome))
        for directory_path in outcome.directory_recovery_paths:
            path_label = directory_path.as_posix()
            issues.append(f"{path_label}: created directory rollback could not be verified")
            unresolved_paths.append(path_label)
            recovery_changes.append(
                Change(
                    "recovery",
                    path_label,
                    "A transaction-created directory requires manual recovery verification.",
                )
            )
            directory_entry = _owned_directory_recovery_entry(item, directory_path)
            if directory_entry is not None:
                recovery_entries.append(directory_entry)
    return ScaffoldRollbackReport(
        receipt_changes=tuple(receipt_changes),
        recovery_changes=tuple(recovery_changes),
        issues=tuple(issues),
        recovery_artifacts=tuple(dict.fromkeys(recovery_artifacts)),
        unresolved_paths=tuple(unresolved_paths),
        recovery_entries=tuple(recovery_entries),
    )


def scaffold_rollback_journal(
    paths: Sequence[str],
    *,
    recovery_entries: tuple[dict[str, Any], ...] = (),
) -> dict[str, Any]:
    """Build the closed rollback journal without duplicating typed entries."""

    typed_paths = {str(entry.get("path")) for entry in recovery_entries}
    return {
        "schema": _JOURNAL_SCHEMA,
        "entries": [
            *recovery_entries,
            *({"action": "delete", "path": path} for path in paths if path not in typed_paths),
        ],
    }


def scaffold_directory_recovery_paths(error: BaseException | None) -> tuple[str, ...]:
    """Return safe relative directory recovery paths carried by an error."""

    if error is None:
        return ()
    raw_paths = getattr(error, "scaffold_directory_recovery_paths", ())
    if isinstance(raw_paths, str | Path):
        raw_paths = (raw_paths,)
    return tuple(
        dict.fromkeys(
            path.as_posix()
            for raw_path in raw_paths
            if not (path := Path(str(raw_path))).is_absolute() and ".." not in path.parts
        )
    )


def scaffold_directory_recovery_entries(error: BaseException | None) -> tuple[dict[str, Any], ...]:
    """Return typed directory recovery entries carried by an error."""

    if error is None:
        return ()
    raw_entries = getattr(error, "scaffold_directory_recovery_entries", ())
    if not isinstance(raw_entries, tuple):
        return ()
    return tuple(entry for entry in raw_entries if isinstance(entry, dict))


def _owned_file_recovery_entry(item: ConfinedFileCreation) -> dict[str, Any]:
    return {
        "action": "verify_and_remove",
        "kind": "file",
        "path": item.path.as_posix(),
        "device": item.device,
        "inode": item.inode,
        "require_empty": False,
    }


def _owned_directory_recovery_entry(
    item: ConfinedFileCreation,
    path: Path,
) -> dict[str, Any] | None:
    created = next((entry for entry in item.created_directories if entry.path == path), None)
    if created is None:
        return None
    return {
        "action": "verify_and_remove",
        "kind": "directory",
        "path": path.as_posix(),
        "device": created.device,
        "inode": created.inode,
        "require_empty": True,
    }


def _rollback_change(outcome: ConfinedFileRollbackOutcome) -> Change:
    if outcome.directory_recovery_paths:
        return Change(
            "recovery_required",
            outcome.path.as_posix(),
            "Scaffold-created bytes were removed, but directory cleanup requires recovery.",
        )
    if outcome.removed:
        message = "Scaffold-created bytes were rolled back."
        if outcome.preserved:
            message += " A concurrent pathname winner was preserved."
        return Change("rolled_back", outcome.path.as_posix(), message)
    if outcome.preserved:
        return Change(
            "preserved",
            outcome.path.as_posix(),
            "The pathname no longer contained scaffold-owned bytes and was preserved.",
        )
    return Change(
        "rolled_back",
        outcome.path.as_posix(),
        "No scaffold-owned bytes remain; the pathname was already absent.",
    )


def _recovery_change(outcome: ConfinedFileRollbackOutcome) -> Change:
    assert outcome.recovery_path is not None
    return Change(
        "recovery",
        outcome.recovery_path.as_posix(),
        f"Concurrent bytes displaced from {outcome.path.as_posix()} were preserved for manual recovery.",
    )


def _error_recovery_change(path: str, *, source_path: Path) -> Change:
    return Change(
        "recovery",
        path,
        f"Rollback recovery bytes for {source_path.as_posix()} were preserved for manual recovery.",
    )


def _error_recovery_paths(error: Exception, *, source_path: Path) -> tuple[str, ...]:
    raw_paths = getattr(error, "recovery_artifacts", ())
    if isinstance(raw_paths, str | Path):
        raw_paths = (raw_paths,)
    candidates = [Path(str(path)) for path in raw_paths]
    recovery_name = getattr(error, "recovery_name", None)
    if recovery_name:
        try:
            candidates.append(source_path.with_name(str(recovery_name)))
        except ValueError:
            pass
    return tuple(
        dict.fromkeys(path.as_posix() for path in candidates if not path.is_absolute() and ".." not in path.parts)
    )


__all__ = [
    "ScaffoldRollbackReport",
    "rollback_scaffold_files",
    "scaffold_directory_recovery_entries",
    "scaffold_directory_recovery_paths",
    "scaffold_rollback_journal",
]
