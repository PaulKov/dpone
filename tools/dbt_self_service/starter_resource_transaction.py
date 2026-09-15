"""Bounded resource writes with owned compensation, never a directory commit.

The caller supplies concrete source/result validators and the existing authoring
lock. That lock covers cooperating producers only. Unknown displacement or an
unacknowledged mutation preserves recovery material and blocks the next apply.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass
from pathlib import Path

from tools.dbt_self_service.starter_resource_files import (
    preserve_mode as _preserve_mode,
)
from tools.dbt_self_service.starter_resource_files import (
    reject_leaf_journal as _reject_leaf_journal,
)
from tools.dbt_self_service.starter_resource_files import (
    require_snapshot as _require_snapshot,
)
from tools.dbt_self_service.starter_resource_files import (
    snapshot as _snapshot,
)
from tools.dbt_self_service.starter_resource_files import (
    verify_creation as _verify_creation,
)
from tools.dbt_self_service.starter_resource_journal import (
    MAX_RESOURCE_BYTES,
    RESOURCE_PATHS,
    ResourceJournal,
    recovery_report,
)

from dpone.manifest.confined_atomic_exchange import exchange_back, get_native_atomic_exchange, same_identity
from dpone.manifest.confined_files import ConfinedFileSnapshot, read_confined_leaf
from dpone.manifest.confined_mutations import replace_file_if_digest
from dpone.manifest.project_root import ProjectRootIdentity, inspect_project_root
from dpone.ports.project_authoring_lock import AuthoringLockFactory
from dpone.readiness.airflow_authoring_directories import open_confined_parent
from dpone.readiness.airflow_pipeline_source import ConfinedFileCreation

_ERROR = "Starter resource update did not complete; inspect the recovery report."
PhaseHook = Callable[[str, str | None], None]


@dataclass(frozen=True, slots=True)
class ResourceWriteReceipt:
    passed: bool
    status: str
    changed_paths: tuple[str, ...] = ()
    recovery_required: bool = False
    operation: str | None = None


@dataclass
class _Entry:
    path: str
    desired: bytes
    old: ConfinedFileSnapshot | None
    backup: ConfinedFileCreation | None = None
    candidate: ConfinedFileCreation | None = None
    candidate_snapshot: ConfinedFileSnapshot | None = None


@dataclass
class _Applied:
    entry: _Entry
    snapshot: ConfinedFileSnapshot
    creation: ConfinedFileCreation | None
    owned: bool
    cleanup_required: bool = False


def apply_resource_plan(
    root: Path,
    files: Mapping[str, bytes],
    *,
    source_revision: str,
    revalidate_inputs: Callable[[], None],
    validate_result: Callable[[], None],
    authoring_lock: AuthoringLockFactory,
    phase_hook: PhaseHook | None = None,
) -> ResourceWriteReceipt:
    """Apply exactly sixteen bounded resource files; never accept arbitrary paths."""
    try:
        captured = dict(files)
        if set(captured) != set(RESOURCE_PATHS) or re.fullmatch(r"[0-9a-f]{40}", source_revision) is None:
            raise ValueError(_ERROR)
        for value in captured.values():
            if not isinstance(value, bytes) or len(value) > MAX_RESOURCE_BYTES:
                raise ValueError(_ERROR)
            value.decode("utf-8")
        identity = inspect_project_root(root)
        if identity is None:
            raise ValueError(_ERROR)
        with authoring_lock(identity.path):
            if recovery_report(identity.path).pending:
                return ResourceWriteReceipt(False, "RECOVERY_REQUIRED", recovery_required=True)
            revalidate_inputs()
            entries = [_Entry(path, captured[path], _snapshot(identity, path)) for path in RESOURCE_PATHS]
            if all(entry.old is not None and entry.old.content == entry.desired for entry in entries):
                validate_result()
                return ResourceWriteReceipt(True, "NOOP")
            return _Writer(identity, entries, source_revision, revalidate_inputs, validate_result, phase_hook).run()
    except Exception:
        pending = recovery_report(root).pending
        return ResourceWriteReceipt(False, "RECOVERY_REQUIRED" if pending else "FAILED", recovery_required=pending)


class _Writer:
    def __init__(
        self,
        identity: ProjectRootIdentity,
        entries: list[_Entry],
        revision: str,
        revalidate: Callable[[], None],
        validate: Callable[[], None],
        hook: PhaseHook | None,
    ) -> None:
        self.identity, self.entries, self.revision = identity, entries, revision
        self.revalidate, self.validate, self.hook = revalidate, validate, hook
        self.prepared: list[ConfinedFileCreation] = []
        self.applied: list[_Applied] = []
        self.pending: _Entry | None = None
        self.mutation_started = False
        self.journal: ResourceJournal | None = None

    def run(self) -> ResourceWriteReceipt:
        import hashlib

        manifest = [
            {
                "path": entry.path,
                "old": None
                if entry.old is None
                else {"identity": asdict(entry.old.identity), "sha256": entry.old.sha256},
                "desired_sha256": "sha256:" + hashlib.sha256(entry.desired).hexdigest(),
            }
            for entry in self.entries
        ]
        try:
            self.journal = ResourceJournal.start(self.identity.path, self.revision, manifest)
            journal = self.journal
            self._prepare()
            self.revalidate()
            for entry in self.entries:
                _require_snapshot(self.identity, entry.path, entry.old)
            journal.append({"phase": "PREPARED"})
            self._hook("prepared", None)
            for entry in self.entries:
                if entry.old is not None and entry.old.content == entry.desired:
                    continue
                self.pending, self.mutation_started = entry, False
                journal.append({"phase": "APPLYING", "path": entry.path})
                self._hook("before_apply", entry.path)
                for created in self.prepared:
                    if created.path.is_relative_to(journal.directory):
                        _verify_creation(self.identity, created)
                self.mutation_started = True
                applied = self._apply(entry)
                self.applied.append(applied)
                self._hook("after_mutation", entry.path)
                event = {
                    "phase": "APPLIED",
                    "path": entry.path,
                    "committed": True,
                    "cleanup_required": applied.cleanup_required,
                    "owned": applied.owned,
                }
                if applied.owned:
                    event["identity"] = asdict(applied.snapshot.identity)
                if applied.creation is not None:
                    event["directories"] = [
                        {"path": item.path.as_posix(), "device": item.device, "inode": item.inode}
                        for item in applied.creation.created_directories
                    ]
                journal.append(event)
                self.pending = None
                if applied.cleanup_required:
                    raise OSError(_ERROR)
                self._hook("after_observation", entry.path)
            self.revalidate()
            self.validate()
            for applied in self.applied:
                _require_snapshot(self.identity, applied.entry.path, applied.snapshot)
            journal.append({"phase": "VERIFIED"})
            self._hook("verified", None)
            self._cleanup()
            return ResourceWriteReceipt(True, "APPLIED", tuple(item.entry.path for item in self.applied if item.owned))
        except Exception:
            return self._failed()
        finally:
            if self.journal is not None:
                self.journal.close()

    def _prepare(self) -> None:
        journal = self._journal()
        for index, entry in enumerate(self.entries):
            for kind, content in (("old", None if entry.old is None else entry.old.content), ("new", entry.desired)):
                if content is None:
                    continue
                creation = self._create(journal.directory / kind / f"{index:03d}.bin", content)
                if kind == "old":
                    entry.backup = creation
                observed = _snapshot(self.identity, creation.path.as_posix())
                assert observed is not None
                journal.append(
                    {
                        "phase": "PREPARING",
                        "path": entry.path,
                        "artifact": "backup" if kind == "old" else "staging",
                        "identity": asdict(observed.identity),
                    }
                )
            if entry.old is not None and entry.old.content != entry.desired:
                path = Path(entry.path)
                candidate = path.with_name(f".{path.name}.{journal.operation}.new")
                entry.candidate = self._create(candidate, entry.desired)
                _preserve_mode(self.identity, entry.candidate, entry.old.identity.mode)
                entry.candidate_snapshot = _verify_creation(self.identity, entry.candidate)
                assert entry.candidate_snapshot is not None
                journal.append(
                    {
                        "phase": "PREPARING",
                        "path": entry.path,
                        "artifact": "candidate",
                        "identity": asdict(entry.candidate_snapshot.identity),
                    }
                )

    def _create(self, path: Path, content: bytes) -> ConfinedFileCreation:
        created = self._journal().filesystem.create(path, content)
        if created is None:
            raise ValueError(_ERROR)
        self.prepared.append(created)
        return created

    def _apply(self, entry: _Entry) -> _Applied:
        journal = self._journal()
        _require_snapshot(self.identity, entry.path, entry.old)
        if entry.old is None:
            creation = journal.filesystem.create(Path(entry.path), entry.desired)
            observed = _snapshot(self.identity, entry.path)
            if observed is None or observed.content != entry.desired:
                raise ValueError(_ERROR)
            if creation is not None and (creation.device, creation.inode) != (
                observed.identity.device,
                observed.identity.inode,
            ):
                raise ValueError(_ERROR)
            return _Applied(entry, observed, creation, creation is not None)
        assert entry.candidate is not None and entry.candidate_snapshot is not None
        _require_snapshot(self.identity, entry.candidate.path.as_posix(), entry.candidate_snapshot)
        with open_confined_parent(
            self.identity.path, Path(entry.path).parts, create=False, root_identity=self.identity
        ) as parent:
            if parent.descriptor is None:
                raise ValueError(_ERROR)
            _reject_leaf_journal(parent.descriptor, Path(entry.path).name)
            outcome = replace_file_if_digest(
                parent.descriptor,
                Path(entry.path).name,
                entry.candidate.path.name,
                expected_sha256=entry.old.sha256,
                max_bytes=MAX_RESOURCE_BYTES,
            )
            observed = read_confined_leaf(parent.descriptor, Path(entry.path).name, max_bytes=MAX_RESOURCE_BYTES)
        if (
            not outcome.committed
            or observed.content != entry.desired
            or not same_identity(observed.identity, entry.candidate_snapshot.identity)
        ):
            raise ValueError(_ERROR)
        return _Applied(entry, observed, None, True, outcome.cleanup_required)

    def _failed(self) -> ResourceWriteReceipt:
        journal = self.journal
        if journal is not None:
            try:
                journal.append({"phase": "COMPENSATING"})
                if any(item.cleanup_required for item in self.applied):
                    raise ValueError(_ERROR)
                for item in reversed(self.applied):
                    journal.append({"phase": "COMPENSATING", "path": item.entry.path})
                    self._hook("before_compensate", item.entry.path)
                    self._compensate(item)
                    self._hook("after_compensate_mutation", item.entry.path)
                    journal.append({"phase": "COMPENSATED", "path": item.entry.path})
                if self.pending is not None and not any(item.entry is self.pending for item in self.applied):
                    if self.mutation_started:
                        raise ValueError(_ERROR)
                    _require_snapshot(self.identity, self.pending.path, self.pending.old)
                    journal.append({"phase": "COMPENSATED", "path": self.pending.path})
                journal.append({"phase": "ROLLED_BACK"})
                self._cleanup()
                return ResourceWriteReceipt(False, "ROLLED_BACK")
            except Exception:
                try:
                    journal.append({"phase": "RECOVERY_REQUIRED"})
                except Exception:
                    pass
        return ResourceWriteReceipt(
            False,
            "RECOVERY_REQUIRED",
            tuple(item.entry.path for item in self.applied if item.owned),
            True,
            None if journal is None else journal.operation,
        )

    def _compensate(self, item: _Applied) -> None:
        if not item.owned:
            return
        journal = self._journal()
        _require_snapshot(self.identity, item.entry.path, item.snapshot)
        if item.creation is not None:
            outcome = journal.filesystem.rollback(item.creation)
            if outcome.preserved or outcome.recovery_path or outcome.directory_recovery_paths:
                raise ValueError(_ERROR)
            return
        assert item.entry.backup is not None and item.entry.old is not None
        backup = _snapshot(self.identity, item.entry.backup.path.as_posix())
        if backup is None or backup.content != item.entry.old.content:
            raise ValueError(_ERROR)
        target = Path(item.entry.path)
        restored = self._create(target.with_name(f".{target.name}.{journal.operation}.restore"), backup.content)
        _preserve_mode(self.identity, restored, item.entry.old.identity.mode)
        restored_snapshot = _verify_creation(self.identity, restored)
        assert restored_snapshot is not None
        journal.append(
            {
                "phase": "COMPENSATING",
                "path": item.entry.path,
                "artifact": "restore",
                "identity": asdict(restored_snapshot.identity),
            }
        )
        with open_confined_parent(
            self.identity.path, target.parts, create=False, root_identity=self.identity
        ) as parent:
            if parent.descriptor is None:
                raise ValueError(_ERROR)
            _reject_leaf_journal(parent.descriptor, target.name)
            result = exchange_back(
                parent.descriptor,
                target_name=target.name,
                exchange_name=restored.path.name,
                displaced_source=restored_snapshot,
                desired_sha256=item.snapshot.sha256,
                desired_identity=item.snapshot.identity,
                atomic_exchange=get_native_atomic_exchange(),
                max_bytes=MAX_RESOURCE_BYTES,
            )
        if result.cleanup_required or result.recovery_name:
            journal.append(
                {
                    "phase": "RECOVERY_REQUIRED",
                    "path": item.entry.path,
                    "recovery_paths": []
                    if result.recovery_name is None
                    else [target.with_name(result.recovery_name).as_posix()],
                }
            )
            raise ValueError(_ERROR)

    def _cleanup(self) -> None:
        journal = self._journal()
        self._hook("before_cleanup", None)
        for created in reversed(self.prepared):
            outcome = journal.filesystem.rollback(created)
            if outcome.preserved or outcome.recovery_path or outcome.directory_recovery_paths:
                raise ValueError(_ERROR)
        journal.append({"phase": "COMPLETE"})
        journal.close()
        for created in (journal.log_creation, journal.manifest_creation):
            outcome = journal.filesystem.rollback(created)
            if outcome.preserved or outcome.recovery_path or outcome.directory_recovery_paths:
                raise ValueError(_ERROR)
        if recovery_report(self.identity.path).pending:
            raise ValueError(_ERROR)

    def _journal(self) -> ResourceJournal:
        assert self.journal is not None
        return self.journal

    def _hook(self, phase: str, path: str | None) -> None:
        if self.hook is not None:
            self.hook(phase, path)
