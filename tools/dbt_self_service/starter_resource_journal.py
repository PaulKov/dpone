"""Bounded durable observations for the sixteen-file starter resource writer.

One resource is bounded by the existing one-MiB authoring input limit. The
manifest is bounded by that limit; at most nine events per resource plus eight
batch events are accepted, each at most 4096 bytes (the confined leaf journal
budget). Recovery reports are observations, never permission to delete files.
"""

from __future__ import annotations

import os
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any
from uuid import uuid4

from tools.dbt_self_service.starter_resource_files import write_recovery_sidecar
from tools.dbt_self_service.starter_resource_journal_schema import (
    MAX_EVENT_BYTES,
    MAX_EVENTS,
    MAX_LOG_BYTES,
    MAX_RESOURCE_BYTES,
    METADATA_ROOT,
)
from tools.dbt_self_service.starter_resource_journal_schema import (
    RESOURCE_PATHS as RESOURCE_PATHS,
)
from tools.dbt_self_service.starter_resource_journal_schema import (
    validate_event as _validate_event,
)
from tools.dbt_self_service.starter_resource_journal_schema import (
    validate_manifest as _validate_manifest,
)
from tools.dbt_self_service.starter_resource_recovery import RecoveryReport as RecoveryReport
from tools.dbt_self_service.starter_resource_recovery import leaf_recovery_paths as leaf_recovery_paths
from tools.dbt_self_service.starter_resource_recovery import recovery_report as recovery_report

from dpone.contracts.strict_json import canonical_json_bytes
from dpone.manifest.confined_files import ConfinedFileSnapshot, read_confined_leaf, read_stable_descriptor
from dpone.manifest.confined_mutations import ConfinedMutationError, ConfinedReplaceOutcome
from dpone.manifest.project_root import ProjectRootIdentity, inspect_project_root
from dpone.readiness.airflow_authoring_directories import open_confined_parent
from dpone.readiness.airflow_pipeline_source import (
    ConfinedAuthoringFileSystem,
    ConfinedFileCreation,
    ConfinedFileRollbackOutcome,
)

_ERROR = "Starter resource transaction requires inspection; no recovery files were removed."


class ResourceJournal:
    """Own the append descriptor and the exact bytes written by this operation."""

    root: Path
    identity: ProjectRootIdentity
    operation: str
    directory: Path
    filesystem: ConfinedAuthoringFileSystem
    manifest_creation: ConfinedFileCreation
    log_creation: ConfinedFileCreation
    log_path: Path
    _content: bytes
    _sequence: int
    _descriptor: int | None
    directory_identity: ProjectRootIdentity
    unpersisted_paths: tuple[str, ...]

    @classmethod
    def start(cls, root: Path, revision: str, entries: list[dict[str, Any]]) -> ResourceJournal:
        identity = inspect_project_root(root)
        if identity is None or recovery_report(root).pending:
            raise ValueError(_ERROR)
        operation = str(uuid4())
        manifest = {
            "schema": "dpone.starter-resource-transaction.v1",
            "operation": operation,
            "revision": revision,
            "root": {"device": identity.device, "inode": identity.inode},
            "entries": entries,
        }
        _validate_manifest(manifest, operation, identity.device, identity.inode)
        encoded = canonical_json_bytes(manifest)
        if len(encoded) > MAX_RESOURCE_BYTES:
            raise ValueError(_ERROR)
        self = cls()
        self.root = identity.path
        self.identity = identity
        self.operation = operation
        self.directory = Path(METADATA_ROOT) / operation
        self.filesystem = ConfinedAuthoringFileSystem(root, root_identity=identity)
        manifest_creation = self.filesystem.create(self.directory / "manifest.json", encoded)
        log_creation = self.filesystem.create(self.directory / "events.jsonl", b"")
        if manifest_creation is None or log_creation is None:
            raise ValueError(_ERROR)
        self.manifest_creation, self.log_creation = manifest_creation, log_creation
        self.log_path = self.root / self.directory / "events.jsonl"
        self._content = b""
        self._sequence = 0
        self._descriptor = None
        self.unpersisted_paths = ()
        with open_confined_parent(
            root, (*self.directory.parts, "events.jsonl"), create=False, root_identity=identity
        ) as parent:
            if parent.descriptor is None:
                raise ValueError(_ERROR)
            metadata = os.fstat(parent.descriptor)
            self.directory_identity = ProjectRootIdentity(self.root / self.directory, metadata.st_dev, metadata.st_ino)
            descriptor = os.open(
                "events.jsonl",
                os.O_RDWR | os.O_APPEND | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=parent.descriptor,
            )
        try:
            metadata = os.fstat(descriptor)
            if (metadata.st_dev, metadata.st_ino) != (self.log_creation.device, self.log_creation.inode):
                raise ValueError(_ERROR)
            self._descriptor = descriptor
            self.append({"phase": "PREPARING"})
        except BaseException:
            os.close(descriptor)
            self._descriptor = None
            raise
        return self

    def append(self, event: dict[str, Any]) -> None:
        """Append and fsync one closed event, refusing torn or foreign log bytes."""
        _validate_event(event, self.operation)
        encoded = canonical_json_bytes({"sequence": self._sequence, **event}) + b"\n"
        if len(encoded) > MAX_EVENT_BYTES or self._sequence >= MAX_EVENTS:
            raise ValueError(_ERROR)
        descriptor = self._descriptor
        if descriptor is None:
            raise ValueError(_ERROR)
        os.lseek(descriptor, 0, os.SEEK_SET)
        snapshot = read_stable_descriptor(descriptor, max_bytes=MAX_LOG_BYTES)
        if snapshot.content != self._content:
            raise ValueError(_ERROR)
        self._verify_log_path(snapshot)
        pending = memoryview(encoded)
        while pending:
            written = os.write(descriptor, pending)
            if written <= 0:
                raise OSError(_ERROR)
            pending = pending[written:]
        os.fsync(descriptor)
        os.lseek(descriptor, 0, os.SEEK_SET)
        snapshot = read_stable_descriptor(descriptor, max_bytes=MAX_LOG_BYTES)
        if snapshot.content != self._content + encoded:
            raise ValueError(_ERROR)
        self._verify_log_path(snapshot)
        self._content += encoded
        self._sequence += 1
        self.log_creation = replace(self.log_creation, content=self._content)

    def observe_mutation(self, path: str, outcome: ConfinedMutationError | ConfinedReplaceOutcome) -> None:
        """Persist the primitive's observation without inferring installed ownership."""
        event = {
            "phase": "APPLYING",
            "path": path,
            "committed": outcome.committed,
            "cleanup_required": outcome.cleanup_required,
            "recovery_paths": []
            if outcome.recovery_name is None
            else [Path(path).with_name(outcome.recovery_name).as_posix()],
        }
        _validate_event(event, self.operation)
        self.unpersisted_paths = tuple((path, *event["recovery_paths"]))
        self.append(event)
        self.unpersisted_paths = ()

    def observe_rollback(self, created: ConfinedFileCreation, outcome: ConfinedFileRollbackOutcome) -> None:
        """Record a failed cleanup using the actual creation and rollback receipts."""
        record = self._rollback_record(created, outcome)
        _validate_event({"phase": "RECOVERY_REQUIRED", "rollback": record}, self.operation)
        self.unpersisted_paths = tuple(
            dict.fromkeys(
                (
                    record["path"],
                    *record["directory_recovery_paths"],
                    *(() if record["recovery_path"] is None else (record["recovery_path"],)),
                )
            )
        )
        self.append({"phase": "RECOVERY_REQUIRED", "rollback": record})
        self.unpersisted_paths = ()

    def _rollback_record(self, created: ConfinedFileCreation, outcome: ConfinedFileRollbackOutcome) -> dict[str, Any]:
        if outcome.path != created.path:
            raise ValueError(_ERROR)
        return {
            "path": created.path.as_posix(),
            "device": created.device,
            "inode": created.inode,
            "directories": [{**asdict(item), "path": item.path.as_posix()} for item in created.created_directories],
            "removed": outcome.removed,
            "preserved": outcome.preserved,
            "recovery_path": None if outcome.recovery_path is None else outcome.recovery_path.as_posix(),
            "directory_recovery_paths": [path.as_posix() for path in outcome.directory_recovery_paths],
        }

    def record_cleanup_failure(self, created: ConfinedFileCreation, outcome: ConfinedFileRollbackOutcome) -> None:
        """Keep a failure-only sidecar when the ordinary log is being removed."""
        from tools.dbt_self_service.starter_resource_journal_schema import validate_sidecar

        record = self._rollback_record(created, outcome)
        value = {
            "schema": "dpone.starter-resource-recovery.v1",
            "operation": self.operation,
            "root": {"device": self.identity.device, "inode": self.identity.inode},
            "directory": {"device": self.directory_identity.device, "inode": self.directory_identity.inode},
            "rollback": record,
        }
        validate_sidecar(
            value,
            self.operation,
            (self.identity.device, self.identity.inode),
            (self.directory_identity.device, self.directory_identity.inode),
        )
        self.unpersisted_paths = tuple(
            dict.fromkeys(
                (
                    record["path"],
                    *record["directory_recovery_paths"],
                    *(() if record["recovery_path"] is None else (record["recovery_path"],)),
                )
            )
        )
        write_recovery_sidecar(self.identity, self.operation, self.directory_identity, canonical_json_bytes(value))
        self.unpersisted_paths = ()

    def _verify_log_path(self, snapshot: ConfinedFileSnapshot) -> None:
        with open_confined_parent(
            self.root, (*self.directory.parts, "events.jsonl"), create=False, root_identity=self.identity
        ) as parent:
            if parent.descriptor is None:
                raise ValueError(_ERROR)
            current = read_confined_leaf(parent.descriptor, "events.jsonl", max_bytes=MAX_LOG_BYTES)
            if current.identity != snapshot.identity:
                raise ValueError(_ERROR)

    def close(self) -> None:
        descriptor, self._descriptor = self._descriptor, None
        if descriptor is not None:
            os.close(descriptor)

    def __enter__(self) -> ResourceJournal:
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
