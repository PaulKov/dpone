"""Bounded durable observations for the sixteen-file starter resource writer.

One resource is bounded by the existing one-MiB authoring input limit. The
manifest is bounded by that limit; at most nine events per resource plus eight
batch events are accepted, each at most 4096 bytes (the confined leaf journal
budget). Recovery reports are observations, never permission to delete files.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from itertools import islice
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID, uuid4

from tools.dbt_self_service.starter_resource_journal_schema import (
    MAX_EVENT_BYTES,
    MAX_EVENTS,
    MAX_LOG_BYTES,
    MAX_RESOURCE_BYTES,
    METADATA_ROOT,
    RESOURCE_PATHS,
)
from tools.dbt_self_service.starter_resource_journal_schema import (
    read_events as _read_events,
)
from tools.dbt_self_service.starter_resource_journal_schema import (
    validate_event as _validate_event,
)
from tools.dbt_self_service.starter_resource_journal_schema import (
    validate_manifest as _validate_manifest,
)

from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.manifest.confined_files import ConfinedFileSnapshot, read_confined_leaf, read_stable_descriptor
from dpone.manifest.confined_mutations import ConfinedMutationError, ConfinedReplaceOutcome
from dpone.manifest.confined_transaction_journal import transaction_journal_name
from dpone.manifest.project_root import ProjectRootIdentity, inspect_project_root
from dpone.readiness.airflow_authoring_directories import open_confined_parent
from dpone.readiness.airflow_pipeline_source import ConfinedAuthoringFileSystem, ConfinedFileCreation

_ERROR = "Starter resource transaction requires inspection; no recovery files were removed."


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    """Validated paths and observations, not an exhaustive cleanup inventory.

    Mutation tuples contain path, committed and cleanup_required exactly as
    observed, not ownership. Every pending report requires discovery: a crash
    or failed append can leave an unrecorded artifact in the confined scope.
    """

    pending: bool
    status: str
    operation: str | None = None
    paths: tuple[str, ...] = ()
    unresolved: tuple[str, ...] = ()
    observations: tuple[tuple[str, str, tuple[tuple[str, int], ...]], ...] = ()
    mutation_outcomes: tuple[tuple[str, bool, bool], ...] = ()
    discovery_required: bool = False


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
        with open_confined_parent(
            root, (*self.directory.parts, "events.jsonl"), create=False, root_identity=identity
        ) as parent:
            if parent.descriptor is None:
                raise ValueError(_ERROR)
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
        _validate_event(event)
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
        self.append(
            {
                "phase": "APPLYING",
                "path": path,
                "committed": outcome.committed,
                "cleanup_required": outcome.cleanup_required,
                "recovery_paths": []
                if outcome.recovery_name is None
                else [Path(path).with_name(outcome.recovery_name).as_posix()],
            }
        )

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


def recovery_report(root: Path) -> RecoveryReport:
    """Combine batch observations with all fixed per-leaf recovery obligations."""
    batch = _batch_report(root)
    batch = replace(batch, discovery_required=batch.pending)
    try:
        paths = leaf_recovery_paths(root)
    except (OSError, ValueError):
        return replace(batch, pending=True, status="INVALID", discovery_required=True)
    if not paths:
        return batch
    return replace(
        batch,
        pending=True,
        status="RECOVERY_REQUIRED",
        discovery_required=True,
        paths=tuple(dict.fromkeys((*batch.paths, *paths))),
    )


def leaf_recovery_paths(root: Path) -> tuple[str, ...]:
    """Observe journal entries without opening, interpreting or recovering them."""
    identity = inspect_project_root(root)
    if identity is None:
        raise ValueError(_ERROR)
    paths = []
    for resource in RESOURCE_PATHS:
        target = Path(resource)
        with open_confined_parent(root, target.parts, create=False, root_identity=identity) as parent:
            if parent.descriptor is None:
                continue
            name = transaction_journal_name(target.name)
            try:
                os.stat(name, dir_fd=parent.descriptor, follow_symlinks=False)
            except FileNotFoundError:
                continue
            paths.append(target.with_name(name).as_posix())
    return tuple(paths)


def _batch_report(root: Path) -> RecoveryReport:
    """Inspect at most one bounded operation; unknown data stays untouched."""
    try:
        identity = inspect_project_root(root)
        if identity is None:
            raise ValueError(_ERROR)
        with open_confined_parent(root, (METADATA_ROOT, "probe"), create=False, root_identity=identity) as metadata:
            if metadata.descriptor is None:
                return RecoveryReport(False, "CLEAR")
            with os.scandir(metadata.descriptor) as scan:
                names = [entry.name for entry in islice(scan, 2)]
        if not names:
            return RecoveryReport(False, "CLEAR")
        if len(names) != 1 or str(UUID(names[0])) != names[0]:
            raise ValueError(_ERROR)
        operation = names[0]
        directory = Path(METADATA_ROOT) / operation
        with open_confined_parent(
            root, (*directory.parts, "manifest.json"), create=False, root_identity=identity
        ) as parent:
            if parent.descriptor is None:
                raise ValueError(_ERROR)
            with os.scandir(parent.descriptor) as scan:
                children = {entry.name for entry in islice(scan, 5)}
            if not {"manifest.json", "events.jsonl"} <= children <= {"manifest.json", "events.jsonl", "old", "new"}:
                raise ValueError(_ERROR)
            manifest = strict_json_object(
                read_confined_leaf(parent.descriptor, "manifest.json", max_bytes=MAX_RESOURCE_BYTES).content
            )
            _validate_manifest(manifest, operation, identity.device, identity.inode)
            content = read_confined_leaf(parent.descriptor, "events.jsonl", max_bytes=MAX_LOG_BYTES).content
        events = _read_events(content)
        _verify_backups(root, directory, manifest, identity)
        unresolved: set[str] = set()
        observations = []
        recovery_paths = []
        outcomes = []
        for event in events:
            if "committed" in event and "cleanup_required" in event and "path" in event:
                outcomes.append((event["path"], event["committed"], event["cleanup_required"]))
            if event["phase"] in {"APPLYING", "COMPENSATING"} and "path" in event:
                unresolved.add(event["path"])
            elif event["phase"] in {"APPLIED", "COMPENSATED"}:
                unresolved.discard(event["path"])
            if "identity" in event:
                artifact = event.get("artifact", "target")
                index = next(index for index, entry in enumerate(manifest["entries"]) if entry["path"] == event["path"])
                target = PurePosixPath(event["path"])
                if artifact in {"backup", "staging"}:
                    kind = "old" if artifact == "backup" else "new"
                    recovery_paths.append(str(directory / kind / f"{index:03d}.bin"))
                elif artifact in {"candidate", "restore"}:
                    suffix = "new" if artifact == "candidate" else "restore"
                    recovery_paths.append(str(target.with_name(f".{target.name}.{operation}.{suffix}")))
                observations.append(
                    (event["path"], event.get("artifact", "target"), tuple(sorted(event["identity"].items())))
                )
            recovery_paths.extend(event.get("recovery_paths", ()))
        return RecoveryReport(
            True,
            events[-1]["phase"] if events else "PREPARING",
            operation,
            tuple(dict.fromkeys((directory.as_posix(), *RESOURCE_PATHS, *recovery_paths))),
            tuple(sorted(unresolved)),
            tuple(observations),
            tuple(outcomes),
        )
    except (OSError, ValueError, TypeError, KeyError):
        return RecoveryReport(True, "INVALID", paths=(METADATA_ROOT,))


def _verify_backups(root: Path, directory: Path, manifest: dict[str, Any], identity: ProjectRootIdentity) -> None:
    for kind in ("old", "new"):
        expected = {
            f"{index:03d}.bin": entry["old"]["sha256"] if kind == "old" else entry["desired_sha256"]
            for index, entry in enumerate(manifest["entries"])
            if kind == "new" or entry["old"] is not None
        }
        with open_confined_parent(
            root, (*directory.parts, kind, "probe"), create=False, root_identity=identity
        ) as parent:
            if parent.descriptor is None:
                continue
            with os.scandir(parent.descriptor) as scan:
                names = [entry.name for entry in islice(scan, len(RESOURCE_PATHS) + 1)]
            if not set(names) <= set(expected):
                raise ValueError(_ERROR)
            for name in names:
                observed = read_confined_leaf(parent.descriptor, name, max_bytes=MAX_RESOURCE_BYTES)
                if observed.sha256 != expected[name]:
                    raise ValueError(_ERROR)
