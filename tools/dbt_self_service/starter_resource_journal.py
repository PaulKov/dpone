"""Bounded durable observations for the sixteen-file starter resource writer.

One resource is bounded by the existing one-MiB authoring input limit. The
manifest is bounded by that limit; at most eight events per resource plus eight
batch events are accepted, each at most 4096 bytes (the confined leaf journal
budget). Recovery reports are observations, never permission to delete files.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, replace
from itertools import islice
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID, uuid4

from dpone.adapters.dbt_starter_resources import _PACKAGE_FILES
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.manifest.bounded_yaml import BoundedYamlLimits
from dpone.manifest.confined_files import ConfinedFileSnapshot, read_confined_leaf, read_stable_descriptor
from dpone.manifest.project_root import ProjectRootIdentity, inspect_project_root
from dpone.readiness.airflow_authoring_directories import open_confined_parent
from dpone.readiness.airflow_pipeline_source import ConfinedAuthoringFileSystem, ConfinedFileCreation

RESOURCE_PATHS = tuple("src/dpone/_assets/dbt_dpone/" + name for name in _PACKAGE_FILES) + (
    "src/dpone/_assets/dbt_starter/v4/packages.yml",
    "src/dpone/_assets/dbt_starter/v4/package-lock.yml",
)
MAX_RESOURCE_BYTES = BoundedYamlLimits().max_bytes
MAX_EVENTS = len(RESOURCE_PATHS) * 8 + 8
MAX_EVENT_BYTES = 4096
MAX_LOG_BYTES = MAX_EVENTS * MAX_EVENT_BYTES
METADATA_ROOT = ".dpone-starter-resource-transactions"
_ERROR = "Starter resource transaction requires inspection; no recovery files were removed."
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")
_IDENTITY_KEYS = {"device", "inode", "mode", "size", "modified_ns", "changed_ns"}
_PHASES = {
    "PREPARING",
    "PREPARED",
    "APPLYING",
    "APPLIED",
    "VERIFIED",
    "COMPENSATING",
    "COMPENSATED",
    "ROLLED_BACK",
    "RECOVERY_REQUIRED",
    "COMPLETE",
}


@dataclass(frozen=True, slots=True)
class RecoveryReport:
    """Only validated relative paths and bounded ownership observations."""

    pending: bool
    status: str
    operation: str | None = None
    paths: tuple[str, ...] = ()
    unresolved: tuple[str, ...] = ()
    observations: tuple[tuple[str, str, tuple[tuple[str, int], ...]], ...] = ()


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
        for event in events:
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


def _validate_manifest(value: dict[str, Any], operation: str, device: int, inode: int) -> None:
    if (
        set(value) != {"schema", "operation", "revision", "root", "entries"}
        or value["schema"] != "dpone.starter-resource-transaction.v1"
        or value["operation"] != operation
        or not isinstance(value["revision"], str)
        or re.fullmatch(r"[0-9a-f]{40}", value["revision"]) is None
        or value["root"] != {"device": device, "inode": inode}
    ):
        raise ValueError(_ERROR)
    entries = value["entries"]
    if not isinstance(entries, list) or len(entries) != len(RESOURCE_PATHS):
        raise ValueError(_ERROR)
    observed = []
    for entry in entries:
        if not isinstance(entry, dict) or set(entry) != {"path", "old", "desired_sha256"}:
            raise ValueError(_ERROR)
        if entry["path"] not in RESOURCE_PATHS or not _valid_digest(entry["desired_sha256"]):
            raise ValueError(_ERROR)
        observed.append(entry["path"])
        if entry["old"] is not None:
            old = entry["old"]
            if not isinstance(old, dict) or set(old) != {"identity", "sha256"} or not _valid_digest(old["sha256"]):
                raise ValueError(_ERROR)
            _validate_identity(old["identity"])
    if set(observed) != set(RESOURCE_PATHS):
        raise ValueError(_ERROR)


def _validate_event(event: dict[str, Any]) -> None:
    if (
        not isinstance(event.get("phase"), str)
        or set(event)
        - {
            "phase",
            "path",
            "identity",
            "artifact",
            "committed",
            "cleanup_required",
            "owned",
            "directories",
            "recovery_paths",
        }
        or event.get("phase") not in _PHASES
    ):
        raise ValueError(_ERROR)
    if "path" in event and event["path"] not in RESOURCE_PATHS:
        raise ValueError(_ERROR)
    if event["phase"] in {"APPLYING", "APPLIED", "COMPENSATED"} and "path" not in event:
        raise ValueError(_ERROR)
    if "identity" in event:
        if "path" not in event:
            raise ValueError(_ERROR)
        _validate_identity(event["identity"])
    if "artifact" in event and event["artifact"] not in {"target", "backup", "staging", "candidate", "restore"}:
        raise ValueError(_ERROR)
    if event["phase"] == "APPLIED" and (
        not {"committed", "cleanup_required", "owned"} <= set(event) or (event.get("owned") and "identity" not in event)
    ):
        raise ValueError(_ERROR)
    for key in ("committed", "cleanup_required", "owned"):
        if key in event and type(event[key]) is not bool:
            raise ValueError(_ERROR)
    directories = event.get("directories", [])
    if not isinstance(directories, list) or len(directories) > 8:
        raise ValueError(_ERROR)
    for directory in directories:
        if not isinstance(directory, dict) or set(directory) != {"path", "device", "inode"}:
            raise ValueError(_ERROR)
        if directory["path"] not in {
            str(parent) for path in RESOURCE_PATHS for parent in PurePosixPath(path).parents if str(parent) != "."
        }:
            raise ValueError(_ERROR)
        if any(type(directory[key]) is not int or directory[key] < 0 for key in ("device", "inode")):
            raise ValueError(_ERROR)
    paths = event.get("recovery_paths", [])
    if not isinstance(paths, list) or len(paths) > len(RESOURCE_PATHS):
        raise ValueError(_ERROR)
    for path in paths:
        if not isinstance(path, str) or len(path) > 512 or "\\" in path or "\0" in path:
            raise ValueError(_ERROR)
        parsed = PurePosixPath(path)
        if (
            parsed.as_posix() != path
            or re.fullmatch(r"[A-Za-z0-9_./-]+", path) is None
            or parsed.is_absolute()
            or ".." in parsed.parts
            or str(parsed.parent) not in {str(PurePosixPath(name).parent) for name in RESOURCE_PATHS}
        ):
            raise ValueError(_ERROR)


def _validate_identity(value: object) -> None:
    if not isinstance(value, dict) or set(value) != _IDENTITY_KEYS:
        raise ValueError(_ERROR)
    if any(type(number) is not int or number < 0 for number in value.values()) or value["size"] > MAX_RESOURCE_BYTES:
        raise ValueError(_ERROR)


def _valid_digest(value: object) -> bool:
    return isinstance(value, str) and _DIGEST.fullmatch(value) is not None


def _read_events(content: bytes) -> list[dict[str, Any]]:
    if content and not content.endswith(b"\n"):
        raise ValueError(_ERROR)
    lines = content.splitlines()
    if len(lines) > MAX_EVENTS:
        raise ValueError(_ERROR)
    events = []
    for number, line in enumerate(lines):
        if len(line) + 1 > MAX_EVENT_BYTES:
            raise ValueError(_ERROR)
        record = strict_json_object(line)
        if type(record.get("sequence")) is not int or record.pop("sequence") != number:
            raise ValueError(_ERROR)
        _validate_event(record)
        events.append(record)
    return events
