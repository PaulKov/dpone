"""Bounded read-only recovery observations; never mutation authority."""

from __future__ import annotations

import os
from dataclasses import dataclass, replace
from itertools import islice
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID

from tools.dbt_self_service.starter_resource_journal_schema import (
    MAX_EVENT_BYTES,
    MAX_LOG_BYTES,
    MAX_RESOURCE_BYTES,
    METADATA_ROOT,
    RESOURCE_PATHS,
    validate_sidecar,
)
from tools.dbt_self_service.starter_resource_journal_schema import read_events as _read_events
from tools.dbt_self_service.starter_resource_journal_schema import validate_manifest as _validate_manifest

from dpone.contracts.strict_json import strict_json_object
from dpone.manifest.confined_files import read_confined_leaf
from dpone.manifest.confined_transaction_journal import transaction_journal_name
from dpone.manifest.project_root import ProjectRootIdentity, inspect_project_root
from dpone.readiness.airflow_authoring_directories import open_confined_parent

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
    rollback_outcomes: tuple[tuple[str, bool, bool], ...] = ()
    inverse_outcomes: tuple[tuple[str, bool], ...] = ()


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
                children = {entry.name for entry in islice(scan, 7)}
            if "recovery.json" in children:
                return _sidecar_report(parent.descriptor, identity, operation, directory, children)
            if not {"manifest.json", "events.jsonl"} <= children <= {"manifest.json", "events.jsonl", "old", "new"}:
                raise ValueError(_ERROR)
            manifest = strict_json_object(
                read_confined_leaf(parent.descriptor, "manifest.json", max_bytes=MAX_RESOURCE_BYTES).content
            )
            _validate_manifest(manifest, operation, identity.device, identity.inode)
            content = read_confined_leaf(parent.descriptor, "events.jsonl", max_bytes=MAX_LOG_BYTES).content
        events = _read_events(content, operation)
        unresolved: set[str] = set()
        observations = []
        recovery_paths: list[str] = []
        outcomes = []
        rollbacks = []
        inverses = []
        for event in events:
            if event["phase"] == "RECOVERY_REQUIRED" and "path" in event and "cleanup_required" in event:
                inverses.append((event["path"], event["cleanup_required"]))
            if "rollback" in event:
                item = event["rollback"]
                rollbacks.append((item["path"], item["removed"], item["preserved"]))
                recovery_paths.extend((item["path"], *item["directory_recovery_paths"]))
                if item["recovery_path"] is not None:
                    recovery_paths.append(item["recovery_path"])
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
        _verify_backups(root, directory, manifest, identity, recovery_paths)
        return RecoveryReport(
            True,
            events[-1]["phase"] if events else "PREPARING",
            operation,
            tuple(dict.fromkeys((directory.as_posix(), *RESOURCE_PATHS, *recovery_paths))),
            tuple(sorted(unresolved)),
            tuple(observations),
            tuple(outcomes),
            rollback_outcomes=tuple(rollbacks),
            inverse_outcomes=tuple(inverses),
        )
    except (OSError, ValueError, TypeError, KeyError):
        return RecoveryReport(True, "INVALID", paths=(METADATA_ROOT,))


def _sidecar_report(
    descriptor: int, identity: ProjectRootIdentity, operation: str, directory: Path, children: set[str]
) -> RecoveryReport:
    metadata = os.fstat(descriptor)
    value = strict_json_object(read_confined_leaf(descriptor, "recovery.json", max_bytes=MAX_EVENT_BYTES).content)
    validate_sidecar(value, operation, (identity.device, identity.inode), (metadata.st_dev, metadata.st_ino))
    item = value["rollback"]
    paths = [str(directory / "recovery.json"), item["path"], *item["directory_recovery_paths"]]
    if item["recovery_path"] is not None:
        paths.append(item["recovery_path"])
    allowed = {"manifest.json", "events.jsonl", "old", "new", "recovery.json"}
    allowed.update(PurePosixPath(path).name for path in paths if PurePosixPath(path).parent == PurePosixPath(directory))
    return RecoveryReport(
        True,
        "RECOVERY_REQUIRED" if children <= allowed else "INVALID",
        operation,
        tuple(dict.fromkeys(paths)),
        discovery_required=True,
        rollback_outcomes=((item["path"], item["removed"], item["preserved"]),),
    )


def _verify_backups(
    root: Path, directory: Path, manifest: dict[str, Any], identity: ProjectRootIdentity, recovery_paths: list[str]
) -> None:
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
            retained = {
                PurePosixPath(path).name
                for path in recovery_paths
                if PurePosixPath(path).parent == PurePosixPath(directory / kind)
            }
            with os.scandir(parent.descriptor) as scan:
                names = [entry.name for entry in islice(scan, len(RESOURCE_PATHS) + len(retained) + 1)]
            if not set(names) <= set(expected) | retained:
                raise ValueError(_ERROR)
            for name in names:
                if name not in expected:
                    continue
                observed = read_confined_leaf(parent.descriptor, name, max_bytes=MAX_RESOURCE_BYTES)
                if observed.sha256 != expected[name]:
                    raise ValueError(_ERROR)
