"""Pure closed codec and bounds for starter resource transaction evidence.

This module performs no filesystem access and grants no mutation authority.
"""

from __future__ import annotations

import re
from pathlib import PurePosixPath
from typing import Any
from uuid import UUID

from dpone.adapters.dbt_starter_resources import _PACKAGE_FILES
from dpone.contracts.strict_json import strict_json_object
from dpone.manifest.bounded_yaml import BoundedYamlLimits

RESOURCE_PATHS = tuple("src/dpone/_assets/dbt_dpone/" + name for name in _PACKAGE_FILES) + (
    "src/dpone/_assets/dbt_starter/v4/packages.yml",
    "src/dpone/_assets/dbt_starter/v4/package-lock.yml",
)
MAX_RESOURCE_BYTES = BoundedYamlLimits().max_bytes
MAX_EVENTS = len(RESOURCE_PATHS) * 9 + 8
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


def validate_manifest(value: dict[str, Any], operation: str, device: int, inode: int) -> None:
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


def validate_event(event: dict[str, Any], operation: str | None = None) -> None:
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
            "rollback",
        }
        or event.get("phase") not in _PHASES
    ):
        raise ValueError(_ERROR)
    if "rollback" in event:
        if operation is None or event["phase"] != "RECOVERY_REQUIRED":
            raise ValueError(_ERROR)
        validate_rollback(event["rollback"], operation)
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
    if paths and "path" not in event:
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
        target = PurePosixPath(event["path"])
        operation_siblings = set()
        if operation is not None:
            if str(UUID(operation)) != operation:
                raise ValueError(_ERROR)
            operation_siblings = {f".{target.name}.{operation}.{suffix}" for suffix in ("new", "restore")}
        if parsed.parent != target.parent or (
            parsed.name != f".{target.name}.dpone-transaction.json"
            and parsed.name not in operation_siblings
            and re.fullmatch(r"\.dpone-(?:rollback|recovery)-[0-9a-f]{32}", parsed.name) is None
        ):
            raise ValueError(_ERROR)


def _validate_identity(value: object) -> None:
    if not isinstance(value, dict) or set(value) != _IDENTITY_KEYS:
        raise ValueError(_ERROR)
    if any(type(number) is not int or number < 0 for number in value.values()) or value["size"] > MAX_RESOURCE_BYTES:
        raise ValueError(_ERROR)


def _valid_digest(value: object) -> bool:
    return isinstance(value, str) and _DIGEST.fullmatch(value) is not None


def validate_rollback(value: object, operation: str) -> None:
    """Accept only outcomes tied to a captured creation and its directory receipts."""
    if not isinstance(value, dict) or set(value) != {
        "path",
        "device",
        "inode",
        "directories",
        "removed",
        "preserved",
        "recovery_path",
        "directory_recovery_paths",
    }:
        raise ValueError(_ERROR)
    path = _created_path(value["path"], operation)
    if any(type(value[key]) is not int or value[key] < 0 for key in ("device", "inode")):
        raise ValueError(_ERROR)
    if any(type(value[key]) is not bool for key in ("removed", "preserved")):
        raise ValueError(_ERROR)
    directories = value["directories"]
    if not isinstance(directories, list) or len(directories) > 8:
        raise ValueError(_ERROR)
    owned = []
    for directory in directories:
        if not isinstance(directory, dict) or set(directory) != {"path", "device", "inode"}:
            raise ValueError(_ERROR)
        if not isinstance(directory["path"], str) or directory["path"] not in {
            str(parent) for parent in path.parents if str(parent) != "."
        }:
            raise ValueError(_ERROR)
        if any(type(directory[key]) is not int or directory[key] < 0 for key in ("device", "inode")):
            raise ValueError(_ERROR)
        owned.append(directory["path"])
    unresolved = value["directory_recovery_paths"]
    if not isinstance(unresolved, list) or len(unresolved) > 8 or any(item not in owned for item in unresolved):
        raise ValueError(_ERROR)
    if len(owned) != len(set(owned)):
        raise ValueError(_ERROR)
    recovery = value["recovery_path"]
    if recovery is not None:
        if not isinstance(recovery, str) or recovery != str(PurePosixPath(recovery)):
            raise ValueError(_ERROR)
        parsed = PurePosixPath(recovery)
        if (
            parsed.parent != path.parent
            or re.fullmatch(r"\.dpone-(?:rollback|recovery)-[0-9a-f]{32}", parsed.name) is None
        ):
            raise ValueError(_ERROR)


def _created_path(value: object, operation: str) -> PurePosixPath:
    if str(UUID(operation)) != operation or not isinstance(value, str):
        raise ValueError(_ERROR)
    directory = PurePosixPath(METADATA_ROOT) / operation
    allowed = set(RESOURCE_PATHS)
    allowed.update(str(directory / name) for name in ("manifest.json", "events.jsonl", "recovery.json"))
    allowed.update(
        str(directory / kind / f"{index:03d}.bin") for kind in ("old", "new") for index in range(len(RESOURCE_PATHS))
    )
    for resource in RESOURCE_PATHS:
        target = PurePosixPath(resource)
        allowed.update(str(target.with_name(f".{target.name}.{operation}.{suffix}")) for suffix in ("new", "restore"))
    if value not in allowed:
        raise ValueError(_ERROR)
    return PurePosixPath(value)


def validate_sidecar(value: dict[str, Any], operation: str, root: tuple[int, int], directory: tuple[int, int]) -> None:
    """Bind failure evidence to both the source root and the retained operation directory."""
    if set(value) != {"schema", "operation", "root", "directory", "rollback"}:
        raise ValueError(_ERROR)
    if value["schema"] != "dpone.starter-resource-recovery.v1" or value["operation"] != operation:
        raise ValueError(_ERROR)
    for key, expected in (("root", root), ("directory", directory)):
        identity = value[key]
        if not isinstance(identity, dict) or set(identity) != {"device", "inode"}:
            raise ValueError(_ERROR)
        if any(type(identity[name]) is not int for name in ("device", "inode")):
            raise ValueError(_ERROR)
        if (identity["device"], identity["inode"]) != expected:
            raise ValueError(_ERROR)
    validate_rollback(value["rollback"], operation)


def read_events(content: bytes, operation: str | None = None) -> list[dict[str, Any]]:
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
        validate_event(record, operation)
        events.append(record)
    return events
