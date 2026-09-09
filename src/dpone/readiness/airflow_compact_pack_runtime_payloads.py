"""Validate and materialize compact-pack runtime payloads.

The compact Airflow release builder treats ``runtime_payload_ids`` as an
ordered workload contract while storing the corresponding release inventory in
stable identifier order.  This module owns that boundary: reference
validation, canonical dbt ordering, on-disk locations, payload bytes, and
release descriptors.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from dpone.manifest.confined_files import ConfinedFileError, read_confined_file
from dpone.manifest.project_root import ProjectRootError, ProjectRootIdentity, inspect_project_root

_DBT_PROJECT_MEDIA_TYPE = "application/vnd.dpone.dbt-project-bundle+gzip"
_DBT_MANIFEST_MEDIA_TYPE = "application/vnd.dbt.manifest+json"
_DBT_SELECTION_MEDIA_TYPE = "application/vnd.dpone.dbt-selection-lock+json"
# Keep ``<workflow>.selection-lock.json`` within portable 255-byte leaf limits.
# The workflow alphabet is ASCII, so characters and encoded bytes align.
_SELECTION_ID = re.compile(r"^dbt_selection_([A-Za-z0-9_.-]{1,235})$")
_RUNTIME_PAYLOAD_ID = re.compile(r"^[A-Za-z0-9_.-]{1,256}$")
_MAX_RUNTIME_PAYLOADS = 64
_MAX_RUNTIME_PAYLOAD_BYTES = 256 * 1024 * 1024
# This compact-v1 producer bound is intentionally fixed independently of
# configurable publication/runtime limits.  Changing it is a public
# compatibility-contract change, not a local tuning knob.
_MAX_RUNTIME_PAYLOAD_TOTAL_BYTES = 512 * 1024 * 1024
_ErrorFactory = Callable[[str, str], Exception]


def _runtime_payload_ids_from_pack(
    pack: Mapping[str, Any],
    *,
    error_factory: _ErrorFactory,
) -> tuple[str, ...]:
    """Return validated payload IDs without changing their declared order."""

    value = pack.get("runtime_payload_ids")
    if value is None:
        return ()
    if not isinstance(value, list) or not value:
        raise error_factory(
            "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_REFS_INVALID",
            "pack runtime_payload_ids must be a non-empty array when present",
        )
    if len(value) > 16 or any(
        not isinstance(item, str) or _RUNTIME_PAYLOAD_ID.fullmatch(item) is None for item in value
    ):
        raise error_factory(
            "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_REFS_INVALID",
            "pack runtime_payload_ids must be unique bounded logical ids",
        )
    result = tuple(value)
    if len(result) != len(set(result)):
        raise error_factory(
            "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_REFS_INVALID",
            "pack runtime_payload_ids must be unique",
        )
    # Preserve pack order: init-fetch plans select payloads in this sequence,
    # and dbt runtime identity requires project, manifest, then selection lock.
    _require_canonical_dbt_runtime_payload_ids(result, error_factory=error_factory)
    return result


def _materialize_runtime_payloads(
    *,
    pack_root: Path,
    pack_artifacts: Sequence[Mapping[str, Any]],
    error_factory: _ErrorFactory,
) -> tuple[dict[str, bytes], list[dict[str, Any]]]:
    """Read every referenced payload and build its deterministic inventory."""

    required: set[str] = set()
    for pack in pack_artifacts:
        refs = pack.get("runtime_payload_ids")
        if isinstance(refs, list):
            required.update(str(item) for item in refs if isinstance(item, str))
    if not required:
        return {}, []
    if len(required) > _MAX_RUNTIME_PAYLOADS:
        raise error_factory(
            "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_REFS_INVALID",
            f"release runtime payload inventory exceeds {_MAX_RUNTIME_PAYLOADS} unique ids",
        )
    try:
        root_identity = inspect_project_root(pack_root)
    except ProjectRootError as exc:
        raise error_factory(
            "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_UNSAFE",
            "runtime payload root cannot be identified safely",
        ) from exc
    assert root_identity is not None

    files: dict[str, bytes] = {}
    descriptors: list[dict[str, Any]] = []
    total_bytes = 0
    for payload_id in sorted(required):
        relative, kind, media_type = _runtime_payload_locator(payload_id, error_factory=error_factory)
        remaining_bytes = _MAX_RUNTIME_PAYLOAD_TOTAL_BYTES - total_bytes
        if remaining_bytes <= 0:
            raise error_factory(
                "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_TOTAL_LIMIT_EXCEEDED",
                "release runtime payload inventory exceeds the aggregate byte limit",
            )
        max_bytes = min(_MAX_RUNTIME_PAYLOAD_BYTES, remaining_bytes)
        payload = _read_runtime_payload(
            relative,
            pack_root=pack_root,
            root_identity=root_identity,
            payload_id=payload_id,
            max_bytes=max_bytes,
            aggregate_limited=max_bytes < _MAX_RUNTIME_PAYLOAD_BYTES,
            error_factory=error_factory,
        )
        total_bytes += len(payload)
        files[relative] = payload
        descriptors.append(
            {
                "id": payload_id,
                "kind": kind,
                "path": relative,
                "sha256": _sha256_bytes(payload),
                "bytes": len(payload),
                "media_type": media_type,
            }
        )
    return files, descriptors


def _read_runtime_payload(
    relative_path: str,
    *,
    pack_root: Path,
    root_identity: ProjectRootIdentity,
    payload_id: str,
    max_bytes: int,
    aggregate_limited: bool,
    error_factory: _ErrorFactory,
) -> bytes:
    """Read one stable regular file through the canonical confinement boundary."""

    try:
        payload = read_confined_file(
            pack_root,
            relative_path,
            max_bytes=max_bytes,
            root_identity=root_identity,
        )
    except ConfinedFileError as exc:
        code = _confined_payload_error_code(exc.code, aggregate_limited=aggregate_limited)
        raise error_factory(code, f"runtime payload {payload_id!r} cannot be read safely") from exc
    except OSError as exc:
        raise error_factory(
            "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_UNREADABLE",
            f"runtime payload {payload_id!r} is unreadable",
        ) from exc
    if not payload:
        raise error_factory(
            "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_EMPTY",
            f"runtime payload {payload_id!r} is empty",
        )
    return payload


def _confined_payload_error_code(code: str, *, aggregate_limited: bool) -> str:
    if code == "file_not_found":
        return "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_MISSING"
    if code == "file_too_large":
        if aggregate_limited:
            return "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_TOTAL_LIMIT_EXCEEDED"
        return "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_OVERSIZED"
    if code in {"path_invalid", "symlink_forbidden", "not_regular_file", "root_changed", "source_changed"}:
        return "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_UNSAFE"
    return "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_UNREADABLE"


def _require_canonical_dbt_runtime_payload_ids(
    payload_ids: tuple[str, ...],
    *,
    error_factory: _ErrorFactory,
) -> None:
    """Fail closed when a dbt payload trio is present but not in contract order."""

    selection_ids = [item for item in payload_ids if _SELECTION_ID.fullmatch(item)]
    dbt_ids = {"dbt_project", "dbt_manifest", *selection_ids}
    if not dbt_ids.intersection(payload_ids):
        return
    if len(selection_ids) != 1 or set(payload_ids) != {"dbt_project", "dbt_manifest", selection_ids[0]}:
        raise error_factory(
            "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_REFS_INVALID",
            "dbt runtime_payload_ids must be exactly "
            f"[dbt_project, dbt_manifest, dbt_selection_<workflow>] (got {list(payload_ids)!r})",
        )
    expected = ("dbt_project", "dbt_manifest", selection_ids[0])
    if payload_ids != expected:
        raise error_factory(
            "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_ORDER_INVALID",
            "dbt runtime_payload_ids order must be "
            f"{list(expected)!r} (got {list(payload_ids)!r}); "
            "sorted/rewritten order breaks verified dbt runtime identity",
        )


def _runtime_payload_locator(
    payload_id: str,
    *,
    error_factory: _ErrorFactory,
) -> tuple[str, str, str]:
    if payload_id == "dbt_project":
        return "runtime/dbt/project.tar.gz", "dbt_project_bundle", _DBT_PROJECT_MEDIA_TYPE
    if payload_id == "dbt_manifest":
        return "runtime/dbt/manifest.json", "dbt_manifest", _DBT_MANIFEST_MEDIA_TYPE
    match = _SELECTION_ID.fullmatch(payload_id)
    if match is not None:
        workflow = match.group(1)
        return (
            f"runtime/dbt/{workflow}.selection-lock.json",
            "dbt_selection_lock",
            _DBT_SELECTION_MEDIA_TYPE,
        )
    raise error_factory(
        "DPONE_COMPACT_PACK_RELEASE_RUNTIME_PAYLOAD_UNSUPPORTED",
        f"unsupported runtime_payload_id {payload_id!r} for compact release materialize",
    )


def _sha256_bytes(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()
