"""Bounded textual updates for domain workload-catalog membership."""

from __future__ import annotations

import re
from pathlib import Path

from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.manifest.domain_catalog_membership import catalog_membership_entry_text
from dpone.readiness.workload_init_catalog_storage import CATALOG_MAX_BYTES

_YAML_LIMITS = BoundedYamlLimits(max_bytes=CATALOG_MAX_BYTES)
_WORKLOADS_BLOCK_HEADER = re.compile(r"^workloads:\s*(?:#.*)?$")
_WORKLOADS_FLOW_HEADER = re.compile(r"^workloads:\s*(\{\}|\[\])\s*(?:#.*)?$")
_WORKLOAD_KEY_LINE = re.compile(r"^(\s+)(\S+):\s*(?:#.*)?$")


class CatalogTextError(ValueError):
    """Catalog bytes cannot be parsed or updated without ambiguity."""


def catalog_payload(text: str, catalog_path: Path) -> dict[str, object]:
    """Parse one bounded catalog mapping."""

    try:
        loaded = load_bounded_yaml(text.encode("utf-8"), limits=_YAML_LIMITS)
    except BoundedYamlError as exc:
        raise CatalogTextError(f"Domain catalog {catalog_path.as_posix()} is not safe YAML: {exc}") from exc
    if loaded is None:
        return {}
    if not isinstance(loaded, dict):
        raise CatalogTextError(f"Domain catalog {catalog_path.as_posix()} root must be a mapping.")
    return loaded


def registered_manifest(payload: dict[str, object], pipeline_id: str) -> str | None:
    """Return the registered manifest, including an empty invalid value."""

    workloads = payload.get("workloads")
    if not isinstance(workloads, dict) or pipeline_id not in workloads:
        return None
    entry = workloads[pipeline_id]
    manifest = entry.get("manifest") if isinstance(entry, dict) else None
    return str(manifest) if manifest is not None else ""


def _workloads_header_kind(line: str) -> str | None:
    stripped = line.rstrip("\r\n").strip()
    if _WORKLOADS_FLOW_HEADER.match(stripped):
        return "flow"
    if _WORKLOADS_BLOCK_HEADER.match(stripped):
        return "block"
    return None


def insert_workload_entry(text: str, pipeline_id: str, manifest_ref: str) -> str:
    """Insert one entry while preserving comments and the existing key order."""

    lines = text.splitlines(keepends=True)
    block_start = next(
        (index for index, line in enumerate(lines) if _workloads_header_kind(line) is not None),
        None,
    )
    key_indent = "  "
    if block_start is None:
        suffix = "" if not text or text.endswith("\n") else "\n"
        entry = catalog_membership_entry_text(pipeline_id, manifest_ref, key_indent=key_indent)
        return f"{text}{suffix}workloads:\n{entry}"
    block_end = len(lines)
    existing_keys: list[tuple[int, str]] = []
    block_key_indent: str | None = None
    for index in range(block_start + 1, len(lines)):
        line = lines[index]
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if not line[:1].isspace():
            block_end = index
            break
        key_match = _WORKLOAD_KEY_LINE.match(line.rstrip("\r\n"))
        if key_match is None:
            continue
        indent = key_match.group(1)
        if block_key_indent is None:
            block_key_indent = indent
        if indent == block_key_indent:
            existing_keys.append((index, key_match.group(2)))
    if block_key_indent is not None:
        key_indent = block_key_indent
    entry = catalog_membership_entry_text(pipeline_id, manifest_ref, key_indent=key_indent)
    if _workloads_header_kind(lines[block_start]) == "flow":
        lines[block_start] = "workloads:\n"
        lines.insert(block_start + 1, entry)
        return "".join(lines)
    insert_at = block_end
    keys = [key for _, key in existing_keys]
    if keys == sorted(keys):
        insert_at = next((index for index, key in existing_keys if pipeline_id < key), block_end)
    if insert_at > 0 and not lines[insert_at - 1].endswith("\n"):
        lines[insert_at - 1] += "\n"
    lines.insert(insert_at, entry)
    return "".join(lines)


def require_inserted_manifest(
    desired: str,
    catalog_path: Path,
    pipeline_id: str,
    manifest_ref: str,
) -> None:
    """Reparse candidate bytes and prove the intended membership value."""

    workloads = catalog_payload(desired, catalog_path).get("workloads")
    entry = workloads.get(pipeline_id) if isinstance(workloads, dict) else None
    manifest = entry.get("manifest") if isinstance(entry, dict) else None
    if manifest != manifest_ref:
        raise CatalogTextError(
            f"Membership self-check failed for {catalog_path.as_posix()}: "
            f"inserted manifest {manifest!r} != expected {manifest_ref!r}."
        )


__all__ = [
    "CatalogTextError",
    "catalog_payload",
    "insert_workload_entry",
    "registered_manifest",
    "require_inserted_manifest",
]
