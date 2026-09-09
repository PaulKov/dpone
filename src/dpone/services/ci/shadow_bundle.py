"""Canonical digest loading for the trusted CI shadow control-plane bundle."""

from __future__ import annotations

import hashlib
from pathlib import Path

import yaml

from dpone.services.ci.shadow import ShadowContractError, canonical_bytes


def load_bundle_entries(path: Path) -> tuple[dict[str, str], ...]:
    """Load the closed manifest entries trusted by the auditor composition root."""

    try:
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError) as exc:
        raise ShadowContractError("trusted bundle manifest is unavailable") from exc
    if not isinstance(payload, dict) or set(payload) != {"schema_version", "files"}:
        raise ShadowContractError("trusted bundle manifest is malformed")
    if payload.get("schema_version") != "dpone.ci-shadow-bundle.v1":
        raise ShadowContractError("trusted bundle schema is unsupported")
    files = payload.get("files")
    if not isinstance(files, list) or not files:
        raise ShadowContractError("trusted bundle files are malformed")
    normalized = [_entry(value) for value in files]
    if len({entry["path"] for entry in normalized}) != len(normalized):
        raise ShadowContractError("trusted bundle has duplicate paths")
    return tuple(normalized)


def load_bundle_digest(path: Path) -> str:
    """Validate a closed trusted manifest and return its canonical digest."""

    entries = load_bundle_entries(path)
    return (
        "sha256:"
        + hashlib.sha256(canonical_bytes({"schema_version": "dpone.ci-shadow-bundle.v1", "files": entries})).hexdigest()
    )


def _entry(value: object) -> dict[str, str]:
    if not isinstance(value, dict) or set(value) != {"path", "role", "sha256"}:
        raise ShadowContractError("trusted bundle entry is malformed")
    path, role, digest = value.get("path"), value.get("role"), value.get("sha256")
    if not isinstance(path, str) or not path or path.startswith("/") or ".." in Path(path).parts:
        raise ShadowContractError("trusted bundle path is unsafe")
    if role not in {"TRUST_CORE", "EXECUTION_WORKFLOW", "SUBJECT_INPUT"}:
        raise ShadowContractError("trusted bundle role is unsupported")
    if not isinstance(digest, str) or len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
        raise ShadowContractError("trusted bundle file digest is malformed")
    return {"path": path, "role": str(role), "sha256": digest}
