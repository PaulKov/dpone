"""Registry lint utilities (Step 10).

The landing/raw naming convention requires that source codes ({source}, {db})
are maintained in a source registry (host/type/etc.). The registry itself is a
separate YAML file (or a few files).

This module provides a lint that checks:
- which (src_system, src_database) pairs are used by manifests
- whether the registry contains an entry for each pair
- optionally, whether the entry contains required fields (e.g. host/type)

The lint is designed for CI and does NOT require applying the registry.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.errors import ManifestConfigurationError
from dpone.manifest.registry import load_registries
from dpone.manifest.validation import Severity


@dataclass(frozen=True, slots=True)
class RegistryLintIssue:
    severity: Severity
    code: str
    message: str
    manifest_path: Path
    src_system: str | None = None
    src_database: str | None = None


_LANDING_DS_RE = re.compile(r"^landing__([a-z][a-z0-9_]*)__([a-z][a-z0-9_]*)$")


def lint_registry(
    manifest_paths: Sequence[Path],
    *,
    registry_paths: Sequence[Path],
    require_fields: Sequence[str] = (),
    infer_from_single_sink_dataset: bool = True,
) -> list[RegistryLintIssue]:
    """Lint manifests against source registry.

    Args:
        manifest_paths: YAML manifest paths to scan.
        registry_paths: one or more registry YAML files.
        require_fields: registry vars that must be present for each entry (e.g. host, type).
        infer_from_single_sink_dataset: for legacy single manifests, try to infer
            (src_system, src_database) from sink.table.schema if it looks like
            'landing__{source}__{db}'.

    Returns:
        List of lint issues.
    """

    if not registry_paths:
        raise ManifestConfigurationError("registry_paths is required for lint_registry")

    registry = load_registries(registry_paths)

    issues: list[RegistryLintIssue] = []

    for path in manifest_paths:
        raw = _read_yaml_safe(path, issues)
        if raw is None:
            continue

        key = _extract_source_key(raw, infer_from_single_sink_dataset=infer_from_single_sink_dataset)
        if key is None:
            # Non-source manifests (or non-landing legacy manifests) are ignored.
            continue

        src_system, src_database = key

        entry = registry.lookup(src_system=src_system, src_database=src_database)
        if entry is None:
            issues.append(
                RegistryLintIssue(
                    severity=Severity.ERROR,
                    code="REGISTRY_MISSING_ENTRY",
                    message=f"Registry does not contain entry for src_system='{src_system}', src_database='{src_database}'",
                    manifest_path=path,
                    src_system=src_system,
                    src_database=src_database,
                )
            )
            continue

        for f in require_fields:
            if (
                f not in entry
                or entry.get(f) is None
                or (isinstance(entry.get(f), str) and not str(entry.get(f)).strip())
            ):
                issues.append(
                    RegistryLintIssue(
                        severity=Severity.ERROR,
                        code="REGISTRY_MISSING_FIELD",
                        message=f"Registry entry for src_system='{src_system}', src_database='{src_database}' is missing required field '{f}'",
                        manifest_path=path,
                        src_system=src_system,
                        src_database=src_database,
                    )
                )

    return issues


def _read_yaml_safe(path: Path, issues: list[RegistryLintIssue]) -> Mapping[str, Any] | None:
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as exc:
        issues.append(
            RegistryLintIssue(
                severity=Severity.ERROR,
                code="YAML_PARSE_ERROR",
                message=str(exc),
                manifest_path=path,
            )
        )
        return None

    if not isinstance(data, Mapping):
        return None
    return data


def _extract_source_key(
    raw: Mapping[str, Any],
    *,
    infer_from_single_sink_dataset: bool,
) -> tuple[str, str] | None:
    # Batch manifests: vars.src_system / vars.src_database
    kind = str(raw.get("kind") or "").strip()
    if kind == "dpone.batch.v1":
        vars_block = raw.get("vars")
        if not isinstance(vars_block, Mapping):
            return None
        src_system = vars_block.get("src_system")
        src_database = vars_block.get("src_database")
        if (
            isinstance(src_system, str)
            and src_system.strip()
            and isinstance(src_database, str)
            and src_database.strip()
        ):
            return (src_system.strip(), src_database.strip())
        return None

    if not infer_from_single_sink_dataset:
        return None

    # Legacy single manifests: best-effort inference from sink.table.schema
    sink = raw.get("sink")
    if not isinstance(sink, Mapping):
        return None
    table = sink.get("table")
    if not isinstance(table, Mapping):
        return None
    dataset = table.get("schema")
    if not isinstance(dataset, str) or not dataset.strip():
        return None

    ds = dataset.strip()

    # Best-effort: if starts with landing__, allow extra segments (legacy).
    # Prefer segment splitting over the greedy regex so
    # landing__demo_source__demo_db__archive resolves as:
    #   src_system=demo_source, src_database=demo_db__archive
    # instead of src_system=demo_source__demo_db, src_database=archive.
    if ds.startswith("landing__"):
        rest = ds[len("landing__") :]
        parts = [p for p in rest.split("__") if p]
        if len(parts) >= 2:
            src_system = parts[0]
            src_database = "__".join(parts[1:])
            return (src_system, src_database)

    # Strict standard pattern: landing__{source}__{db}
    m = _LANDING_DS_RE.match(ds)
    if m:
        return (m.group(1), m.group(2))

    return None
