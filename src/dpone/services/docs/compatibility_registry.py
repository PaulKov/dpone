from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .generated_block import (
    is_generated_doc_in_sync,
    sync_generated_doc,
)

MATRIX_START = "<!-- DPONE_COMPAT_MATRIX_START -->"
MATRIX_END = "<!-- DPONE_COMPAT_MATRIX_END -->"

EXPECTED_DEPRECATED_PATHS: tuple[str, ...] = (
    "dpone.yaml_config_handler",
    "dpone.source",
    "dpone.sink",
    "dpone.etl",
    "dpone.etl_logging",
    "dpone.credentials",
    "dpone.state",
    "dpone.reconciliation",
    "dpone.sql_helpers",
    "dpone.xmin",
    "dpone.lib.connectors",
    "dpone.core.artifacts",
    "dpone.core.errors",
    "dpone.core.etl_types",
    "dpone.core.runtime",
    "dpone.lib.technical_columns",
    "dpone.lib.connectors.base",
    "dpone.lib.utils.data_type_mapper",
    "dpone.lib.utils.timezone_converter",
)


@dataclass(frozen=True)
class CompatibilityEntry:
    deprecated: str
    canonical: str
    scope: str
    status: str
    removal: str
    notes: str = ""
    removal_batch: str = ""


@dataclass(frozen=True)
class CompatibilityIssue:
    kind: str
    subject: str
    detail: str


@dataclass(frozen=True)
class CompatibilityReport:
    registry_path: str
    doc_path: str
    entry_count: int
    issues: tuple[CompatibilityIssue, ...]
    doc_synced: bool

    @property
    def ok(self) -> bool:
        return not self.issues and self.doc_synced

    @property
    def issue_count(self) -> int:
        return len(self.issues)


def load_registry(path: Path, *, yaml_codec: Any) -> list[CompatibilityEntry]:
    payload = yaml_codec.load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Compatibility registry must be a mapping: {path}")
    raw_entries = payload.get("entries")
    if not isinstance(raw_entries, list):
        raise ValueError(f"Compatibility registry must contain an 'entries' list: {path}")

    entries: list[CompatibilityEntry] = []
    for idx, item in enumerate(raw_entries):
        if not isinstance(item, dict):
            raise ValueError(f"Registry entry #{idx} must be a mapping")
        deprecated = str(item.get("deprecated", "")).strip()
        canonical = str(item.get("canonical", "")).strip()
        scope = str(item.get("scope", "")).strip()
        status = str(item.get("status", "")).strip()
        removal = str(item.get("removal", "")).strip()
        notes = str(item.get("notes", "") or "").strip()
        removal_batch = str(item.get("removal_batch", "") or "").strip()
        if not all([deprecated, canonical, scope, status, removal]):
            raise ValueError(f"Registry entry #{idx} must define deprecated/canonical/scope/status/removal")
        entries.append(
            CompatibilityEntry(
                deprecated=deprecated,
                canonical=canonical,
                scope=scope,
                status=status,
                removal=removal,
                notes=notes,
                removal_batch=removal_batch,
            )
        )
    return entries


def validate_registry(entries: list[CompatibilityEntry], *, package_dir: Path) -> tuple[CompatibilityIssue, ...]:
    issues: list[CompatibilityIssue] = []
    by_deprecated: dict[str, CompatibilityEntry] = {}
    for entry in entries:
        if entry.deprecated in by_deprecated:
            issues.append(
                CompatibilityIssue(
                    kind="duplicate-entry",
                    subject=entry.deprecated,
                    detail="Deprecated path appears more than once in compatibility registry.",
                )
            )
            continue
        by_deprecated[entry.deprecated] = entry
        if entry.deprecated == entry.canonical:
            issues.append(
                CompatibilityIssue(
                    kind="self-mapping",
                    subject=entry.deprecated,
                    detail="Deprecated path must not map to itself.",
                )
            )
        if not _module_like_exists(package_dir, entry.deprecated):
            issues.append(
                CompatibilityIssue(
                    kind="missing-deprecated-path",
                    subject=entry.deprecated,
                    detail="Deprecated module/package path does not exist in src/dpone.",
                )
            )
        if not _module_like_exists(package_dir, entry.canonical):
            issues.append(
                CompatibilityIssue(
                    kind="missing-canonical-path",
                    subject=entry.canonical,
                    detail=f"Canonical target for {entry.deprecated} does not exist in src/dpone.",
                )
            )

    for expected in EXPECTED_DEPRECATED_PATHS:
        if expected not in by_deprecated:
            issues.append(
                CompatibilityIssue(
                    kind="missing-registry-entry",
                    subject=expected,
                    detail="Expected deprecated shim is missing from compatibility registry.",
                )
            )
    return tuple(sorted(issues, key=lambda x: (x.kind, x.subject)))


def render_compatibility_matrix(entries: list[CompatibilityEntry]) -> str:
    lines = [
        MATRIX_START,
        "| Deprecated path | Canonical path | Scope | Status | Removal policy |",
        "|---|---|---|---|---|",
    ]
    for entry in sorted(entries, key=lambda x: x.deprecated):
        lines.append(
            f"| `{entry.deprecated}` | `{entry.canonical}` | {entry.scope} | {entry.status} | {entry.removal} |"
        )
        if entry.notes:
            lines.append(f"|  |  |  |  | _{entry.notes}_ |")
    lines.append(MATRIX_END)
    return "\n".join(lines)


def sync_compatibility_doc(doc_path: Path, *, rendered_matrix: str) -> tuple[bool, str]:
    return sync_generated_doc(
        doc_path,
        rendered_block=rendered_matrix,
        start_marker=MATRIX_START,
        end_marker=MATRIX_END,
    )


def is_compatibility_doc_in_sync(doc_path: Path, *, rendered_matrix: str) -> bool:
    return is_generated_doc_in_sync(
        doc_path,
        rendered_block=rendered_matrix,
        start_marker=MATRIX_START,
        end_marker=MATRIX_END,
    )


def format_compatibility_report_text(report: CompatibilityReport) -> str:
    if report.ok:
        return f"✅ Compatibility policy OK: {report.entry_count} registry entries, documentation block is in sync."
    lines = [
        "Compatibility policy check failed:",
        f"- registry: {report.registry_path}",
        f"- doc: {report.doc_path}",
        f"- entries: {report.entry_count}",
        f"- doc_synced: {'yes' if report.doc_synced else 'no'}",
    ]
    for issue in report.issues:
        lines.append(f"- {issue.kind}: {issue.subject}")
        lines.append(f"    {issue.detail}")
    return "\n".join(lines)


def format_compatibility_report_jsonable(report: CompatibilityReport) -> dict:
    return {
        "ok": report.ok,
        "registry_path": report.registry_path,
        "doc_path": report.doc_path,
        "entry_count": report.entry_count,
        "doc_synced": report.doc_synced,
        "issue_count": report.issue_count,
        "issues": [
            {
                "kind": issue.kind,
                "subject": issue.subject,
                "detail": issue.detail,
            }
            for issue in report.issues
        ],
    }


def _module_like_exists(package_dir: Path, module_name: str) -> bool:
    if not module_name.startswith("dpone"):
        return False
    rel_parts = module_name.split(".")[1:]
    if not rel_parts:
        return False
    base = package_dir.joinpath(*rel_parts)
    return base.is_dir() or base.with_suffix(".py").exists()
