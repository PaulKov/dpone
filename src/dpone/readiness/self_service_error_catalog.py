"""Self-service error catalog with remediation hints and doc links."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any

from dpone.readiness.airflow_self_service_models import Change
from dpone.readiness.error_contract import dpone_error, error_docs_url, manual_fix

_CODE_SCAFFOLD_CONFLICT = "DPONE_WORKLOAD_INIT_SCAFFOLD_CONFLICT"
_CODE_DOMAIN_CONFLICT = "DPONE_WORKLOAD_INIT_DOMAIN_CONFLICT"
_CODE_OWNERSHIP_CONFLICT = "DPONE_WORKLOAD_INIT_OWNERSHIP_CONFLICT"
_CODE_MANIFEST_INVALID = "DPONE_WORKLOAD_INIT_MANIFEST_INVALID"
_CODE_BATCH_VAR_RESERVED = "DPONE_WORKLOAD_INIT_BATCH_VAR_RESERVED"
_CODE_INVALID_REFERENCE = "DPONE_WORKLOAD_INIT_INVALID_REFERENCE"

_RESERVED_VAR_RE = re.compile(
    r"(?:Переменная|Variable)\s+'(?P<var>[^']+)'\s+"
    r"(?:зарезервирована|is reserved)",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ErrorCatalogEntry:
    code: str
    summary: str
    docs_path: str
    default_fixes: tuple[dict[str, str], ...] = ()


_CATALOG: dict[str, ErrorCatalogEntry] = {
    _CODE_SCAFFOLD_CONFLICT: ErrorCatalogEntry(
        code=_CODE_SCAFFOLD_CONFLICT,
        summary="Generated scaffold file already exists with different content.",
        docs_path="docs/errors/DPONE_WORKLOAD_INIT_SCAFFOLD_CONFLICT.md",
        default_fixes=(
            manual_fix("review_scaffold_diff"),
            manual_fix("rename_workload_or_merge_manually"),
        ),
    ),
    _CODE_DOMAIN_CONFLICT: ErrorCatalogEntry(
        code=_CODE_DOMAIN_CONFLICT,
        summary="Domain catalog already declares this workload id.",
        docs_path="docs/errors/DPONE_WORKLOAD_INIT_DOMAIN_CONFLICT.md",
        default_fixes=(
            manual_fix("choose_new_workload_id", command="dpone workload init --help"),
            manual_fix("update_domain_catalog_manually"),
        ),
    ),
    _CODE_OWNERSHIP_CONFLICT: ErrorCatalogEntry(
        code=_CODE_OWNERSHIP_CONFLICT,
        summary="Ownership entry already exists with a different owner.",
        docs_path="docs/errors/DPONE_WORKLOAD_INIT_OWNERSHIP_CONFLICT.md",
        default_fixes=(
            manual_fix("align_owner_flag", command="dpone workload init --owner <team>"),
            manual_fix("update_ownership_manually"),
        ),
    ),
    _CODE_MANIFEST_INVALID: ErrorCatalogEntry(
        code=_CODE_MANIFEST_INVALID,
        summary="Generated manifest failed metadata validation.",
        docs_path="docs/errors/DPONE_WORKLOAD_INIT_MANIFEST_INVALID.md",
        default_fixes=(
            manual_fix("fix_manifest_fields"),
            manual_fix("rerun_workload_init_apply", command="dpone workload init <domain>/<id> --apply"),
        ),
    ),
    _CODE_BATCH_VAR_RESERVED: ErrorCatalogEntry(
        code=_CODE_BATCH_VAR_RESERVED,
        summary="Batch manifest uses a reserved template variable name.",
        docs_path="docs/errors/DPONE_WORKLOAD_INIT_BATCH_VAR_RESERVED.md",
        default_fixes=(
            manual_fix("remove_reserved_batch_var_override"),
            manual_fix("switch_to_catalog_layout", command="dpone workload init <domain>/<id> --layout catalog"),
        ),
    ),
    _CODE_INVALID_REFERENCE: ErrorCatalogEntry(
        code=_CODE_INVALID_REFERENCE,
        summary="Workload reference or domain flags are invalid.",
        docs_path="docs/errors/DPONE_WORKLOAD_INIT_INVALID_REFERENCE.md",
        default_fixes=(
            manual_fix(
                "use_domain_workload_reference", command="dpone workload init marketing/my_app --source ... --sink ..."
            ),
        ),
    ),
}


def catalog_entry(code: str) -> ErrorCatalogEntry | None:
    return _CATALOG.get(code)


def classify_manifest_validation(message: str) -> str:
    if _RESERVED_VAR_RE.search(message):
        return _CODE_BATCH_VAR_RESERVED
    return _CODE_MANIFEST_INVALID


def enrich_self_service_error(
    code: str,
    message: str,
    *,
    stage: str,
    path: str | None = None,
    entity: dict[str, str] | None = None,
    fixes: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    entry = catalog_entry(code)
    docs_url = entry.docs_path if entry else error_docs_url(code)
    resolved_fixes = fixes if fixes is not None else list(entry.default_fixes) if entry else []
    payload = dpone_error(
        code,
        message,
        stage=stage,
        path=path,
        entity=entity,
        fixes=resolved_fixes,
        docs_url=docs_url,
    )
    if entry:
        payload["summary"] = entry.summary
    return payload


def errors_from_workload_conflicts(
    changes: tuple[Change, ...],
    *,
    domain: str,
    workload_id: str,
) -> tuple[dict[str, Any], ...]:
    errors: list[dict[str, Any]] = []
    entity = {"kind": "workload", "id": f"{domain}/{workload_id}"}
    for change in changes:
        if change.action != "conflict":
            continue
        code = _conflict_code(change)
        message = change.message or f"conflict at {change.path}"
        errors.append(
            enrich_self_service_error(
                code,
                message,
                stage="workload_init",
                path=change.path or None,
                entity=entity,
            )
        )
    return tuple(errors)


def manifest_validation_error(*, message: str, path: str | None, domain: str, workload_id: str) -> dict[str, Any]:
    code = classify_manifest_validation(message)
    return enrich_self_service_error(
        code,
        message,
        stage="validate",
        path=path,
        entity={"kind": "workload", "id": f"{domain}/{workload_id}"},
    )


def invalid_reference_error(message: str) -> dict[str, Any]:
    return enrich_self_service_error(
        _CODE_INVALID_REFERENCE,
        message,
        stage="workload_init",
    )


def _conflict_code(change: Change) -> str:
    path = change.path or ""
    message = change.message or ""
    if "domain catalog" in message or "/domains/" in path:
        return _CODE_DOMAIN_CONFLICT
    if path == "ownership.yaml" or "ownership for" in message:
        return _CODE_OWNERSHIP_CONFLICT
    return _CODE_SCAFFOLD_CONFLICT


__all__ = [
    "ErrorCatalogEntry",
    "catalog_entry",
    "classify_manifest_validation",
    "enrich_self_service_error",
    "errors_from_workload_conflicts",
    "invalid_reference_error",
    "manifest_validation_error",
]
