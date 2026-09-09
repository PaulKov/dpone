"""Additive GitOps catalog patches for workload init."""

from __future__ import annotations

import difflib
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from dpone.manifest.bounded_yaml import BoundedYamlError, BoundedYamlLimits, load_bounded_yaml
from dpone.readiness.workload_init_catalog_storage import (
    CATALOG_MAX_BYTES,
    CatalogPatchConflict,
    CatalogPatchReceipt,
    apply_catalog_content,
    read_catalog_text,
    rollback_catalog_patch,
    verify_catalog_content,
)
from dpone.security_redaction import redact_text

_CATALOG_DIFF_MAX_BYTES = 16 * 1024
_CATALOG_DIFF_INPUT_MAX_CHARS = _CATALOG_DIFF_MAX_BYTES * 4
_DIFF_TRUNCATION_MARKER = "\n... [diff truncated by dpone]\n"
_OWNERSHIP_SCHEMA = "dpone.ownership.v1"
_CATALOG_YAML_LIMITS = BoundedYamlLimits(max_bytes=CATALOG_MAX_BYTES)


class _CatalogShapeError(ValueError):
    """Existing catalog YAML cannot be changed additively without data loss."""


@dataclass(frozen=True)
class CatalogPatchPlan:
    path: Path
    action: str
    reason: str | None = None
    diff: str | None = None
    desired_text: str | None = None
    expected_text: str | None = None


def plan_domain_catalog_patch(
    *,
    repo_root: Path,
    path: Path,
    domain: str,
    workload_id: str,
    manifest_ref: str,
    dag_id: str | None,
    dag_declaration: dict[str, Any] | None,
) -> CatalogPatchPlan:
    existing_text, conflict = _read_for_plan(repo_root, path)
    if conflict is not None:
        return conflict
    try:
        payload = _yaml_mapping(existing_text)
        workloads = _validate_domain_catalog(payload, expected_domain=domain)
        workload_exists = workload_id in workloads
        if workload_exists:
            existing_workload = workloads[workload_id]
            if existing_workload.get("manifest") != manifest_ref:
                return CatalogPatchPlan(
                    path=path,
                    action="conflict",
                    reason=f"workload {workload_id!r} already declared in domain catalog",
                    expected_text=existing_text,
                )
        dag_exists = dag_id is None or dag_declaration is None
        if not dag_exists and isinstance(payload.get("dags"), dict) and dag_id in payload["dags"]:
            if payload["dags"][dag_id] != dag_declaration:
                raise _CatalogShapeError(f"catalog DAG {dag_id!r} differs from the requested declaration")
            dag_exists = True
        if existing_text is not None and "domain" in payload and workload_exists and dag_exists:
            return CatalogPatchPlan(
                path=path,
                action="no_op",
                desired_text=existing_text,
                expected_text=existing_text,
            )
        desired_text = _render_domain_catalog(
            payload=payload,
            domain=domain,
            workload_id=workload_id,
            manifest_ref=manifest_ref,
            dag_id=dag_id,
            dag_declaration=dag_declaration,
        )
    except _CatalogShapeError as exc:
        return _shape_conflict(path, existing_text=existing_text, error=exc)
    if existing_text == desired_text:
        return CatalogPatchPlan(
            path=path,
            action="no_op",
            desired_text=desired_text,
            expected_text=existing_text,
        )
    if existing_text is None:
        return CatalogPatchPlan(
            path=path,
            action="create",
            desired_text=desired_text,
            expected_text=None,
            diff=_catalog_diff(
                "",
                desired_text,
                fromfile="/dev/null",
                tofile=path.as_posix(),
            ),
        )
    return CatalogPatchPlan(
        path=path,
        action="update",
        desired_text=desired_text,
        expected_text=existing_text,
        diff=_catalog_diff(
            existing_text,
            desired_text,
            fromfile=path.as_posix(),
            tofile=f"desired/{path.as_posix()}",
        ),
    )


def plan_ownership_patch(
    *,
    repo_root: Path,
    path: Path,
    domain: str,
    workload_id: str,
    owner: str,
) -> CatalogPatchPlan:
    existing_text, conflict = _read_for_plan(repo_root, path)
    if conflict is not None:
        return conflict
    try:
        payload = _yaml_mapping(existing_text)
        workloads = _validate_ownership_catalog(payload)
        existing_owner = _ownership_entry(workloads, domain=domain, workload_id=workload_id)
        if existing_owner is not None and existing_owner.get("owner") != owner:
            return CatalogPatchPlan(
                path=path,
                action="conflict",
                reason=f"ownership for {domain}/{workload_id} already exists",
                expected_text=existing_text,
            )
        desired_text = _render_ownership(
            payload=payload,
            domain=domain,
            workload_id=workload_id,
            owner=owner,
        )
    except _CatalogShapeError as exc:
        return _shape_conflict(path, existing_text=existing_text, error=exc)
    if existing_text == desired_text:
        return CatalogPatchPlan(
            path=path,
            action="no_op",
            desired_text=desired_text,
            expected_text=existing_text,
        )
    if existing_text is None:
        return CatalogPatchPlan(
            path=path,
            action="create",
            desired_text=desired_text,
            expected_text=None,
            diff=_catalog_diff(
                "",
                desired_text,
                fromfile="/dev/null",
                tofile=path.as_posix(),
            ),
        )
    if existing_owner is not None:
        return CatalogPatchPlan(
            path=path,
            action="no_op",
            desired_text=existing_text,
            expected_text=existing_text,
        )
    return CatalogPatchPlan(
        path=path,
        action="update",
        desired_text=desired_text,
        expected_text=existing_text,
        diff=_catalog_diff(
            existing_text,
            desired_text,
            fromfile=path.as_posix(),
            tofile=f"desired/{path.as_posix()}",
        ),
    )


def apply_catalog_patch(plan: CatalogPatchPlan, *, repo_root: Path) -> CatalogPatchReceipt | None:
    if plan.action == "conflict" or plan.desired_text is None:
        return None
    expected = plan.expected_text.encode("utf-8") if plan.expected_text is not None else None
    if plan.action == "no_op":
        if expected is None:
            raise CatalogPatchConflict("No-op catalog plan has no expected state.")
        verify_catalog_content(repo_root=repo_root, path=plan.path, expected=expected)
        return None
    desired = plan.desired_text.encode("utf-8")
    return apply_catalog_content(
        repo_root=repo_root,
        path=plan.path,
        expected=expected,
        desired=desired,
    )


def _read_for_plan(repo_root: Path, path: Path) -> tuple[str | None, CatalogPatchPlan | None]:
    try:
        payload = read_catalog_text(repo_root=repo_root, path=path)
    except (CatalogPatchConflict, UnicodeDecodeError) as exc:
        return None, CatalogPatchPlan(
            path=path,
            action="conflict",
            reason=f"catalog path cannot be accessed safely: {exc}",
        )
    return payload, None


def catalog_diff(existing: str, desired: str, *, fromfile: str, tofile: str) -> str:
    """Bounded, redacted unified diff for one catalog compare-and-swap receipt."""

    return _catalog_diff(existing, desired, fromfile=fromfile, tofile=tofile)


def _catalog_diff(existing: str, desired: str, *, fromfile: str, tofile: str) -> str:
    diff = "".join(
        difflib.unified_diff(
            existing[:_CATALOG_DIFF_INPUT_MAX_CHARS].splitlines(keepends=True),
            desired[:_CATALOG_DIFF_INPUT_MAX_CHARS].splitlines(keepends=True),
            fromfile=fromfile,
            tofile=tofile,
        )
    )
    redacted = redact_text(diff)
    encoded = redacted.encode("utf-8")
    if len(encoded) <= _CATALOG_DIFF_MAX_BYTES:
        return redacted
    marker = _DIFF_TRUNCATION_MARKER.encode()
    prefix = encoded[: _CATALOG_DIFF_MAX_BYTES - len(marker)].decode("utf-8", errors="ignore")
    return prefix + _DIFF_TRUNCATION_MARKER


def _render_domain_catalog(
    *,
    payload: dict[str, Any],
    domain: str,
    workload_id: str,
    manifest_ref: str,
    dag_id: str | None,
    dag_declaration: dict[str, Any] | None,
) -> str:
    if "domain" not in payload:
        payload["domain"] = domain
    elif payload["domain"] != domain:
        raise _CatalogShapeError(f"catalog domain must be {domain!r}")
    workloads = _mapping_section(payload, "workloads")
    if workload_id not in workloads:
        workloads[workload_id] = {"manifest": manifest_ref}
    if dag_id and dag_declaration is not None:
        dags = _mapping_section(payload, "dags")
        if dag_id not in dags:
            dags[dag_id] = dag_declaration
        elif dags[dag_id] != dag_declaration:
            raise _CatalogShapeError(f"catalog DAG {dag_id!r} differs from the requested declaration")
    return yaml.safe_dump(payload, sort_keys=False, allow_unicode=False)


def _render_ownership(*, payload: dict[str, Any], domain: str, workload_id: str, owner: str) -> str:
    payload.setdefault("schema", _OWNERSHIP_SCHEMA)
    workloads = _list_section(payload, "workloads")
    if not any(
        isinstance(item, dict) and item.get("domain") == domain and item.get("workload_id") == workload_id
        for item in workloads
    ):
        workloads.append({"domain": domain, "workload_id": workload_id, "owner": owner, "contacts": []})
    return yaml.safe_dump(payload, sort_keys=False, allow_unicode=False)


def _yaml_mapping(existing_text: str | None) -> dict[str, Any]:
    if existing_text is None or not existing_text.strip():
        return {}
    try:
        loaded = load_bounded_yaml(existing_text.encode("utf-8"), limits=_CATALOG_YAML_LIMITS)
    except BoundedYamlError as exc:
        raise _CatalogShapeError(f"catalog YAML is unsafe: {exc}") from exc
    if not isinstance(loaded, dict):
        raise _CatalogShapeError("catalog root must be a mapping")
    return loaded


def _mapping_section(payload: dict[str, Any], name: str) -> dict[str, Any]:
    section = payload.setdefault(name, {})
    if not isinstance(section, dict):
        raise _CatalogShapeError(f"catalog field {name!r} must be a mapping")
    return section


def _list_section(payload: dict[str, Any], name: str) -> list[Any]:
    section = payload.setdefault(name, [])
    if not isinstance(section, list):
        raise _CatalogShapeError(f"catalog field {name!r} must be a list")
    return section


def _validate_domain_catalog(payload: dict[str, Any], *, expected_domain: str) -> dict[str, Any]:
    if "domain" in payload and payload["domain"] != expected_domain:
        raise _CatalogShapeError(f"catalog domain must be {expected_domain!r}")
    workloads = _mapping_section(payload, "workloads")
    for raw_workload_id, item in workloads.items():
        if not isinstance(raw_workload_id, str) or not raw_workload_id.strip():
            raise _CatalogShapeError("catalog workload ids must be non-empty text")
        if not isinstance(item, dict):
            raise _CatalogShapeError(f"catalog workload {raw_workload_id!r} must be a mapping")
        manifest = item.get("manifest")
        if not isinstance(manifest, str) or not manifest.strip():
            raise _CatalogShapeError(f"catalog workload {raw_workload_id!r} manifest must be non-empty text")
    if "dags" in payload:
        dags = _mapping_section(payload, "dags")
        for raw_dag_id, item in dags.items():
            if not isinstance(raw_dag_id, str) or not raw_dag_id.strip():
                raise _CatalogShapeError("catalog DAG ids must be non-empty text")
            if not isinstance(item, dict):
                raise _CatalogShapeError(f"catalog DAG {raw_dag_id!r} must be a mapping")
    return workloads


def _validate_ownership_catalog(payload: dict[str, Any]) -> list[Any]:
    if "schema" in payload and payload["schema"] != _OWNERSHIP_SCHEMA:
        raise _CatalogShapeError(f"ownership schema must be {_OWNERSHIP_SCHEMA!r}")
    workloads = _list_section(payload, "workloads")
    identities: set[tuple[str, str]] = set()
    for index, item in enumerate(workloads):
        if not isinstance(item, dict):
            raise _CatalogShapeError(f"ownership workload entry {index} must be a mapping")
        domain = _required_entry_text(item, "domain", index=index)
        workload_id = _required_entry_text(item, "workload_id", index=index)
        _required_entry_text(item, "owner", index=index)
        if "contacts" in item and not isinstance(item["contacts"], list):
            raise _CatalogShapeError(f"ownership workload entry {index} contacts must be a list")
        identity = (domain, workload_id)
        if identity in identities:
            raise _CatalogShapeError(f"ownership identity {domain}/{workload_id} must be unique")
        identities.add(identity)
    return workloads


def _required_entry_text(item: dict[Any, Any], field: str, *, index: int) -> str:
    value = item.get(field)
    if not isinstance(value, str) or not value.strip():
        raise _CatalogShapeError(f"ownership workload entry {index} field {field!r} must be non-empty text")
    return value


def _shape_conflict(path: Path, *, existing_text: str | None, error: _CatalogShapeError) -> CatalogPatchPlan:
    return CatalogPatchPlan(
        path=path,
        action="conflict",
        reason=f"catalog cannot be merged safely: {error}",
        expected_text=existing_text,
    )


def _ownership_entry(workloads: list[Any], *, domain: str, workload_id: str) -> dict[str, Any] | None:
    for item in workloads:
        if isinstance(item, dict) and item.get("domain") == domain and item.get("workload_id") == workload_id:
            return item
    return None


__all__ = [
    "CatalogPatchConflict",
    "CatalogPatchPlan",
    "CatalogPatchReceipt",
    "apply_catalog_patch",
    "catalog_diff",
    "plan_domain_catalog_patch",
    "plan_ownership_patch",
    "rollback_catalog_patch",
]
