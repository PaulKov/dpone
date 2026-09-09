"""Support helpers for data product access enforcement contracts."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness import data_product_compliance_support as support
from dpone.readiness.migration_control import stable_fingerprint

PASSING_GATE_STATUSES = {"allowed", "warning"}
PASSING_RUN_STATUSES = {"applied", "warning"}
PASSING_DRIFT_STATUSES = {"clean", "warning"}


def target_from_connection(config: Mapping[str, Any], product_id: str | None = None) -> dict[str, Any]:
    database, table = _database_table(config, product_id)
    sink_type = str(config.get("type", config.get("sink_type", "")) or "unknown").lower()
    qualified = ".".join(item for item in (database, table) if item)
    return {
        key: value
        for key, value in {
            "sink_type": sink_type,
            "database": database,
            "table": table,
            "qualified_name": f"{sink_type}.{qualified}" if qualified else sink_type,
            "fingerprint": config.get("fingerprint") or config.get("target_fingerprint") or fingerprint_target(config),
            "lock_id": config.get("lock_id") or _mapping(config.get("lock")).get("lock_id"),
        }.items()
        if value is not None
    }


def product_ref(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return support.product_ref(support.product(manifest))


def options_mapping(manifest: Mapping[str, Any]) -> dict[str, Any]:
    raw = support.product(manifest).get("access_enforcement")
    return dict(raw) if isinstance(raw, Mapping) else {}


def bool_value(raw: Any, default: bool) -> bool:
    return support.bool_value(raw, default)


def strings(raw: Any) -> tuple[str, ...]:
    return support.strings(raw)


def mappings(raw: Any) -> tuple[Mapping[str, Any], ...]:
    return support.mappings(raw)


def profile_status(blockers: Sequence[str], warnings: Sequence[str], ready_status: str) -> str:
    if blockers:
        return "blocked"
    if warnings:
        return "warning"
    return ready_status


def blocked_or_warning(blockers: Sequence[str], warnings: Sequence[str], profile: str) -> tuple[list[str], list[str]]:
    current_blockers = list(dict.fromkeys(blockers))
    current_warnings = list(dict.fromkeys(warnings))
    if profile == "advisory":
        current_warnings = list(dict.fromkeys([*current_warnings, *current_blockers]))
        current_blockers = []
    return current_blockers, current_warnings


def evidence_blockers(
    *,
    access_gate: Mapping[str, Any] | None,
    authority_gate: Mapping[str, Any] | None,
    require_access_gate: bool,
    require_authority_gate: bool,
) -> list[str]:
    blockers: list[str] = []
    if require_access_gate and not access_gate:
        blockers.append("data_product_access_enforcement.access_gate_required")
    elif access_gate and access_gate.get("status") not in PASSING_GATE_STATUSES:
        blockers.append("data_product_access_enforcement.access_gate_blocked")
    if require_authority_gate and not authority_gate:
        blockers.append("data_product_access_enforcement.authority_gate_required")
    elif authority_gate and authority_gate.get("status") not in PASSING_GATE_STATUSES:
        blockers.append("data_product_access_enforcement.authority_gate_blocked")
    return blockers


def precondition_blockers(target: Mapping[str, Any], *, require_fingerprint: bool, require_lock: bool) -> list[str]:
    blockers: list[str] = []
    if require_fingerprint and not target.get("fingerprint"):
        blockers.append("data_product_access_enforcement.target_fingerprint_required")
    if require_lock and not target.get("lock_id"):
        blockers.append("data_product_access_enforcement.lock_required")
    return blockers


def normalized_requirements(entitlement_plan: Mapping[str, Any]) -> tuple[dict[str, Any], ...]:
    requirements: list[dict[str, Any]] = []
    for decision in support.mappings(entitlement_plan.get("decisions")):
        if decision.get("blockers"):
            continue
        subject = str(decision.get("subject") or "")
        column = str(decision.get("column") or "")
        if not subject or not column:
            continue
        if {"read", "export"} & set(support.strings(decision.get("actions")) or ("read",)):
            requirements.append(_requirement("column_grant", decision, column=column, subject=subject))
        if decision.get("masking_required"):
            requirements.append(_requirement("mask", decision, column=column, subject=subject))
        if decision.get("row_filter"):
            requirements.append(_requirement("row_filter", decision, column=column, subject=subject))
    return tuple(sorted(requirements, key=lambda item: (item["type"], item["subject"], item["column"])))


def desired_state(requirements: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    grants: dict[str, set[str]] = {}
    masks: list[dict[str, Any]] = []
    row_filters: list[dict[str, Any]] = []
    sensitive_columns: set[str] = set()
    for requirement in requirements:
        subject = str(requirement.get("subject"))
        column = str(requirement.get("column"))
        if requirement.get("sensitive"):
            sensitive_columns.add(column)
        if requirement.get("type") == "column_grant":
            grants.setdefault(subject, set()).add(column)
        elif requirement.get("type") == "mask":
            masks.append(_state_item(requirement))
        elif requirement.get("type") == "row_filter":
            row_filters.append(_state_item(requirement))
    return {
        "grants": [{"subject": subject, "columns": sorted(columns)} for subject, columns in sorted(grants.items())],
        "masks": sorted(masks, key=_state_key),
        "row_filters": sorted(row_filters, key=_state_key),
        "sensitive_columns": sorted(sensitive_columns),
    }


def missing_grants(desired: Mapping[str, Any], actual: Mapping[str, Any]) -> list[str]:
    actual_map = _grant_map(actual)
    blockers: list[str] = []
    for grant in support.mappings(desired.get("grants")):
        actual_columns = actual_map.get(str(grant.get("subject")), set())
        for column in support.strings(grant.get("columns")):
            if column not in actual_columns:
                blockers.append(f"data_product_access_drift.grant_missing:{grant.get('subject')}:{column}")
    return blockers


def extra_sensitive_grants(desired: Mapping[str, Any], actual: Mapping[str, Any]) -> list[str]:
    desired_map = _grant_map(desired)
    sensitive = set(support.strings(desired.get("sensitive_columns")))
    blockers: list[str] = []
    for grant in support.mappings(actual.get("grants")):
        subject = str(grant.get("subject"))
        desired_columns = desired_map.get(subject, set())
        for column in support.strings(grant.get("columns")):
            if column in sensitive and column not in desired_columns:
                blockers.append(f"data_product_access_drift.extra_sensitive_grant:{subject}:{column}")
    return blockers


def missing_items(desired: Mapping[str, Any], actual: Mapping[str, Any], key: str, code: str) -> list[str]:
    actual_items = {_state_key(item) for item in support.mappings(actual.get(key))}
    return [
        f"{code}:{item.get('subject')}:{item.get('column')}"
        for item in support.mappings(desired.get(key))
        if _state_key(item) not in actual_items
    ]


def payload_id(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    current = dict(payload)
    current[key] = stable_fingerprint(current)
    return current


def fingerprint_target(config: Mapping[str, Any]) -> str:
    public = {
        key: value
        for key, value in config.items()
        if key not in {"password", "token", "secret", "access_state"} and not str(key).endswith("_key")
    }
    canonical = json.dumps(public, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return "sha256:" + hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _requirement(kind: str, decision: Mapping[str, Any], *, subject: str, column: str) -> dict[str, Any]:
    return {
        "type": kind,
        "subject": subject,
        "owner": decision.get("owner"),
        "column": column,
        "class": decision.get("class"),
        "sensitive": bool(decision.get("sensitive")),
        "masking": decision.get("masking") or "none",
        "row_filter": decision.get("row_filter"),
    }


def _database_table(config: Mapping[str, Any], product_id: str | None) -> tuple[str | None, str | None]:
    database = config.get("database") or config.get("schema")
    table = config.get("table")
    if (not database or not table) and product_id and "." in product_id:
        left, right = product_id.rsplit(".", 1)
        database = database or left
        table = table or right
    return (str(database) if database else None, str(table) if table else None)


def _state_item(requirement: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in {
            "subject": requirement.get("subject"),
            "column": requirement.get("column"),
            "masking": requirement.get("masking"),
            "row_filter": requirement.get("row_filter"),
        }.items()
        if value not in {None, ""}
    }


def _state_key(item: Mapping[str, Any]) -> tuple[str, str]:
    return str(item.get("subject")), str(item.get("column"))


def _grant_map(raw: Mapping[str, Any]) -> dict[str, set[str]]:
    return {
        str(item.get("subject")): set(support.strings(item.get("columns")))
        for item in support.mappings(raw.get("grants"))
    }


def _mapping(raw: Any) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = [
    "PASSING_DRIFT_STATUSES",
    "PASSING_GATE_STATUSES",
    "PASSING_RUN_STATUSES",
    "blocked_or_warning",
    "bool_value",
    "desired_state",
    "evidence_blockers",
    "extra_sensitive_grants",
    "fingerprint_target",
    "missing_grants",
    "missing_items",
    "mappings",
    "normalized_requirements",
    "options_mapping",
    "payload_id",
    "precondition_blockers",
    "product_ref",
    "profile_status",
    "strings",
    "target_from_connection",
]
