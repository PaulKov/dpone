"""Internal helpers for provider-neutral post-apply verification."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness.migration_control import MigrationPack, stable_fingerprint
from dpone.readiness.physical_reconciliation import PhysicalDesignDriftDetector
from dpone.readiness.physical_state import PhysicalTableState

POST_APPLY_PLAN_SCHEMA = "dpone.schema_migration_post_apply_plan.v1"
POST_APPLY_RUN_SCHEMA = "dpone.schema_migration_post_apply_run.v1"
POST_APPLY_CERTIFICATE_SCHEMA = "dpone.schema_migration_post_apply_certificate.v1"

PROFILES = {"advisory", "stage", "prod_strict", "regulated"}
_APPLIED_STATUSES = {"applied", "phase_applied"}
_UNSAFE_SQL = re.compile(r"\b(insert|alter|drop|truncate|delete|update|create|exchange|rename)\b", re.IGNORECASE)


def post_apply_options(manifest: Mapping[str, Any] | None) -> dict[str, Any]:
    options = manifest or {}
    for key in ("sink", "options", "physical_design", "migration", "post_apply"):
        value = options.get(key) if isinstance(options, Mapping) else {}
        options = value if isinstance(value, Mapping) else {}
    return dict(options)


def checks(options: Mapping[str, Any]) -> dict[str, bool]:
    raw = options.get("verification", {})
    raw = raw if isinstance(raw, Mapping) else {}
    defaults = {
        "ledger_state": True,
        "physical_design": True,
        "row_count": False,
        "typed_hash": False,
        "null_distribution": False,
        "duplicate_key": False,
        "null_key": False,
        "nested_parent_child": False,
        "canary_queries": True,
        "rollback_window": True,
    }
    return {key: bool(raw.get(key, default)) for key, default in defaults.items()}


def normalize_canaries(raw: object) -> tuple[list[dict[str, Any]], list[str]]:
    canaries: list[dict[str, Any]] = []
    blockers: list[str] = []
    if not isinstance(raw, list):
        return canaries, blockers
    for item in raw:
        if not isinstance(item, Mapping):
            continue
        canary_id = str(item.get("id") or "").strip()
        query = str(item.get("query") or "").strip()
        expect = item.get("expect") if isinstance(item.get("expect"), Mapping) else {}
        if not canary_id:
            blockers.append("schema_migration_post_apply.canary_id_required")
            continue
        if not query or not query.lower().lstrip().startswith("select") or _UNSAFE_SQL.search(query):
            blockers.append(f"schema_migration_post_apply.unsafe_canary_sql:{canary_id}")
        if not expect:
            blockers.append(f"schema_migration_post_apply.canary_expect_required:{canary_id}")
        canaries.append(
            {
                "id": canary_id,
                "type": str(item.get("type") or "sql"),
                "owner": str(item.get("owner") or ""),
                "severity": str(item.get("severity") or "medium"),
                "query": query,
                "expect": dict(expect),
            }
        )
    return canaries, blockers


def pack_blockers(pack: MigrationPack) -> list[str]:
    return ["schema_migration_post_apply.pack_blocked"] if pack.blockers else []


def bundle_blockers(pack: MigrationPack, bundle: Mapping[str, Any] | None) -> list[str]:
    if bundle and bundle.get("pack_id") != pack.pack_id:
        return ["schema_migration_post_apply.bundle_pack_id_mismatch"]
    return []


def ledger_blockers(pack: MigrationPack, records: Sequence[Mapping[str, Any]], environment: str) -> list[str]:
    pack_records = [record for record in records if record.get("pack_id") == pack.pack_id]
    applied = [record for record in pack_records if record.get("status") in _APPLIED_STATUSES]
    blockers: list[str] = []
    if not applied:
        blockers.append("schema_migration_post_apply.ledger_applied_record_missing")
    if pack_records and environment and not any(record.get("environment") == environment for record in pack_records):
        blockers.append("schema_migration_post_apply.ledger_environment_mismatch")
    return blockers


def target_blockers(pack: MigrationPack, target_connection: Mapping[str, Any]) -> list[str]:
    sink_type = str(target_connection.get("type") or target_connection.get("sink_type") or pack.target.sink_type)
    if sink_type.lower() != pack.target.sink_type.lower():
        return [f"schema_migration_post_apply.target_mismatch:{sink_type}"]
    if sink_type.lower() != "clickhouse":
        return [f"schema_migration_post_apply.unsupported_target:{sink_type}"]
    return []


def ledger_records(ledger: Mapping[str, Any] | None) -> tuple[Mapping[str, Any], ...]:
    records = ledger.get("records", []) if isinstance(ledger, Mapping) else []
    return tuple(item for item in records if isinstance(item, Mapping)) if isinstance(records, list) else ()


def ledger_summary(pack: MigrationPack, records: Sequence[Mapping[str, Any]], environment: str) -> dict[str, Any]:
    matched = [record for record in records if record.get("pack_id") == pack.pack_id]
    return {
        "records": len(matched),
        "applied": any(record.get("status") in _APPLIED_STATUSES for record in matched),
        "environment": environment,
        "last_phase": next((record.get("phase") for record in reversed(matched) if record.get("phase")), None),
    }


def target_connection_public(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in raw.items()
        if key not in {"password", "token", "secret"} and isinstance(value, str | int | float | bool)
    }


def physical_blockers(
    plan: Mapping[str, Any], actual: PhysicalTableState, check_results: list[dict[str, Any]]
) -> list[str]:
    if not checks_enabled(plan).get("physical_design", True):
        return []
    desired_raw = plan.get("desired", {})
    desired = PhysicalTableState.from_mapping(desired_raw if isinstance(desired_raw, Mapping) else {})
    changes = PhysicalDesignDriftDetector().detect(desired, actual)
    check_results.append(
        {
            "name": "physical_design",
            "status": "failed" if changes else "passed",
            "changes": [change.to_dict() for change in changes],
        }
    )
    return ["schema_migration_post_apply.physical_drift"] if changes else []


def run_canaries(plan: Mapping[str, Any], executor: Any | None) -> dict[str, Any]:
    if not checks_enabled(plan).get("canary_queries", True):
        return {"checks": [], "blockers": [], "warnings": []}
    canaries = [dict(item) for item in plan.get("canaries", []) if isinstance(item, Mapping)]
    if canaries and executor is None:
        return {"checks": [], "blockers": ["schema_migration_post_apply.canary_executor_required"], "warnings": []}
    check_results: list[dict[str, Any]] = []
    blockers: list[str] = []
    warnings: list[str] = []
    for canary in canaries:
        rows = [dict(row) for row in executor.execute(canary)] if executor else []
        passed = expectation_passed(rows, canary.get("expect", {}))
        check_results.append(
            {
                "name": f"canary:{canary.get('id')}",
                "status": "passed" if passed else "failed",
                "severity": canary.get("severity"),
                "row_count": len(rows),
            }
        )
        if not passed:
            key = f"schema_migration_post_apply.canary_failed:{canary.get('id')}"
            if str(canary.get("severity")) in {"critical", "high"}:
                blockers.append(key)
            else:
                warnings.append(key)
    return {"checks": check_results, "blockers": blockers, "warnings": warnings}


def run_data_profile(plan: Mapping[str, Any], profiler: Any | None) -> dict[str, Any]:
    profile_checks = {
        key: value
        for key, value in checks_enabled(plan).items()
        if key
        in {
            "row_count",
            "typed_hash",
            "null_distribution",
            "duplicate_key",
            "null_key",
            "nested_parent_child",
        }
        and value
    }
    if not profile_checks:
        return {"checks": [], "blockers": [], "warnings": [], "metrics": {}}
    if profiler is None:
        return {
            "checks": [],
            "blockers": ["schema_migration_post_apply.data_profiler_required"],
            "warnings": [],
            "metrics": {},
        }
    result = profiler.profile(dict(plan))
    return {
        "checks": list(result.get("checks", [])) if isinstance(result, Mapping) else [],
        "blockers": strings(result.get("blockers", [])) if isinstance(result, Mapping) else [],
        "warnings": strings(result.get("warnings", [])) if isinstance(result, Mapping) else [],
        "metrics": dict(result.get("metrics", {}))
        if isinstance(result, Mapping) and isinstance(result.get("metrics"), Mapping)
        else {},
    }


def expectation_passed(rows: Sequence[Mapping[str, Any]], expect: object) -> bool:
    if not isinstance(expect, Mapping):
        return False
    if expect.get("non_empty") is True:
        return bool(rows)
    column = str(expect.get("column") or "")
    value = rows[0].get(column) if rows and column else None
    if "equals" in expect:
        return value == expect.get("equals")
    if "min" in expect:
        return _number(value) is not None and _number(value) >= _number(expect.get("min"))
    if "max" in expect:
        return _number(value) is not None and _number(value) <= _number(expect.get("max"))
    if "regex" in expect:
        return re.search(str(expect.get("regex")), str(value or "")) is not None
    return False


def rollback_window(plan: Mapping[str, Any]) -> dict[str, str]:
    rollback = plan.get("rollback", {})
    rollback = rollback if isinstance(rollback, Mapping) else {}
    if not rollback.get("supported"):
        return {"status": "unsupported"}
    supported_until = str(rollback.get("supported_until_phase") or "")
    last_phase = str(mapping(plan.get("ledger_summary")).get("last_phase") or "")
    if supported_until == "contract" and last_phase == "contract":
        return {"status": "closed", "supported_until_phase": supported_until}
    return {"status": "open", "supported_until_phase": supported_until or "apply"}


def run_payload(
    plan: Mapping[str, Any],
    *,
    status: str,
    execute: bool,
    blockers: Sequence[str] = (),
    warnings: Sequence[str] = (),
    checks: Sequence[Mapping[str, Any]] = (),
    rollback_window_value: Mapping[str, Any] | None = None,
    duration_ms: int = 0,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": POST_APPLY_RUN_SCHEMA,
        "status": status,
        "executed": execute,
        "post_apply_plan_id": plan.get("post_apply_plan_id"),
        "pack_id": plan.get("pack_id"),
        "bundle_id": plan.get("bundle_id"),
        "environment": plan.get("environment"),
        "target": dict(plan.get("target", {})) if isinstance(plan.get("target"), Mapping) else {},
        "checks": list(checks),
        "rollback_window": dict(rollback_window_value or {}),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "metrics": {
            "duration_ms": duration_ms,
            "checks_executed": len(checks),
            "canaries_executed": sum(1 for item in checks if str(item.get("name", "")).startswith("canary:")),
        },
    }
    payload["post_apply_run_id"] = stable_fingerprint(
        {
            "plan_id": payload["post_apply_plan_id"],
            "status": status,
            "executed": execute,
            "checks": list(checks),
            "rollback_window": payload["rollback_window"],
            "blockers": payload["blockers"],
            "warnings": payload["warnings"],
        }
    )
    return payload


def checks_enabled(plan: Mapping[str, Any]) -> dict[str, bool]:
    raw = plan.get("checks", {})
    return {str(key): bool(value) for key, value in raw.items()} if isinstance(raw, Mapping) else {}


def rollback_required_but_closed(run: Mapping[str, Any]) -> bool:
    rollback = run.get("rollback_window", {})
    return isinstance(rollback, Mapping) and rollback.get("status") == "closed"


def default_profile(environment: str) -> str:
    return "prod_strict" if environment == "prod" else "stage"


def bundle_id(bundle: Mapping[str, Any] | None) -> str | None:
    return str(bundle.get("bundle_id")) if isinstance(bundle, Mapping) and bundle.get("bundle_id") else None


def mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def target_key(value: object) -> str:
    target = mapping(value)
    return ".".join(item for item in (str(target.get("sink_type") or ""), str(target.get("table") or "")) if item)


def strings(raw: object) -> list[str]:
    return [str(item) for item in raw if str(item)] if isinstance(raw, list | tuple) else []


def _number(value: object) -> float | None:
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


__all__ = [
    "POST_APPLY_CERTIFICATE_SCHEMA",
    "POST_APPLY_PLAN_SCHEMA",
    "POST_APPLY_RUN_SCHEMA",
    "PROFILES",
    "bundle_blockers",
    "bundle_id",
    "checks",
    "default_profile",
    "ledger_blockers",
    "ledger_records",
    "ledger_summary",
    "normalize_canaries",
    "pack_blockers",
    "physical_blockers",
    "post_apply_options",
    "rollback_required_but_closed",
    "rollback_window",
    "run_canaries",
    "run_data_profile",
    "run_payload",
    "strings",
    "target_blockers",
    "target_connection_public",
    "target_key",
]
