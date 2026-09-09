"""Internal helpers for provider-neutral schema migration release watch."""

from __future__ import annotations

import math
import re
import time
from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness.migration_control import MigrationPack, stable_fingerprint
from dpone.readiness.physical_reconciliation import PhysicalDesignDriftDetector
from dpone.readiness.physical_state import PhysicalTableState
from dpone.readiness.schema_migration_post_apply_support import (
    expectation_passed,
    rollback_window,
    target_connection_public,
    target_key,
)

WATCH_PLAN_SCHEMA = "dpone.schema_migration_watch_plan.v1"
WATCH_RUN_SCHEMA = "dpone.schema_migration_watch_run.v1"
WATCH_CERTIFICATE_SCHEMA = "dpone.schema_migration_watch_certificate.v1"
PROFILES = {"advisory", "stage", "prod_strict", "regulated"}
REMEDIATION_DECISIONS = {"continue", "extend_watch", "rollback_recommended", "rollback_required"}

_UNSAFE_SQL = re.compile(r"\b(insert|alter|drop|truncate|delete|update|create|exchange|rename)\b", re.IGNORECASE)
_DURATION = re.compile(r"^\s*(\d+)\s*(ms|s|m|h)?\s*$", re.IGNORECASE)


def watch_options(manifest: Mapping[str, Any] | None) -> dict[str, Any]:
    options: Mapping[str, Any] | None = manifest or {}
    for key in ("sink", "options", "physical_design", "migration", "watch"):
        value = options.get(key) if isinstance(options, Mapping) else {}
        options = value if isinstance(value, Mapping) else {}
    return dict(options or {})


def checks(options: Mapping[str, Any]) -> dict[str, bool]:
    raw = options.get("verification", {})
    raw = raw if isinstance(raw, Mapping) else {}
    defaults = {
        "post_apply_recheck": True,
        "physical_design": True,
        "row_count": False,
        "typed_hash": False,
        "null_distribution": False,
        "duplicate_key": False,
        "null_key": False,
        "canary_queries": True,
        "query_health": False,
        "rollback_window": True,
    }
    return {key: bool(raw.get(key, default)) for key, default in defaults.items()}


def normalize_window(raw: Mapping[str, Any]) -> tuple[dict[str, int], list[str]]:
    duration = parse_duration_seconds(raw.get("duration", "15m"))
    interval = parse_duration_seconds(raw.get("interval", "5m"))
    min_success = int(raw.get("min_successful_samples", 1) or 1)
    max_failed = int(raw.get("max_failed_samples", 0) or 0)
    planned = _planned_samples(duration_seconds=duration, interval_seconds=interval, min_success=min_success)
    blockers: list[str] = []
    if min_success > planned:
        blockers.append("schema_migration_watch.min_successful_samples_exceeds_planned")
    if min_success < 1:
        blockers.append("schema_migration_watch.min_successful_samples_invalid")
    if max_failed < 0:
        blockers.append("schema_migration_watch.max_failed_samples_invalid")
    return (
        {
            "duration_seconds": duration,
            "interval_seconds": interval,
            "min_successful_samples": min_success,
            "max_failed_samples": max_failed,
            "planned_samples": planned,
        },
        blockers,
    )


def parse_duration_seconds(value: object) -> int:
    if isinstance(value, int | float):
        return max(0, int(value))
    match = _DURATION.match(str(value or "0s"))
    if not match:
        raise ValueError(f"invalid watch duration: {value}")
    amount = int(match.group(1))
    unit = (match.group(2) or "s").lower()
    factor = {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[unit]
    return max(0, int(amount * factor))


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
            blockers.append("schema_migration_watch.canary_id_required")
            continue
        if not query or not query.lower().lstrip().startswith("select") or _UNSAFE_SQL.search(query):
            blockers.append(f"schema_migration_watch.unsafe_canary_sql:{canary_id}")
        if not expect:
            blockers.append(f"schema_migration_watch.canary_expect_required:{canary_id}")
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


def post_apply_blockers(
    *, pack: MigrationPack, certificate: Mapping[str, Any], mode: str, environment: str
) -> list[str]:
    blockers: list[str] = []
    if certificate.get("pack_id") != pack.pack_id:
        blockers.append("schema_migration_watch.post_apply_pack_id_mismatch")
    if mode == "gate" and certificate.get("status") != "verified":
        blockers.append("schema_migration_watch.post_apply_not_verified")
    if environment and certificate.get("environment") not in {None, environment}:
        blockers.append("schema_migration_watch.post_apply_environment_mismatch")
    return blockers


def target_blockers(pack: MigrationPack, target_connection: Mapping[str, Any]) -> list[str]:
    sink_type = str(target_connection.get("type") or target_connection.get("sink_type") or pack.target.sink_type)
    if sink_type.lower() != pack.target.sink_type.lower():
        return [f"schema_migration_watch.target_mismatch:{sink_type}"]
    if sink_type.lower() != "clickhouse":
        return [f"schema_migration_watch.unsupported_target:{sink_type}"]
    return []


def query_health_options(options: Mapping[str, Any]) -> dict[str, Any]:
    raw = options.get("query_health", {})
    raw = raw if isinstance(raw, Mapping) else {}
    return {
        "lookback_seconds": parse_duration_seconds(raw.get("lookback", "15m")),
        "max_error_count": int(raw.get("max_error_count", 0) or 0),
        "max_p95_ms": int(raw.get("max_p95_ms", 0) or 0),
        "max_read_rows": int(raw.get("max_read_rows", 0) or 0),
    }


def remediation_options(options: Mapping[str, Any]) -> dict[str, Any]:
    raw = options.get("remediation", {})
    raw = raw if isinstance(raw, Mapping) else {}
    rollback_on = raw.get("rollback_on", [])
    return {
        "mode": "recommend",
        "rollback_on": [str(item) for item in rollback_on if str(item)] if isinstance(rollback_on, list) else [],
    }


def run_canaries(canaries: Sequence[Mapping[str, Any]], executor: Any | None) -> dict[str, Any]:
    if canaries and executor is None:
        return {"checks": [], "blockers": ["schema_migration_watch.canary_executor_required"], "warnings": []}
    checks_out: list[dict[str, Any]] = []
    blockers: list[str] = []
    warnings: list[str] = []
    for canary in canaries:
        rows = [dict(row) for row in executor.execute(dict(canary))] if executor else []
        passed = expectation_passed(rows, canary.get("expect", {}))
        checks_out.append(
            {
                "name": f"canary:{canary.get('id')}",
                "status": "passed" if passed else "failed",
                "severity": canary.get("severity"),
                "row_count": len(rows),
            }
        )
        if not passed:
            key = f"schema_migration_watch.canary_failed:{canary.get('id')}"
            if str(canary.get("severity")) in {"critical", "high"}:
                blockers.append(key)
            else:
                warnings.append(key)
    return {"checks": checks_out, "blockers": blockers, "warnings": warnings}


def sample_summary(samples: Sequence[Mapping[str, Any]], planned: int) -> dict[str, int]:
    passed = sum(1 for sample in samples if sample.get("status") == "passed")
    failed = sum(1 for sample in samples if sample.get("status") == "failed")
    return {"planned": planned, "executed": len(samples), "passed": passed, "failed": failed}


def physical_blockers(
    plan: Mapping[str, Any], actual: PhysicalTableState, checks_out: list[dict[str, Any]]
) -> list[str]:
    desired = PhysicalTableState.from_mapping(mapping(plan.get("desired")))
    changes = PhysicalDesignDriftDetector().detect(desired, actual)
    checks_out.append(
        {
            "name": "physical_design",
            "status": "failed" if changes else "passed",
            "changes": [change.to_dict() for change in changes],
        }
    )
    return ["schema_migration_watch.physical_drift"] if changes else []


def checks_enabled(plan: Mapping[str, Any]) -> dict[str, bool]:
    raw = plan.get("checks", {})
    return {str(key): bool(value) for key, value in raw.items()} if isinstance(raw, Mapping) else {}


def profile_required(plan: Mapping[str, Any]) -> bool:
    return any(
        checks_enabled(plan).get(key, False)
        for key in ("row_count", "typed_hash", "null_distribution", "duplicate_key", "null_key")
    )


def planned_samples(plan: Mapping[str, Any]) -> int:
    return int(mapping(plan.get("samples")).get("planned", 0) or 0)


def rollback_on(plan: Mapping[str, Any]) -> tuple[str, ...]:
    raw = mapping(plan.get("remediation")).get("rollback_on", [])
    return tuple(str(item) for item in raw if str(item)) if isinstance(raw, list) else ()


def matches_rollback_signal(blocker: str, rollback_signals: Sequence[str]) -> bool:
    signal_map = {
        "critical_canary_failure": "schema_migration_watch.canary_failed",
        "physical_drift": "schema_migration_watch.physical_drift",
        "query_health_blocked": "schema_migration_watch.query_health_blocked",
        "rollback_window_closing": "schema_migration_watch.rollback_window_closing",
    }
    return any(blocker.startswith(signal_map.get(signal, signal)) for signal in rollback_signals)


def rollback_command(pack_id: str) -> str:
    return f"dpone schema migration rollback --pack-id {pack_id} --format json"


def should_stop_early(plan: Mapping[str, Any], blockers: Sequence[str]) -> bool:
    return str(plan.get("mode") or "gate") == "gate" and any(
        matches_rollback_signal(blocker, rollback_on(plan)) for blocker in blockers
    )


def sleep_between_samples(plan: Mapping[str, Any]) -> None:
    interval = int(mapping(plan.get("window")).get("interval_seconds", 0) or 0)
    if interval > 0:
        time.sleep(interval)


def default_profile(environment: str) -> str:
    return "prod_strict" if environment == "prod" else "stage"


def disabled_plan(
    pack: MigrationPack,
    post_apply_certificate: Mapping[str, Any],
    target_connection: Mapping[str, Any],
    environment: str,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": WATCH_PLAN_SCHEMA,
        "status": "disabled",
        "pack_id": pack.pack_id,
        "post_apply_certificate_id": post_apply_certificate.get("certificate_id"),
        "environment": environment,
        "target": pack.target.to_dict(),
        "target_connection": target_connection_public(target_connection),
        "checks": {},
        "canaries": [],
        "samples": {"planned": 0},
        "blockers": [],
        "warnings": ["schema_migration_watch.disabled"],
    }
    payload["watch_plan_id"] = stable_fingerprint(payload)
    return payload


def strings(raw: object) -> list[str]:
    return [str(item) for item in raw if str(item)] if isinstance(raw, list | tuple) else []


def mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _planned_samples(*, duration_seconds: int, interval_seconds: int, min_success: int) -> int:
    if interval_seconds == 0:
        return max(1, min_success)
    return max(min_success, math.ceil(max(duration_seconds, interval_seconds) / interval_seconds))


__all__ = [
    "PROFILES",
    "WATCH_CERTIFICATE_SCHEMA",
    "WATCH_PLAN_SCHEMA",
    "WATCH_RUN_SCHEMA",
    "checks",
    "checks_enabled",
    "default_profile",
    "disabled_plan",
    "mapping",
    "matches_rollback_signal",
    "normalize_canaries",
    "normalize_window",
    "physical_blockers",
    "post_apply_blockers",
    "planned_samples",
    "profile_required",
    "query_health_options",
    "remediation_options",
    "rollback_command",
    "rollback_on",
    "rollback_window",
    "run_canaries",
    "sample_summary",
    "should_stop_early",
    "sleep_between_samples",
    "strings",
    "target_blockers",
    "target_connection_public",
    "target_key",
    "watch_options",
]
