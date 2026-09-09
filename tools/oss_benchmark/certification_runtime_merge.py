"""Freshness merge policy for executable certification evidence."""

from __future__ import annotations

from datetime import datetime
from typing import Any


def merge_runtime_certification_v2(
    current_records: list[dict[str, Any]],
    *,
    previous_payload: dict[str, Any] | None,
    requested_scenarios: set[str],
    attempted_at: str,
    allow_stale: bool,
) -> dict[str, Any]:
    """Merge executable scenario results with prior evidence without fabrication."""

    previous_records = _previous_records(previous_payload)
    final: dict[str, dict[str, Any]] = {}
    for scenario_id, record in previous_records.items():
        if scenario_id not in requested_scenarios:
            final[scenario_id] = record
    for record in current_records:
        scenario_id = str(record.get("scenario_id") or "")
        if _is_refresh_error(record):
            final[scenario_id] = _stale_or_unavailable(
                record,
                previous=previous_records.get(scenario_id),
                attempted_at=attempted_at,
                allow_stale=allow_stale,
            )
        else:
            final[scenario_id] = _mark_fresh(record, attempted_at)
    records = sorted(final.values(), key=lambda item: str(item.get("scenario_id") or ""))
    return _certification_payload(records, attempted_at=attempted_at)


def _previous_records(payload: dict[str, Any] | None) -> dict[str, dict[str, Any]]:
    matrix = (payload or {}).get("runtime_certification_v2") or {}
    return {str(record.get("scenario_id")): dict(record) for record in matrix.get("scenarios") or []}


def _is_refresh_error(record: dict[str, Any]) -> bool:
    return bool(record.get("error_class")) and str(record.get("error_class")) != "contract_failure"


def _stale_or_unavailable(
    failed_record: dict[str, Any],
    *,
    previous: dict[str, Any] | None,
    attempted_at: str,
    allow_stale: bool,
) -> dict[str, Any]:
    if previous and allow_stale:
        return _mark_stale(previous, attempted_at=attempted_at, error=str(failed_record.get("last_error") or "failed"))
    return _mark_unavailable(failed_record, attempted_at=attempted_at)


def _mark_fresh(record: dict[str, Any], attempted_at: str) -> dict[str, Any]:
    updated = dict(record)
    updated["freshness"] = {
        "status": "fresh",
        "last_updated_at": attempted_at,
        "refresh_attempted_at": attempted_at,
        "last_error": None,
        "stale_age_days": 0,
    }
    return updated


def _mark_stale(record: dict[str, Any], *, attempted_at: str, error: str) -> dict[str, Any]:
    updated = dict(record)
    previous = record.get("freshness") or {}
    last_updated = previous.get("last_updated_at") or previous.get("refresh_attempted_at")
    updated["freshness"] = {
        "status": "stale",
        "last_updated_at": last_updated,
        "refresh_attempted_at": attempted_at,
        "last_error": error,
        "stale_age_days": _stale_age_days(last_updated, attempted_at),
    }
    return updated


def _mark_unavailable(record: dict[str, Any], *, attempted_at: str) -> dict[str, Any]:
    updated = dict(record)
    updated["status"] = "unavailable"
    updated["freshness"] = {
        "status": "unavailable",
        "last_updated_at": None,
        "refresh_attempted_at": attempted_at,
        "last_error": record.get("last_error") or record.get("error_class") or "unavailable",
        "stale_age_days": None,
    }
    return updated


def _certification_payload(records: list[dict[str, Any]], *, attempted_at: str) -> dict[str, Any]:
    contract_checks = _flatten_contract_checks(records)
    return {
        "schema_version": 2,
        "generated_at": attempted_at,
        "summary": _summary(records),
        "scenarios": records,
        "run_ledger": [_ledger_record(record) for record in records],
        "contract_checks": contract_checks,
        "golden_datasets": {
            "policy": "Deterministic local fixtures validate nested lineage, incremental sync, CDC replay, schema evolution and artifact contracts.",
            "hash_policy": "SHA-256 over normalized JSON with volatile runtime timestamps removed.",
        },
    }


def _flatten_contract_checks(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    for record in records:
        for check in record.get("contract_checks") or []:
            checks.append({"scenario_id": record.get("scenario_id"), **check})
    return checks


def _ledger_record(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "scenario_id": record.get("scenario_id"),
        "category": record.get("category"),
        "runner": record.get("runner"),
        "status": record.get("status"),
        "duration_ms": record.get("duration_ms"),
        "input_hash": record.get("input_hash"),
        "output_hash": record.get("output_hash"),
        "artifact_paths": record.get("artifact_paths") or [],
        "row_counts": record.get("row_counts") or {},
        "error_class": record.get("error_class"),
        "freshness": (record.get("freshness") or {}).get("status"),
    }


def _summary(records: list[dict[str, Any]]) -> dict[str, int]:
    summary = {"passed": 0, "failed": 0, "stale": 0, "unavailable": 0}
    for record in records:
        freshness = (record.get("freshness") or {}).get("status")
        status = str(record.get("status") or "failed")
        if freshness == "stale":
            summary["stale"] += 1
        elif status == "unavailable" or freshness == "unavailable":
            summary["unavailable"] += 1
        elif status == "passed":
            summary["passed"] += 1
        else:
            summary["failed"] += 1
    return summary


def _stale_age_days(last_updated_at: Any, attempted_at: str) -> int | None:
    if not last_updated_at:
        return None
    try:
        last = datetime.fromisoformat(str(last_updated_at).replace("Z", "+00:00"))
        attempted = datetime.fromisoformat(str(attempted_at).replace("Z", "+00:00"))
    except ValueError:
        return None
    return max(0, int((attempted - last).total_seconds() // 86_400))
