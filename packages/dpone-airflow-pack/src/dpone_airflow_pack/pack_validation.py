"""Validation helpers for compact dpone Airflow packs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone_airflow_pack.pack_identity import PackIdentityError, parse_pack_json
from dpone_airflow_pack.runtime_adapter import DponeAirflowContractError


def parse_airflow_pack_json(text: str, *, location: str) -> dict[str, Any]:
    """Parse and validate a compact `gitops.airflow_pack` JSON document."""

    try:
        payload = parse_pack_json(text)
    except PackIdentityError as exc:
        raise DponeAirflowContractError(
            f"dpone Airflow pack is invalid JSON: {location}",
            blockers=(
                {
                    "code": "airflow_pack_json_invalid",
                    "path": location,
                    "message": "Pack must be one duplicate-free UTF-8 JSON object",
                },
            ),
        ) from exc
    return validate_airflow_pack_payload(payload, location=location)


def validate_airflow_pack_payload(payload: object, *, location: str) -> dict[str, Any]:
    """Validate the small scheduler-side pack contract."""

    if not isinstance(payload, dict) or payload.get("kind") != "gitops.airflow_pack":
        raise DponeAirflowContractError(
            f"dpone Airflow pack has invalid kind: {location}",
            blockers=(
                {"code": "airflow_pack_kind_invalid", "path": location, "message": "Expected gitops.airflow_pack"},
            ),
        )
    mapped = _contains_mapped_plan(payload)
    legacy_kwargs = payload.get("kpo_kwargs")
    guarded_kwargs = payload.get("mapped_kpo_kwargs")
    if mapped and isinstance(legacy_kwargs, Mapping):
        raise DponeAirflowContractError(
            f"dpone mapped Airflow pack has an unsafe legacy provider projection: {location}",
            blockers=(
                {
                    "code": "airflow_pack_mapping_provider_guard_invalid",
                    "path": location,
                    "message": "Mapped packs must use mapped_kpo_kwargs so older providers fail closed",
                },
            ),
        )
    if mapped and isinstance(guarded_kwargs, Mapping):
        normalized = dict(payload)
        normalized["kpo_kwargs"] = dict(guarded_kwargs)
        normalized.pop("mapped_kpo_kwargs", None)
        return normalized
    if not isinstance(legacy_kwargs, Mapping):
        raise DponeAirflowContractError(
            f"dpone Airflow pack has no kpo_kwargs: {location}",
            blockers=(
                {"code": "airflow_pack_kpo_kwargs_missing", "path": location, "message": "kpo_kwargs is required"},
            ),
        )
    return dict(payload)


def _contains_mapped_plan(payload: Mapping[str, Any]) -> bool:
    plan = payload.get("mapping_plan")
    if isinstance(plan, Mapping) and plan.get("mode") in {"visible", "summary"}:
        return True
    process_plans = payload.get("process_plans")
    if not isinstance(process_plans, Mapping):
        return False
    return any(
        isinstance(process, Mapping)
        and isinstance(process.get("mapping_plan"), Mapping)
        and process["mapping_plan"].get("mode") in {"visible", "summary"}
        for process in process_plans.values()
    )
