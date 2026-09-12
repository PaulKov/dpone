"""Coherent dry-run projections for a platform-selected R1 route."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any


def cohere_selected_r1_plan(
    plan: dict[str, Any],
    *,
    correctness: Mapping[str, object] | None,
    unique_key: tuple[str, ...],
) -> dict[str, Any]:
    """Replace compatibility execution details only for an explicitly selected R1 plan."""

    if not correctness or correctness.get("selected") is not True:
        return plan

    source_mode = str(correctness.get("source_mode") or "")
    coherent = dict(plan)
    coherent["strategy"] = _strategy(plan.get("strategy"), source_mode=source_mode)
    coherent["staging"] = _staging()
    coherent["schema_evolution"] = _schema_evolution(plan.get("schema_evolution"))
    coherent["physical_design"] = _physical_design(plan.get("physical_design"), unique_key=unique_key)
    coherent["state"] = _target_local_authority(source_mode=source_mode)
    if "strategy_intelligence" in plan:
        coherent["strategy_intelligence"] = _strategy_intelligence(plan.get("strategy_intelligence"))
    return coherent


def _strategy(value: object, *, source_mode: str) -> dict[str, Any]:
    strategy = dict(value) if isinstance(value, Mapping) else {}
    strategy["mssql_contract"] = {
        "mode": source_mode,
        "execution": "target_local_v2_unit_of_work",
        "transaction_scope": "same_target_database",
        "legacy_projection": "suppressed",
    }
    return strategy


def _staging() -> dict[str, Any]:
    return {
        "staging_first": True,
        "schema": None,
        "shadow_or_swap": False,
        "finalization": "target_local_v2_unit_of_work",
        "provisioning": "external",
        "authority": "sealed_target_local_artifact",
        "legacy_projection": "suppressed",
    }


def _schema_evolution(value: object) -> dict[str, Any]:
    generic = dict(value) if isinstance(value, Mapping) else {}
    return {
        "enabled": False,
        "configured_enabled": bool(generic.get("enabled", True)),
        "mode": "external_contract",
        "apply_safe": False,
        "ddl_preview": [],
        "on_type_change": generic.get("on_type_change", "fail"),
        "runtime_execution": "bypassed",
        "provisioning": "external",
        "authority": "registered_target_contract",
    }


def _physical_design(value: object, *, unique_key: tuple[str, ...]) -> dict[str, Any]:
    generic = dict(value) if isinstance(value, Mapping) else {}
    options_value = generic.get("options")
    configured_options = options_value if isinstance(options_value, Mapping) else {}
    configured_enabled = bool(configured_options.get("enabled", True))
    key_contract = {
        "columns": list(unique_key),
        "exact_unique": True,
        "nullable": False,
    }
    return {
        "options": {"enabled": False},
        "configured_enabled": configured_enabled,
        "ddl": [],
        "apply_runtime": False,
        "runtime_ddl": "disabled",
        "provisioning": "external",
        "ddl_authority": "platform_provisioner",
        "generated_ddl_suppressed": True,
        "external_contract": {
            "route": "postgres_mssql_target_uow_v2",
            "target_must_exist": True,
            "schema_must_exist": True,
            "object_profile": "ordinary_disk_rowstore",
            "business_key": key_contract,
            "unique_index": {
                "required": True,
                "columns": list(unique_key),
                "nullable": False,
                "exact": True,
            },
        },
    }


def _target_local_authority(*, source_mode: str) -> dict[str, Any]:
    return {
        "backend": "mssql",
        "atomicity": "same_target_database_transaction",
        "provisioning": "external",
        "location_authority": "target_local",
        "authority_contract": "mssql_effect_receipt_v2",
        "writer_fence": "target_local_v2",
        "row_hash": "generation_scoped",
        "checkpoint": "target_local_v2" if source_mode == "xmin_current_state" else "not_applicable",
        "physical_identifiers": "redacted",
    }


def _strategy_intelligence(value: object) -> dict[str, Any]:
    intelligence = value if isinstance(value, Mapping) else {}
    decision_value = intelligence.get("decision")
    decision = decision_value if isinstance(decision_value, Mapping) else {}
    return {
        "decision": {
            "strategy_mode": decision.get("strategy_mode"),
            "execution_profile": "postgres_mssql_target_uow_v2",
            "legacy_projection": "suppressed",
        }
    }


__all__ = ["cohere_selected_r1_plan"]
