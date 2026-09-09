"""File-IO facade for schema contract adoption commands."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class SchemaContractAdoptionFacade:
    """Thin facade; adoption business rules live in readiness modules."""

    def plan(
        self,
        *,
        manifest_path: str,
        consumer_matrix_path: str,
        compatibility_view_plan_path: str | None = None,
    ) -> dict[str, Any]:
        manifest = _read_mapping(manifest_path)
        matrix = _read_mapping(consumer_matrix_path)
        view_plan = _read_optional(compatibility_view_plan_path)
        head = _contracts().SchemaContractVersionBuilder().build(manifest=manifest)
        base = _base_contract(matrix, view_plan, head)
        return (
            _adoption()
            .SchemaContractAdoptionPlanner()
            .plan(
                manifest=manifest,
                base_contract=base,
                head_contract=head,
                consumer_matrix=matrix,
                compatibility_view_plan=view_plan,
            )
        )

    def status(
        self,
        *,
        plan_path: str,
        registry_path: str | None = None,
        consumer_certification_path: str | None = None,
    ) -> dict[str, Any]:
        return (
            _adoption()
            .SchemaContractAdoptionStatusBuilder()
            .build(
                plan=_read_mapping(plan_path),
                registry_records=_registry_records(registry_path),
                consumer_certifications=_certifications(consumer_certification_path),
            )
        )

    def gate(self, *, status_path: str, profile: str) -> dict[str, Any]:
        return (
            _adoption()
            .SchemaContractRetirementGate()
            .evaluate(
                status=_read_mapping(status_path),
                profile=profile,
            )
        )

    def retire(self, *, gate_path: str, compatibility_view_plan_path: str | None = None) -> dict[str, Any]:
        return (
            _adoption()
            .SchemaContractRetirementGate()
            .retire(
                gate=_read_mapping(gate_path),
                compatibility_view_plan=_read_optional(compatibility_view_plan_path),
            )
        )

    def summary(self, *, manifest_path: str) -> dict[str, Any] | None:
        manifest = _read_mapping(manifest_path)
        options = _adoption().SchemaContractAdoptionOptions.from_manifest(manifest)
        if not options.enabled:
            return None
        head = _contracts().SchemaContractVersionBuilder().build(manifest=manifest)
        base = _latest_contract(head)
        if base is None:
            return {
                "enabled": True,
                "status": "warning",
                "contract_id": head.get("contract_id"),
                "version": head.get("version"),
                "adoption_plan_id": None,
                "blockers": [],
                "warnings": ["schema_contract_adoption.base_contract_not_found"],
            }
        matrix = _manual_consumer_matrix(manifest=manifest, base=base, head=head)
        view_plan = _compatibility_view_plan(manifest=manifest, base=base, head=head, matrix=matrix)
        plan = (
            _adoption()
            .SchemaContractAdoptionPlanner()
            .plan(
                manifest=manifest,
                base_contract=base,
                head_contract=head,
                consumer_matrix=matrix,
                compatibility_view_plan=view_plan,
            )
        )
        summary = dict(plan.get("summary", {})) if isinstance(plan.get("summary"), Mapping) else {}
        return {
            "enabled": True,
            "mode": plan.get("mode"),
            "profile": plan.get("profile"),
            "status": plan.get("status"),
            "contract_id": plan.get("contract_id"),
            "version": plan.get("head_version"),
            "adoption_plan_id": plan.get("adoption_plan_id"),
            "consumer_matrix_id": plan.get("consumer_matrix_id"),
            "compatibility_view_plan_id": plan.get("compatibility_view_plan_id"),
            "active_consumers": summary.get("active_consumers", 0),
            "covered_by_compatibility_view": summary.get("covered_by_compatibility_view", 0),
            "blockers": plan.get("blockers", []),
            "warnings": plan.get("warnings", []),
        }


def _base_contract(
    matrix: Mapping[str, Any],
    view_plan: Mapping[str, Any] | None,
    head: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "contract_id": matrix.get("contract_id") or head.get("contract_id"),
        "version": matrix.get("base_version") or _optional(view_plan, "base_version"),
        "contract_version_id": _optional(view_plan, "base_contract_version_id"),
    }


def _latest_contract(head: Mapping[str, Any]) -> dict[str, Any] | None:
    registry = _mapping(head.get("registry"))
    backend = str(registry.get("store_backend") or "local_json")
    uri = str(registry.get("store_uri") or ".dpone/schema-contracts/registry.json")
    if backend == "sqlite":
        return _sqlite_registry().SqliteSchemaContractRegistryStore(uri).latest(contract_id=str(head["contract_id"]))
    return _json_registry().LocalJsonSchemaContractRegistryStore(uri).latest(contract_id=str(head["contract_id"]))


def _manual_consumer_matrix(
    *, manifest: Mapping[str, Any], base: Mapping[str, Any], head: Mapping[str, Any]
) -> dict[str, Any]:
    consumers = _mapping(_schema_contract(manifest).get("consumers")).get("manual", [])
    inventory = {
        "schema_version": "dpone.schema_contract_consumer_inventory.v1",
        "contract_id": head.get("contract_id"),
        "status": "discovered",
        "blockers": [],
        "warnings": [],
        "consumers": [dict(item) for item in consumers if isinstance(item, Mapping)],
    }
    return (
        _matrix()
        .SchemaConsumerMatrixBuilder()
        .build(
            base=base,
            head=head,
            inventory=inventory,
            unknown_consumer=str(_versioning(manifest).get("unknown_consumer") or "warn"),
        )
    )


def _compatibility_view_plan(
    *,
    manifest: Mapping[str, Any],
    base: Mapping[str, Any],
    head: Mapping[str, Any],
    matrix: Mapping[str, Any],
) -> dict[str, Any] | None:
    options = _views().SchemaContractServingOptions.from_manifest(manifest)
    if not options.enabled:
        return None
    return (
        _views()
        .CompatibilityViewPlanner()
        .plan(
            manifest=manifest,
            base_contract=base,
            head_contract=head,
            consumer_matrix=matrix,
        )
    )


def _registry_records(path: str | None) -> tuple[dict[str, Any], ...]:
    if not path:
        return ()
    payload = _read_mapping(path)
    records = payload.get("records", [])
    return tuple(dict(item) for item in records if isinstance(item, Mapping)) if isinstance(records, list) else ()


def _certifications(path: str | None) -> tuple[dict[str, Any], ...]:
    payload = _read_optional(path)
    return (payload,) if payload else ()


def _read_optional(path: str | None) -> dict[str, Any] | None:
    return _read_mapping(path) if path else None


def _read_mapping(path: str) -> dict[str, Any]:
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _schema_contract(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_mapping(_mapping(manifest.get("sink")).get("options")).get("schema_contract"))


def _versioning(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_schema_contract(manifest).get("versioning"))


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _optional(payload: Mapping[str, Any] | None, key: str) -> Any:
    return payload.get(key) if payload else None


def _contracts() -> Any:
    return import_module("dpone.readiness.schema_contract_registry")


def _adoption() -> Any:
    return import_module("dpone.readiness.schema_contract_adoption")


def _views() -> Any:
    return import_module("dpone.readiness.schema_contract_compatibility_views")


def _matrix() -> Any:
    return import_module("dpone.readiness.schema_contract_consumer_matrix")


def _json_registry() -> Any:
    return import_module("dpone.readiness.schema_contract_registry_store")


def _sqlite_registry() -> Any:
    return import_module("dpone.readiness.schema_contract_registry_sqlite")


__all__ = ["SchemaContractAdoptionFacade"]
