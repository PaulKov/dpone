"""File-IO facade for schema contract registry commands and migration integration."""

from __future__ import annotations

import json
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml

CONTRACT_GATE_SCHEMA = "dpone.schema_contract_gate.v1"


class SchemaContractFacade:
    def publish(
        self, *, manifest_path: str, store_backend: str | None = None, store_uri: str | None = None
    ) -> dict[str, Any]:
        manifest = _read_mapping(manifest_path)
        version = _contracts().SchemaContractVersionBuilder().build(manifest=manifest)
        store = _store_from_version(version, store_backend=store_backend, store_uri=store_uri)
        latest = store.latest(contract_id=str(version["contract_id"]))
        if latest and latest.get("contract_version_id") != version.get("contract_version_id"):
            plan = _compatibility(latest, version, manifest)
            if plan.get("status") == "blocked":
                return {**version, "status": "blocked", "compatibility_plan": plan, "blockers": plan["blockers"]}
        append = store.append(version)
        if append["status"] == "blocked":
            return {**version, "status": "blocked", "blockers": append["blockers"]}
        return {**version, "append_status": append["status"]}

    def check(self, *, manifest_path: str, against: str, compatibility: str | None = None) -> dict[str, Any]:
        manifest = _read_mapping(manifest_path)
        head = _contracts().SchemaContractVersionBuilder().build(manifest=manifest)
        base = _resolve_against(against, head, manifest)
        if base is None:
            return _blocked_plan(head, f"schema_contract_registry.against_not_found:{against}")
        return (
            _contracts()
            .SchemaContractComparator()
            .compare(
                base=base,
                head=head,
                compatibility=compatibility or str(head.get("compatibility") or "backward"),
            )
        )

    def gate(self, *, manifest_path: str, pack_path: str | None = None) -> dict[str, Any]:
        manifest = _read_mapping(manifest_path)
        head = _contracts().SchemaContractVersionBuilder().build(manifest=manifest)
        store = _store_from_version(head)
        base = store.latest(contract_id=str(head["contract_id"]))
        plan = (
            _contracts()
            .SchemaContractComparator()
            .compare(base=base, head=head, compatibility=str(head["compatibility"]))
            if base
            else _new_contract_plan(head)
        )
        decision = (
            _contracts()
            .SchemaConsumerCompatibilityGate()
            .evaluate(
                contract_version=head,
                compatibility_plan=plan,
                consumers=_consumers(manifest),
                unknown_consumer=str(_versioning(manifest).get("unknown_consumer") or "warn"),
            )
        )
        pack = _read_optional(pack_path)
        if plan.get("status") in {"blocked", "breaking"}:
            decision["blockers"] = list(dict.fromkeys([*decision["blockers"], *plan.get("blockers", [])]))
            decision["status"] = "blocked"
        decision.update(
            {
                "pack_id": pack.get("pack_id") if pack else None,
                "compatibility_plan_id": plan.get("compatibility_plan_id"),
                "required_bump": plan.get("required_bump"),
                "contract_breaking": plan.get("required_bump") == "major",
            }
        )
        consumer_summary = _consumer_matrix_summary(manifest_path)
        if consumer_summary is not None:
            decision["consumer_matrix_summary"] = consumer_summary
            if consumer_summary.get("status") == "blocked":
                decision["status"] = "blocked"
                decision["blockers"] = list(
                    dict.fromkeys([*decision.get("blockers", []), *consumer_summary.get("blockers", [])])
                )
        decision["contract_gate_id"] = _fingerprint(decision)
        return decision

    def history(self, *, contract_id: str, store_backend: str, store_uri: str | None) -> dict[str, Any]:
        return {
            "schema_version": "dpone.schema_contract_registry.v1",
            "contract_id": contract_id,
            "versions": list(_store(store_backend, store_uri).history(contract_id=contract_id)),
        }

    def latest(self, *, contract_id: str, store_backend: str, store_uri: str | None) -> dict[str, Any]:
        latest = _store(store_backend, store_uri).latest(contract_id=contract_id)
        return latest or {
            "schema_version": "dpone.schema_contract_version.v1",
            "status": "blocked",
            "blockers": ["schema_contract_registry.contract_not_found"],
        }

    def consumers(self, *, contract_id: str, version: str, store_backend: str, store_uri: str | None) -> dict[str, Any]:
        del version
        latest = _store(store_backend, store_uri).latest(contract_id=contract_id)
        consumers = list(latest.get("consumers", [])) if latest else []
        return {
            "schema_version": "dpone.schema_contract_consumer_binding.v1",
            "contract_id": contract_id,
            "consumers": consumers,
        }

    def deprecate(
        self,
        *,
        contract_id: str,
        column: str,
        remove_after: str,
        store_backend: str,
        store_uri: str | None,
    ) -> dict[str, Any]:
        latest = _store(store_backend, store_uri).latest(contract_id=contract_id)
        blockers = [] if latest else ["schema_contract_registry.contract_not_found"]
        payload: dict[str, Any] = {
            "schema_version": CONTRACT_GATE_SCHEMA,
            "status": "blocked" if blockers else "allowed",
            "contract_id": contract_id,
            "contract_version_id": latest.get("contract_version_id") if latest else None,
            "deprecation": {"column": column, "remove_after": remove_after},
            "blockers": blockers,
            "warnings": ["schema_contract.deprecation_recorded"],
        }
        payload["contract_gate_id"] = _fingerprint(payload)
        return payload


def attach_contract_compatibility_summary(*, payload: dict[str, Any], manifest_path: str) -> dict[str, Any]:
    manifest = _read_mapping(manifest_path)
    head = _contracts().SchemaContractVersionBuilder().build(manifest=manifest)
    registry = _registry(head)
    if not registry.get("enabled"):
        return payload
    store = _store(str(registry["store_backend"]), str(registry["store_uri"]))
    base = store.latest(contract_id=str(head["contract_id"]))
    plan = (
        _contracts().SchemaContractComparator().compare(base=base, head=head, compatibility=str(head["compatibility"]))
        if base
        else _new_contract_plan(head)
    )
    summary = {
        "enabled": True,
        "mode": registry.get("mode"),
        "contract_id": head["contract_id"],
        "version": head["version"],
        "contract_version_id": head["contract_version_id"],
        "status": plan["status"],
        "required_bump": plan["required_bump"],
        "contract_breaking": plan["required_bump"] == "major",
        "blockers": plan["blockers"],
        "warnings": plan["warnings"],
    }
    result = {**payload, "contract_version_id": head["contract_version_id"], "contract_compatibility_summary": summary}
    consumer_summary = _consumer_matrix_summary(manifest_path)
    if consumer_summary is not None:
        result["consumer_matrix_summary"] = consumer_summary
    view_summary = _compatibility_view_summary(manifest_path)
    if view_summary is not None:
        result["compatibility_view_summary"] = view_summary
        result["phases"] = _merge_compatibility_view_phases(
            list(result.get("phases", [])),
            view_summary.get("phases", []),
        )
    adoption_summary = _adoption_summary(manifest_path)
    if adoption_summary is not None:
        result["adoption_summary"] = adoption_summary
    return result


def schema_contract_apply_blockers(plan_payload: Mapping[str, Any]) -> tuple[str, ...]:
    summary = plan_payload.get("contract_compatibility_summary")
    if not isinstance(summary, Mapping) or not summary.get("enabled"):
        return ()
    if summary.get("mode") != "gate":
        return ()
    if summary.get("status") in {"compatible", "new"} and not summary.get("blockers"):
        return _compatibility_view_apply_blockers(plan_payload)
    return (
        "schema_contract_gate.not_allowed",
        *tuple(str(item) for item in summary.get("blockers", []) if str(item)),
        *_compatibility_view_apply_blockers(plan_payload),
    )


def _compatibility(base: Mapping[str, Any], head: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, Any]:
    return (
        _contracts()
        .SchemaContractComparator()
        .compare(
            base=base,
            head=head,
            compatibility=str(_schema_contract(manifest).get("compatibility") or "backward"),
        )
    )


def _resolve_against(against: str, head: Mapping[str, Any], manifest: Mapping[str, Any]) -> dict[str, Any] | None:
    path = Path(against)
    if path.exists():
        return _read_mapping(str(path))
    contract_id, _, version = against.partition("@")
    if not version:
        return None
    return _store_from_version(head).get(contract_id=contract_id, version=version)


def _new_contract_plan(head: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": "dpone.schema_contract_compatibility_plan.v1",
        "status": "new",
        "contract_id": head.get("contract_id"),
        "base_version": None,
        "head_version": head.get("version"),
        "head_contract_version_id": head.get("contract_version_id"),
        "compatibility": head.get("compatibility"),
        "required_bump": "minor",
        "changes": [],
        "risk_tags": [],
        "blockers": [],
        "warnings": ["schema_contract.new_contract"],
        "compatibility_plan_id": _fingerprint({"new": head.get("contract_version_id")}),
    }


def _blocked_plan(head: Mapping[str, Any], blocker: str) -> dict[str, Any]:
    return {
        "schema_version": "dpone.schema_contract_compatibility_plan.v1",
        "status": "blocked",
        "contract_id": head.get("contract_id"),
        "head_version": head.get("version"),
        "head_contract_version_id": head.get("contract_version_id"),
        "required_bump": "major",
        "changes": [],
        "risk_tags": ["schema_contract.compatibility_breaking"],
        "blockers": [blocker],
        "warnings": [],
        "compatibility_plan_id": _fingerprint({"blocked": blocker, "head": head.get("contract_version_id")}),
    }


def _store_from_version(
    version: Mapping[str, Any], *, store_backend: str | None = None, store_uri: str | None = None
) -> Any:
    registry = _registry(version)
    return _store(store_backend or str(registry["store_backend"]), store_uri or str(registry["store_uri"]))


def _store(backend: str, uri: str | None) -> Any:
    normalized = backend.strip().lower().replace("-", "_")
    if normalized == "sqlite":
        return import_module("dpone.readiness.schema_contract_registry_sqlite").SqliteSchemaContractRegistryStore(
            uri or ".dpone/schema-contracts/registry.sqlite3"
        )
    return import_module("dpone.readiness.schema_contract_registry_store").LocalJsonSchemaContractRegistryStore(
        uri or ".dpone/schema-contracts/registry.json"
    )


def _read_optional(path: str | None) -> dict[str, Any]:
    return _read_mapping(path) if path else {}


def _read_mapping(path: str) -> dict[str, Any]:
    text = Path(path).read_text(encoding="utf-8")
    raw = json.loads(text) if path.lower().endswith(".json") else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _schema_contract(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_mapping(_mapping(manifest.get("sink")).get("options")).get("schema_contract"))


def _versioning(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_schema_contract(manifest).get("versioning"))


def _consumers(manifest: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = _mapping(_schema_contract(manifest).get("consumers")).get("manual", [])
    return [dict(item) for item in raw if isinstance(item, Mapping)]


def _registry(version: Mapping[str, Any]) -> dict[str, Any]:
    return dict(version.get("registry", {})) if isinstance(version.get("registry"), Mapping) else {}


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _contracts() -> Any:
    return import_module("dpone.readiness.schema_contract_registry")


def _fingerprint(payload: Mapping[str, Any]) -> str:
    from dpone.readiness.migration_control import stable_fingerprint

    return stable_fingerprint(dict(payload))


def _consumer_matrix_summary(manifest_path: str) -> dict[str, Any] | None:
    module = import_module("dpone.services.schema_contract_consumers")
    return module.SchemaConsumerDiscoveryFacade().summary(manifest_path=manifest_path)


def _compatibility_view_summary(manifest_path: str) -> dict[str, Any] | None:
    module = import_module("dpone.services.schema_contract_compatibility_views")
    return module.SchemaContractCompatibilityViewFacade().summary(manifest_path=manifest_path)


def _adoption_summary(manifest_path: str) -> dict[str, Any] | None:
    module = import_module("dpone.services.schema_contract_adoption")
    return module.SchemaContractAdoptionFacade().summary(manifest_path=manifest_path)


def _merge_compatibility_view_phases(current: list[Any], compatibility_phases: object) -> list[Any]:
    phases = (
        [dict(item) for item in compatibility_phases if isinstance(item, Mapping)]
        if isinstance(compatibility_phases, list)
        else []
    )
    if not phases:
        return current
    expand = [item for item in phases if item.get("name") == "compatibility_views_expand"]
    validate = [item for item in phases if item.get("name") == "compatibility_views_validate"]
    return [*expand, *current, *validate]


def _compatibility_view_apply_blockers(plan_payload: Mapping[str, Any]) -> tuple[str, ...]:
    summary = plan_payload.get("compatibility_view_summary")
    if not isinstance(summary, Mapping) or not summary.get("enabled"):
        return ()
    if summary.get("mode") != "gate":
        return ()
    if summary.get("status") in {"planned", "warning"} and not summary.get("blockers"):
        return ()
    return (
        "schema_contract_view_gate.not_allowed",
        *tuple(str(item) for item in summary.get("blockers", []) if str(item)),
    )
