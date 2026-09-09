"""Provider-neutral schema contract adoption and retirement evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

ADOPTION_PLAN_SCHEMA = "dpone.schema_contract_adoption_plan.v1"
ADOPTION_STATUS_SCHEMA = "dpone.schema_contract_adoption_status.v1"
RETIREMENT_GATE_SCHEMA = "dpone.schema_contract_retirement_gate.v1"
RETIREMENT_PLAN_SCHEMA = "dpone.schema_contract_retirement_plan.v1"


@dataclass(frozen=True, slots=True)
class SchemaContractAdoptionOptions:
    enabled: bool
    mode: str = "gate"
    profile: str = "prod_strict"
    default_migration_window_days: int = 90
    expired_window_policy: str = "block"
    require_consumer_certification: bool = True
    unknown_consumer: str = "warn"

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> SchemaContractAdoptionOptions:
        raw = _mapping(_schema_contract(manifest).get("adoption"))
        if not raw:
            return cls(enabled=False)
        return cls(
            enabled=bool(raw.get("enabled", False)),
            mode=str(raw.get("mode") or "gate"),
            profile=str(raw.get("profile") or "prod_strict"),
            default_migration_window_days=_int(raw.get("default_migration_window_days"), 90),
            expired_window_policy=str(raw.get("expired_window_policy") or "block"),
            require_consumer_certification=bool(raw.get("require_consumer_certification", True)),
            unknown_consumer=str(raw.get("unknown_consumer") or "warn"),
        )


class SchemaContractAdoptionPlanner:
    """Builds an owner-routed adoption plan from consumer and view evidence."""

    def plan(
        self,
        *,
        manifest: Mapping[str, Any],
        base_contract: Mapping[str, Any],
        head_contract: Mapping[str, Any],
        consumer_matrix: Mapping[str, Any],
        compatibility_view_plan: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        options = SchemaContractAdoptionOptions.from_manifest(manifest)
        consumers = [
            _consumer_plan_item(item, compatibility_view_plan)
            for item in consumer_matrix.get("consumers", [])
            if isinstance(item, Mapping)
        ]
        blockers = _plan_blockers(options, consumers)
        warnings = _plan_warnings(options, blockers)
        payload: dict[str, Any] = {
            "schema_version": ADOPTION_PLAN_SCHEMA,
            "status": "blocked" if blockers else "planned" if options.enabled else "disabled",
            "contract_id": head_contract.get("contract_id"),
            "base_version": base_contract.get("version"),
            "head_version": head_contract.get("version"),
            "base_contract_version_id": base_contract.get("contract_version_id"),
            "head_contract_version_id": head_contract.get("contract_version_id"),
            "consumer_matrix_id": consumer_matrix.get("consumer_matrix_id"),
            "compatibility_view_plan_id": _optional(compatibility_view_plan, "compatibility_view_plan_id"),
            "mode": options.mode,
            "profile": options.profile,
            "options": options_to_dict(options),
            "consumers": consumers,
            "summary": _plan_summary(consumers),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["adoption_plan_id"] = stable_fingerprint(payload)
        return payload


class SchemaContractAdoptionStatusBuilder:
    """Computes current adoption state from plan, registry and certification evidence."""

    def build(
        self,
        *,
        plan: Mapping[str, Any],
        registry_records: Sequence[Mapping[str, Any]] = (),
        consumer_certifications: Sequence[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        consumers = [
            _consumer_status_item(item, registry_records, consumer_certifications)
            for item in plan.get("consumers", [])
            if isinstance(item, Mapping)
        ]
        blockers = [str(item) for item in plan.get("blockers", []) if str(item)]
        blockers.extend(_status_blockers(plan, consumers))
        warnings = [str(item) for item in plan.get("warnings", []) if str(item)]
        payload: dict[str, Any] = {
            "schema_version": ADOPTION_STATUS_SCHEMA,
            "status": _status(consumers, blockers),
            "contract_id": plan.get("contract_id"),
            "base_version": plan.get("base_version"),
            "head_version": plan.get("head_version"),
            "adoption_plan_id": plan.get("adoption_plan_id"),
            "consumer_matrix_id": plan.get("consumer_matrix_id"),
            "compatibility_view_plan_id": plan.get("compatibility_view_plan_id"),
            "consumers": consumers,
            "summary": _status_summary(consumers),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["adoption_status_id"] = stable_fingerprint(payload)
        return payload


class SchemaContractRetirementGate:
    """Fail-closed gate for compatibility-view or old-contract retirement."""

    def evaluate(self, *, status: Mapping[str, Any], profile: str = "prod_strict") -> dict[str, Any]:
        blockers = [str(item) for item in status.get("blockers", []) if str(item)]
        warnings = [str(item) for item in status.get("warnings", []) if str(item)]
        blockers.extend(_retirement_blockers(status, profile))
        if profile == "advisory":
            warnings.extend(blockers)
            blockers = []
        payload: dict[str, Any] = {
            "schema_version": RETIREMENT_GATE_SCHEMA,
            "status": "blocked" if blockers else "warning" if warnings else "allowed",
            "profile": profile,
            "contract_id": status.get("contract_id"),
            "adoption_status_id": status.get("adoption_status_id"),
            "consumer_matrix_id": status.get("consumer_matrix_id"),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["retirement_gate_id"] = stable_fingerprint(payload)
        return payload

    def retire(
        self,
        *,
        gate: Mapping[str, Any],
        compatibility_view_plan: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        blockers = [str(item) for item in gate.get("blockers", []) if str(item)]
        views = _retirement_views(compatibility_view_plan)
        payload: dict[str, Any] = {
            "schema_version": RETIREMENT_PLAN_SCHEMA,
            "status": "blocked" if blockers else "ready" if gate.get("status") == "allowed" else "warning",
            "contract_id": gate.get("contract_id"),
            "retirement_gate_id": gate.get("retirement_gate_id"),
            "compatibility_view_plan_id": _optional(compatibility_view_plan, "compatibility_view_plan_id"),
            "views": views,
            "blockers": blockers,
            "warnings": [str(item) for item in gate.get("warnings", []) if str(item)],
            "recommendations": _recommendations(blockers, gate.get("warnings", [])),
        }
        payload["retirement_plan_id"] = stable_fingerprint(payload)
        return payload


def options_to_dict(options: SchemaContractAdoptionOptions) -> dict[str, Any]:
    return {
        "enabled": options.enabled,
        "mode": options.mode,
        "profile": options.profile,
        "default_migration_window_days": options.default_migration_window_days,
        "expired_window_policy": options.expired_window_policy,
        "require_consumer_certification": options.require_consumer_certification,
        "unknown_consumer": options.unknown_consumer,
    }


def _consumer_plan_item(consumer: Mapping[str, Any], view_plan: Mapping[str, Any] | None) -> dict[str, Any]:
    constraint = str(consumer.get("version_constraint") or "")
    view = _matching_view(constraint, view_plan)
    blockers = [] if view else list(consumer.get("blockers", [])) if isinstance(consumer.get("blockers"), list) else []
    return {
        "consumer_id": str(consumer.get("id") or ""),
        "owner": consumer.get("owner"),
        "type": consumer.get("type"),
        "source": consumer.get("source"),
        "version_constraint": constraint,
        "reads": dict(consumer.get("reads", {})) if isinstance(consumer.get("reads"), Mapping) else {},
        "confidence": consumer.get("confidence") or _confidence(consumer),
        "compatibility_view": view.get("view") if view else None,
        "remove_after": view.get("remove_after") if view else None,
        "state": "not_started",
        "blockers": blockers,
    }


def _consumer_status_item(
    consumer: Mapping[str, Any],
    records: Sequence[Mapping[str, Any]],
    certifications: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    item = dict(consumer)
    consumer_id = str(item.get("consumer_id") or "")
    stages = _consumer_stages(records, consumer_id)
    certified = any(cert.get("status") == "certified" for cert in certifications)
    if "consumer_migrated" in stages:
        state = "migrated"
    elif certified:
        state = "certified"
    elif "consumer_migrating" in stages:
        state = "in_progress"
    elif item.get("blockers"):
        state = "blocked"
    else:
        state = "not_started"
    item["state"] = state
    item["registry_stages"] = sorted(stages)
    item["certified"] = certified
    return item


def _matching_view(constraint: str, plan: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if not constraint or not plan:
        return None
    for view in plan.get("views", []):
        if (
            isinstance(view, Mapping)
            and view.get("version_constraint") == constraint
            and view.get("status") != "blocked"
        ):
            return dict(view)
    return None


def _consumer_stages(records: Sequence[Mapping[str, Any]], consumer_id: str) -> set[str]:
    stages: set[str] = set()
    for record in records:
        if not _record_mentions_consumer(record, consumer_id):
            continue
        stage = str(record.get("stage") or "")
        if stage:
            stages.add(stage)
    return stages


def _record_mentions_consumer(record: Mapping[str, Any], consumer_id: str) -> bool:
    if not consumer_id:
        return False
    for ref in record.get("artifact_refs", []):
        if isinstance(ref, Mapping) and consumer_id in {str(ref.get("path") or ""), str(ref.get("consumer_id") or "")}:
            return True
    return str(record.get("consumer_id") or "") == consumer_id


def _plan_blockers(options: SchemaContractAdoptionOptions, consumers: Sequence[Mapping[str, Any]]) -> list[str]:
    if not options.enabled:
        return []
    blockers: list[str] = []
    for consumer in consumers:
        cid = str(consumer.get("consumer_id") or "")
        if consumer.get("version_constraint") and not consumer.get("compatibility_view"):
            blockers.append(f"schema_contract_adoption.compatibility_view_missing:{cid}")
        if expired := _expired_blocker(consumer, options):
            blockers.append(expired)
    return blockers if options.mode == "gate" else []


def _plan_warnings(options: SchemaContractAdoptionOptions, blockers: Sequence[str]) -> list[str]:
    return list(blockers) if options.mode == "observe" else []


def _status_blockers(plan: Mapping[str, Any], consumers: Sequence[Mapping[str, Any]]) -> list[str]:
    del plan, consumers
    return []


def _retirement_blockers(status: Mapping[str, Any], profile: str) -> list[str]:
    if profile == "advisory":
        return []
    blockers: list[str] = []
    for consumer in status.get("consumers", []):
        if not isinstance(consumer, Mapping):
            continue
        if consumer.get("state") != "migrated":
            blockers.append(f"schema_contract_retirement.active_consumer:{consumer.get('consumer_id')}")
        if profile == "regulated" and not consumer.get("owner"):
            blockers.append(f"schema_contract_retirement.owner_missing:{consumer.get('consumer_id')}")
    return blockers


def _status(consumers: Sequence[Mapping[str, Any]], blockers: Sequence[str]) -> str:
    if blockers or any(item.get("state") == "blocked" for item in consumers):
        return "blocked"
    if consumers and all(item.get("state") == "migrated" for item in consumers):
        return "migrated"
    return "active" if consumers else "empty"


def _plan_summary(consumers: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return {
        "consumers_count": len(consumers),
        "active_consumers": sum(1 for item in consumers if item.get("version_constraint")),
        "covered_by_compatibility_view": sum(1 for item in consumers if item.get("compatibility_view")),
        "blocked_consumers": sum(1 for item in consumers if item.get("blockers")),
    }


def _status_summary(consumers: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    return {
        "consumers_count": len(consumers),
        "migrated": sum(1 for item in consumers if item.get("state") == "migrated"),
        "certified": sum(1 for item in consumers if item.get("state") == "certified"),
        "active": sum(1 for item in consumers if item.get("state") not in {"migrated", "blocked"}),
        "blocked": sum(1 for item in consumers if item.get("state") == "blocked"),
    }


def _retirement_views(plan: Mapping[str, Any] | None) -> list[dict[str, Any]]:
    if not plan:
        return []
    return [
        {
            "view": view.get("view"),
            "version_constraint": view.get("version_constraint"),
            "remove_after": view.get("remove_after"),
            "status": "retirement_planned",
        }
        for view in plan.get("views", [])
        if isinstance(view, Mapping)
    ]


def _expired_blocker(consumer: Mapping[str, Any], options: SchemaContractAdoptionOptions) -> str | None:
    remove_after = str(consumer.get("remove_after") or "")
    if not remove_after:
        due = date.today() + timedelta(days=options.default_migration_window_days)
        return None if due >= date.today() else f"schema_contract_adoption.window_expired:{consumer.get('consumer_id')}"
    try:
        expired = date.fromisoformat(remove_after) < date.today()
    except ValueError:
        return f"schema_contract_adoption.remove_after_invalid:{consumer.get('consumer_id')}"
    if expired and options.expired_window_policy == "block":
        return f"schema_contract_adoption.window_expired:{consumer.get('consumer_id')}"
    return None


def _schema_contract(manifest: Mapping[str, Any]) -> dict[str, Any]:
    return _mapping(_mapping(_mapping(manifest.get("sink")).get("options")).get("schema_contract"))


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _optional(payload: Mapping[str, Any] | None, key: str) -> Any:
    return payload.get(key) if payload else None


def _confidence(consumer: Mapping[str, Any]) -> str:
    reads = consumer.get("reads")
    if isinstance(reads, Mapping) and reads.get("columns"):
        return "explicit"
    return "table_only"


def _int(value: object, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _recommendations(blockers: Sequence[object], warnings: Sequence[object]) -> list[str]:
    if blockers:
        return ["Finish consumer migration or keep compatibility views before retirement."]
    if warnings:
        return ["Review adoption warnings before promoting the retirement pack."]
    return ["Consumer adoption evidence is clean."]
