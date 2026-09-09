"""Provider-neutral data product fleet reliability control tower contracts."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from dpone.readiness.data_product_fleet_rendering import (
    FLEET_REPORT_SCHEMA,
    RELIABILITY_EXPORT_SCHEMA,
    ROUTE_DELIVERY_RECEIPT_SCHEMA,
    IncidentRouteDryRunEvaluator,
    ReliabilityExportRenderer,
)
from dpone.readiness.migration_control import stable_fingerprint

FLEET_EVALUATION_SCHEMA = "dpone.data_product_fleet_evaluation.v1"
FLEET_GATE_SCHEMA = "dpone.data_product_fleet_gate.v1"


@dataclass(frozen=True, slots=True)
class FleetReliabilityOptions:
    enabled: bool
    mode: str
    profile: str
    unknown_product: str
    stale_evidence_policy: str
    stale_after_seconds: int
    freeze_on: tuple[str, ...]

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> FleetReliabilityOptions:
        tower = _tower(manifest)
        fleet = _mapping(tower.get("fleet"))
        return cls(
            enabled=bool(tower.get("enabled", False)),
            mode=str(tower.get("mode") or "gate"),
            profile=str(tower.get("profile") or "prod_strict"),
            unknown_product=str(fleet.get("unknown_product") or "warn"),
            stale_evidence_policy=str(fleet.get("stale_evidence_policy") or "block"),
            stale_after_seconds=int(fleet.get("stale_after_seconds") or 86400),
            freeze_on=tuple(str(item) for item in fleet.get("freeze_on", []) if str(item)),
        )


class FleetReliabilityEvaluator:
    """Evaluates fleet health from manifests and local registry evidence."""

    def evaluate(
        self,
        *,
        manifests: Sequence[Mapping[str, Any]],
        registry_records: Sequence[Mapping[str, Any]],
        observed_at: str | None = None,
    ) -> dict[str, Any]:
        products = [_product(manifest) for manifest in manifests]
        options = _fleet_options(manifests)
        records = _sorted_records(registry_records)
        observed = _observed_at(records, observed_at)
        evaluated = [
            _evaluate_product(
                product=product, options=options, records=_records_for_product(product, records), observed=observed
            )
            for product in products
        ]
        summary = _summary(evaluated)
        blockers = _evaluation_blockers(evaluated, options)
        warnings = _evaluation_warnings(evaluated, options)
        status = "disabled" if not options.enabled else _fleet_status(summary, blockers, warnings)
        payload: dict[str, Any] = {
            "schema_version": FLEET_EVALUATION_SCHEMA,
            "status": status,
            "profile": options.profile,
            "observed_at": _iso(observed),
            "summary": summary,
            "products": evaluated,
            "by_owner": _group(evaluated, "owner"),
            "by_tier": _group(evaluated, "tier"),
            "by_criticality": _group(evaluated, "criticality"),
            "release_freeze_reasons": _freeze_reasons(evaluated),
            "top_risks": _top_risks(evaluated),
            "source_records": records,
            "blockers": blockers,
            "warnings": warnings,
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["fleet_evaluation_id"] = stable_fingerprint(payload)
        return payload


class FleetReliabilityGate:
    """Profile-aware fleet release/freeze gate."""

    def evaluate(self, *, evaluation: Mapping[str, Any], profile: str = "prod_strict") -> dict[str, Any]:
        blockers = [str(item) for item in evaluation.get("blockers", []) if str(item)]
        warnings = [str(item) for item in evaluation.get("warnings", []) if str(item)]
        if evaluation.get("status") == "frozen":
            blockers.insert(0, "data_product_fleet.release_frozen")
        if profile == "advisory":
            warnings.extend(blockers)
            blockers = []
        elif profile == "stage":
            blockers = [item for item in blockers if "sev1" in item or "critical" in item]
        elif profile == "regulated":
            blockers.extend(_regulated_blockers(evaluation))
        status = (
            "frozen"
            if blockers and evaluation.get("status") == "frozen"
            else "blocked"
            if blockers
            else "warning"
            if warnings
            else "allowed"
        )
        payload: dict[str, Any] = {
            "schema_version": FLEET_GATE_SCHEMA,
            "status": status,
            "profile": profile,
            "fleet_evaluation_id": evaluation.get("fleet_evaluation_id"),
            "summary": evaluation.get("summary", {}),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["fleet_gate_id"] = stable_fingerprint(payload)
        return payload


def _evaluate_product(
    *,
    product: Mapping[str, Any],
    options: FleetReliabilityOptions,
    records: Sequence[Mapping[str, Any]],
    observed: datetime,
) -> dict[str, Any]:
    latest = records[-1] if records else None
    blockers = list(latest.get("blockers", [])) if latest else []
    warnings = list(latest.get("warnings", [])) if latest else []
    if latest and _is_stale(latest, observed, options):
        code = f"data_product_fleet.stale_evidence:{product['id']}"
        (blockers if options.stale_evidence_policy == "block" else warnings).append(code)
    status = _product_status(latest, blockers, warnings)
    return {
        **product,
        "status": "unknown" if not options.enabled else status,
        "latest_record_id": latest.get("record_id") if latest else None,
        "latest_recorded_at": latest.get("recorded_at") if latest else None,
        "artifact_refs": list(latest.get("artifact_refs", [])) if latest else [],
        "blockers": list(dict.fromkeys(str(item) for item in blockers if str(item))),
        "warnings": list(dict.fromkeys(str(item) for item in warnings if str(item))),
        "action": _action(status, blockers),
    }


def _product(manifest: Mapping[str, Any]) -> dict[str, Any]:
    sink = _mapping(manifest.get("sink"))
    options = _mapping(sink.get("options"))
    product = _mapping(options.get("data_product"))
    contract = _mapping(options.get("schema_contract"))
    product_id = str(product.get("id") or contract.get("id") or _target_table(sink) or "")
    return {
        "id": product_id,
        "owner": str(product.get("owner") or "unknown"),
        "tier": str(product.get("tier") or "unknown"),
        "criticality": str(product.get("criticality") or "medium"),
        "contract_id": contract.get("id"),
        "contract_version": contract.get("version"),
    }


def _product_status(latest: Mapping[str, Any] | None, blockers: Sequence[object], warnings: Sequence[object]) -> str:
    if latest is None:
        return "unknown"
    if _has_freeze_signal(blockers):
        return "frozen"
    if _has_incident_signal(blockers):
        return "incident_active"
    if blockers:
        return "frozen"
    if warnings or latest.get("status") == "warning":
        return "degraded"
    return "healthy"


def _has_freeze_signal(items: Sequence[object]) -> bool:
    text = " ".join(str(item) for item in items)
    return any(
        token in text
        for token in ("fast_burn", "budget_remaining_exhausted", "release_closeout", "sev1", "data_product_policy")
    )


def _has_incident_signal(items: Sequence[object]) -> bool:
    return any("incident" in str(item) for item in items)


def _summary(products: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    statuses = ("healthy", "degraded", "incident_active", "frozen", "unknown")
    return {
        "products": len(products),
        **{status: sum(1 for item in products if item.get("status") == status) for status in statuses},
    }


def _fleet_status(summary: Mapping[str, int], blockers: Sequence[object], warnings: Sequence[object]) -> str:
    if summary.get("frozen", 0) > 0:
        return "frozen"
    if blockers:
        return "blocked"
    if summary.get("incident_active", 0) > 0:
        return "incident_active"
    if warnings or summary.get("degraded", 0) > 0 or summary.get("unknown", 0) > 0:
        return "degraded"
    return "healthy"


def _evaluation_blockers(products: Sequence[Mapping[str, Any]], options: FleetReliabilityOptions) -> list[str]:
    if not options.enabled:
        return []
    blockers = [str(item) for product in products for item in product.get("blockers", [])]
    if any(product.get("status") == "unknown" for product in products) and options.unknown_product == "block":
        blockers.append("data_product_fleet.unknown_product")
    return list(dict.fromkeys(blockers))


def _evaluation_warnings(products: Sequence[Mapping[str, Any]], options: FleetReliabilityOptions) -> list[str]:
    warnings = [str(item) for product in products for item in product.get("warnings", [])]
    if any(product.get("status") == "unknown" for product in products) and options.unknown_product == "warn":
        warnings.append("data_product_fleet.unknown_product")
    return list(dict.fromkeys(warnings))


def _group(products: Sequence[Mapping[str, Any]], key: str) -> dict[str, dict[str, int]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for product in products:
        grouped.setdefault(str(product.get(key) or "unknown"), []).append(product)
    return {name: _summary(items) for name, items in sorted(grouped.items())}


def _freeze_reasons(products: Sequence[Mapping[str, Any]]) -> list[str]:
    reasons: list[str] = []
    for product in products:
        product_id = product.get("id")
        text = " ".join(str(item) for item in product.get("blockers", []))
        if "fast_burn" in text:
            reasons.append(f"{product_id} fast_burn exceeded")
        if "budget_remaining_exhausted" in text:
            reasons.append(f"{product_id} budget exhausted")
        if "sev1" in text:
            reasons.append(f"{product_id} has open Sev1 incident")
        if "release_closeout" in text:
            reasons.append(f"{product_id} release closeout blocked")
        if "data_product_policy" in text:
            reasons.append(f"{product_id} policy gate blocked")
    return reasons


def _top_risks(products: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return [
        {"product_id": product.get("id"), "status": product.get("status"), "blockers": product.get("blockers", [])}
        for product in products
        if product.get("status") in {"frozen", "incident_active", "unknown"}
    ]


def _records_for_product(
    product: Mapping[str, Any], records: Sequence[Mapping[str, Any]]
) -> tuple[dict[str, Any], ...]:
    product_id = str(product.get("id"))
    return tuple(record for record in records if _record_product_id(record) == product_id)


def _record_product_id(record: Mapping[str, Any]) -> str:
    target = _mapping(record.get("target"))
    return str(record.get("product_id") or target.get("table") or "")


def _sorted_records(records: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    return sorted(
        (dict(item) for item in records), key=lambda item: (str(item.get("recorded_at")), str(item.get("record_id")))
    )


def _fleet_options(manifests: Sequence[Mapping[str, Any]]) -> FleetReliabilityOptions:
    for manifest in manifests:
        options = FleetReliabilityOptions.from_manifest(manifest)
        if options.enabled:
            return options
    return (
        FleetReliabilityOptions.from_manifest(manifests[0])
        if manifests
        else FleetReliabilityOptions(False, "gate", "prod_strict", "warn", "block", 86400, ())
    )


def _is_stale(record: Mapping[str, Any], observed: datetime, options: FleetReliabilityOptions) -> bool:
    recorded = _parse_datetime(record.get("recorded_at"))
    return bool(recorded and observed - recorded > timedelta(seconds=options.stale_after_seconds))


def _observed_at(records: Sequence[Mapping[str, Any]], raw: str | None) -> datetime:
    parsed = _parse_datetime(raw)
    if parsed:
        return parsed
    candidates = [_parse_datetime(item.get("recorded_at")) for item in records]
    return max((item for item in candidates if item is not None), default=datetime(1970, 1, 1, tzinfo=UTC))


def _parse_datetime(raw: object) -> datetime | None:
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def _tower(manifest: Mapping[str, Any]) -> dict[str, Any]:
    product = _mapping(_mapping(_mapping(manifest.get("sink")).get("options")).get("data_product"))
    return _mapping(product.get("reliability_control_tower"))


def _target_table(sink: Mapping[str, Any]) -> str | None:
    table = sink.get("table")
    if isinstance(table, Mapping):
        schema = table.get("schema")
        name = table.get("name")
        return f"{schema}.{name}" if schema and name else str(name or schema or "")
    return str(table) if table else None


def _regulated_blockers(evaluation: Mapping[str, Any]) -> list[str]:
    return (
        ["data_product_fleet.owner_missing"]
        if any(not item.get("owner") for item in evaluation.get("products", []))
        else []
    )


def _action(status: str, blockers: Sequence[object]) -> str:
    if status == "frozen":
        return "Resolve release freeze blockers and rerun fleet gate"
    if blockers:
        return "Investigate blockers"
    return "Continue"


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _iso(raw: datetime) -> str:
    return raw.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _recommendations(blockers: Sequence[object], warnings: Sequence[object] = ()) -> list[str]:
    if blockers:
        return ["Freeze risky releases until fleet blockers are resolved."]
    if warnings:
        return ["Review fleet warnings before release closeout."]
    return ["Attach fleet reliability evidence to release closeout artifacts."]


__all__ = [
    "FLEET_EVALUATION_SCHEMA",
    "FLEET_GATE_SCHEMA",
    "FLEET_REPORT_SCHEMA",
    "RELIABILITY_EXPORT_SCHEMA",
    "ROUTE_DELIVERY_RECEIPT_SCHEMA",
    "FleetReliabilityEvaluator",
    "FleetReliabilityGate",
    "FleetReliabilityOptions",
    "IncidentRouteDryRunEvaluator",
    "ReliabilityExportRenderer",
]
