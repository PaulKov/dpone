"""Provider-neutral data product governance export planning."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from typing import Any

from dpone.readiness.data_product_governance_constants import SUPPORTED_GOVERNANCE_PROVIDERS
from dpone.readiness.migration_control import stable_fingerprint

GOVERNANCE_PLAN_SCHEMA = "dpone.data_product_governance_export_plan.v1"

_PASSING_STATUSES = {
    "acknowledged",
    "allowed",
    "approved",
    "assessed",
    "certified",
    "classified",
    "degraded",
    "disabled",
    "dry_run",
    "healthy",
    "migrated",
    "passed",
    "published",
    "ready",
    "rendered",
    "resolved",
    "signed",
    "stable",
    "usable",
    "verified",
    "waived",
    "warning",
}
_DEFAULT_EVIDENCE_KINDS = (
    "data_product_assertion_gate",
    "data_product_policy_gate",
    "data_product_waiver",
    "data_product_slo_gate",
    "data_product_error_budget_gate",
    "data_product_fleet_gate",
    "data_product_release_closeout_gate",
    "schema_contract_gate",
    "contract_gate",
    "consumer_gate",
)


@dataclass(frozen=True, slots=True)
class DataProductGovernanceExportOptions:
    enabled: bool
    mode: str
    profile: str
    stale_evidence_policy: str
    targets: tuple[dict[str, str], ...]

    @classmethod
    def from_product(
        cls,
        product: Mapping[str, Any],
        target_override: Sequence[str] = (),
    ) -> DataProductGovernanceExportOptions:
        raw = product.get("governance_export")
        options = raw if isinstance(raw, Mapping) else {}
        return cls(
            enabled=_bool(options.get("enabled"), False),
            mode=str(options.get("mode") or "gate"),
            profile=str(options.get("profile") or "prod_strict"),
            stale_evidence_policy=str(options.get("stale_evidence_policy") or "block"),
            targets=_targets(options.get("targets"), target_override),
        )


class GovernanceEvidenceIndex:
    """Normalizes local governance evidence for export planning."""

    def build(
        self,
        *,
        product_id: str | None,
        evidence: Mapping[str, Mapping[str, Any]],
        registry_records: Sequence[Mapping[str, Any]],
        stale_policy: str,
    ) -> dict[str, Any]:
        refs: list[dict[str, Any]] = []
        blockers: list[str] = []
        warnings: list[str] = []
        for kind, payload in sorted(evidence.items()):
            current = _evidence_ref(kind, payload)
            if not current:
                continue
            refs.append(current)
            blockers.extend(_evidence_blockers(kind, payload, product_id, stale_policy))
            warnings.extend(str(item) for item in payload.get("warnings", []) if str(item))
        return {
            "refs": refs,
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "registry_summary": _registry_summary(registry_records),
        }


class GovernanceExportPlanner:
    """Builds deterministic governance export plans from local evidence."""

    def plan(
        self,
        *,
        manifest: Mapping[str, Any],
        evidence: Mapping[str, Mapping[str, Any]],
        registry_records: Sequence[Mapping[str, Any]],
        targets: Sequence[str] = (),
        pack_id: str | None = None,
        bundle_id: str | None = None,
    ) -> dict[str, Any]:
        product = _product(manifest)
        options = DataProductGovernanceExportOptions.from_product(product, targets)
        if not options.enabled:
            return _plan_payload(
                status="disabled",
                product=product,
                options=options,
                evidence_index={"refs": [], "blockers": [], "warnings": [], "registry_summary": {}},
                export_items=(),
                pack_id=pack_id,
                bundle_id=bundle_id,
            )
        index = GovernanceEvidenceIndex().build(
            product_id=_product_id(product),
            evidence=evidence,
            registry_records=registry_records,
            stale_policy=options.stale_evidence_policy,
        )
        export_items = tuple(_export_item(target) for target in options.targets)
        blockers = [*index["blockers"], *_target_blockers(export_items)]
        warnings = list(index["warnings"])
        if options.profile == "advisory":
            warnings.extend(blockers)
            blockers = []
        status = "blocked" if blockers else "warning" if warnings else "ready"
        return _plan_payload(
            status=status,
            product=product,
            options=options,
            evidence_index={**index, "blockers": blockers, "warnings": warnings},
            export_items=export_items,
            pack_id=pack_id or _first_value(evidence, "pack_id"),
            bundle_id=bundle_id or _first_value(evidence, "bundle_id"),
        )


def _plan_payload(
    *,
    status: str,
    product: Mapping[str, Any],
    options: DataProductGovernanceExportOptions,
    evidence_index: Mapping[str, Any],
    export_items: Sequence[Mapping[str, Any]],
    pack_id: str | None,
    bundle_id: str | None,
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": GOVERNANCE_PLAN_SCHEMA,
        "status": status,
        "mode": options.mode,
        "profile": options.profile,
        "product": _product_ref(product),
        "contract": _contract_ref(product),
        "pack_id": pack_id,
        "bundle_id": bundle_id,
        "export_items": [dict(item) for item in export_items],
        "evidence_refs": list(evidence_index.get("refs", [])),
        "registry_summary": dict(evidence_index.get("registry_summary", {})),
        "blockers": list(evidence_index.get("blockers", [])),
        "warnings": list(evidence_index.get("warnings", [])),
        "recommendations": _recommendations(status),
    }
    payload["governance_export_plan_id"] = stable_fingerprint(payload)
    return payload


def _product(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    sink = manifest.get("sink")
    options = sink.get("options") if isinstance(sink, Mapping) else {}
    product = options.get("data_product") if isinstance(options, Mapping) else {}
    return product if isinstance(product, Mapping) else {}


def _product_ref(product: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in {
            "id": product.get("id"),
            "owner": product.get("owner"),
            "tier": product.get("tier"),
            "criticality": product.get("criticality"),
        }.items()
        if value is not None
    }


def _contract_ref(product: Mapping[str, Any]) -> dict[str, Any]:
    raw = product.get("schema_contract")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _product_id(product: Mapping[str, Any]) -> str | None:
    return str(product.get("id")) if product.get("id") else None


def _bool(raw: Any, default: bool) -> bool:
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _targets(raw: Any, overrides: Sequence[str]) -> tuple[dict[str, str], ...]:
    if overrides:
        return tuple({"provider": item.lower().strip(), "mode": "render"} for item in overrides if item.strip())
    if isinstance(raw, list):
        return tuple(
            {"provider": str(item.get("provider", "")).lower(), "mode": str(item.get("mode") or "render")}
            for item in raw
            if isinstance(item, Mapping) and item.get("provider")
        )
    return ({"provider": "json", "mode": "render"},)


def _export_item(target: Mapping[str, Any]) -> dict[str, Any]:
    provider = str(target.get("provider") or "").lower()
    blockers = (
        []
        if provider in SUPPORTED_GOVERNANCE_PROVIDERS
        else [f"data_product_governance.unsupported_provider:{provider}"]
    )
    return {"provider": provider, "mode": str(target.get("mode") or "render"), "blockers": blockers}


def _target_blockers(items: Sequence[Mapping[str, Any]]) -> list[str]:
    return [str(blocker) for item in items for blocker in item.get("blockers", []) if str(blocker)]


def _evidence_ref(kind: str, payload: Mapping[str, Any]) -> dict[str, Any]:
    if kind not in _DEFAULT_EVIDENCE_KINDS and not str(kind).startswith("data_product_"):
        return {}
    return {
        key: value
        for key, value in {
            "kind": kind,
            "schema_version": payload.get("schema_version"),
            "status": payload.get("status"),
            "evidence_id": _evidence_id(payload),
            "product_id": _payload_product_id(payload),
        }.items()
        if value is not None
    }


def _evidence_blockers(
    kind: str,
    payload: Mapping[str, Any],
    product_id: str | None,
    stale_policy: str,
) -> list[str]:
    blockers: list[str] = []
    payload_product = _payload_product_id(payload)
    if product_id and payload_product and payload_product != product_id:
        blockers.append(f"data_product_governance.product_id_mismatch:{kind}")
    if payload.get("status") not in _PASSING_STATUSES:
        blockers.append(f"data_product_governance.evidence_status_blocked:{kind}")
    blockers.extend(str(item) for item in payload.get("blockers", []) if str(item))
    if stale_policy == "block" and _is_stale(payload):
        blockers.append(f"data_product_governance.evidence_stale:{kind}")
    return blockers


def _payload_product_id(payload: Mapping[str, Any]) -> str | None:
    product = payload.get("product")
    if isinstance(product, Mapping) and product.get("id"):
        return str(product.get("id"))
    return str(payload.get("product_id")) if payload.get("product_id") else None


def _evidence_id(payload: Mapping[str, Any]) -> str | None:
    preferred = (
        "policy_gate_id",
        "assertion_gate_id",
        "slo_gate_id",
        "error_budget_gate_id",
        "fleet_gate_id",
        "release_closeout_gate_id",
        "waiver_id",
        "contract_gate_id",
        "consumer_gate_id",
    )
    for key in preferred:
        if payload.get(key):
            return str(payload[key])
    for key, value in payload.items():
        if key.endswith("_id") and str(value).startswith("sha256:"):
            return str(value)
    return None


def _is_stale(payload: Mapping[str, Any]) -> bool:
    text = " ".join(
        str(item) for item in [payload.get("status"), *payload.get("blockers", []), *payload.get("warnings", [])]
    )
    return "stale" in text.lower()


def _registry_summary(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    stages = sorted({str(record.get("stage")) for record in records if record.get("stage")})
    return {"record_count": len(records), "stages": stages}


def _first_value(evidence: Mapping[str, Mapping[str, Any]], key: str) -> str | None:
    for payload in evidence.values():
        if payload.get(key):
            return str(payload[key])
    return None


def _recommendations(status: str) -> list[str]:
    if status == "blocked":
        return ["Fix stale, mismatched or blocked governance evidence before exporting to catalogs."]
    if status == "disabled":
        return ["Enable sink.options.data_product.governance_export to publish governance evidence."]
    return ["Render provider payloads and record publish verification in the evidence registry."]


def __getattr__(name: str) -> Any:
    if name == "GovernancePayloadRenderer":
        return import_module("dpone.readiness.data_product_governance_rendering").GovernancePayloadRenderer
    if name in {"GovernancePublisher", "GovernancePublishVerifier"}:
        return getattr(import_module("dpone.readiness.data_product_governance_publish"), name)
    raise AttributeError(name)


__all__ = [
    "GOVERNANCE_PLAN_SCHEMA",
    "DataProductGovernanceExportOptions",
    "GovernanceEvidenceIndex",
    "GovernanceExportPlanner",
]
