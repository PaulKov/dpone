"""Support helpers for provider-neutral data product access governance."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.readiness import data_product_compliance_support as support
from dpone.readiness.migration_control import stable_fingerprint

_SENSITIVE_CLASSES = {"pii", "financial", "confidential", "regulated"}
_CRITICAL_CLASSES = {"pii", "regulated"}


def classification_payload(
    *,
    schema: str,
    product: Mapping[str, Any],
    options: Any,
    columns: Sequence[Mapping[str, Any]],
    status: str,
    blockers: Sequence[str] = (),
    warnings: Sequence[str] = (),
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": schema,
        "status": status,
        "mode": options.mode,
        "profile": options.profile,
        "product": support.product_ref(product),
        "product_id": product.get("id"),
        "default_class": options.default_class,
        "columns": [dict(item) for item in columns],
        "summary": {
            "columns_count": len(columns),
            "sensitive_columns": sum(1 for item in columns if item.get("sensitive")),
        },
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }
    payload["access_classification_id"] = stable_fingerprint(payload)
    return payload


def entitlement_payload(
    *,
    schema: str,
    product: Mapping[str, Any],
    classification: Mapping[str, Any],
    consumer_matrix: Mapping[str, Any] | None,
    decisions: Sequence[Mapping[str, Any]],
    status: str,
    blockers: Sequence[str],
    warnings: Sequence[str] = (),
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": schema,
        "status": status,
        "product": support.product_ref(product) or dict(classification.get("product", {})),
        "product_id": product.get("id") or classification.get("product_id"),
        "access_classification_id": classification.get("access_classification_id"),
        "consumer_matrix_id": (consumer_matrix or {}).get("consumer_matrix_id"),
        "decisions": [dict(item) for item in decisions],
        "summary": {
            "decisions_count": len(decisions),
            "sensitive_decisions": sum(1 for item in decisions if item.get("sensitive")),
            "blocked_decisions": sum(1 for item in decisions if item.get("blockers")),
        },
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }
    payload["entitlement_plan_id"] = stable_fingerprint(payload)
    return payload


def privacy_payload(
    *,
    schema: str,
    product: Mapping[str, Any],
    entitlement_plan: Mapping[str, Any],
    authority_gate: Mapping[str, Any] | None,
    sensitive: Sequence[Mapping[str, Any]],
    status: str,
    blockers: Sequence[str],
    warnings: Sequence[str],
) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": schema,
        "status": status,
        "product": support.product_ref(product) or dict(entitlement_plan.get("product", {})),
        "product_id": product.get("id") or entitlement_plan.get("product_id"),
        "entitlement_plan_id": entitlement_plan.get("entitlement_plan_id"),
        "authority_gate_ref": authority_ref(authority_gate),
        "sensitive_access": [dict(item) for item in sensitive],
        "summary": {"sensitive_access_count": len(sensitive), "blockers_count": len(blockers)},
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }
    payload["privacy_impact_id"] = stable_fingerprint(payload)
    return payload


def column_names(*, manifest: Mapping[str, Any], schema_contract: Mapping[str, Any], options: Any) -> set[str]:
    names = set(columns_from_schema(schema_contract))
    names.update(columns_from_schema(_mapping(schema_contract_from_manifest(manifest))))
    names.update(_mapping(options.classification.get("columns")).keys())
    names.update(column for item in options.entitlements for column in support.strings(item.get("columns")))
    return {str(item) for item in names if str(item)}


def columns_from_schema(raw: Mapping[str, Any]) -> tuple[str, ...]:
    columns = raw.get("columns")
    if isinstance(columns, Mapping):
        return tuple(str(name) for name in columns if str(name))
    if isinstance(columns, Sequence) and not isinstance(columns, (str, bytes)):
        return tuple(str(item.get("name")) for item in columns if isinstance(item, Mapping) and item.get("name"))
    return ()


def column_classification(name: str, options: Any) -> dict[str, Any]:
    raw = _mapping(_mapping(options.classification.get("columns")).get(name))
    klass = str(raw.get("class") or options.default_class)
    masking = str(raw.get("masking") or "none")
    return {
        "name": name,
        "class": klass,
        "glossary_terms": list(support.strings(raw.get("glossary_terms"))),
        "masking": masking,
        "lawful_basis_required": support.bool_value(raw.get("lawful_basis_required"), False),
        "sensitive": klass in _SENSITIVE_CLASSES,
    }


def decision(
    consumer: Mapping[str, Any],
    column: str,
    class_map: Mapping[str, Mapping[str, Any]],
    entitlements: Sequence[Mapping[str, Any]],
    options: Any,
) -> dict[str, Any]:
    classification = class_map.get(column, default_column(column, options.default_class))
    entitlement = matching_entitlement(consumer, column, entitlements)
    blockers, warnings = _coverage_findings(consumer, column, classification, entitlement, options)
    if entitlement or options.unknown_consumer != "allow":
        blockers.extend(masking_blockers(consumer, column, classification, entitlement))
    return {
        "subject": str(consumer.get("id") or (entitlement or {}).get("subject") or "unknown"),
        "owner": consumer.get("owner") or (entitlement or {}).get("owner"),
        "column": column,
        "class": classification.get("class"),
        "sensitive": bool(classification.get("sensitive")),
        "severity": severity(str(classification.get("class") or "")),
        "actions": list(support.strings((entitlement or {}).get("actions") or ("read",))),
        "purpose": (entitlement or {}).get("purpose"),
        "lawful_basis": (entitlement or {}).get("lawful_basis"),
        "lawful_basis_required": bool(classification.get("lawful_basis_required")),
        "masking": (entitlement or {}).get("masking"),
        "masking_required": masking_required(classification, entitlement),
        "entitlement_present": entitlement is not None,
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }


def consumer_reads(
    consumer_matrix: Mapping[str, Any] | None,
    entitlements: Sequence[Mapping[str, Any]],
) -> tuple[tuple[Mapping[str, Any], str], ...]:
    reads: list[tuple[Mapping[str, Any], str]] = []
    if consumer_matrix:
        for consumer in support.mappings(consumer_matrix.get("consumers")):
            for column in support.strings(_mapping(consumer.get("reads")).get("columns")):
                reads.append((consumer, column))
    if not reads:
        for entitlement in entitlements:
            consumer = {"id": entitlement.get("subject"), "owner": entitlement.get("owner")}
            reads.extend((consumer, column) for column in support.strings(entitlement.get("columns")))
    return tuple(sorted(reads, key=lambda item: (str(item[0].get("id")), item[1])))


def normalize_entitlement(raw: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **dict(raw),
        "columns": list(support.strings(raw.get("columns"))),
        "actions": list(support.strings(raw.get("actions"))),
    }


def matching_entitlement(
    consumer: Mapping[str, Any],
    column: str,
    entitlements: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any] | None:
    consumer_id = str(consumer.get("id") or "")
    owner = str(consumer.get("owner") or "")
    for entitlement in entitlements:
        subjects = {str(entitlement.get("subject") or ""), str(entitlement.get("owner") or "")}
        if column in support.strings(entitlement.get("columns")) and ({consumer_id, owner} & subjects):
            return entitlement
    return None


def classification_map(classification: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    return {str(item.get("name")): dict(item) for item in support.mappings(classification.get("columns"))}


def purpose_blockers(decision: Mapping[str, Any]) -> list[str]:
    return (
        [f"data_product_privacy.purpose_missing:{decision.get('subject')}:{decision.get('column')}"]
        if not decision.get("purpose")
        else []
    )


def lawful_basis_blockers(decision: Mapping[str, Any], options: Any) -> list[str]:
    required = set(support.strings(options.privacy.get("require_lawful_basis_for")))
    if (decision.get("class") in required or decision.get("lawful_basis_required")) and not decision.get(
        "lawful_basis"
    ):
        return [f"data_product_privacy.lawful_basis_missing:{decision.get('subject')}:{decision.get('column')}"]
    return []


def approval_blockers(
    decision: Mapping[str, Any],
    options: Any,
    authority_gate: Mapping[str, Any] | None,
) -> list[str]:
    gate_ok = bool(authority_gate and authority_gate.get("status") in {"allowed", "warning"})
    blockers: list[str] = []
    required = set(support.strings(options.privacy.get("require_authority_gate_for")))
    if decision.get("class") in required and not gate_ok:
        blockers.append(f"data_product_privacy.authority_gate_required:{decision.get('column')}")
    export_needs_approval = (
        support.bool_value(options.privacy.get("block_sensitive_export_without_approval"), False)
        and bool(decision.get("sensitive"))
        and "export" in support.strings(decision.get("actions"))
    )
    if export_needs_approval and not gate_ok:
        blockers.append(f"data_product_privacy.sensitive_export_approval_required:{decision.get('column')}")
    return blockers


def profile_blockers(blockers: Sequence[str], profile: str) -> list[str]:
    if profile == "stage":
        return [item for item in blockers if "unknown_sensitive_consumer" in item or "masking_required" in item]
    return list(blockers)


def authority_ref(payload: Mapping[str, Any] | None) -> dict[str, Any]:
    if not payload:
        return {}
    return {"status": payload.get("status"), "authority_gate_id": payload.get("authority_gate_id")}


def product_from_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    return dict(payload.get("product", {})) if isinstance(payload.get("product"), Mapping) else {}


def gate_summary(entitlement_plan: Mapping[str, Any], privacy_impact: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "entitlement": dict(entitlement_plan.get("summary", {})),
        "privacy": dict(privacy_impact.get("summary", {})),
    }


def recommendations(status: str) -> list[str]:
    if status == "blocked":
        return ["Add entitlement, masking, lawful-basis or authority evidence before regulated closeout."]
    if status == "warning":
        return ["Review access governance warnings before release promotion."]
    return ["Record access gate and privacy impact artifacts in the evidence registry."]


def schema_contract_from_manifest(manifest: Mapping[str, Any]) -> Mapping[str, Any]:
    sink = manifest.get("sink")
    options = sink.get("options") if isinstance(sink, Mapping) else {}
    return _mapping(options.get("schema_contract")) if isinstance(options, Mapping) else {}


def default_column(column: str, klass: str) -> dict[str, Any]:
    return {
        "name": column,
        "class": klass,
        "masking": "none",
        "lawful_basis_required": False,
        "sensitive": klass in _SENSITIVE_CLASSES,
    }


def masking_required(classification: Mapping[str, Any], entitlement: Mapping[str, Any] | None) -> bool:
    return str(classification.get("masking") or "none") != "none" or support.bool_value(
        (entitlement or {}).get("masking_required"), False
    )


def masking_blockers(
    consumer: Mapping[str, Any],
    column: str,
    classification: Mapping[str, Any],
    entitlement: Mapping[str, Any] | None,
) -> list[str]:
    if masking_required(classification, entitlement) and str((entitlement or {}).get("masking") or "none") == "none":
        return [f"data_product_access.masking_required:{consumer.get('id')}:{column}"]
    return []


def severity(klass: str) -> str:
    if klass in _CRITICAL_CLASSES:
        return "critical"
    if klass in {"financial", "confidential"}:
        return "high"
    return "low"


def _coverage_findings(
    consumer: Mapping[str, Any],
    column: str,
    classification: Mapping[str, Any],
    entitlement: Mapping[str, Any] | None,
    options: Any,
) -> tuple[list[str], list[str]]:
    blockers: list[str] = []
    warnings: list[str] = []
    if classification.get("sensitive") and not entitlement and options.unknown_consumer != "allow":
        code = f"data_product_access.unknown_sensitive_consumer:{consumer.get('id')}:{column}"
        (blockers if options.unknown_consumer == "block" else warnings).append(code)
        signal = f"data_product_access.sensitive_access_uncovered:{consumer.get('id')}:{column}"
        (blockers if options.mode == "gate" else warnings).append(signal)
    return blockers, warnings


def _mapping(raw: Any) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


__all__ = [
    "approval_blockers",
    "classification_map",
    "classification_payload",
    "column_classification",
    "column_names",
    "consumer_reads",
    "decision",
    "entitlement_payload",
    "gate_summary",
    "lawful_basis_blockers",
    "normalize_entitlement",
    "privacy_payload",
    "product_from_payload",
    "profile_blockers",
    "purpose_blockers",
    "recommendations",
]
