"""Shared contracts for remediation execution evidence."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from dpone.readiness import data_product_compliance_support as base
from dpone.readiness import data_product_remediation_support as remediation
from dpone.readiness.data_product_cost_policy import apply_profile, status
from dpone.readiness.migration_control import stable_fingerprint

PLAN_SCHEMA = "dpone.data_product_remediation_execution_plan.v1"
RUN_SCHEMA = "dpone.data_product_remediation_execution_run.v1"
CERTIFICATE_SCHEMA = "dpone.data_product_remediation_execution_certificate.v1"
REPORT_SCHEMA = "dpone.data_product_remediation_execution_report.v1"


@dataclass(frozen=True, slots=True)
class RemediationExecutionOptions:
    enabled: bool
    mode: str
    profile: str
    require_remediation_gate: bool
    require_authority_gate: bool
    require_lock: bool
    require_idempotency_key: bool
    unresolved_command_policy: str
    dry_run_status: str
    command_timeout_seconds: int
    allowed_command_prefixes: tuple[tuple[str, ...], ...]
    product: Mapping[str, Any]

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> RemediationExecutionOptions:
        product = base.product(manifest)
        remediation_raw = _mapping(product.get("remediation"))
        raw = _mapping(remediation_raw.get("execution"))
        return cls(
            enabled=base.bool_value(raw.get("enabled"), False),
            mode=str(raw.get("mode") or "gate"),
            profile=str(raw.get("profile") or "prod_strict"),
            require_remediation_gate=base.bool_value(raw.get("require_remediation_gate"), True),
            require_authority_gate=base.bool_value(raw.get("require_authority_gate"), False),
            require_lock=base.bool_value(raw.get("require_lock"), True),
            require_idempotency_key=base.bool_value(raw.get("require_idempotency_key"), True),
            unresolved_command_policy=str(raw.get("unresolved_command_policy") or "block"),
            dry_run_status=str(raw.get("dry_run_status") or "warning"),
            command_timeout_seconds=_positive_int(raw.get("command_timeout_seconds"), 300),
            allowed_command_prefixes=_prefixes(raw.get("allowed_command_prefixes")),
            product=base.product_ref(product),
        )


def evidence_ref(payload: Mapping[str, Any], fallback_product: Mapping[str, Any]) -> dict[str, Any]:
    return remediation.evidence_ref(payload, fallback_product)


def payload_id(payload: Mapping[str, Any], key: str) -> dict[str, Any]:
    result = dict(payload)
    result[key] = stable_fingerprint(result)
    return result


def dedupe(values: Sequence[str]) -> list[str]:
    return list(dict.fromkeys(str(value) for value in values if str(value)))


def mappings(raw: Any) -> tuple[Mapping[str, Any], ...]:
    return base.mappings(raw)


def strings(raw: Any) -> tuple[str, ...]:
    return base.strings(raw)


def _prefixes(raw: Any) -> tuple[tuple[str, ...], ...]:
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        return (("dpone",),)
    prefixes: list[tuple[str, ...]] = []
    for item in raw:
        if isinstance(item, str):
            prefixes.append(tuple(part for part in item.split() if part))
        elif isinstance(item, Sequence):
            prefixes.append(tuple(str(part) for part in item if str(part)))
    return tuple(prefix for prefix in prefixes if prefix) or (("dpone",),)


def _positive_int(raw: Any, default: int) -> int:
    try:
        value = int(raw)
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def _mapping(raw: Any) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


__all__ = [
    "CERTIFICATE_SCHEMA",
    "PLAN_SCHEMA",
    "REPORT_SCHEMA",
    "RUN_SCHEMA",
    "RemediationExecutionOptions",
    "apply_profile",
    "dedupe",
    "evidence_ref",
    "mappings",
    "payload_id",
    "status",
    "strings",
]
