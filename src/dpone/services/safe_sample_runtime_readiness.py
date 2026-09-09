"""Readiness report for the safe sample runtime path."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Iterable

    from dpone.services.safe_sample_data_copier_registry import SafeSampleDataCopierRegistry
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan
    from dpone.services.safe_sample_policy import TemporaryTargetPlan

_BASE_CONTRACTS = (
    "binding_resolver",
    "cache_materializer",
    "connection_registry",
    "credential_runtime",
    "init_fetch_plan",
    "safe_sample_execution_plan",
    "safe_sample_source_request",
    "safe_sample_runtime_runner",
    "safe_sample_runtime_evidence_writer",
    "certified_data_copier_registry",
    "fail_closed_data_copier",
    "mssql_clickhouse_bounded_copy_executor",
    "credential_resolving_mssql_clickhouse_copy_executor",
)

_TARGET_CONTRACTS = (
    "temporary_target_plan",
    "temporary_target_lifecycle_executor",
    "temporary_target_adapter_registry",
)

_BLOCKER_BY_ERROR_CODE = {
    "DPONE_DEPLOYMENT_CURRENT_NOT_FOUND": "runnable_deployment_set",
    "DPONE_DEPLOYMENT_ENVIRONMENT_MISMATCH": "deployment_environment",
    "DPONE_DEPLOYMENT_NOT_RUNNABLE": "runnable_deployment_set",
    "DPONE_SAFE_SAMPLE_EXECUTABLE_INDEX_UNSUPPORTED": "airflow_provider_v2_path_required",
    "DPONE_RUNTIME_TEMPORARY_TARGET_PLAN_MISSING": "temporary_target_plan",
    "DPONE_RUNTIME_SAMPLE_SIZE_INVALID": "safe_sample_policy",
    "DPONE_SECURITY_SAMPLE_TARGET_UNSAFE": "safe_sample_policy",
    "DPONE_SECURITY_SAMPLE_PUSHDOWN_REQUIRED": "pushdown_sampling_capability",
    "DPONE_SECURITY_SAMPLE_FULL_SCAN_FORBIDDEN": "safe_sample_policy",
    "DPONE_SECURITY_SAMPLE_BUDGET_REQUIRED": "safe_sample_policy",
    "DPONE_SECURITY_SAMPLE_BUDGET_EXCEEDED": "safe_sample_policy",
}


@dataclass(frozen=True, slots=True)
class SafeSampleRuntimeReadinessReport:
    """Machine-readable answer to "can this safe sample plan run now?"."""

    ready: bool
    available_contracts: tuple[str, ...]
    blockers: tuple[str, ...]
    errors: tuple[dict[str, Any], ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.safe-sample-runtime-readiness.v1",
            "ready": self.ready,
            "available_contracts": list(self.available_contracts),
            "blockers": list(self.blockers),
            "errors": [dict(error) for error in self.errors],
        }


class SafeSampleRuntimeReadinessEvaluator:
    """Evaluate runtime readiness without touching data systems, secrets or Airflow state."""

    def evaluate(
        self,
        plan: SafeSampleExecutionPlan,
        *,
        temporary_target_plan: TemporaryTargetPlan | None = None,
        certified_data_copier_available: bool = False,
        data_copier_registry: SafeSampleDataCopierRegistry | None = None,
    ) -> SafeSampleRuntimeReadinessReport:
        has_certified_copier = _has_certified_data_copier(
            plan,
            certified_data_copier_available=certified_data_copier_available,
            data_copier_registry=data_copier_registry,
        )
        errors = [dict(error) for error in plan.blockers]
        blocker_candidates: list[str] = []
        for error in errors:
            blocker = _blocker_for(error)
            if blocker:
                blocker_candidates.append(blocker)
        if not has_certified_copier:
            blocker_candidates.append("certified_source_data_copier")
            errors.append(
                _error(
                    "DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED",
                    "A certified source sampling/data-copy implementation is not available yet.",
                )
            )
        blockers = _stable_unique(blocker_candidates)
        available_contracts = _available_contracts(
            temporary_target_plan or plan.temporary_target_plan,
            certified_data_copier_available=has_certified_copier,
        )
        return SafeSampleRuntimeReadinessReport(
            ready=not blockers and not errors,
            available_contracts=available_contracts,
            blockers=blockers,
            errors=tuple(errors),
        )


def _has_certified_data_copier(
    plan: SafeSampleExecutionPlan,
    *,
    certified_data_copier_available: bool,
    data_copier_registry: SafeSampleDataCopierRegistry | None,
) -> bool:
    if data_copier_registry is not None:
        return data_copier_registry.has_copier_for(plan)
    return certified_data_copier_available


def _available_contracts(
    temporary_target_plan: TemporaryTargetPlan | None,
    *,
    certified_data_copier_available: bool,
) -> tuple[str, ...]:
    contracts = [*_BASE_CONTRACTS]
    if certified_data_copier_available:
        contracts.append("certified_source_data_copier")
    if temporary_target_plan is not None:
        contracts.extend(_TARGET_CONTRACTS)
        if temporary_target_plan.sink_type == "clickhouse":
            contracts.append("clickhouse_temporary_target_adapter")
    return tuple(contracts)


def _blocker_for(error: dict[str, Any]) -> str | None:
    return _BLOCKER_BY_ERROR_CODE.get(str(error.get("code") or ""))


def _stable_unique(values: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    unique: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        unique.append(value)
    return tuple(unique)


def _error(code: str, message: str) -> dict[str, Any]:
    return {
        "schema": "dpone.error.v1",
        "code": code,
        "stage": "safe_sample_runtime_readiness",
        "severity": "error",
        "message": message,
        "fixes": [],
    }


__all__ = [
    "SafeSampleRuntimeReadinessEvaluator",
    "SafeSampleRuntimeReadinessReport",
]
