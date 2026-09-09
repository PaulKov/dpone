"""Provider-neutral data product error budget planning, evaluation and gates."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from dpone.readiness.migration_control import stable_fingerprint

ERROR_BUDGET_PLAN_SCHEMA = "dpone.data_product_error_budget_plan.v1"
ERROR_BUDGET_EVALUATION_SCHEMA = "dpone.data_product_error_budget_evaluation.v1"
ERROR_BUDGET_GATE_SCHEMA = "dpone.data_product_error_budget_gate.v1"


@dataclass(frozen=True, slots=True)
class ErrorBudgetWindow:
    name: str
    duration_seconds: int
    max_burn_rate: float | None = None
    min_budget_remaining: float | None = None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> ErrorBudgetWindow:
        return cls(
            name=str(payload.get("name") or "window"),
            duration_seconds=int(
                payload.get("duration_seconds") or _duration_seconds(payload.get("duration"), default=3600)
            ),
            max_burn_rate=_number(payload.get("max_burn_rate")),
            min_budget_remaining=_number(payload.get("min_budget_remaining")),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in {
                "name": self.name,
                "duration_seconds": self.duration_seconds,
                "max_burn_rate": self.max_burn_rate,
                "min_budget_remaining": self.min_budget_remaining,
            }.items()
            if value is not None
        }


@dataclass(frozen=True, slots=True)
class DataProductErrorBudgetOptions:
    enabled: bool
    product_id: str
    owner: str | None
    mode: str
    profile: str
    objective_window_seconds: int
    windows: tuple[ErrorBudgetWindow, ...]
    release_policy: dict[str, Any]

    @classmethod
    def from_manifest(cls, manifest: Mapping[str, Any]) -> DataProductErrorBudgetOptions:
        sink = _mapping(manifest.get("sink"))
        options = _mapping(sink.get("options"))
        contract = _mapping(options.get("schema_contract"))
        product = _mapping(options.get("data_product"))
        slo = _mapping(product.get("slo"))
        budget = _mapping(slo.get("error_budget"))
        windows = tuple(ErrorBudgetWindow.from_payload(item) for item in _list_of_mappings(budget.get("windows")))
        product_id = str(product.get("id") or contract.get("id") or _target_table(sink) or "")
        return cls(
            enabled=bool(budget.get("enabled", False)),
            product_id=product_id,
            owner=_optional_string(product.get("owner")),
            mode=str(budget.get("mode") or slo.get("mode") or "gate"),
            profile=str(budget.get("profile") or slo.get("profile") or "prod_strict"),
            objective_window_seconds=_duration_seconds(budget.get("objective_window"), default=30 * 24 * 3600),
            windows=windows,
            release_policy=dict(_mapping(budget.get("release_policy"))),
        )

    def product_dict(self) -> dict[str, Any]:
        return {"id": self.product_id, "owner": self.owner}


class DataProductErrorBudgetPlanner:
    """Builds deterministic error-budget plans from manifest and SLO evidence."""

    def plan(
        self,
        *,
        manifest: Mapping[str, Any],
        slo_evaluation: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        options = DataProductErrorBudgetOptions.from_manifest(manifest)
        blockers = _plan_blockers(options, slo_evaluation)
        status = "disabled" if not options.enabled else "blocked" if blockers else "ready"
        payload: dict[str, Any] = {
            "schema_version": ERROR_BUDGET_PLAN_SCHEMA,
            "status": status,
            "mode": options.mode,
            "profile": options.profile,
            "product": options.product_dict(),
            "objective_window_seconds": options.objective_window_seconds,
            "windows": [item.to_dict() for item in options.windows] if options.enabled else [],
            "release_policy": options.release_policy if options.enabled else {},
            "slo_evaluation_id": _optional(slo_evaluation, "slo_evaluation_id"),
            "slo_evaluation_status": _optional(slo_evaluation, "status"),
            "blockers": blockers,
            "warnings": [],
            "recommendations": _recommendations(blockers),
        }
        payload["error_budget_plan_id"] = stable_fingerprint(payload)
        return payload


class DataProductErrorBudgetEvaluator:
    """Evaluates SLO history against an error-budget plan."""

    def evaluate(
        self,
        *,
        plan: Mapping[str, Any],
        history: Sequence[Mapping[str, Any]] = (),
        observed_at: str | None = None,
    ) -> dict[str, Any]:
        if plan.get("status") == "disabled":
            return _disabled_evaluation(plan)
        blockers = [str(item) for item in plan.get("blockers", []) if str(item)]
        warnings = [str(item) for item in plan.get("warnings", []) if str(item)]
        observed = _observed_at(history, observed_at)
        objective_seconds = int(plan.get("objective_window_seconds") or 30 * 24 * 3600)
        windows = [
            _evaluate_window(
                window=ErrorBudgetWindow.from_payload(item),
                history=history,
                observed=observed,
                objective_seconds=objective_seconds,
                warnings=warnings,
            )
            for item in _list_of_mappings(plan.get("windows"))
        ]
        for window in windows:
            blockers.extend(str(item) for item in window["blockers"])
        blockers = list(dict.fromkeys(blockers))
        warnings = list(dict.fromkeys(warnings))
        payload: dict[str, Any] = {
            "schema_version": ERROR_BUDGET_EVALUATION_SCHEMA,
            "status": "blocked" if blockers else "warning" if warnings else "passed",
            "error_budget_plan_id": plan.get("error_budget_plan_id"),
            "product_id": _product_id(plan),
            "observed_at": _iso(observed),
            "windows": windows,
            "blockers": blockers,
            "warnings": warnings,
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["error_budget_evaluation_id"] = stable_fingerprint(payload)
        return payload


class DataProductErrorBudgetGate:
    """Profile-aware go/no-go gate for error-budget evaluations."""

    def evaluate(self, *, evaluation: Mapping[str, Any], profile: str = "prod_strict") -> dict[str, Any]:
        blockers = [str(item) for item in evaluation.get("blockers", []) if str(item)]
        warnings = [str(item) for item in evaluation.get("warnings", []) if str(item)]
        if profile == "advisory":
            warnings.extend(blockers)
            blockers = []
        payload: dict[str, Any] = {
            "schema_version": ERROR_BUDGET_GATE_SCHEMA,
            "status": "blocked" if blockers else "warning" if warnings else "allowed",
            "profile": profile,
            "product_id": evaluation.get("product_id"),
            "error_budget_evaluation_id": evaluation.get("error_budget_evaluation_id"),
            "pack_id": evaluation.get("pack_id"),
            "bundle_id": evaluation.get("bundle_id"),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "recommendations": _recommendations(blockers, warnings),
        }
        payload["error_budget_gate_id"] = stable_fingerprint(payload)
        return payload


def _evaluate_window(
    *,
    window: ErrorBudgetWindow,
    history: Sequence[Mapping[str, Any]],
    observed: datetime,
    objective_seconds: int,
    warnings: list[str],
) -> dict[str, Any]:
    samples = _window_samples(history, observed=observed, duration_seconds=window.duration_seconds)
    if not samples:
        warnings.append(f"data_product_error_budget.window_missing_samples:{window.name}")
    failed = sum(1 for item in samples if _failed(item))
    total = len(samples)
    consumed = failed / total if total else 0.0
    elapsed_ratio = max(window.duration_seconds / max(objective_seconds, 1), 0.000001)
    burn_rate = round(consumed / elapsed_ratio, 6)
    remaining = round(max(0.0, 1.0 - consumed), 6)
    blockers: list[str] = []
    if window.max_burn_rate is not None and burn_rate > window.max_burn_rate:
        blockers.append(f"data_product_error_budget.fast_burn_exceeded:{window.name}")
    if window.min_budget_remaining is not None and remaining < window.min_budget_remaining:
        blockers.append(f"data_product_error_budget.budget_remaining_exhausted:{window.name}")
    return {
        "name": window.name,
        "status": "blocked" if blockers else "passed",
        "duration_seconds": window.duration_seconds,
        "samples": {"total": total, "failed": failed, "passed": total - failed},
        "burn_rate": burn_rate,
        "budget_remaining": remaining,
        "blockers": blockers,
        "warnings": [],
    }


def _window_samples(
    history: Sequence[Mapping[str, Any]], *, observed: datetime, duration_seconds: int
) -> tuple[Mapping[str, Any], ...]:
    start = observed - timedelta(seconds=duration_seconds)
    selected: list[Mapping[str, Any]] = []
    for item in history:
        recorded = _parse_datetime(item.get("recorded_at"))
        if recorded is None or start <= recorded <= observed:
            selected.append(item)
    return tuple(selected)


def _plan_blockers(options: DataProductErrorBudgetOptions, slo_evaluation: Mapping[str, Any] | None) -> list[str]:
    if not options.enabled:
        return []
    blockers: list[str] = []
    if not options.windows:
        blockers.append("data_product_error_budget.windows_missing")
    if slo_evaluation and slo_evaluation.get("product_id") not in {None, options.product_id}:
        blockers.append("data_product_error_budget.slo_evaluation_product_mismatch")
    return blockers if options.mode == "gate" else []


def _disabled_evaluation(plan: Mapping[str, Any]) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "schema_version": ERROR_BUDGET_EVALUATION_SCHEMA,
        "status": "disabled",
        "error_budget_plan_id": plan.get("error_budget_plan_id"),
        "product_id": _product_id(plan),
        "windows": [],
        "blockers": [],
        "warnings": [],
        "recommendations": ["Enable data_product.slo.error_budget to evaluate error budget."],
    }
    payload["error_budget_evaluation_id"] = stable_fingerprint(payload)
    return payload


def _failed(item: Mapping[str, Any]) -> bool:
    return bool(item.get("blockers")) or str(item.get("status")) in {"blocked", "failed"}


def _observed_at(history: Sequence[Mapping[str, Any]], raw: str | None) -> datetime:
    parsed = _parse_datetime(raw)
    if parsed is not None:
        return parsed
    candidates = [_parse_datetime(item.get("recorded_at")) for item in history]
    return max((item for item in candidates if item is not None), default=datetime(1970, 1, 1, tzinfo=UTC))


def _parse_datetime(raw: object) -> datetime | None:
    if not raw:
        return None
    try:
        text = str(raw).replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    except ValueError:
        return None


def _duration_seconds(raw: object, *, default: int) -> int:
    if raw in {None, ""}:
        return default
    text = str(raw).strip().lower()
    if text.isdigit():
        return int(text)
    unit = text[-1:]
    value = _number(text[:-1])
    multipliers = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    return int(value * multipliers[unit]) if value is not None and unit in multipliers else default


def _list_of_mappings(raw: object) -> tuple[dict[str, Any], ...]:
    return tuple(dict(item) for item in raw if isinstance(item, Mapping)) if isinstance(raw, list) else ()


def _target_table(sink: Mapping[str, Any]) -> str | None:
    table = sink.get("table")
    if isinstance(table, Mapping):
        schema = table.get("schema")
        name = table.get("name")
        return f"{schema}.{name}" if schema and name else str(name or schema or "")
    return str(table) if table else None


def _product_id(plan: Mapping[str, Any]) -> str | None:
    product = plan.get("product")
    return str(product.get("id")) if isinstance(product, Mapping) and product.get("id") else None


def _mapping(raw: object) -> dict[str, Any]:
    return dict(raw) if isinstance(raw, Mapping) else {}


def _optional(payload: Mapping[str, Any] | None, key: str) -> Any:
    return payload.get(key) if isinstance(payload, Mapping) else None


def _optional_string(raw: object) -> str | None:
    return str(raw) if raw not in {None, ""} else None


def _number(raw: object) -> float | None:
    if raw in {None, ""}:
        return None
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _iso(raw: datetime) -> str:
    return raw.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _recommendations(blockers: Sequence[object], warnings: Sequence[object] = ()) -> list[str]:
    if blockers:
        return ["Pause risky releases until error-budget blockers are resolved."]
    if warnings:
        return ["Review error-budget warnings before release closeout."]
    return ["Attach this error-budget receipt to release closeout evidence."]


__all__ = [
    "ERROR_BUDGET_EVALUATION_SCHEMA",
    "ERROR_BUDGET_GATE_SCHEMA",
    "ERROR_BUDGET_PLAN_SCHEMA",
    "DataProductErrorBudgetEvaluator",
    "DataProductErrorBudgetGate",
    "DataProductErrorBudgetOptions",
    "DataProductErrorBudgetPlanner",
    "ErrorBudgetWindow",
]
