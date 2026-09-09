"""Decision service for columnar object-storage fast path."""

from __future__ import annotations

from typing import Any

from dpone.runtime.columnar_fast_path_models import ColumnarFastPathDecision, preflight_details

ALLOWED_MODES = {"auto", "off", "required", "benchmark_only"}
ALLOWED_PROVIDERS = {"auto", "object_storage_pull", "direct_push_columnar"}


class ColumnarFastPathPlanner:
    """Select object-storage pull only when the route is certifiably ready."""

    def decide(
        self,
        *,
        columnar_options: dict[str, Any] | None,
        preflight: Any | None,
        schema_supported: bool,
        parquet_writer_available: bool,
        clickhouse_pull_available: bool,
        current_provider: str = "streaming_fallback",
        expected_speedup_pct: float | None = None,
        route_decision: Any | None = None,
    ) -> ColumnarFastPathDecision:
        options = dict(columnar_options or {})
        mode = _option(options, "mode", "auto")
        provider = _option(options, "provider", "auto")
        _validate(mode, provider)

        if mode == "off":
            return _decision(
                mode=mode,
                provider=provider,
                selected=current_provider,
                current=current_provider,
                details={"reason": "columnar_fast_path_disabled"},
            )

        blockers = _eligibility_blockers(
            preflight=preflight,
            schema_supported=schema_supported,
            parquet_writer_available=parquet_writer_available,
            clickhouse_pull_available=clickhouse_pull_available,
        )
        warnings = tuple(preflight.warnings if preflight is not None else ())
        details = {
            **preflight_details(preflight),
            "schema_supported": schema_supported,
            "parquet_writer_available": parquet_writer_available,
            "clickhouse_pull_available": clickhouse_pull_available,
            "expected_speedup_pct": expected_speedup_pct,
        }
        if route_decision is not None:
            details["route_capabilities"] = _route_decision_evidence(route_decision)

        if mode == "benchmark_only":
            return _decision(
                mode=mode,
                provider=provider,
                selected=current_provider,
                current=current_provider,
                fallback_reason="benchmark_only",
                fallback_allowed=True,
                warnings=("columnar_fast_path_benchmark_only", *warnings),
                blockers=blockers,
                details=details,
            )

        if blockers:
            if mode == "required":
                return _decision(
                    mode=mode,
                    provider=provider,
                    selected="blocked",
                    current=current_provider,
                    should_start_source_io=False,
                    blockers=blockers,
                    warnings=warnings,
                    details=details,
                )
            return _decision(
                mode=mode,
                provider=provider,
                selected=current_provider,
                current=current_provider,
                fallback_allowed=True,
                fallback_reason=blockers[0],
                blockers=blockers,
                warnings=warnings,
                details=details,
            )

        if provider == "direct_push_columnar":
            return _decision(
                mode=mode,
                provider=provider,
                selected="direct_push_columnar",
                current=current_provider,
                warnings=("direct_push_columnar_not_certified_for_prod_default", *warnings),
                details=details,
            )

        return _decision(
            mode=mode,
            provider=provider,
            selected="object_storage_pull",
            current=current_provider,
            warnings=warnings,
            details=details,
        )


def _eligibility_blockers(
    *,
    preflight: Any | None,
    schema_supported: bool,
    parquet_writer_available: bool,
    clickhouse_pull_available: bool,
) -> tuple[str, ...]:
    blockers: list[str] = []
    if preflight is None:
        blockers.append("object_storage_preflight_missing")
    elif not preflight.passed:
        blockers.extend(preflight.blockers or ("object_storage_preflight_failed",))
    if not schema_supported:
        blockers.append("columnar_schema_not_certified")
    if not parquet_writer_available:
        blockers.append("parquet_writer_unavailable")
    if not clickhouse_pull_available:
        blockers.append("clickhouse_columnar_pull_unavailable")
    return tuple(dict.fromkeys(blockers))


def _decision(
    *,
    mode: str,
    provider: str,
    selected: str,
    current: str,
    fallback_allowed: bool = False,
    should_start_source_io: bool = True,
    fallback_reason: str | None = None,
    blockers: tuple[str, ...] = (),
    warnings: tuple[str, ...] = (),
    details: dict[str, object] | None = None,
) -> ColumnarFastPathDecision:
    return ColumnarFastPathDecision(
        requested_mode=mode,
        requested_provider=provider,
        selected_provider=selected,
        current_provider=current,
        fallback_allowed=fallback_allowed,
        should_start_source_io=should_start_source_io,
        fallback_reason=fallback_reason,
        blockers=blockers,
        warnings=warnings,
        details=details or {},
    )


def _option(options: dict[str, Any], key: str, default: str) -> str:
    value = str(options.get(key) or default).strip().lower()
    return value or default


def _validate(mode: str, provider: str) -> None:
    if mode not in ALLOWED_MODES:
        raise ValueError(f"Unsupported columnar_fast_path.mode: {mode}")
    if provider not in ALLOWED_PROVIDERS:
        raise ValueError(f"Unsupported columnar_fast_path.provider: {provider}")


def _route_decision_evidence(route_decision: Any) -> dict[str, object]:
    if hasattr(route_decision, "to_evidence"):
        return dict(route_decision.to_evidence())
    return {"selected_route_id": str(getattr(route_decision, "selected_route_id", "unknown"))}


__all__ = ["ColumnarFastPathPlanner"]
