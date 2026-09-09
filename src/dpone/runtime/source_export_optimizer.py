"""Pure source export provider decision service."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Protocol

from dpone.runtime.export_optimizer_models import (
    ExportOptimizerPolicy,
    ExportProviderDecision,
    ExportProviderProbe,
)


class ExportCacheKey(Protocol):
    def fingerprint(self) -> str:
        """Return a stable cache fingerprint."""
        ...


class SourceExportOptimizer:
    """Select the fastest safe source export provider from bounded probes."""

    def decide(
        self,
        policy: ExportOptimizerPolicy,
        *,
        current_default: str,
        probes: Sequence[ExportProviderProbe],
        cache_key: ExportCacheKey | None = None,
    ) -> ExportProviderDecision:
        requested = policy.mode
        normalized_default = current_default.strip().lower()
        filtered = _filter_candidates(policy, probes)
        rejected = _rejected(filtered)
        eligible = tuple(probe for probe in filtered if probe.rejection_reason is None)
        warnings: list[str] = []
        reasons: list[str] = []
        blockers: list[str] = []

        if requested == "off":
            return _decision(
                policy,
                normalized_default,
                selected=normalized_default,
                recommended=None,
                speedup=None,
                cache_key=cache_key,
                probes=filtered,
                rejected=rejected,
                reasons=("export_optimizer_disabled",),
            )

        if not eligible:
            if requested == "required":
                blockers.append("export_optimizer_no_safe_provider")
            else:
                warnings.append("export_optimizer_no_safe_provider")
            return _decision(
                policy,
                normalized_default,
                selected=None if blockers else normalized_default,
                recommended=None,
                speedup=None,
                cache_key=cache_key,
                probes=filtered,
                rejected=rejected,
                blockers=tuple(blockers),
                warnings=tuple(warnings),
            )

        fastest = max(eligible, key=lambda item: item.primary_speed or 0.0)
        default_probe = _probe_by_id(eligible, normalized_default)
        default_speed = default_probe.primary_speed if default_probe else None
        fastest_speed = fastest.primary_speed or 0.0
        if default_speed is None or default_speed == 0:
            warnings.append("export_optimizer_default_probe_missing")
            speedup = None
        else:
            speedup = round((fastest_speed / default_speed - 1) * 100, 2)

        selected = normalized_default
        recommended = fastest.provider_id
        if requested == "benchmark_only":
            reasons.append("export_optimizer_benchmark_only")
        elif speedup is None and fastest.provider_id != normalized_default:
            selected = fastest.provider_id
            reasons.append("export_optimizer_selected_measured_provider")
        elif speedup is not None and speedup >= policy.min_speedup_pct:
            selected = fastest.provider_id
            reasons.append("export_optimizer_selected_faster_provider")
        elif fastest.provider_id == normalized_default:
            reasons.append("export_optimizer_default_fastest")
        else:
            reasons.append("export_optimizer_speedup_below_threshold")

        if requested == "required" and selected == normalized_default and fastest.provider_id != normalized_default:
            blockers.append("export_optimizer_required_speedup_not_met")

        return _decision(
            policy,
            normalized_default,
            selected=None if blockers else selected,
            recommended=recommended,
            speedup=speedup,
            cache_key=cache_key,
            probes=filtered,
            rejected=rejected,
            blockers=tuple(blockers),
            warnings=tuple(warnings),
            reasons=tuple(reasons),
        )


def _decision(
    policy: ExportOptimizerPolicy,
    current_default: str,
    *,
    selected: str | None,
    recommended: str | None,
    speedup: float | None,
    cache_key: ExportCacheKey | None,
    probes: Sequence[ExportProviderProbe],
    rejected: dict[str, str],
    blockers: tuple[str, ...] = (),
    warnings: tuple[str, ...] = (),
    reasons: tuple[str, ...] = (),
) -> ExportProviderDecision:
    return ExportProviderDecision(
        requested_mode=policy.mode,
        current_default=current_default,
        selected_provider=selected,
        recommended_provider=recommended,
        measured_speedup_pct=speedup,
        source_bottleneck="export",
        cache_key=cache_key.fingerprint() if cache_key else None,
        release_gate=_release_gate(blockers, warnings),
        probes=tuple(probes),
        rejected=rejected,
        blockers=blockers,
        warnings=warnings,
        reasons=tuple(dict.fromkeys(item for item in reasons if item)),
    )


def _filter_candidates(
    policy: ExportOptimizerPolicy,
    probes: Sequence[ExportProviderProbe],
) -> tuple[ExportProviderProbe, ...]:
    allowed = set(policy.candidates)
    return tuple(probe for probe in probes if probe.provider_id in allowed)


def _rejected(probes: Sequence[ExportProviderProbe]) -> dict[str, str]:
    return {probe.provider_id: reason for probe in probes if (reason := probe.rejection_reason) is not None}


def _probe_by_id(probes: Sequence[ExportProviderProbe], provider_id: str) -> ExportProviderProbe | None:
    for probe in probes:
        if probe.provider_id == provider_id:
            return probe
    return None


def _release_gate(blockers: Sequence[str], warnings: Sequence[str]) -> str:
    if blockers:
        return "blocked"
    if warnings:
        return "warning"
    return "green"


__all__ = ["ExportCacheKey", "SourceExportOptimizer"]
