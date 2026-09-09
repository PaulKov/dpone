"""Connector-neutral source export benchmark service."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from dpone.runtime.export_optimizer_models import (
    ExportOptimizerPolicy,
    ExportProviderDecision,
    ExportProviderProbe,
)


class ExportCacheKey(Protocol):
    """Stable cache fingerprint provider."""

    def fingerprint(self) -> str:
        """Return a stable cache fingerprint."""


class ExportDecisionService(Protocol):
    """Provider decision service used after probes are collected."""

    def decide(
        self,
        policy: ExportOptimizerPolicy,
        *,
        current_default: str,
        probes: Sequence[ExportProviderProbe],
        cache_key: ExportCacheKey | None = None,
    ) -> ExportProviderDecision:
        """Return the selected provider decision."""


@dataclass(frozen=True, slots=True)
class ExportProbeRequest:
    """Bounded source query probe request shared by provider runners."""

    source_type: str
    query: str
    schema: tuple[tuple[str, str], ...]
    work_dir: Path
    probe_rows: int
    max_probe_seconds: int
    batch_size: int


class SourceExportProbeRunner(Protocol):
    """Probe runner for one source export provider."""

    provider_id: str

    def probe(self, request: ExportProbeRequest) -> ExportProviderProbe:
        """Run a bounded probe and return normalized metrics."""


class SourceExportBenchmarkService:
    """Run bounded probes and delegate provider selection to the optimizer."""

    def __init__(
        self,
        runners: Sequence[SourceExportProbeRunner],
        optimizer: ExportDecisionService,
    ) -> None:
        self._runners = {runner.provider_id: runner for runner in runners}
        self._optimizer = optimizer

    def benchmark(
        self,
        policy: ExportOptimizerPolicy,
        *,
        current_default: str,
        request: ExportProbeRequest,
        cache_key: ExportCacheKey | None = None,
    ) -> ExportProviderDecision:
        probes = [self._run_candidate(candidate, request) for candidate in policy.candidates]
        return self._optimizer.decide(policy, current_default=current_default, probes=probes, cache_key=cache_key)

    def _run_candidate(self, provider_id: str, request: ExportProbeRequest) -> ExportProviderProbe:
        runner = self._runners.get(provider_id)
        if runner is None:
            return ExportProviderProbe(
                provider_id=provider_id,
                blockers=("export_provider_runner_missing",),
                safe=False,
                certified=False,
            )
        try:
            return runner.probe(request)
        except Exception as exc:  # noqa: BLE001 - benchmark evidence must record provider failure.
            return ExportProviderProbe(
                provider_id=provider_id,
                blockers=(f"export_provider_probe_failed:{exc.__class__.__name__}",),
                warnings=(str(exc),),
                safe=False,
            )


__all__ = [
    "ExportDecisionService",
    "ExportProbeRequest",
    "ExportProviderProbe",
    "SourceExportBenchmarkService",
    "SourceExportProbeRunner",
]
