"""Execution contract builder for route run supervision."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Protocol

DEFAULT_ROUTE_RUN_MODE = "evidence_only"
RouteRunContractStageStatus = Literal[
    "complete",
    "missing",
    "blocked",
    "retryable",
    "unsafe_to_retry",
    "manual_approval_required",
    "optional",
]

ROUTE_RUN_MODE_REQUIRED_EVIDENCE: Mapping[str, tuple[str, ...]] = {
    "evidence_only": (
        "route_readiness",
        "route_execution_ledger",
        "state_promotion",
    ),
    "route_refresh": (
        "route_readiness",
        "route_refresh_execution",
        "route_refresh_snapshot_capture",
        "route_refresh_verification",
        "route_execution_ledger",
        "state_promotion",
    ),
}


@dataclass(frozen=True, slots=True)
class RouteRunStageDefinition:
    """Static execution-plane stage metadata for one evidence domain."""

    name: str
    phase: str
    evidence: str
    command_template: str


@dataclass(frozen=True, slots=True)
class RouteRunContractStageSpec:
    """Internal stage plan emitted before public report-model adaptation."""

    name: str
    phase: str
    evidence: str
    required: bool
    status: RouteRunContractStageStatus
    command: str
    path: str
    blockers: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class RouteRunExecutionContractSpec:
    """Internal execution contract plan emitted by the generic builder."""

    mode: str
    ready: bool
    stages: tuple[RouteRunContractStageSpec, ...]
    next_commands: tuple[str, ...]


class _RouteContext(Protocol):
    @property
    def source(self) -> str: ...

    @property
    def sink(self) -> str: ...

    @property
    def strategy(self) -> str: ...


class _RunContext(Protocol):
    @property
    def run_id(self) -> str: ...

    @property
    def dataset(self) -> str: ...

    @property
    def manifest(self) -> str: ...


class _EvidenceContext(Protocol):
    @property
    def name(self) -> str: ...

    @property
    def missing(self) -> bool: ...

    @property
    def passed(self) -> bool: ...

    @property
    def path(self) -> str: ...

    @property
    def blockers(self) -> tuple[str, ...]: ...

    @property
    def safe_to_retry(self) -> bool | None: ...

    @property
    def requires_manual_approval(self) -> bool: ...


class RouteRunExecutionContractBuilder:
    """Build self-service stage contracts without executing route workloads."""

    def build(
        self,
        *,
        mode: str,
        route: _RouteContext,
        run: _RunContext,
        output_dir: str | Path,
        required_evidence: Sequence[str],
        evidence: Sequence[_EvidenceContext],
    ) -> RouteRunExecutionContractSpec:
        normalized_mode = normalize_run_mode(mode)
        evidence_by_name = {item.name: item for item in evidence}
        required = tuple(dict.fromkeys(str(item) for item in required_evidence if str(item)))
        stage_names = tuple(dict.fromkeys((*_mode_stage_names(normalized_mode), *required)))
        stages = tuple(
            self._stage(
                definition=_definition(name),
                route=route,
                run=run,
                output_dir=Path(output_dir),
                required=name in required,
                evidence=evidence_by_name.get(name),
            )
            for name in stage_names
        )
        next_commands = tuple(stage.command for stage in stages if stage.required and stage.status != "complete")
        return RouteRunExecutionContractSpec(
            mode=normalized_mode,
            ready=all(stage.status == "complete" for stage in stages if stage.required),
            stages=stages,
            next_commands=next_commands,
        )

    @staticmethod
    def _stage(
        *,
        definition: RouteRunStageDefinition,
        route: _RouteContext,
        run: _RunContext,
        output_dir: Path,
        required: bool,
        evidence: _EvidenceContext | None,
    ) -> RouteRunContractStageSpec:
        status = _stage_status(required=required, evidence=evidence)
        return RouteRunContractStageSpec(
            name=definition.name,
            phase=definition.phase,
            evidence=definition.evidence,
            required=required,
            status=status,
            command=_render_command(
                definition.command_template,
                route=route,
                run=run,
                output_dir=output_dir,
                evidence=definition.evidence,
            ),
            path=evidence.path if evidence else "",
            blockers=evidence.blockers if evidence else ((f"{definition.evidence}.missing",) if required else ()),
        )


def normalize_run_mode(value: str | None) -> str:
    """Normalize and validate route run supervisor mode."""

    mode = str(value or DEFAULT_ROUTE_RUN_MODE).strip().lower().replace("-", "_")
    if mode not in ROUTE_RUN_MODE_REQUIRED_EVIDENCE:
        allowed = ", ".join(sorted(ROUTE_RUN_MODE_REQUIRED_EVIDENCE))
        raise ValueError(f"route run mode must be one of: {allowed}")
    return mode


def required_evidence_for_mode(mode: str) -> tuple[str, ...]:
    """Return default required evidence domains for a run mode."""

    return ROUTE_RUN_MODE_REQUIRED_EVIDENCE[normalize_run_mode(mode)]


def _mode_stage_names(mode: str) -> tuple[str, ...]:
    return ROUTE_RUN_MODE_REQUIRED_EVIDENCE[normalize_run_mode(mode)]


def _definition(name: str) -> RouteRunStageDefinition:
    return _STAGE_DEFINITIONS.get(
        name,
        RouteRunStageDefinition(
            name=name,
            phase="other",
            evidence=name,
            command_template="Attach `{evidence}` evidence with --artifact {evidence}=<artifact.json>.",
        ),
    )


def _stage_status(*, required: bool, evidence: _EvidenceContext | None) -> RouteRunContractStageStatus:
    if evidence is None or evidence.missing:
        return "missing" if required else "optional"
    if evidence.passed:
        return "complete"
    if evidence.requires_manual_approval:
        return "manual_approval_required"
    if evidence.safe_to_retry is True:
        return "retryable"
    if evidence.safe_to_retry is False:
        return "unsafe_to_retry"
    return "blocked"


def _render_command(
    template: str,
    *,
    route: _RouteContext,
    run: _RunContext,
    output_dir: Path,
    evidence: str,
) -> str:
    return template.format(
        source=route.source,
        sink=route.sink,
        strategy=route.strategy,
        run_id=run.run_id,
        dataset=run.dataset,
        manifest=run.manifest or "<manifest.yml>",
        output_dir=output_dir,
        evidence=evidence,
    )


_STAGE_DEFINITIONS: dict[str, RouteRunStageDefinition] = {
    "route_readiness": RouteRunStageDefinition(
        name="route_readiness",
        phase="preflight",
        evidence="route_readiness",
        command_template=(
            "uv run dpone ops route-readiness --source {source} --sink {sink} "
            "--strategy {strategy} --output-dir {output_dir}/route-readiness --format json"
        ),
    ),
    "route_refresh_execution": RouteRunStageDefinition(
        name="route_refresh_execution",
        phase="execution",
        evidence="route_refresh_execution",
        command_template=(
            "uv run dpone ops route-refresh-execute --route-refresh-plan-json <route_refresh_plan.json> "
            "--runner-id {run_id} --executor <executor_backend> --executor-config-json <executor-config.json> "
            "--execute --output-dir {output_dir}/route-refresh-execute --format json"
        ),
    ),
    "route_refresh_snapshot_capture": RouteRunStageDefinition(
        name="route_refresh_snapshot_capture",
        phase="reconciliation",
        evidence="route_refresh_snapshot_capture",
        command_template=(
            "uv run dpone ops route-refresh-capture-snapshots "
            "--route-refresh-execution-json <route_refresh_execution.json> --runner-id {run_id}-capture "
            "--key <key_column> --boundary-column <boundary_column> --column <column> "
            "--output-dir {output_dir}/route-refresh-capture --format json"
        ),
    ),
    "route_refresh_verification": RouteRunStageDefinition(
        name="route_refresh_verification",
        phase="reconciliation",
        evidence="route_refresh_verification",
        command_template=(
            "uv run dpone ops route-refresh-verify --route-refresh-execution-json <route_refresh_execution.json> "
            "--source-snapshot-json <source_route_refresh_snapshot.json> "
            "--sink-snapshot-json <sink_route_refresh_snapshot.json> --runner-id {run_id}-verify "
            "--key <key_column> --boundary-column <boundary_column> --column <column> "
            "--output-dir {output_dir}/route-refresh-verify --format json"
        ),
    ),
    "route_execution_ledger": RouteRunStageDefinition(
        name="route_execution_ledger",
        phase="execution",
        evidence="route_execution_ledger",
        command_template=(
            "uv run dpone ops route-execution-ledger --source {source} --sink {sink} --strategy {strategy} "
            "--dataset {dataset} --run-id {run_id} --stage quality_checked --status succeeded "
            "--runner-id <runner_id> --source-boundary <source_boundary> --sink-boundary <sink_boundary> "
            "--artifact route_refresh_verification=<route_refresh_verification.json> "
            "--output-dir {output_dir}/route-execution-ledger --format json"
        ),
    ),
    "state_promotion": RouteRunStageDefinition(
        name="state_promotion",
        phase="state",
        evidence="state_promotion",
        command_template=(
            "uv run dpone ops route-state-promote --source {source} --sink {sink} --strategy {strategy} "
            "--dataset {dataset} --run-id {run_id} --ledger-json <route_execution_ledger.json> "
            "--proposed-state <source_state> --source-boundary <source_boundary> "
            "--sink-boundary <sink_boundary> --idempotency-key <idempotency_key> "
            "--commit-token <commit_token> --target {dataset} "
            "--output-dir {output_dir}/route-state-promotion --format json"
        ),
    ),
}


__all__ = [
    "DEFAULT_ROUTE_RUN_MODE",
    "ROUTE_RUN_MODE_REQUIRED_EVIDENCE",
    "RouteRunContractStageSpec",
    "RouteRunExecutionContractSpec",
    "RouteRunExecutionContractBuilder",
    "RouteRunStageDefinition",
    "normalize_run_mode",
    "required_evidence_for_mode",
]
