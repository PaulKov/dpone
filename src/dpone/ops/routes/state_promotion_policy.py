"""Fail-closed route state promotion policy."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Protocol

_DURABLE_SINK_STAGES = {"loaded_to_staging", "finalized", "quality_checked"}
_SUCCESSFUL_STATUSES = {"succeeded", "committed"}

_NEXT_ACTIONS = {
    "state_promotion.ledger_not_passed": "Fix route execution ledger blockers before promoting source state.",
    "state_promotion.route_mismatch": "Use a ledger from the same source, sink, and strategy route.",
    "state_promotion.dataset_mismatch": "Use a ledger for the same dataset.",
    "state_promotion.run_id_mismatch": "Use a ledger for the same run id.",
    "state_promotion.missing_durable_sink_success": (
        "Record a successful `loaded_to_staging`, `finalized`, or `quality_checked` ledger step first."
    ),
    "state_promotion.boundary_mismatch": "Use a commit receipt whose source and sink boundaries match durable ledger evidence.",
    "state_promotion.fencing_token_mismatch": "Use the active ledger lease fencing token before promoting state.",
    "state_promotion.idempotency_conflict": "Use a new idempotency key for a new state transition.",
    "state_promotion.concurrent_write_conflict": "Re-read promoted state and retry against the latest state version.",
}


@dataclass(frozen=True, slots=True)
class RouteStatePromotionDecision:
    """Pure state promotion policy decision."""

    passed: bool
    level: str
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]


class RouteStatePromotionPolicy:
    """Evaluate whether a commit receipt can advance source state."""

    def evaluate(
        self,
        *,
        route: _RouteIdentity,
        dataset: str,
        run_id: str,
        ledger_payload: Mapping[str, object],
        receipt: _CommitReceipt,
        blockers: tuple[str, ...] = (),
        warnings: tuple[str, ...] = (),
    ) -> RouteStatePromotionDecision:
        all_blockers = [*blockers]
        all_warnings = [*warnings]

        if ledger_payload.get("passed") is not True:
            all_blockers.append("state_promotion.ledger_not_passed")
        ledger_route = ledger_payload.get("route", {})
        if not isinstance(ledger_route, Mapping) or str(ledger_route.get("case_id", "")) != route.case_id:
            all_blockers.append("state_promotion.route_mismatch")
        if str(ledger_payload.get("dataset", "")) != dataset:
            all_blockers.append("state_promotion.dataset_mismatch")
        if str(ledger_payload.get("run_id", "")) != run_id:
            all_blockers.append("state_promotion.run_id_mismatch")

        durable_steps = _durable_success_steps(ledger_payload)
        if not durable_steps:
            all_blockers.append("state_promotion.missing_durable_sink_success")
        elif not _has_matching_boundary(durable_steps, receipt):
            all_blockers.append("state_promotion.boundary_mismatch")

        lease = ledger_payload.get("lease")
        if isinstance(lease, Mapping):
            ledger_token = str(lease.get("fencing_token", ""))
            if ledger_token and receipt.fencing_token != ledger_token:
                all_blockers.append("state_promotion.fencing_token_mismatch")

        unique_blockers = tuple(dict.fromkeys(all_blockers))
        unique_warnings = tuple(dict.fromkeys(all_warnings))
        return RouteStatePromotionDecision(
            passed=not unique_blockers,
            level="blocked" if unique_blockers else "promoted",
            blockers=unique_blockers,
            warnings=unique_warnings,
            next_actions=tuple(_NEXT_ACTIONS[item] for item in unique_blockers if item in _NEXT_ACTIONS),
        )


def _durable_success_steps(ledger_payload: Mapping[str, object]) -> tuple[Mapping[str, object], ...]:
    raw_steps = ledger_payload.get("steps", [])
    if not isinstance(raw_steps, list):
        return tuple()
    return tuple(
        step
        for step in raw_steps
        if isinstance(step, Mapping)
        and str(step.get("stage", "")) in _DURABLE_SINK_STAGES
        and str(step.get("status", "")) in _SUCCESSFUL_STATUSES
    )


def _has_matching_boundary(steps: tuple[Mapping[str, object], ...], receipt: _CommitReceipt) -> bool:
    return any(
        str(step.get("source_boundary", "")) == receipt.source_boundary
        and str(step.get("sink_boundary", "")) == receipt.sink_boundary
        for step in steps
    )


class _RouteIdentity(Protocol):
    @property
    def case_id(self) -> str: ...


class _CommitReceipt(Protocol):
    @property
    def source_boundary(self) -> str: ...

    @property
    def sink_boundary(self) -> str: ...

    @property
    def fencing_token(self) -> str: ...
