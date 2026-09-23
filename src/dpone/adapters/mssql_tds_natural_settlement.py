"""Pure admission and deadline policy for one child settlement command."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import cast


class TdsNaturalSettlementPolicyError(ValueError):
    """A resolved settlement plan cannot be trusted."""


def _deadline(value: object) -> float:
    if type(value) not in (int, float):
        raise ValueError("mssql_native.tds_natural_settlement_deadline")
    try:
        numeric = float(cast("int | float", value))
    except OverflowError:
        raise ValueError("mssql_native.tds_natural_settlement_deadline") from None
    if not math.isfinite(numeric):
        raise ValueError("mssql_native.tds_natural_settlement_deadline")
    return numeric


@dataclass(frozen=True, slots=True)
class TdsNaturalSettlementCommand:
    natural_deadline: float
    containment_deadline: float | None = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "natural_deadline", _deadline(self.natural_deadline))
        if self.containment_deadline is not None:
            object.__setattr__(self, "containment_deadline", _deadline(self.containment_deadline))


@dataclass(frozen=True, slots=True)
class TdsNaturalSettlementPlan:
    natural_deadline: float
    cleanup_deadline: float
    force_immediately: bool


class TdsNaturalSettlementState:
    """One-shot command state with no process or clock effects."""

    def __init__(self) -> None:
        self._phase = "AVAILABLE"
        self._command: TdsNaturalSettlementCommand | None = None
        self._plan: TdsNaturalSettlementPlan | None = None
        self._now: float | None = None

    @property
    def force_requested(self) -> bool:
        return self._phase == "FORCE"

    @property
    def plan(self) -> TdsNaturalSettlementPlan | None:
        return self._plan

    def admit(self, command: TdsNaturalSettlementCommand) -> None:
        if self._command is not None:
            raise ValueError("mssql_native.tds_natural_settlement_already_requested")
        if self._phase != "AVAILABLE":
            raise ValueError("mssql_native.tds_natural_settlement_unavailable")
        self._command, self._phase = command, "ADMITTED"

    def resolve(self, *, operation_deadline: float, allowance: float, now: float) -> TdsNaturalSettlementPlan:
        if self._phase != "ADMITTED" or self._command is None:
            raise TdsNaturalSettlementPolicyError("mssql_native.tds_natural_settlement_unknown")
        try:
            operation, extra, observed = map(_deadline, (operation_deadline, allowance, now))
            natural = min(self._command.natural_deadline, operation)
            cleanup = natural + extra
            if self._command.containment_deadline is not None:
                cleanup = min(cleanup, self._command.containment_deadline)
            if not math.isfinite(cleanup) or cleanup <= observed:
                raise ValueError
        except ValueError:
            raise TdsNaturalSettlementPolicyError("mssql_native.tds_natural_settlement_unknown") from None
        immediate = cleanup <= natural or natural <= observed
        self._plan = TdsNaturalSettlementPlan(natural, cleanup, immediate)
        self._now, self._phase = observed, "FORCE" if immediate else "NATURAL"
        return self._plan

    def request_force(self) -> None:
        if self._phase != "UNKNOWN":
            self._phase = "FORCE"

    def natural_ceiling(self) -> float:
        if self._phase == "UNKNOWN" or self._plan is None or self._now is None:
            raise TdsNaturalSettlementPolicyError("mssql_native.tds_natural_settlement_unknown")
        return self._now if self._phase == "FORCE" else self._plan.natural_deadline

    def force_if_natural_expired(self, now: float) -> bool:
        if self._plan is None:
            raise TdsNaturalSettlementPolicyError("mssql_native.tds_natural_settlement_unknown")
        if _deadline(now) >= self._plan.natural_deadline:
            self.request_force()
        return self.force_requested

    def permit_forced_transition(
        self, *, error_args: tuple[object, ...], reap_consumed: bool, handle_unknown: bool
    ) -> bool:
        exact_timeout = (
            type(error_args) is tuple
            and len(error_args) == 1
            and type(error_args[0]) is str
            and error_args[0] == "tds_process_deadline_exceeded"
        )
        permitted = self._phase in {"NATURAL", "FORCE"} and exact_timeout and not reap_consumed and not handle_unknown
        self._phase = "FORCE" if permitted else "UNKNOWN"
        return permitted

    def mark_unknown(self) -> None:
        self._phase = "UNKNOWN"
