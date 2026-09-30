"""Read-only planning and single-attempt retirement through admitted ports.

No connector, credential lookup or deployment policy lives here. Composition
must supply the trusted observer and dedicated retired-slot store; the public
CLI cannot provide observation facts as if they were authenticated evidence.
"""

from __future__ import annotations

import hmac
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from dpone.contracts.publication_retirement import (
    PublicationRetirementPlan,
    plan_retirement,
    require_retirement_history,
)

if TYPE_CHECKING:
    from dpone.ports.publication_retirement import (
        HeldRetirementObservation,
        PublicationRetirementAttempts,
        PublicationRetirementObserver,
        PublicationRetirementStore,
    )


@dataclass(frozen=True, slots=True)
class PublicationRetirementResult:
    """Retirement outcome only; deliberately no load-success or DDL capability."""

    status: Literal["retired_unpublished", "blocked", "outcome_unknown"]
    plan_digest: str
    reason_code: str


class PublicationRetirementService:
    def __init__(
        self,
        *,
        observer: PublicationRetirementObserver,
        store: PublicationRetirementStore,
        attempts: PublicationRetirementAttempts,
        clock: Callable[[], int],
    ) -> None:
        self._observer, self._store, self._clock = observer, store, clock
        self._attempts = attempts

    def plan(self) -> PublicationRetirementPlan:
        """Observe without SQL mutation; reject unverifiable exclusion/history."""
        with self._observer.hold() as held:
            plan = plan_retirement(held.observe(), now=self._clock())
            held.require_held()
            return plan

    def apply(self, plan: PublicationRetirementPlan, *, confirmation_digest: str) -> PublicationRetirementResult:
        """Re-observe, insert at most once and verify under the same held freeze.

        If a mutation could have happened, failures remain UNKNOWN. The only
        recovery action supplied here is read-only verify, never a write retry.
        """
        self._confirm(plan, confirmation_digest)
        with self._observer.hold() as held:
            self._revalidate(held, plan)
            before = self._store.inspect(plan)
            if before == "exact":
                held.require_held()
                plan_retirement(plan.observation, now=self._clock())
                return self._verified(plan)
            if before != "absent":
                return PublicationRetirementResult(
                    "blocked" if before == "conflict" else "outcome_unknown", plan.digest, "destination_not_absent"
                )
            held.require_held()
            plan_retirement(plan.observation, now=self._clock())
            try:
                if not self._attempts.claim(plan.attempt_key):
                    return PublicationRetirementResult(
                        "outcome_unknown", plan.digest, "prior_attempt_requires_readback"
                    )
                held.require_held()
                plan_retirement(plan.observation, now=self._clock())
                self._store.retire_if_absent(plan)
                held.require_held()
                after = self._store.inspect(plan)
                held.require_held()
                plan_retirement(plan.observation, now=self._clock())
                if after == "exact":
                    return self._verified(plan)
                if after == "conflict":
                    return PublicationRetirementResult("blocked", plan.digest, "destination_conflict")
            except Exception:
                # A lost ACK, readback or exclusion failure cannot justify
                # repeating a durable action. Preserve the original plan.
                pass
            return PublicationRetirementResult("outcome_unknown", plan.digest, "retirement_requires_readback")

    def verify(self, plan: PublicationRetirementPlan, *, confirmation_digest: str) -> PublicationRetirementResult:
        """Resolve an ambiguous SQL outcome without writing anything."""
        self._confirm(plan, confirmation_digest, current=False)
        with self._observer.hold() as held:
            renewed = plan_retirement(held.observe(), now=self._clock())
            if renewed.source_snapshot != plan.source_snapshot:
                raise ValueError("retirement source facts changed during readback")
            held.require_held()
            observed = self._store.inspect(plan)
            held.require_held()
            plan_retirement(renewed.observation, now=self._clock())
            if observed == "exact":
                return self._verified(plan)
            return PublicationRetirementResult("outcome_unknown", plan.digest, "exact_retirement_not_observed")

    def _revalidate(self, held: HeldRetirementObservation, plan: PublicationRetirementPlan) -> None:
        current = plan_retirement(held.observe(), now=self._clock())
        if current.payload != plan.payload:
            raise ValueError("retirement observations changed; a new plan is required")
        held.require_held()

    def _confirm(self, plan: PublicationRetirementPlan, digest: str, *, current: bool = True) -> None:
        if not isinstance(plan, PublicationRetirementPlan) or not isinstance(digest, str):
            raise ValueError("exact retirement plan confirmation required")
        if current:
            plan_retirement(plan.observation, now=self._clock())
        else:
            require_retirement_history(plan)
        if not hmac.compare_digest(plan.digest, digest):
            raise ValueError("retirement confirmation digest differs")

    @staticmethod
    def _verified(plan: PublicationRetirementPlan) -> PublicationRetirementResult:
        return PublicationRetirementResult("retired_unpublished", plan.digest, "exact_retirement_observed")
