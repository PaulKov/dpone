"""Guarded legacy retirement composition; no source I/O or target mutation."""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.adapters.publication_plan_file import read_private_plan, write_private_plan
from dpone.app.publication_operator_context import (
    OperatorScopeBlocked,
    context_subject,
    resolve_authority,
    verified_context,
)
from dpone.contracts.publication_retirement_operator_plan import (
    PublicationRetirementOperatorPlan,
    decode_retirement_operator_plan,
)
from dpone.runtime.publication_authority_composition import build_publication_retirement
from dpone.services.publication_retirement import PublicationRetirementService

if TYPE_CHECKING:
    from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.ports.publication_retirement import PublicationRetirementAttemptJournal, PublicationRetirementObserver


class PublicationRetirementApplication:
    """A pre-bound observer/journal are required; CLI arguments cannot admit them."""

    def __init__(
        self,
        *,
        environ: Mapping[str, str] | None = None,
        observer: PublicationRetirementObserver | None = None,
        journal: PublicationRetirementAttemptJournal | None = None,
        connector_factory: Callable[[ResolvedBindingConnection], Any] | None = None,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self._environ, self._observer, self._journal = environ, observer, journal
        self._factory = connector_factory
        self._clock = clock if clock is not None else lambda: int(time.time())

    def plan(self, *, connection_ref: str, environment: str, path: Path) -> dict[str, str]:
        """Read-only observation and existing-catalog admission, then private plan."""
        digest = ""
        try:
            service, binding, pin, subject, journal = self._service(connection_ref, environment)
            retirement = service.plan()
            plan = PublicationRetirementOperatorPlan(binding, pin, subject, journal, retirement)
            digest = plan.digest
            if self._ready() != journal:
                raise OperatorScopeBlocked("plan_journal_differs")
            write_private_plan(path, plan.payload.encode())
            return _result("ready", "retirement_planned", digest)
        except OperatorScopeBlocked as error:
            return _result("blocked", str(error), digest)
        except Exception:
            return _result("outcome_unknown", "retirement_plan_unavailable", digest)

    def apply(self, *, path: Path, environment: str, confirmation_digest: str) -> dict[str, str]:
        """One durable attempt; ambiguous outcomes require exact readback."""
        return self._perform(path, environment, confirmation_digest, verify=False)

    def verify(self, *, path: Path, environment: str, confirmation_digest: str) -> dict[str, str]:
        """Read original plan provenance under renewed observation; never write."""
        return self._perform(path, environment, confirmation_digest, verify=True)

    def _perform(self, path: Path, environment: str, confirmation: str, *, verify: bool) -> dict[str, str]:
        digest = ""
        try:
            plan = decode_retirement_operator_plan(read_private_plan(path))
            digest = plan.digest
            if not plan.confirms(confirmation):
                raise OperatorScopeBlocked("plan_confirmation_required")
            if environment != plan.binding.environment:
                raise OperatorScopeBlocked("plan_environment_differs")
            service, *_ = self._service(plan.binding.connection_ref, environment, expected=plan)
            action = service.verify if verify else service.apply
            result = action(plan.retirement, confirmation_digest=plan.retirement.digest)
            return _result(result.status, result.reason_code, digest)
        except OperatorScopeBlocked as error:
            return _result("blocked", str(error), digest)
        except Exception:
            return _result("outcome_unknown", "retirement_requires_readback", digest)

    def _ready(self) -> str:
        if self._observer is None:
            raise OperatorScopeBlocked("held_retirement_observer_required")
        if self._journal is None:
            raise OperatorScopeBlocked("admitted_retirement_journal_required")
        try:
            return self._journal.require_ready()
        except Exception:
            raise OperatorScopeBlocked("retirement_journal_unavailable") from None

    def _service(
        self,
        connection_ref: str,
        environment: str,
        expected: PublicationRetirementOperatorPlan | None = None,
    ) -> tuple[PublicationRetirementService, PublicationAuthorityBinding, str, str, str]:
        journal = self._ready()
        if expected is not None and journal != expected.journal_identity:
            raise OperatorScopeBlocked("plan_journal_differs")
        context = verified_context(self._environ, environment)
        subject = context_subject(context)
        if expected is not None and subject != expected.context_subject:
            raise OperatorScopeBlocked("plan_context_differs")
        sql, binding, pin = resolve_authority(context, connection_ref)
        if expected is not None and (binding != expected.binding or pin != expected.endpoint_identity):
            raise OperatorScopeBlocked("plan_authority_scope_differs")
        store = build_publication_retirement(
            connection=sql,
            binding=binding,
            environment=environment,
            clock=self._clock,
            connector_factory=self._factory,
        )
        assert self._observer is not None and self._journal is not None
        service = PublicationRetirementService(
            observer=self._observer, store=store, attempts=self._journal, clock=self._clock
        )
        return service, binding, pin, subject, journal


def _result(status: str, reason: str, digest: str) -> dict[str, str]:
    return {
        "contract": "dpone.publication-retirement-result.v1",
        "status": status,
        "reason_code": reason,
        "plan_digest": digest,
    }
