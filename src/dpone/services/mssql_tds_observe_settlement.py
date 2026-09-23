"""Original PREPARED continuation; durable ACKs never replace producer custody.

The application retains the concrete helper and checks it against the three
producer captures around effects. This owner alone submits evidence and directory
proofs; completion remains provisional until the final sequence guard succeeds.
"""

from __future__ import annotations

import math
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import replace
from time import monotonic
from typing import Any, NoReturn
from uuid import UUID

from dpone.ports.mssql_sqlclient_departure_evidence import (
    SqlClientDepartureEvidenceGateway,
    SqlClientObserveContainmentEvidenceGateway,
)
from dpone.ports.mssql_tds_directory import (
    AssertDirectoryAuthority,
)
from dpone.services.mssql_sqlclient_observe_helper_evidence import ObserveHelperEvidence
from dpone.services.mssql_tds_observe_settlement_custody import ObserveSettlementCustodyMixin
from dpone.services.mssql_tds_original_continuation import PreparationTransition
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientDatabasePrincipal,
    SqlClientObserveContainment,
    SqlClientObserveContainmentObservation,
    SqlClientObserveDeparturePlan,
    SqlClientObserveDepartureRequest,
    SqlClientObserveDepartureResult,
    TdsAttemptPhase,
    TdsChildExit,
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
    WindowOutcomeUnknown,
    attempt_identity_digest,
    authority_digest,
    deadline_nanoseconds,
    encode_observe_containment,
    observe_containment_receipt,
    process_identity_digest,
    session_authority_digest,
    validate_original_record,
)
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientDepartureEvidenceKind as Kind,
)

ERROR = "mssql_native.sqlclient_observe_settlement_unknown"


class ObserveSettlement(ObserveSettlementCustodyMixin):
    """One consumed original continuation, including retained uncertain attempts."""

    def __init__(self, attempt: Any, origin: PreparationTransition, helper_id: UUID, *, deadline: float) -> None:
        if type(origin) is not PreparationTransition or type(helper_id) is not UUID or not helper_id.int:
            raise ValueError(ERROR)
        if type(deadline) is not float or not math.isfinite(deadline) or deadline <= 0:
            raise ValueError(ERROR)
        self.attempt, self.origin, self.helper_id, self.deadline = attempt, origin, helper_id, deadline
        self.used = self.failed = self.complete = self._provisional = False
        self.pending = True
        self._checking = self._acting = self._containment_closing = self._containment_closed = False
        self._close_reentered = False
        self._cleanup_deadline: tuple[float] | None = None
        self._containment_bound = False
        self.directory: TdsDirectorySnapshot | None = None
        self.local_ack: TdsDirectorySnapshot | None = None
        self.remote_ack: TdsDirectorySnapshot | None = None
        self.local_return: object = None
        self.remote_return: object = None
        self.plan: SqlClientObserveDeparturePlan | None = None
        self._containment: SqlClientObserveContainmentEvidenceGateway | None = None
        self._original_containment: SqlClientObserveContainmentEvidenceGateway | None = None
        self.containment_capture: tuple | None = None
        self._containment_snapshot: tuple | None = None
        self._helper = ObserveHelperEvidence()
        self._original_helper = self._helper

    @property
    def request(self) -> SqlClientObserveDepartureRequest | None:
        return self._helper.request

    @property
    def result(self) -> SqlClientObserveDepartureResult | None:
        return self._helper.result

    @property
    def local_exit(self) -> TdsChildExit | None:
        return self._helper.local_exit

    @property
    def helper_records(self) -> Mapping[Kind, tuple]:
        return self._helper.records

    def _reject(self) -> NoReturn:
        self.failed = self.attempt._poisoned = True
        raise WindowOutcomeUnknown(ERROR)

    @contextmanager
    def sequence(self) -> Iterator[ObserveSettlement]:
        """Register before external effects; only final successful exit releases work."""
        a = self.attempt
        a._owned()
        a._require_unpoisoned()
        if self.used or getattr(a, "_observe_settlement", None) is not None or a._prepared_origin is not self.origin:
            self._reject()
        self.used = a._busy = True
        a._observe_settlement = self
        try:
            o = self.origin
            self.deadline = min(self.deadline, o.deadline)
            self._entry_values = (self.helper_id, self.deadline)
            self._references = (a, o, o.factory, o.pool, a._lifecycle, a._directory)
            self.directory = deepcopy(o.directory)
            self.assert_current(deadline=self.deadline)
            yield self
            self.assert_current(deadline=self.deadline)
            if not self._provisional:
                self._reject()
            self.complete, self.pending = True, False
        except BaseException:
            self._reject()
        finally:
            a._busy = False

    def _assert_original(self, deadline: float) -> None:
        a, o = self.attempt, self.origin
        a._require_unpoisoned()
        if (
            self.failed
            or not self.used
            or not a._busy
            or a._observe_settlement is not self
            or a._prepared_origin is not o
            or a._composition_origin is not o.factory
            or not o.cleaned
            or o.failed
            or a._preparation is not None
            or (self.helper_id, self.deadline) != self._entry_values
            or type(deadline) is not float
            or not math.isfinite(deadline)
            or not 0 < deadline <= min(self.deadline, o.deadline)
        ):
            self._reject()
        if (
            any(
                x is not y
                for x, y in zip((a, o, o.factory, o.pool, a._lifecycle, a._directory), self._references, strict=True)
            )
            or self._containment is not self._original_containment
            or self._helper is not self._original_helper
        ):
            self._reject()
        o.validate_origin()
        self._helper.validate_current(self.plan)
        if self._containment_snapshot is not None:
            validate_original_record(self.containment_capture)
            if self.containment_capture != self._containment_snapshot:
                self._reject()

    def assert_current(self, *, deadline: float) -> None:
        """Check PREPARED and predicted directory; never reread closed original actors."""
        if self._checking:
            self._reject()
        self._checking = True
        try:
            self._assert_original(deadline)
            a, o = self.attempt, self.origin
            if monotonic() >= deadline:
                self._reject()
            a._require_unpoisoned()
            a._lifecycle.assert_authority(deadline=deadline)
            a._require_unpoisoned()
            parent = a._lifecycle.snapshot
            validate_original_record((parent, self.directory))
            if parent != o.expected or parent.state.phase is not TdsAttemptPhase.PREPARED:
                self._reject()
            a._require_unpoisoned()
            observed = a._directory.execute(AssertDirectoryAuthority(), deadline=deadline)
            a._require_unpoisoned()
            validate_original_record(observed)
            if observed != self.directory or a._directory.observation.snapshot != observed:
                self._reject()
            a._require_unpoisoned()
            o.pool.assert_deadline(deadline=deadline)
            current = (a._lifecycle.snapshot, a._directory.observation.snapshot)
            validate_original_record(current)
            self._assert_original(deadline)
            if current != (parent, observed) or observed != self.directory:
                self._reject()
            self._helper.validate_current(self.plan, final=self._provisional)
            a._require_unpoisoned()
        except BaseException:
            self._reject()
        finally:
            self._checking = False

    @contextmanager
    def _effect(self, deadline: float, *, capture: bool = False) -> Iterator[None]:
        if self._acting or self._checking:
            self._reject()
        self._acting = True
        try:
            if not capture:
                self.assert_current(deadline=deadline)
            yield
            self.assert_current(deadline=deadline)
        except BaseException:
            self._reject()
        finally:
            self._acting = False

    def retain_containment_gateway(self, gateway: SqlClientObserveContainmentEvidenceGateway | None) -> None:
        """Retain cleanup capability before any fallible gateway observation."""
        if self._containment_bound:
            self._reject()
        self._containment_bound = True
        self._containment = self._original_containment = gateway

    def bind_helper_evidence(self, gateway: SqlClientDepartureEvidenceGateway | None) -> None:
        """Nonowning evidence reference; application custody alone closes it."""
        with self._effect(self.deadline, capture=True):
            self._helper.bind_gateway(gateway)

    def acknowledge_containment(self, *, deadline: float) -> TdsDirectorySnapshot:
        with self._effect(deadline):
            if self.containment_capture is not None or self._containment is None:
                self._reject()
            o = self.origin
            preparation = o.evidence_receipt
            assert preparation is not None
            registration, authority = o.original_evidence[1][1], o.original_evidence[2][1]
            value = SqlClientObserveContainment(
                attempt_sha256=attempt_identity_digest(o.identity.parent),
                observe_operation=o.identity,
                registration_artifact_sha256=registration.payload_sha256,
                authority_artifact_sha256=authority.payload_sha256,
                original_authority_sha256=authority_digest(o.authority),
                preparation_artifact_sha256=preparation.payload_sha256,
                exit=o.contained_exit,
            )
            payload = encode_observe_containment(value, process=o.startup.process)
            expected = observe_containment_receipt(payload, process=o.startup.process)
            self.containment_capture = (payload, expected, None, None)
            receipt = self._containment.write(payload, deadline=deadline)
            self.containment_capture = (payload, expected, receipt, None)
            self.assert_current(deadline=deadline)
            observed = self._containment.observation
            self.containment_capture = (payload, expected, receipt, observed)
            self.assert_current(deadline=deadline)
            validate_original_record((receipt, observed))
            if (
                receipt != expected
                or type(observed) is not SqlClientObserveContainmentObservation
                or observed.receipt != receipt
            ):
                self._reject()
            self._containment_snapshot = deepcopy(self.containment_capture)
            self.assert_current(deadline=deadline)
            self.local_ack = self._directory_ack(
                TdsLocalContainment(
                    value.attempt_sha256,
                    o.identity.operation_id,
                    process_identity_digest(o.startup.process),
                    receipt.payload_sha256,
                ),
                deadline,
            )
        return self.local_ack

    def bind_plan(self, plan: SqlClientObserveDeparturePlan) -> None:
        with self._effect(self.deadline):
            if (
                self.plan is not None
                or type(plan) is not SqlClientObserveDeparturePlan
                or self.local_ack is None
                or self.containment_capture is None
            ):
                self._reject()
            validate_original_record(plan)
            o = self.origin
            preparation = o.evidence_receipt
            assert preparation is not None
            resolution = o.management_incarnation.authority.principal_resolution
            if plan != replace(
                plan,
                helper_id=self.helper_id,
                attempt=o.parent.state.identity,
                ownership=o.parent.state.ownership,
                observe_operation=o.identity,
                observe_process=o.startup.process,
                original_registration_artifact_sha256=o.original_evidence[1][1].payload_sha256,
                original_authority_artifact_sha256=o.original_evidence[2][1].payload_sha256,
                original_authority_sha256=authority_digest(o.authority),
                original_containment_artifact_sha256=self.containment_capture[2].payload_sha256,
                preparation_artifact_sha256=preparation.payload_sha256,
                original=o.authority.session,
                database=o.authority.database,
                management_admission=o.management_admission,
                operation_deadline=self.deadline,
                principal=SqlClientDatabasePrincipal(resolution.principal_id, resolution.name, resolution.sid),
            ):
                self._reject()
            self.plan = plan
            self._helper.bind_plan(plan)

    def acknowledge_settlement(self, *, deadline: float) -> TdsDirectorySnapshot:
        with self._effect(deadline):
            if self.remote_ack is not None or self.local_ack is None:
                self._reject()
            self._helper.validate_complete()
            result = self.result
            assert result is not None
            self.remote_ack = self._directory_ack(
                TdsRemoteSettlement(
                    attempt_identity_digest(self.origin.identity.parent),
                    self.origin.identity.operation_id,
                    session_authority_digest(result.departure.observer.authority).hex(),
                    self.helper_records[Kind.EXCLUSION][2].payload_sha256,
                ),
                deadline,
            )
        return self.remote_ack

    def close_containment(self, *, deadline: float) -> None:
        """Bounded rewait of the same actor, never replay an ambiguous raw close."""
        if self._containment_closing or self._containment is not self._original_containment:
            self._close_reentered = True
            self._reject()
        if self._containment_closed or self._containment is None:
            return
        if self._cleanup_deadline is None:
            self._cleanup_deadline = (deadline,)
        self._containment_closing = True
        try:
            deadline_nanoseconds(self._cleanup_deadline[0])
            deadline_nanoseconds(deadline)
            self._containment.close(deadline=min(deadline, self._cleanup_deadline[0]))
            if self._close_reentered:
                self._reject()
            self._containment_closed = True
        except BaseException:
            self._reject()
        finally:
            self._containment_closing = False

    def finish(self, *, deadline: float) -> None:
        with self._effect(deadline):
            self._helper.validate_complete()
            if self.remote_ack is None or not self._containment_closed or self._provisional:
                self._reject()
            self._provisional = True
