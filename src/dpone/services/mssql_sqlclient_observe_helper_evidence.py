"""Finite helper producer capture and six ACKs, without parent or close authority.

Only the original settlement orchestrates the three write stages around its
journal guards. Actual returns survive observation/guard failure; no caller can
supply a receipt or repair an uncertain attempt with a later matching read.
"""

import os
from collections.abc import Mapping
from copy import deepcopy
from threading import current_thread
from types import MappingProxyType
from typing import NoReturn

from dpone.ports.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceGateway
from dpone.services.mssql_sqlclient_observe_helper_custody import (
    HelperCustodyFacts as HelperCustodyFacts,
)
from dpone.services.mssql_sqlclient_observe_helper_custody import (
    ObserveHelperOperationMixin,
)
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientDepartureEvidenceKind as Kind,
)
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientDepartureEvidenceObservation,
    SqlClientDepartureEvidenceReceipt,
    SqlClientDepartureEvidenceRecord,
    SqlClientObserveDeparturePlan,
    SqlClientObserveDepartureRequest,
    SqlClientObserveDepartureResult,
    TdsChildExit,
    TdsCoordinatorStartup,
    TdsProcessIdentity,
    WindowOutcomeUnknown,
    decode_observe_departure_evidence,
    decode_observe_departure_result,
    validate_observe_exclusion_chain,
    validate_original_record,
)

ERROR = "mssql_native.sqlclient_observe_helper_evidence_unknown"


class ObserveHelperEvidence(ObserveHelperOperationMixin):
    """One original helper's immutable producer observations and attempted writes."""

    def __init__(self) -> None:
        self._pid, self._thread = os.getpid(), current_thread()
        self.failed = self._busy = self._observation_attempted = False
        self._plan: SqlClientObserveDeparturePlan | None = None
        self._plan_capture: tuple | None = None
        self.request: SqlClientObserveDepartureRequest | None = None
        self.result: SqlClientObserveDepartureResult | None = None
        self.local_exit: TdsChildExit | None = None
        self._request_capture: tuple | None = None
        self._result_capture: tuple | None = None
        self._exit_capture: tuple | None = None
        self._gateway: SqlClientDepartureEvidenceGateway | None = None
        self._original_gateway: SqlClientDepartureEvidenceGateway | None = None
        self._records: dict[Kind, tuple] = {}
        self._pending: SqlClientDepartureEvidenceRecord | None = None
        self._facts: HelperCustodyFacts | None = None
        self._original_facts: HelperCustodyFacts | None = None
        self._child_capture: tuple | None = None
        self._process_capture: tuple | None = None
        self._startup_capture: tuple | None = None
        self._raw_capture: tuple | None = None
        self._gateway_bound = False

    @property
    def records(self) -> Mapping[Kind, tuple]:
        return MappingProxyType(self._records)

    def _reject(self) -> NoReturn:
        self.failed = True
        raise WindowOutcomeUnknown(ERROR)

    def bind_custody(self, facts: HelperCustodyFacts) -> None:
        with self._operation():
            if self._facts is not None:
                self._reject()
            self._facts = self._original_facts = facts

    def capture_child(self) -> None:
        with self._operation():
            if self._facts is None or self._child_capture is not None:
                self._reject()
            self._child_capture = (self._facts.child,)
            if self._child_capture[0] is None:
                self._reject()

    def capture_process(self) -> None:
        with self._operation():
            if self._facts is None or self._process_capture is not None:
                self._reject()
            value = self._facts.process
            self._process_capture = (value, deepcopy(value))
            if type(value) is not TdsProcessIdentity or self._child_capture is None:
                self._reject()

    def capture_startup(self) -> None:
        with self._operation():
            if self._facts is None or self._startup_capture is not None:
                self._reject()
            value = self._facts.startup
            self._startup_capture = (value, deepcopy(value))
            if type(value) is not TdsCoordinatorStartup or self._process_capture is None:
                self._reject()

    def capture_raw_result(self) -> None:
        with self._operation():
            if self._facts is None:
                self._reject()
            raw = self._facts.raw_result
            if self._raw_capture is not None:
                if raw is not self._raw_capture[0]:
                    self._reject()
                return
            self._raw_capture = (raw,)

    def _validate_facts(self, *, final: bool = False) -> None:
        facts = self._facts
        if facts is None or facts is not self._original_facts:
            self._reject()
        if (
            facts.child is not (self._child_capture[0] if self._child_capture else None)
            or facts.unresolved_launch is not None
            or facts.helper_evidence is not self._gateway
        ):
            self._reject()
        for value, capture in ((facts.process, self._process_capture), (facts.startup, self._startup_capture)):
            validate_original_record((value, capture))
            if (capture is None and value is not None) or (
                capture is not None and (value is not capture[0] or value != capture[1])
            ):
                self._reject()
        startup = facts.startup
        if startup is not None and startup.process != facts.process:
            self._reject()
        if self.request is not None and self.request.startup is not startup:
            self._reject()
        if facts.local_exit is not self.local_exit or facts.raw_result is not (
            self._raw_capture[0] if self._raw_capture else None
        ):
            self._reject()
        if self.result is not None:
            raw, request, plan = facts.raw_result, self.request, self._plan
            if (
                type(raw) is not bytes
                or request is None
                or plan is None
                or decode_observe_departure_result(raw, request=request, observer_admission=plan.observer_admission)
                != self.result
            ):
                self._reject()
        if (Kind.LOCAL_EXIT in self._records and facts.child_closed is not True) or (
            final
            and (
                self._child_capture is None
                or self._gateway is None
                or facts.child_closed is not True
                or facts.helper_evidence_closed is not True
            )
        ):
            self._reject()

    def bind_plan(self, plan: SqlClientObserveDeparturePlan) -> None:
        with self._operation():
            if self._plan is not None or type(plan) is not SqlClientObserveDeparturePlan:
                self._reject()
            self._plan = plan
            self._plan_capture = (plan, deepcopy(plan))
            self.validate_current(plan)

    def bind_gateway(self, gateway: SqlClientDepartureEvidenceGateway | None) -> None:
        with self._operation():
            if self._gateway_bound:
                self._reject()
            self._gateway_bound = True
            self._gateway = self._original_gateway = gateway

    def validate_current(self, plan: SqlClientObserveDeparturePlan | None, *, final: bool = False) -> None:
        """Pure phase-aware inspection, valid while a raw ACK is still pending."""
        try:
            if (
                self.failed
                or os.getpid() != self._pid
                or current_thread() is not self._thread
                or self._gateway is not self._original_gateway
                or plan is not self._plan
            ):
                self._reject()
            for value, captured in (
                (plan, self._plan_capture),
                (self.request, self._request_capture),
                (self.result, self._result_capture),
                (self.local_exit, self._exit_capture),
            ):
                validate_original_record((value, captured))
                if (captured is None and value is not None) or (
                    captured is not None and (value is not captured[0] or value != captured[1])
                ):
                    self._reject()
            if self.request is not None:
                capture = self._request_capture
                if (
                    capture is None
                    or self.request.plan is not plan
                    or self.request.startup is not capture[2]
                    or self.request.startup.process is not capture[3]
                ):
                    self._reject()
            for kind, entry in self._records.items():
                record, expected, receipt, observed, snapshot = entry
                validate_original_record(entry)
                if (
                    type(record) is not SqlClientDepartureEvidenceRecord
                    or record.kind is not kind
                    or record.receipt != expected
                ):
                    self._reject()
                if snapshot is not None and ((record, receipt, observed) != snapshot or receipt != expected):
                    self._reject()
                if self._pending is not record and snapshot is None:
                    self._reject()
            self._validate_facts(final=final)
        except BaseException:
            self.failed = True
            raise

    def capture_request(self, request: SqlClientObserveDepartureRequest) -> None:
        with self._operation():
            if self._request_capture is not None or type(request) is not SqlClientObserveDepartureRequest:
                self._reject()
            self.request = request
            self._request_capture = (request, deepcopy(request), request.startup, request.startup.process)
            self._require_phase(1)
            self.validate_current(self._plan)

    def capture_result(self, result: SqlClientObserveDepartureResult) -> None:
        with self._operation():
            if self._result_capture is not None or type(result) is not SqlClientObserveDepartureResult:
                self._reject()
            self.result = result
            self._result_capture = (result, deepcopy(result))
            self._require_phase(3)
            self.validate_current(self._plan)

    def capture_exit(self, exit: TdsChildExit) -> None:
        with self._operation():
            if self._exit_capture is not None or type(exit) is not TdsChildExit:
                self._reject()
            self.local_exit = exit
            self._exit_capture = (exit, deepcopy(exit))
            self._require_phase(4)
            self.validate_current(self._plan)
            if (
                self.request is None
                or exit.identity != self.request.startup.process
                or exit.reaped is not True
                or exit.exit_code != 0
            ):
                self._reject()

    def _require_phase(self, count: int) -> None:
        if tuple(self._records) != tuple(Kind)[:count] or any(entry[4] is None for entry in self._records.values()):
            self._reject()

    def _validate_payload(self, record: SqlClientDepartureEvidenceRecord) -> None:
        plan, request, kind = self._plan, self.request, record.kind
        if plan is None or record.observer_admission is not plan.observer_admission:
            self._reject()
        if kind is Kind.LAUNCH_INTENT:
            if record.observe_plan is not plan:
                self._reject()
            return
        if request is None or record.observe_request is not request:
            self._reject()
        value = decode_observe_departure_evidence(
            record.payload, kind=kind, request=request, observer_admission=plan.observer_admission
        )
        if (
            kind is Kind.REGISTRATION
            and value.launch_intent_sha256 != self._records[Kind.LAUNCH_INTENT][2].payload_sha256
        ):
            self._reject()
        if (
            kind is Kind.CREDENTIAL_INTENT
            and value.registration_sha256 != self._records[Kind.REGISTRATION][2].payload_sha256
        ):
            self._reject()
        if kind is Kind.RESULT and (
            self.result is None
            or value.result != self.result
            or value.credential_intent_sha256 != self._records[Kind.CREDENTIAL_INTENT][2].payload_sha256
        ):
            self._reject()
        if kind is Kind.LOCAL_EXIT and (
            self.local_exit is None
            or value.exit != self.local_exit
            or value.result_sha256 != self._records[Kind.RESULT][2].payload_sha256
            or value.registration_sha256 != self._records[Kind.REGISTRATION][2].payload_sha256
        ):
            self._reject()
        if kind is Kind.EXCLUSION:
            validate_observe_exclusion_chain(
                value,
                request=request,
                observer_admission=plan.observer_admission,
                payloads=tuple(self._records[k][0].payload for k in tuple(Kind)[:5]),
            )

    def write_attempt(self, record: SqlClientDepartureEvidenceRecord, *, deadline: float) -> None:
        """Capture original raw write return before any observation or owner guard."""
        with self._operation():
            self.validate_current(self._plan)
            if (
                type(record) is not SqlClientDepartureEvidenceRecord
                or self._gateway is None
                or self._pending is not None
            ):
                self._reject()
            if type(record.kind) is not Kind:
                self._reject()
            self._require_phase(list(Kind).index(record.kind))
            self._validate_payload(record)
            expected = record.receipt
            self._pending = record
            self._observation_attempted = False
            self._records[record.kind] = (record, expected, None, None, None)
            receipt = self._gateway.write(record, deadline=deadline)
            self._records[record.kind] = (record, expected, receipt, None, None)

    def capture_observation(self) -> None:
        """No supplied receipt/observation; an unsuccessful read consumes this stage."""
        with self._operation():
            self.validate_current(self._plan)
            record = self._pending
            if record is None or self._gateway is None or self._observation_attempted:
                self._reject()
            self._observation_attempted = True
            entry = self._records[record.kind]
            observed = self._gateway.observation
            self._records[record.kind] = (*entry[:3], observed, None)

    def validate_ack(self) -> SqlClientDepartureEvidenceReceipt:
        with self._operation():
            self.validate_current(self._plan)
            record = self._pending
            if record is None or not self._observation_attempted:
                self._reject()
            _, expected, receipt, observed, _ = self._records[record.kind]
            if (
                type(receipt) is not SqlClientDepartureEvidenceReceipt
                or type(observed) is not SqlClientDepartureEvidenceObservation
                or receipt != expected
                or observed != SqlClientDepartureEvidenceObservation(record.helper_id, record.attempt_sha256, receipt)
            ):
                self._reject()
            self._validate_payload(record)
            self._records[record.kind] = (record, expected, receipt, observed, deepcopy((record, receipt, observed)))
            self._pending = None
            return receipt

    def validate_complete(self) -> None:
        """Validate six actual ACKs and captured producers, never release authority."""
        with self._operation():
            self.validate_current(self._plan)
            self._require_phase(6)
            if self.request is None or self.result is None or self.local_exit is None:
                self._reject()
            for record, *_ in self._records.values():
                self._validate_payload(record)
