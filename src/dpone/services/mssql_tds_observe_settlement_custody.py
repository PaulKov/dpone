"""Helper custody capture and directory acknowledgement for observe settlement."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from dpone.ports.mssql_tds_directory import RecordDirectoryContainment, RecordDirectorySettlement
from dpone.services.mssql_sqlclient_observe_helper_evidence import HelperCustodyFacts
from dpone.services.mssql_tds_writer_contracts import (
    SqlClientDepartureEvidenceReceipt,
    SqlClientDepartureEvidenceRecord,
    SqlClientObserveDepartureRequest,
    SqlClientObserveDepartureResult,
    TdsChildExit,
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
    record_local_containment,
    record_remote_settlement,
    validate_original_record,
)


class ObserveSettlementCustodyMixin:
    """Delegate custody captures and bind their directory acknowledgements."""

    def bind_helper_custody(self: Any, facts: HelperCustodyFacts) -> None:
        if self.used:
            self._reject()
        self._helper.bind_custody(facts)

    def capture_helper_child(self: Any) -> None:
        with self._effect(self.deadline, capture=True):
            self._helper.capture_child()

    def capture_helper_process(self: Any) -> None:
        with self._effect(self.deadline, capture=True):
            self._helper.capture_process()

    def capture_helper_startup(self: Any) -> None:
        with self._effect(self.deadline, capture=True):
            self._helper.capture_startup()

    def capture_helper_raw_result(self: Any) -> None:
        with self._effect(self.deadline, capture=True):
            self._helper.capture_raw_result()

    def capture_helper_request(self: Any, request: SqlClientObserveDepartureRequest) -> None:
        with self._effect(self.deadline, capture=True):
            self._helper.capture_request(request)

    def capture_helper_result(self: Any, result: SqlClientObserveDepartureResult) -> None:
        with self._effect(self.deadline, capture=True):
            self._helper.capture_result(result)

    def capture_helper_exit(self: Any, exit: TdsChildExit) -> None:
        with self._effect(self.deadline, capture=True):
            self._helper.capture_exit(exit)

    def write_helper_record(
        self: Any, record: SqlClientDepartureEvidenceRecord, *, deadline: float
    ) -> SqlClientDepartureEvidenceReceipt:
        with self._effect(deadline):
            self._helper.write_attempt(record, deadline=deadline)
            self.assert_current(deadline=deadline)
            self._helper.capture_observation()
            self.assert_current(deadline=deadline)
            receipt = self._helper.validate_ack()
        return receipt

    def _directory_ack(
        self: Any, proof: TdsLocalContainment | TdsRemoteSettlement, deadline: float
    ) -> TdsDirectorySnapshot:
        index = self.origin.identity.slot_index
        previous = self.directory
        assert previous is not None
        if isinstance(proof, TdsLocalContainment):
            predicted = record_local_containment(previous.state, index, proof)
            observed = self.attempt._directory.execute(RecordDirectoryContainment(index, proof), deadline=deadline)
            self.local_return = observed
        else:
            predicted = record_remote_settlement(previous.state, index, proof)
            observed = self.attempt._directory.execute(RecordDirectorySettlement(index, proof), deadline=deadline)
            self.remote_return = observed
        self.attempt._require_unpoisoned()
        validate_original_record(observed)
        if (
            type(observed) is not TdsDirectorySnapshot
            or observed.state != predicted
            or observed.ownership != previous.ownership
            or observed.revision <= previous.revision
            or self.attempt._directory.observation.snapshot != observed
        ):
            self._reject()
        self.directory = deepcopy(observed)
        return observed
