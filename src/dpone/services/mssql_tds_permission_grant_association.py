from copy import deepcopy
from time import monotonic
from typing import Any, NoReturn, cast
from uuid import UUID

from dpone.contracts.mssql_tds_directory import (
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
    record_local_containment,
    record_remote_settlement,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest
from dpone.ports.mssql_tds_directory import RecordDirectoryContainment, RecordDirectorySettlement
from dpone.services.mssql_tds_permission_grant_verify_origin import PermissionGrantVerifyOrigin


class PermissionGrantAssociation(PermissionGrantVerifyOrigin):
    def __init__(self, host, operation_id, hashes, deadline, operations):
        if type(operation_id) is not UUID or type(operation_id.int) is not int or not operation_id.int:
            raise ValueError("mssql_native.tds_permission_grant_operation_invalid")
        for value in hashes:
            if type(value) is not str or len(value) != 64 or any(c not in "0123456789abcdef" for c in value):
                raise ValueError("mssql_native.tds_permission_grant_digest_invalid")
        if type(deadline) is not float or not deadline > monotonic():
            raise ValueError("mssql_native.tds_permission_grant_deadline_invalid")
        self._host, self._operation_id, self._hashes, self._deadline = host, operation_id, hashes, deadline
        self._operations, self._phase = operations, "new"

    def _capture_originals(self, current_directory=None) -> tuple:
        prepared, validate_original, contract_error = self._operations[1], self._operations[5], self._operations[7]
        h, o, s = self._host, self._host._prepared_origin, self._host._observe_settlement
        parent, directory = h._lifecycle.snapshot, h._directory.observation.snapshot
        validate_original((o.expected, s.directory, s.local_ack, s.remote_ack, parent, directory))
        o.validate_origin()
        references, flags = getattr(s, "_references", ()), (o.failed, o.cleaned, s.complete, s.pending, s.failed)
        exact_references = (h, o, o.factory, o.pool, h._lifecycle, h._directory)
        if (
            any(
                a is not b
                for a, b in ((o.attempt, h), (s.attempt, h), (s.origin, o), (h._composition_origin, o.factory))
            )
            or any(a is not b for a, b in ((o.factory, o.handle.factory), (o.pool, o.handle.pool)))
            or len(references) != len(exact_references)
            or any(a is not b for a, b in zip(exact_references, references, strict=True))
            or flags != (False, True, True, False, False)
            or None in (s.local_ack, s.remote_ack)
            or s.local_return is not s.local_ack
            or s.remote_return is not s.remote_ack
            or parent != o.expected
            or parent.state.phase is not prepared
            or directory != (s.directory if current_directory is None else current_directory)
            or (current_directory is None and directory != s.remote_ack)
            or self._hashes[1] != o.identity.implementation_sha256
            or not 0 < self._deadline <= min(o.deadline, s.deadline)
            or monotonic() >= self._deadline
        ):
            raise contract_error("mssql_native.tds_permission_grant_origin_changed")
        return (h, o, s, o.factory, o.pool, h._lifecycle, h._directory, deepcopy(parent), deepcopy(s.directory))

    def _assert_originals(self, parent, directory, current_directory=None) -> None:
        validate_original, contract_error = self._operations[5], self._operations[7]
        capture = self._capture
        current = self._capture_originals(current_directory)
        validate_original((capture[7:], current[7:], parent, directory))
        if any(a is not b for a, b in zip(current[:7], capture[:7], strict=True)) or current[7:] != capture[7:]:
            raise contract_error("mssql_native.tds_permission_grant_origin_changed")
        if parent != capture[7] or directory != (capture[8] if current_directory is None else current_directory):
            raise contract_error("mssql_native.tds_permission_grant_origin_changed")

    def _register(self) -> None:
        if self._phase != "new" or self._host._permission_grant_owner is not self:
            self._unknown()
        self._phase = "registered"
        try:
            self._capture = self._capture_originals()
        except BaseException:
            self._unknown()

    def _unknown(self) -> NoReturn:
        self._phase = "unknown"
        raise self._host._permission_grant_unknown()

    def _verify_unknown(self) -> NoReturn:
        self._verify_phase = "unknown"
        raise self._host._permission_grant_unknown()

    def _verify_parent_digest(self, parent: object) -> str:
        return attempt_identity_digest(cast(Any, parent))

    def _guard(self) -> None:
        if self._host._lifecycle is not self._capture[5] or self._host._directory is not self._capture[6]:
            self._unknown()

    def _call(self, request, pool):
        self._guard()
        return self._host._permission_grant_call(self, request, pool, deadline=self._deadline)

    def reserve(self) -> "PermissionGrantAssociation":
        if self._phase != "registered":
            self._unknown()
        try:
            identity_factory, reserve, request_factory = self._operations[2:5]
            validate_original, validate_binding, contract_error = self._operations[5:8]
            parent, previous = self._call(None, None)
            self._assert_originals(parent, previous)
            identity = identity_factory(
                parent.state.identity,
                len(previous.state.slots),
                self._operation_id,
                self._operations[0],
                self._hashes[0],
                previous.ownership.fence,
                self._hashes[1],
            )
            predicted = reserve(
                previous.state,
                operation_id=self._operation_id,
                command=self._operations[0],
                command_sha256=self._hashes[0],
                owner_fence=previous.ownership.fence,
            )
            request = request_factory(self._operation_id, self._operations[0], self._hashes[0])
            validate_original((identity, predicted, previous))
            if identity.implementation_sha256 != self._hashes[1]:
                raise contract_error("mssql_native.tds_permission_grant_identity_invalid")
            self._phase = "reservation_attempted"
            returned = self._call(request, None)
            self._identity, self._reservation, self._phase = identity, returned, "ack_returned"
            validate_original(returned)
            self._guard()
            if (
                returned.state != predicted
                or returned.ownership != previous.ownership
                or returned.revision <= previous.revision
                or self._host._directory.observation.snapshot != returned
            ):
                raise contract_error("mssql_native.tds_permission_grant_ack_invalid")
            validate_binding(parent, returned, identity)
            current_parent, current_directory = self._call(None, self._capture[4])
            self._assert_originals(current_parent, current_directory, returned)
            self._identity_snapshot, self._reservation_snapshot = deepcopy(identity), deepcopy(returned)
            self._phase = "ready"
            return self
        except BaseException:
            self._unknown()

    def assert_ready(self) -> None:
        if self._phase != "ready":
            self._unknown()
        try:
            parent, directory = self._call(None, self._capture[4])
            self._assert_originals(parent, directory, self._reservation)
            self._operations[5](
                (self._identity, self._reservation, self._identity_snapshot, self._reservation_snapshot)
            )
            if self._identity != self._identity_snapshot or self._reservation != self._reservation_snapshot:
                raise self._operations[7]("mssql_native.tds_permission_grant_capture_changed")
        except BaseException:
            self._unknown()

    def _settlement_guard(self, phase: str, current: object) -> None:
        if self._phase != phase:
            self._unknown()
        try:
            parent, observed = self._call(None, self._capture[4])
            self._assert_originals(parent, observed, current)
            if observed is not current or self._host._directory.observation.snapshot is not observed:
                raise self._operations[7]("mssql_native.tds_permission_grant_settlement_changed")
        except BaseException:
            self._unknown()

    def _settlement_ack(self, request, predicted, previous: TdsDirectorySnapshot, attempted: str):
        self._phase = attempted
        try:
            returned = self._call(request, None)
            if (
                type(returned) is not TdsDirectorySnapshot
                or returned.state != predicted
                or returned.ownership != previous.ownership
                or returned.revision <= previous.revision
                or self._host._directory.observation.snapshot is not returned
            ):
                raise self._operations[7]("mssql_native.tds_permission_grant_settlement_ack_invalid")
            parent, observed = self._call(None, self._capture[4])
            self._assert_originals(parent, observed, returned)
            if observed is not returned:
                raise self._operations[7]("mssql_native.tds_permission_grant_settlement_ack_invalid")
            return returned
        except BaseException:
            self._unknown()

    def record_local_containment(self, proof: TdsLocalContainment) -> TdsDirectorySnapshot:
        """Record the exact GRANT slot's local proof once; ambiguity is sticky."""
        current = self._reservation
        self._settlement_guard("ready", current)
        try:
            if (
                type(proof) is not TdsLocalContainment
                or proof.parent_sha256 != attempt_identity_digest(self._identity.parent)
                or proof.operation_id != self._identity.operation_id
            ):
                raise self._operations[7]("mssql_native.tds_permission_grant_local_proof_invalid")
            predicted = record_local_containment(current.state, self._identity.slot_index, proof)
            returned = self._settlement_ack(
                RecordDirectoryContainment(self._identity.slot_index, proof), predicted, current, "local_attempted"
            )
            self._local_settlement = self._local_settlement_ref = returned
            self._local_settlement_snapshot = deepcopy(returned)
            self._phase = "local_settled"
            self.assert_locally_contained()
            return returned
        except BaseException:
            self._unknown()

    def assert_locally_contained(self) -> None:
        current = getattr(self, "_local_settlement", None)
        self._settlement_guard("local_settled", current)
        try:
            if (
                type(current) is not TdsDirectorySnapshot
                or current is not self._local_settlement_ref
                or current != self._local_settlement_snapshot
            ):
                raise self._operations[7]("mssql_native.tds_permission_grant_local_settlement_changed")
        except BaseException:
            self._unknown()

    def record_remote_settlement(self, proof: TdsRemoteSettlement) -> TdsDirectorySnapshot:
        """Record remote SQL-session settlement only after exact local containment."""
        self.assert_locally_contained()
        current = self._local_settlement
        try:
            if (
                type(proof) is not TdsRemoteSettlement
                or proof.parent_sha256 != attempt_identity_digest(self._identity.parent)
                or proof.operation_id != self._identity.operation_id
            ):
                raise self._operations[7]("mssql_native.tds_permission_grant_remote_proof_invalid")
            predicted = record_remote_settlement(current.state, self._identity.slot_index, proof)
            returned = self._settlement_ack(
                RecordDirectorySettlement(self._identity.slot_index, proof), predicted, current, "remote_attempted"
            )
            self._remote_settlement = self._remote_settlement_ref = returned
            self._remote_settlement_snapshot = deepcopy(returned)
            self._phase = "settled"
            self.assert_settled()
            return returned
        except BaseException:
            self._unknown()

    def assert_settled(self) -> None:
        current = getattr(self, "_remote_settlement", None)
        self._settlement_guard("settled", current)
        try:
            if (
                type(current) is not TdsDirectorySnapshot
                or current is not self._remote_settlement_ref
                or current != self._remote_settlement_snapshot
            ):
                raise self._operations[7]("mssql_native.tds_permission_grant_remote_settlement_changed")
        except BaseException:
            self._unknown()

    @property
    def settled_directory(self) -> TdsDirectorySnapshot:
        self.assert_settled()
        return deepcopy(self._remote_settlement)

    @property
    def identity(self):
        self.assert_ready()
        return deepcopy(self._identity)

    @property
    def reservation(self):
        self.assert_ready()
        return deepcopy(self._reservation)
