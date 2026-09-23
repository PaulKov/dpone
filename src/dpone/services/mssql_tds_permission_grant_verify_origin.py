"""Exact P8 settlement authority for one P9 VERIFY reservation."""

import json
import math
from copy import deepcopy
from time import monotonic
from typing import Any, NoReturn, cast

from dpone.services.mssql_tds_restricted_writer_verify_origin import RestrictedWriterVerifyOrigin


class PermissionGrantVerifyOrigin(RestrictedWriterVerifyOrigin):
    """Keep VERIFY effects on the original permission-grant association."""

    _host: Any
    _operations: Any
    _remote_settlement_ref: Any

    def _verify_unknown(self) -> NoReturn:
        raise NotImplementedError

    def _verify_parent_digest(self, parent: object) -> str:
        raise NotImplementedError

    def bind_restricted_writer_verify(self, settled: object, operations: object, *, deadline: float):
        """Bind P9 only to the exact terminal P8b settlement object."""
        if getattr(self, "_verify_phase", None) is not None:
            self._verify_unknown()
        self._verify_phase = "binding"
        try:
            if type(deadline) is not float or not math.isfinite(deadline) or monotonic() >= deadline:
                raise ValueError
            owner = cast(Any, getattr(self, "_remote_settlement_owner", None))
            exact_settled = cast(Any, settled)
            host = self._host
            prepared = host._prepared_origin
            owner.integrity()
            held = owner.local.held_owner
            grant_evidence = held.result
            grant_request = grant_evidence.request
            payload = prepared.preparation_payload
            if (
                host._permission_grant_owner is not self
                or getattr(self, "_settled_capability", None) is not settled
                or owner._settled_ref is not settled
                or owner._unknown
                or owner._busy
                or owner.phase != "SETTLED"
                or held.result is not held._result_ref
                or held.result.request is not grant_request
                or type(payload) is not bytes
            ):
                raise ValueError
            current_parent, current_directory = host._permission_grant_call(self, None, None, deadline=deadline)
            if (
                exact_settled.directory is not self._remote_settlement_ref
                or current_directory is not exact_settled.directory
            ):
                raise ValueError
            prepared.validate_origin()
            body = json.loads(payload)
            effective = body["profile_open"]["PREP_EFFECTIVE"]
            if type(effective) is not list or len(effective) != 1 or type(effective[0]) is not dict:
                raise ValueError
            self._verify_operations = cast(Any, operations)
            self._verify_settled_ref = settled
            self._verify_owner_ref = owner
            self._verify_parent_ref = current_parent
            self._verify_parent_value = deepcopy(current_parent)
            self._verify_directory_ref = current_directory
            self._verify_grant_ref = grant_evidence
            self._verify_grant_request_ref = grant_request
            self._verify_payload_ref = payload
            self._verify_effective = deepcopy(effective[0])
            self._verify_deadline = deadline
            self._verify_phase = "bound"
            return self
        except BaseException:
            self._verify_unknown()

    def _assert_verify_origin(self, expected_directory: object, *, deadline: float, phase: str) -> None:
        if getattr(self, "_verify_phase", None) != phase:
            self._verify_unknown()
        try:
            if (
                type(deadline) is not float
                or not math.isfinite(deadline)
                or monotonic() >= deadline
                or deadline > self._verify_deadline
            ):
                raise ValueError
            owner = cast(Any, self._verify_owner_ref)
            owner.integrity()
            parent, directory = self._host._permission_grant_call(self, None, None, deadline=deadline)
            if (
                self._host._permission_grant_owner is not self
                or getattr(self, "_settled_capability", None) is not self._verify_settled_ref
                or owner._settled_ref is not self._verify_settled_ref
                or owner._unknown
                or owner._busy
                or owner.phase != "SETTLED"
                or parent is not self._verify_parent_ref
                or parent != self._verify_parent_value
                or directory is not expected_directory
                or self._host._directory.observation.snapshot is not directory
                or self._verify_grant_ref is not owner.local.held_owner.result
                or self._verify_grant_request_ref is not owner.local.held_owner.result.request
                or self._verify_payload_ref is not self._host._prepared_origin.preparation_payload
            ):
                raise ValueError
        except BaseException:
            self._verify_unknown()

    def reserve_verify(self, request: object, *, request_sha256: str, deadline: float) -> object:
        """Append VERIFY exactly once; a lost acknowledgement is never replayed."""
        if getattr(self, "_verify_phase", None) != "bound":
            self._verify_unknown()
        self._verify_phase = "validating"
        try:
            if (
                type(deadline) is not float
                or not math.isfinite(deadline)
                or monotonic() >= deadline
                or deadline > self._verify_deadline
            ):
                raise ValueError
            exact_request = cast(Any, request)
            self._verify_operations.validate_origin_request(
                request, self._verify_grant_request_ref, self._verify_effective
            )
            if self._verify_phase != "validating":
                raise ValueError
            encoded = self._verify_operations.encode_request(request)
            if self._verify_phase != "validating":
                raise ValueError
            expected_sha256 = self._verify_operations.request_digest(encoded)
            if (
                self._verify_phase != "validating"
                or type(request_sha256) is not str
                or expected_sha256 != request_sha256
            ):
                raise ValueError
            self._assert_verify_origin(self._verify_directory_ref, deadline=deadline, phase="validating")
            if self._verify_phase != "validating":
                raise ValueError
            identity_factory, reserve, request_factory = self._operations[2:5]
            validate_original, contract_error, validate_binding = (
                self._operations[5],
                self._operations[7],
                self._operations[8],
            )
            parent, previous = self._verify_parent_ref, self._verify_directory_ref
            verify_command = type(self._operations[0]).VERIFY
            identity = identity_factory(
                parent.state.identity,
                len(previous.state.slots),
                exact_request.operation_id,
                verify_command,
                request_sha256,
                previous.ownership.fence,
                exact_request.implementation_sha256,
            )
            predicted = reserve(
                previous.state,
                operation_id=exact_request.operation_id,
                command=verify_command,
                command_sha256=request_sha256,
                owner_fence=previous.ownership.fence,
            )
            command = request_factory(exact_request.operation_id, verify_command, request_sha256)
            validate_original((identity, predicted, previous))
            self._verify_phase = "reservation_attempted"
            returned = self._host._permission_grant_call(self, command, None, deadline=deadline)
            self._verify_directory_ref = returned
            validate_binding(parent, returned, identity)
            if (
                returned.state != predicted
                or returned.ownership != previous.ownership
                or returned.revision <= previous.revision
                or self._host._directory.observation.snapshot is not returned
            ):
                raise contract_error("mssql_native.tds_restricted_writer_verify_ack_invalid")
            reservation = self._verify_operations.origin_reservation(
                exact_request.operation_id,
                request_sha256,
                self._verify_parent_digest(parent.state.identity),
                returned.ownership,
            )
            self._verify_identity = identity
            self._verify_request_ref = request
            self._verify_request_sha256 = request_sha256
            self._verify_reservation = reservation
            self._verify_reservation_value = deepcopy(reservation)
            self._verify_phase = "ready"
            self.assert_verify_reservation(reservation, request, deadline=deadline)
            return reservation
        except BaseException:
            self._verify_unknown()

    def assert_verify_reservation(self, reservation: object, request: object, *, deadline: float) -> None:
        phase = getattr(self, "_verify_phase", None)
        if phase not in ("ready", "settling", "local_settled", "settled"):
            self._verify_unknown()
        try:
            exact_request, exact_reservation = cast(Any, request), cast(Any, reservation)
            if (
                reservation is not self._verify_reservation
                or request is not self._verify_request_ref
                or reservation != self._verify_reservation_value
                or exact_reservation.operation_id != exact_request.operation_id
                or exact_reservation.request_sha256 != self._verify_request_sha256
                or exact_reservation.execution_owner is not self._verify_directory_ref.ownership
            ):
                raise ValueError
            self._verify_operations.validate_origin_request(
                request, self._verify_grant_request_ref, self._verify_effective
            )
            if self._verify_phase != phase:
                raise ValueError
            self._assert_verify_origin(self._verify_directory_ref, deadline=deadline, phase=phase)
        except BaseException:
            self._verify_unknown()

    def assert_verify_settlement(self, retained: object, *, deadline: float) -> None:
        """Latch and guard the sole exact P9a owner without reconstructing it."""
        phase = getattr(self, "_verify_phase", None)
        if phase not in ("ready", "settling", "local_settled", "settled"):
            self._verify_unknown()
        try:
            exact = cast(Any, retained)
            owner = exact._owner
            if phase == "ready":
                if (
                    owner._association is not self
                    or owner._request is not self._verify_request_ref
                    or owner._reservation is not self._verify_reservation
                    or owner._result is None
                    or owner._phase != "complete"
                ):
                    raise ValueError
                self._verify_retained_ref = retained
                self._verify_local_owner_ref = owner
                self._verify_result_ref = owner._result
                self._verify_receipt_refs = tuple(owner._receipts)
                self._verify_phase = phase = "settling"
            if (
                retained is not self._verify_retained_ref
                or owner is not self._verify_local_owner_ref
                or owner._association is not self
                or owner._request is not self._verify_request_ref
                or owner._reservation is not self._verify_reservation
                or owner._result is not self._verify_result_ref
                or len(owner._receipts) != len(self._verify_receipt_refs)
                or any(
                    value is not reference
                    for value, reference in zip(owner._receipts, self._verify_receipt_refs, strict=True)
                )
            ):
                raise ValueError
            self._assert_verify_origin(self._verify_directory_ref, deadline=deadline, phase=phase)
        except BaseException:
            self._verify_unknown()

    def _verify_settlement_ack(
        self, request: object, predicted: object, previous: object, attempted: str, deadline: float
    ):
        self._verify_phase = attempted
        try:
            returned = self._host._permission_grant_call(self, request, None, deadline=deadline)
            if (
                returned.state != predicted
                or returned.ownership != cast(Any, previous).ownership
                or returned.revision <= cast(Any, previous).revision
                or self._host._directory.observation.snapshot is not returned
            ):
                raise ValueError
            self._verify_directory_ref = returned
            return returned
        except BaseException:
            self._verify_unknown()

    def record_verify_local_containment(self, proof: object, *, deadline: float) -> object:
        """Record the exact VERIFY slot's local containment once."""
        if getattr(self, "_verify_phase", None) != "settling":
            self._verify_unknown()
        try:
            directory_contract, directory_ports = self._operations[9]
            local_type = directory_contract.TdsLocalContainment
            exact = cast(Any, proof)
            self.assert_verify_settlement(self._verify_retained_ref, deadline=deadline)
            if (
                type(proof) is not local_type
                or exact.parent_sha256 != self._verify_parent_digest(self._verify_identity.parent)
                or exact.operation_id != self._verify_identity.operation_id
            ):
                raise ValueError
            current = self._verify_directory_ref
            predicted = directory_contract.record_local_containment(
                current.state, self._verify_identity.slot_index, proof
            )
            returned = self._verify_settlement_ack(
                directory_ports.RecordDirectoryContainment(self._verify_identity.slot_index, proof),
                predicted,
                current,
                "local_attempted",
                deadline,
            )
            self._verify_local_directory_ref = returned
            self._verify_local_directory_value = deepcopy(returned)
            self._verify_phase = "local_settled"
            self.assert_verify_settlement(self._verify_retained_ref, deadline=deadline)
            return returned
        except BaseException:
            self._verify_unknown()

    def record_verify_remote_settlement(self, proof: object, *, deadline: float) -> object:
        """Record remote settlement only after the exact VERIFY local proof."""
        if getattr(self, "_verify_phase", None) != "local_settled":
            self._verify_unknown()
        try:
            directory_contract, directory_ports = self._operations[9]
            remote_type = directory_contract.TdsRemoteSettlement
            exact = cast(Any, proof)
            self.assert_verify_settlement(self._verify_retained_ref, deadline=deadline)
            current = self._verify_directory_ref
            if (
                type(proof) is not remote_type
                or current is not self._verify_local_directory_ref
                or current != self._verify_local_directory_value
                or exact.parent_sha256 != self._verify_parent_digest(self._verify_identity.parent)
                or exact.operation_id != self._verify_identity.operation_id
            ):
                raise ValueError
            predicted = directory_contract.record_remote_settlement(
                current.state, self._verify_identity.slot_index, proof
            )
            returned = self._verify_settlement_ack(
                directory_ports.RecordDirectorySettlement(self._verify_identity.slot_index, proof),
                predicted,
                current,
                "remote_attempted",
                deadline,
            )
            self._verify_remote_directory_ref = returned
            self._verify_remote_directory_value = deepcopy(returned)
            self._verify_phase = "settled"
            self.assert_verify_settlement(self._verify_retained_ref, deadline=deadline)
            return returned
        except BaseException:
            self._verify_unknown()
