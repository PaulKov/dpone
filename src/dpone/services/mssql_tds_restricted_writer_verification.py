from __future__ import annotations

import math
from collections.abc import Callable
from copy import deepcopy
from hashlib import sha256
from typing import Any, NoReturn, cast

from dpone.ports.mssql_tds_worker import TdsLaunchUnknown, TdsUnresolvedLaunch
from dpone.services.mssql_tds_restricted_writer_verify_coordinator import (
    _RETAINED_TOKEN,
    RestrictedWriterVerifyContract,
    RestrictedWriterVerifyLocalUnknown,
    RestrictedWriterVerifyRetained,
    VerifyCoordinator,
    VerifyCoordinatorCustody,
    VerifyCredentialSupplier,
    VerifyEvidence,
    VerifyLauncher,
    VerifyLaunchInputs,
    VerifyProcess,
    discard_verify_exception,
)
from dpone.services.mssql_tds_restricted_writer_verify_origin import (
    ERROR,
    VerifyCustodySlotMixin,
)
from dpone.services.mssql_tds_restricted_writer_verify_origin import (
    RestrictedWriterVerifyOrigin as RestrictedWriterVerifyOrigin,
)


class _VerifyOwner(VerifyCustodySlotMixin):
    _custody_tampered: bool

    def __setattr__(self, name: str, value: object) -> None:
        if getattr(self, "_custody_sealed", False) and name in {
            "_coordinator_custody",
            "_coordinator_custody_ref",
        }:
            object.__setattr__(self, "_custody_tampered", True)
            raise AttributeError(ERROR)
        object.__setattr__(self, name, value)

    def __init__(self, association, evidence, launcher, launch, coordinator_factory, contract, deadline, clock) -> None:
        object.__setattr__(self, "_custody_sealed", False)
        object.__setattr__(self, "_custody_tampered", False)
        if not isinstance(association, RestrictedWriterVerifyOrigin):
            raise ValueError(ERROR)
        self._association, self._evidence, self._launcher, self._launch = association, evidence, launcher, launch
        self._contract, self._coordinator_factory = contract, coordinator_factory
        if type(deadline) is not float or not math.isfinite(deadline):
            raise ValueError(ERROR)
        self._deadline, self._clock = min(deadline, launch.operation_deadline), clock
        self._request = launch.request
        self._request_bytes = contract.encode_request(self._request)
        self._public_payload: bytes | None = None
        self._public_sha256: str | None = None
        self._references = (association, evidence, launcher, launch, coordinator_factory, self._request)
        self._values = deepcopy((launch, self._request))
        self._phase = "new"
        self._receipts: tuple[object, ...] = ()
        self._reservation: Any | None = None
        self._registration: Any | None = None
        self._process: VerifyProcess | None = None
        self._coordinator_custody: VerifyCoordinatorCustody | None = None
        self._coordinator_custody_ref: VerifyCoordinatorCustody | None = None
        self._unresolved_launch: TdsUnresolvedLaunch | None = None
        self._containment_attempted = False
        self._result: Any | None = None
        self._exit: Any | None = None
        self._cleanup_attempted = False
        object.__setattr__(self, "_custody_sealed", True)

    def _exact_custody(self) -> VerifyCoordinatorCustody:
        custody = self._coordinator_custody_ref
        if (
            self._custody_tampered
            or type(custody) is not VerifyCoordinatorCustody
            or self._coordinator_custody is not custody
        ):
            raise ValueError(ERROR)
        return custody

    def contain_unknown(self, *, deadline: float) -> None:
        launch = self._unresolved_launch
        if launch is None:
            return
        if (
            self._phase != "unknown"
            or self._containment_attempted
            or type(deadline) is not float
            or not math.isfinite(deadline)
            or self._clock() >= deadline
        ):
            raise RestrictedWriterVerifyLocalUnknown(self) from None
        self._containment_attempted = True
        failure: BaseException | None = None
        try:
            launch.contain(deadline=deadline)
            if self._unresolved_launch is not launch:
                raise ValueError(ERROR)
            launch.close()
            return
        except BaseException as error:
            failure = error
        assert failure is not None
        discard_verify_exception(failure)
        del failure
        raise RestrictedWriterVerifyLocalUnknown(self) from None

    def _unknown(self, exact_custody: VerifyCoordinatorCustody | None = None) -> NoReturn:
        self._phase = "unknown"
        process = self._process
        if process is not None and not self._cleanup_attempted:
            self._cleanup_attempted = True
            try:
                process.cleanup()
            except BaseException:
                pass
        retained_custody = _VerifyOwner._release_custody(self)
        custody = exact_custody if exact_custody is not None else retained_custody
        if custody is not None:
            try:
                custody.close()
            except BaseException:
                pass
        failure = RestrictedWriterVerifyLocalUnknown(self)
        failure.__traceback__ = failure.__cause__ = failure.__context__ = None
        raise failure from None

    def _check(self, exact_custody: VerifyCoordinatorCustody | None = None) -> None:
        if self._phase == "unknown" or self._references != (
            self._association,
            self._evidence,
            self._launcher,
            self._launch,
            self._coordinator_factory,
            self._request,
        ):
            _VerifyOwner._unknown(self, exact_custody)
        try:
            if (
                type(self._deadline) is not float
                or not math.isfinite(self._deadline)
                or self._clock() >= self._deadline
                or (self._launch, self._request) != self._values
                or self._launch.request is not self._request
                or self._contract.encode_request(self._request) != self._request_bytes
                or (
                    self._public_payload is not None and sha256(self._public_payload).hexdigest() != self._public_sha256
                )
            ):
                raise ValueError
            if self._reservation is not None:
                self._association.assert_verify_reservation(self._reservation, self._request, deadline=self._deadline)
            if exact_custody is not None:
                if self._coordinator_custody is not exact_custody or self._coordinator_custody_ref is not exact_custody:
                    raise ValueError
                exact_custody.assert_current()
            elif self._coordinator_custody_ref is not None:
                self._exact_custody().assert_current()
        except BaseException:
            _VerifyOwner._unknown(self, exact_custody)

    def _advance(self, event: object) -> None:
        self._check()
        try:
            self._exact_custody().advance(event)
        except BaseException:
            self._unknown()

    def _persist(self, kind: object, **facts: object) -> object:
        self._check()
        if len(self._receipts) >= len(self._contract.order) or kind is not self._contract.order[len(self._receipts)]:
            self._unknown()
        try:
            record = self._contract.evidence_record(self._request, kind, facts)
            expected = record.receipt
            received = self._evidence.write(record, deadline=self._deadline)
            observation = self._evidence.observation
            if (
                type(received) is not self._contract.receipt_type
                or received != expected
                or type(observation.receipts) is not tuple
                or len(observation.receipts) != len(self._receipts) + 1
                or observation.receipts[-1] is not received
                or any(
                    observed is not retained
                    for observed, retained in zip(observation.receipts[:-1], self._receipts, strict=True)
                )
            ):
                raise ValueError
            self._receipts += (received,)
            return received
        except BaseException:
            self._unknown()

    def _execute(self, supplier: VerifyCredentialSupplier) -> RestrictedWriterVerifyRetained:
        if self._phase != "new":
            self._unknown()
        try:
            request_sha256 = self._contract.request_digest(self._request_bytes)
            self._phase = "reservation-attempted"
            reservation = self._association.reserve_verify(
                self._request, request_sha256=request_sha256, deadline=self._deadline
            )
            if (
                type(reservation) is not self._contract.reservation_type
                or reservation.operation_id != self._request.operation_id
                or reservation.request_sha256 != request_sha256
            ):
                raise ValueError
            self._reservation = reservation
            self._check()
            custody = VerifyCoordinatorCustody(self._coordinator_factory, deadline=self._deadline)
            self._retain_custody(custody)
            custody.admit(self._association, reservation, self._contract)
            self._persist(
                self._contract.order[0], request_sha256=request_sha256, parent_sha256=reservation.parent_sha256
            )
            public_payload = self._launch.public_payload()
            if type(public_payload) is not bytes or not public_payload:
                raise ValueError
            self._public_payload = public_payload
            self._public_sha256 = sha256(public_payload).hexdigest()
            self._persist(
                self._contract.order[1],
                public_sha256=self._public_sha256,
                implementation_sha256=self._request.implementation_sha256,
            )
            self._phase = "launch-attempted"
            try:
                process = self._launcher.launch(self._launch, reservation, public_payload=public_payload)
            except TdsLaunchUnknown as error:
                self._unresolved_launch = error.launch
                raise
            self._process = process
            self._check()
            registration = process.registration(deadline=min(self._deadline, self._launch.startup_deadline))
            if (
                type(registration) is not self._contract.registration_type
                or registration.reservation is not reservation
                or registration.execution_owner is not reservation.execution_owner
            ):
                raise ValueError
            self._registration = registration
            registered = cast(Any, self._persist(self._contract.order[2], process_pid=registration.process.pid))
            self._advance(self._contract.process_registered(registration.process, registered.payload_sha256))
            self._persist(
                self._contract.order[3],
                credential_frame="dpone.sqlclient.restricted-writer-verify-credentials.v1",
            )
            self._advance(self._contract.credential_intent())
            credentials = supplier.take(self._launch, registration, public_payload=public_payload)
            self._phase = "credential-attempted"
            try:
                process.send_credentials(credentials, deadline=self._deadline)
            finally:
                credentials[:] = b"\x00" * len(credentials)
                process.scrub_credentials(credentials)
                del credentials
            process.assert_credentials_scrubbed()
            opening = process.writer_session(deadline=self._deadline)
            if type(opening) is not self._contract.opening_type:
                raise ValueError
            self._contract.validate_opening(self._request, opening)
            self._advance(self._contract.session_registered(opening))
            process.authorize_probe(opening, deadline=self._deadline)
            result = process.result(deadline=self._deadline)
            self._contract.validate_result(self._request, result)
            if result.opening != opening.context:
                raise ValueError
            result_bytes = self._contract.encode_result(result)
            self._result = deepcopy(result)
            self._persist(
                self._contract.order[4],
                result_sha256=sha256(result_bytes).hexdigest(),
                session_authority_sha256=result.opening.session.authority_sha256.hex(),
            )
            self._phase = "exit-attempted"
            exit_ = process.require_eof_and_zero(deadline=self._deadline)
            if exit_.identity != registration.process or not exit_.reaped or exit_.exit_code != 0:
                raise ValueError
            self._exit = exit_
            self._persist(self._contract.order[5], process_pid=registration.process.pid, exit_code=0, reaped=True)
            self._phase = "complete"
            self._assert_complete()
            return RestrictedWriterVerifyRetained(
                _RETAINED_TOKEN,
                self,
                _VerifyOwner._assert_complete,
                _VerifyOwner._exact_custody,
                _VerifyOwner._unknown,
            )
        except RestrictedWriterVerifyLocalUnknown:
            raise
        except BaseException:
            self._unknown()

    def run(self, supplier: VerifyCredentialSupplier) -> RestrictedWriterVerifyRetained:
        failure: BaseException | None = None
        try:
            return self._execute(supplier)
        except BaseException as error:
            failure = error
        assert failure is not None
        discard_verify_exception(failure)
        del failure
        del supplier
        self._unknown()

    def _assert_complete(self, exact_custody: VerifyCoordinatorCustody | None = None) -> None:
        if self._phase != "complete" or len(self._receipts) != len(self._contract.order):
            _VerifyOwner._unknown(self, exact_custody)
        _VerifyOwner._check(self, exact_custody)
        try:
            reservation = self._reservation
            registration = self._registration
            result = self._result
            exit_ = self._exit
            observation = self._evidence.observation
            custody = exact_custody if exact_custody is not None else _VerifyOwner._exact_custody(self)
            if self._coordinator_custody is not custody or self._coordinator_custody_ref is not custody:
                raise ValueError
            assert reservation is not None and registration is not None and result is not None and exit_ is not None
            if (
                registration.reservation is not reservation
                or registration.execution_owner is not reservation.execution_owner
                or exit_.identity != registration.process
                or not exit_.reaped
                or exit_.exit_code != 0
                or custody.snapshot.state.session != result.opening.session
                or type(observation.receipts) is not tuple
                or len(observation.receipts) != len(self._receipts)
                or any(
                    observed is not retained
                    for observed, retained in zip(observation.receipts, self._receipts, strict=True)
                )
            ):
                raise ValueError
            custody.assert_current()
            self._contract.validate_result(self._request, result)
        except BaseException:
            _VerifyOwner._unknown(self, exact_custody)


def verify_restricted_writer(
    association: RestrictedWriterVerifyOrigin,
    evidence: VerifyEvidence,
    launcher: VerifyLauncher,
    launch: VerifyLaunchInputs,
    supplier: VerifyCredentialSupplier,
    coordinator_factory: Callable[[], VerifyCoordinator],
    contract: RestrictedWriterVerifyContract,
    *,
    deadline: float,
    clock: Callable[[], float],
) -> RestrictedWriterVerifyRetained:
    owner = _VerifyOwner(association, evidence, launcher, launch, coordinator_factory, contract, deadline, clock)
    try:
        return owner.run(supplier)
    finally:
        del supplier
