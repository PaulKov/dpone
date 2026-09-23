"""Exact coordinator custody for one P9a VERIFY execution."""

from __future__ import annotations

from collections.abc import Callable
from copy import deepcopy
from threading import Lock
from traceback import clear_frames
from typing import Any, Protocol, cast

from dpone.ports.mssql_tds_coordinator import AdvanceCoordinator

ERROR = "mssql_native.sqlclient_restricted_writer_verify_invalid"


def discard_verify_exception(error: BaseException) -> None:
    """Sever subordinate frames before translating a private VERIFY failure."""
    pending, seen = [error], set()
    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))
        pending.extend(value for value in (current.__cause__, current.__context__) if value is not None)
        traceback = current.__traceback__
        current.__traceback__ = current.__cause__ = current.__context__ = None
        if traceback is not None:
            clear_frames(traceback)


class VerifyCoordinator(Protocol):
    @property
    def observation(self): ...

    def execute(self, request: object, *, deadline: float): ...
    def close(self, *, deadline: float) -> None: ...


class VerifyCoordinatorContract(Protocol):
    @property
    def snapshot_type(self) -> type: ...

    def expected_advance(self, state: object, event: object) -> object: ...


class RestrictedWriterVerifyContract(VerifyCoordinatorContract, Protocol):
    """Injected nominal DTO, evidence, handshake and reducer operations."""

    order: Any
    reservation_type: Any
    registration_type: Any
    receipt_type: Any
    opening_type: Any

    def encode_request(self, value: object) -> bytes: ...
    def request_digest(self, payload: bytes) -> str: ...
    def evidence_record(self, request: object, kind: object, facts: dict[str, object]) -> object: ...
    def validate_result(self, request: object, result: object) -> None: ...
    def encode_result(self, result: object) -> bytes: ...
    def validate_opening(self, request: object, opening: object) -> None: ...
    def process_registered(self, process: object, authentication_sha256: str) -> object: ...
    def credential_intent(self) -> object: ...
    def session_registered(self, opening: object) -> object: ...


class VerifyEvidence(Protocol):
    observation: Any

    def write(self, record: object, *, deadline: float): ...


class VerifyLaunchInputs(Protocol):
    request: object
    startup_deadline: float
    operation_deadline: float
    termination_timeout: float

    def public_payload(self) -> bytes: ...


class VerifyCredentialSupplier(Protocol):
    def take(self, launch: VerifyLaunchInputs, registration: object, *, public_payload: bytes) -> bytearray: ...


class VerifyProcess(Protocol):
    def registration(self, *, deadline: float) -> object: ...
    def send_credentials(self, payload: bytearray, *, deadline: float) -> None: ...
    def scrub_credentials(self, payload: bytearray) -> None: ...
    def assert_credentials_scrubbed(self) -> None: ...
    def writer_session(self, *, deadline: float) -> object: ...
    def authorize_probe(self, opening: object, *, deadline: float) -> None: ...
    def result(self, *, deadline: float) -> object: ...
    def require_eof_and_zero(self, *, deadline: float): ...
    def cleanup(self): ...


class VerifyLauncher(Protocol):
    def launch(self, inputs: VerifyLaunchInputs, reservation: object, *, public_payload: bytes) -> VerifyProcess: ...


class _CoordinatorCustodyState:
    __slots__ = ("closed", "contract", "deadline", "snapshot", "tampered")

    def __init__(self, deadline: float) -> None:
        self.closed = self.tampered = False
        self.contract: VerifyCoordinatorContract | None = None
        self.deadline = deadline
        self.snapshot: Any | None = None


class VerifyCoordinatorCustody(tuple):
    """Tuple-backed exact gateway operations plus isolated mutable protocol state."""

    __slots__ = ()

    def __new__(cls, factory: Callable[[], VerifyCoordinator], *, deadline: float):
        gateway = factory()
        descriptor = getattr(type(gateway), "observation", None)
        if isinstance(descriptor, property) and descriptor.fget is not None:
            observation = descriptor.fget.__get__(gateway, type(gateway))
        else:
            exact_observation = gateway.observation

            def observation() -> Any:
                return exact_observation

        execute = getattr(gateway, "execute", None)
        if not callable(execute):

            def execute(*args: object, **kwargs: object) -> Any:
                del args, kwargs
                raise ValueError(ERROR)

        return tuple.__new__(
            cls,
            (
                gateway,
                _CoordinatorCustodyState(deadline),
                observation,
                execute,
                gateway.close,
            ),
        )

    def __init__(self, factory: Callable[[], VerifyCoordinator], *, deadline: float) -> None:
        del factory, deadline

    @property
    def _state(self) -> _CoordinatorCustodyState:
        return cast(_CoordinatorCustodyState, self[1])

    @property
    def gateway(self) -> VerifyCoordinator:
        return cast(VerifyCoordinator, self[0])

    @gateway.setter
    def gateway(self, value: object) -> None:
        del value
        self._state.tampered = True
        raise AttributeError(ERROR)

    @property
    def snapshot(self) -> Any:
        return self._state.snapshot

    @snapshot.setter
    def snapshot(self, value: Any) -> None:
        self._state.snapshot = value

    @property
    def observation(self) -> Any:
        return cast(Callable[[], Any], self[2])()

    def execute(self, request: object, *, deadline: float) -> Any:
        return cast(Callable[..., Any], self[3])(request, deadline=deadline)

    def __deepcopy__(self, memo: dict) -> VerifyCoordinatorCustody:
        del memo
        self._state.tampered = True
        raise TypeError(ERROR)

    def admit(self, association: object, reservation: object, contract: VerifyCoordinatorContract) -> None:
        state = self._state
        if state.tampered or state.contract is not None or state.snapshot is not None:
            raise ValueError(ERROR)
        current = self.observation.snapshot
        if type(current) is not contract.snapshot_type:
            raise ValueError(ERROR)
        state.contract, state.snapshot = contract, current
        state = cast(Any, current).state
        if (
            state.phase.value != "intent"
            or state.identity != getattr(association, "_verify_identity", None)
            or state.execution_owner != cast(Any, reservation).execution_owner
            or state.local is not None
            or state.remote is not None
        ):
            raise ValueError(ERROR)

    def assert_current(self) -> None:
        if self._state.tampered or self.observation.snapshot is not self.snapshot:
            raise ValueError(ERROR)

    def transfer_gateway(self) -> VerifyCoordinator:
        """Return the exact construction-time gateway after a current-state guard."""
        self.assert_current()
        return self.gateway

    def advance(self, event: object) -> None:
        self.assert_current()
        contract = self._state.contract
        if contract is None:
            raise ValueError(ERROR)
        previous = cast(Any, self.snapshot)
        expected = contract.expected_advance(previous.state, event)
        observed = self.execute(
            AdvanceCoordinator(cast(Any, event), previous.state.phase),
            deadline=self._state.deadline,
        )
        if type(observed) is not contract.snapshot_type:
            raise ValueError(ERROR)
        checked = cast(Any, observed)
        if (
            checked.state != expected
            or checked.revision <= previous.revision
            or self.observation.snapshot is not observed
        ):
            raise ValueError(ERROR)
        self.snapshot = observed

    def close(self, *, deadline: float | None = None) -> None:
        state = self._state
        if not state.closed:
            state.closed = True
            cast(Callable[..., None], self[4])(deadline=state.deadline if deadline is None else deadline)
        if state.tampered:
            raise ValueError(ERROR)


class VerifyUnknownOwner(Protocol):
    def contain_unknown(self, *, deadline: float) -> None: ...


class RestrictedWriterVerifyLocalUnknown(RuntimeError):
    def __init__(self, retained: VerifyUnknownOwner) -> None:
        self.retained = retained
        super().__init__("mssql_native.sqlclient_restricted_writer_verify_unknown")


_RETAINED_TOKEN = object()


class _RetainedState:
    __slots__ = ("claim_lock", "p9b_owner")

    def __init__(self) -> None:
        self.claim_lock = Lock()
        self.p9b_owner: object | None = None


class RestrictedWriterVerifyRetained(tuple):
    """Immutable owner/custody/gateway identity with mutable one-shot claim state."""

    __slots__ = ()

    def __new__(
        cls,
        token: object,
        owner: object,
        assert_complete: Callable[[Any, VerifyCoordinatorCustody | None], None],
        exact_custody: Callable[[Any], VerifyCoordinatorCustody],
        unknown: Callable[[Any, VerifyCoordinatorCustody | None], Any],
    ):
        if token is not _RETAINED_TOKEN:
            raise ValueError(ERROR)
        assert_complete(owner, None)
        custody = exact_custody(owner)
        return tuple.__new__(
            cls,
            (owner, custody, custody.transfer_gateway(), _RetainedState(), assert_complete, exact_custody, unknown),
        )

    def __init__(self, token: object, owner: object, *callbacks: Callable[..., object]) -> None:
        del token, owner, callbacks

    @property
    def _owner(self) -> Any:
        return self[0]

    @property
    def _custody(self) -> VerifyCoordinatorCustody:
        return cast(VerifyCoordinatorCustody, self[1])

    @property
    def _gateway(self) -> VerifyCoordinator:
        return cast(VerifyCoordinator, self[2])

    @property
    def _state(self) -> _RetainedState:
        return cast(_RetainedState, self[3])

    def _exact_custody(self) -> VerifyCoordinatorCustody:
        owner, exact = self._owner, self._custody
        try:
            custody = cast(Callable[[Any], VerifyCoordinatorCustody], self[5])(owner)
            if custody is not exact:
                raise ValueError(ERROR)
        except RestrictedWriterVerifyLocalUnknown:
            raise
        except BaseException:
            cast(Callable[[Any, VerifyCoordinatorCustody | None], Any], self[6])(owner, exact)
        return self._custody

    def _claim_p9b(self, owner: object) -> None:
        if owner is None:
            raise ValueError(ERROR)
        self.assert_retained()
        with self._state.claim_lock:
            self.assert_retained()
            if self._state.p9b_owner is not None:
                raise ValueError(ERROR)
            self._state.p9b_owner = owner

    def _assert_p9b_claim(self, owner: object) -> None:
        if owner is None:
            raise ValueError(ERROR)
        self.assert_retained()
        with self._state.claim_lock:
            self.assert_retained()
            if self._state.p9b_owner is not owner:
                raise ValueError(ERROR)

    def assert_retained(self) -> None:
        owner, custody, gateway = self._owner, self._custody, self._gateway
        try:
            if (
                owner._coordinator_custody is not custody
                or owner._coordinator_custody_ref is not custody
                or custody.transfer_gateway() is not gateway
            ):
                raise ValueError(ERROR)
            cast(Callable[[Any, VerifyCoordinatorCustody | None], None], self[4])(owner, custody)
            if owner._coordinator_custody is not custody or owner._coordinator_custody_ref is not custody:
                raise ValueError(ERROR)
        except RestrictedWriterVerifyLocalUnknown:
            raise
        except BaseException:
            cast(Callable[[Any, VerifyCoordinatorCustody | None], Any], self[6])(owner, custody)

    @property
    def reservation(self) -> object:
        self.assert_retained()
        assert self._owner._reservation is not None
        return self._owner._reservation

    @property
    def result(self) -> object:
        self.assert_retained()
        assert self._owner._result is not None
        return deepcopy(self._owner._result)

    @property
    def receipts(self) -> tuple[object, ...]:
        self.assert_retained()
        return self._owner._receipts

    def _p9b_coordinator(self, owner: object) -> VerifyCoordinator:
        self._assert_p9b_claim(owner)
        self.assert_retained()
        return self._gateway

    def _p9b_observation(self, owner: object) -> Any:
        self._assert_p9b_claim(owner)
        self.assert_retained()
        return self._custody.observation

    def _p9b_execute(self, owner: object, request: object, *, deadline: float) -> Any:
        self._assert_p9b_claim(owner)
        self.assert_retained()
        return self._custody.execute(request, deadline=deadline)

    def _p9b_close(self, *, deadline: float) -> None:
        self._custody.close(deadline=deadline)
