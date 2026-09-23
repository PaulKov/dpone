"""Closed six-ACK runner and child dispatch for P9b management verification."""

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from secrets import token_bytes
from time import monotonic
from typing import Any, cast

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_departure_evidence_actor import (
    SqlClientDepartureEvidenceActor,
    restricted_writer_departure_record,
)
from dpone.adapters.mssql_sqlclient_restricted_writer_settlement import (
    SqlClientRestrictedWriterSettlementObserver,
)
from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown
from dpone.app.mssql_sqlclient_departure_runner import (
    DepartureChildSession,
    DepartureEvidenceSession,
    DepartureOperationStrategy,
    DepartureRecordContext,
    DepartureRunCompletion,
    DepartureRunFacts,
    DepartureRunner,
)
from dpone.contracts.mssql_tds_api import codec
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1
from dpone.services.mssql_tds_restricted_writer_verify_coordinator import RestrictedWriterVerifyRetained

ERROR = "mssql_native.sqlclient_restricted_writer_departure_invalid"
CREDENTIAL_SCHEMA = "dpone.sqlclient.restricted-writer-departure-credentials.v1"
RESULT_FRAME_LIMIT = codec.RESULT_FRAME_LIMIT


@dataclass(frozen=True, slots=True)
class _StrategyContext:
    plan: Any


def _make_request(context: object, startup: object) -> object:
    if type(context) is not _StrategyContext or type(context.plan) is not codec.RestrictedWriterDeparturePlan:
        raise ValueError(ERROR)
    return codec.RestrictedWriterDepartureRequest(plan=context.plan, startup=cast(Any, startup))


def _validate_request(context: object, request: object) -> None:
    if (
        type(context) is not _StrategyContext
        or type(request) is not codec.RestrictedWriterDepartureRequest
        or request.plan is not context.plan
    ):
        raise ValueError(ERROR)
    request.__post_init__()


def _decode_result(context: object, raw: bytes, request: object) -> object:
    _validate_request(context, request)
    return codec.decode_result(raw, cast(Any, request))


def _record(context: object, kind: object, run: DepartureRecordContext):
    if type(context) is not _StrategyContext or run.facts.plan is not context.plan:
        raise ValueError(ERROR)
    try:
        index = tuple(codec.Kind).index(kind)
    except ValueError:
        raise ValueError(ERROR) from None
    payload = codec.evidence_payload(
        context.plan,
        cast(Any, kind),
        startup=run.startup if index >= 1 else None,
        request=cast(Any, run.request) if index >= 2 else None,
        result=cast(Any, run.result) if index >= 3 else None,
        local_exit=run.local_exit if index >= 4 else None,
        previous_sha256=tuple(item.receipt.payload_sha256 for item in run.records),
    )
    evidence_context = codec.RestrictedWriterDepartureEvidenceContext(context.plan)
    return restricted_writer_departure_record(
        context.plan.helper_id,
        codec.attempt_identity_digest(context.plan.attempt),
        kind,
        payload,
        evidence_context,
    )


class _Owner:
    def __init__(self, retained: RestrictedWriterVerifyRetained, plan: object) -> None:
        self.retained, self.plan = retained, cast(Any, plan)
        self.request: Any | None = None
        self.result: Any | None = None

    def guard(self, deadline: float) -> None:
        self.retained.assert_retained()
        owner = self.retained._owner
        if (
            monotonic() >= deadline
            or self.plan.verify_request is not owner._request
            or self.plan.verify_result is not owner._result
        ):
            raise ValueError(ERROR)

    def bind_request(self, value: object) -> None:
        if self.request is not None or type(value) is not codec.RestrictedWriterDepartureRequest:
            raise ValueError(ERROR)
        self.request = value

    def bind_result(self, value: object) -> None:
        if self.result is not None or type(value) is not codec.RestrictedWriterDepartureResult:
            raise ValueError(ERROR)
        self.result = value

    def validate_request_owner(self) -> None:
        if self.request is None:
            raise ValueError(ERROR)
        self.guard(self.request.plan.operation_deadline)

    def validate_final_owner(self) -> None:
        if self.request is None or self.result is None:
            raise ValueError(ERROR)
        self.guard(self.request.plan.operation_deadline)
        codec.validate_result(self.result, self.request)


class _Evidence(DepartureEvidenceSession):
    def __init__(self, gateway, context, deadline: float) -> None:
        self.gateway, self.context, self.deadline = gateway, context, deadline
        self._receipts: dict[object, object] = {}
        self.closed = False

    def persist(self, kind, payload: bytes):
        record = restricted_writer_departure_record(
            self.context.plan.helper_id,
            codec.attempt_identity_digest(self.context.plan.attempt),
            kind,
            payload,
            self.context,
        )
        receipt = self.gateway.write(record, deadline=self.deadline)
        if self.gateway.observation.receipt != receipt:
            raise ValueError(ERROR)
        self._receipts[kind] = receipt
        return receipt

    def receipts(self) -> Mapping:
        return dict(self._receipts)

    def expected(self) -> Mapping:
        return dict(self._receipts)

    def close(self, deadline: float) -> None:
        if not self.closed:
            self.closed = True
            self.gateway.close(deadline=deadline)

    def assert_closed(self, deadline: float) -> None:
        return None


class _Child(DepartureChildSession):
    def __init__(self, child, supplier: Callable[[], object]) -> None:
        self.child, self.supplier = child, supplier
        self.nonce, self.closed = token_bytes(32), False

    def startup(self, deadline: float):
        return self.child.startup(deadline=deadline)

    def deliver(self, request: object, deadline: float) -> None:
        if type(request) is not codec.RestrictedWriterDepartureRequest or self.supplier is None:
            raise ValueError(ERROR)
        supplier, self.supplier = self.supplier, None  # type: ignore[assignment]
        material = supplier()
        try:
            credentials = codec.RestrictedWriterDepartureCredentials(
                request=request, connection_material=cast(Any, material), session_nonce=self.nonce
            )
            self.child.send_request(codec.encode_credentials(credentials), deadline=deadline)
        finally:
            del material, supplier

    def receive(self, deadline: float) -> bytes:
        return self.child.receive_result_bounded(deadline=deadline, max_payload=RESULT_FRAME_LIMIT)

    def settle(self, deadline: float):
        return self.child.wait(deadline=deadline)

    def close(self) -> None:
        if not self.closed:
            self.closed = True
            self.child.close()


def _close_resources(resources: Mapping[str, object], deadline: float) -> None:
    """Attempt both idempotent closes and preserve the first exact failure."""
    failure = None
    for name in ("child", "evidence"):
        resource = resources.get(name)
        if resource is None:
            continue
        try:
            exact = cast(Any, resource)
            exact.close(deadline) if name == "evidence" else exact.close()
        except BaseException as error:
            if failure is None:
                failure = error
    if failure is not None:
        raise failure


def make_verifier_factory(pool, evidence_root: Path, launcher, credentials, retained):
    """Build one exact P9b management verifier factory for settlement composition."""
    if not evidence_root.is_absolute() or type(retained) is not RestrictedWriterVerifyRetained:
        raise ValueError(ERROR)

    def factory(plan):
        retained.assert_retained()
        context = codec.RestrictedWriterDepartureEvidenceContext(plan)
        strategy = DepartureOperationStrategy(
            _StrategyContext(plan), _make_request, _validate_request, _decode_result, _record
        )
        facts = DepartureRunFacts(plan.helper_id, plan, True, strategy)
        resources: dict[str, object] = {}

        def evidence_factory(_facts, deadline):
            @contextmanager
            def writer() -> Iterator[CreateOnlyEvidenceWriterV1]:
                yield DescriptorPinnedCreateOnlyEvidenceWriter(evidence_root)

            try:
                gateway = pool.open(
                    lambda d, c: SqlClientDepartureEvidenceActor(
                        writer, plan.helper_id, codec.attempt_identity_digest(plan.attempt), d, c
                    ),
                    deadline=deadline,
                )
            except TdsJournalActorUnknown as error:
                resources["evidence"] = _Evidence(error.gateway, context, deadline)
                raise
            evidence = _Evidence(gateway, context, deadline)
            resources["evidence"] = evidence
            return evidence

        def launch_factory(_facts):
            child = launcher.spawn(startup_deadline=plan.startup_deadline, operation_deadline=plan.operation_deadline)
            session = _Child(child, credentials)
            resources["child"] = session
            return session

        runner = DepartureRunner(facts, _Owner(retained, plan), evidence_factory, launch_factory)

        def run():
            completion: DepartureRunCompletion = runner.run()
            return codec.RestrictedWriterDepartureCompletion(
                cast(Any, completion.request),
                cast(Any, completion.result),
                completion.local_exit,
                completion.receipts,
            )

        def close(deadline: float) -> None:
            _close_resources(resources, deadline)

        return run, close

    return factory


def decode_child_credentials(
    payload: bytes,
    *,
    startup,
    admission_sha256: str,
    startup_deadline: float,
    operation_deadline: float,
    max_address_space_bytes: int,
):
    """Decode the fixed P9b credential schema inside the existing child."""
    credentials = codec.decode_credentials(payload)
    plan = credentials.request.plan
    if (
        credentials.request.startup != startup
        or plan.admission_sha256 != admission_sha256
        or plan.startup_deadline != startup_deadline
        or plan.operation_deadline != operation_deadline
        or plan.max_address_space_bytes != max_address_space_bytes
    ):
        raise ValueError(ERROR)
    return credentials


def is_credentials(value: object) -> bool:
    return type(value) is codec.RestrictedWriterDepartureCredentials


def is_request(value: object) -> bool:
    return type(value) is codec.RestrictedWriterDepartureRequest


def observe_child(connection, request, session_nonce: object):
    """Execute one P9b management observation while SQL ownership is live."""
    if not is_request(request) or type(session_nonce) is not bytes or len(session_nonce) != 32:
        raise ValueError(ERROR)
    operations = codec.RestrictedWriterSettlementOperations(
        codec.encode_request, codec.validate_result, codec.remote_settlement_payload
    )
    return SqlClientRestrictedWriterSettlementObserver(connection, request, cast(Any, operations)).observe(
        cast(bytes, session_nonce)
    )


def encode_child_result(result, request) -> bytes:
    return codec.encode_result(result, request)
