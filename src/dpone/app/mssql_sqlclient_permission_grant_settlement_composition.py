"""Concrete isolated verifier composition for terminal GRANT settlement."""

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from secrets import token_bytes
from time import monotonic
from uuid import uuid4

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_departure_evidence_actor import SqlClientDepartureEvidenceActor
from dpone.adapters.mssql_sqlclient_departure_launch import PythonSqlClientDepartureLauncher
from dpone.adapters.mssql_tds_actor_core import TdsJournalActorUnknown
from dpone.app.mssql_sqlclient_departure_infrastructure_composition import admitted_departure_launcher as _admission
from dpone.app.mssql_sqlclient_departure_runner import (
    DepartureChildSession,
    DepartureEvidenceSession,
    DepartureOperationStrategy,
    DepartureRecordContext,
    DepartureRunCompletion,
    DepartureRunFacts,
    DepartureRunner,
)
from dpone.contracts.mssql_sqlclient_departure_evidence import (
    SqlClientDepartureEvidenceKind as Kind,
)
from dpone.contracts.mssql_sqlclient_departure_evidence import (
    SqlClientDepartureEvidenceReceipt,
    SqlClientDepartureEvidenceRecord,
)
from dpone.contracts.mssql_sqlclient_permission_grant import SqlClientPermissionGrantEvidence
from dpone.contracts.mssql_sqlclient_permission_grant_departure_codec import (
    PermissionGrantDepartureCompletion,
    PermissionGrantDepartureEvidenceContext,
    SqlClientObserverAdmission,
    SqlClientPermissionGrantDepartureCredentials,
    SqlClientPermissionGrantDeparturePlan,
    SqlClientPermissionGrantDepartureRequest,
    SqlClientPermissionGrantDepartureResult,
    TdsChildExit,
    TdsConnectionMaterial,
    TdsCoordinatorStartup,
    attempt_identity_digest,
    decode_result,
    encode_credentials,
    evidence_payload,
    validate_result,
)
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1
from dpone.services.mssql_tds_permission_grant_release import PermissionGrantLocallyReleased
from dpone.services.mssql_tds_permission_grant_settlement import (
    PermissionGrantSettled,
    settle_permission_grant_remotely,
)

ERROR = "mssql_native.sqlclient_permission_grant_settlement_composition_invalid"


@dataclass(frozen=True, slots=True)
class _StrategyContext:
    plan: SqlClientPermissionGrantDeparturePlan


def _make_request(context: object, startup: TdsCoordinatorStartup) -> object:
    if type(context) is not _StrategyContext or type(startup) is not TdsCoordinatorStartup:
        raise ValueError(ERROR)
    return SqlClientPermissionGrantDepartureRequest(plan=context.plan, startup=startup)


def _validate_request(context: object, request: object) -> None:
    if type(context) is not _StrategyContext or type(request) is not SqlClientPermissionGrantDepartureRequest:
        raise ValueError(ERROR)
    request.__post_init__()
    if request.plan is not context.plan:
        raise ValueError(ERROR)


def _decode_result(context: object, raw: bytes, request: object) -> object:
    if type(context) is not _StrategyContext or type(request) is not SqlClientPermissionGrantDepartureRequest:
        raise ValueError(ERROR)
    result = decode_result(raw, request)
    validate_result(result, request)
    return result


def _record(context: object, kind: Kind, run: DepartureRecordContext) -> SqlClientDepartureEvidenceRecord:
    if type(context) is not _StrategyContext or run.facts.plan is not context.plan:
        raise ValueError(ERROR)
    startup = run.startup
    request = run.request
    result = run.result
    if request is not None and type(request) is not SqlClientPermissionGrantDepartureRequest:
        raise ValueError(ERROR)
    if result is not None and type(result) is not SqlClientPermissionGrantDepartureResult:
        raise ValueError(ERROR)
    payload = evidence_payload(
        context.plan,
        kind,
        startup=startup,
        request=request,
        result=result,
        local_exit=run.local_exit,
        previous_sha256=tuple(item.receipt.payload_sha256 for item in run.records),
    )
    evidence_context = PermissionGrantDepartureEvidenceContext(context.plan)
    return SqlClientDepartureEvidenceRecord(
        context.plan.helper_id,
        attempt_identity_digest(context.plan.attempt),
        kind,
        payload,
        permission_grant_context=evidence_context,
    )


class _Owner:
    def __init__(self, local: PermissionGrantLocallyReleased) -> None:
        self.local = local
        self.request: SqlClientPermissionGrantDepartureRequest | None = None
        self.result: SqlClientPermissionGrantDepartureResult | None = None

    def guard(self, deadline: float) -> None:
        self.local.assert_local_exit()
        if monotonic() >= deadline:
            raise ValueError(ERROR)

    def bind_request(self, value: object) -> None:
        if self.request is not None or type(value) is not SqlClientPermissionGrantDepartureRequest:
            raise ValueError(ERROR)
        self.request = value

    def bind_result(self, value: object) -> None:
        if self.result is not None or type(value) is not SqlClientPermissionGrantDepartureResult:
            raise ValueError(ERROR)
        self.result = value

    def validate_request_owner(self) -> None:
        request = self.request
        if request is None:
            raise ValueError(ERROR)
        self.guard(request.plan.operation_deadline)

    def validate_final_owner(self) -> None:
        request, result = self.request, self.result
        if request is None or result is None:
            raise ValueError(ERROR)
        self.guard(request.plan.operation_deadline)
        validate_result(result, request)


class _Evidence(DepartureEvidenceSession):
    def __init__(self, gateway, context: PermissionGrantDepartureEvidenceContext, deadline: float) -> None:
        self.gateway, self.context, self.deadline = gateway, context, deadline
        self._receipts: dict[Kind, SqlClientDepartureEvidenceReceipt] = {}
        self.closed = False

    def persist(self, kind: Kind, payload: bytes) -> SqlClientDepartureEvidenceReceipt:
        record = SqlClientDepartureEvidenceRecord(
            self.context.plan.helper_id,
            attempt_identity_digest(self.context.plan.attempt),
            kind,
            payload,
            permission_grant_context=self.context,
        )
        receipt = self.gateway.write(record, deadline=self.deadline)
        if self.gateway.observation.receipt != receipt:
            raise ValueError(ERROR)
        self._receipts[kind] = receipt
        return receipt

    def receipts(self) -> Mapping[Kind, SqlClientDepartureEvidenceReceipt]:
        return dict(self._receipts)

    def expected(self) -> Mapping[Kind, SqlClientDepartureEvidenceReceipt]:
        return dict(self._receipts)

    def close(self, deadline: float) -> None:
        if self.closed:
            return
        self.closed = True
        self.gateway.close(deadline=deadline)

    def assert_closed(self, deadline: float) -> None:
        return None


class _Child(DepartureChildSession):
    def __init__(self, child, supplier: Callable[[], TdsConnectionMaterial]) -> None:
        self.child = child
        self.supplier: Callable[[], TdsConnectionMaterial] | None = supplier
        self.nonce = token_bytes(32)
        self.closed = False

    def startup(self, deadline: float):
        return self.child.startup(deadline=deadline)

    def deliver(self, request: object, deadline: float) -> None:
        if type(request) is not SqlClientPermissionGrantDepartureRequest or self.supplier is None:
            raise ValueError(ERROR)
        supplier, self.supplier = self.supplier, None
        if supplier is None:
            raise ValueError(ERROR)
        material = supplier()
        try:
            self.child.send_request(
                encode_credentials(
                    SqlClientPermissionGrantDepartureCredentials(
                        request=request, connection_material=material, session_nonce=self.nonce
                    )
                ),
                deadline=deadline,
            )
        finally:
            del material, supplier

    def receive(self, deadline: float) -> bytes:
        return self.child.receive_result(deadline=deadline)

    def settle(self, deadline: float) -> TdsChildExit:
        return self.child.wait(deadline=deadline)

    def close(self) -> None:
        if self.closed:
            return
        self.closed = True
        self.child.close()


def settle_mssql_sqlclient_permission_grant(
    pool,
    local: PermissionGrantLocallyReleased,
    evidence_root: Path,
    departure_launcher: PythonSqlClientDepartureLauncher,
    management_admission: SqlClientObserverAdmission,
    writer_admission: SqlClientObserverAdmission,
    management_credentials: Callable[[], TdsConnectionMaterial],
    *,
    deadline: float,
    helper_startup_timeout: float,
    cleanup_deadline: float,
) -> PermissionGrantSettled:
    """Run a distinct GRANT verifier and settle coordinator/directory exactly once."""
    if type(local) is not PermissionGrantLocallyReleased or not evidence_root.is_absolute():
        raise ValueError(ERROR)
    local.assert_local_exit()
    _, admission_sha, implementation, package_root, address_space = _admission(departure_launcher)
    startup_deadline = min(deadline, monotonic() + helper_startup_timeout)
    held = local.held_owner
    grant_evidence = held.result
    if type(grant_evidence) is not SqlClientPermissionGrantEvidence:
        raise ValueError(ERROR)
    plan = SqlClientPermissionGrantDeparturePlan(
        helper_id=uuid4(),
        grant_evidence=grant_evidence,
        management_admission=management_admission,
        writer_admission=writer_admission,
        implementation_sha256=implementation,
        package_root=package_root,
        admission_sha256=admission_sha,
        startup_deadline=startup_deadline,
        operation_deadline=deadline,
        max_address_space_bytes=address_space,
    )
    strategy = DepartureOperationStrategy(
        _StrategyContext(plan), _make_request, _validate_request, _decode_result, _record
    )
    facts = DepartureRunFacts(plan.helper_id, plan, True, strategy)
    context = PermissionGrantDepartureEvidenceContext(plan)
    retained: dict[str, object] = {}

    def evidence_factory(facts: DepartureRunFacts, deadline: float) -> DepartureEvidenceSession:
        @contextmanager
        def writer() -> Iterator[CreateOnlyEvidenceWriterV1]:
            yield DescriptorPinnedCreateOnlyEvidenceWriter(evidence_root)

        try:
            gateway = pool.open(
                lambda d, c: SqlClientDepartureEvidenceActor(
                    writer, plan.helper_id, attempt_identity_digest(plan.attempt), d, c
                ),
                deadline=deadline,
            )
        except TdsJournalActorUnknown as error:
            retained["evidence"] = _Evidence(error.gateway, context, deadline)
            raise
        evidence = _Evidence(gateway, context, deadline)
        retained["evidence"] = evidence
        return evidence

    def launch_factory(facts: DepartureRunFacts) -> DepartureChildSession:
        child = departure_launcher.spawn(
            startup_deadline=plan.startup_deadline, operation_deadline=plan.operation_deadline
        )
        session = _Child(child, management_credentials)
        retained["child"] = session
        return session

    runner = DepartureRunner(facts, _Owner(local), evidence_factory, launch_factory)

    def close_verifier(end: float) -> None:
        failure = None
        child = retained.get("child")
        evidence = retained.get("evidence")
        for callback in (
            (lambda: getattr(child, "close")()) if child is not None else None,
            (lambda: getattr(evidence, "close")(end)) if evidence is not None else None,
        ):
            if callback is None:
                continue
            try:
                callback()
            except BaseException as error:
                failure = failure or error
        if failure is not None:
            raise failure

    def run_verifier() -> PermissionGrantDepartureCompletion:
        completion: DepartureRunCompletion = runner.run()
        if (
            type(completion.request) is not SqlClientPermissionGrantDepartureRequest
            or type(completion.result) is not SqlClientPermissionGrantDepartureResult
        ):
            raise ValueError(ERROR)
        return PermissionGrantDepartureCompletion(
            completion.request,
            completion.result,
            completion.local_exit,
            completion.receipts,
        )

    return settle_permission_grant_remotely(
        local,
        run_verifier,
        close_verifier,
        deadline=deadline,
        cleanup_deadline=cleanup_deadline,
        clock=monotonic,
    )
