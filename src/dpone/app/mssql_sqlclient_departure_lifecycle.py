"""CREATE provenance, public outcomes, and legacy departure strategy lifecycle."""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from dpone.app.mssql_sqlclient_departure_custody import strict_exit as _strict_exit
from dpone.app.mssql_sqlclient_departure_runner import (
    DepartureOperationStrategy,
    DepartureRecordContext,
    Kind,
    TdsCoordinatorStartup,
)
from dpone.app.mssql_tds_coordinator_supervisor import TdsCoordinatorCreateOutcome
from dpone.app.mssql_tds_create_provenance import validate_create_provenance
from dpone.contracts.mssql_sqlclient_departure_models import (
    SqlClientDepartureEvidenceReceipt,
    SqlClientDepartureEvidenceRecord,
    SqlClientDeparturePlan,
    SqlClientDeparturePlanV2,
    SqlClientDepartureRequest,
    SqlClientDepartureRequestV2,
    SqlClientDepartureResult,
    SqlClientDepartureResultV2,
    SqlClientObserverAdmission,
    TdsAttemptOwnership,
    TdsChildExit,
    TdsCoordinatorIdentity,
    TdsCreateRequest,
    TdsProcessIdentity,
)
from dpone.contracts.mssql_tds_api import (
    CreateKind,
    chain,
    decode_departure_result,
    decode_departure_result_v2,
    decode_observe_departure_result,
    ipc,
    ipc_v2,
    observe_ipc,
    validate_departure_observer_binding_v2,
)
from dpone.contracts.mssql_tds_result import attempt_identity_digest


class DepartureCreateValidationMixin:
    """Validate the exact CREATE producer and observer bindings."""

    create_outcome: TdsCoordinatorCreateOutcome | None
    owner: TdsAttemptOwnership | None
    original_request: TdsCreateRequest
    create_identity: TdsCoordinatorIdentity
    request: SqlClientDepartureRequest | SqlClientDepartureRequestV2 | None
    observer_admission: SqlClientObserverAdmission | None
    plan: SqlClientDeparturePlan | SqlClientDeparturePlanV2 | None
    create_admission: bytes
    create_source: str
    create_root: str

    def validate_create(self) -> None:
        """Recompare real producer provenance to separately captured originals."""
        created = self.create_outcome
        if created is None or created.provenance is None or self.owner is None:
            raise ValueError("mssql_native.sqlclient_departure_create_required")
        validate_create_provenance(
            created.provenance,
            response=created.response,
            local_exit=created.local_exit,
            receipts=created.receipts,
            request=self.original_request,
            identity=self.create_identity,
            ownership=self.owner,
        )
        if type(self.request) is SqlClientDepartureRequestV2:
            if self.observer_admission is None:
                raise ValueError("mssql_native.sqlclient_departure_observer_binding")
            validate_departure_observer_binding_v2(self.request, observer_admission=self.observer_admission)
        provenance = created.provenance
        if self.plan is not None:
            receipts = {receipt.kind: receipt for receipt in created.receipts}
            evidence = created.response.evidence
            assert evidence is not None
            if (
                self.plan.create_operation != self.create_identity
                or self.plan.ownership != self.owner
                or self.plan.create_process != provenance.startup.process
                or (self.plan.original != evidence.session)
                or (self.plan.database != evidence.database)
                or (self.plan.create_result_sha256 != receipts[CreateKind.RESULT].payload_sha256)
                or (self.plan.create_local_exit_sha256 != receipts[CreateKind.LOCAL_EXIT].payload_sha256)
            ):
                raise ValueError("mssql_native.sqlclient_departure_create_links_changed")
        if (
            provenance.admission != self.create_admission
            or provenance.startup.implementation_sha256 != self.create_source
            or provenance.startup.package_root != self.create_root
        ):
            raise ValueError("mssql_native.sqlclient_departure_create_binding")


def strict_exit(value: TdsChildExit, process: TdsProcessIdentity | None) -> TdsChildExit:
    """Preserve the original public validation entrypoint and signature."""
    return _strict_exit(value, process)


@dataclass(frozen=True)
class SqlClientDepartureOutcome:
    """Six acknowledged helper components, without preparation or route authority."""

    plan: SqlClientDeparturePlan | SqlClientDeparturePlanV2
    request: SqlClientDepartureRequest | SqlClientDepartureRequestV2
    result: SqlClientDepartureResult | SqlClientDepartureResultV2
    local_exit: TdsChildExit
    receipts: tuple[SqlClientDepartureEvidenceReceipt, ...]

    def __post_init__(self) -> None:
        if any(
            type(value) in (SqlClientDeparturePlanV2, SqlClientDepartureRequestV2, SqlClientDepartureResultV2)
            for value in (self.plan, self.request, self.result)
        ):
            if (type(self.plan), type(self.request), type(self.result)) != (
                SqlClientDeparturePlanV2,
                SqlClientDepartureRequestV2,
                SqlClientDepartureResultV2,
            ):
                raise ValueError("mssql_native.sqlclient_departure_version_invalid")
            self.plan.__post_init__()
            self.request.__post_init__()
            self.result.__post_init__()
            if self.request.plan != self.plan:
                raise ValueError("mssql_native.sqlclient_departure_version_invalid")


@dataclass(frozen=True)
class SqlClientCreateDepartureOutcome:
    """Actual original CREATE and its local departure continuation only."""

    create_outcome: TdsCoordinatorCreateOutcome
    helper_outcome: SqlClientDepartureOutcome


Plan = ipc.SqlClientDeparturePlan | ipc_v2.SqlClientDeparturePlanV2 | observe_ipc.SqlClientObserveDeparturePlan
Request = (
    ipc.SqlClientDepartureRequest | ipc_v2.SqlClientDepartureRequestV2 | observe_ipc.SqlClientObserveDepartureRequest
)
Result = ipc.SqlClientDepartureResult | ipc_v2.SqlClientDepartureResultV2 | observe_ipc.SqlClientObserveDepartureResult


@dataclass(frozen=True, slots=True)
class _LegacyOperation:
    plan: Plan
    observer: SqlClientObserverAdmission | None


def legacy_strategy(plan: object, observer: object, retain: bool) -> DepartureOperationStrategy:
    """Admit one exact immutable legacy operation strategy before effects."""
    if type(plan) not in (
        ipc.SqlClientDeparturePlan,
        ipc_v2.SqlClientDeparturePlanV2,
        observe_ipc.SqlClientObserveDeparturePlan,
    ):
        raise ValueError("mssql_native.sqlclient_departure_version_invalid")
    if (type(plan) is observe_ipc.SqlClientObserveDeparturePlan) != retain:
        raise ValueError("mssql_native.sqlclient_departure_version_invalid")
    if type(plan) in (ipc_v2.SqlClientDeparturePlanV2, observe_ipc.SqlClientObserveDeparturePlan):
        if type(observer) is not SqlClientObserverAdmission:
            raise ValueError("mssql_native.sqlclient_departure_version_invalid")
    elif observer is not None:
        raise ValueError("mssql_native.sqlclient_departure_version_invalid")
    return DepartureOperationStrategy(
        _LegacyOperation(cast(Plan, plan), cast(SqlClientObserverAdmission | None, observer)),
        _request,
        _validate,
        _decode,
        _record,
    )


def _request(raw: object, startup: TdsCoordinatorStartup) -> object:
    assert type(raw) is _LegacyOperation
    plan = raw.plan
    if type(plan) is observe_ipc.SqlClientObserveDeparturePlan:
        return observe_ipc.SqlClientObserveDepartureRequest(plan=plan, startup=startup)
    if type(plan) is ipc_v2.SqlClientDeparturePlanV2:
        return ipc_v2.SqlClientDepartureRequestV2(plan=plan, startup=startup)
    assert type(plan) is ipc.SqlClientDeparturePlan
    return ipc.SqlClientDepartureRequest(plan=plan, startup=startup)


def _validate(raw: object, request: object) -> None:
    assert type(raw) is _LegacyOperation
    plan = raw.plan
    if type(request) is observe_ipc.SqlClientObserveDepartureRequest:
        return
    if type(request) is ipc_v2.SqlClientDepartureRequestV2 and type(plan) is ipc_v2.SqlClientDeparturePlanV2:
        assert raw.observer is not None
        ipc_v2.validate_departure_observer_binding_v2(request, observer_admission=raw.observer)
        ipc_v2.validate_departure_request_binding_v2(
            request,
            startup=request.startup,
            admission_sha256=plan.admission_sha256,
            startup_deadline=plan.startup_deadline,
            operation_deadline=plan.operation_deadline,
            max_address_space_bytes=plan.max_address_space_bytes,
        )
    elif type(request) is ipc.SqlClientDepartureRequest and type(plan) is ipc.SqlClientDeparturePlan:
        ipc.validate_departure_request_binding(
            request,
            startup=request.startup,
            admission_sha256=plan.admission_sha256,
            startup_deadline=plan.startup_deadline,
            operation_deadline=plan.operation_deadline,
            max_address_space_bytes=plan.max_address_space_bytes,
        )
    else:
        raise ValueError("mssql_native.sqlclient_departure_version_invalid")


def _decode(raw: object, body: bytes, request: object) -> object:
    assert type(raw) is _LegacyOperation
    if type(request) is observe_ipc.SqlClientObserveDepartureRequest:
        assert raw.observer is not None
        return decode_observe_departure_result(body, request=request, observer_admission=raw.observer)
    if type(request) is ipc_v2.SqlClientDepartureRequestV2:
        return decode_departure_result_v2(body, request=request)
    if type(request) is ipc.SqlClientDepartureRequest:
        return decode_departure_result(body, request=request)
    raise ValueError("mssql_native.sqlclient_departure_version_invalid")


def _record(raw: object, kind: Kind, value: DepartureRecordContext) -> SqlClientDepartureEvidenceRecord:
    assert type(raw) is _LegacyOperation
    plan, request, result, exited = (raw.plan, value.request, value.result, value.local_exit)
    subject = (value.facts.helper_id, attempt_identity_digest(value.facts.plan.attempt))
    observe = type(plan) is observe_ipc.SqlClientObserveDeparturePlan
    if kind is Kind.LAUNCH_INTENT:
        payload = (
            chain.departure_launch_payload(plan, observer_admission=raw.observer)
            if observe
            else chain.departure_launch_payload(plan)
        )
    elif kind is Kind.REGISTRATION:
        assert value.startup is not None
        payload = chain.departure_registration_payload(plan, value.startup, value.records[0].receipt.payload_sha256)
    elif kind is Kind.CREDENTIAL_INTENT:
        assert isinstance(request, Request)
        payload = (
            chain.departure_credential_payload(
                request, value.records[1].receipt.payload_sha256, observer_admission=raw.observer
            )
            if observe
            else chain.departure_credential_payload(request, value.records[1].receipt.payload_sha256)
        )
    elif kind is Kind.RESULT:
        assert isinstance(request, Request) and isinstance(result, Result)
        payload = (
            chain.departure_result_payload(
                request, result, value.records[2].receipt.payload_sha256, observer_admission=raw.observer
            )
            if observe
            else chain.departure_result_payload(request, result, value.records[2].receipt.payload_sha256)
        )
    elif kind is Kind.LOCAL_EXIT:
        assert isinstance(request, Request) and exited is not None
        payload = chain.departure_local_exit_payload(
            request, exited, value.records[1].receipt.payload_sha256, value.records[3].receipt.payload_sha256
        )
    else:
        assert value.startup is not None and exited is not None
        if observe:
            assert type(plan) is observe_ipc.SqlClientObserveDeparturePlan
            assert type(request) is observe_ipc.SqlClientObserveDepartureRequest
            assert type(result) is observe_ipc.SqlClientObserveDepartureResult and raw.observer is not None
            return chain.reconstruct_observe_departure_chain(
                plan, request, value.startup, result, exited, observer_admission=raw.observer
            )[5]
        assert isinstance(plan, (ipc.SqlClientDeparturePlan, ipc_v2.SqlClientDeparturePlanV2))
        assert isinstance(request, (ipc.SqlClientDepartureRequest, ipc_v2.SqlClientDepartureRequestV2))
        assert isinstance(result, (ipc.SqlClientDepartureResult, ipc_v2.SqlClientDepartureResultV2))
        return chain.reconstruct_departure_chain(plan, request, value.startup, result, exited)[5]
    if observe:
        assert type(plan) is observe_ipc.SqlClientObserveDeparturePlan and raw.observer is not None
        if kind is Kind.LAUNCH_INTENT:
            return SqlClientDepartureEvidenceRecord(
                *subject, kind, payload, observe_plan=plan, observer_admission=raw.observer
            )
        assert type(request) is observe_ipc.SqlClientObserveDepartureRequest
        return SqlClientDepartureEvidenceRecord(
            *subject, kind, payload, observe_request=request, observer_admission=raw.observer
        )
    if kind is Kind.RESULT:
        assert isinstance(request, (ipc.SqlClientDepartureRequest, ipc_v2.SqlClientDepartureRequestV2))
        return SqlClientDepartureEvidenceRecord(*subject, kind, payload, result_context=request)
    return SqlClientDepartureEvidenceRecord(*subject, kind, payload)
