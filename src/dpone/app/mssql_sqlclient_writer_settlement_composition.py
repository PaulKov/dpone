"""Parent-owned P10f helper composition with exact process provenance."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict
from hashlib import sha256
from time import monotonic
from uuid import uuid4

from dpone.adapters.mssql_sqlclient_departure_launch import PythonSqlClientDepartureLauncher
from dpone.adapters.mssql_sqlclient_departure_process import SqlClientDepartureProcess
from dpone.app.mssql_sqlclient_departure_custody import strict_exit
from dpone.app.mssql_sqlclient_departure_infrastructure_composition import admitted_departure_launcher as _admission
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_tds_api import (
    SqlClientInputDescriptor,
    SqlClientStageContentExpectation,
    SqlClientStageIdentity,
    SqlClientWriterObservationRecord,
    SqlClientWriterSettlementCredentials,
    SqlClientWriterSettlementObservation,
    SqlClientWriterSettlementPlan,
    SqlClientWriterSettlementProvenance,
    SqlClientWriterSettlementRequest,
    canonical_json_bytes,
    decode_writer_settlement_result,
    encode_startup,
    encode_writer_settlement_credentials,
    encode_writer_settlement_request,
    writer_settlement_request_digest,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds

ERROR = "mssql_native.sqlclient_writer_settlement_helper_unknown"


class ContainedSqlClientWriterSettlementVerifier:
    """Launch and consume one fixed P10f child; never reconnect or retry SQL."""

    def __init__(
        self,
        launcher: PythonSqlClientDepartureLauncher,
        *,
        management_admission: SqlClientObserverAdmission,
        management_credentials: Callable[[], TdsConnectionMaterial],
        helper_startup_timeout: float,
        cleanup_timeout: float,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        if (
            type(launcher) is not PythonSqlClientDepartureLauncher
            or type(management_admission) is not SqlClientObserverAdmission
            or not callable(management_credentials)
            or not callable(clock)
        ):
            raise ValueError(ERROR)
        management_admission.__post_init__()
        for value in (helper_startup_timeout, cleanup_timeout):
            deadline_nanoseconds(value)
        self._launcher = launcher
        self._management_admission = management_admission
        self._supplier: Callable[[], TdsConnectionMaterial] | None = management_credentials
        self._startup_timeout = helper_startup_timeout
        self._cleanup_timeout = cleanup_timeout
        self._clock = clock
        self._child: SqlClientDepartureProcess | None = None
        self._termination_attempted = False
        self._close_attempted = False
        self._reaped = False
        self._used = False
        self._closed = False
        self._provenance: SqlClientWriterSettlementProvenance | None = None

    @property
    def provenance(self) -> SqlClientWriterSettlementProvenance:
        if self._provenance is None:
            raise RuntimeError(ERROR)
        return self._provenance

    def _cleanup(self) -> None:
        child = self._child
        if child is None:
            return
        failure = False
        if not self._reaped and not self._termination_attempted:
            self._termination_attempted = True
            try:
                child.terminate(deadline=self._clock() + self._cleanup_timeout)
            except BaseException:
                failure = True
        elif not self._reaped:
            failure = True
        if not self._close_attempted:
            self._close_attempted = True
            try:
                child.close()
            except BaseException:
                failure = True
        else:
            failure = True
        if failure:
            raise RuntimeError(ERROR)
        self._child = None

    def observe(
        self,
        *,
        writer_observation: SqlClientWriterObservationRecord,
        writer_admission: SqlClientObserverAdmission,
        stage: SqlClientStageIdentity,
        input_descriptor: SqlClientInputDescriptor,
        expectation: SqlClientStageContentExpectation,
        operation_deadline: float,
    ) -> SqlClientWriterSettlementObservation:
        if self._used or self._closed or self._supplier is None:
            raise RuntimeError(ERROR)
        self._used = True
        deadline_nanoseconds(operation_deadline)
        try:
            _, admission_sha256, implementation_sha256, package_root, address_space = _admission(self._launcher)
            startup_deadline = min(operation_deadline, self._clock() + self._startup_timeout)
            deadline_nanoseconds(startup_deadline)
            plan = SqlClientWriterSettlementPlan(
                helper_id=uuid4(),
                attempt=writer_observation.binding.identity,
                writer_observation=writer_observation,
                writer_admission=writer_admission,
                management_admission=self._management_admission,
                stage=stage,
                input_descriptor=input_descriptor,
                expectation=expectation,
                implementation_sha256=implementation_sha256,
                package_root=package_root,
                admission_sha256=admission_sha256,
                startup_deadline=startup_deadline,
                operation_deadline=operation_deadline,
                max_address_space_bytes=address_space,
            )
            child = self._launcher.spawn(startup_deadline=startup_deadline, operation_deadline=operation_deadline)
            self._child = child
            startup = child.startup(deadline=startup_deadline)
            request = SqlClientWriterSettlementRequest(plan=plan, startup=startup)
            supplier, self._supplier = self._supplier, None
            if supplier is None:
                raise ValueError(ERROR)
            material = supplier()
            try:
                credentials = SqlClientWriterSettlementCredentials(
                    request=request,
                    request_sha256=writer_settlement_request_digest(request),
                    material=material,
                )
                child.send_request(encode_writer_settlement_credentials(credentials), deadline=operation_deadline)
            finally:
                del material, supplier
            raw_result = child.receive_result_bounded(deadline=operation_deadline, max_payload=512 * 1024)
            result = decode_writer_settlement_result(raw_result, request=request)
            local_exit = strict_exit(child.wait(deadline=operation_deadline), child.identity)
            self._reaped = True
            if local_exit.exit_code != 0:
                raise ValueError(ERROR)
            self._close_attempted = True
            child.close()
            self._child = None
            local_exit_bytes = canonical_json_bytes(
                {
                    "schema": "dpone.sqlclient.writer-settlement-local-exit.v1",
                    "identity": asdict(local_exit.identity),
                    "exit_code": local_exit.exit_code,
                    "reaped": local_exit.reaped,
                }
            )
            self._provenance = SqlClientWriterSettlementProvenance(
                startup_sha256=sha256(encode_startup(startup)).hexdigest(),
                request_sha256=sha256(encode_writer_settlement_request(request)).hexdigest(),
                result_sha256=sha256(raw_result).hexdigest(),
                local_exit_sha256=sha256(local_exit_bytes).hexdigest(),
                implementation_sha256=implementation_sha256,
                admission_sha256=admission_sha256,
            )
            return result.observation
        except BaseException:
            try:
                self._cleanup()
            except BaseException:
                pass
            raise RuntimeError(ERROR) from None

    def close(self) -> None:
        if self._closed:
            raise RuntimeError(ERROR)
        self._closed = True
        self._supplier = None
        self._cleanup()


__all__ = ("ContainedSqlClientWriterSettlementVerifier",)
