"""Closed P10f helper intent and private connection delivery contracts."""

from dataclasses import dataclass, fields
from typing import Any
from uuid import UUID

from dpone.contracts.mssql_sqlclient_input import SqlClientInputDescriptor, input_descriptor_digest
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_sqlclient_stage_identity import SqlClientStageIdentity
from dpone.contracts.mssql_sqlclient_writer_evidence import SqlClientWriterObservationRecord
from dpone.contracts.mssql_sqlclient_writer_settlement import (
    SqlClientStageContentExpectation,
    SqlClientWriterSettlementObservation,
)
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_validation import _hash, _integer, deadline_nanoseconds
from dpone.contracts.mssql_tds_worker import TdsAttemptIdentity

ERROR = "mssql_native.sqlclient_writer_settlement_ipc_invalid"
# The shared length-prefixed transport has a hard 1 MiB payload ceiling.
# Keeping the domain bound identical prevents a contract that no real helper
# process could satisfy.
MAX_FRAME_BYTES = 1024 * 1024
_FAILURES = (ValueError, TypeError, OverflowError, RecursionError, UnicodeError, AttributeError)


def _exact(value: Any, cls: type) -> None:
    if type(value) is not cls:
        raise ValueError(ERROR)
    cls(**{field.name: getattr(value, field.name) for field in fields(cls)})


def _admission_from_observation(value: SqlClientWriterObservationRecord) -> SqlClientObserverAdmission:
    authority = value.authority
    return SqlClientObserverAdmission(authority.server, authority.database, authority.login, authority.transport)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientWriterSettlementPlan:
    """Nonsecret immutable facts admitted before starting the P10f helper."""

    helper_id: UUID
    attempt: TdsAttemptIdentity
    writer_observation: SqlClientWriterObservationRecord
    writer_admission: SqlClientObserverAdmission
    management_admission: SqlClientObserverAdmission
    stage: SqlClientStageIdentity
    input_descriptor: SqlClientInputDescriptor
    expectation: SqlClientStageContentExpectation
    implementation_sha256: str
    package_root: str
    admission_sha256: str
    startup_deadline: float
    operation_deadline: float
    max_address_space_bytes: int
    schema: str = "dpone.sqlclient.writer-settlement-plan.v1"

    def __post_init__(self) -> None:
        try:
            if self.schema != "dpone.sqlclient.writer-settlement-plan.v1":
                raise ValueError
            if type(self.helper_id) is not UUID or not self.helper_id.int:
                raise ValueError
            for value, cls in (
                (self.attempt, TdsAttemptIdentity),
                (self.writer_observation, SqlClientWriterObservationRecord),
                (self.writer_admission, SqlClientObserverAdmission),
                (self.management_admission, SqlClientObserverAdmission),
                (self.stage, SqlClientStageIdentity),
                (self.input_descriptor, SqlClientInputDescriptor),
                (self.expectation, SqlClientStageContentExpectation),
            ):
                _exact(value, cls)
            for digest in (self.implementation_sha256, self.admission_sha256):
                _hash(digest)
            deadline_nanoseconds(self.startup_deadline)
            deadline_nanoseconds(self.operation_deadline)
            _integer(self.max_address_space_bytes, 1)
            if (
                self.startup_deadline > self.operation_deadline
                or self.attempt != self.writer_observation.binding.identity
                or deadline_nanoseconds(self.operation_deadline) > self.writer_observation.binding.operation_deadline_ns
                or _admission_from_observation(self.writer_observation) != self.writer_admission
                or (self.writer_admission.server, self.writer_admission.database)
                != (self.management_admission.server, self.management_admission.database)
                or self.writer_admission.login == self.management_admission.login
                or self.writer_admission.login.sid == self.management_admission.login.sid
                or (
                    self.attempt.database,
                    self.attempt.schema,
                    self.attempt.table,
                    self.attempt.owner_binding,
                    self.attempt.file_sha256,
                )
                != (
                    self.stage.database_name,
                    self.stage.schema_name,
                    self.stage.table_name,
                    self.stage.owner_binding,
                    self.expectation.file_sha256,
                )
                or self.stage.database_id != self.writer_admission.database.database_id
                or self.stage.database_guid.hex != self.writer_admission.database.database_guid.replace("-", "")
                or self.input_descriptor.expected.file_sha256 != self.expectation.file_sha256
                or self.input_descriptor.expected.rows != self.expectation.rows
                or input_descriptor_digest(self.input_descriptor)
                != self.writer_observation.binding.input_binding_sha256
            ):
                raise ValueError
            if (
                type(self.package_root) is not str
                or not self.package_root.startswith("/")
                or len(self.package_root.encode()) > 4096
                or any(ord(char) < 32 or ord(char) == 127 for char in self.package_root)
            ):
                raise ValueError
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientWriterSettlementRequest:
    """Public helper request bound to the authenticated startup receipt."""

    plan: SqlClientWriterSettlementPlan
    startup: TdsCoordinatorStartup
    schema: str = "dpone.sqlclient.writer-settlement-request.v1"

    def __post_init__(self) -> None:
        try:
            if self.schema != "dpone.sqlclient.writer-settlement-request.v1":
                raise ValueError
            _exact(self.plan, SqlClientWriterSettlementPlan)
            _exact(self.startup, TdsCoordinatorStartup)
            if (
                self.startup.implementation_sha256 != self.plan.implementation_sha256
                or self.startup.package_root != self.plan.package_root
            ):
                raise ValueError
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True, repr=False)
class SqlClientWriterSettlementCredentials:
    """Private one-request connection material; never persist or log."""

    request: SqlClientWriterSettlementRequest
    request_sha256: str
    material: TdsConnectionMaterial
    schema: str = "dpone.sqlclient.writer-settlement-credentials.v1"

    def __post_init__(self) -> None:
        try:
            if self.schema != "dpone.sqlclient.writer-settlement-credentials.v1":
                raise ValueError
            _exact(self.request, SqlClientWriterSettlementRequest)
            _hash(self.request_sha256)
            _exact(self.material, TdsConnectionMaterial)
        except _FAILURES:
            raise ValueError(ERROR) from None


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientWriterSettlementResult:
    """Credential-free P10f observation bound to exact canonical request bytes."""

    helper_id: UUID
    request_sha256: str
    observation: SqlClientWriterSettlementObservation
    schema: str = "dpone.sqlclient.writer-settlement-result.v1"

    def __post_init__(self) -> None:
        try:
            if self.schema != "dpone.sqlclient.writer-settlement-result.v1":
                raise ValueError
            if type(self.helper_id) is not UUID or not self.helper_id.int:
                raise ValueError
            _hash(self.request_sha256)
            _exact(self.observation, SqlClientWriterSettlementObservation)
        except _FAILURES:
            raise ValueError(ERROR) from None


__all__ = (
    "MAX_FRAME_BYTES",
    "SqlClientWriterSettlementCredentials",
    "SqlClientWriterSettlementPlan",
    "SqlClientWriterSettlementRequest",
    "SqlClientWriterSettlementResult",
)
