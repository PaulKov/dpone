"""Compose one frozen production P7-P10f fresh-chunk execution."""

from __future__ import annotations

import math
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass
from uuid import UUID

from dpone.adapters.mssql_sqlclient_writer_observer_process import PythonSqlClientWriterObserverLauncher
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.app.mssql_sqlclient_authorization_composition import SqlClientAuthorizationInputs
from dpone.app.mssql_sqlclient_fresh_chunk_executor import FreshSqlClientChunkExecution
from dpone.app.mssql_sqlclient_prepared_attempt_factory import SqlClientPreparedAttemptFactory
from dpone.contracts.mssql_sqlclient_credential_admission import SqlClientCredentialProfile
from dpone.contracts.mssql_sqlclient_credentials import SqlClientCredentials
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1
from dpone.services.mssql_sqlclient_attempt_retirement_custody import SqlClientAttemptRetirementCustody
from dpone.services.mssql_tds_attempt import TdsAttempt

ERROR = "mssql_native.sqlclient_fresh_chunk_composition_invalid"

EvidenceWriterFactory = Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]]


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientFreshChunkDeployment:
    """Credential-safe deployment facts excluding prepared-attempt authority."""

    authorization: Callable[[TdsAttempt], SqlClientAuthorizationInputs]
    pool: TdsActorPool
    evidence_writer_factory: EvidenceWriterFactory
    startup_deadline: float
    operation_deadline: float
    termination_timeout_seconds: int
    max_worker_address_space_bytes: int
    writer_profile: SqlClientCredentialProfile
    writer_credentials: Callable[[], SqlClientCredentials]
    session_nonce: Callable[[], bytes | None]
    observer_admission: SqlClientObserverAdmission
    observer_profile: SqlClientCredentialProfile
    observer_credentials: Callable[[], SqlClientCredentials]
    observer_launcher: PythonSqlClientWriterObserverLauncher
    observer_startup_timeout: float
    observer_termination_timeout: float
    grant_id: Callable[[], UUID]
    settlement_verifier: Callable[[], object]
    clock_ns: Callable[[], int]

    def validate(self) -> None:
        """Validate deployment structure without opening credential closures."""
        if (
            type(self.pool) is not TdsActorPool
            or type(self.writer_profile) is not SqlClientCredentialProfile
            or type(self.observer_admission) is not SqlClientObserverAdmission
            or type(self.observer_profile) is not SqlClientCredentialProfile
            or type(self.observer_launcher) is not PythonSqlClientWriterObserverLauncher
            or any(
                not callable(value)
                for value in (
                    self.authorization,
                    self.evidence_writer_factory,
                    self.writer_credentials,
                    self.session_nonce,
                    self.observer_credentials,
                    self.grant_id,
                    self.settlement_verifier,
                    self.clock_ns,
                )
            )
            or any(
                type(value) is not float or not math.isfinite(value) or value <= 0
                for value in (
                    self.startup_deadline,
                    self.operation_deadline,
                    self.observer_startup_timeout,
                    self.observer_termination_timeout,
                )
            )
            or type(self.termination_timeout_seconds) is not int
            or self.termination_timeout_seconds <= 0
            or type(self.max_worker_address_space_bytes) is not int
            or self.max_worker_address_space_bytes <= 0
            or self.startup_deadline > self.operation_deadline
        ):
            raise ValueError(ERROR)


def compose_sqlclient_fresh_chunk_execution(
    deployment: SqlClientFreshChunkDeployment,
    prepared: SqlClientPreparedAttemptFactory,
    retirement_custody: SqlClientAttemptRetirementCustody,
) -> FreshSqlClientChunkExecution:
    """Bind one admitted PREPARED factory without executing P7-P10f effects."""
    if (
        type(deployment) is not SqlClientFreshChunkDeployment
        or type(prepared) is not SqlClientPreparedAttemptFactory
        or type(retirement_custody) is not SqlClientAttemptRetirementCustody
    ):
        raise ValueError(ERROR)
    deployment.validate()
    return FreshSqlClientChunkExecution(
        prepared_attempt=prepared.prepare,
        authorization=deployment.authorization,
        pool=deployment.pool,
        evidence_writer_factory=deployment.evidence_writer_factory,
        startup_deadline=deployment.startup_deadline,
        operation_deadline=deployment.operation_deadline,
        termination_timeout_seconds=deployment.termination_timeout_seconds,
        max_worker_address_space_bytes=deployment.max_worker_address_space_bytes,
        writer_profile=deployment.writer_profile,
        writer_credentials=deployment.writer_credentials,
        session_nonce=deployment.session_nonce,
        observer_admission=deployment.observer_admission,
        observer_profile=deployment.observer_profile,
        observer_credentials=deployment.observer_credentials,
        observer_launcher=deployment.observer_launcher,
        observer_startup_timeout=deployment.observer_startup_timeout,
        observer_termination_timeout=deployment.observer_termination_timeout,
        grant_id=deployment.grant_id,
        settlement_verifier=deployment.settlement_verifier,
        clock_ns=deployment.clock_ns,
        retirement_custody=retirement_custody,
    )


__all__ = ("SqlClientFreshChunkDeployment", "compose_sqlclient_fresh_chunk_execution")
