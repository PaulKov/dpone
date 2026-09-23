"""Nonsecret endpoint admission for one managed SqlClient writer.

The host-to-server mapping is supplied by the trusted composition root.  This
record binds that mapping to the exact P9 writer admission; it contains no
password and grants no permission to connect.
"""

import re
from dataclasses import dataclass
from typing import Literal, TypeAlias

from dpone.contracts import mssql_sqlclient_attempt as _attempt
from dpone.contracts import mssql_sqlclient_execution_evidence as _execution_evidence
from dpone.contracts import mssql_sqlclient_job as _job
from dpone.contracts import mssql_sqlclient_observation as _observation
from dpone.contracts import mssql_sqlclient_registration as _registration
from dpone.contracts import mssql_sqlclient_restricted_writer_settlement as _settlement
from dpone.contracts import mssql_sqlclient_result as _result
from dpone.contracts import mssql_sqlclient_session_control as _session
from dpone.contracts import mssql_sqlclient_writer_evidence as _writer_evidence
from dpone.contracts import mssql_sqlclient_writer_launch as _launch
from dpone.contracts import mssql_tds_worker as _worker
from dpone.contracts.mssql_sqlclient_credentials import SqlClientCredentials
from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
from dpone.contracts.mssql_tds_validation import _integer

ERROR = "mssql_native.sqlclient_credential_admission_invalid"


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientCredentialProfile:
    """Trusted nonsecret endpoint mapping for the exact P9 writer identity."""

    writer_admission: SqlClientObserverAdmission
    host: str
    port: int
    database: str
    username: str
    tls_profile: Literal["verified", "disposable_test"]
    allow_disposable_test: bool = False

    def __post_init__(self) -> None:
        try:
            if type(self.writer_admission) is not SqlClientObserverAdmission:
                raise ValueError
            self.writer_admission.__post_init__()
            if type(self.host) is not str or re.fullmatch(r"[A-Za-z0-9.\-:\[\]]{1,255}", self.host) is None:
                raise ValueError
            _integer(self.port, 1, 65535)
            if (
                type(self.database) is not str
                or type(self.username) is not str
                or self.database != self.writer_admission.database.database_name
                or self.username != self.writer_admission.login.name
                or self.writer_admission.transport.auth_scheme != "SQL"
                or self.writer_admission.transport.encrypt_option != "TRUE"
                or type(self.tls_profile) is not str
                or self.tls_profile not in ("verified", "disposable_test")
                or type(self.allow_disposable_test) is not bool
                or (self.tls_profile == "disposable_test" and not self.allow_disposable_test)
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError, UnicodeError):
            raise ValueError(ERROR) from None


def validate_credentials(profile: SqlClientCredentialProfile, credentials: SqlClientCredentials) -> None:
    """Require secret connection material to match the admitted endpoint exactly."""
    try:
        if type(profile) is not SqlClientCredentialProfile or type(credentials) is not SqlClientCredentials:
            raise ValueError
        profile.__post_init__()
        credentials.__post_init__()
        if (
            credentials.host,
            credentials.port,
            credentials.database,
            credentials.username,
            credentials.tls_profile,
        ) != (profile.host, profile.port, profile.database, profile.username, profile.tls_profile):
            raise ValueError
    except (ValueError, TypeError, AttributeError, UnicodeError):
        raise ValueError(ERROR) from None


LaunchIntent: TypeAlias = _launch.LaunchIntent
NativeBulkTransportPolicy: TypeAlias = _launch.NativeBulkTransportPolicy
PermissionGrantParentEvidenceKind: TypeAlias = _launch.PermissionGrantParentEvidenceKind
PermissionGrantParentEvidenceReceipt: TypeAlias = _launch.PermissionGrantParentEvidenceReceipt
ProcessRegistered: TypeAlias = _launch.ProcessRegistered
SqlClientEvidenceBinding: TypeAlias = _launch.SqlClientEvidenceBinding
SqlClientEvidenceKind: TypeAlias = _launch.SqlClientEvidenceKind
SqlClientEvidenceObservation: TypeAlias = _launch.SqlClientEvidenceObservation
SqlClientEvidenceReceipt: TypeAlias = _launch.SqlClientEvidenceReceipt
SqlClientEvidenceRecord: TypeAlias = _launch.SqlClientEvidenceRecord
SqlClientInputDescriptor: TypeAlias = _launch.SqlClientInputDescriptor
SqlClientLaunch: TypeAlias = _launch.SqlClientLaunch
SqlClientReady: TypeAlias = _launch.SqlClientReady
SqlClientRegistration: TypeAlias = _launch.SqlClientRegistration
TdsAttemptPhase: TypeAlias = _launch.TdsAttemptPhase
TdsAttemptSnapshot: TypeAlias = _launch.TdsAttemptSnapshot
TdsChildExit: TypeAlias = _launch.TdsChildExit
TdsLifecycleEvent: TypeAlias = _launch.TdsLifecycleEvent
SqlClientAttemptEvidence: TypeAlias = _attempt.SqlClientAttemptEvidence
SqlClientCredentialIntent: TypeAlias = _attempt.SqlClientCredentialIntent
SqlClientWriterObserved: TypeAlias = _attempt.SqlClientWriterObserved
SqlClientGrantIntent: TypeAlias = _attempt.SqlClientGrantIntent
SqlClientJob: TypeAlias = _job.SqlClientJob
SqlClientCredentialIntentRecord: TypeAlias = _registration.SqlClientCredentialIntentRecord
SqlClientResult: TypeAlias = _result.SqlClientResult
SqlClientSessionAnnouncement: TypeAlias = _session.SqlClientSessionAnnouncement
SqlClientBulkGrant: TypeAlias = _session.SqlClientBulkGrant
SqlClientWriterObservation: TypeAlias = _observation.SqlClientWriterObservation
SqlClientWriterObservationRecord: TypeAlias = _writer_evidence.SqlClientWriterObservationRecord
RestrictedWriterSettlementReceipt: TypeAlias = _settlement.RestrictedWriterSettlementReceipt
SqlClientLocalExit: TypeAlias = _execution_evidence.SqlClientLocalExit
SqlClientResultContext: TypeAlias = _execution_evidence.SqlClientResultContext
Exited: TypeAlias = _worker.Exited
ContainmentRequired: TypeAlias = _worker.ContainmentRequired

advance_state = _launch.advance_state
attempt_identity_digest = _launch.attempt_identity_digest
build_credential_intent = _registration.build_credential_intent
canonical_json_bytes = _launch.canonical_json_bytes
decode_session_announcement = _session.decode_session_announcement
decode_sqlclient_result = _result.decode_sqlclient_result
encode_credential_intent = _registration.encode_credential_intent
encode_input_descriptor = _launch.encode_input_descriptor
encode_job = _job.encode_job
encode_registration = _launch.encode_registration
encode_stage_identity = _launch.encode_stage_identity
input_descriptor_digest = _launch.input_descriptor_digest
job_binding_digest = _job.job_binding_digest
launch_digest = _launch.launch_digest
validate_credential_intent = _registration.validate_credential_intent
validate_job = _job.validate_job
validate_original_record = _launch.validate_original_record
validate_ready = _launch.validate_ready
validate_session_announcement = _session.validate_session_announcement
encode_bulk_grant = _session.encode_bulk_grant
validate_bulk_grant = _session.validate_bulk_grant
encode_writer_observation = _writer_evidence.encode_writer_observation
validate_writer_observation = _writer_evidence.validate_writer_observation
encode_worker_local_exit = _execution_evidence.encode_worker_local_exit
validate_writer_admission_limits = _launch.validate_writer_admission_limits
validate_writer_admission_values = _launch.validate_writer_admission_values


__all__ = (
    "SqlClientCredentialProfile",
    "SqlClientCredentials",
    "validate_credentials",
)
