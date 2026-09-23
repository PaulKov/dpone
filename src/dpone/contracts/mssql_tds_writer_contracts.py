"""Service-side import boundary for the cohesive private P10 contract."""

# ruff: noqa: F401

from typing import TypeAlias

from dpone.contracts import mssql_sqlclient_credential_admission as _contracts
from dpone.contracts.bounded_window import (
    WindowContractError,
    WindowLease,
    WindowOutcomeUnknown,
    WindowRecord,
    window_record_ack_matches,
)
from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_sqlclient_create_evidence import (
    MAX_CREATE_SEAL_BYTES,
    SqlClientCreateSeal,
    create_seal_key,
    decode_create_seal,
    encode_create_seal,
    snapshot_create_state,
    validate_create_lineage,
)
from dpone.contracts.mssql_sqlclient_departure_evidence import SqlClientDepartureEvidenceRecord
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceKind,
    SqlClientDepartureEvidenceObservation,
    SqlClientDepartureEvidenceReceipt,
)
from dpone.contracts.mssql_sqlclient_evidence_types import evidence_limit
from dpone.contracts.mssql_sqlclient_observation import session_authority_digest
from dpone.contracts.mssql_sqlclient_observe import SqlClientObserveRequest
from dpone.contracts.mssql_sqlclient_observe_departure import (
    SqlClientObserveContainment,
    SqlClientObserveContainmentObservation,
    SqlClientObserveDeparturePlan,
    SqlClientObserveDepartureRequest,
    SqlClientObserveDepartureResult,
)
from dpone.contracts.mssql_sqlclient_observe_departure_codec import (
    decode_observe_departure_result,
    encode_observe_containment,
    observe_containment_receipt,
)
from dpone.contracts.mssql_sqlclient_observe_departure_evidence import (
    decode_observe_departure_evidence,
    validate_observe_exclusion_chain,
)
from dpone.contracts.mssql_sqlclient_observe_handshake import validate_authority
from dpone.contracts.mssql_sqlclient_permission_grant import (
    decode_permission_grant_evidence,
    encode_permission_grant_evidence,
    encode_permission_grant_request,
    validate_permission_binding,
)
from dpone.contracts.mssql_sqlclient_permission_grant_parent_evidence import (
    PermissionGrantEvidenceSubject,
    PermissionGrantHeldReadyEvidence,
    PermissionGrantParentEvidenceRecord,
    encode_permission_grant_held_ready_evidence,
)
from dpone.contracts.mssql_sqlclient_permission_grant_settlement import (
    RELEASE_KIND,
    RELEASED_KIND,
    PermissionGrantSettlementEvidenceContext,
    PermissionGrantSettlementEvidenceReceipt,
    PermissionGrantSettlementEvidenceRecord,
    PermissionGrantSettlementEvidenceSubject,
    encode_observed_permission_message,
    encode_permission_grant_local_exit,
    encode_permission_grant_release,
    permission_grant_settlement_subject,
    permission_grant_settlement_subject_snapshot,
    require_permission_grant_local_exit,
)
from dpone.contracts.mssql_sqlclient_permission_grant_wire import (
    PermissionBoundary,
    PermissionWireBinding,
    PermissionWireKind,
    encode_permission_message,
)
from dpone.contracts.mssql_sqlclient_preparation import (
    PROFILE,
    QUERY_PROFILE,
    PreparationObservation,
    PreparationReceipt,
    parse_baseline,
)
from dpone.contracts.mssql_sqlclient_session_control import SqlClientDatabasePrincipal
from dpone.contracts.mssql_sqlclient_stage_locator import (
    STATE_DOMAIN_KEY,
    SqlClientStageLocator,
    SqlClientStageLocatorSnapshot,
    SqlClientStageLookup,
    decode_stage_locator_record,
    encode_stage_locator,
    encode_state_domain,
    stage_locator_key,
    validate_locator_request,
    validate_state_domain_record,
)
from dpone.contracts.mssql_sqlclient_writer_settlement import (
    SqlClientStageContentExpectation,
    SqlClientWriterSettlementObservation,
    SqlClientWriterSettlementProvenance,
    SqlClientWriterSettlementRecord,
    encode_writer_settlement,
)
from dpone.contracts.mssql_tds_attempt_reservation import (
    validate_prepared_grant_binding,
    validate_prepared_verify_binding,
    validate_reservation_binding,
)
from dpone.contracts.mssql_tds_coordinator import (
    TdsCoordinatorIdentity,
    initial_coordinator_state,
)
from dpone.contracts.mssql_tds_coordinator_authority import (
    TdsCoordinatorAuthority,
    authority_digest,
    decode_authority,
    encode_authority,
)
from dpone.contracts.mssql_tds_coordinator_evidence import (
    TdsCoordinatorEvidenceObservation,
    TdsCoordinatorEvidenceReceipt,
    TdsCoordinatorEvidenceRecord,
)
from dpone.contracts.mssql_tds_coordinator_ipc import TdsCoordinatorStartup
from dpone.contracts.mssql_tds_create import TdsCreateRequest
from dpone.contracts.mssql_tds_directory import (
    TdsDirectorySnapshot,
    TdsLocalContainment,
    TdsRemoteSettlement,
    record_local_containment,
    record_remote_settlement,
)
from dpone.contracts.mssql_tds_directory_codec import process_identity_digest
from dpone.contracts.mssql_tds_session import (
    TdsRemoteSessionIdentity,
    encode_session_identity,
)
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds
from dpone.contracts.mssql_tds_worker import (
    Prepared,
    RetirementRequired,
    TdsAttemptOwnership,
    TdsAttemptState,
    TdsProcessIdentity,
    Verified,
    initial_state,
)
from dpone.contracts.mssql_tds_worker_identity import (
    _integer,
    _text,
)
from dpone.contracts.strict_json import strict_json_object

LaunchIntent: TypeAlias = _contracts.LaunchIntent
NativeBulkTransportPolicy: TypeAlias = _contracts.NativeBulkTransportPolicy
PermissionGrantParentEvidenceKind: TypeAlias = _contracts.PermissionGrantParentEvidenceKind
PermissionGrantParentEvidenceReceipt: TypeAlias = _contracts.PermissionGrantParentEvidenceReceipt
ProcessRegistered: TypeAlias = _contracts.ProcessRegistered
SqlClientEvidenceBinding: TypeAlias = _contracts.SqlClientEvidenceBinding
SqlClientEvidenceKind: TypeAlias = _contracts.SqlClientEvidenceKind
SqlClientEvidenceObservation: TypeAlias = _contracts.SqlClientEvidenceObservation
SqlClientEvidenceReceipt: TypeAlias = _contracts.SqlClientEvidenceReceipt
SqlClientEvidenceRecord: TypeAlias = _contracts.SqlClientEvidenceRecord
SqlClientInputDescriptor: TypeAlias = _contracts.SqlClientInputDescriptor
SqlClientLaunch: TypeAlias = _contracts.SqlClientLaunch
SqlClientReady: TypeAlias = _contracts.SqlClientReady
SqlClientRegistration: TypeAlias = _contracts.SqlClientRegistration
TdsAttemptPhase: TypeAlias = _contracts.TdsAttemptPhase
TdsAttemptSnapshot: TypeAlias = _contracts.TdsAttemptSnapshot
TdsChildExit: TypeAlias = _contracts.TdsChildExit
TdsLifecycleEvent: TypeAlias = _contracts.TdsLifecycleEvent
SqlClientAttemptEvidence: TypeAlias = _contracts.SqlClientAttemptEvidence
SqlClientCredentialIntent: TypeAlias = _contracts.SqlClientCredentialIntent
SqlClientWriterObserved: TypeAlias = _contracts.SqlClientWriterObserved
SqlClientGrantIntent: TypeAlias = _contracts.SqlClientGrantIntent
SqlClientCredentialProfile: TypeAlias = _contracts.SqlClientCredentialProfile
SqlClientCredentials: TypeAlias = _contracts.SqlClientCredentials
SqlClientJob: TypeAlias = _contracts.SqlClientJob
encode_job = _contracts.encode_job
job_binding_digest, validate_job = _contracts.job_binding_digest, _contracts.validate_job
SqlClientCredentialIntentRecord: TypeAlias = _contracts.SqlClientCredentialIntentRecord
build_credential_intent = _contracts.build_credential_intent
encode_credential_intent = _contracts.encode_credential_intent
validate_credential_intent = _contracts.validate_credential_intent
SqlClientResult: TypeAlias = _contracts.SqlClientResult
decode_sqlclient_result = _contracts.decode_sqlclient_result
SqlClientSessionAnnouncement: TypeAlias = _contracts.SqlClientSessionAnnouncement
SqlClientObserverAdmission: TypeAlias = _contracts.SqlClientObserverAdmission
SqlClientWriterObservation: TypeAlias = _contracts.SqlClientWriterObservation
SqlClientWriterObservationRecord: TypeAlias = _contracts.SqlClientWriterObservationRecord
SqlClientBulkGrant: TypeAlias = _contracts.SqlClientBulkGrant
RestrictedWriterSettlementReceipt: TypeAlias = _contracts.RestrictedWriterSettlementReceipt
SqlClientLocalExit: TypeAlias = _contracts.SqlClientLocalExit
SqlClientResultContext: TypeAlias = _contracts.SqlClientResultContext
Exited: TypeAlias = _contracts.Exited
ContainmentRequired: TypeAlias = _contracts.ContainmentRequired
decode_session_announcement = _contracts.decode_session_announcement
validate_session_announcement = _contracts.validate_session_announcement
validate_credentials = _contracts.validate_credentials
encode_writer_observation = _contracts.encode_writer_observation
validate_writer_observation = _contracts.validate_writer_observation
encode_worker_local_exit = _contracts.encode_worker_local_exit
encode_bulk_grant = _contracts.encode_bulk_grant
validate_bulk_grant = _contracts.validate_bulk_grant
advance_state = _contracts.advance_state
attempt_identity_digest = _contracts.attempt_identity_digest
canonical_json_bytes = _contracts.canonical_json_bytes
encode_input_descriptor = _contracts.encode_input_descriptor
encode_registration = _contracts.encode_registration
encode_stage_identity = _contracts.encode_stage_identity
input_descriptor_digest = _contracts.input_descriptor_digest
launch_digest = _contracts.launch_digest
validate_original_record = _contracts.validate_original_record
validate_ready = _contracts.validate_ready
validate_writer_admission_limits = _contracts.validate_writer_admission_limits
validate_writer_admission_values = _contracts.validate_writer_admission_values

__all__ = (
    "LaunchIntent",
    "NativeBulkTransportPolicy",
    "PermissionGrantParentEvidenceKind",
    "PermissionGrantParentEvidenceReceipt",
    "ProcessRegistered",
    "SqlClientEvidenceBinding",
    "SqlClientEvidenceKind",
    "SqlClientEvidenceObservation",
    "SqlClientEvidenceReceipt",
    "SqlClientEvidenceRecord",
    "SqlClientInputDescriptor",
    "SqlClientLaunch",
    "SqlClientReady",
    "SqlClientRegistration",
    "SqlClientAttemptEvidence",
    "SqlClientCredentialIntent",
    "SqlClientWriterObserved",
    "SqlClientGrantIntent",
    "SqlClientCredentialIntentRecord",
    "SqlClientCredentialProfile",
    "SqlClientCredentials",
    "SqlClientJob",
    "SqlClientResult",
    "SqlClientSessionAnnouncement",
    "SqlClientObserverAdmission",
    "SqlClientWriterObservation",
    "SqlClientWriterObservationRecord",
    "SqlClientBulkGrant",
    "RestrictedWriterSettlementReceipt",
    "SqlClientLocalExit",
    "SqlClientResultContext",
    "Exited",
    "ContainmentRequired",
    "TdsAttemptPhase",
    "TdsAttemptSnapshot",
    "TdsChildExit",
    "TdsLifecycleEvent",
    "advance_state",
    "attempt_identity_digest",
    "build_credential_intent",
    "canonical_json_bytes",
    "encode_input_descriptor",
    "encode_credential_intent",
    "encode_job",
    "encode_registration",
    "encode_stage_identity",
    "input_descriptor_digest",
    "job_binding_digest",
    "launch_digest",
    "validate_original_record",
    "validate_credential_intent",
    "validate_credentials",
    "encode_writer_observation",
    "validate_writer_observation",
    "encode_worker_local_exit",
    "encode_bulk_grant",
    "validate_bulk_grant",
    "validate_job",
    "decode_session_announcement",
    "decode_sqlclient_result",
    "validate_session_announcement",
    "validate_ready",
    "validate_writer_admission_limits",
    "validate_writer_admission_values",
)

# Canonical leaf symbols shared by more than one service collaborator.
from dpone.contracts.mssql_sqlclient_permission_grant_settlement import (  # noqa: E402
    PermissionGrantSettlementEvidenceKind,
)
from dpone.contracts.mssql_tds_coordinator import (  # noqa: E402
    CoordinatorCredentialIntent,
    CoordinatorGrantIntent,
    CoordinatorProcessRegistered,
    CoordinatorResultReceived,
    CoordinatorSessionRegistered,
    TdsCoordinatorEvent,
    TdsCoordinatorGrant,
    TdsCoordinatorPhase,
    TdsCoordinatorResult,
    TdsCoordinatorResultKind,
    TdsCoordinatorSnapshot,
    advance_coordinator_state,
    coordinator_grant_digest,
    coordinator_identity_digest,
)
from dpone.contracts.mssql_tds_coordinator_evidence import TdsCoordinatorEvidenceKind  # noqa: E402
from dpone.contracts.mssql_tds_coordinator_ipc import decode_startup  # noqa: E402
from dpone.contracts.mssql_tds_coordinator_ipc import (  # noqa: E402
    encode_registration as encode_coordinator_registration,
)
from dpone.contracts.mssql_tds_directory import (  # noqa: E402
    TdsCoordinatorCommand,
    directory_key,
    reserve_operation,
)
from dpone.contracts.mssql_tds_worker import (  # noqa: E402
    Contained,
    Running,
    TdsAttemptError,
)
