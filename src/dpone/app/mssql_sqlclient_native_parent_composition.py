"""Production composition for source-free SqlClient parent settlement."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from dpone.adapters.mssql_native_route_capabilities import (
    ExactNativeChunkInspectionEvidence,
    FileSqlClientInputCustody,
    NativeChunkJournal,
    NativeChunkRetirementJournalObserver,
    NativeParentJournalInspectionIndex,
    SqlClientCheckpointCas,
    WindowStoreSqlClientNativeRetirementState,
)
from dpone.app.mssql_sqlclient_native_parent_bridges import DurableInputCustody, SqlClientParentJournalBridge
from dpone.app.mssql_sqlclient_native_retirement_composition import (
    SqlClientNativeRetirementDeployment,
    compose_sqlclient_native_retirement_effects,
)
from dpone.app.mssql_sqlclient_parent_checkpoint_bridge import SqlClientParentCheckpointBridge
from dpone.app.mssql_sqlclient_parent_input_releaser import SqlClientParentInputReleaser
from dpone.contracts.mssql_native_chunk_retirement_authority import NativeChunkRetirementRequest
from dpone.contracts.mssql_native_route_capabilities import NativeCheckpointReceipt, TdsDirectoryLimits
from dpone.contracts.mssql_tds_worker import TdsAttemptPhase
from dpone.ports.mssql_native_route_capabilities import (
    ExactEvidenceReaderV1,
    NativeCheckpointRequest,
    TdsAttemptObserver,
    TdsDirectoryObserver,
)
from dpone.services.mssql_sqlclient_attempt_retirement_custody import SqlClientAttemptRetirementCustody


class SqlClientParentJournalFacade:
    """Expose one v4 chunk snapshot and its co-owned publication transitions."""

    def __init__(self, journal: NativeChunkJournal) -> None:
        if type(journal) is not NativeChunkJournal or journal.parent_schema_version != 4:
            raise ValueError("mssql_native.sqlclient_parent_composition_invalid")
        self._journal = journal
        self._publication = journal.publication

    @property
    def data(self) -> dict[str, Any] | None:
        return self._journal.data

    def state(self):
        return self._publication.state()

    def authority(self):
        return self._publication.authority()

    def abort_required(self):
        return self._publication.abort_required()

    def abort_confirmed(self, receipt):
        return self._publication.abort_confirmed(receipt)

    def retirement_required(self, authority):
        return self._publication.retirement_required(authority)

    def retiring(self):
        return self._publication.retiring()

    def chunk_retired(self, receipt):
        return self._publication.chunk_retired(receipt)

    def retired(self):
        return self._publication.retired()

    def retirement_receipt(self):
        return self._publication.retirement_receipt()

    def checkpoint_required(self):
        return self._publication.checkpoint_required()

    def succeeded(self, receipt):
        return self._publication.succeeded(receipt)


class _CheckpointJournal:
    def __init__(self, facade: SqlClientParentJournalFacade, identity: SqlClientParentJournalBridge) -> None:
        self._facade = facade
        self._identity = identity

    def settlement_binding(self):
        return self._identity.settlement_binding()

    def retirement_receipt(self):
        return self._facade.retirement_receipt()


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeParentCapabilities:
    """Concrete existing ports needed by source-free parent settlement."""

    file_custody: FileSqlClientInputCustody
    journal: Any
    fence: Callable[[], int]
    rollback_no_commit: Callable[[], dict[str, Any]]
    inspection_index: Any
    lifecycle_observer: Any
    directory_observer: Any
    evidence_reader: Any
    retirement_state: Any
    retirement_effects: Any
    retirement_authorizations: Any
    release_inputs: Callable[..., str]
    input_custody: DurableInputCustody
    checkpoint_advance: Callable[[NativeCheckpointRequest], NativeCheckpointReceipt]
    directory_limits: TdsDirectoryLimits
    prepare_retirement: Callable[[NativeChunkRetirementRequest], None]
    close_retained: Callable[[], None]


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeParentDeployment:
    """Deployment-owned durable effects used by the parent composition root."""

    journal: NativeChunkJournal
    lifecycle_observer: TdsAttemptObserver
    directory_observer: TdsDirectoryObserver
    rollback_no_commit: Callable[[], dict[str, Any]]
    evidence_reader: ExactEvidenceReaderV1
    retirement: SqlClientNativeRetirementDeployment
    file_custody: FileSqlClientInputCustody
    input_custody: DurableInputCustody
    checkpoint: SqlClientCheckpointCas
    directory_limits: TdsDirectoryLimits
    attempt_retirement_custody: SqlClientAttemptRetirementCustody
    recover_verified_retirement: Callable[[NativeChunkRetirementRequest, float], None]


def compose_sqlclient_native_parent_capabilities(
    deployment: SqlClientNativeParentDeployment,
) -> SqlClientNativeParentCapabilities:
    """Bind one fenced v4 parent to exact inspection, retirement and custody."""
    if type(deployment) is not SqlClientNativeParentDeployment:
        raise ValueError("mssql_native.sqlclient_parent_composition_invalid")
    journal = deployment.journal
    facade = SqlClientParentJournalFacade(journal)
    retirement = deployment.retirement
    if (
        type(retirement) is not SqlClientNativeRetirementDeployment
        or retirement.lifecycle_observer is not deployment.lifecycle_observer
        or retirement.directory_observer is not deployment.directory_observer
        or retirement.directory_limits is not deployment.directory_limits
    ):
        raise ValueError("mssql_native.sqlclient_parent_composition_invalid")
    retirement_state = WindowStoreSqlClientNativeRetirementState(
        journal.store,
        journal.lease,
        attempts=deployment.lifecycle_observer,
        directories=deployment.directory_observer,
        directory_limits=deployment.directory_limits,
    )
    retirement_effects = compose_sqlclient_native_retirement_effects(retirement, retirement_state)
    authorizations = NativeChunkRetirementJournalObserver(journal)
    identity = SqlClientParentJournalBridge(
        facade,
        fence=lambda: journal.lease.fence,
        rollback_no_commit=deployment.rollback_no_commit,
    )

    checkpoint = SqlClientParentCheckpointBridge(
        journal=_CheckpointJournal(facade, identity), checkpoint=deployment.checkpoint
    )

    def checkpoint_advance(request: NativeCheckpointRequest) -> NativeCheckpointReceipt:
        return checkpoint.advance(request)

    def prepare_retirement(request: NativeChunkRetirementRequest) -> None:
        retirement_state.observe(request)
        observed = deployment.lifecycle_observer.read(request.projection.attempt)
        if observed is None:
            raise RuntimeError("mssql_native.sqlclient_retirement_lifecycle_missing")
        if observed.state.phase in (TdsAttemptPhase.RETIREMENT_REQUIRED, TdsAttemptPhase.RETIRED):
            return
        if observed.state.phase not in (TdsAttemptPhase.VERIFIED, TdsAttemptPhase.CONTAINED):
            raise RuntimeError("mssql_native.sqlclient_retirement_lifecycle_invalid")
        deadline = retirement.monotonic() + retirement.operation_timeout
        try:
            deployment.attempt_retirement_custody.prepare_verified(request, deadline=deadline)
        except LookupError:
            deployment.recover_verified_retirement(request, deadline)
            deployment.attempt_retirement_custody.prepare_verified(request, deadline=deadline)

    return SqlClientNativeParentCapabilities(
        file_custody=deployment.file_custody,
        journal=facade,
        fence=lambda: journal.lease.fence,
        rollback_no_commit=deployment.rollback_no_commit,
        inspection_index=NativeParentJournalInspectionIndex(journal),
        lifecycle_observer=deployment.lifecycle_observer,
        directory_observer=deployment.directory_observer,
        evidence_reader=ExactNativeChunkInspectionEvidence(deployment.evidence_reader),
        retirement_state=retirement_state,
        retirement_effects=retirement_effects,
        retirement_authorizations=authorizations,
        release_inputs=SqlClientParentInputReleaser(journal, deployment.file_custody),
        input_custody=deployment.input_custody,
        checkpoint_advance=checkpoint_advance,
        directory_limits=deployment.directory_limits,
        prepare_retirement=prepare_retirement,
        close_retained=deployment.attempt_retirement_custody.close_all,
    )


__all__ = (
    "SqlClientNativeParentDeployment",
    "SqlClientParentJournalFacade",
    "compose_sqlclient_native_parent_capabilities",
)
