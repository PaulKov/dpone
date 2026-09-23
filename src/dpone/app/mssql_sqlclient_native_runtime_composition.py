"""Production composition root for the explicit SqlClient native route."""

from __future__ import annotations

import sys
from collections.abc import Callable
from contextlib import AbstractContextManager
from dataclasses import dataclass, replace
from typing import Any

from dpone.adapters.mssql_native_route_capabilities import (
    FileSqlClientInputCustody,
    SqlClientNativeChunkImporter,
    plan_sha256,
    validate_native_chunk_receipt,
)
from dpone.app.mssql_sqlclient_fresh_chunk_executor import (
    FreshSqlClientChunkExecution,
    FreshSqlClientChunkExecutor,
)
from dpone.app.mssql_sqlclient_native_capability_bundle import (
    SqlClientNativeBindingCapabilities,
    SqlClientNativeImportCapabilitiesFactory,
)
from dpone.app.mssql_sqlclient_native_import_execution import SqlClientNativeImportExecution
from dpone.app.mssql_sqlclient_native_parent_bridges import (
    SqlClientParentCheckpoint,
    SqlClientParentChunkRetirer,
    SqlClientParentInputCustody,
    SqlClientParentJournalBridge,
    terminal_parent_result,
)
from dpone.app.mssql_sqlclient_native_parent_composition import SqlClientNativeParentCapabilities
from dpone.app.mssql_sqlclient_prepared_attempt_factory import SqlClientPreparedAttemptFactory
from dpone.contracts.mssql_native_route_capabilities import (
    NativeChunkReceipt,
    SqlClientNativeChunkProjection,
    sqlclient_physical_stage,
)
from dpone.ports.mssql_native_route_capabilities import (
    NativeActorCapacity,
    SqlClientNativeRouteBackend,
    SqlClientNativeRuntimeBinding,
)
from dpone.ports.mssql_sqlclient_failed_attempt_settlement import SqlClientFailedAttemptSettlementCapability
from dpone.runtime.mssql_native_route_capabilities import SqlClientNativeParentSettlement
from dpone.services.mssql_native_route_capabilities import (
    NativeChunkRetirementService,
    SourceFreeSqlClientChunkInspector,
)


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientNativeImportCapabilities:
    """Injected durable stores and effects; the importer retains all ordering."""

    custody: SqlClientNativeInputCustody
    executor: FreshSqlClientChunkExecutor
    inspect_projection: Callable[..., object]
    failed_settlement: SqlClientFailedAttemptSettlementCapability
    allocated_bytes: Callable[[], int]


class SqlClientNativeInputCustody:
    """One cohesive retained-file capability for import and parent settlement."""

    def __init__(self, storage: FileSqlClientInputCustody) -> None:
        if type(storage) is not FileSqlClientInputCustody:
            raise ValueError("mssql_native.sqlclient_custody_capability_invalid")
        self.storage = storage

    def retain(self, plan, file, attempt_id, _lease):
        return self.storage.retain_file(
            file.path,
            expected_size=file.encoded_bytes,
            expected_sha256=file.file_sha256,
            plan_sha256=plan_sha256(plan),
            target_id=plan.target_id,
            run_id=plan.run_id,
            window_fingerprint=plan.window_fingerprint,
            attempt_id=attempt_id,
            ordinal=file.ordinal,
            rows=file.rows,
            typed_digest=file.typed_digest,
        )

    def observe(self, _plan, receipt, _lease):
        evidence = validate_native_chunk_receipt(receipt)
        with self.storage.open_pinned(evidence.input_custody):
            pass
        return evidence.input_custody


def compose_sqlclient_native_import_capabilities(
    *,
    prepared_attempts: SqlClientPreparedAttemptFactory,
    input_custody: FileSqlClientInputCustody,
    execution: FreshSqlClientChunkExecution,
    inspect_projection: Callable[..., object],
    failed_settlement: SqlClientFailedAttemptSettlementCapability,
    allocated_bytes: Callable[[], int],
) -> SqlClientNativeImportCapabilities:
    """Close the fresh importer over the production PREPARED-attempt factory."""
    if (
        type(prepared_attempts) is not SqlClientPreparedAttemptFactory
        or type(input_custody) is not FileSqlClientInputCustody
        or type(execution) is not FreshSqlClientChunkExecution
    ):
        raise ValueError("mssql_native.sqlclient_capability_incomplete")
    bound = replace(execution, prepared_attempt=prepared_attempts.prepare)
    return SqlClientNativeImportCapabilities(
        custody=SqlClientNativeInputCustody(input_custody),
        executor=FreshSqlClientChunkExecutor(bound),
        inspect_projection=inspect_projection,
        failed_settlement=failed_settlement,
        allocated_bytes=allocated_bytes,
    )


class _SettlementJournal:
    """Explicitly delegate the complete settlement protocol and add v4 identity."""

    def __init__(self, journal: Any, identity: SqlClientParentJournalBridge) -> None:
        self._journal = journal
        self._identity = identity

    def state(self):
        return self._journal.state()

    def authority(self):
        return self._journal.authority()

    def settlement_binding(self):
        return self._identity.settlement_binding()

    def retirement_required(self, authority):
        return self._journal.retirement_required(authority)

    def retiring(self):
        return self._journal.retiring()

    def chunk_retired(self, receipt):
        return self._journal.chunk_retired(receipt)

    def retired(self):
        return self._journal.retired()

    def retirement_receipt(self):
        return self._journal.retirement_receipt()

    def checkpoint_required(self):
        return self._journal.checkpoint_required()

    def succeeded(self, receipt):
        return self._journal.succeeded(receipt)


def compose_sqlclient_native_runtime_binding(
    *,
    capabilities: SqlClientNativeBindingCapabilities | None = None,
    imports: SqlClientNativeImportCapabilities | None = None,
    parent: SqlClientNativeParentCapabilities | None = None,
    capacity: NativeActorCapacity | None = None,
    implementation_sha256: str | None = None,
    open_import_capabilities: SqlClientNativeImportCapabilitiesFactory | None = None,
) -> SqlClientNativeRuntimeBinding:
    """Build one closed runtime binding from admitted deployment capabilities."""
    if capabilities is not None:
        if type(capabilities) is not SqlClientNativeBindingCapabilities:
            raise ValueError("mssql_native.sqlclient_capability_incomplete")
        if any(
            value is not None for value in (imports, parent, capacity, implementation_sha256, open_import_capabilities)
        ):
            raise ValueError("mssql_native.sqlclient_capability_bundle_ambiguous")
        imports = capabilities.imports
        parent = capabilities.parent
        capacity = capabilities.capacity
        implementation_sha256 = capabilities.implementation_sha256
        open_import_capabilities = capabilities.open_import_capabilities
    if (
        type(imports) is not SqlClientNativeImportCapabilities
        or type(parent) is not SqlClientNativeParentCapabilities
        or type(capacity) is not NativeActorCapacity
        or type(implementation_sha256) is not str
        or not callable(open_import_capabilities)
    ):
        raise ValueError("mssql_native.sqlclient_capability_incomplete")
    actor_capacity = capacity
    implementation = implementation_sha256
    open_imports = open_import_capabilities
    if imports.custody.storage is not parent.file_custody:
        raise ValueError("mssql_native.sqlclient_custody_capability_mismatch")

    def importer(capabilities: SqlClientNativeImportCapabilities) -> SqlClientNativeChunkImporter:
        if type(capabilities) is not SqlClientNativeImportCapabilities:
            raise ValueError("mssql_native.sqlclient_capability_incomplete")
        if type(capabilities.executor) is not FreshSqlClientChunkExecutor:
            raise ValueError("mssql_native.sqlclient_capability_incomplete")
        if (
            type(capabilities.custody) is not SqlClientNativeInputCustody
            or capabilities.custody.storage is not parent.file_custody
        ):
            raise ValueError("mssql_native.sqlclient_custody_capability_mismatch")
        execution = SqlClientNativeImportExecution(capabilities.executor.execute)
        return SqlClientNativeChunkImporter(
            projection_type=SqlClientNativeChunkProjection,
            retain_input=capabilities.custody.retain,
            observe_custody=capabilities.custody.observe,
            execute=execution.execute,
            inspect_projection=capabilities.inspect_projection,
            failed_settlement=capabilities.failed_settlement,
            allocated_bytes=capabilities.allocated_bytes,
        )

    def require_canonical_imports(candidate: SqlClientNativeImportCapabilities) -> None:
        """Reject a session factory that drifts any admitted importer capability."""
        if type(candidate) is not SqlClientNativeImportCapabilities:
            raise ValueError("mssql_native.sqlclient_capability_incomplete")
        if (
            type(candidate.custody) is not SqlClientNativeInputCustody
            or candidate.custody.storage is not parent.file_custody
        ):
            raise ValueError("mssql_native.sqlclient_custody_capability_mismatch")
        if any(
            actual is not expected
            for actual, expected in (
                (candidate.custody, imports.custody),
                (candidate.executor, imports.executor),
                (candidate.inspect_projection, imports.inspect_projection),
                (candidate.failed_settlement, imports.failed_settlement),
                (candidate.allocated_bytes, imports.allocated_bytes),
            )
        ):
            raise ValueError("mssql_native.sqlclient_import_capability_identity_changed")

    identity = SqlClientParentJournalBridge(
        parent.journal,
        fence=parent.fence,
        rollback_no_commit=parent.rollback_no_commit,
    )
    inspector = SourceFreeSqlClientChunkInspector(
        parent.inspection_index,
        parent.lifecycle_observer,
        parent.directory_observer,
        parent.evidence_reader,
    )
    retirement = NativeChunkRetirementService(
        parent.retirement_state,
        parent.retirement_effects,
        parent.retirement_authorizations,
    )
    retirer = SqlClientParentChunkRetirer(
        parent.retirement_authorizations,
        retirement,
        parent.prepare_retirement,
    )
    custody = SqlClientParentInputCustody(
        parent.journal.retirement_receipt,
        parent.release_inputs,
        parent.input_custody,
    )
    checkpoint = SqlClientParentCheckpoint(parent.checkpoint_advance)
    settlement = SqlClientNativeParentSettlement(
        journal=_SettlementJournal(parent.journal, identity),
        inspector=inspector,
        retirer=retirer,
        custody=custody,
        checkpoint=checkpoint,
        directory_limits=parent.directory_limits,
    )
    parent_capabilities = parent
    control = SqlClientNativeRouteBackend(importer(imports), settlement, actor_capacity, implementation)

    class _BackendSession:
        def __init__(self, capabilities: SqlClientNativeImportCapabilities) -> None:
            self._capabilities = capabilities
            self._context: AbstractContextManager[SqlClientNativeImportCapabilities] | None = None

        def __enter__(self) -> SqlClientNativeRouteBackend:
            context = open_imports()
            capabilities = context.__enter__()
            try:
                require_canonical_imports(capabilities)
                backend = SqlClientNativeRouteBackend(
                    importer(capabilities), settlement, actor_capacity, implementation
                )
            except BaseException:
                context.__exit__(*sys.exc_info())
                raise
            self._context = context
            return backend

        def __exit__(self, exc_type, exc, traceback) -> bool | None:
            if self._context is None:
                raise RuntimeError("mssql_native.sqlclient_runtime_session_unopened")
            try:
                return self._context.__exit__(exc_type, exc, traceback)
            finally:
                parent_capabilities.close_retained()

    def physical_stage(receipt: object) -> object:
        if type(receipt) is not NativeChunkReceipt:
            raise ValueError("mssql_native.sqlclient_native_chunk_receipt_invalid")
        return sqlclient_physical_stage(receipt)

    def abort_parent(_prepared: object) -> None:
        identity.abort()

    return SqlClientNativeRuntimeBinding(
        backend=control,
        open_backend=lambda: _BackendSession(imports),
        physical_stage=physical_stage,
        abort_parent=abort_parent,
        terminal_result=terminal_parent_result,
    )


__all__ = (
    "SqlClientNativeImportCapabilities",
    "SqlClientNativeInputCustody",
    "compose_sqlclient_native_import_capabilities",
    "compose_sqlclient_native_runtime_binding",
)
