"""Production composition for one exact SqlClient PREPARED attempt context."""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.adapters.mssql_permission_preparation_capabilities import (
    AdmittedSqlClientInstallation,
    FileSqlClientInputCustody,
    PinnedEvidenceReadFactory,
    PythonSqlClientDepartureLauncher,
    PythonSqlClientObserveLauncher,
    PythonTdsCoordinatorLauncher,
    SqlClientInputCustody,
    TdsActorPool,
    plan_sha256,
)
from dpone.app.mssql_sqlclient_dependency_bundle import (
    SqlClientPreparedContextDependencies,
    prepared_context_dependencies_from_namespace,
    prepared_context_dependency_proxy,
)
from dpone.app.mssql_sqlclient_prepared_attempt_factory import (
    SqlClientPreparedAttemptContext,
    SqlClientPreparedAttemptFactory,
    prepared_input_descriptor,
)
from dpone.app.mssql_sqlclient_stage_locator_composition import _AdmittedSqlClientStoreFactory as _AF
from dpone.contracts.mssql_permission_preparation_capabilities import (
    EncodedNativeFile,
    NativeChunkPlan,
    NativeWireColumnLayout,
    SqlClientDatabasePrincipal,
    SqlClientGrantInventoryLimits,
    SqlClientGrantPrincipal,
    SqlClientObserverAdmission,
    SqlClientObserveRequest,
    TdsConnectionMaterial,
    TdsCoordinatorCommand,
    TdsCoordinatorIdentity,
    TdsCreateColumn,
    TdsCreateRequest,
    TdsDirectoryLimits,
    WindowLease,
    create_command_digest,
    deadline_nanoseconds,
)
from dpone.contracts.mssql_sqlclient_preparation_qualification import (
    AdmittedPreparationBaseline,
    PreparationBaselineAuthority,
)
from dpone.contracts.mssql_sqlclient_prepared_attempt_identity import attempt_number, operation_id, parent_identity

ERROR = "mssql_native.sqlclient_prepared_attempt_context_invalid"
_FAILURE_CLOSURE = "mssql_native.sqlclient_prepared_attempt_failure_closure"


create_tds_attempt = prepared_context_dependency_proxy("create_attempt")
run_sqlclient_create_departure_v2 = prepared_context_dependency_proxy("run_create")
settle_sqlclient_create_departure = prepared_context_dependency_proxy("settle_create")
stage_identity_from_create = prepared_context_dependency_proxy("stage_from_create")
open_sqlclient_observe = prepared_context_dependency_proxy("open_observe")
settle_prepared_observe = prepared_context_dependency_proxy("settle_observe")
_cleanup = prepared_context_dependency_proxy("cleanup_attempt")


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPreparedAttemptDeployment:
    """Admitted process, SQL, qualification and target capabilities for P7."""

    input_custody: FileSqlClientInputCustody
    pool: TdsActorPool
    admitted_factory: _AF
    directory_limits: TdsDirectoryLimits
    supervisor_token: str
    implementation_sha256: str
    database: str
    schema: str
    table: str
    owner_binding: str
    create_columns: tuple[TdsCreateColumn, ...]
    wire_columns: tuple[NativeWireColumnLayout, ...]
    max_row_bytes: int
    create_launcher: PythonTdsCoordinatorLauncher
    departure_launcher: PythonSqlClientDepartureLauncher
    observe_launcher: PythonSqlClientObserveLauncher
    creator_admission: SqlClientObserverAdmission
    creator_principal: SqlClientDatabasePrincipal
    writer_admission: SqlClientObserverAdmission
    writer_principal: SqlClientGrantPrincipal
    create_connection_material: Callable[[], TdsConnectionMaterial]
    observer_connection_material: Callable[[], TdsConnectionMaterial]
    baseline: AdmittedPreparationBaseline
    baseline_authority: PreparationBaselineAuthority
    build: AdmittedSqlClientInstallation
    evidence_root: Path
    operation_deadline: float
    create_startup_timeout: float
    helper_startup_timeout: float
    observe_startup_timeout: float
    termination_timeout: float
    inventory_limits: SqlClientGrantInventoryLimits = SqlClientGrantInventoryLimits()


@dataclass(frozen=True, slots=True, kw_only=True)
class SqlClientPreparedAttemptAuthority:
    """Frozen PREPARED facts awaiting lease-bound state-domain admission."""

    input_custody: FileSqlClientInputCustody
    pool: TdsActorPool
    directory_limits: TdsDirectoryLimits
    supervisor_token: str
    implementation_sha256: str
    database: str
    schema: str
    table: str
    owner_binding: str
    create_columns: tuple[TdsCreateColumn, ...]
    wire_columns: tuple[NativeWireColumnLayout, ...]
    max_row_bytes: int
    create_launcher: PythonTdsCoordinatorLauncher
    departure_launcher: PythonSqlClientDepartureLauncher
    observe_launcher: PythonSqlClientObserveLauncher
    creator_admission: SqlClientObserverAdmission
    creator_principal: SqlClientDatabasePrincipal
    writer_admission: SqlClientObserverAdmission
    writer_principal: SqlClientGrantPrincipal
    create_connection_material: Callable[[], TdsConnectionMaterial]
    observer_connection_material: Callable[[], TdsConnectionMaterial]
    baseline: AdmittedPreparationBaseline
    baseline_authority: PreparationBaselineAuthority
    build: AdmittedSqlClientInstallation
    evidence_root: Path
    operation_deadline: float
    create_startup_timeout: float
    helper_startup_timeout: float
    observe_startup_timeout: float
    termination_timeout: float
    inventory_limits: SqlClientGrantInventoryLimits = SqlClientGrantInventoryLimits()

    def bind(self, admitted_factory: _AF) -> SqlClientPreparedAttemptDeployment:
        """Construct the nominal deployment only after exact lease admission."""
        if type(admitted_factory) is not _AF:
            raise ValueError(ERROR)
        return SqlClientPreparedAttemptDeployment(
            admitted_factory=admitted_factory,
            **{name: getattr(self, name) for name in self.__dataclass_fields__},
        )


@contextmanager
def open_sqlclient_prepared_attempt_context(
    deployment: SqlClientPreparedAttemptDeployment,
    plan: NativeChunkPlan,
    file: EncodedNativeFile,
    attempt_id: str,
    lease: WindowLease,
    custody: SqlClientInputCustody,
    *,
    dependencies: SqlClientPreparedContextDependencies | None = None,
) -> Iterator[SqlClientPreparedAttemptContext]:
    if (
        type(deployment) is not SqlClientPreparedAttemptDeployment
        or type(deployment.admitted_factory) is not _AF
        or type(plan) is not NativeChunkPlan
        or type(file) is not EncodedNativeFile
        or type(lease) is not WindowLease
        or type(custody) is not SqlClientInputCustody
        or lease.target_id != plan.target_id
        or custody.plan_sha256 != plan_sha256(plan)
        or (custody.target_id, custody.run_id, custody.window_fingerprint)
        != (plan.target_id, plan.run_id, plan.window_fingerprint)
        or (custody.attempt_id, custody.ordinal) != (attempt_id, file.ordinal)
        or (custody.rows, custody.encoded_bytes, custody.file_sha256, custody.typed_digest)
        != (file.rows, file.encoded_bytes, file.file_sha256, file.typed_digest)
    ):
        raise ValueError(ERROR)
    attempt_number_value = attempt_number(plan, file, attempt_id)
    for value in (
        deployment.operation_deadline,
        deployment.create_startup_timeout,
        deployment.helper_startup_timeout,
        deployment.observe_startup_timeout,
        deployment.termination_timeout,
    ):
        deadline_nanoseconds(value)
    root = deployment.evidence_root / custody.durable_object_id
    create_root, observe_root = root / "create", root / "observe"
    preparation_root, settlement_root = root / "preparation", root / "observe-settlement"
    operations = dependencies or prepared_context_dependencies_from_namespace(globals())
    attempt: Any = None
    handle = None
    transferred = False
    with deployment.input_custody.open_pinned(custody) as retained_fd:
        input_fd = os.dup(retained_fd)
        input_released = False

        def release_input() -> None:
            nonlocal input_released
            if not input_released:
                os.close(input_fd)
                input_released = True

        try:
            parent_input = prepared_input_descriptor(deployment, file, input_fd)
            parent = parent_identity(deployment, plan, file, attempt_number_value)
            request = TdsCreateRequest(parent, operation_id(parent, b"create-request"), deployment.create_columns)
            identity = TdsCoordinatorIdentity(
                parent,
                0,
                operation_id(parent, b"coordinator-operation"),
                TdsCoordinatorCommand.CREATE,
                create_command_digest(request),
                lease.fence,
                deployment.implementation_sha256,
            )
            attempt = operations.create_attempt(
                deployment.pool,
                deployment.admitted_factory,
                parent,
                deployment.directory_limits,
                lease,
                supervisor_token=deployment.supervisor_token,
                deadline=deployment.operation_deadline,
                backend="mssql_sqlclient",
            )
            attempt.reserve_operation(
                identity.operation_id,
                identity.command,
                identity.command_sha256,
                deadline=deployment.operation_deadline,
            )
            outcome = operations.run_create(
                attempt,
                request,
                identity,
                observer_admission=deployment.creator_admission,
                pool=deployment.pool,
                store_factory=deployment.admitted_factory,
                lease=lease,
                evidence_root=create_root,
                create_launcher=deployment.create_launcher,
                departure_launcher=deployment.departure_launcher,
                creator_admission=deployment.creator_admission,
                creator_principal=deployment.creator_principal,
                create_connection_material=deployment.create_connection_material,
                observer_connection_material=deployment.observer_connection_material,
                operation_deadline=deployment.operation_deadline,
                create_startup_timeout=deployment.create_startup_timeout,
                helper_startup_timeout=deployment.helper_startup_timeout,
                termination_timeout=deployment.termination_timeout,
            )
            operations.settle_create(
                attempt,
                admitted_factory=deployment.admitted_factory,
                pool=deployment.pool,
                deadline=deployment.operation_deadline,
            )
            evidence = outcome.create_outcome.response.evidence
            if evidence is None:
                raise ValueError(ERROR)
            observe_request = SqlClientObserveRequest(
                parent=parent,
                selected_stage=operations.stage_from_create(evidence),
                management_admission=deployment.creator_admission,
                writer_admission=deployment.writer_admission,
                writer_principal=deployment.writer_principal,
                limits=deployment.inventory_limits,
                operation_deadline_ns=deadline_nanoseconds(deployment.operation_deadline),
            )
            handle = operations.open_observe(
                attempt,
                observe_request,
                pool=deployment.pool,
                admitted_factory=deployment.admitted_factory,
                lease=lease,
                evidence_root=observe_root,
                launcher=deployment.observe_launcher,
                connection_material=deployment.observer_connection_material,
                startup_timeout=deployment.observe_startup_timeout,
                termination_timeout=deployment.termination_timeout,
            )
            yield SqlClientPreparedAttemptContext(
                handle=handle,
                input_fd=input_fd,
                parent_input=parent_input,
                baseline=deployment.baseline,
                baseline_authority=deployment.baseline_authority,
                build=deployment.build,
                reader_factory=PinnedEvidenceReadFactory(create_root),
                evidence_root=preparation_root,
                release_input=release_input,
            )
            operations.settle_observe(
                attempt,
                admitted_factory=deployment.admitted_factory,
                pool=deployment.pool,
                evidence_root=settlement_root,
                departure_launcher=deployment.departure_launcher,
                observer_admission=deployment.creator_admission,
                management_credentials=deployment.observer_connection_material,
                deadline=deployment.operation_deadline,
                helper_startup_timeout=deployment.helper_startup_timeout,
                termination_timeout=deployment.termination_timeout,
            )
            transferred = True
        finally:
            if not transferred:
                primary = sys.exc_info()[1]
                failures: list[BaseException] = [] if primary is None else [primary]
                try:
                    operations.cleanup_attempt(handle, attempt, deadline=deployment.operation_deadline)
                except BaseException as cleanup_error:
                    failures.append(cleanup_error)
                try:
                    release_input()
                except BaseException as release_error:
                    failures.append(release_error)
                if len(failures) > (1 if primary is not None else 0):
                    if len(failures) == 1:
                        raise failures[0]
                    raise BaseExceptionGroup(_FAILURE_CLOSURE, failures) from None


def compose_sqlclient_prepared_attempt_factory(
    deployment: SqlClientPreparedAttemptDeployment,
) -> SqlClientPreparedAttemptFactory:
    if type(deployment) is not SqlClientPreparedAttemptDeployment or type(deployment.admitted_factory) is not _AF:
        raise ValueError(ERROR)

    def open_context(
        plan: NativeChunkPlan,
        file: EncodedNativeFile,
        attempt_id: str,
        lease: WindowLease,
        custody: SqlClientInputCustody,
    ):
        return open_sqlclient_prepared_attempt_context(deployment, plan, file, attempt_id, lease, custody)

    return SqlClientPreparedAttemptFactory(open_context)


__all__ = (
    "SqlClientPreparedAttemptAuthority",
    "SqlClientPreparedAttemptDeployment",
    "compose_sqlclient_prepared_attempt_factory",
    "open_sqlclient_prepared_attempt_context",
)
