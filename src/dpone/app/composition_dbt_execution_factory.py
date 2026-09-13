"""Compose the protected native dbt worker from already verified authority.

``CompositionDbtExecutionRoot`` owns ordering policy and nothing else; this module
is the single place where its collaborators become the real protected adapters.
Separating the two keeps the root readable as a sequence and lets an offline test
substitute a boundary double without a parallel production wiring living in a
test module.

Nothing here provisions, enrolls, grants or resolves credentials. Every input is
an already verified capability:

* ``control`` is a reopened protected control session for one enrolled service,
  and every attempt, gate, capture and outcome transaction is bound to it;
* ``supervisor`` is the sealed deployment capability, and child identities are
  derived from it rather than from a directly supplied identity range;
* the materialization seams and the undispatched-closure observation are supplied
  by the activation root, because they need occurrence-scoped target authority
  that this cell must not resolve for itself.

The undispatched-closure observer is required, not optional: without it a
pre-dispatch failure could not be sealed as a protected UNDISPATCHED closure, and
the attempt would block in durable RUNNING ownership instead.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from dpone.adapters.composition_dbt_capture import LinuxDbtBuildRunner, ProtectedDbtCapture
from dpone.adapters.composition_dbt_capture_store import MssqlDbtCaptureStore
from dpone.adapters.composition_mssql_attempts import (
    MssqlCompositionAttemptStore,
    composition_control_transaction,
)
from dpone.adapters.composition_mssql_dbt_materialization import MssqlDbtMaterializationObserver
from dpone.adapters.composition_mssql_execution_evidence import persist_execution_proof
from dpone.adapters.composition_mssql_login_gate import MssqlCompositionLoginGate
from dpone.adapters.composition_mssql_schema import require_control_schema
from dpone.adapters.composition_supervisor_filesystem import protected_original_reader
from dpone.adapters.dbt_executable import current_environment_dbt_executable
from dpone.adapters.dbt_runtime import DistributionDbtToolchainInspector, SubprocessDbtCommandRunner
from dpone.app.composition_credentials import IssuedSqlCredentials, issued_dbt_process_factory
from dpone.app.composition_dbt_execution import (
    DEFAULT_COMPOSITION_PROFILES_ROOT,
    DEFAULT_COMPOSITION_SUPERVISOR_ROOT,
    CompositionDbtExecutionDependencies,
    build_composition_dbt_process_boundary,
)
from dpone.contracts.composition_activation import CompositionAdmissionError

if TYPE_CHECKING:
    from dpone.adapters.composition_mssql_dbt_materialization import OpenTarget, ReadContracts, RequireTarget
    from dpone.contracts.composition_activation import CompositionActivationOccurrence
    from dpone.contracts.composition_attempt import CompositionAttemptIdentity
    from dpone.contracts.composition_supervisor import CompositionSupervisorProjection
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.ports.sql_connection import SqlControlConnection

ControlConnectionFactory = Callable[[], "SqlControlConnection"]
ClosureObserver = Callable[..., bytes]
ChildEnvironment = Callable[[], Mapping[str, str]]


@dataclass(frozen=True, slots=True)
class CompositionDbtControlAuthority:
    """One reopened protected control session shared by every parent transaction.

    The service UUID comes from the verified registry descriptor, never from an
    argument this cell invented. ``connection_factory`` is called once per short
    control transaction; execution never holds a control connection.
    """

    connection_factory: ControlConnectionFactory
    expected_service_id: str
    control_database: str
    control_schema: str = "dpone_control"

    def __post_init__(self) -> None:
        if not callable(self.connection_factory):
            raise CompositionAdmissionError("control_connection")
        try:
            valid = str(UUID(self.expected_service_id)) == self.expected_service_id
        except (AttributeError, TypeError, ValueError):
            valid = False
        if not valid:
            raise CompositionAdmissionError("control_service_id")
        require_control_schema(self.control_database)
        require_control_schema(self.control_schema)


def build_composition_dbt_execution_dependencies(
    *,
    supervisor: CompositionSupervisorProjection,
    control: CompositionDbtControlAuthority,
    read_active: Callable[[], CompositionActivationOccurrence],
    target: ResolvedBindingConnection,
    open_materialization_target: OpenTarget,
    require_materialization_target: RequireTarget,
    observe_undispatched_closure: ClosureObserver,
    dbt_executable: str | None = None,
    supervisor_root: Path = DEFAULT_COMPOSITION_SUPERVISOR_ROOT,
    profiles_root: Path = DEFAULT_COMPOSITION_PROFILES_ROOT,
    preflight_popen: Callable[..., Any] = subprocess.Popen,
) -> CompositionDbtExecutionDependencies:
    """Return the protected collaborators of one supervised native dbt cell."""

    control.__post_init__()
    if not callable(observe_undispatched_closure):
        raise CompositionAdmissionError("worker_undispatched_observer")
    root = Path(supervisor_root)
    boundary = build_composition_dbt_process_boundary(
        supervisor=supervisor,
        supervisor_root=root,
        profiles_root=Path(profiles_root),
    )
    executable = dbt_executable or current_environment_dbt_executable()

    def capture_store(source_verifier: Callable[..., Any]) -> MssqlDbtCaptureStore:
        return MssqlDbtCaptureStore(
            control.connection_factory,
            expected_service_id=control.expected_service_id,
            control_database=control.control_database,
            source_verifier=source_verifier,
            control_schema=control.control_schema,
            closure_verifier=observe_undispatched_closure,
        )

    def capture(store: Any, environment: ChildEnvironment) -> ProtectedDbtCapture:
        return ProtectedDbtCapture(store, LinuxDbtBuildRunner(_attempt_bound(environment)))

    def materializations(read_contracts: ReadContracts) -> MssqlDbtMaterializationObserver:
        return MssqlDbtMaterializationObserver(
            read_contracts=read_contracts,
            open_target=open_materialization_target,
            require_target=require_materialization_target,
        )

    def preflight_command_runner(
        attempt: CompositionAttemptIdentity,
        credentials: IssuedSqlCredentials,
    ) -> SubprocessDbtCommandRunner:
        return SubprocessDbtCommandRunner(
            popen_factory=issued_dbt_process_factory(
                preflight_popen,
                attempt=attempt,
                credentials=credentials,
            ),
            dbt_executable=executable,
        )

    return CompositionDbtExecutionDependencies(
        supervisor=supervisor,
        read_active=read_active,
        attempts=MssqlCompositionAttemptStore(
            control.connection_factory,
            expected_service_id=control.expected_service_id,
            control_schema=control.control_schema,
        ),
        gate=MssqlCompositionLoginGate(
            control.connection_factory,
            expected_service_id=control.expected_service_id,
            control_database=control.control_database,
            control_schema=control.control_schema,
        ),
        boundary=boundary,
        capture_store=capture_store,
        capture=capture,
        materializations=materializations,
        outcome_proof_writer=persist_execution_proof,
        expected_service_id=control.expected_service_id,
        toolchain_inspector=DistributionDbtToolchainInspector(),
        preflight_command_runner_factory=preflight_command_runner,
        manifest_reader=protected_original_reader,
        outcome_transaction=lambda: composition_control_transaction(
            control.connection_factory,
            control.control_schema,
            control.expected_service_id,
        ),
        target=target,
        dbt_executable=executable,
    )


def _attempt_bound(environment: ChildEnvironment) -> Callable[[CompositionAttemptIdentity], Mapping[str, str]]:
    """Adapt the already attempt-scoped issued provider to the Linux runner.

    ``issued_dbt_child_environment`` binds one attempt and one issued credential
    at construction, so the runner's argument carries no additional authority
    here and deliberately cannot select a different environment.
    """

    def provide(_attempt: CompositionAttemptIdentity) -> Mapping[str, str]:
        return environment()

    return provide


__all__ = [
    "CompositionDbtControlAuthority",
    "build_composition_dbt_execution_dependencies",
]
