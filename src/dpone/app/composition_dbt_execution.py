"""Composition root that executes one native dbt build under parent authority.

This is the only place where the protected parent attempt store, login gate,
child-identity boundary, issued-credential renderer, dispatch fence, capture
store, materialization observer, OUTCOME proof producer and the shared dbt
execution engine are wired together. Every collaborator is injected, so a
deployment can substitute a real protected adapter without changing policy here.

Ordering is a security property, not an implementation detail:

1. reopen the ACTIVE parent occurrence and derive the exact attempt identity;
2. reserve the attempt atomically, then reserve its dedicated child identity;
3. issue one-time SQL credentials that never leave memory, argv or evidence;
4. run non-mutating preflight and pin the root-owned preflight manifest bytes;
5. register the derived dispatch intent, launch the child once, capture originals
   only after the runtime has written its final execution evidence;
6. close the login gate, prove quiescence, reopen the actual business outcome and
   persist an immutable OUTCOME proof before the attempt is finalized.

A native-v2 workspace attempt is never constructed for an authenticated v3
execution, and no process return value alone can terminalize a parent attempt.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.adapters.composition_child_identity_allocator import CompositionChildIdentityAllocator
from dpone.adapters.composition_dbt_command_runner import ProtectedDbtCommandRunner
from dpone.adapters.composition_dbt_process_boundary import LinuxDbtProcessBoundary
from dpone.adapters.composition_supervisor_filesystem import protected_original_reader
from dpone.adapters.dbt_artifacts import LocalDbtExecutionEvidenceWriter, LocalDbtRunResultsReader
from dpone.adapters.dbt_run_results_schema import OfficialDbtRunResultsValidator
from dpone.app.composition_credentials import IssuedDbtProfileRenderer, issued_dbt_child_environment
from dpone.app.composition_dbt_attempt_session import (
    AdmittedExecutionSlot,
    DbtChildProcessBoundary,
    NativeDbtAttemptStore,
    SupervisedDbtSession,
    TerminalDbtOutcome,
    observed_child_allocation,
)
from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionAdmissionError,
)
from dpone.contracts.composition_attempt import CompositionAttemptIdentity, CompositionAttemptReceipt
from dpone.contracts.composition_dbt_materialization import DbtMaterializationContract
from dpone.contracts.composition_dbt_outcome import DbtDispatchIntent, DbtOutcomeExpectation
from dpone.contracts.composition_execution_authority import supervisor_from_transport
from dpone.contracts.composition_supervisor import CompositionSupervisorProjection
from dpone.contracts.dbt_runtime import (
    AirflowAttemptCorrelation,
    AirflowRunIdentity,
    DbtExecutionInterval,
    DbtExecutionPack,
)
from dpone.contracts.runtime_connection import ResolvedBindingConnection
from dpone.ports.dbt_publishing import DbtCommandRunner, DbtExecutionOutcome, DbtToolchainInspector
from dpone.runtime.composition_dbt_bootstrap import (
    build_dbt_runtime_preflight,
    composition_dbt_trusted_commands,
)
from dpone.runtime.dbt_execution_policy import confined_dbt_project_directory
from dpone.runtime.dbt_execution_service import DbtExecutionService
from dpone.services.composition_dbt_attempt import (
    CompositionDbtAttemptLifecycle,
    build_composition_dbt_attempt,
)
from dpone.services.composition_dbt_capture_authority import (
    EXECUTION_EVIDENCE_ARTIFACT,
    PREFLIGHT_MANIFEST_ARTIFACT,
    CompositionDbtCaptureAuthority,
)
from dpone.services.composition_worker import CompositionWorker

DEFAULT_COMPOSITION_SUPERVISOR_ROOT = Path("/var/lib/dpone/composition")
DEFAULT_COMPOSITION_PROFILES_ROOT = Path("/dev/shm/dpone-composition")

ChildEnvironment = Callable[[], Mapping[str, str]]
ReadContracts = Callable[[CompositionAttemptIdentity], tuple[DbtMaterializationContract, ...]]
SourceVerifier = Callable[[CompositionAttemptIdentity], tuple[DbtDispatchIntent, DbtOutcomeExpectation]]
MaterializationObserver = Callable[..., bytes]


@dataclass(frozen=True, slots=True)
class CompositionDbtExecutionRequest:
    """One verified pack invocation bound to its immutable deployment identity."""

    pack: DbtExecutionPack
    run_identity: AirflowRunIdentity
    airflow_attempt: AirflowAttemptCorrelation
    runtime_root: Path
    interval: DbtExecutionInterval


@dataclass(frozen=True, slots=True)
class CompositionDbtExecutionDependencies:
    """Injected protected capabilities; this root owns no ambient client.

    ``capture_store``, ``capture`` and ``materializations`` are factories because
    the real adapters must receive the per-attempt source verifier, issued child
    environment and declared-contract reader at construction time, which only
    exist after admission and issuance.

    ``manifest_reader`` defaults to the protected no-follow supervisor reader, so
    a deployment cannot accidentally bind the preflight authority to an ordinary
    read that would follow a symlink on the shared supervisor volume. Only an
    offline test may substitute it, and off-supervisor the default fails closed.
    """

    supervisor: CompositionSupervisorProjection
    read_active: Callable[[], CompositionActivationOccurrence]
    attempts: NativeDbtAttemptStore
    gate: Any
    boundary: DbtChildProcessBoundary
    capture_store: Callable[[SourceVerifier], Any]
    capture: Callable[[Any, ChildEnvironment], Any]
    materializations: Callable[[ReadContracts], MaterializationObserver]
    outcome_proof_writer: Callable[..., None]
    expected_service_id: str
    toolchain_inspector: DbtToolchainInspector
    preflight_command_runner: DbtCommandRunner
    outcome_transaction: Callable[[], Any]
    target: ResolvedBindingConnection
    dbt_executable: str
    manifest_reader: Callable[[Path, str], Callable[[], bytes]] = protected_original_reader


class CompositionDbtExecutionRoot:
    """Execute one native dbt pack as a supervised parent attempt."""

    def __init__(self, dependencies: CompositionDbtExecutionDependencies) -> None:
        if type(dependencies.supervisor) is not CompositionSupervisorProjection:
            raise CompositionAdmissionError("worker_supervisor_authority")
        self._deps = dependencies

    def execute_native_pack(
        self,
        *,
        pack: DbtExecutionPack,
        run_identity: AirflowRunIdentity,
        airflow_attempt: AirflowAttemptCorrelation,
        runtime_root: Path,
        interval: DbtExecutionInterval,
        supervisor_transport: str,
    ) -> DbtExecutionOutcome:
        """Require the caller's pinned supervisor capability to be this root's.

        The transport is parsed here, not in the runtime bootstrap, so a
        deployment cannot execute a supervised attempt against any capability
        other than the one this root was composed with.
        """
        try:
            supervisor = supervisor_from_transport(supervisor_transport)
        except ValueError:
            raise CompositionAdmissionError("worker_supervisor_authority") from None
        if supervisor != self._deps.supervisor:
            raise CompositionAdmissionError("worker_supervisor_authority")
        return self.execute(CompositionDbtExecutionRequest(pack, run_identity, airflow_attempt, runtime_root, interval))

    def execute(self, request: CompositionDbtExecutionRequest) -> DbtExecutionOutcome:
        """Run the complete admission, dispatch, capture and seal sequence once."""
        occurrence = self._deps.read_active()
        occurrence.require_state("ACTIVE")
        attempt = build_composition_dbt_attempt(
            occurrence,
            pack=request.pack,
            run_identity=request.run_identity,
            airflow_attempt=request.airflow_attempt,
        )
        session = SupervisedDbtSession(request, occurrence, attempt)
        worker: CompositionWorker[Any] = CompositionWorker(
            attempts=AdmittedExecutionSlot(self._deps, session),
            gate=self._deps.gate,
            outcome_observer=TerminalDbtOutcome(self._deps, session),
        )
        result = worker.run(attempt, execute=lambda credentials: self._build(session, credentials))
        return self._agreed(session, result.receipt)

    def _build(self, session: SupervisedDbtSession, credentials: Any) -> DbtExecutionOutcome:
        """Derive authority, fence dispatch once and capture actual originals."""
        deps, request = self._deps, session.request
        allocation = session.allocation
        if allocation is None:
            raise CompositionAdmissionError("worker_allocation_missing")
        project = confined_dbt_project_directory(Path(request.runtime_root), request.pack.project_subdir)
        profile_path = allocation.profile_store.profile_path
        authority = CompositionDbtCaptureAuthority(
            attempt=session.attempt,
            pack=request.pack,
            run_identity=request.run_identity,
            allocation=observed_child_allocation(allocation, profile_path),
            toolchain=deps.toolchain_inspector.inspect(request.pack.profile.adapter_type),
            manifest_reader=deps.manifest_reader(allocation.output_directory, PREFLIGHT_MANIFEST_ARTIFACT),
            commands=lambda: composition_dbt_trusted_commands(
                request.pack,
                project_dir=project,
                profile_path=profile_path,
                preflight_target=allocation.preflight_target,
                preflight_logs=allocation.preflight_logs,
                target=allocation.target,
                logs=allocation.logs,
                interval_vars_json=request.interval.dbt_vars_json(),
            ),
            sql_principal_sid="mssql-sid:" + credentials.login_sid.hex(),
            project_directory=project,
            dbt_executable=deps.dbt_executable,
        )
        store = deps.capture_store(authority.verify_source)
        session.authority, session.store = authority, store
        capture = deps.capture(
            store,
            issued_dbt_child_environment(attempt=session.attempt, credentials=credentials, home=allocation.logs),
        )
        outcome = self._invoke(session, authority, store, capture, credentials, project=project)
        session.outcome = outcome
        if outcome.evidence.build_started:
            capture.capture(session.attempt)
            return outcome
        self._register_undispatched(session, store)
        return outcome

    def _invoke(
        self,
        session: SupervisedDbtSession,
        authority: CompositionDbtCaptureAuthority,
        store: Any,
        capture: Any,
        credentials: Any,
        *,
        project: Path,
    ) -> DbtExecutionOutcome:
        """Run the shared pinned engine behind the one-shot dispatch fence."""
        deps, request = self._deps, session.request
        allocation = session.allocation
        assert allocation is not None  # noqa: S101 - established by the caller
        runner = ProtectedDbtCommandRunner(
            attempt=session.attempt,
            trusted_commands=authority.trusted_commands,
            trusted_intent=authority.intent,
            store=store,
            capture=capture,
            preflight_runner=deps.preflight_command_runner,
        )
        artifacts = LocalDbtRunResultsReader()
        service = DbtExecutionService(
            command_runner=runner,
            toolchain_inspector=deps.toolchain_inspector,
            profile_renderer=IssuedDbtProfileRenderer(
                attempt=session.attempt,
                connection_ref=request.pack.profile.connection_ref,
                target=deps.target,
                credentials=credentials,
            ),
            profile_store=allocation.profile_store,
            run_results_reader=artifacts,
            run_results_validator=OfficialDbtRunResultsValidator(),
            preflight=build_dbt_runtime_preflight(runner, artifact_reader=artifacts),
            evidence_writer=LocalDbtExecutionEvidenceWriter(allocation.output_directory / EXECUTION_EVIDENCE_ARTIFACT),
            composition_attempt=CompositionDbtAttemptLifecycle(
                attempt=session.attempt,
                occurrence=session.occurrence,
                pack=request.pack,
                attempts=deps.attempts,
                read_active=deps.read_active,
            ),
        )
        return service.execute(
            request.pack,
            runtime_root=Path(request.runtime_root),
            run_output_root=allocation.run_output_root,
            run_identity=request.run_identity,
            airflow_attempt=request.airflow_attempt,
            interval=request.interval,
        )

    @staticmethod
    def _register_undispatched(session: SupervisedDbtSession, store: Any) -> None:
        """Bind the real preflight bytes while the gate can still authorize it."""
        try:
            store.register(session.attempt)
        except Exception:  # noqa: BLE001 - an unregisterable attempt must stay open
            session.unsealed = True
            return
        session.undispatched = True

    @staticmethod
    def _agreed(session: SupervisedDbtSession, receipt: CompositionAttemptReceipt) -> DbtExecutionOutcome:
        """Require the durable terminal receipt and written evidence to agree."""
        outcome = session.outcome
        if (
            outcome is None
            or receipt.state != "SUCCEEDED"
            or not outcome.passed
            or outcome.exit_code != 0
            or outcome.evidence.status != "passed"
        ):
            raise CompositionAdmissionError("worker_evidence_disagreement")
        return outcome


def build_composition_dbt_process_boundary(
    *,
    supervisor: CompositionSupervisorProjection,
    supervisor_root: Path = DEFAULT_COMPOSITION_SUPERVISOR_ROOT,
    profiles_root: Path = DEFAULT_COMPOSITION_PROFILES_ROOT,
) -> LinuxDbtProcessBoundary:
    """Build the production boundary from the authenticated supervisor capability.

    Production roots must derive child identities from the pinned projection and
    the mounted supervisor root; a directly constructed identity range is for
    tests and tooling only.
    """
    root = Path(supervisor_root)
    return LinuxDbtProcessBoundary(
        root / "run",
        Path(profiles_root),
        identities=CompositionChildIdentityAllocator.from_projection(root, supervisor),
    )


def execute_composition_dbt_pack(
    root: CompositionDbtExecutionRoot, request: CompositionDbtExecutionRequest
) -> DbtExecutionOutcome:
    """Execute one native pack and return its evidence-agreeing outcome."""
    return root.execute(request)


__all__ = [
    "DEFAULT_COMPOSITION_PROFILES_ROOT",
    "DEFAULT_COMPOSITION_SUPERVISOR_ROOT",
    "CompositionDbtExecutionDependencies",
    "CompositionDbtExecutionRequest",
    "CompositionDbtExecutionRoot",
    "build_composition_dbt_process_boundary",
    "execute_composition_dbt_pack",
]
