"""Offline authority-boundary and fault coverage for the native dbt parent root.

These tests use the real parent worker, attempt lifecycle, capture authority,
command fence, execution service, preflight, run-results parser, evidence writer
and outcome observer. Only the boundaries that require an actual Linux root
supervisor, an actual dbt distribution or a protected SQL Server are replaced by
deterministic doubles that still write and read real files. Live Linux,
Kubernetes and SQL Server behaviour remains UNVERIFIED here.
"""

from __future__ import annotations

import json
from contextlib import contextmanager
from dataclasses import dataclass, field, fields, replace
from pathlib import Path, PurePosixPath
from typing import Any

import pytest

from dpone.adapters.composition_dbt_process_boundary import DbtProcessAllocation
from dpone.adapters.composition_mssql_gate_proofs import gate_proof
from dpone.adapters.composition_mssql_issuance import MssqlIssuedCredentials
from dpone.adapters.composition_supervisor_filesystem import protected_original_reader
from dpone.app.composition_dbt_execution import (
    CompositionDbtExecutionDependencies,
    CompositionDbtExecutionRequest,
    CompositionDbtExecutionRoot,
)
from dpone.contracts.airflow_correlation import AirflowAttemptCorrelation
from dpone.contracts.airflow_run_identity import AirflowArtifactIdentity, AirflowRunIdentity
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_dbt_outcome import (
    DbtArtifactOriginal,
    DbtCaptureError,
    DbtCaptureRecord,
    DbtChildExit,
    DbtDispatchIntent,
    DbtExitRecord,
)
from dpone.contracts.composition_execution_authority import supervisor_transport
from dpone.contracts.composition_persistence import CompositionAttemptReceipt
from dpone.contracts.composition_supervisor import CompositionSupervisorProjection
from dpone.contracts.dbt_relation_writes import selected_relation_writes
from dpone.contracts.dbt_runtime import (
    AIRFLOW_RUN_IDENTITY_ENV,
    DAG_ID_ENV,
    DAG_RUN_ID_ENV,
    TRY_NUMBER_ENV,
    DbtPublishingError,
)
from dpone.contracts.dbt_workspace_control import dbt_relation_write_subject
from dpone.contracts.run_interval import INTERVAL_END_ENV, INTERVAL_START_ENV
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.ports.composition_dbt import COMPOSITION_SUPERVISOR_B64_ENV
from dpone.ports.dbt_publishing import DbtCommandResult
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.dbt_execution_bootstrap import _execute_loaded_pack, execute_dbt_pack
from tests.composition_mssql_gate_helpers import SERVICE, occurrence
from tests.test_dbt_runtime_execution import (
    _AUTHORITY_MANIFEST,
    _PROJECT_YAML,
    _interval,
    _real_sqlserver_fixture_pack,
    _ToolchainInspector,
)

ISSUED_PASSWORD = "issued-one-time-secret"
SUPERVISOR = CompositionSupervisorProjection(
    persistent_volume_claim="dpone-composition-supervisor",
    child_uid_start=1_000_000,
    child_gid_start=1_000_000,
    child_identity_count=1_000_000,
)
MODEL = "model.dpone_dbt_demo.competitive_pricing"


def _manifest() -> dict[str, Any]:
    """Copy the real fixture manifest, minus unsupported column constraints.

    The accepted materialization contract fails closed on declarative column
    constraints, so this offline fixture declares only the name and type
    obligations that a supervised attempt is allowed to observe.
    """
    manifest = json.loads(json.dumps(_AUTHORITY_MANIFEST))
    for node in manifest["nodes"].values():
        for column in node.get("columns", {}).values():
            column["constraints"] = []
    return manifest


def _parent(pack):
    """Bind the fixture pack's real relation writes into an ACTIVE occurrence."""
    active = occurrence()
    writes = tuple(
        sorted(
            dbt_relation_write_subject(write)
            for write in selected_relation_writes(
                project_path=pack.project_subdir, execution=pack, manifest=_manifest()
            )
        )
    )
    native = replace(active.request.workloads[0], workload_id=f"dbt__{pack.workflow_id}", write_subjects=writes)
    ordinary = replace(active.request.workloads[1], workload_id="z_ordinary")
    request = replace(
        active.request,
        workloads=(native, ordinary),
        resources=(
            replace(
                active.request.resources[0],
                write_subjects=tuple(sorted(writes + ordinary.write_subjects)),
            ),
        ),
    )
    return replace(active, request=request, receipt=replace(active.receipt, request_sha256=request.request_sha256))


def _run_identity(active) -> AirflowRunIdentity:
    workload = active.request.workloads[0]
    return AirflowRunIdentity(
        active.request.release_id,
        active.request.deployment_id,
        AirflowArtifactIdentity(workload.workload_id, workload.pack_sha256),
    )


def _target(pack) -> ResolvedBindingConnection:
    return ResolvedBindingConnection(
        CredentialsConfig(
            host="sql",
            database=pack.profile.database,
            schema=pack.profile.schema,
            username="ambient-binding",
            password="ambient-binding-secret",
        ),
        {},
        ResolvedConnectionDescriptor("mssql", {}),
    )


def _run_results(invocation: str, *, statuses: tuple[str, ...] = ("success",)) -> dict[str, Any]:
    return {
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/run-results/v6.json",
            "dbt_version": "1.12.3",
            "generated_at": "2026-07-27T00:00:01Z",
            "invocation_id": invocation,
            "invocation_started_at": "2026-07-27T00:00:00Z",
            "env": {},
        },
        "results": [
            {
                "status": status,
                "timing": [],
                "thread_id": "Thread-1",
                "execution_time": 0.25,
                "adapter_response": {},
                "message": "must not enter evidence",
                "failures": 0,
                "unique_id": MODEL,
                "compiled": True,
                "compiled_code": "select 1 as date_id",
                "relation_name": "[DWH_Stage].[pricing_pricing].[competitive_pricing]",
                "batch_results": None,
            }
            for status in statuses
        ],
        "elapsed_time": 0.5,
        "args": {},
    }


@dataclass
class _Ledger:
    """Shared durable state of the protected control doubles."""

    admitted: bool = False
    issued: bool = False
    gate_state: str = "READY"
    closed: str | None = None
    quiescence: str | None = None
    registration: tuple[Any, Any] | None = None
    dispatched: bool = False
    record: DbtCaptureRecord | None = None
    exit_record: DbtExitRecord | None = None
    events: list[str] = field(default_factory=list)


class _Occurrences:
    """Fresh ACTIVE parent reads; only the first is an ordered root boundary."""

    def __init__(self, ledger: _Ledger, active) -> None:
        self._ledger, self._active = ledger, active
        self.reads = 0

    def read_active(self):
        self.reads += 1
        if self.reads == 1:
            self._ledger.events.append("read_active")
        return self._active


class _Attempts:
    """Protected attempt reservation and terminal receipt double."""

    def __init__(self, ledger: _Ledger) -> None:
        self._ledger = ledger
        self.receipt: CompositionAttemptReceipt | None = None
        self.exact_reads = 0
        self.terminal: list[tuple[str, str]] = []
        self.before_exact_read = lambda: None

    def admit_once(self, attempt) -> CompositionAttemptReceipt:
        self._ledger.events.append("admit_attempt")
        if self._ledger.admitted:
            raise CompositionAdmissionError("attempt_replay")
        self._ledger.admitted = True
        self.receipt = CompositionAttemptReceipt(attempt, "RUNNING")
        return self.receipt

    def read_exact(self, attempt) -> CompositionAttemptReceipt:
        self.exact_reads += 1
        self.before_exact_read()
        if self.receipt is None:
            raise CompositionAdmissionError("attempt_missing")
        return self.receipt

    def finalize(self, attempt, *, state: str, outcome_evidence_sha256: str) -> CompositionAttemptReceipt:
        self._ledger.events.append("finalize_attempt")
        self.terminal.append((state, outcome_evidence_sha256))
        self.receipt = CompositionAttemptReceipt(
            attempt, state, self._ledger.closed, self._ledger.quiescence, outcome_evidence_sha256
        )
        return self.receipt


class _Gate:
    """One-time issuance plus independent closure and quiescence proofs."""

    def __init__(self, ledger: _Ledger, *, close_error: str | None = None) -> None:
        self._ledger = ledger
        self.sid = bytes.fromhex("61" * 16)
        self.close_error = close_error
        self.issued: list[MssqlIssuedCredentials] = []

    def issue_once(self, attempt) -> MssqlIssuedCredentials:
        self._ledger.events.append("issue_login")
        if self._ledger.issued:
            raise CompositionAdmissionError("login_issuance_replay")
        self._ledger.issued = True
        credentials = MssqlIssuedCredentials("dpone_v3_" + attempt.attempt_sha256[7:], self.sid, ISSUED_PASSWORD)
        self.issued.append(credentials)
        return credentials

    def close(self, attempt):
        self._ledger.events.append("close_gate")
        if self.close_error is not None:
            raise CompositionAdmissionError(self.close_error)
        proof = self._proof(attempt, "CLOSED_GATES")
        self._ledger.gate_state = "CLOSED"
        self._ledger.closed = proof.proof_sha256
        return proof

    def prove_quiescence(self, attempt):
        self._ledger.events.append("prove_quiescence")
        proof = self._proof(attempt, "QUIESCENCE")
        self._ledger.quiescence = proof.proof_sha256
        return proof

    def _proof(self, attempt, kind: str):
        return gate_proof(
            attempt,
            service_id=SERVICE,
            sid=self.sid,
            kind=kind,
            evidence={
                "schema": "dpone.composition-mssql-gate-observation.v1",
                "attempt_sha256": attempt.attempt_sha256,
                "kind": kind,
                "login_sid": self.sid.hex(),
            },
        )


class _ProfileStore:
    """Root-owned one-shot profile with a fixed pre-derivable path."""

    def __init__(self, directory: Path) -> None:
        self._directory = directory
        directory.mkdir(parents=True, exist_ok=True)
        self.materializations = 0

    @property
    def profile_path(self) -> Path:
        return self._directory / "profiles.yml"

    @contextmanager
    def materialize(self, content: bytes):
        self.materializations += 1
        path = self.profile_path
        path.write_bytes(content)
        path.chmod(0o440)
        try:
            yield path
        finally:
            path.unlink(missing_ok=True)


class _Boundary:
    """Deterministic stand-in for the Linux root-supervised allocation."""

    def __init__(self, ledger: _Ledger, output_root: Path, profiles_root: Path) -> None:
        self._ledger, self._root, self._profiles = ledger, output_root, profiles_root
        self.allocations: list[DbtProcessAllocation] = []

    def allocate(self, attempt, *, runtime_attempt_id: str, target_path: str = "target"):
        self._ledger.events.append("allocate_identity")
        if self.allocations:
            raise DbtCaptureError("capture_child_identity_replay")
        name = "attempt-" + attempt.attempt_sha256.removeprefix("sha256:")
        run = self._root / name
        output = run / "attempts" / runtime_attempt_id
        parts = PurePosixPath(target_path).parts
        for path in (
            output / "preflight" / "target",
            output / "preflight" / "logs",
            output.joinpath(*parts),
            output / "logs",
        ):
            path.mkdir(parents=True, exist_ok=True)
        allocation = DbtProcessAllocation(
            run,
            output,
            output / "preflight" / "target",
            output / "preflight" / "logs",
            output.joinpath(*parts),
            output / "logs",
            _ProfileStore(self._profiles / name),  # type: ignore[arg-type]
        )
        self.allocations.append(allocation)
        return allocation


class _PreflightRunner:
    """Real-file preflight double; a build command here would be a defect."""

    def __init__(self, ledger: _Ledger, *, parse_exit: int = 0, selection_drift: bool = False) -> None:
        self._ledger = ledger
        self.parse_exit = parse_exit
        self.selection_drift = selection_drift
        self.calls: list[tuple[str, ...]] = []
        self.recorded = False

    def run(self, args, *, cwd, timeout_seconds, redactions) -> DbtCommandResult:
        assert args[0] == "dbt"
        assert ISSUED_PASSWORD in redactions
        self.calls.append(args)
        target = Path(args[args.index("--target-path") + 1])
        if "parse" in args:
            if not self.recorded:
                self._ledger.events.append("preflight")
                self.recorded = True
            if self.parse_exit:
                return DbtCommandResult(exit_code=self.parse_exit)
            target.mkdir(parents=True, exist_ok=True)
            (target / "manifest.json").write_text(json.dumps(_manifest()), encoding="utf-8")
            return DbtCommandResult(exit_code=0)
        if "ls" in args:
            if self.selection_drift:
                return DbtCommandResult(exit_code=0, stdout=json.dumps({"unique_id": MODEL + "_drifted"}))
            return DbtCommandResult(exit_code=0, stdout=json.dumps({"unique_id": MODEL}))
        raise AssertionError("a build command must never reach the preflight runner")


def _issued_preflight_factory(preflight: _PreflightRunner):
    """Require the root to bind the issued login before any preflight child runs."""

    def factory(attempt, credentials):
        if (
            credentials.password != ISSUED_PASSWORD
            or credentials.login_name != "dpone_v3_" + attempt.attempt_sha256[7:]
        ):
            raise AssertionError("preflight must receive the issued attempt credentials")
        return preflight

    return factory


class _CaptureStore:
    """Append-only registration/original double for MssqlDbtCaptureStore."""

    def __init__(self, ledger: _Ledger, verify_source, *, lost_ack: bool = False) -> None:
        self._ledger, self.verify_source = ledger, verify_source
        self.lost_ack = lost_ack
        self.source_calls = 0

    def register(self, attempt) -> DbtDispatchIntent:
        if self._ledger.dispatched:
            raise DbtCaptureError("capture_dispatch_replay")
        intent, expectation = self.verify_source(attempt)
        self.source_calls += 1
        if type(intent) is not DbtDispatchIntent or intent.attempt != attempt:
            raise DbtCaptureError("capture_registration_subject")
        if self._ledger.registration not in (None, (intent, expectation)):
            raise DbtCaptureError("capture_registration_conflict")
        # Only a durable row is an ordered boundary: a rejected derivation is not.
        self._ledger.events.append("register_dispatch")
        self._ledger.registration = (intent, expectation)
        if self.lost_ack:
            raise DbtCaptureError("capture_commit_unknown")
        return intent

    def _registration(self):
        if self._ledger.registration is None:
            raise DbtCaptureError("capture_registration_missing")
        return self._ledger.registration

    def load_intent(self, attempt) -> DbtDispatchIntent:
        return self._registration()[0]

    def load_expectation(self, attempt):
        return self._registration()[1]

    def read_capture(self, attempt) -> DbtCaptureRecord | None:
        return self._ledger.record

    def read_exit(self, attempt) -> DbtExitRecord | None:
        return self._ledger.exit_record

    def record_exit_once(self, value: DbtExitRecord) -> None:
        self._ledger.exit_record = value

    def capture_once(self, value: DbtCaptureRecord) -> None:
        self._ledger.record = value

    def record_undispatched(self, attempt) -> None:
        if self._ledger.dispatched or self._ledger.gate_state != "CLOSED":
            raise DbtCaptureError("capture_undispatched_conflict")
        intent = self._registration()[0]
        self._ledger.record = DbtCaptureRecord(
            intent,
            "UNDISPATCHED",
            undispatched_closure_original=canonical_json_bytes(
                {
                    "attempt_sha256": attempt.attempt_sha256,
                    "intent_sha256": intent.intent_sha256,
                    "build_dispatched": False,
                    "closed": True,
                }
            ),
        )


class _Capture:
    """Child dispatch and original capture double that uses real artifacts."""

    def __init__(
        self,
        ledger: _Ledger,
        store: _CaptureStore,
        environment,
        *,
        exit_code: int = 0,
        write_results: bool = True,
        mutate_original: bool = False,
        invocation: str = "invocation-01",
    ) -> None:
        self._ledger, self._store, self._environment = ledger, store, environment
        self.exit_code, self.write_results, self.invocation = exit_code, write_results, invocation
        self.mutate_original = mutate_original
        self.child_environments: list[dict[str, str]] = []

    def dispatch(self, attempt) -> DbtChildExit:
        self._ledger.events.append("run_child")
        self._ledger.dispatched = True
        intent = self._store.load_intent(attempt)
        self.child_environments.append(dict(self._environment()))
        output = Path(intent.output_directory)
        manifest = _manifest()
        manifest["metadata"]["invocation_id"] = self.invocation
        artifacts = dict(intent.artifact_paths)
        build = output / artifacts["build_manifest"]
        build.parent.mkdir(parents=True, exist_ok=True)
        build.write_bytes(canonical_json_bytes(manifest))
        if self.write_results:
            (output / artifacts["run_results"]).write_text(json.dumps(_run_results(self.invocation)), encoding="utf-8")
        child = DbtChildExit(4321, 100, self.exit_code)
        self._store.record_exit_once(
            DbtExitRecord(intent, child, self._quiescence(intent, child), self._original(intent, "preflight_manifest"))
        )
        if self.mutate_original:
            preflight = output / dict(intent.artifact_paths)["preflight_manifest"]
            preflight.write_text(json.dumps(_manifest(), indent=1), encoding="utf-8")
        return child

    def capture(self, attempt) -> DbtCaptureRecord:
        self._ledger.events.append("capture_originals")
        intent = self._store.load_intent(attempt)
        exited = self._store.read_exit(attempt)
        if exited is None:
            raise DbtCaptureError("capture_exit_missing")
        originals = tuple(self._original(intent, role) for role, _ in intent.artifact_paths)
        record = DbtCaptureRecord(intent, "CAPTURED", exited, originals)
        self._store.capture_once(record)
        return record

    @staticmethod
    def _quiescence(intent: DbtDispatchIntent, child: DbtChildExit) -> bytes:
        return canonical_json_bytes(
            {
                "schema": "dpone.composition-dbt-quiescence.v1",
                "intent_sha256": intent.intent_sha256,
                "pid": child.pid,
                "start_ticks": child.start_ticks,
                "child_uid": intent.child_uid,
                "boot_id": "offline-boot",
                "pid_namespace": "offline-pid-namespace",
            }
        )

    @staticmethod
    def _original(intent: DbtDispatchIntent, role: str) -> DbtArtifactOriginal:
        relative = dict(intent.artifact_paths)[role]
        path = Path(intent.output_directory) / relative
        try:
            return DbtArtifactOriginal(role, relative, path.read_bytes())
        except OSError:
            raise DbtCaptureError("capture_artifact_unavailable") from None


class _Materializations:
    """Observation double that must reopen source-derived contracts itself."""

    def __init__(self, ledger: _Ledger, *, corrupt: bool = False) -> None:
        self._ledger, self.corrupt = ledger, corrupt
        self.contract_reads = 0

    def __call__(self, read_contracts):
        def observe(attempt, intent, expectation, invocation) -> bytes:
            self._ledger.events.append("observe_materialization")
            contracts = read_contracts(attempt)
            self.contract_reads += 1
            if self.corrupt:
                return canonical_json_bytes({"schema": "wrong"})
            return canonical_json_bytes(
                {
                    "schema": "dpone.composition-dbt-materialization-observation.v1",
                    "attempt_sha256": attempt.attempt_sha256,
                    "intent_sha256": intent.intent_sha256,
                    "invocation_id": invocation,
                    "materializations": [list(row.expectation) for row in contracts],
                    "catalog": [
                        {
                            "unique_id": row.write.resource_id,
                            "kind": row.kind,
                            "schema_sha256": row.schema_sha256,
                            "declared_columns": [column.name for column in row.columns],
                        }
                        for row in contracts
                    ],
                }
            )

        return observe


class _ProofWriter:
    """Create-then-reopen persistence double for the OUTCOME producer."""

    def __init__(self, ledger: _Ledger, *, error: bool = False) -> None:
        self._ledger, self.error = ledger, error
        self.writes: list[tuple[str, bool]] = []

    def __call__(self, context, proof, document, *, create: bool = True) -> None:
        if create:
            self._ledger.events.append("persist_outcome")
        if self.error:
            raise CompositionAdmissionError("execution_proof_conflict")
        self.writes.append((proof.kind, create))


@dataclass
class Harness:
    root: CompositionDbtExecutionRoot
    request: CompositionDbtExecutionRequest
    dependencies: CompositionDbtExecutionDependencies
    ledger: _Ledger
    occurrences: _Occurrences
    attempts: _Attempts
    gate: _Gate
    boundary: _Boundary
    preflight: _PreflightRunner
    store: _CaptureStore
    capture: _Capture
    materializations: _Materializations
    proofs: _ProofWriter
    manifest_reads: list[Path]

    @property
    def events(self) -> list[str]:
        return self.ledger.events

    def evidence_path(self) -> Path:
        return self.boundary.allocations[0].output_directory / "execution-evidence.json"


def build(tmp_path: Path, **overrides: Any) -> Harness:
    """Compose the real root over deterministic protected-boundary doubles."""

    project = tmp_path / "project"
    project.mkdir()
    (project / "dbt_project.yml").write_text(_PROJECT_YAML, encoding="utf-8")
    pack = _real_sqlserver_fixture_pack(_manifest())
    active = _parent(pack)
    ledger = _Ledger()
    occurrences = _Occurrences(ledger, active)
    attempts = _Attempts(ledger)
    gate = _Gate(ledger, close_error=overrides.get("close_error"))
    boundary = _Boundary(ledger, tmp_path / "supervisor", tmp_path / "profiles")
    if overrides.get("mutate_before_build"):

        def mutate_preflight() -> None:
            manifest = boundary.allocations[0].preflight_target / "manifest.json"
            manifest.write_text(json.dumps(_manifest(), indent=1), encoding="utf-8")

        attempts.before_exact_read = mutate_preflight
    preflight = _PreflightRunner(
        ledger,
        parse_exit=overrides.get("parse_exit", 0),
        selection_drift=overrides.get("selection_drift", False),
    )
    materializations = _Materializations(ledger, corrupt=overrides.get("corrupt_materialization", False))
    proofs = _ProofWriter(ledger, error=overrides.get("proof_error", False))
    manifest_reads: list[Path] = []
    stores: list[_CaptureStore] = []
    captures: list[_Capture] = []

    def capture_store(verify_source) -> _CaptureStore:
        stores.append(_CaptureStore(ledger, verify_source, lost_ack=overrides.get("lost_ack", False)))
        return stores[-1]

    def capture(store, environment) -> _Capture:
        captures.append(
            _Capture(
                ledger,
                store,
                environment,
                exit_code=overrides.get("child_exit_code", 0),
                write_results=overrides.get("write_results", True),
                mutate_original=overrides.get("mutate_original", False),
            )
        )
        return captures[-1]

    def manifest_reader(root: Path, relative: str):
        def read() -> bytes:
            path = root / relative
            manifest_reads.append(path)
            try:
                return path.read_bytes()
            except OSError:
                raise DbtCaptureError("capture_artifact_unavailable") from None

        return read

    dependencies = CompositionDbtExecutionDependencies(
        supervisor=SUPERVISOR,
        read_active=occurrences.read_active,
        attempts=attempts,
        gate=gate,
        boundary=boundary,
        capture_store=capture_store,
        capture=capture,
        materializations=materializations,
        outcome_proof_writer=proofs,
        expected_service_id=SERVICE,
        toolchain_inspector=_ToolchainInspector(),
        preflight_command_runner_factory=_issued_preflight_factory(preflight),
        manifest_reader=manifest_reader,
        outcome_transaction=_transaction,
        target=_target(pack),
        dbt_executable="/opt/dpone/bin/dbt",
    )
    request = CompositionDbtExecutionRequest(
        pack=pack,
        run_identity=_run_identity(active),
        airflow_attempt=AirflowAttemptCorrelation(
            dag_id=pack.workflow_id,
            task_id=f"dbt__{pack.workflow_id}__dpone_runtime",
            run_id="scheduled__2026-07-27T00:00:00+00:00",
            try_number=1,
            map_index=-1,
        ),
        runtime_root=project,
        interval=_interval(),
    )
    root = CompositionDbtExecutionRoot(dependencies)
    harness = Harness(
        root,
        request,
        dependencies,
        ledger,
        occurrences,
        attempts,
        gate,
        boundary,
        preflight,
        _LazyList(stores),  # type: ignore[arg-type]
        _LazyList(captures),  # type: ignore[arg-type]
        materializations,
        proofs,
        manifest_reads,
    )
    return harness


@contextmanager
def _transaction():
    yield _Ledger()


class _LazyList:
    """Expose the single per-attempt collaborator built inside the root."""

    def __init__(self, values: list[Any]) -> None:
        self._values = values

    def __getattr__(self, name: str) -> Any:
        if not self._values:
            raise AttributeError(name)
        return getattr(self._values[-1], name)


@pytest.fixture
def authority(tmp_path: Path) -> Harness:
    return build(tmp_path)


def test_native_root_orders_all_authority_boundaries(authority: Harness) -> None:
    outcome = authority.root.execute(authority.request)

    assert outcome.passed
    assert outcome.exit_code == 0
    assert authority.events == [
        "read_active",
        "admit_attempt",
        "allocate_identity",
        "issue_login",
        "preflight",
        "register_dispatch",
        "run_child",
        "capture_originals",
        "close_gate",
        "prove_quiescence",
        "observe_materialization",
        "persist_outcome",
        "finalize_attempt",
    ]
    assert authority.attempts.terminal[0][0] == "SUCCEEDED"
    assert authority.proofs.writes == [("OUTCOME", True), ("OUTCOME", False)]
    # The pre-build fence independently reopens the parent and the attempt.
    assert authority.occurrences.reads == 2
    assert authority.attempts.exact_reads == 1


def test_root_derives_every_authority_value_from_verified_sources(authority: Harness) -> None:
    authority.root.execute(authority.request)

    intent, expectation = authority.ledger.registration
    allocation = authority.boundary.allocations[0]
    assert intent.argv[0] == "/opt/dpone/bin/dbt" and "build" in intent.argv
    assert intent.output_directory == str(allocation.output_directory)
    assert intent.sql_principal_sid == "mssql-sid:" + authority.gate.sid.hex()
    assert intent.supervisor_uid == 0
    assert (intent.child_uid, intent.child_gid) != (0, 0)
    assert tuple(role for role, _ in intent.artifact_paths) == (
        "preflight_manifest",
        "build_manifest",
        "run_results",
        "execution_evidence",
    )
    assert dict(intent.artifact_paths)["execution_evidence"] == "execution-evidence.json"
    assert expectation.selected_graph_unique_ids == (MODEL,)
    assert expectation.materializations[0][0] == MODEL and expectation.materializations[0][1] == "table"
    assert expectation.dbt_core_version == "1.12.3"
    # Only the root reopens the preflight manifest; every callback re-reads it.
    assert len(authority.manifest_reads) >= 3
    assert {path.name for path in authority.manifest_reads} == {"manifest.json"}
    assert authority.materializations.contract_reads == 1


def test_issued_credentials_never_reach_argv_evidence_or_originals(authority: Harness) -> None:
    authority.root.execute(authority.request)

    intent, _ = authority.ledger.registration
    issued = authority.gate.issued[0]
    evidence = authority.evidence_path().read_bytes()
    assert ISSUED_PASSWORD not in " ".join(intent.argv)
    assert issued.login_name not in " ".join(intent.argv)
    assert ISSUED_PASSWORD.encode() not in evidence
    assert issued.login_name.encode() not in evidence
    assert b"ambient-binding-secret" not in evidence
    assert all(
        ISSUED_PASSWORD.encode() not in original.content and issued.login_name.encode() not in original.content
        for original in authority.ledger.record.originals
    )
    assert not authority.boundary.allocations[0].profile_store.profile_path.exists()
    environments = authority.capture.child_environments
    assert environments and environments[0]["DBT_ENV_SECRET_DPONE_COMPOSITION_PASSWORD"] == ISSUED_PASSWORD
    assert "DPONE_COMPOSITION_SUPERVISOR_B64" not in environments[0]


def test_preflight_drift_is_protected_undispatched_and_never_pretends_dispatch(tmp_path: Path) -> None:
    harness = build(tmp_path, selection_drift=True)

    with pytest.raises(CompositionAdmissionError, match="worker_failed"):
        harness.root.execute(harness.request)

    assert "run_child" not in harness.events
    assert "capture_originals" not in harness.events
    assert harness.events[-1] == "finalize_attempt"
    assert harness.attempts.terminal == [("FAILED", harness.attempts.receipt.outcome_evidence_sha256)]
    assert harness.ledger.record.phase == "UNDISPATCHED"
    assert json.loads(harness.evidence_path().read_text())["status"] == "failed"


def test_unregisterable_preflight_failure_never_seals_the_attempt(tmp_path: Path) -> None:
    """No preflight manifest means no intent, so no terminal state may exist."""
    harness = build(tmp_path, parse_exit=1)

    with pytest.raises(CompositionAdmissionError, match="worker_predispatch_unsealed"):
        harness.root.execute(harness.request)

    assert "register_dispatch" not in harness.events
    assert "run_child" not in harness.events
    assert harness.attempts.terminal == []
    assert harness.attempts.receipt.state == "RUNNING"
    assert harness.ledger.record is None


@pytest.mark.parametrize(
    "fault,expected",
    [
        ({"lost_ack": True}, "worker_executor_failed"),
        ({"child_exit_code": 1}, "worker_commit_unknown"),
        ({"write_results": False}, "worker_executor_failed"),
        ({"corrupt_materialization": True}, "worker_commit_unknown"),
    ],
)
def test_post_dispatch_faults_stay_commit_unknown(tmp_path: Path, fault: dict[str, Any], expected: str) -> None:
    harness = build(tmp_path, **fault)

    with pytest.raises(CompositionAdmissionError, match=expected):
        harness.root.execute(harness.request)

    assert harness.attempts.terminal == [("COMMIT_UNKNOWN", harness.attempts.receipt.outcome_evidence_sha256)]
    assert harness.events.count("close_gate") == 1
    assert harness.events.count("prove_quiescence") == 1
    assert harness.events[-1] == "finalize_attempt"
    assert not harness.boundary.allocations[0].profile_store.profile_path.exists()


def test_gate_closure_failure_retains_running_ownership(tmp_path: Path) -> None:
    harness = build(tmp_path, close_error="login_gate_close_readback")

    with pytest.raises(CompositionAdmissionError):
        harness.root.execute(harness.request)

    assert harness.attempts.terminal == []
    assert harness.attempts.receipt.state == "RUNNING"
    assert "prove_quiescence" not in harness.events
    assert "persist_outcome" not in harness.events


def test_outcome_commit_ambiguity_never_finalizes_the_attempt(tmp_path: Path) -> None:
    harness = build(tmp_path, proof_error=True)

    with pytest.raises(CompositionAdmissionError):
        harness.root.execute(harness.request)

    assert harness.attempts.terminal == []
    assert harness.attempts.receipt.state == "RUNNING"
    assert harness.events.count("persist_outcome") == 1
    assert "finalize_attempt" not in harness.events


def test_mutated_preflight_original_can_never_be_a_successful_outcome(tmp_path: Path) -> None:
    harness = build(tmp_path, mutate_original=True)

    with pytest.raises(CompositionAdmissionError, match="worker_commit_unknown"):
        harness.root.execute(harness.request)

    assert harness.attempts.terminal == [("COMMIT_UNKNOWN", harness.attempts.receipt.outcome_evidence_sha256)]


def test_capture_authority_rejects_a_mutated_original_between_callbacks(tmp_path: Path) -> None:
    """Every authority callback rehashes the original and pins the first digest."""
    harness = build(tmp_path)
    harness.root.execute(harness.request)
    # The store only ever receives the authority's own bound source verifier.
    authority = harness.store.verify_source.__self__
    manifest = harness.manifest_reads[-1]

    assert authority.materialization_contracts(harness.attempts.receipt.attempt)
    manifest.write_text(json.dumps(_manifest(), indent=1), encoding="utf-8")
    with pytest.raises(DbtCaptureError, match="capture_preflight_changed"):
        authority.materialization_contracts(harness.attempts.receipt.attempt)
    with pytest.raises(DbtCaptureError, match="capture_preflight_changed"):
        authority.intent()


def test_manifest_substitution_before_build_never_dispatches_or_relaunches(tmp_path: Path) -> None:
    harness = build(tmp_path, mutate_before_build=True)

    with pytest.raises((CompositionAdmissionError, DbtCaptureError)) as error:
        harness.root.execute(harness.request)

    issued = harness.gate.issued[0]
    assert ISSUED_PASSWORD not in str(error.value)
    assert issued.login_name not in str(error.value)
    assert harness.events.count("run_child") == 0
    assert harness.ledger.dispatched is False
    assert harness.ledger.record is None
    assert harness.attempts.receipt.state == "RUNNING"

    with pytest.raises(CompositionAdmissionError, match="attempt_replay"):
        harness.root.execute(harness.request)
    assert harness.events.count("run_child") == 0
    assert harness.events.count("allocate_identity") == 1
    assert harness.events.count("issue_login") == 1


def test_replayed_attempt_never_allocates_or_issues_twice(authority: Harness) -> None:
    authority.root.execute(authority.request)
    allocations = len(authority.boundary.allocations)
    issued = len(authority.gate.issued)

    with pytest.raises(CompositionAdmissionError, match="attempt_replay"):
        authority.root.execute(authority.request)

    assert len(authority.boundary.allocations) == allocations
    assert len(authority.gate.issued) == issued
    assert authority.events.count("allocate_identity") == 1
    assert authority.events.count("issue_login") == 1


def test_supervisor_authority_must_match_the_constructed_root(authority: Harness) -> None:
    with pytest.raises(CompositionAdmissionError, match="worker_supervisor_authority"):
        authority.root.execute_native_pack(
            pack=authority.request.pack,
            run_identity=authority.request.run_identity,
            airflow_attempt=authority.request.airflow_attempt,
            runtime_root=authority.request.runtime_root,
            interval=authority.request.interval,
            supervisor_transport=supervisor_transport(replace(SUPERVISOR, persistent_volume_claim="other-claim")),
        )
    with pytest.raises(CompositionAdmissionError, match="worker_supervisor_authority"):
        authority.root.execute_native_pack(
            pack=authority.request.pack,
            run_identity=authority.request.run_identity,
            airflow_attempt=authority.request.airflow_attempt,
            runtime_root=authority.request.runtime_root,
            interval=authority.request.interval,
            supervisor_transport="not-a-canonical-transport",
        )
    assert authority.events == []


def test_pinned_supervisor_transport_reaches_the_composed_root(authority: Harness) -> None:
    outcome = authority.root.execute_native_pack(
        pack=authority.request.pack,
        run_identity=authority.request.run_identity,
        airflow_attempt=authority.request.airflow_attempt,
        runtime_root=authority.request.runtime_root,
        interval=authority.request.interval,
        supervisor_transport=supervisor_transport(SUPERVISOR),
    )

    assert outcome.passed
    assert authority.events[-1] == "finalize_attempt"


def test_production_preflight_reader_is_the_protected_no_follow_original(authority: Harness) -> None:
    """An omitted reader must be the protected reopen, never an ordinary read.

    The protected reader only runs inside the actual Linux root supervisor, so
    here it must refuse the boundary instead of silently reading the volume.
    Live root-supervisor behaviour remains UNVERIFIED offline.
    """
    injected = authority.dependencies
    dependencies = CompositionDbtExecutionDependencies(
        **{field.name: getattr(injected, field.name) for field in fields(injected) if field.name != "manifest_reader"}
    )
    assert dependencies.manifest_reader is protected_original_reader

    read = dependencies.manifest_reader(
        Path("/var/lib/dpone/composition/run/attempt/attempts/one"), "preflight/target/manifest.json"
    )
    with pytest.raises(DbtCaptureError, match="capture_supervisor_boundary"):
        read()

    # A refused boundary can never launch a child or seal a terminal state.
    with pytest.raises((CompositionAdmissionError, DbtCaptureError)):
        CompositionDbtExecutionRoot(dependencies).execute(authority.request)
    assert "run_child" not in authority.events
    assert authority.ledger.registration is None
    assert authority.ledger.record is None
    assert authority.attempts.terminal == []
    assert authority.attempts.receipt.state == "RUNNING"


def _scheduler_environment(harness: Harness) -> dict[str, str]:
    """Project exactly the scheduler values an activated v3 task carries.

    No ambient runtime connection context or Vault coordinate is included: a
    supervised attempt receives its credentials from the parent gate only.
    """
    attempt = harness.request.airflow_attempt
    return {
        AIRFLOW_RUN_IDENTITY_ENV: harness.request.run_identity.to_json(),
        DAG_ID_ENV: attempt.dag_id,
        DAG_RUN_ID_ENV: attempt.run_id,
        TRY_NUMBER_ENV: str(attempt.try_number),
        INTERVAL_START_ENV: harness.request.interval.start,
        INTERVAL_END_ENV: harness.request.interval.end,
        COMPOSITION_SUPERVISOR_B64_ENV: supervisor_transport(SUPERVISOR),
    }


def _written_pack(harness: Harness) -> str:
    path = Path(harness.request.runtime_root) / "pack.json"
    path.write_text(json.dumps(harness.request.pack.to_dict()), encoding="utf-8")
    return path.name


def test_authenticated_runtime_routes_the_verified_pack_to_the_composed_root(authority: Harness) -> None:
    """The production bootstrap must reach the parent root, not workspace v2."""
    outcome = execute_dbt_pack(
        _written_pack(authority),
        environ=_scheduler_environment(authority),
        runtime_root=Path(authority.request.runtime_root),
        composition_executor=authority.root,
    )

    assert outcome.passed
    assert authority.events[-1] == "finalize_attempt"
    intent, _ = authority.ledger.registration
    # The attempt identity came from the scheduler environment, not the caller.
    assert intent.attempt.attempt_sha256 == authority.attempts.receipt.attempt.attempt_sha256


def test_authenticated_runtime_fails_closed_without_a_composed_root(authority: Harness) -> None:
    with pytest.raises(DbtPublishingError) as exc_info:
        execute_dbt_pack(
            _written_pack(authority),
            environ=_scheduler_environment(authority),
            runtime_root=Path(authority.request.runtime_root),
        )

    assert exc_info.value.code == "DPONE_DBT_COMPOSITION_EXECUTOR_UNAVAILABLE"
    assert authority.events == []


def test_authenticated_runtime_fails_closed_when_execution_is_substituted(authority: Harness) -> None:
    """A substituted runner or preflight must never execute a supervised pack."""
    for substitution in ({"command_runner": object()}, {"preflight": object()}):
        with pytest.raises(DbtPublishingError) as exc_info:
            _execute_loaded_pack(
                authority.request.pack,
                environment=_scheduler_environment(authority),
                runtime_root=Path(authority.request.runtime_root),
                run_output_root=Path(authority.request.runtime_root) / "runs",
                profiles_tmpfs_root=Path(authority.request.runtime_root) / "profiles",
                composition_executor=authority.root,
                **substitution,
            )

        assert exc_info.value.code == "DPONE_DBT_COMPOSITION_EXECUTOR_UNAVAILABLE"
    assert authority.events == []
