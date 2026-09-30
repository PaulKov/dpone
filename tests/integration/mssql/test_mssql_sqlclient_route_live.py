"""Real SqlBulkCopy through journal-v2, grant-lock, and target-local verification."""

from __future__ import annotations

import json
import os
import signal
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from threading import Event, Lock, Thread
from typing import Any

import pytest
from tests.integration.mssql.mssql_certification_identity import baked_source_identity
from tests.integration.mssql.mssql_live_support import mssql_connector, wait_until_ready
from tests.integration.mssql.mssql_sqlclient_live_cases import sqlclient_live_case

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_custody import NativeTargetCustody
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.contracts.mssql_native_chunks import NativeChunkLimits, NativeChunkPlan
from dpone.contracts.mssql_native_stage_writer import NativeStageWriteRequest
from dpone.contracts.mssql_sqlclient_ipc import (
    MssqlSqlClientCredentials,
    sqlclient_application_name,
)
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.mssql_native_chunks import NativeReextractRequired
from dpone.runtime.mssql_native_chunks_observations import summarize_native_phases
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_composition import compose_native_stage_context
from dpone.runtime.sinks.mssql_native_import import native_attempt_table_name
from dpone.runtime.sinks.mssql_sqlclient_composition import resolve_sqlclient_backend

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]


@pytest.mark.parametrize(
    "fixture_id",
    ["wide100-sqlclient-v1"]
    if os.environ.get("DPONE_SQLCLIENT_FORCE_KILL") == "1"
    else ["narrow-sqlclient-v1", "wide100-sqlclient-v1"],
)
def test_sqlclient_transport_certification_matrix(tmp_path: Path, fixture_id: str) -> None:
    if os.environ.get("DPONE_RUN_SQLCLIENT_LIVE") != "1":
        pytest.skip("set DPONE_RUN_SQLCLIENT_LIVE=1 inside the certified Linux x86-64 runner")
    row_count = int(os.environ.get("DPONE_SQLCLIENT_CERT_ROWS", "10000"))
    if row_count not in {0, 10_000, 1_000_000}:
        pytest.fail("DPONE_SQLCLIENT_CERT_ROWS must select empty, 10k, or 1m certification size")
    import_parallelism = int(os.environ.get("DPONE_SQLCLIENT_CERT_IMPORT_PARALLELISM", "1"))
    if import_parallelism not in {1, 2}:
        pytest.fail("DPONE_SQLCLIENT_CERT_IMPORT_PARALLELISM must select one or two target writers")
    layout_version = int(os.environ.get("DPONE_SQLCLIENT_CERT_LAYOUT_VERSION", "1"))
    if layout_version not in {1, 2}:
        pytest.fail("DPONE_SQLCLIENT_CERT_LAYOUT_VERSION must select one or two")
    commit_sha, source_tree_oid, runner_image_sha256 = baked_source_identity()

    case = sqlclient_live_case(fixture_id)
    force_kill = os.environ.get("DPONE_SQLCLIENT_FORCE_KILL") == "1"
    expected_max_rows = 5_000 if force_kill else 8_192 if fixture_id == "wide100-sqlclient-v1" else 65_536
    max_rows = int(os.environ.get("DPONE_SQLCLIENT_CERT_MAX_ROWS", str(expected_max_rows)))
    max_bytes = int(os.environ.get("DPONE_SQLCLIENT_CERT_MAX_BYTES", str(48 << 20)))
    max_pending = int(os.environ.get("DPONE_SQLCLIENT_CERT_MAX_PENDING", "1"))
    max_staging_tables = int(os.environ.get("DPONE_SQLCLIENT_CERT_MAX_STAGING_TABLES", "128"))
    encoding_parallelism = int(os.environ.get("DPONE_SQLCLIENT_CERT_ENCODING_PARALLELISM", "2"))
    if (max_rows, max_bytes, max_pending, max_staging_tables, encoding_parallelism) != (
        expected_max_rows,
        48 << 20,
        1,
        128,
        2,
    ):
        pytest.fail("SqlClient certification resource policy does not match the versioned campaign")
    target = mssql_connector()
    wait_until_ready("mssql SqlClient certification target", lambda: target.get_records("SELECT 1"))
    suffix = uuid.uuid4().hex[:12]
    target_id = f"synthetic-sqlclient-{suffix}"
    wire = build_mssql_bcp_native_contract(
        schema=case.columns,
        query=f"synthetic:{case.fixture_id}:{row_count}",
        target_format="mssql_native",
    )
    plan = NativeChunkPlan(
        f"synthetic-{suffix}",
        target_id,
        f"synthetic-query-{case.fixture_id}",
        f"synthetic-window-{row_count}",
        case.schema_sha256,
        wire.type_layout_hash,
    )

    def credentials(_request: object) -> MssqlSqlClientCredentials:
        return MssqlSqlClientCredentials(
            host=target.host,
            port=target.port,
            database=target.database,
            username=target.user or "",
            password=target.password or "",
            encrypt=True,
            trust_server_certificate=target.trust_server_certificate == "yes",
        )

    killer = _ActiveBulkCopyKiller() if force_kill else None
    diagnostics: list[str] = []

    def process_started(process_id: int, request: NativeStageWriteRequest) -> None:
        assert killer is not None
        killer.start(process_id, request)

    backend = resolve_sqlclient_backend(
        plan,
        timeout_seconds=1800,
        credentials_provider=credentials,
        process_started=process_started if force_kill else None,
        diagnostic_sink=diagnostics.append,
        layout_version=layout_version,
    )
    recording_writer = _RecordingWriter(backend.writer)
    store = SQLiteWindowStore(tmp_path / "state.sqlite", clock=lambda: 1.0)
    lease = store.acquire(target_id, "synthetic-certifier", 7200)
    custody = NativeTargetCustody(store, target_id)
    custody.claim(lease, backend.identity.invocation_key)
    stages = []

    @contextmanager
    def importer_connection():
        connector = mssql_connector()
        connector.get_records_iterator = lambda *_args, **_kwargs: pytest.fail(
            "target-local verification returned business rows"
        )
        connector.bcp_import_process = lambda *_args, **_kwargs: pytest.fail("SqlClient route invoked BCP")
        try:
            yield connector
        finally:
            connector.close()

    limits = NativeChunkLimits(
        max_total_encoded_bytes=16 << 30,
        stage_allocated_bytes_stop_threshold=32 << 30,
        max_rows=max_rows,
        max_bytes=max_bytes,
        max_row_bytes=1 << 20,
        max_pending=max_pending,
        max_staging_tables=max_staging_tables,
        parallelism=2 if force_kill else 1,
        encoding_parallelism=encoding_parallelism,
        import_parallelism=import_parallelism,
    )
    context = compose_native_stage_context(
        store=store,
        plan=plan,
        lease=lease,
        wire_contract=wire,
        limits=limits,
        work_dir=tmp_path / "native-files",
        target_connector=target,
        importer_connection=importer_connection,
        bcp_options_factory=lambda **values: BcpOptions(bcp_path=target.bcp_path, **values),
        database=target.database,
        schema="dbo",
        row_source=lambda: case.rows(row_count),
        journal_factory=lambda: pytest.fail("SqlClient route must select journal v2"),
        cancelled=Event(),
        required_target_headroom_bytes=256 << 20,
        verification_identity=backend.identity,
        target_local_timeout_seconds=1800,
        native_stage_writer=recording_writer,
        persisted_hash_layout=layout_version == 2,
    )
    started = time.monotonic()
    try:
        if force_kill:
            with pytest.raises(WindowOutcomeUnknown, match="writer_outcome_unknown"):
                context.executor.stage(plan, case.rows(row_count), wire, lease)
            assert killer is not None
            kill_observation = killer.require_observation()
            projection = context.journal_factory().data
            attempt_id = projection["chunks"]["0"]["attempt_id"]
            terminal = projection["events"][attempt_id][-1]
            assert terminal["event"] == "UNKNOWN"
            assert terminal["observation"]["writer_outcome"] == "lost_ack"
            assert all(event["event"] != "VERIFIED" for event in projection["events"][attempt_id])
            qualified = str(
                target.qualified_name("dbo", native_attempt_table_name(plan, attempt_id), database=target.database)
            )
            store.release(lease)
            recovery_lease = store.acquire(target_id, "synthetic-recovery", 7200)
            assert custody.claim(recovery_lease, backend.identity.invocation_key).recovery_only is True

            class NoRelaunchWriter:
                def write(self, *_args: object, **_kwargs: object) -> object:
                    pytest.fail("recovery must not relaunch SqlBulkCopy")

            recovered = compose_native_stage_context(
                store=store,
                plan=plan,
                lease=recovery_lease,
                wire_contract=wire,
                limits=limits,
                work_dir=tmp_path / "native-files",
                target_connector=target,
                importer_connection=importer_connection,
                bcp_options_factory=lambda **values: BcpOptions(bcp_path=target.bcp_path, **values),
                database=target.database,
                schema="dbo",
                row_source=lambda: pytest.fail("recovery must not reopen the source"),
                journal_factory=lambda: pytest.fail("SqlClient route must select journal v2"),
                cancelled=Event(),
                required_target_headroom_bytes=256 << 20,
                verification_identity=backend.identity,
                target_local_timeout_seconds=1800,
                native_stage_writer=NoRelaunchWriter(),
                persisted_hash_layout=layout_version == 2,
            )
            with pytest.raises(NativeReextractRequired, match="reextract_required"):
                recovered.executor.recover(plan, recovery_lease)
            recovered_projection = recovered.journal_factory().data
            assert recovered_projection["events"][attempt_id][-1]["event"] == "RETIRED"
            assert target.get_records("SELECT OBJECT_ID(?)", (qualified,)) == [(None,)]
            assert custody.inspect(recovery_lease).state == "clear"
            _write_evidence(
                fixture_id=fixture_id,
                row_count=row_count,
                commit_sha=commit_sha,
                elapsed_seconds=time.monotonic() - started,
                stage_seconds=None,
                verification_seconds=None,
                receipt_count=0,
                import_parallelism=import_parallelism,
                layout_version=layout_version,
                delivery_phases={},
                writer_phases={},
                companion=backend.companion,
                scenario="force_kill_recovery",
                recovery_classification="partial_retired",
                source_tree_oid=source_tree_oid,
                runner_image_sha256=runner_image_sha256,
                recovery_observation={
                    "initial_terminal": "UNKNOWN",
                    "initial_writer_outcome": "lost_ack",
                    **kill_observation,
                    "barrier_settled": True,
                    "repeated_digest_stable": True,
                    "final_terminal": "RETIRED",
                    "stage_absent": True,
                    "custody_clear": True,
                    "source_reopened": False,
                    "writer_relaunched": False,
                },
            )
            return
        try:
            complete = context.executor.stage(plan, case.rows(row_count), wire, lease)
        except WindowOutcomeUnknown as error:
            diagnostic = diagnostics[-1] if diagnostics else "mssql_sqlclient.diagnostic_unavailable"
            pytest.fail(f"{error}; companion diagnostic={diagnostic}")
        staged_at = time.monotonic()
        context.verify_receipts(complete.receipts)
        verified_at = time.monotonic()
        assert complete.rows == row_count
        assert sum(receipt.rows for receipt in complete.receipts) == row_count
        assert bool(complete.receipts) is (row_count > 0)
        stages.extend(complete.receipts)
        _write_evidence(
            fixture_id=fixture_id,
            row_count=row_count,
            commit_sha=commit_sha,
            elapsed_seconds=time.monotonic() - started,
            stage_seconds=staged_at - started,
            verification_seconds=verified_at - staged_at,
            receipt_count=len(complete.receipts),
            import_parallelism=import_parallelism,
            layout_version=layout_version,
            delivery_phases=summarize_native_phases(complete.observations),
            writer_phases=recording_writer.phase_totals(),
            companion=backend.companion,
            scenario="success",
            recovery_classification="not_required",
            source_tree_oid=source_tree_oid,
            runner_image_sha256=runner_image_sha256,
            recovery_observation=None,
        )
    finally:
        for receipt in stages:
            try:
                with context.executor.importer_factory() as importer:
                    importer.drop_exact_owned(plan, receipt, lease)
            except Exception:
                pass
        target.close()


def _write_evidence(
    *,
    fixture_id: str,
    row_count: int,
    commit_sha: str,
    elapsed_seconds: float,
    stage_seconds: float | None,
    verification_seconds: float | None,
    receipt_count: int,
    import_parallelism: int,
    layout_version: int,
    delivery_phases: dict[str, dict[str, Any]],
    writer_phases: dict[str, float],
    companion: object,
    scenario: str,
    recovery_classification: str,
    source_tree_oid: str,
    runner_image_sha256: str,
    recovery_observation: dict[str, object] | None,
) -> None:
    configured = os.environ.get("DPONE_SQLCLIENT_EVIDENCE_DIR")
    if not configured:
        return
    root = Path(configured)
    root.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": "dpone.mssql-sqlclient.transport-certification.v2",
        "status": "PASS",
        "scenario": scenario,
        "source_commit_sha": commit_sha,
        "source_tree_oid": source_tree_oid,
        "runner_image_sha256": runner_image_sha256,
        "fixture_id": fixture_id,
        "row_count": row_count,
        "receipt_count": receipt_count,
        "import_parallelism": import_parallelism,
        "layout_version": layout_version,
        "elapsed_seconds": round(elapsed_seconds, 6),
        "stage_seconds": None if stage_seconds is None else round(stage_seconds, 6),
        "verification_seconds": None if verification_seconds is None else round(verification_seconds, 6),
        "delivery_phases": delivery_phases,
        "writer_phases": writer_phases,
        "business_rows_read_back": 0,
        "bcp_process_count": 0,
        "synthetic_only": True,
        "package_version": companion.package_version,
        "artifact_sha256": companion.artifact_sha256,
        "writer_identity_sha256": companion.writer_identity_sha256,
        "runtime_identity_sha256": companion.runtime_identity_sha256,
        "protocol": ("dpone.mssql-sqlclient.ipc.v1" if layout_version == 1 else "dpone.mssql-sqlclient.ipc.v2"),
        "recovery_classification": recovery_classification,
        "recovery_observation": recovery_observation,
    }
    filename = f"{scenario}-{fixture_id}-{row_count}-layout{layout_version}-parallel{import_parallelism}.json"
    (root / filename).write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class _ActiveBulkCopyKiller:
    """Kill only after SQL Server proves an active transactional bulk request."""

    def __init__(
        self,
        *,
        connector_factory=mssql_connector,
        kill_process_group=os.killpg,
        signal_process_group=os.killpg,
        timeout_seconds: float = 60.0,
        poll_seconds: float = 0.01,
    ) -> None:
        self._threads: list[Thread] = []
        self._observations: list[dict[str, object]] = []
        self._errors: list[str] = []
        self._lock = Lock()
        self._active = Event()
        self._active_processes: set[int] = set()
        self._target_process_id: int | None = None
        self._launched: list[tuple[int, NativeStageWriteRequest]] = []
        self._connector_factory = connector_factory
        self._kill_process_group = kill_process_group
        self._signal_process_group = signal_process_group
        self._timeout_seconds = timeout_seconds
        self._poll_seconds = poll_seconds

    def start(self, process_id: int, request: NativeStageWriteRequest) -> None:
        self._signal_process_group(process_id, signal.SIGSTOP)
        with self._lock:
            if self._target_process_id is None:
                self._target_process_id = process_id
            self._launched.append((process_id, request))
        thread = Thread(target=self._observe_and_kill, args=(process_id, request), daemon=True)
        self._threads.append(thread)
        thread.start()
        with self._lock:
            launched = tuple(self._launched) if len(self._launched) == 2 else ()
        for launched_process_id, _request in launched:
            self._signal_process_group(launched_process_id, signal.SIGCONT)

    def require_observation(self) -> dict[str, object]:
        for thread in self._threads:
            thread.join(timeout=65)
        if any(thread.is_alive() for thread in self._threads):
            pytest.fail("bulk-copy observation did not settle")
        with self._lock:
            if self._errors:
                pytest.fail(self._errors[0])
            if len(self._observations) != 1:
                pytest.fail("force-kill certification requires exactly one observed bulk writer")
            return dict(self._observations[0])

    def _observe_and_kill(self, process_id: int, request: NativeStageWriteRequest) -> None:
        connector = self._connector_factory()
        expires_at = time.monotonic() + self._timeout_seconds
        try:
            application_name = sqlclient_application_name(
                request.attempt_id,
                request.grant_token_sha256,
                request.object_id,
                request.stage_id_sha256,
            )
            while time.monotonic() < expires_at:
                rows = connector.get_records(
                    "SELECT TOP (1) r.session_id,r.command,r.open_transaction_count,"
                    "CASE WHEN (SELECT COUNT_BIG(*) FROM sys.dm_tran_locks l "
                    "WHERE l.request_session_id=r.session_id AND l.resource_type=N'APPLICATION' "
                    "AND l.request_status=N'GRANT')=1 THEN 1 ELSE 0 END,"
                    "CASE WHEN EXISTS (SELECT 1 FROM sys.dm_tran_locks l "
                    "LEFT JOIN sys.partitions p ON p.hobt_id=l.resource_associated_entity_id "
                    "OR p.partition_id=l.resource_associated_entity_id "
                    "WHERE l.request_session_id=r.session_id AND l.resource_database_id=DB_ID() "
                    "AND l.request_status=N'GRANT' AND l.resource_type IN (N'OBJECT',N'HOBT') "
                    "AND l.request_mode IN (N'BU',N'IX',N'X') "
                    "AND (l.resource_associated_entity_id=? OR p.object_id=?)) THEN 1 ELSE 0 END "
                    "FROM sys.dm_exec_requests r JOIN sys.dm_exec_sessions s ON s.session_id=r.session_id "
                    "WHERE s.program_name=? AND r.session_id<>@@SPID "
                    "AND r.database_id=DB_ID() AND r.command=N'BULK INSERT' ORDER BY r.session_id DESC",
                    (request.object_id, request.object_id, application_name),
                )
                if rows:
                    _session_id, command, open_transactions, applock_held, stage_lock_held = rows[0]
                    if (
                        command == "BULK INSERT"
                        and open_transactions > 0
                        and applock_held == 1
                        and stage_lock_held == 1
                    ):
                        with self._lock:
                            self._active_processes.add(process_id)
                            if len(self._active_processes) >= 2:
                                self._active.set()
                        if not self._active.wait(max(0.0, expires_at - time.monotonic())):
                            raise TimeoutError("competing SqlBulkCopy writer was not observed")
                        if process_id == self._target_process_id:
                            observation = {
                                "bulk_copy_active": True,
                                "transaction_active": True,
                                "session_applock_held": True,
                                "exact_stage_lock_held": True,
                                "competing_writer_active": True,
                                "competing_writer_ignored": True,
                            }
                            self._kill_process_group(process_id, signal.SIGKILL)
                            with self._lock:
                                self._observations.append(observation)
                        return
                time.sleep(self._poll_seconds)
            raise TimeoutError("active transactional SqlBulkCopy was not observed")
        except BaseException as error:
            with self._lock:
                self._errors.append(f"force-kill synchronization failed: {type(error).__name__}")
        finally:
            connector.close()


class _RecordingWriter:
    """Thread-safe diagnostic decorator; delivery authority stays with the real writer."""

    def __init__(self, delegate: Any) -> None:
        self._delegate = delegate
        self._metrics = []
        self._lock = Lock()

    def write(self, request: Any, *, deadline: Any) -> Any:
        observation = self._delegate.write(request, deadline=deadline)
        with self._lock:
            self._metrics.append(observation.metrics)
        return observation

    def phase_totals(self) -> dict[str, float]:
        with self._lock:
            metrics = tuple(self._metrics)
        fields = ("launch_seconds", "write_seconds", "dispose_seconds")
        if any(getattr(metric, field) is None for metric in metrics for field in fields):
            raise AssertionError("live SqlClient certification requires complete writer metrics")
        return {field: round(sum(float(getattr(metric, field)) for metric in metrics), 6) for field in fields}
