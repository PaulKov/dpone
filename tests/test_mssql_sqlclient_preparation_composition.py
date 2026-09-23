"""Actual SQLite owner transitions; controlled fixtures do not qualify SQL."""

from time import monotonic
from uuid import uuid4

import pytest

from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from tests.test_mssql_sqlclient_observe_composition import observe_parent as observe_parent
from tests.test_mssql_sqlclient_observe_composition import retained_parent


def test_terminal_sequence_commits_prepared_without_creation_exit_check(composed_preparation, tmp_path):
    from dpone.contracts.mssql_sqlclient_stage_identity import stage_object_identity
    from dpone.contracts.mssql_tds_worker import TdsAttemptPhase
    from dpone.services.mssql_tds_original_continuation import PreparationTransition

    parent = composed_preparation
    handle = parent.handle
    transition = PreparationTransition(parent.attempt, handle)
    parent.attempt._preparation = transition
    with transition.sequence():
        receipt = acknowledged_preparation(transition, tmp_path)
        transition.advance(stage_object_identity(handle.request.selected_stage), receipt)
    assert parent.attempt.lifecycle.state.phase is TdsAttemptPhase.PREPARED
    from dpone.app.mssql_sqlclient_preparation_composition import _cleanup

    _cleanup(transition, monotonic() + 1)
    transition.finish_cleanup()
    assert parent.attempt._preparation is None


def test_preparation_association_blocks_forward_after_observe_latch_clears(observe_parent):
    from dpone.services.mssql_tds_original_continuation import PreparationTransition

    parent = observe_parent
    handle = retained_parent(parent)
    transition = PreparationTransition(parent.attempt, handle)
    parent.attempt._preparation = transition
    parent.attempt._end_observe(handle.helper_id)
    try:
        with pytest.raises(Exception):
            parent.attempt.reserve_operation(uuid4(), TdsCoordinatorCommand.OBSERVE, "a" * 64, deadline=monotonic() + 1)
        assert parent.attempt._preparation is transition
    finally:
        parent.attempt._preparation = None  # release synthetic fixture, not a production recovery path


class PreparationCursor:
    """Actual finite SQL algorithms consume these explicit synthetic driver rows."""

    @staticmethod
    def create(h):
        from dataclasses import replace

        from dpone.adapters import mssql_sqlclient_preparation_sql as sql
        from dpone.adapters.mssql_sqlclient_stage_catalog_sql import SCHEMA_SQL, STAGE_VISIBILITY_SQL
        from dpone.contracts.mssql_sqlclient_observe_rows import OWNERSHIP_LABELS
        from tests.test_mssql_sqlclient_observer_incarnation_adapter import own_row
        from tests.test_mssql_sqlclient_source_free_inventory import consumer

        connection, _ = consumer(h, h.pool)
        cursor = connection.cursor
        cursor.permissions = []
        cursor.writer = replace(cursor.writer, login=replace(cursor.writer.login, is_sysadmin=False))
        original = cursor.execute
        cursor.pending = []

        def execute(statement, *parameters):
            cursor.pending = []
            login, stage = cursor.writer.login, cursor.stage
            if statement == sql.ENV_SQL:
                rows = [
                    (
                        "16.0.synthetic",
                        "synthetic",
                        3,
                        16,
                        160,
                        "synthetic",
                        stage.database_id,
                        stage.database_name,
                        0,
                        0,
                        0,
                        login.principal_id,
                        login.name,
                        bytes.fromhex(login.sid),
                        "SQL_LOGIN",
                        0,
                        5,
                        "writer_user",
                        b"\xaa",
                        "SQL_USER",
                        "INSTANCE",
                        b"\xbb",
                    )
                ]
            elif statement in {text for text, _ in sql.OWNERSHIP_SQL}:
                index = next(i for i, (text, _) in enumerate(sql.OWNERSHIP_SQL) if text == statement)
                rows = [(OWNERSHIP_LABELS[index], 0)]
            elif statement == sql.SERVER_COUNT_SQL:
                rows = [(0,)]
            elif statement == sql.SERVER_PERMISSIONS_SQL:
                rows = []
            elif statement == sql.STAGE_SECURITY_SQL:
                rows = [(stage.object_id, 0)]
            elif statement in {text for entry in sql.IDENTITY_SQL for text in entry[1:3]}:
                name, count, query, *_ = next(entry for entry in sql.IDENTITY_SQL if statement in entry[1:3])
                records = {
                    "server_principals": [
                        (login.principal_id, login.name, bytes.fromhex(login.sid), "SQL_LOGIN", 0),
                        (2, "public", b"\x02", "SERVER_ROLE", 0),
                    ],
                    "database_principals": [
                        (0, "public", b"\x00", "DATABASE_ROLE", "NONE"),
                        (1, "dbo", b"\xbb", "SQL_USER", "INSTANCE"),
                        (5, "writer_user", b"\xaa", "SQL_USER", "INSTANCE"),
                    ],
                    "database_securables": [],
                    "server_endpoints": [],
                }[name]
                rows = [(len(records),)] if statement == count else records
            elif statement == sql.EFFECTIVE_SQL:
                restored = own_row(cursor.own)
                restored[51], restored[57:62] = 1, [None] * 5
                rows = [
                    (
                        login.name,
                        bytes.fromhex(login.sid),
                        5,
                        "writer_user",
                        stage.database_id,
                        stage.database_name,
                        0,
                        0,
                        cursor.management.login.name,
                    )
                ]
                cursor.pending = [
                    [(login.principal_id, bytes.fromhex(login.sid), login.name, "SQL LOGIN", "GRANT OR DENY")],
                    [(5, b"\xaa", "writer_user", "SQL USER", "GRANT OR DENY")],
                    [],
                    [],
                    [restored],
                ]
            elif statement == STAGE_VISIBILITY_SQL:
                rows = [(16, 1, 1, 1)]
            elif statement == SCHEMA_SQL:
                rows = [(stage.schema_id, stage.schema_name)]
            elif statement.startswith("SELECT CASE WHEN EXISTS"):
                rows = [(1,)]
            else:
                return original(statement, *parameters)
            cursor.calls.append((statement, parameters))
            cursor.rows = rows

        def nextset():
            if cursor.pending:
                cursor.rows = cursor.pending.pop(0)
                return True
            return None

        cursor.execute, cursor.nextset = execute, nextset
        return cursor


@pytest.fixture
def composed_preparation(tmp_path, monkeypatch):
    """Original CREATE/SQLite and open-OBSERVE join, mock only OS process and SQL I/O."""
    import os
    import socket
    import sys
    from dataclasses import replace
    from pathlib import Path
    from threading import Thread
    from types import SimpleNamespace

    import tests.test_mssql_sqlclient_create_departure_composition as creator
    from dpone.adapters.mssql_sqlclient_observe_process import PythonSqlClientObserveLauncher
    from dpone.adapters.mssql_tds_actor_core import TdsActorPool
    from dpone.adapters.mssql_tds_coordinator_connection import TdsCoordinatorConnection, TdsSqlConnection
    from dpone.adapters.mssql_tds_installation import worker_installation_digest
    from dpone.app import mssql_sqlclient_observe_bootstrap as bootstrap
    from dpone.app import mssql_sqlclient_observe_composition as observe
    from dpone.app import mssql_sqlclient_preparation_composition as prepare
    from dpone.contracts.mssql_sqlclient_grant_inventory import SqlClientGrantInventoryLimits, SqlClientGrantPrincipal
    from dpone.contracts.mssql_sqlclient_observe import SqlClientObserveRequest
    from dpone.contracts.mssql_tds_validation import deadline_nanoseconds
    from tests.test_mssql_sqlclient_stage_locator_wiring import TracedHarness

    monkeypatch.setattr(
        creator, "TdsActorPool", lambda **kw: TdsActorPool(capacity=8, clock=kw.get("clock", monotonic))
    )
    from hashlib import sha256

    from dpone.app.mssql_sqlclient_create_settlement import settle_sqlclient_create_departure
    from dpone.contracts.mssql_native_chunks import NativeBulkTransportPolicy
    from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest
    from dpone.contracts.mssql_tds_create import create_command_digest
    from dpone.contracts.strict_json import canonical_json_bytes

    policy = NativeBulkTransportPolicy("mssql_sqlclient", "rows", 8 << 30)

    class PreparationHarness(TracedHarness):
        def prepare_factory(self, factory):
            self.request = replace(
                self.request,
                parent=replace(
                    self.request.parent, policy_sha256=sha256(canonical_json_bytes(policy.to_dict())).hexdigest()
                ),
            )
            self.identity = replace(
                self.identity, parent=self.request.parent, command_sha256=create_command_digest(self.request)
            )
            digest = coordinator_identity_digest(self.identity)
            self.authority = replace(self.authority, operation_sha256=digest)
            self.proof = replace(self.proof, operation_sha256=digest, command_sha256=self.identity.command_sha256)
            return super().prepare_factory(factory)

    h = PreparationHarness(tmp_path)
    outcome = h.run()
    settle_sqlclient_create_departure(h.attempt, admitted_factory=h.factory, pool=h.pool, deadline=monotonic() + 5)
    h.policy = policy
    from dpone.adapters.mssql_sqlclient_installation import AdmittedSqlClientInstallation

    h.build = AdmittedSqlClientInstallation(
        Path("/python"),
        "a" * 64,
        Path("/framework"),
        "b" * 64,
        Path("/runtime/dotnet"),
        Path("/runtime"),
        Path("/companion"),
        Path("/companion/Worker.dll"),
        Path("/manifest.json"),
        "c" * 64,
        (),
    )
    monkeypatch.setattr(
        AdmittedSqlClientInstallation, "assert_admitted", lambda self, **kw: {"synthetic_controlled_build": "fixture"}
    )
    assert h.attempt._create_departure_outcome is outcome
    cursor = PreparationCursor.create(h)
    request = SqlClientObserveRequest(
        parent=h.request.parent,
        selected_stage=cursor.stage,
        management_admission=cursor.management,
        writer_admission=cursor.writer,
        writer_principal=SqlClientGrantPrincipal(5, "writer_user", "aa", "SQL_USER", "INSTANCE"),
        limits=SqlClientGrantInventoryLimits(),
        # This composed fixture performs CREATE, OBSERVE, PREPARE and departure
        # before later exact-chain tests enter their own body. Keep a ceiling
        # that remains valid on loaded CI hosts; individual blocking operations
        # retain their smaller explicit timeouts.
        operation_deadline_ns=deadline_nanoseconds(monotonic() + 300),
    )
    monkeypatch.setattr(
        TdsCoordinatorConnection,
        "connect",
        lambda self, material, deadline: TdsSqlConnection(SimpleNamespace(close=lambda: None), cursor),
    )
    monkeypatch.setattr("dpone.adapters.mssql_tds_coordinator_connection._admit", lambda build: SimpleNamespace())
    monkeypatch.setattr(bootstrap, "install_worker_guard", lambda **kw: None)
    process = replace(h.helper_startup.process, pid=9999)
    monkeypatch.setattr(bootstrap.LinuxTdsProcess, "identify", lambda pid: process)
    package_root = Path(__file__).parents[1] / "src"
    launcher = PythonSqlClientObserveLauncher(
        python_executable=Path(sys.executable),
        package_root=package_root,
        implementation_sha256=worker_installation_digest(package_root),
        admission=h.admission,
        max_address_space_bytes=2**30,
    )

    class ScriptedProcess:
        def __init__(self, **kw):
            self.cleanup_deadline = None
            self.channel, other = socket.socketpair()
            self.channel.setblocking(False)
            other.setblocking(False)
            self.thread = Thread(
                target=bootstrap.run_observe,
                kwargs=dict(
                    expected_parent_pid=os.getppid(),
                    address_space=2**30,
                    startup_deadline=kw["startup_deadline"],
                    operation_deadline=kw["operation_deadline"],
                    channel_fd=other.detach(),
                    launch_nonce=b"x" * 32,
                    implementation_sha256=launcher.implementation_sha256,
                    admission=launcher.admission,
                ),
                daemon=True,
            )
            self.thread.start()

        def startup(self):
            from dpone.adapters.mssql_sqlclient_observe_transport import read_frame
            from dpone.contracts.mssql_tds_coordinator_ipc import decode_startup

            return decode_startup(read_frame(self.channel, deadline=monotonic() + 5, limit=1048576))

        def assert_current(self):
            assert self.thread.is_alive()

        def contain(self, *, deadline):
            try:
                self.channel.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.thread.join(max(0, deadline - monotonic()))
            assert not self.thread.is_alive()
            from dpone.contracts.mssql_tds_worker import TdsChildExit

            return TdsChildExit(process, -9, True)

        def close(self):
            self.channel.close()

    monkeypatch.setattr(PythonSqlClientObserveLauncher, "spawn", lambda self, **kw: ScriptedProcess(**kw))
    handle = observe.open_sqlclient_observe(
        h.attempt,
        request,
        pool=h.pool,
        admitted_factory=h.factory,
        lease=h.lease,
        evidence_root=h.evidence_root,
        launcher=launcher,
        connection_material=lambda: replace(h.material, username=cursor.management.login.name),
        startup_timeout=5.0,
        termination_timeout=2.0,
    )
    baseline = dict(
        schema="dpone.sqlclient.preparation-baseline.v1",
        profile="standalone-sql-login-preparation-v1",
        query_profile="sqlclient-preparation-catalog-v1",
        server_build=dict(
            product_version="16.0.synthetic",
            edition="synthetic",
            engine_edition=3,
            platform="Linux",
            image_digest="sha256:" + "b" * 64,
        ),
        database_profile=dict(
            compatibility_level=160, collation="synthetic", containment=0, trustworthy=0, database_chaining=0
        ),
        provenance=dict(
            provisioning_script_sha256="1" * 64,
            observation_script_sha256="2" * 64,
            canonicalizer_sha256="3" * 64,
            first_raw_evidence_sha256="4" * 64,
            repeat_raw_evidence_sha256="5" * 64,
            reviewed_projection_sha256="6" * 64,
            review_receipt_sha256="7" * 64,
        ),
        effective_server_permissions=[],
        effective_database_permissions=[],
        stock_server_permissions=[],
        stock_database_permissions=[],
        token_rules={
            name: [
                dict(
                    subject="writer",
                    type=kind,
                    usage="GRANT OR DENY",
                )
            ]
            for name, kind in (("login", "SQL LOGIN"), ("user", "SQL USER"))
        },
    )
    from dpone.services.mssql_sqlclient_preparation_qualification import admit_preparation_baseline
    from tests.test_mssql_sqlclient_preparation_qualification import authority, qualification_payload

    baseline_payload = canonical_json_bytes(baseline)
    qualification = qualification_payload(baseline_payload)
    h.baseline_authority = authority(qualification)
    h.baseline = admit_preparation_baseline(baseline_payload, qualification, h.baseline_authority)
    h.handle, h.preparation_api, h.cursor = handle, prepare, cursor
    try:
        yield h
    finally:
        if not handle._closed:
            handle.close(deadline=monotonic() + 2)
        # Test teardown releases a failed preparation association only to join threads.
        h.attempt._preparation = None
        h.cleanup()


def test_actual_original_join_acknowledges_file_before_prepared(composed_preparation, tmp_path):
    from dataclasses import replace

    from dpone.adapters.filesystem_evidence import PinnedEvidenceReadFactory
    from dpone.adapters.mssql_sqlclient_installation import AdmittedSqlClientInstallation
    from dpone.contracts.mssql_sqlclient_input import encode_input_descriptor
    from dpone.contracts.mssql_tds_worker import TdsAttemptPhase
    from dpone.contracts.strict_json import canonical_json_bytes
    from tests.test_mssql_sqlclient_launch import typed_descriptor

    h = composed_preparation
    path = tmp_path / "input"
    path.write_bytes(b"synthetic")
    with path.open("rb") as stream:
        descriptor = typed_descriptor(stream.fileno())
        descriptor = replace(
            descriptor, expected=replace(descriptor.expected, file_sha256=h.request.parent.file_sha256)
        )
        build_observations = []

        original_assert_admitted = AdmittedSqlClientInstallation.assert_admitted

        def assert_admitted(build, **kwargs):
            captured = h.attempt._preparation.writer_inputs()
            build_observations.append(captured)
            assert captured.installation is build
            return original_assert_admitted(build, **kwargs)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(AdmittedSqlClientInstallation, "assert_admitted", assert_admitted)
            receipt = h.preparation_api.prepare_sqlclient_attempt(
                h.handle,
                parent_input=descriptor,
                input_fd=stream.fileno(),
                baseline=h.baseline,
                baseline_authority=h.baseline_authority,
                policy=h.policy,
                build=h.build,
                reader_factory=PinnedEvidenceReadFactory(h.evidence_root),
                evidence_root=h.evidence_root,
                expected_typed_digest="d" * 64,
            )
        assert not stream.closed and stream.tell() == 0
    transition = h.attempt._prepared_origin
    captured = transition.writer_inputs()
    assert build_observations == [captured, captured]
    assert transition.writer_inputs() is captured
    assert captured.transition is transition
    assert captured.policy is h.policy
    assert captured.input_descriptor is descriptor
    assert captured.installation is h.build
    assert captured.policy_snapshot == canonical_json_bytes(h.policy.to_dict())
    assert captured.input_snapshot == encode_input_descriptor(descriptor)
    assert captured.build_sha256 == h.build.build_sha256
    assert captured.operation_deadline_ns == h.handle.request.operation_deadline_ns
    assert h.attempt.lifecycle.state.phase is TdsAttemptPhase.PREPARED
    assert (h.evidence_root / receipt.relative_name).stat().st_size == receipt.byte_count
    assert h.attempt._preparation is None and h.handle._closed


def acknowledged_preparation(transition, root, *, payload_size=None):
    """Lower-layer synthetic envelope exercises the actual create-only ACK actor."""
    from contextlib import contextmanager

    from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
    from dpone.adapters.mssql_sqlclient_preparation_evidence_actor import PreparationEvidenceActor
    from dpone.app.mssql_sqlclient_preparation_composition import _plain
    from dpone.contracts.mssql_sqlclient_preparation import PREPARATION_KEYS, preparation_bytes
    from dpone.contracts.mssql_tds_result import attempt_identity_digest

    @contextmanager
    def factory():
        yield DescriptorPinnedCreateOnlyEvidenceWriter(root)

    subject = attempt_identity_digest(transition.parent.state.identity)
    transition.evidence = transition.pool.open(
        lambda end, clock: PreparationEvidenceActor(factory, subject, end, clock), deadline=transition.deadline
    )
    value = {key: {} for key in PREPARATION_KEYS}
    value.update(
        schema="dpone.sqlclient.preparation.v1",
        empty=1,
        operation_deadline_ns=int(transition.deadline * 10**9),
        attempt=_plain(transition.parent.state.identity),
    )
    if payload_size is not None:
        value["build"] = {"padding": ""}
        value["build"]["padding"] = "x" * (payload_size - len(preparation_bytes(value)))
    payload = preparation_bytes(value)
    transition.begin_preparation(payload)
    receipt = transition.evidence.write(payload, deadline=transition.deadline)
    transition.capture_preparation(payload, receipt)
    return receipt


def test_matching_receipt_without_actual_actor_ack_cannot_advance(observe_parent):
    from dpone.contracts.mssql_sqlclient_preparation import PreparationReceipt
    from dpone.contracts.mssql_sqlclient_stage_identity import stage_object_identity
    from dpone.services.mssql_tds_original_continuation import PreparationTransition

    parent = observe_parent
    handle = retained_parent(parent)
    state = PreparationTransition(parent.attempt, handle)
    parent.attempt._preparation = state
    try:
        with pytest.raises(Exception), state.sequence():
            state.advance(
                stage_object_identity(parent.request.selected_stage), PreparationReceipt.for_payload("a" * 64, b"{}")
            )
        assert not state.attempted
    finally:
        parent.attempt._preparation = None


@pytest.mark.parametrize("fault", ["late_ack", "bad_ack"])
def test_actual_prepared_lost_ack_is_never_adopted(observe_parent, tmp_path, monkeypatch, fault):
    from dataclasses import replace

    from dpone.contracts.mssql_sqlclient_stage_identity import stage_object_identity
    from dpone.services.mssql_tds_original_continuation import PreparationTransition

    parent = observe_parent
    handle = retained_parent(parent)
    state = PreparationTransition(parent.attempt, handle)
    parent.attempt._preparation = state
    original = parent.attempt._lifecycle.advance
    calls = []

    def advance(*args, **kwargs):
        calls.append(1)
        result = original(*args, **kwargs)
        if fault == "late_ack":
            raise OSError("ack lost after actual SQLite save")
        return replace(result, revision=result.revision + 1)

    monkeypatch.setattr(parent.attempt._lifecycle, "advance", advance)
    try:
        with pytest.raises(Exception), state.sequence():
            receipt = acknowledged_preparation(state, tmp_path)
            state.advance(stage_object_identity(parent.request.selected_stage), receipt)
        assert state.attempted and state.expected is None and parent.attempt._poisoned
        with pytest.raises(Exception):
            state.advance(stage_object_identity(parent.request.selected_stage), receipt)
        assert len(calls) == 1
    finally:
        parent.attempt._preparation = None


def test_cleanup_unknown_retains_original_after_ordinary_latch_clears(observe_parent, tmp_path, monkeypatch):
    from dpone.app.mssql_sqlclient_preparation_composition import SqlClientPreparationUnknown, _cleanup
    from dpone.contracts.mssql_sqlclient_stage_identity import stage_object_identity
    from dpone.services.mssql_tds_original_continuation import PreparationTransition

    parent = observe_parent
    handle = retained_parent(parent)
    state = PreparationTransition(parent.attempt, handle)
    parent.attempt._preparation = state
    try:
        with state.sequence():
            receipt = acknowledged_preparation(state, tmp_path)
            state.advance(stage_object_identity(parent.request.selected_stage), receipt)
        close = state.evidence.close
        calls = []

        def lost_close(*, deadline):
            calls.append(deadline)
            close(deadline=deadline)
            raise OSError("ack lost")

        monkeypatch.setattr(state.evidence, "close", lost_close)
        with pytest.raises(SqlClientPreparationUnknown):
            _cleanup(state, monotonic() + 1)
        assert parent.attempt._observe_helper_id is None
        assert parent.attempt._preparation is state and state.evidence is not None
        with pytest.raises(SqlClientPreparationUnknown):
            _cleanup(state, monotonic() + 10)
        assert len(calls) == 1
        with pytest.raises(Exception):
            parent.attempt.reserve_operation(uuid4(), TdsCoordinatorCommand.OBSERVE, "a" * 64, deadline=monotonic() + 1)
    finally:
        parent.attempt._preparation = None


@pytest.mark.parametrize(
    "fault", ["replacement", "profile_drift", "evidence_lost", "receipt_copy", "fd_moved", "cleanup_lost"]
)
def test_actual_join_never_turns_ambiguity_into_prepared_success(composed_preparation, tmp_path, monkeypatch, fault):
    import os
    from copy import deepcopy
    from dataclasses import replace

    from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter, PinnedEvidenceReadFactory
    from dpone.adapters.mssql_sqlclient_preparation_evidence_actor import PreparationEvidenceActor
    from dpone.adapters.mssql_sqlclient_preparation_sql import ENV_SQL
    from dpone.contracts.mssql_tds_worker import TdsAttemptPhase
    from tests.test_mssql_sqlclient_launch import typed_descriptor

    h = composed_preparation
    path = tmp_path / "input"
    path.write_bytes(b"synthetic")
    with path.open("rb") as stream:
        descriptor = typed_descriptor(stream.fileno())
        descriptor = replace(
            descriptor, expected=replace(descriptor.expected, file_sha256=h.request.parent.file_sha256)
        )
        if fault == "replacement":
            h.attempt._create_settlement.outcome = deepcopy(h.attempt._create_departure_outcome)
        elif fault == "profile_drift":
            execute = h.cursor.execute
            reads = []

            def drift(statement, *args):
                result = execute(statement, *args)
                if statement == ENV_SQL:
                    reads.append(1)
                    if len(reads) == 2:
                        row = list(h.cursor.rows[0])
                        row[2] = 4
                        h.cursor.rows = [row]
                return result

            monkeypatch.setattr(h.cursor, "execute", drift)
        elif fault == "evidence_lost":
            write = DescriptorPinnedCreateOnlyEvidenceWriter.write

            def lose(self, name, data):
                write(self, name, data)
                raise OSError("lost file ACK after actual write")

            monkeypatch.setattr(DescriptorPinnedCreateOnlyEvidenceWriter, "write", lose)
        elif fault in ("receipt_copy", "fd_moved"):
            write = PreparationEvidenceActor.write

            def altered(self, *args, **kwargs):
                receipt = write(self, *args, **kwargs)
                if fault == "fd_moved":
                    os.lseek(stream.fileno(), 1, os.SEEK_SET)
                    return receipt
                return replace(receipt)

            monkeypatch.setattr(PreparationEvidenceActor, "write", altered)
        else:
            close = h.handle.evidence.close
            closes = []

            def lose_close(*, deadline):
                closes.append(1)
                close(deadline=deadline)
                raise OSError("lost observer evidence close ACK")

            monkeypatch.setattr(h.handle.evidence, "close", lose_close)
        with pytest.raises(Exception):
            h.preparation_api.prepare_sqlclient_attempt(
                h.handle,
                parent_input=descriptor,
                input_fd=stream.fileno(),
                baseline=h.baseline,
                baseline_authority=h.baseline_authority,
                policy=h.policy,
                build=h.build,
                reader_factory=PinnedEvidenceReadFactory(h.evidence_root),
                evidence_root=h.evidence_root,
                expected_typed_digest="d" * 64,
            )
        assert not stream.closed
        if fault != "replacement":
            state = h.attempt._preparation
            assert state is not None and state.attempt is h.attempt and state.handle is h.handle
            assert state.failed and h.attempt._poisoned
            if fault == "evidence_lost":
                assert state.preparation_payload is not None
                assert state._preparation_capture[0] is state.preparation_payload
                assert state._preparation_capture[1:4] == (None, None, None)
                assert h.attempt._prepared_origin is None
            with pytest.raises(Exception):
                h.attempt.reserve_operation(uuid4(), TdsCoordinatorCommand.OBSERVE, "a" * 64, deadline=monotonic() + 1)
            if fault == "cleanup_lost":
                assert state.expected.state.phase is TdsAttemptPhase.PREPARED
                assert h.attempt._observe_helper_id is None
                assert h.handle._contained_exit.reaped and len(closes) == 1
                # Retained ambiguous close is deliberately not retried by test teardown.
                h.handle._closed = True
            else:
                assert state.expected is None
