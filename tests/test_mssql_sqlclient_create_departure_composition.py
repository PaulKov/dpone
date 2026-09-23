"""Actual CREATE supervisor and SQLite actors; synthetic SQL/process observations."""

from contextlib import contextmanager
from dataclasses import replace
from time import monotonic
from types import SimpleNamespace

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.app.mssql_sqlclient_create_departure_composition import run_sqlclient_create_departure
from dpone.app.mssql_sqlclient_departure_request import decode_departure_credentials
from dpone.app.mssql_sqlclient_departure_supervision import SqlClientCreateDepartureUnknown
from dpone.app.mssql_tds_attempt_composition import create_tds_attempt
from dpone.contracts.mssql_sqlclient_departure_ipc_codec import encode_departure_result, make_departure_result
from dpone.contracts.mssql_tds_worker import TdsChildExit
from tests.test_mssql_sqlclient_create_departure_codec import sample
from tests.test_mssql_tds_coordinator_supervisor import Harness as CreateHarness
from tests.test_mssql_tds_directory import LIMITS


class Harness(CreateHarness):
    """Keep the real CREATE execution while injecting only storage and process ports."""

    entrypoint = staticmethod(run_sqlclient_create_departure)

    def __init__(self, root):
        super().__init__()
        self.pool.close(deadline=100.0)
        self.now = monotonic()
        self.pool = TdsActorPool(capacity=4, clock=lambda: self.now)
        self.departure = sample()
        self.request = replace(self.request, parent=replace(self.request.parent, database=self.departure.database.name))
        from dpone.contracts.mssql_tds_create import create_command_digest

        self.identity = replace(
            self.identity, parent=self.request.parent, command_sha256=create_command_digest(self.request)
        )
        self.store = SQLiteWindowStore(root / "state.sqlite", clock=lambda: 1.0)
        self.lease = self.store.acquire(self.request.parent.target_key, self.owner.owner, 30)
        self.owner = replace(self.owner, fence=self.lease.fence)
        self.identity = replace(self.identity, original_fence=self.lease.fence)
        from dpone.contracts.mssql_tds_coordinator import coordinator_identity_digest

        self.authority = replace(
            self.authority,
            operation_sha256=coordinator_identity_digest(self.identity),
            execution_owner=self.owner,
            session=self.departure.original,
            database=self.departure.database,
        )
        self.proof = replace(
            self.proof,
            operation_sha256=coordinator_identity_digest(self.identity),
            database=self.departure.database,
            session=self.departure.original,
            command_sha256=self.identity.command_sha256,
        )
        self.material = replace(self.material, database=self.departure.database.name)
        self.launcher.admission = self.admission
        self.launcher.implementation_sha256 = self.identity.implementation_sha256
        self.launcher.package_root = "/admitted"
        self.launcher.max_address_space_bytes = 1 << 30
        self.evidence_root = root / "evidence"
        self.evidence_root.mkdir()
        self.factory = self.prepare_factory(self.factory)
        self.attempt = create_tds_attempt(
            self.pool,
            self.factory,
            self.request.parent,
            LIMITS,
            self.lease,
            supervisor_token=self.owner.supervisor_id,
            deadline=self.now + 100.0,
            backend="mssql_sqlclient",
        )
        slot = self.attempt.reserve_operation(
            self.identity.operation_id, self.identity.command, self.identity.command_sha256, deadline=self.now + 100.0
        ).state.slots[-1]
        self.identity = replace(self.identity, slot_index=slot.index)
        self.helper_startup = replace(
            self.startup, process=replace(self.startup.process, pid=999), implementation_sha256="e" * 64
        )
        self.helper = SimpleNamespace(
            identity=self.helper_startup.process,
            declared_startup=self.helper_startup,
            startup_receipt=self.helper_startup,
            startup=self.helper_start,
            send_request=self.send_request,
            receive_result=self.helper_result,
            received_result=None,
            wait=self.helper_wait,
            terminate=self.helper_terminate,
            close=lambda: self.hit("helper.close"),
        )
        self.helper_launcher = SimpleNamespace(
            admission=self.admission,
            admission_sha256=self.launcher.admission_sha256,
            implementation_sha256="e" * 64,
            package_root="/admitted",
            max_address_space_bytes=1 << 30,
            spawn=self.helper_spawn,
        )
        self.helper_exit_code = 0
        self.helper_request = None

    def prepare_factory(self, factory):
        return factory

    @contextmanager
    def factory(self):
        yield self.store

    def helper_spawn(self, **kw):
        self.helper_deadlines = kw
        self.hit("helper.spawn", kw["startup_deadline"])
        return self.helper

    def helper_start(self, *, deadline):
        self.hit("helper.startup", deadline)
        return self.helper_startup

    def observer(self):
        self.hit("observer")
        return self.material

    def send_request(self, body, *, deadline):
        self.hit("helper.credentials", deadline)
        self.helper_request = decode_departure_credentials(
            body,
            startup=self.helper.declared_startup,
            admission_sha256=self.helper_launcher.admission_sha256,
            max_address_space_bytes=self.helper_launcher.max_address_space_bytes,
            **self.helper_deadlines,
        ).request

    def helper_result(self, *, deadline):
        d = replace(self.departure, original=self.authority.session)
        result = make_departure_result(self.helper_request, d)
        self.helper.received_result = encode_departure_result(result, request=self.helper_request)
        self.hit("helper.result", deadline)
        return self.helper.received_result

    def helper_wait(self, *, deadline):
        self.hit("helper.wait", deadline)
        return TdsChildExit(self.helper.identity, self.helper_exit_code, True)

    def helper_terminate(self, *, deadline):
        self.hit("helper.terminate", deadline)
        return TdsChildExit(self.helper.identity, -9, True)

    def run(self, **changes):
        arguments = dict(
            pool=self.pool,
            store_factory=self.factory,
            lease=self.lease,
            evidence_root=self.evidence_root,
            create_launcher=self.launcher,
            departure_launcher=self.helper_launcher,
            creator_admission=self.departure.admission,
            creator_principal=self.departure.principal,
            create_connection_material=self.supply,
            observer_connection_material=self.observer,
            operation_deadline=self.now + 100.0,
            create_startup_timeout=10.0,
            helper_startup_timeout=10.0,
            termination_timeout=5.0,
            clock=lambda: self.now,
        )
        arguments.update(changes)
        return self.entrypoint(self.attempt, self.request, self.identity, **arguments)

    def cleanup(self):
        self.attempt.close(deadline=self.now + 100.0)
        self.pool.close(deadline=self.now + 100.0)


@pytest.fixture
def harness(tmp_path):
    h = Harness(tmp_path)
    yield h
    h.cleanup()


def test_actual_create_then_six_helper_records(harness):
    h = harness
    outcome = h.run()
    assert outcome.create_outcome.response.evidence.session == h.authority.session
    assert outcome.helper_outcome.request == h.helper_request
    assert len(outcome.helper_outcome.receipts) == 6
    assert len(list(h.evidence_root.rglob("*.json"))) == 12
    assert h.events.index("child.close") < h.events.index("helper.spawn")
    assert h.events.count("supplier") == h.events.count("observer") == 1
    assert h.pool.live_count == 2


@pytest.mark.parametrize(
    "name,value", [("operation_deadline", True), ("helper_startup_timeout", 10), ("termination_timeout", float("inf"))]
)
def test_invalid_budget_retains_attempt_before_effect(harness, name, value):
    with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
        harness.run(**{name: value})
    assert caught.value.retained.attempt is harness.attempt
    assert "spawn" not in harness.events


@pytest.mark.parametrize("descriptor_fault", [False, True])
def test_actual_helper_three_pipe_lifecycle(harness, monkeypatch, descriptor_fault):
    """Real framed EOF transport; synthetic handle observations are not live reap evidence."""
    import os
    from threading import Thread

    from dpone.adapters.mssql_sqlclient_departure_process import SqlClientDepartureProcess
    from dpone.adapters.mssql_tds_channels import read_worker_message
    from dpone.contracts.mssql_tds_coordinator_ipc import encode_startup
    from dpone.contracts.mssql_tds_frames import encode_message

    h = harness
    pairs = [os.pipe() for _ in range(3)]
    identities = {fd: os.fstat(fd).st_ino for pair in pairs for fd in pair}
    for fd in identities:
        os.set_blocking(fd, False)
    errors = []
    observed = []
    threads = []
    handle = SimpleNamespace(
        identity=h.helper_startup.process,
        close=lambda: observed.append("handle.close"),
        wait=lambda **kw: SimpleNamespace(reaped=True, exit_code=0),
        contain=lambda **kw: SimpleNamespace(reaped=True, exit_code=-9),
    )
    monkeypatch.setattr(
        "dpone.adapters.mssql_sqlclient_departure_process.LinuxTdsProcess.identify", lambda pid: handle.identity
    )

    def spawn(**kw):
        worker = SqlClientDepartureProcess(
            SimpleNamespace(stdout=None, returncode=None),
            handle,
            (pairs[0][0], pairs[1][1], pairs[2][0]),
            expected=h.helper_startup,
            **kw,
        )
        if descriptor_fault:
            close_descriptor = worker._resources.close_descriptor

            def fail_close(fd):
                close_descriptor(fd)
                if fd == pairs[2][0]:
                    raise OSError("synthetic_post_eof_descriptor_failure")

            monkeypatch.setattr(worker._resources, "close_descriptor", fail_close)

        def helper_thread():
            try:
                os.write(pairs[0][1], encode_message(encode_startup(h.helper_startup), max_payload=16384))
                os.close(pairs[0][1])
                body = read_worker_message(pairs[1][0], deadline=kw["operation_deadline"], max_payload=196608)
                request = decode_departure_credentials(
                    body,
                    startup=h.helper_startup,
                    admission_sha256=h.helper_launcher.admission_sha256,
                    max_address_space_bytes=1 << 30,
                    **kw,
                ).request
                observed.append("request.eof")
                result = make_departure_result(request, replace(h.departure, original=h.authority.session))
                os.write(
                    pairs[2][1], encode_message(encode_departure_result(result, request=request), max_payload=32768)
                )
                os.close(pairs[2][1])
            except BaseException as error:
                errors.append(type(error).__name__)

        thread = Thread(target=helper_thread)
        threads.append(thread)
        thread.start()
        return worker

    h.helper_launcher.spawn = spawn
    try:
        if descriptor_fault:
            with pytest.raises(SqlClientCreateDepartureUnknown) as caught:
                h.run()
            assert caught.value.retained.raw_result
            assert caught.value.retained.result is None
            assert len(caught.value.retained.receipts) == 3
        else:
            outcome = h.run()
            assert len(outcome.helper_outcome.receipts) == 6
        assert observed == ["request.eof", "handle.close"]
        assert not errors
    finally:
        for thread in threads:
            thread.join(2)
            assert not thread.is_alive()
        for fd, inode in identities.items():
            try:
                if os.fstat(fd).st_ino == inode:
                    os.close(fd)
            except OSError:
                pass


@pytest.mark.parametrize(
    "mutation",
    [
        "column_enum_string",
        "column_nullable_int",
        "column_name_subclass",
        "columns_list",
        "column_subclass",
        "request_subclass",
        "request_nonce_string",
        "parent_ordinal_float",
        "parent_attempt_bool",
        "parent_database_subclass",
        "parent_digest_subclass",
        "parent_subclass",
        "identity_command_string",
        "identity_slot_float",
        "identity_fence_bool",
        "identity_parent_float",
    ],
)
def test_original_create_scalars_rejected_before_encoding_hashing_or_effects(harness, monkeypatch, mutation):
    """Strict original DTO validation precedes normalization and preserves the reservation."""
    import dpone.app.mssql_sqlclient_create_departure_composition as module
    import dpone.app.mssql_tds_create_provenance as provenance
    from dpone.contracts.mssql_tds_directory import directory_key

    h = harness
    key = directory_key(h.request.parent)
    durable_before = h.store.load(key)

    class Text(str):
        pass

    def subclass(value):
        return type("Alias", (type(value),), {})(**vars(value))

    column = h.request.columns[0]
    changes = {
        "column_enum_string": (column, "type", column.type.value),
        "column_nullable_int": (column, "nullable", 1),
        "column_name_subclass": (column, "name", Text(column.name)),
        "columns_list": (h.request, "columns", list(h.request.columns)),
        "column_subclass": (h.request, "columns", (subclass(column),)),
        "request_nonce_string": (h.request, "object_nonce", str(h.request.object_nonce)),
        "parent_ordinal_float": (h.request.parent, "ordinal", float(h.request.parent.ordinal)),
        "parent_attempt_bool": (h.request.parent, "attempt", False),
        "parent_database_subclass": (h.request.parent, "database", Text(h.request.parent.database)),
        "parent_digest_subclass": (h.request.parent, "plan_sha256", Text(h.request.parent.plan_sha256)),
        "parent_subclass": (h.request, "parent", subclass(h.request.parent)),
        "identity_command_string": (h.identity, "command", h.identity.command.value),
        "identity_slot_float": (h.identity, "slot_index", float(h.identity.slot_index)),
        "identity_fence_bool": (h.identity, "original_fence", True),
        "identity_parent_float": (h.identity.parent, "attempt", float(h.identity.parent.attempt)),
    }
    if mutation == "request_subclass":
        h.request = subclass(h.request)
    else:
        object.__setattr__(*changes[mutation])
    effects = []
    for owner, name in (
        (module, "create_command_digest"),
        (module, "encode_create_request"),
        (provenance, "encode_create_request"),
        (module, "_admission"),
        (h.pool, "open"),
    ):
        original = getattr(owner, name)

        def observe(*args, method=original, label=name, **kw):
            effects.append(label)
            return method(*args, **kw)

        monkeypatch.setattr(owner, name, observe)
    with pytest.raises(SqlClientCreateDepartureUnknown):
        h.run()
    assert effects == []
    assert h.events == []
    assert h.store.load(key) == durable_before
