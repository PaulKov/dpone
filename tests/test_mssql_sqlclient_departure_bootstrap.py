"""Real pipe helper lifecycle with synthetic admission/SQL, not live certification."""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

# Import before the v1 test fixture patches its constructor: v2 inherits its lifecycle.
from dpone.adapters.mssql_sqlclient_create_exclusion_v2 import (
    SqlClientCreateExclusionObserverV2 as SqlClientCreateExclusionObserverV2,
)
from dpone.adapters.mssql_tds_channels import read_worker_message, write_worker_control
from dpone.adapters.mssql_tds_coordinator_connection import (
    TdsBinaryPin,
    TdsConnectionMaterial,
    TdsConnectionProfile,
    TdsCoordinatorBuild,
    TdsSqlConnection,
    encode_connection_admission,
)
from dpone.app.mssql_sqlclient_departure_bootstrap import run_departure
from dpone.contracts.mssql_sqlclient_departure_ipc_codec import decode_departure_result
from dpone.contracts.mssql_tds_coordinator_ipc import decode_startup
from dpone.contracts.mssql_tds_frames import encode_message
from tests.test_mssql_sqlclient_create_departure_codec import sample
from tests.test_mssql_sqlclient_departure_ipc import request


@pytest.fixture
def harness(tmp_path, monkeypatch):
    from dpone.adapters import mssql_sqlclient_create_exclusion as observers
    from dpone.adapters import mssql_tds_coordinator_connection as connections
    from dpone.adapters import mssql_tds_installation as installation
    from dpone.adapters import mssql_tds_process as process
    from dpone.adapters import mssql_tds_worker_guard as guard
    from dpone.app import mssql_sqlclient_departure_bootstrap as bootstrap

    events = []
    fault = set()
    pin = TdsBinaryPin(tmp_path / "unused-synthetic-pin", "a" * 64)
    admission = encode_connection_admission(
        TdsCoordinatorBuild(pin, pin, pin, pin), TdsConnectionProfile.SYNTHETIC_LOCAL
    )
    r = request()
    deadline = time.monotonic() + 3.0
    root = str(Path(bootstrap.__file__).resolve().parents[2])
    r = replace(
        r,
        plan=replace(
            r.plan,
            package_root=root,
            admission_sha256=sha256(admission).hexdigest(),
            startup_deadline=deadline,
            operation_deadline=deadline,
        ),
        startup=replace(r.startup, package_root=root),
    )
    material = TdsConnectionMaterial("synthetic", 1433, r.plan.database.name, "observer", "synthetic-secret")
    monkeypatch.setattr(guard, "install_worker_guard", lambda **kwargs: events.append("guard"))
    monkeypatch.setattr(
        installation,
        "worker_installation_digest",
        lambda root: "f" * 64 if "source" in fault else r.plan.implementation_sha256,
    )
    monkeypatch.setattr(process.LinuxTdsProcess, "identify", lambda pid: r.startup.process)

    class Factory:
        def __init__(self, build, profile):
            events.append("admission")
            if "admission" in fault:
                raise ValueError("synthetic")

        def connect(self, value, *, deadline):
            events.append("connect")
            assert value == material
            if "connect" in fault:
                raise ValueError("synthetic")

            def close():
                events.append("sql-close")
                if "close" in fault:
                    raise ValueError("synthetic")
                if "late-close" in fault:
                    monkeypatch.setattr(time, "monotonic", lambda: deadline + 1.0)

            return TdsSqlConnection(
                SimpleNamespace(close=close), SimpleNamespace(close=lambda: events.append("cursor-close"))
            )

    class Observer:
        def __init__(self, cursor, **kwargs):
            events.append("observer")

        def observe_departure(self, **kwargs):
            events.append("observe")
            assert kwargs == dict(original=r.plan.original, database=r.plan.database, principal=r.plan.principal)
            if "observe" in fault:
                raise ValueError("synthetic")
            return sample()

    monkeypatch.setattr(connections, "TdsCoordinatorConnection", Factory)
    monkeypatch.setattr(observers, "SqlClientCreateExclusionObserver", Observer)
    pairs = [os.pipe() for _ in range(3)]
    identities = {fd: os.fstat(fd).st_ino for pair in pairs for fd in pair}
    for fd in identities:
        os.set_blocking(fd, False)
    args = dict(
        expected_parent_pid=os.getpid(),
        address_space=r.plan.max_address_space_bytes,
        startup_deadline=deadline,
        operation_deadline=deadline,
        startup_fd=pairs[0][1],
        request_fd=pairs[1][0],
        result_fd=pairs[2][1],
        launch_nonce=r.startup.launch_nonce,
        implementation_sha256=r.plan.implementation_sha256,
        admission=admission,
    )
    yield SimpleNamespace(
        args=args,
        parent=dict(startup=pairs[0][0], request=pairs[1][1], result=pairs[2][0]),
        request=r,
        material=material,
        events=events,
        fault=fault,
        deadline=deadline,
    )
    for fd, inode in identities.items():
        try:
            if os.fstat(fd).st_ino == inode:
                os.close(fd)
        except OSError:
            pass


def submit_request(h, payload=None, *, eof=True):
    from dpone.app.mssql_sqlclient_departure_request import SqlClientDepartureCredentials, encode_departure_credentials

    startup = decode_startup(read_worker_message(h.parent["startup"], deadline=h.deadline, max_payload=16384))
    assert startup == h.request.startup
    assert h.events == ["guard", "admission"]
    if payload is None:
        payload = encode_departure_credentials(
            SqlClientDepartureCredentials(request=h.request, connection_material=h.material)
        )
    write_worker_control(
        h.parent["request"], encode_message(payload, max_payload=196608), deadline=h.deadline, max_bytes=196612
    )
    if eof:
        os.close(h.parent["request"])


def test_closes_sql_before_first_result_write(harness, monkeypatch):
    from dpone.adapters import mssql_tds_channels as channels

    h = harness
    original = channels.write_worker_control

    def write(fd, *args, **kwargs):
        if fd == h.args["result_fd"]:
            assert h.events[-2:] == ["cursor-close", "sql-close"]
            h.events.append("result-write")
        return original(fd, *args, **kwargs)

    monkeypatch.setattr(channels, "write_worker_control", write)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_departure, **h.args)
        submit_request(h)
        body = read_worker_message(h.parent["result"], deadline=h.deadline, max_payload=32768)
        assert decode_departure_result(body, request=h.request).departure == sample()
        assert future.result(timeout=2) == 0
    assert h.events.count("connect") == h.events.count("sql-close") == 1


@pytest.mark.parametrize("fault", ["connect", "observe", "close", "late-close"])
def test_sql_failure_or_late_close_never_sends_success(harness, fault):
    h = harness
    h.fault.add(fault)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_departure, **h.args)
        submit_request(h)
        assert future.result(timeout=2) == 1
        assert os.read(h.parent["result"], 1) == b""
    assert h.events.count("connect") == 1
    assert h.events.count("sql-close") == int(fault != "connect")


@pytest.mark.parametrize("fault", ["source", "admission"])
def test_admission_failure_before_startup_and_request(harness, fault):
    h = harness
    h.fault.add(fault)
    assert run_departure(**h.args) == 1
    assert os.read(h.parent["startup"], 1) == b""
    assert "connect" not in h.events
    with pytest.raises(BrokenPipeError):
        os.write(h.parent["request"], b"synthetic")


def test_invalid_private_request_never_connects(harness):
    h = harness
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_departure, **h.args)
        submit_request(h, b"{}")
        assert future.result(timeout=2) == 1
    assert "connect" not in h.events


def test_permission_grant_request_uses_distinct_bootstrap_dispatch(harness, monkeypatch):
    """Exercise the real three-pipe bootstrap with the closed GRANT wire schema."""
    from dpone.adapters import mssql_sqlclient_permission_grant_departure as grant_adapter
    from dpone.adapters import mssql_tds_coordinator_connection as connections
    from dpone.adapters import mssql_tds_installation as installation
    from dpone.contracts.mssql_sqlclient_permission_grant_departure import (
        SqlClientPermissionGrantDepartureCredentials,
    )
    from dpone.contracts.mssql_sqlclient_permission_grant_departure_codec import (
        decode_result,
        encode_credentials,
        encode_request,
    )
    from tests.test_mssql_sqlclient_permission_grant_departure import grant_departure_fixture

    h = harness
    _, fixture_request, fixture_result = grant_departure_fixture()
    h.args["implementation_sha256"] = fixture_request.plan.implementation_sha256
    monkeypatch.setattr(
        installation, "worker_installation_digest", lambda root: fixture_request.plan.implementation_sha256
    )

    class GrantFactory:
        def __init__(self, build, profile):
            h.events.append("grant-admission")

        def connect(self, value, *, deadline):
            h.events.append("grant-connect")
            return TdsSqlConnection(
                SimpleNamespace(close=lambda: h.events.append("sql-close")),
                SimpleNamespace(close=lambda: h.events.append("cursor-close")),
            )

    monkeypatch.setattr(connections, "TdsCoordinatorConnection", GrantFactory)

    class Observer:
        def __init__(self, connection, request):
            h.events.append("grant-observer")
            self.request = request

        def observe(self, nonce):
            h.events.append("grant-observe")
            return replace(
                fixture_result,
                request_sha256=sha256(encode_request(self.request)).hexdigest(),
            )

    monkeypatch.setattr(grant_adapter, "SqlClientPermissionGrantDepartureObserver", Observer)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_departure, **h.args)
        startup = decode_startup(read_worker_message(h.parent["startup"], deadline=h.deadline, max_payload=16384))
        plan = replace(
            fixture_request.plan,
            implementation_sha256=h.args["implementation_sha256"],
            package_root=startup.package_root,
            admission_sha256=sha256(h.args["admission"]).hexdigest(),
            startup_deadline=h.args["startup_deadline"],
            operation_deadline=h.args["operation_deadline"],
            max_address_space_bytes=h.args["address_space"],
        )
        grant_request = replace(fixture_request, plan=plan, startup=startup)
        grant_result = replace(fixture_result, request_sha256=sha256(encode_request(grant_request)).hexdigest())
        h.material = TdsConnectionMaterial(
            "synthetic",
            1433,
            plan.grant_evidence.authority.database.name,
            plan.management_admission.login.name,
            "synthetic-secret",
        )
        payload = encode_credentials(
            SqlClientPermissionGrantDepartureCredentials(
                request=grant_request,
                connection_material=h.material,
                session_nonce=b"n" * 32,
            )
        )
        write_worker_control(
            h.parent["request"], encode_message(payload, max_payload=196608), deadline=h.deadline, max_bytes=196612
        )
        os.close(h.parent["request"])
        raw = read_worker_message(h.parent["result"], deadline=h.deadline, max_payload=65536)
        assert decode_result(raw, grant_request) == grant_result
        assert future.result(timeout=2) == 0


def test_restricted_writer_request_uses_distinct_bootstrap_dispatch(harness, monkeypatch):
    """Exercise the real child branch for independent P9b management verification."""
    from dpone.adapters import mssql_tds_coordinator_connection as connections
    from dpone.adapters import mssql_tds_installation as installation
    from dpone.app import mssql_sqlclient_restricted_writer_departure as restricted_departure
    from dpone.contracts.mssql_sqlclient_restricted_writer_settlement import (
        RestrictedWriterDepartureCredentials,
    )
    from dpone.contracts.mssql_sqlclient_restricted_writer_settlement_codec import (
        decode_result,
        encode_credentials,
        encode_request,
    )
    from tests.test_mssql_sqlclient_restricted_writer_settlement_codec import settlement_fixture

    h = harness
    fixture_plan, _, fixture_result = settlement_fixture()
    h.args["implementation_sha256"] = fixture_plan.implementation_sha256
    monkeypatch.setattr(installation, "worker_installation_digest", lambda root: fixture_plan.implementation_sha256)

    class Factory:
        def __init__(self, build, profile):
            h.events.append("restricted-admission")

        def connect(self, value, *, deadline):
            h.events.append("restricted-connect")
            return TdsSqlConnection(
                SimpleNamespace(close=lambda: h.events.append("sql-close")),
                SimpleNamespace(close=lambda: h.events.append("cursor-close")),
            )

    monkeypatch.setattr(connections, "TdsCoordinatorConnection", Factory)

    class Observer:
        def __init__(self, connection, request, operations):
            h.events.append("restricted-observer")
            self.request = request

        def observe(self, nonce):
            h.events.append("restricted-observe")
            return replace(fixture_result, request_sha256=sha256(encode_request(self.request)).hexdigest())

    monkeypatch.setattr(restricted_departure, "SqlClientRestrictedWriterSettlementObserver", Observer)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_departure, **h.args)
        startup = decode_startup(read_worker_message(h.parent["startup"], deadline=h.deadline, max_payload=16384))
        plan = replace(
            fixture_plan,
            implementation_sha256=h.args["implementation_sha256"],
            package_root=startup.package_root,
            admission_sha256=sha256(h.args["admission"]).hexdigest(),
            startup_deadline=h.args["startup_deadline"],
            operation_deadline=h.args["operation_deadline"],
            max_address_space_bytes=h.args["address_space"],
        )
        request = restricted_departure.codec.RestrictedWriterDepartureRequest(plan=plan, startup=startup)
        expected = replace(fixture_result, request_sha256=sha256(encode_request(request)).hexdigest())
        h.material = TdsConnectionMaterial(
            "synthetic",
            1433,
            plan.grant_evidence.authority.database.name,
            plan.management_admission.login.name,
            "synthetic-secret",
        )
        payload = encode_credentials(
            RestrictedWriterDepartureCredentials(
                request=request,
                connection_material=h.material,
                session_nonce=b"n" * 32,
            )
        )
        write_worker_control(
            h.parent["request"], encode_message(payload, max_payload=196608), deadline=h.deadline, max_bytes=196612
        )
        os.close(h.parent["request"])
        raw = read_worker_message(h.parent["result"], deadline=h.deadline, max_payload=65536)
        assert decode_result(raw, request) == expected
        assert future.result(timeout=2) == 0


def test_request_requires_actual_eof(harness):
    h = harness
    deadline = time.monotonic() + 0.2
    h.args.update(startup_deadline=deadline, operation_deadline=deadline)
    h.request = replace(h.request, plan=replace(h.request.plan, startup_deadline=deadline, operation_deadline=deadline))
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_departure, **h.args)
        submit_request(h, eof=False)
        assert future.result(timeout=2) == 1
    assert "connect" not in h.events


@pytest.mark.parametrize(
    "field,value", [("startup_deadline", True), ("operation_deadline", float("nan")), ("startup_deadline", 1e30)]
)
def test_invalid_original_deadlines_no_startup(harness, field, value):
    h = harness
    h.args[field] = value
    assert run_departure(**h.args) == 1
    assert os.read(h.parent["startup"], 1) == b""
    assert "admission" not in h.events


def test_fixed_three_pipe_entrypoint():
    import inspect

    parameters = inspect.signature(run_departure).parameters
    assert {p for p in parameters if p.endswith("_fd")} == {"startup_fd", "request_fd", "result_fd"}


def test_blocked_sql_close_has_no_result_until_actual_completion(harness, monkeypatch):
    from threading import Event

    h = harness
    entered, release = Event(), Event()
    original = TdsSqlConnection.close

    def close(connection):
        entered.set()
        assert release.wait(2)
        original(connection)

    monkeypatch.setattr(TdsSqlConnection, "close", close)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_departure, **h.args)
        try:
            submit_request(h)
            assert entered.wait(1)
            with pytest.raises(BlockingIOError):
                os.read(h.parent["result"], 1)
            assert not future.done()
        finally:
            release.set()
        body = read_worker_message(h.parent["result"], deadline=h.deadline, max_payload=32768)
        assert decode_departure_result(body, request=h.request).departure == sample()
        assert future.result(timeout=2) == 0


def test_result_close_ambiguity_preserves_frame_but_nonzero_exit(harness, monkeypatch):
    h = harness
    original = os.close
    closes = []

    def close(fd):
        original(fd)
        if fd == h.args["result_fd"]:
            closes.append(fd)
            raise OSError("synthetic close acknowledgment lost")

    monkeypatch.setattr(os, "close", close)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(run_departure, **h.args)
        submit_request(h)
        body = read_worker_message(h.parent["result"], deadline=h.deadline, max_payload=32768)
        assert decode_departure_result(body, request=h.request).departure == sample()
        assert future.result(timeout=2) == 1
    assert closes == [h.args["result_fd"]]


def test_admission_late_return_cannot_acknowledge_startup(harness, monkeypatch):
    from dpone.adapters import mssql_tds_coordinator_connection as connections

    h = harness
    original = connections.TdsCoordinatorConnection

    def late(*args):
        factory = original(*args)
        monkeypatch.setattr(time, "monotonic", lambda: h.deadline + 1)
        return factory

    monkeypatch.setattr(connections, "TdsCoordinatorConnection", late)
    assert run_departure(**h.args) == 1
    assert os.read(h.parent["startup"], 1) == b""
    assert "connect" not in h.events


def test_noncanonical_launch_admission_is_rejected(harness):
    h = harness
    h.args["admission"] += b" "
    assert run_departure(**h.args) == 1
    assert "admission" not in h.events
    assert os.read(h.parent["startup"], 1) == b""


@pytest.mark.parametrize("operation", ["create", "observe"])
@pytest.mark.parametrize("reused", [False, True])
@pytest.mark.parametrize(
    "fault",
    [
        None,
        "close",
        "source",
        "mixed",
        "unknown",
        "username",
        "database",
        "late-close",
        "own",
        "uuid",
        "request",
        "transaction",
        "third-party",
        "extra",
        "no-eof",
        "trailing",
        "truncated",
        "oversize",
        "cross-binding",
        "result-close",
        "blocked-close",
    ],
)
def test_v2_actual_observer_single_connection_closed_dispatch(harness, monkeypatch, reused, fault, operation):
    """Real bootstrap/SQL adapter with scripted cursor, never a live SQL claim."""
    from dataclasses import fields
    from threading import Event

    from dpone.adapters import mssql_sqlclient_create_exclusion as legacy
    from dpone.adapters import mssql_tds_channels as channels
    from dpone.adapters import mssql_tds_coordinator_connection as connections
    from dpone.adapters.mssql_sqlclient_create_exclusion_v2_sql import CONNECTIONS_SQL, REQUESTS_SQL, TRANSACTIONS_SQL
    from dpone.app.mssql_sqlclient_departure_request import (
        SqlClientDepartureCredentialsV2,
        encode_departure_credentials_v2,
    )
    from dpone.contracts.mssql_sqlclient_departure_ipc_v2 import SqlClientDeparturePlanV2, SqlClientDepartureRequestV2
    from dpone.contracts.mssql_sqlclient_departure_ipc_v2_codec import decode_departure_result_v2
    from dpone.contracts.mssql_sqlclient_observation import SqlClientObserverAdmission
    from dpone.contracts.mssql_sqlclient_observer_incarnation import observer_incarnation_digest
    from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
    from tests.mssql_sqlclient_departure_v2_fixtures import sample as v2_sample
    from tests.test_mssql_sqlclient_create_exclusion_v2 import V2Cursor

    h = harness
    entered, release = Event(), Event()
    d = v2_sample(reused=reused)
    if operation == "observe":
        a = d.observer.authority
        login = replace(
            a.login, name="verifier", original_name="verifier", sid="cc", original_sid="cc", principal_id=301
        )
        authority = replace(
            a,
            login=login,
            principal_resolution=replace(a.principal_resolution, name="verifier_user", sid="cc", principal_id=6),
        )
        own = replace(d.observer, authority=authority)
        digest = observer_incarnation_digest(own)
        d = replace(
            d, observer=own, samples=tuple(replace(x, before_sha256=digest, after_sha256=digest) for x in d.samples)
        )
    plan = SqlClientDeparturePlanV2(
        **{f.name: getattr(h.request.plan, f.name) for f in fields(h.request.plan) if f.name != "schema"},
        observer_admission=SqlClientObserverAdmission(
            d.observer.authority.server,
            d.observer.authority.database,
            d.observer.authority.login,
            d.observer.authority.transport,
        ),
    )
    h.request = SqlClientDepartureRequestV2(plan=plan, startup=h.request.startup)
    if operation == "observe":
        from tests.test_mssql_sqlclient_observe_departure_contract import from_create_request

        h.request = from_create_request(h.request)
    h.material = replace(h.material, username=d.observer.authority.login.name)
    cursor = V2Cursor(d)
    cursor.close = lambda: h.events.append("cursor-close")
    if fault in ("uuid", "third-party", "request", "transaction"):
        query, row = {
            "uuid": (CONNECTIONS_SQL, (1, 1, 1)),
            "third-party": (CONNECTIONS_SQL, (2, 1, 0)),
            "request": (REQUESTS_SQL, (1, 0, 1, None, None, None, None)),
            "transaction": (TRANSACTIONS_SQL, (1,)),
        }[fault]
        cursor.results[query] = [row]
    if fault == "extra":
        cursor.extra = True
    if fault == "own":
        cursor.mutate = lambda index, row: row.__setitem__(0, str(d.original.connection_id))
    if fault == "no-eof":
        deadline = time.monotonic() + 0.2
        h.args.update(startup_deadline=deadline, operation_deadline=deadline)
        h.request = replace(
            h.request, plan=replace(h.request.plan, startup_deadline=deadline, operation_deadline=deadline)
        )
    original_write = channels.write_worker_control

    def write(fd, *args, **kwargs):
        if fd == h.args["result_fd"]:
            assert h.events[-2:] == ["cursor-close", "sql-close"]
            h.events.append("result-write")
        return original_write(fd, *args, **kwargs)

    monkeypatch.setattr(channels, "write_worker_control", write)
    original_close = os.close

    def fd_close(fd):
        original_close(fd)
        if fault == "result-close" and fd == h.args["result_fd"]:
            h.events.append("result-close")
            raise OSError("synthetic close ambiguity")

    monkeypatch.setattr(os, "close", fd_close)

    class Factory:
        def __init__(self, *args):
            h.events.append("admission")

        def connect(self, value, **kw):
            assert value == h.material
            h.events.append("connect")

            def close():
                h.events.append("sql-close")
                if fault == "close":
                    raise OSError("synthetic close ambiguity")
                if fault == "late-close":
                    monkeypatch.setattr(time, "monotonic", lambda: h.deadline + 1)
                if fault == "blocked-close":
                    entered.set()
                    assert release.wait(2)

            return TdsSqlConnection(SimpleNamespace(close=close), cursor)

    def forbidden(*args, **kw):
        raise AssertionError("v1 observer must not run for explicit v2")

    monkeypatch.setattr(connections, "TdsCoordinatorConnection", Factory)
    monkeypatch.setattr(legacy, "SqlClientCreateExclusionObserver", forbidden)
    if operation == "create":
        payload = encode_departure_credentials_v2(
            SqlClientDepartureCredentialsV2(request=h.request, connection_material=h.material)
        )
    if operation == "observe":
        from dpone.app.mssql_sqlclient_departure_request import (
            SqlClientObserveDepartureCredentials,
            encode_observe_departure_credentials,
        )

        payload = encode_observe_departure_credentials(
            SqlClientObserveDepartureCredentials(request=h.request, connection_material=h.material)
        )
    if fault in ("mixed", "unknown", "username", "database"):
        data = strict_json_object(payload)
        if fault in ("mixed", "unknown"):
            data["schema"] = "dpone.sqlclient.departure-credentials." + ("v1" if fault == "mixed" else "v3")
        else:
            data["connection_material"][fault] = "wrong"
        payload = canonical_json_bytes(data)
    if fault == "cross-binding":
        data = strict_json_object(payload)
        data["request"]["plan"]["admission_sha256"] = "0" * 64
        payload = canonical_json_bytes(data)
    if fault == "trailing":
        payload += b"{}"
    if fault == "source":
        h.fault.add("source")
        assert run_departure(**h.args) == 1
    else:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(run_departure, **h.args)
            if fault in ("oversize", "truncated"):
                decode_startup(read_worker_message(h.parent["startup"], deadline=h.deadline, max_payload=16384))
                os.write(
                    h.parent["request"], (1048577 if fault == "oversize" else len(payload)).to_bytes(4, "big") + b"x"
                )
                os.close(h.parent["request"])
            else:
                submit_request(h, payload, eof=fault != "no-eof")
            if fault == "blocked-close":
                try:
                    assert entered.wait(1)
                    with pytest.raises(BlockingIOError):
                        os.read(h.parent["result"], 1)
                    assert not future.done()
                finally:
                    release.set()
            if fault in (None, "result-close", "blocked-close"):
                body = read_worker_message(h.parent["result"], deadline=h.deadline, max_payload=32768)
                if operation == "observe":
                    from dpone.contracts.mssql_sqlclient_observe_departure_codec import decode_observe_departure_result

                    result = decode_observe_departure_result(
                        body, request=h.request, observer_admission=h.request.plan.observer_admission
                    )
                else:
                    result = decode_departure_result_v2(body, request=h.request)
                assert result.departure == d
                assert future.result(timeout=2) == int(fault == "result-close")
            else:
                assert future.result(timeout=2) == 1
                assert os.read(h.parent["result"], 1) == b""
    effects = fault in (
        None,
        "close",
        "late-close",
        "own",
        "uuid",
        "request",
        "transaction",
        "third-party",
        "extra",
        "result-close",
        "blocked-close",
    )
    assert h.events.count("connect") == h.events.count("sql-close") == int(effects)
    if fault in (None, "close", "late-close", "result-close", "blocked-close"):
        assert len(cursor.calls) == 27 and cursor.capture_count == 13
    elif not effects:
        assert not cursor.calls
    if fault == "result-close":
        assert h.events.count("result-close") == 1
