"""Real SQLite parent actors with explicit failure-only child cleanup probes.

These tests do not fabricate SQL authority or claim process containment. They
exercise original owner/registration actors and require retained failure state.
"""

import importlib
import subprocess
import sys
from contextlib import contextmanager
from dataclasses import replace
from time import monotonic
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_sqlclient_observe_process import PythonSqlClientObserveLauncher
from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.app.mssql_sqlclient_stage_locator_composition import admit_sqlclient_state_domain
from dpone.app.mssql_tds_attempt_composition import create_tds_attempt
from dpone.app.mssql_tds_coordinator_composition import create_tds_coordinator
from dpone.contracts.bounded_window import WindowOutcomeUnknown
from dpone.contracts.mssql_tds_directory import TdsCoordinatorCommand
from dpone.contracts.mssql_tds_validation import deadline_nanoseconds
from dpone.services.mssql_tds_attempt_continuation import TdsObserveContinuation
from tests.test_mssql_sqlclient_observe import request
from tests.test_mssql_tds_directory import LIMITS
from tests.test_mssql_tds_directory_journal import OWNER


@pytest.fixture
def observe_parent(tmp_path):
    """Exact admitted factory and actual original attempt/directory/coordinator."""
    api = importlib.import_module("dpone.app.mssql_sqlclient_observe_composition")
    value = replace(request(), operation_deadline_ns=deadline_nanoseconds(monotonic() + 10.0))
    store = SQLiteWindowStore(tmp_path / "observe.sqlite", clock=lambda: 1.0)
    lease = store.acquire(value.parent.target_key, OWNER.owner, 30)
    pool = TdsActorPool(capacity=6)

    @contextmanager
    def factory():
        yield store

    admitted = admit_sqlclient_state_domain(store_factory=factory, lease=lease, pool=pool, deadline=monotonic() + 5.0)
    attempt = create_tds_attempt(
        pool,
        admitted,
        value.parent,
        LIMITS,
        lease,
        supervisor_token=OWNER.supervisor_id,
        backend="mssql_sqlclient",
        deadline=monotonic() + 5.0,
    )
    parent = SimpleNamespace(
        request=value,
        store=store,
        lease=lease,
        pool=pool,
        factory=admitted,
        attempt=attempt,
        root=tmp_path,
        handles=[],
        api=api,
    )
    try:
        yield parent
    finally:
        # No test in this fixture spawns a child. Release only the failed local
        # association to avoid leaving the real SQLite actor threads behind.
        active = attempt._observe_helper_id
        if active is not None:
            attempt._end_observe(active)
        attempt.close(deadline=monotonic() + 2.0)
        pool.close(deadline=monotonic() + 2.0)


def retained_parent(parent):
    """Construct original retained state through real acknowledged gateways."""
    handle = parent.api.SqlClientRetainedObserve(parent.attempt, parent.request, parent.factory, parent.pool, 1.0)
    reserved = parent.attempt.reserve_operation(
        uuid4(), TdsCoordinatorCommand.OBSERVE, parent.request.command_sha256, deadline=handle.operation_deadline
    )
    handle.identity = TdsObserveContinuation.reserved_identity(parent.request, reserved, "b" * 64)
    handle.writer = create_tds_coordinator(
        parent.pool,
        parent.factory,
        handle.identity,
        reserved.state.limits,
        parent.lease,
        supervisor_token=OWNER.supervisor_id,
        deadline=handle.operation_deadline,
    )
    parent.attempt._begin_observe(handle.helper_id, handle.identity, deadline=handle.operation_deadline)
    handle._active = True
    handle.continuation = TdsObserveContinuation(
        parent.attempt, handle.helper_id, handle.identity, handle.writer, deadline=handle.operation_deadline
    )
    parent.handles.append(handle)
    return handle


def test_begin_observe_failure_cannot_orphan_latched_association(observe_parent, monkeypatch):
    parent = observe_parent
    launcher = object.__new__(PythonSqlClientObserveLauncher)
    launcher.implementation_sha256 = "b" * 64
    monkeypatch.setattr(launcher, "assert_installation", lambda: None)
    monkeypatch.setattr(launcher, "spawn", lambda **kwargs: pytest.fail("spawn after failed ownership"))
    original = parent.attempt._lifecycle.assert_authority

    def fail_after_latch(*, deadline):
        if parent.attempt._observe_helper_id is not None:
            raise WindowOutcomeUnknown("synthetic_lost_owner_ack")
        return original(deadline=deadline)

    monkeypatch.setattr(parent.attempt._lifecycle, "assert_authority", fail_after_latch)
    with pytest.raises(observe_parent.api.SqlClientObserveUnknown) as caught:
        parent.api.open_sqlclient_observe(
            parent.attempt,
            parent.request,
            pool=parent.pool,
            admitted_factory=parent.factory,
            lease=parent.lease,
            evidence_root=parent.root,
            launcher=launcher,
            connection_material=lambda: pytest.fail("premature credentials"),
            startup_timeout=1.0,
            termination_timeout=1.0,
        )
    retained = caught.value.retained
    assert retained.attempt is parent.attempt and retained.pool is parent.pool
    assert retained.factory is parent.factory
    if parent.attempt._observe_helper_id is not None:
        assert retained._active, "latched association must remain owned by retained cleanup capability"
        assert not retained._closed, "cleanup cannot be successful while its association is orphaned"
    else:
        assert retained._closed, "completed no-child cleanup should release its association"


def test_begin_rejection_before_latch_closes_acquired_coordinator(observe_parent, monkeypatch):
    parent = observe_parent
    launcher = object.__new__(PythonSqlClientObserveLauncher)
    launcher.implementation_sha256 = "b" * 64
    monkeypatch.setattr(launcher, "assert_installation", lambda: None)
    monkeypatch.setattr(launcher, "spawn", lambda **kwargs: pytest.fail("spawn after rejected begin"))

    def reject(*args, **kwargs):
        raise ValueError("synthetic_admission_rejection")

    monkeypatch.setattr(parent.attempt, "_begin_observe", reject)
    with pytest.raises(parent.api.SqlClientObserveUnknown) as caught:
        parent.api.open_sqlclient_observe(
            parent.attempt,
            parent.request,
            pool=parent.pool,
            admitted_factory=parent.factory,
            lease=parent.lease,
            evidence_root=parent.root,
            launcher=launcher,
            connection_material=lambda: pytest.fail("premature credentials"),
            startup_timeout=1.0,
            termination_timeout=1.0,
        )
    handle = caught.value.retained
    assert handle.writer is not None and id(handle.writer) in handle._closed_gateways
    assert parent.attempt._observe_helper_id is None
    assert handle._closed and not handle._active


class FailedContainmentProbe:
    """Failure-only local child probe: never supplies a fake exit or authority."""

    cleanup_deadline = None

    def __init__(self):
        self.containment_attempts = []

    def contain(self, *, deadline):
        self.containment_attempts.append(deadline)
        raise WindowOutcomeUnknown("synthetic_unreaped_child")

    def assert_current(self):
        pytest.fail("child check must follow original parent authority")

    def close(self):
        pytest.fail("unreaped child must not close")


@pytest.mark.parametrize("effect", ["poison", "contain"])
def test_public_assert_current_takeover_poisons_and_contains(observe_parent, effect):
    parent = observe_parent
    handle = retained_parent(parent)
    child = FailedContainmentProbe()
    handle.child = child
    original_gateways = (parent.attempt._lifecycle, parent.attempt._directory, handle.writer)
    parent.store.release(parent.lease)
    parent.store.acquire(parent.request.parent.target_key, "successor", 30)
    with pytest.raises(WindowOutcomeUnknown):
        handle.assert_current()
    if effect == "poison":
        assert handle._faulted, "failed public authority call must permanently poison forward use"
    else:
        assert child.containment_attempts, "lost parent authority must initiate independent child containment"
    assert handle._cleanup_deadline is not None
    assert handle._active and parent.attempt._observe_helper_id == handle.helper_id
    # Authority failure may stop the actor thread; its original gateway must
    # remain retained independently of whether that thread is still live.
    assert original_gateways == (parent.attempt._lifecycle, parent.attempt._directory, handle.writer)
    assert all(any(actor is gateway for actor in parent.pool._actors) for gateway in original_gateways)
    assert any(gateway is original_gateways[0] for gateway in handle._failed_gateways)
    assert handle.child is child and not handle._closed
    assert not handle._closed_gateways and not handle._unknown_gateways


class AmbiguousCloseProbe:
    """Unresolved-launch close fault; no SQL authority is supplied or accepted."""

    def __init__(self):
        self.close_calls = 0
        self.contain_calls = []

    def contain(self, *, deadline):
        self.contain_calls.append(deadline)

    def close(self):
        self.close_calls += 1
        raise OSError("synthetic_close_effect_ack_lost")


def test_ambiguous_resource_close_is_not_repeated_or_budget_renewed(observe_parent):
    handle = retained_parent(observe_parent)
    unresolved = AmbiguousCloseProbe()
    handle.unresolved = unresolved
    with pytest.raises(observe_parent.api.SqlClientObserveUnknown):
        handle.close(deadline=monotonic() + 1.0)
    original_budget = handle._cleanup_deadline
    with pytest.raises(observe_parent.api.SqlClientObserveUnknown):
        handle.close(deadline=monotonic() + 5.0)
    assert handle._cleanup_deadline == original_budget
    assert handle.unresolved is unresolved and handle._active
    assert unresolved.close_calls == 1, "lost close acknowledgement cannot authorize repeating descriptor closure"
    assert observe_parent.pool.live_count >= 3


def test_gateway_close_lost_ack_retains_original_without_retry(observe_parent, monkeypatch):
    handle = retained_parent(observe_parent)
    writer = handle.writer
    original_close = writer.close
    calls = []

    def lose_ack(*, deadline):
        calls.append(deadline)
        original_close(deadline=deadline)
        raise OSError("synthetic_gateway_close_ack_lost")

    monkeypatch.setattr(writer, "close", lose_ack)
    with pytest.raises(observe_parent.api.SqlClientObserveUnknown):
        handle.close(deadline=monotonic() + 1.0)
    original_deadline = handle._cleanup_deadline
    with pytest.raises(observe_parent.api.SqlClientObserveUnknown):
        handle.close(deadline=monotonic() + 5.0)
    assert handle.writer is writer and id(writer) in handle._unknown_gateways
    assert id(writer) not in handle._closed_gateways
    assert calls == [original_deadline] and not handle._closed
    assert handle._cleanup_deadline == original_deadline


def test_failed_initial_cleanup_clock_retains_unknown_without_renewal(observe_parent, monkeypatch):
    handle = retained_parent(observe_parent)
    calls = []

    def fail_clock():
        calls.append("clock")
        raise OSError("synthetic_clock_failure")

    monkeypatch.setattr(observe_parent.api, "monotonic", fail_clock)
    with pytest.raises(observe_parent.api.SqlClientObserveUnknown) as caught:
        handle.assert_current()
    assert caught.value.retained is handle
    captured_calls = len(calls)
    monkeypatch.setattr(observe_parent.api, "monotonic", lambda: pytest.fail("renewed failed cleanup budget"))
    with pytest.raises(observe_parent.api.SqlClientObserveUnknown) as repeated:
        handle.close(deadline=monotonic() + 10.0)
    assert repeated.value.retained is handle and len(calls) == captured_calls
    assert handle._active and not handle._closed and handle._cleanup_deadline is None
    assert observe_parent.attempt._observe_helper_id == handle.helper_id


def test_public_observe_composition_imports_in_fresh_interpreter():
    completed = subprocess.run(
        [sys.executable, "-c", "import dpone.app.mssql_sqlclient_observe_composition"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_observe_composition_resolves_effects_through_closed_bundle():
    api = importlib.import_module("dpone.app.mssql_sqlclient_observe_composition")
    bundle = api._observe_dependencies()

    assert bundle.launcher_type is PythonSqlClientObserveLauncher
    assert bundle.catalog_type.__name__ == "SqlClientObserveCatalog"
    assert callable(bundle.read_frame)
    assert callable(bundle.write_frame)
    assert callable(bundle.require_quiet)
    assert callable(bundle.create_coordinator)
    assert callable(bundle.open_evidence)
    assert callable(bundle.decode_connection_admission)


def test_observe_composition_has_no_concrete_effect_adapter_imports():
    from pathlib import Path

    source = Path(__file__).parents[1] / "src/dpone/app/mssql_sqlclient_observe_composition.py"
    text = source.read_text(encoding="utf-8")

    assert "from dpone.adapters.filesystem_evidence import" not in text
    assert "from dpone.adapters.mssql_sqlclient_observe_process import" not in text
    assert "from dpone.adapters.mssql_sqlclient_observe_transport import" not in text
    assert "from dpone.adapters.mssql_tds_coordinator_connection import" not in text


class ReadOnlyObserveCursor:
    """Factory for the existing scripted SQL cursor, never serialized authority."""

    @staticmethod
    def create():
        from dpone.adapters.mssql_sqlclient_stage_catalog_sql import SCHEMA_SQL, STAGE_VISIBILITY_SQL
        from tests.test_mssql_sqlclient_grant_catalog import Cursor

        class CatalogCursor(Cursor):
            def __init__(self):
                super().__init__()
                self.permissions = []

            def execute(self, statement, *parameters):
                if statement == STAGE_VISIBILITY_SQL:
                    rows = [(16, 1, 1, 1)]
                elif statement == SCHEMA_SQL:
                    rows = [(self.stage.schema_id, self.stage.schema_name)]
                elif statement.startswith("SELECT CASE WHEN EXISTS"):
                    rows = [(1,)]
                else:
                    return super().execute(statement, *parameters)
                self.calls.append((statement, parameters))
                self.rows = rows

        return CatalogCursor()


@pytest.mark.skipif(sys.platform != "linux", reason="requires actual Linux guarded child and pidfd containment")
def test_actual_guarded_observe_empty_inventory_and_selected_stage_lifecycle(observe_parent, monkeypatch):
    """Real production bootstrap/session algorithms over scripted SQL, no live SQL.

    Empty inventory intentionally needs no historical CREATE file; file-actor
    provenance is covered by the existing source-free inventory tests separately.
    """
    from pathlib import Path

    from dpone.adapters import mssql_sqlclient_observe_process as process_module
    from dpone.adapters.filesystem_evidence import PinnedEvidenceReadFactory
    from dpone.adapters.mssql_tds_coordinator_connection import (
        TdsBinaryPin,
        TdsCoordinatorBuild,
        encode_connection_admission,
    )
    from dpone.adapters.mssql_tds_installation import worker_installation_digest
    from tests.test_mssql_tds_coordinator_request import values

    parent = observe_parent
    # Exercise the previously intermittent integer -> float -> integer loss on
    # every real child run; the request's exact integer/hash must remain intact.
    start = parent.request.operation_deadline_ns
    lossy = next(ns for ns in range(start, start + 10000) if deadline_nanoseconds(ns / 1e9) != ns)
    parent.request = replace(parent.request, operation_deadline_ns=lossy)
    root = Path(__file__).parents[1]
    package_root = root / "src"
    original, *_ = values()
    pin = TdsBinaryPin(Path(sys.executable).absolute(), "a" * 64)
    admission = encode_connection_admission(TdsCoordinatorBuild(pin, pin, pin, pin), original.driver_profile)
    # Keep the actual source guard, startup, socket framing, request validation,
    # SQL session algorithms and watchdog. Replace vendor binary admission and
    # connection I/O inside this hermetic child; do not manufacture an authority DTO.
    instrument = """
import sys
from types import SimpleNamespace
from dpone.adapters import mssql_tds_coordinator_connection as vendor_boundary
from dpone.adapters.mssql_tds_coordinator_connection import TdsCoordinatorConnection, TdsSqlConnection

_vendor_admit_count = 0

def scripted_admit(build):
    global _vendor_admit_count
    _vendor_admit_count += 1
    assert _vendor_admit_count == 1
    assert type(build) is vendor_boundary.TdsCoordinatorBuild
    assert all(pin.sha256 == "a" * 64 and str(pin.path) == sys.executable
               for pin in (build.interpreter, build.pyodbc, build.driver, build.driver_manager))
    return SimpleNamespace()

# Both vendor binary admission and connection I/O are mocked in this hermetic test.
# Source/OS guard, handshake and SQL authority algorithms remain real.
vendor_boundary._admit = scripted_admit
from tests.test_mssql_sqlclient_observe_composition import ReadOnlyObserveCursor

def scripted_connect(self, material, *, deadline):
    return TdsSqlConnection(SimpleNamespace(close=lambda: None), ReadOnlyObserveCursor.create())

TdsCoordinatorConnection.connect = scripted_connect
"""
    shim = process_module._OBSERVE_SHIM.replace(
        "runpy.run_path(bootstrap, run_name='__main__')",
        instrument + "\nrunpy.run_path(bootstrap, run_name='__main__')",
    )
    monkeypatch.setattr(process_module, "_OBSERVE_SHIM", shim)
    launcher = PythonSqlClientObserveLauncher(
        python_executable=Path(sys.executable),
        package_root=package_root,
        implementation_sha256=worker_installation_digest(package_root),
        admission=admission,
        max_address_space_bytes=2**30,
        dependency_paths=(root, Path(pytest.__file__).parents[1]),
    )
    handle = None
    try:
        handle = parent.api.open_sqlclient_observe(
            parent.attempt,
            parent.request,
            pool=parent.pool,
            admitted_factory=parent.factory,
            lease=parent.lease,
            evidence_root=parent.root,
            launcher=launcher,
            connection_material=lambda: replace(original.connection_material, database=parent.request.parent.database),
            startup_timeout=4.0,
            termination_timeout=2.0,
        )
        result = handle.collect_authenticated(reader_factory=PinnedEvidenceReadFactory(parent.root))
        assert result.creates == () and result.inventory.members == ()
        assert handle.factory is parent.factory and handle.pool is parent.pool
        handle.child.assert_current()  # Must still be alive after END/collection.
        observed = handle.observe_selected()
        assert observed.before == observed.after == parent.request.selected_stage
        assert observed.empty == 1
        handle.assert_current()
        assert parent.pool.live_count >= 4
        child = handle.child
        handle.close(deadline=monotonic() + 2.0)
        assert child._containment.exit.reaped
        assert parent.attempt._observe_helper_id is None
        remaining = parent.pool.live_count
        handle.close(deadline=monotonic() + 10.0)
        assert parent.pool.live_count == remaining == 2
    finally:
        if handle is not None:
            handle.close(deadline=monotonic() + 2.0)


def test_unreaped_child_retains_exact_parent_gateways(observe_parent):
    parent = observe_parent
    handle = retained_parent(parent)
    child = FailedContainmentProbe()
    handle.child = child
    writer = handle.writer
    with pytest.raises(parent.api.SqlClientObserveUnknown) as caught:
        handle.close(deadline=monotonic() + 1.0)
    assert caught.value.retained is handle
    assert handle.writer is writer and handle.child is child
    assert handle.factory is parent.factory and handle.pool is parent.pool
    assert handle._active and not handle._closed
    assert parent.attempt._observe_helper_id == handle.helper_id
    assert parent.pool.live_count == 3
    assert child.containment_attempts == [handle._cleanup_deadline]
