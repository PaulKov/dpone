"""Fault controls preserve durable boundaries and real delegate acknowledgements."""

import pytest
from tools.native_delivery_local.faults import Faults, FaultStore, ReceiptDelegate, TargetDelegate


def test_eof_fault_occurs_only_after_durable_completion():
    calls = []

    class Store:
        def save(self, *args):
            calls.append(args)
            return "ack"

    faults = Faults()
    faults.arm("after_eof")
    store = FaultStore(Store(), faults)
    assert store.save("mssql-native-chunks-v1/x", None, '{"phase":"staging"}', None) == "ack"
    with pytest.raises(RuntimeError, match="after_eof"):
        store.save(
            "mssql-native-chunks-v1/x", 1, '{"phase":"stage_complete","completion_metadata":{"receipt":"eof"}}', None
        )
    assert len(calls) == 2
    assert faults.events == ["after_eof"]


def test_failed_durable_save_never_reports_eof():
    class Store:
        def save(self, *args):
            raise OSError("disk unavailable")

    faults = Faults()
    faults.arm("after_eof")
    with pytest.raises(OSError):
        FaultStore(Store(), faults).save("mssql-native-chunks-v1/x", None, "{}", None)
    assert not faults.events


def test_lost_ack_follows_actual_commit_and_only_finalizer():
    calls = []

    class Connector:
        def commit_transaction(self):
            calls.append("commit")

    faults = Faults()
    faults.arm("lost_ack")
    target = TargetDelegate(Connector(), faults)
    target.commit_transaction()
    assert faults.publications == 0
    faults.active_finalizer = True
    with pytest.raises(RuntimeError, match="lost_ack"):
        target.commit_transaction()
    assert calls == ["commit", "commit"]
    assert faults.publications == 1
    assert not faults.known


def test_failed_rollback_is_never_proof():
    class Connector:
        def rollback(self):
            raise OSError("rollback unavailable")

    faults = Faults()
    faults.active_finalizer = True
    with pytest.raises(OSError):
        TargetDelegate(Connector(), faults).rollback()
    assert not faults.rollback_acknowledged


def test_unknown_probe_never_fabricates_absent_receipt():
    class State:
        def probe_receipt_fresh(self, *args, **kwargs):
            raise AssertionError("fault must make probe unavailable")

    faults = Faults()
    faults.arm("unknown_commit")
    delegate = ReceiptDelegate(State(), faults, None, None, "key")
    with pytest.raises(RuntimeError, match="probe_unavailable"):
        delegate.probe_receipt_fresh("operation")
    assert faults.probes == 1


def test_unknown_cleanup_refuses_before_opening_any_connection():
    from types import SimpleNamespace

    from tools.native_delivery_local.cleanup import cleanup

    with pytest.raises(RuntimeError, match="unknown_outcome_retained"):
        cleanup(SimpleNamespace(faults=SimpleNamespace(known=False)))


def test_source_guard_blocks_another_fixture_ddl_writer(tmp_path):
    from tools.native_delivery_local.source_guard import ddl_lock

    with ddl_lock(tmp_path, writer=False):
        with pytest.raises(RuntimeError, match="source_ddl_busy"):
            with ddl_lock(tmp_path, writer=True):
                pytest.fail("writer must not enter")
    with ddl_lock(tmp_path, writer=True):
        pass


def test_native_session_uses_native_integer_temporal_source():
    """Wiring prevents generic driver timestamps from bypassing native projection."""
    from tools.native_delivery_local import session

    from dpone.runtime.sources.clickhouse_native_source import ClickHouseNativeSource

    assert session.ClickHouseNativeSource is ClickHouseNativeSource
    assert not hasattr(session, "ClickHouseSource")


def test_old_rollback_proof_cannot_bless_later_unknown_commit():
    class Connector:
        def rollback(self):
            pass

        def commit_transaction(self):
            raise OSError("commit acknowledgement unavailable")

    faults = Faults()
    target = TargetDelegate(Connector(), faults)
    faults.begin_finalizer()
    target.rollback()
    assert faults.rollback_acknowledged
    faults.begin_finalizer()
    assert not faults.rollback_acknowledged
    with pytest.raises(OSError):
        target.commit_transaction()
    target.rollback()
    assert faults.commit_attempted
    assert not faults.rollback_acknowledged
    assert not faults.known


def test_cleanup_retries_ack_loss_but_rejects_foreign_replacement(tmp_path):
    import time

    from tools.native_delivery_local.cleanup_progress import drop_owned

    from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore

    store = SQLiteWindowStore(tmp_path / "cleanup.sqlite3", clock=time.time)
    lease = store.acquire("target", "cleanup", 60)
    current = ["owned-uuid"]

    def delete_lost_ack():
        current[0] = None
        raise OSError("lost drop acknowledgement")

    try:
        with pytest.raises(OSError):
            drop_owned(store, "source", "owned-uuid", lambda: current[0], delete_lost_ack, lease)
        drop_owned(store, "source", "owned-uuid", lambda: current[0], lambda: pytest.fail("already absent"), lease)
        current[0] = "foreign-uuid"
        with pytest.raises(ValueError, match="replaced"):
            drop_owned(store, "source", "owned-uuid", lambda: current[0], lambda: pytest.fail("foreign"), lease)
        with pytest.raises(ValueError, match="unproven_absence"):
            drop_owned(store, "unknown", "other", lambda: None, lambda: pytest.fail("absent"), lease)
    finally:
        store.release(lease)
