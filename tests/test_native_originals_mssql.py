"""SQL binding faults preserve immutable identity and never repeat a write."""

from dataclasses import replace

import pytest

from dpone.adapters.native_originals_mssql import MssqlNativeOriginalBindings, NativeOriginalBindingError
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_originals import encode_native_original_binding
from tests.test_native_original_bindings import D, binding


class Database:
    def __init__(self):
        self.saved = None
        self.calls = []
        self.fail = None
        self.transform = lambda data: data

    def connect(self):
        return Connection(self)


class Connection:
    autocommit = True

    def __init__(self, database):
        self.database = database
        self.mutating = False

    def cursor(self):
        return Cursor(self)

    def commit(self):
        self.database.calls.append("commit")
        if self.mutating and self.database.fail == "commit":
            raise OSError("lost acknowledgement")

    def rollback(self):
        self.database.calls.append("rollback")

    def close(self):
        self.database.calls.append("close")


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.rows = []

    def execute(self, sql, *parameters):
        db = self.connection.database
        assert self.connection.autocommit is False
        self.connection.mutating = "native_original_bind_v1" in sql
        db.calls.append("bind" if self.connection.mutating else "resolve")
        if self.connection.mutating:
            if db.fail == "before":
                raise OSError("before dispatch")
            candidate = parameters[-1]
            if db.saved is not None and db.saved != candidate:
                raise OSError("immutable conflict")
            db.saved = candidate
            if db.fail == "execute":
                raise OSError("lost acknowledgement")
        if db.fail == "resolve" and not self.connection.mutating:
            raise OSError("read unavailable")
        self.rows = [] if db.saved is None else [(db.transform(db.saved),)]
        return self

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def close(self):
        pass


def adapter(db, **changes):
    return MssqlNativeOriginalBindings(
        **dict(
            connection_factory=db.connect,
            control_schema="dpone_control",
            control_authority=OriginalRef("control/authority", D),
            max_binding_bytes=1048576,
        )
        | changes
    )


def test_committed_binding_is_independently_resolved():
    db = Database()
    value = binding()
    assert adapter(db).bind(value) == OriginalRef(value.locator, value.payload_sha256)
    assert db.calls == ["bind", "commit", "close", "resolve", "commit", "close"]
    assert db.saved == encode_native_original_binding(value)


@pytest.mark.parametrize("failure", ["execute", "commit"])
def test_lost_ack_is_readonly_reconciled(failure):
    db = Database()
    db.fail = failure
    assert adapter(db).bind(binding()).locator == binding().locator
    assert db.calls.count("bind") == 1
    assert db.calls.count("resolve") == 1


@pytest.mark.parametrize("failure", ["before", "resolve"])
def test_unresolved_failure_never_resends(failure):
    db = Database()
    db.fail = failure
    with pytest.raises((NativeOriginalBindingError, OSError)):
        adapter(db).bind(binding())
    assert db.calls.count("bind") == 1


def test_conflict_cannot_reconcile_another_complete_tuple():
    db = Database()
    store = adapter(db)
    store.bind(binding())
    with pytest.raises(NativeOriginalBindingError):
        store.bind(replace(binding(), object_ref=replace(binding().object_ref, version="different")))
    assert db.saved == encode_native_original_binding(binding())


@pytest.mark.parametrize("transform", [lambda data: b" " + data, lambda data: data[:-1], lambda data: "not bytes"])
def test_malformed_sql_payload_is_not_success(transform):
    db = Database()
    db.transform = transform
    with pytest.raises((NativeOriginalBindingError, ValueError)):
        adapter(db).bind(binding())


@pytest.mark.parametrize(
    "changes",
    [
        dict(max_binding_bytes=True),
        dict(max_binding_bytes=0),
        dict(control_schema="x]; DROP"),
        dict(control_authority=None),
    ],
)
def test_constructor_rejects_invalid_policy_before_io(changes):
    db = Database()
    with pytest.raises((ValueError, TypeError)):
        adapter(db, **changes)
    assert not db.calls


def test_oversized_binding_has_no_sql_effect():
    db = Database()
    with pytest.raises(ValueError):
        adapter(db, max_binding_bytes=10).bind(binding())
    assert not db.calls
