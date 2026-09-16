"""Immutable storage boundary tests; synthetic values do not prove authority."""

from dataclasses import replace

import pytest

from dpone.adapters.dbt_mssql_physical_registration_store import (
    MssqlPhysicalRegistrationStore,
    PhysicalRegistrationStorageError,
    registration_columns,
)
from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from tests.support.dbt_mssql_physical_registration import registration_inputs


class Connection:
    """Owned transaction double with an independent committed record."""

    autocommit = False

    def __init__(self, rows, fault=False):
        self.rows = rows
        self.fault = fault
        self.closed = False
        self.statements = []
        self.result = []

    def cursor(self):
        return self

    def execute(self, sql, *parameters):
        self.statements.append((sql, parameters))
        if "SELECT registration_id," in sql:
            self.result = list(self.rows)
        return self

    def fetchone(self):
        return self.result.pop(0) if self.result else None

    def commit(self):
        if self.fault:
            raise OSError("lost acknowledgement")

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def registration():
    return MssqlPhysicalRuntimeRegistration(**registration_inputs())


def test_register_independently_reads_exact_complete_row_after_lost_ack():
    expected = registration()
    writer = Connection([], fault=True)
    reader = Connection([tuple(registration_columns(expected).values())])
    connections = iter([writer, reader])
    store = MssqlPhysicalRegistrationStore(connection_factory=lambda: next(connections), local_schema="runtime_local")
    assert store.register(expected) == expected
    assert writer.closed and reader.closed
    assert not any("ELSE INSERT [" in sql for sql, _ in reader.statements)
    assert sum("ELSE INSERT [" in sql for sql, _ in writer.statements) == 1


@pytest.mark.parametrize("damage", ["absent", "payload", "projection", "extra"])
def test_readback_mismatch_is_never_success(damage):
    expected = registration()
    row = list(registration_columns(expected).values())
    if damage == "payload":
        row[1] = b"{}"
    elif damage == "projection":
        row[-1] = b"wrong"
    rows = [] if damage == "absent" else [tuple(row)]
    if damage == "extra":
        rows *= 2
    connections = iter([Connection([]), Connection(rows)])
    store = MssqlPhysicalRegistrationStore(connection_factory=lambda: next(connections), local_schema="runtime_local")
    with pytest.raises(PhysicalRegistrationStorageError):
        store.register(expected)


def test_schema_mismatch_rejects_before_connect():
    def unexpected():
        pytest.fail("invalid input must not connect")

    store = MssqlPhysicalRegistrationStore(connection_factory=unexpected, local_schema="runtime_local")
    with pytest.raises(ValueError):
        store.register(replace(registration(), local_schema="other"))
