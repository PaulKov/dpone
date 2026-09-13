"""Cooperative DDL exclusion for exclusively owned disposable source tables.

All fixture DDL writers take the same exclusive lock. This is a local experiment
boundary, not protection from an independent ClickHouse administrator. UUID and
schema checks reject replacement/drift; they do not substitute for the lock.
"""

from __future__ import annotations

import fcntl
import os
from contextlib import contextmanager

from tools.native_delivery_live_support.profiles import Dataset


@contextmanager
def ddl_lock(directory, *, writer):
    descriptor = os.open(directory / "source-ddl.lock", os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    try:
        stat = os.fstat(descriptor)
        if stat.st_uid != os.getuid() or stat.st_mode & 0o077:
            raise ValueError("local_fixture.source_lock_permissions")
        try:
            fcntl.flock(descriptor, (fcntl.LOCK_EX if writer else fcntl.LOCK_SH) | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("local_fixture.source_ddl_busy") from None
        yield
    finally:
        os.close(descriptor)


def verify_source(connector, inventory, results):
    params = {"database": inventory.source_database, "table": inventory.schema}
    rows = connector.get_records(
        "SELECT toString(uuid) FROM system.tables WHERE database=%(database)s AND name=%(table)s", params
    )
    if [tuple(row) for row in rows] != [(results["source_uuid"],)]:
        raise ValueError("local_fixture.source_object_changed")
    columns = connector.get_records(
        "SELECT name,type FROM system.columns WHERE database=%(database)s AND table=%(table)s ORDER BY position", params
    )
    expected = [(c["name"], c["source"]) for c in Dataset(inventory.profile, inventory.rows, inventory.seed).schema()]
    actual = [(name, dtype.replace(" ", "")) for name, dtype in columns]
    if actual != [(name, dtype.replace(" ", "")) for name, dtype in expected]:
        raise ValueError("local_fixture.source_schema_changed")


@contextmanager
def source_guard(directory, connector, inventory, results):
    with ddl_lock(directory, writer=False):
        verify_source(connector, inventory, results)
        try:
            yield
        finally:
            verify_source(connector, inventory, results)
