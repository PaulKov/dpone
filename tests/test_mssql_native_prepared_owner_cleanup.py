"""Prepared cleanup proves exact ownership and authoritative absence."""

from contextlib import contextmanager

import pytest

from dpone.runtime.sinks.mssql_native_prepared_owner import retire_exact_prepared


class Connector:
    def __init__(self, object_id=11, *, user="dbo", sysadmin=0):
        self.object_id = object_id
        self.user = user
        self.sysadmin = sysadmin
        self.binding = "owned"
        self.calls = []

    @contextmanager
    def bounded_query_timeout(self, seconds):
        self.calls.append(("timeout", seconds))
        yield

    def begin(self):
        self.calls.append("begin")

    def commit_transaction(self):
        self.calls.append("commit")

    def rollback(self):
        self.calls.append("rollback")

    @staticmethod
    def quote_identifier(value):
        return f"[{value}]"

    @staticmethod
    def qualified_name(schema, table, *, database=None):
        return f"[{database}].[{schema}].[{table}]"

    def get_records(self, sql, params=()):
        if sql == "SELECT OBJECT_ID(?)":
            return [(self.object_id,)]
        if sql == "SELECT USER_NAME(), IS_SRVROLEMEMBER('sysadmin')":
            return [(self.user, self.sysadmin)]
        if "TABLOCKX, HOLDLOCK" in sql:
            self.calls.append("lock")
            return []
        if "extended_properties" in sql:
            return [(self.binding,)]
        raise AssertionError(sql)


PLANNED = {"database": "db", "schema": "stage", "table": "prepared", "binding": "owned"}


def test_metadata_filtered_null_cannot_release_prepared_ownership():
    connector = Connector(None, user="limited")

    with pytest.raises(ValueError, match="prepared_absence_visibility_unproved"):
        retire_exact_prepared(connector, PLANNED, 11, lambda: pytest.fail("nothing may be dropped"), timeout_seconds=3)

    assert connector.calls == [("timeout", 3), "begin", "rollback"]


def test_cleanup_requires_post_drop_absence_with_authoritative_visibility():
    connector = Connector()

    def drop_without_effect():
        pass

    with pytest.raises(ValueError, match="prepared_retirement_unproved"):
        retire_exact_prepared(connector, PLANNED, 11, drop_without_effect, timeout_seconds=3)

    assert connector.calls[-1] == "rollback"


def test_v1_compatible_visible_owned_object_can_be_retired_by_least_privilege_principal():
    connector = Connector(user="limited")

    def drop():
        connector.object_id = None

    retire_exact_prepared(connector, PLANNED, 11, drop, timeout_seconds=3)

    assert connector.calls[-1] == "commit"
    assert "rollback" not in connector.calls


def test_cleanup_replays_after_crash_following_drop():
    connector = Connector()

    def crash_after_drop():
        connector.object_id = None
        raise RuntimeError("crash after drop")

    with pytest.raises(RuntimeError, match="crash after drop"):
        retire_exact_prepared(connector, PLANNED, 11, crash_after_drop, timeout_seconds=3)

    retire_exact_prepared(connector, PLANNED, 11, lambda: pytest.fail("already absent"), timeout_seconds=3)
    assert connector.calls[-1] == "commit"


def test_cleanup_rejects_replacement_object_before_drop():
    connector = Connector(12)

    with pytest.raises(ValueError, match="prepared_object_identity_changed"):
        retire_exact_prepared(
            connector,
            PLANNED,
            11,
            lambda: pytest.fail("replacement must survive"),
            timeout_seconds=3,
        )

    assert connector.calls[-1] == "rollback"
