"""Pinned activation lookup cannot substitute a newer or foreign occurrence."""

from dataclasses import replace

import pytest

from dpone.adapters.native_activation_lookup import MssqlNativeActivationLookup
from dpone.contracts.airflow_run_identity import AirflowDeploymentIdentity
from tests.test_native_original_subjects import authority

D = "sha256:" + "a" * 64
UUID = "11111111-1111-4111-8111-111111111111"


class Connection:
    autocommit = False

    def __init__(self, rows):
        self.rows = iter(rows)
        self.calls = []
        self.closed = 0
        self.rollbacks = 0

    def cursor(self):
        return self

    def execute(self, sql, *parameters):
        self.calls.append((sql, parameters))
        return self

    def fetchone(self):
        return next(self.rows, None)

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closed += 1

    def commit(self):
        pytest.fail("read-only lookup cannot commit")


def coordinates():
    context = authority()
    return AirflowDeploymentIdentity(context.release_id, context.deployment_id, UUID), context


def record(previous=D):
    identity, context = coordinates()
    return (
        UUID,
        D,
        context.environment,
        identity.release_id,
        identity.deployment_id,
        previous,
        D,
        context.authority_subject_sha256,
        "ACTIVE",
    )


def lookup(connection):
    identity, context = coordinates()
    return MssqlNativeActivationLookup(lambda: connection, control_schema="dpone_control").previous_deployment(
        identity=identity, authority=context, source_inventory_sha256=D
    )


@pytest.mark.parametrize("previous", [None, D])
def test_exact_pinned_row_preserves_actual_previous_and_owns_read_cleanup(previous):
    connection = Connection([record(previous)])
    assert lookup(connection) == previous
    assert connection.closed == 2 and connection.rollbacks == 1
    sql, parameters = connection.calls[0]
    assert parameters == (UUID,)
    assert sql.startswith("SELECT TOP (2)") and "WHERE activation_id = ?" in sql
    assert "current" not in sql.lower()


@pytest.mark.parametrize(
    "column,value",
    [
        (0, "22222222-2222-4222-8222-222222222222"),
        (1, "bad"),
        (2, "other"),
        (3, "sha256:" + "b" * 64),
        (4, "sha256:" + "c" * 64),
        (5, "bad"),
        (6, "sha256:" + "d" * 64),
        (7, "sha256:" + "e" * 64),
        (8, "PREPARED"),
        (8, "RETIRING"),
        (8, "RETIRED"),
    ],
)
def test_foreign_or_inactive_row_rejects(column, value):
    values = list(record())
    values[column] = value
    connection = Connection([tuple(values)])
    with pytest.raises(ValueError):
        lookup(connection)
    assert connection.closed == 2 and connection.rollbacks == 1


@pytest.mark.parametrize("rows", [[], [record(), record()], [(UUID,)]])
def test_missing_duplicate_or_partial_row_never_defaults_previous(rows):
    with pytest.raises(ValueError):
        lookup(Connection(rows))


def test_mismatched_pinned_identity_rejects_before_connection():
    identity, context = coordinates()

    def connect():
        pytest.fail("invalid local coordinates must reject before SQL")

    reader = MssqlNativeActivationLookup(connect, control_schema="dpone_control")
    with pytest.raises(ValueError):
        reader.previous_deployment(
            identity=replace(identity, release_id="sha256:" + "f" * 64), authority=context, source_inventory_sha256=D
        )
