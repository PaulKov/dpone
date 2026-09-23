from dataclasses import replace
from pathlib import Path
from time import monotonic

import pytest

from dpone.adapters.mssql_tds_actor_core import TdsActorPool
from dpone.app.mssql_sqlclient_restricted_writer_verify_composition import (
    run_mssql_sqlclient_restricted_writer_verify,
)
from dpone.app.mssql_sqlclient_restricted_writer_verify_request import RestrictedWriterCredentialSupplier
from dpone.contracts.mssql_tds_connection import TdsConnectionMaterial
from dpone.ports.mssql_tds_worker import TdsLaunchUnknown
from dpone.services.mssql_tds_restricted_writer_verification import RestrictedWriterVerifyLocalUnknown
from tests.test_mssql_sqlclient_restricted_writer_verify_request import launch_request
from tests.test_mssql_tds_restricted_writer_verification import Association, Coordinator, Evidence, Launcher


def test_relative_evidence_root_is_rejected_before_opening_pool():
    with pytest.raises(ValueError, match="composition_invalid"):
        run_mssql_sqlclient_restricted_writer_verify(
            object(),
            object(),
            object(),
            launch_request(),
            object(),
            Path("relative"),
            deadline=300.0,
            coordinator_factory=lambda: object(),
        )


def test_concrete_composition_durably_writes_six_flat_evidence_files(tmp_path):
    deadline = monotonic() + 5.0
    launch = replace(launch_request(), startup_deadline=deadline - 1.0, operation_deadline=deadline)
    pool = TdsActorPool(capacity=1)
    supplier = RestrictedWriterCredentialSupplier(
        lambda: (TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret-value"), b"n" * 32)
    )
    association, events = Association(), []
    try:
        retained = run_mssql_sqlclient_restricted_writer_verify(
            pool,
            association,
            Launcher(events),
            launch,
            supplier,
            tmp_path,
            deadline=deadline,
            coordinator_factory=lambda: Coordinator(association, events),
        )
        assert pool.live_count == 0
        names = sorted(path.name for path in tmp_path.iterdir())
        assert names == sorted(receipt.relative_name for receipt in retained.receipts)
        assert len(names) == 6 and all("/" not in name for name in names)
    finally:
        pool.close(deadline=monotonic() + 2.0)


def test_concrete_composition_closes_exact_actor_after_unknown(tmp_path):
    deadline = monotonic() + 5.0
    launch = replace(launch_request(), startup_deadline=deadline - 1.0, operation_deadline=deadline)
    pool = TdsActorPool(capacity=1)
    supplier = RestrictedWriterCredentialSupplier(
        lambda: (TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret-value"), b"n" * 32)
    )
    association, events = Association(), []
    try:
        with pytest.raises(RestrictedWriterVerifyLocalUnknown, match="restricted_writer_verify_unknown"):
            run_mssql_sqlclient_restricted_writer_verify(
                pool,
                association,
                Launcher(events, result_fault=True),
                launch,
                supplier,
                tmp_path,
                deadline=deadline,
                coordinator_factory=lambda: Coordinator(association, events),
            )
        assert pool.live_count == 0
    finally:
        pool.close(deadline=monotonic() + 2.0)


def test_composition_contains_exact_unknown_launch_once_before_returning_unknown(tmp_path):
    class Unresolved:
        def __init__(self):
            self.contain_count = self.close_count = 0

        def contain(self, *, deadline):
            assert deadline > monotonic()
            self.contain_count += 1

        def close(self):
            self.close_count += 1

    unresolved = Unresolved()

    class UnknownLauncher:
        def launch(self, inputs, reservation, *, public_payload):
            del inputs, reservation, public_payload
            raise TdsLaunchUnknown(unresolved)

    deadline = monotonic() + 5.0
    launch = replace(launch_request(), startup_deadline=deadline - 1.0, operation_deadline=deadline)
    pool = TdsActorPool(capacity=1)
    supplier = RestrictedWriterCredentialSupplier(
        lambda: (TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret-value"), b"n" * 32)
    )
    association, events = Association(), []
    try:
        with pytest.raises(RestrictedWriterVerifyLocalUnknown):
            run_mssql_sqlclient_restricted_writer_verify(
                pool,
                association,
                UnknownLauncher(),
                launch,
                supplier,
                tmp_path,
                deadline=deadline,
                coordinator_factory=lambda: Coordinator(association, events),
            )
        assert unresolved.contain_count == unresolved.close_count == 1
        assert pool.live_count == 0
    finally:
        pool.close(deadline=monotonic() + 2.0)


def test_ambiguous_exact_actor_close_is_attempted_once_and_fails_closed(tmp_path):
    class Actor(Evidence):
        def __init__(self):
            super().__init__()
            self.close_count = 0

        def close(self, *, deadline):
            self.close_count += 1
            raise RuntimeError("ambiguous exact actor close")

    actor = Actor()
    pool = type("Pool", (), {"open": lambda self, build, deadline: actor})()
    launch = launch_request()
    supplier = RestrictedWriterCredentialSupplier(
        lambda: (TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret-value"), b"n" * 32)
    )
    association, events = Association(), []
    with pytest.raises(RuntimeError, match="ambiguous exact actor close"):
        run_mssql_sqlclient_restricted_writer_verify(
            pool,
            association,
            Launcher(events),
            launch,
            supplier,
            tmp_path,
            deadline=300.0,
            coordinator_factory=lambda: Coordinator(association, events),
        )
    assert actor.close_count == 1


def test_preexisting_identical_evidence_is_sticky_unknown_without_reread_healing(tmp_path):
    def run_once():
        deadline = monotonic() + 5.0
        launch = replace(launch_request(), startup_deadline=deadline - 1.0, operation_deadline=deadline)
        pool = TdsActorPool(capacity=1)
        supplier = RestrictedWriterCredentialSupplier(
            lambda: (TdsConnectionMaterial("localhost", 1433, "db", "writer", "secret-value"), b"n" * 32)
        )
        association, events = Association(), []
        try:
            return run_mssql_sqlclient_restricted_writer_verify(
                pool,
                association,
                Launcher(events),
                launch,
                supplier,
                tmp_path,
                deadline=deadline,
                coordinator_factory=lambda: Coordinator(association, events),
            )
        finally:
            pool.close(deadline=monotonic() + 2.0)

    run_once()
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir()}
    with pytest.raises(RestrictedWriterVerifyLocalUnknown):
        run_once()
    assert {path.name: path.read_bytes() for path in tmp_path.iterdir()} == before
