"""Opt-in real SQL v2 lifecycle; parent executes the isolated live environment."""

import importlib
import json
import os
from dataclasses import replace
from uuid import uuid4

import pytest

from dpone.adapters.dbt_mssql_physical_catalog import PhysicalCatalogReadError
from dpone.adapters.dbt_mssql_physical_catalog_binding_schema import CatalogBindingStorageError
from dpone.adapters.dbt_mssql_physical_source import MssqlPhysicalSourceReader, PhysicalSourceReadError
from tests.support.dbt_mssql_physical_catalog_v2_live import (
    ArchiveAuthority,
    CatalogV2Fixture,
    TimedSourceFixture,
    selected_profile,
)
from tests.support.dbt_mssql_physical_source_authority import LOCAL

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_PHYSICAL_CATALOG_V2_LIVE") != "1", reason="isolated v2 lifecycle disabled"
    ),
]


@pytest.fixture
def catalog_v2(tmp_path, monkeypatch, record_property, request):
    for name in ("DPONE_NATIVE_SQL_TEST_HOST", "DPONE_NATIVE_SQL_TEST_PASSWORD"):
        if not os.environ.get(name):
            pytest.skip("isolated SQL fixture input unavailable: " + name)
    profile = selected_profile()
    topology = getattr(request, "param", "two-database")
    assert topology in {"two-database", "same-database"}
    source = TimedSourceFixture(importlib.import_module("pyodbc"), profile, layout=topology.replace("-", "_"))
    source.authority = ArchiveAuthority(tmp_path / "first", monkeypatch, 1)
    try:
        source.install()
        fixture = CatalogV2Fixture(source, tmp_path, monkeypatch, profile)
        # Every negative first proves this exact source/module/binding usable.
        before = source.snapshot()
        assert fixture.read().results["COUNT"][0].row_count_exact == 3
        assert fixture.read(version=1).results["COUNT"][0].row_count_exact == 3
        assert source.snapshot() == before
        record_property("catalog_v2_environment", json.dumps(fixture.evidence(), sort_keys=True))
        yield fixture
    finally:
        source.cleanup()


@pytest.mark.parametrize("catalog_v2", ["two-database", "same-database"], indirect=True)
def test_two_registration_ids_reuse_exact_cohort(catalog_v2, record_property):
    fixture = catalog_v2
    before = fixture.inventory()
    native = fixture.source.snapshot()
    for role in ("metadata", "build"):
        facts = fixture.source.read(role)
        mapping = getattr(fixture.first.principals, role)
        assert facts.observed_model_principal == mapping.model
        assert facts.observed_control_principal == mapping.control
        if fixture.source.databases["model"] == fixture.source.databases["control"]:
            assert mapping.model == mapping.control
        assert fixture.read(role=role).results["COUNT"][0].row_count_exact == 3
    second = replace(fixture.first, registration_id=str(uuid4()))
    assert fixture.registrations.register(second) == second
    binding = fixture.apply(second)
    assert binding != fixture.first_binding
    assert fixture.apply(second) == binding
    for registration, expected in ((fixture.first, fixture.first_binding), (second, binding)):
        assert fixture.registrations.resolve(registration) == registration
        assert fixture.bindings.resolve(registration, expected) == expected
        assert fixture.read(registration).results["COUNT"][0].row_count_exact == 3
    assert fixture.inventory() == before
    assert fixture.source.snapshot() == native
    record_property("registration_ids", json.dumps([fixture.first.registration_id, second.registration_id]))
    record_property("second_binding", json.dumps(binding.to_dict(), sort_keys=True))


def test_changed_policy_uses_real_generation_succession(catalog_v2, record_property):
    fixture = catalog_v2
    before = fixture.inventory()
    old_executor = fixture.source.executor
    old_epoch = fixture.source.request.guard.fencing_epoch
    second, binding = fixture.successor()
    assert second.trusted_profile.reference != fixture.first.trusted_profile.reference
    assert second.platform_subject != fixture.first.platform_subject
    assert fixture.source.request.guard.fencing_epoch > old_epoch
    assert fixture.source.executor.generation_id != old_executor.generation_id
    assert fixture.read(second).results["COUNT"][0].row_count_exact == 3
    for registration, expected in ((fixture.first, fixture.first_binding), (second, binding)):
        assert fixture.registrations.resolve(registration) == registration
        assert fixture.bindings.resolve(registration, expected) == expected
    with pytest.raises(PhysicalSourceReadError):
        MssqlPhysicalSourceReader(
            connection_factory=lambda: fixture.source.connect("model", "metadata"),
            registration=fixture.first,
        ).read(str(old_executor.generation_id), str(old_executor.invocation_id))
    assert fixture.inventory() == before
    record_property("successor", json.dumps(fixture.evidence(), sort_keys=True))
    record_property("successor_binding", json.dumps(binding.to_dict(), sort_keys=True))
    record_property("retired_generation", str(old_executor.generation_id))


@pytest.mark.parametrize("damage", ["profile", "bounds", "archive-bytes"])
def test_actual_authentication_rejects_before_catalog_writes(catalog_v2, damage):
    fixture = catalog_v2
    before = fixture.inventory()
    registration = fixture.first
    if damage == "profile":
        registration = replace(
            registration,
            trusted_profile=replace(
                registration.trusted_profile,
                reference=registration.control_authority,
            ),
        )
    elif damage == "bounds":
        registration = replace(registration, limits=replace(registration.limits, max_catalog_rows=101))
    else:
        archive = fixture.source.authority.archive
        from dpone.contracts.dbt_runtime_release_binding import DbtReleaseArtifactIndex

        index = DbtReleaseArtifactIndex.from_release(
            json.loads((archive.release_root / archive.refs.release.locator).read_bytes())
        )
        owner = next(item for item in fixture.source.authority.sources.workflows if item.source.workflow_id == "alpha")
        path = archive.release_root / index.payloads[owner.source.runtime_payload_ids[0]]["path"]
        path.write_bytes(path.read_bytes() + b" ")
    with pytest.raises(ValueError):
        fixture.apply(registration)
    assert fixture.inventory() == before
    assert fixture.registrations.resolve(fixture.first) == fixture.first
    assert fixture.bindings.resolve(fixture.first, fixture.first_binding) == fixture.first_binding


@pytest.mark.parametrize("column", ["registration_digest", "payload", "binding_digest"])
def test_damaged_protected_binding_never_returns_match(catalog_v2, column):
    fixture = catalog_v2
    native = fixture.source.snapshot()
    with fixture.source.connection(autocommit=True) as admin:
        # Valid SQL width; fail semantic readback rather than a CHECK constraint.
        replacement = b"{}" if column == "payload" else b"sha256:" + b"0" * 64
        admin.execute(
            f"UPDATE [{LOCAL}].physical_catalog_bindings_v1 SET {column}=? WHERE registration_id=?",
            replacement,
            fixture.first.registration_id,
        )
    with pytest.raises(CatalogBindingStorageError):
        fixture.bindings.resolve(fixture.first, fixture.first_binding)
    with pytest.raises(PhysicalCatalogReadError):
        fixture.read()
    assert fixture.source.snapshot() == native


@pytest.mark.parametrize("role", ["metadata", "build", "observer"])
def test_runtime_cannot_read_or_change_binding_table(catalog_v2, role):
    fixture = catalog_v2
    driver_error = fixture.source.driver.Error
    for statement in (
        f"SELECT * FROM [{LOCAL}].physical_catalog_bindings_v1",
        f"DELETE FROM [{LOCAL}].physical_catalog_bindings_v1 WHERE registration_id=?",
    ):
        with fixture.source.connection("model", role) as runtime:
            with pytest.raises(driver_error):
                runtime.execute(statement, *([fixture.first.registration_id] if "?" in statement else []))
    assert fixture.bindings.resolve(fixture.first, fixture.first_binding) == fixture.first_binding


def test_conflicting_binding_uuid_replay_preserves_authenticated_original(catalog_v2):
    fixture = catalog_v2
    # Intentional unauthenticated conflicting storage input, not a policy proof.
    conflict = replace(fixture.first_binding, workflow_id="fixture-conflicting-workflow")
    with pytest.raises(CatalogBindingStorageError) as failure:
        fixture.bindings.register(fixture.first, conflict)
    assert "UUID conflicts with existing bytes" in str(failure.value.__cause__)
    assert fixture.bindings.resolve(fixture.first, fixture.first_binding) == fixture.first_binding
    assert fixture.read().results["COUNT"][0].row_count_exact == 3


def test_missing_binding_rejects_without_repair(catalog_v2):
    fixture = catalog_v2
    native, inventory = fixture.source.snapshot(), fixture.inventory()
    with fixture.source.connection(autocommit=True) as admin:
        admin.execute(
            f"DELETE FROM [{LOCAL}].physical_catalog_bindings_v1 WHERE registration_id=?",
            fixture.first.registration_id,
        )
    with pytest.raises(CatalogBindingStorageError, match="absent"):
        fixture.bindings.resolve(fixture.first, fixture.first_binding)
    with pytest.raises(PhysicalCatalogReadError):
        fixture.read()
    # V1 remains distinct and usable; neither failed observer recreates binding.
    assert fixture.read(version=1).results["COUNT"][0].row_count_exact == 3
    with fixture.source.connection() as admin:
        assert admin.execute(f"SELECT COUNT_BIG(*) FROM [{LOCAL}].physical_catalog_bindings_v1").fetchone()[0] == 0
    assert fixture.source.snapshot() == native
    assert fixture.inventory() == inventory


class LostAcknowledgementConnection:
    """Raise after one real commit; observe SQL without altering its semantics."""

    def __init__(self, actual, events, *, lose_ack=False):
        self.actual, self.events, self.lose_ack = actual, events, lose_ack

    @property
    def autocommit(self):
        return self.actual.autocommit

    @autocommit.setter
    def autocommit(self, value):
        self.actual.autocommit = value

    def cursor(self):
        actual = self.actual.cursor()
        events = self.events

        class Cursor:
            def execute(self, sql, *parameters):
                if sql.startswith(f"INSERT [{LOCAL}].[physical_catalog_bindings_v1]"):
                    events.append("binding-insert")
                actual.execute(sql, *parameters)
                return self

            def fetchone(self):
                return actual.fetchone()

            def close(self):
                actual.close()

        return Cursor()

    def commit(self):
        self.actual.commit()
        if self.lose_ack:
            self.events.append("actual-commit-ack-lost")
            raise OSError("synthetic acknowledgement loss after actual SQL commit")

    def rollback(self):
        self.actual.rollback()

    def close(self):
        self.actual.close()


def test_binding_lost_ack_reconciles_real_commit_without_mutation_retry(catalog_v2, record_property):
    from dpone.adapters.dbt_mssql_physical_catalog_binding_store import MssqlPhysicalCatalogBindingStore

    fixture = catalog_v2
    second = replace(fixture.first, registration_id=str(uuid4()))
    assert fixture.registrations.register(second) == second
    events: list[str] = []
    connections: list[object] = []

    def connect():
        connection = fixture.source.connect()
        connections.append(connection)
        if len(connections) == 1:
            return LostAcknowledgementConnection(connection, events, lose_ack=True)
        events.append("independent-real-readback")
        return LostAcknowledgementConnection(connection, events)

    store = MssqlPhysicalCatalogBindingStore(connection_factory=connect, local_schema=LOCAL)
    binding = fixture.apply(second, bindings=store)
    assert events == [
        "binding-insert",
        "actual-commit-ack-lost",
        "independent-real-readback",
        "independent-real-readback",
    ]
    assert len(connections) == 3 and len({id(connection) for connection in connections}) == 3
    assert fixture.bindings.resolve(second, binding) == binding
    assert fixture.read(second).results["COUNT"][0].row_count_exact == 3
    record_property("binding_ack_fault", json.dumps(events))


def test_concurrent_exact_and_conflicting_replay_preserves_original(catalog_v2):
    from concurrent.futures import ThreadPoolExecutor
    from threading import Barrier

    from tests.support.dbt_mssql_physical_catalog_v2_live import PROFILES

    fixture = catalog_v2
    timeout = PROFILES[fixture.profile]["setup"] * 4
    barrier = Barrier(2, timeout=timeout)
    conflict = replace(fixture.first_binding, workflow_id="fixture-racing-conflict")
    native, inventory = fixture.source.snapshot(), fixture.inventory()

    def attempt(binding):
        barrier.wait()
        try:
            return fixture.bindings.register(fixture.first, binding)
        except CatalogBindingStorageError as failure:
            assert binding == conflict
            assert "UUID conflicts with existing bytes" in str(failure.__cause__)
            return "conflict-rejected"

    with ThreadPoolExecutor(max_workers=2) as pool:
        exact = pool.submit(attempt, fixture.first_binding)
        wrong = pool.submit(attempt, conflict)
        assert exact.result(timeout=timeout) == fixture.first_binding
        assert wrong.result(timeout=timeout) == "conflict-rejected"
    assert fixture.bindings.resolve(fixture.first, fixture.first_binding) == fixture.first_binding
    assert fixture.read().results["COUNT"][0].row_count_exact == 3
    assert fixture.source.snapshot() == native
    assert fixture.inventory() == inventory
