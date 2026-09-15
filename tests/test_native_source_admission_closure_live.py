"""Isolated durable admission closure, not full writer or delivery certification."""

from __future__ import annotations

import os

import pytest

from dpone.adapters.native_generation_mssql import NativeGenerationAdmissionError, NativeWriterAdmissionUncertain
from dpone.adapters.native_generation_mssql_queries import generation_procedure, generation_procedure_name
from dpone.adapters.native_generation_mssql_schema import MssqlNativeGenerationSchemaMigration
from dpone.adapters.native_generation_mssql_upgrade import GENERATION_ADMISSION_CHECK
from dpone.contracts.native_identity import OriginalRef
from tests.test_native_generation_admission_live import generations as generations
from tests.test_native_generation_admission_live import writer
from tests.test_native_original_bindings import D
from tests.test_native_originals_mssql_live import CommitFault
from tests.test_native_originals_mssql_live import ledger as ledger

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_NATIVE_ORIGINAL_MSSQL_LIVE") != "1", reason="isolated SQL acceptance disabled"
    ),
]


def bound(generations):
    request, provider, _, _, _, _, _ = generations
    value = request()
    reservation = provider().reserve(value)
    executor = writer(value)
    snapshot = provider().bind_writer_invocation(reservation, executor, expected_revision=reservation.revision)
    return value, reservation, executor, snapshot


def charged(admin):
    connection = admin()
    try:
        return connection.execute("SELECT charged_bytes FROM dpone_control.native_generation_capacity_v1").fetchone()[0]
    finally:
        connection.close()


@pytest.mark.parametrize("outcome", ["ACTIVE", "UNKNOWN"])
def test_live_close_keeps_phase_outcome_charge_and_exact_replay(generations, outcome):
    _, provider, _, admin, migration, _, _ = generations
    value, reservation, executor, snapshot = bound(generations)
    if outcome == "UNKNOWN":
        # Seed an existing same-owner outcome. Outcome evidence/recording is a
        # separate protocol; this tests preservation, not its authentication.
        connection = admin()
        try:
            connection.execute("UPDATE dpone_control.native_generations_v1 SET outcome='UNKNOWN'")
            connection.commit()
        finally:
            connection.close()
    receipt = provider().close_writer_admission(reservation, expected_revision=snapshot.revision)
    assert receipt.executor == executor
    assert receipt.admission_sequence == 1
    assert receipt.revision == snapshot.revision + 1
    assert provider().read_admission_closure(value.subject.generation_id) == receipt
    assert provider().close_writer_admission(reservation, expected_revision=snapshot.revision) == receipt
    observed = provider().read_custody(value.subject.generation_id)
    assert (observed.state, observed.writer_admission, observed.outcome) == ("BUILDING", "CLOSED", outcome)
    assert observed.closure is None
    assert charged(admin) == 60
    with pytest.raises(NativeWriterAdmissionUncertain):
        provider().bind_writer_invocation(reservation, executor, expected_revision=observed.revision)
    migration.apply()
    assert provider().read_custody(value.subject.generation_id) == observed


@pytest.mark.parametrize("committed", [True, False])
def test_live_close_lost_ack_reads_exact_persisted_outcome_without_second_mutation(generations, committed):
    _, provider, runtime, admin, _, _, _ = generations
    value, reservation, _, snapshot = bound(generations)
    calls = []

    def factory():
        calls.append(1)
        connection = runtime()
        # First connection reads custody; the second performs the sole CAS.
        return CommitFault(connection, committed) if len(calls) == 2 else connection

    if committed:
        receipt = provider(factory).close_writer_admission(reservation, expected_revision=snapshot.revision)
        assert receipt.revision == 3
    else:
        with pytest.raises(NativeGenerationAdmissionError):
            provider(factory).close_writer_admission(reservation, expected_revision=snapshot.revision)
    assert len(calls) == 3
    observed = provider().read_custody(value.subject.generation_id)
    assert observed.writer_admission == ("CLOSED" if committed else "OPEN")
    assert observed.revision == (3 if committed else 2)
    assert observed.closure is None
    assert charged(admin) == 60


def test_live_changed_physical_epoch_cannot_close_admission(generations):
    _, provider, _, admin, _, _, _ = generations
    value, reservation, _, snapshot = bound(generations)
    connection = admin()
    try:
        connection.execute("UPDATE dpone_control.semantic_refresh_guards SET fencing_epoch=fencing_epoch+1")
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(NativeGenerationAdmissionError):
        provider().close_writer_admission(reservation, expected_revision=snapshot.revision)
    assert provider().read_custody(value.subject.generation_id).writer_admission == "OPEN"
    assert charged(admin) == 60


def test_live_regrouped_constraint_is_not_a_recognized_schema(generations):
    _, _, _, admin, migration, _, _ = generations
    bound(generations)
    weaker = GENERATION_ADMISSION_CHECK.replace("AND ((executor IS NULL", "AND (executor IS NULL", 1)[:-1]
    assert weaker.replace("(", "").replace(")", "") == GENERATION_ADMISSION_CHECK.replace("(", "").replace(")", "")
    connection = admin()
    try:
        connection.execute(
            "ALTER TABLE dpone_control.native_generations_v1 DROP CONSTRAINT CK_native_generation_admission_v1"
        )
        connection.execute("ALTER TABLE dpone_control.native_generations_v1 ADD CHECK (" + weaker + ")")
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(Exception, match="EXISTING_SCHEMA_MISMATCH"):
        migration.apply()


def retained_row(admin):
    connection = admin()
    try:
        return tuple(
            connection.execute(
                "SELECT generation_id,guard_hash,guard_epoch,revision,authority_locator,authority_digest,"
                "reservation_locator,reservation_digest,request,executor FROM dpone_control.native_generations_v1"
            ).fetchone()
        )
    finally:
        connection.close()


def runtime_principal(runtime):
    connection = runtime()
    try:
        return connection.execute("SELECT USER_NAME()").fetchone()[0]
    finally:
        connection.close()


def prepare_retained_initial_layout(admin, principal):
    """Reconstruct only this disposable database's known initial layout.

    Production migration never performs this downgrade. The fixture preserves
    its populated rows so upgrade assertions compare actual retained bytes.
    """
    connection = admin()
    try:
        connection.execute(
            "ALTER TABLE dpone_control.native_generations_v1 DROP CONSTRAINT CK_native_generation_admission_v1"
        )
        connection.execute(
            "ALTER TABLE dpone_control.native_generations_v1 DROP COLUMN "
            "writer_admission,outcome,admission_sequence,admission_closure"
        )
        connection.execute(
            "ALTER TABLE dpone_control.native_generations_v1 ADD CHECK "
            "(guard_epoch>0 AND revision=CASE WHEN executor IS NULL THEN 1 ELSE 2 END)"
        )
        for operation in ("reserve", "bind", "read", "close", "closure_read"):
            name = "[dpone_control].[" + generation_procedure_name(operation) + "]"
            connection.execute("DROP PROCEDURE " + name)
            if operation in {"reserve", "bind", "read"}:
                connection.execute(generation_procedure("dpone_control", operation, extended=False))
                connection.execute("GRANT EXECUTE ON OBJECT::" + name + " TO [" + principal.replace("]", "]]") + "]")
        connection.commit()
    finally:
        connection.close()


def test_live_populated_initial_layout_upgrade_preserves_identity_charge_and_closes(generations):
    _, provider, runtime, admin, migration, _, _ = generations
    value, reservation, executor, snapshot = bound(generations)
    before = retained_row(admin)
    prepare_retained_initial_layout(admin, runtime_principal(runtime))
    migration.apply()
    assert retained_row(admin) == before
    assert charged(admin) == 60
    assert provider().read_custody(value.subject.generation_id) == snapshot
    receipt = provider().close_writer_admission(reservation, expected_revision=snapshot.revision)
    assert receipt.executor == executor
    migration.apply()
    assert provider().read_admission_closure(value.subject.generation_id) == receipt
    assert charged(admin) == 60


@pytest.mark.parametrize("privilege", ["db_owner", "schema_alter"])
def test_live_upgrade_rejects_privileged_runtime_before_retained_ddl(generations, privilege):
    _, _, runtime, admin, migration, _, _ = generations
    bound(generations)
    principal = runtime_principal(runtime)
    prepare_retained_initial_layout(admin, principal)
    before = retained_row(admin)
    quoted = "[" + principal.replace("]", "]]") + "]"
    connection = admin()
    try:
        # Remove the explicit deny in this disposable fixture so effective
        # inherited privilege, rather than a harmless masked grant, is tested.
        connection.execute("REVOKE ALTER ON SCHEMA::dpone_control FROM " + quoted)
        connection.execute(
            "ALTER ROLE db_owner ADD MEMBER " + quoted
            if privilege == "db_owner"
            else "GRANT ALTER ON SCHEMA::dpone_control TO " + quoted
        )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(Exception, match="RUNTIME_PRINCIPAL_INVALID"):
        migration.apply()
    assert retained_row(admin) == before
    assert charged(admin) == 60
    connection = admin()
    try:
        assert (
            connection.execute(
                "SELECT COL_LENGTH('dpone_control.native_generations_v1','writer_admission')"
            ).fetchone()[0]
            is None
        )
        for operation in ("reserve", "bind", "read"):
            assert connection.execute(
                "SELECT OBJECT_DEFINITION(OBJECT_ID(?))", "dpone_control." + generation_procedure_name(operation)
            ).fetchone()[0] == generation_procedure("dpone_control", operation, extended=False)
    finally:
        connection.close()


class UpgradeFaultCursor:
    def __init__(self, cursor, events):
        self.cursor, self.events = cursor, events

    def execute(self, sql, *parameters):
        self.cursor.execute(sql, *parameters)
        if sql.startswith("ALTER PROCEDURE"):
            self.events.append("procedure_altered")
            raise RuntimeError("synthetic failure after first procedure ALTER")
        return self

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def close(self):
        self.cursor.close()


class UpgradeFaultConnection(CommitFault):
    def __init__(self, connection, events):
        super().__init__(connection, False)
        self.events = events

    def cursor(self):
        return UpgradeFaultCursor(self.connection.cursor(), self.events)

    def commit(self):
        self.connection.commit()

    def rollback(self):
        self.events.append("rollback")
        self.connection.rollback()


def test_live_mid_upgrade_failure_rolls_back_columns_procedures_rows_and_charge(generations):
    _, _, runtime, admin, migration, _, _ = generations
    value, _, _, _ = bound(generations)
    principal = runtime_principal(runtime)
    prepare_retained_initial_layout(admin, principal)
    before = retained_row(admin)
    events = []
    fault = MssqlNativeGenerationSchemaMigration(
        connection_factory=lambda: UpgradeFaultConnection(admin(), events),
        control_schema="dpone_control",
        control_authority=OriginalRef("control/authority", D),
        runtime_database_principal=principal,
        physical_guard=value.guard.guard_id,
        resource_authority=OriginalRef("resource/authority", D),
        capacity_bytes=100,
        trusted_profile=value.profile,
        max_generation_bytes=100,
    )
    with pytest.raises(RuntimeError, match="synthetic failure"):
        fault.apply()
    assert events == ["procedure_altered", "rollback"]
    assert retained_row(admin) == before
    assert charged(admin) == 60
    connection = admin()
    try:
        assert (
            connection.execute(
                "SELECT COL_LENGTH('dpone_control.native_generations_v1','writer_admission')"
            ).fetchone()[0]
            is None
        )
        for operation in ("reserve", "bind", "read"):
            definition = connection.execute(
                "SELECT OBJECT_DEFINITION(OBJECT_ID(?))", "dpone_control." + generation_procedure_name(operation)
            ).fetchone()[0]
            assert definition == generation_procedure("dpone_control", operation, extended=False)
    finally:
        connection.close()
    migration.apply()
    assert retained_row(admin) == before
