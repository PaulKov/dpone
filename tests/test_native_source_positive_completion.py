"""Positive completion shape is not authentication or permission to freeze."""

import json
from dataclasses import fields, replace
from hashlib import sha256

import pytest

from dpone.contracts.native_delivery import FrozenGeneration, NativeGenerationContractError, SourceClosureReceipt
from dpone.contracts.native_delivery_codec import (
    decode_frozen_generation,
    decode_source_closure_receipt,
    encode_frozen_generation,
    encode_source_closure_receipt,
)
from dpone.contracts.native_delivery_json import decode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import (
    NativeSourceCustodyError,
    SourceCustodySnapshot,
    SourceTrustedBuildCompletion,
    require_trusted_build_completion,
)
from dpone.contracts.native_source_custody_codec import (
    decode_source_trusted_build_completion,
    encode_source_trusted_build_completion,
)
from tests.test_native_source_admission_closure import closure


@pytest.mark.parametrize("read_only", [False, True])
def test_freeze_sql_binds_full_request_and_preserves_lock_order(read_only):
    from dpone.adapters.native_generation_mssql_freeze import freeze_body

    sql = freeze_body(
        "native_control",
        read_only=read_only,
        current_owner_sql="-- current physical owner",
        current_snapshot_sql="-- retained snapshot",
    )
    assert sql.index("-- current physical owner") < sql.index("WITH (UPDLOCK,HOLDLOCK)")
    for field in (
        "admission_closure",
        "completion_payload",
        "completion_locator",
        "completion_digest",
        "frozen_payload",
        "frozen_locator",
        "frozen_digest",
    ):
        assert f"DATALENGTH({field})" in sql
    assert "revision=@expected_revision+1" in sql
    assert "phase='FROZEN'" in sql
    assert "outcome='ACTIVE'" in sql
    assert "HASHBYTES('SHA2_256',@frozen)" in sql
    assert "HASHBYTES('SHA2_256',@closure_bytes)" in sql
    assert "JSON_VALUE" not in sql
    assert "@receipt_digest IS NULL OR DATALENGTH(@receipt_digest)<>71" in sql
    assert "@receipt_locator IS NULL OR DATALENGTH(@receipt_locator) NOT BETWEEN 1 AND 4096" in sql
    assert "DPONE_NATIVE_SOURCE_FREEZE_READBACK_MISMATCH" in sql
    assert "DELETE " not in sql and "INSERT INTO [native_control]" not in sql
    if read_only:
        assert "UPDATE " not in sql
    else:
        assert "UPDATE [native_control].[native_generations_v1]" in sql
        assert "outcome IN ('ACTIVE','UNKNOWN')" in sql


@pytest.mark.parametrize("operation", ["freeze", "freeze_read"])
def test_freeze_procedure_requires_completion_layout_and_protected_envelope(operation):
    from dpone.adapters.native_generation_mssql_queries import generation_procedure, generation_procedure_name

    with pytest.raises(ValueError):
        generation_procedure("native_control", operation)
    sql = generation_procedure("native_control", operation, completed=True)
    assert f"CREATE PROCEDURE [native_control].[{generation_procedure_name(operation)}]" in sql
    assert "@completion_locator varbinary(max), @completion_digest varbinary(max), @frozen varbinary(max)" in sql
    assert "DPONE_NATIVE_GENERATION_RUNTIME_PRINCIPAL_INVALID" in sql
    assert "DPONE_NATIVE_SOURCE_FREEZE_READBACK_MISMATCH" in sql
    assert "frozen_payload, frozen_locator, frozen_digest" in sql


def test_freeze_inspection_proves_exact_pending_or_accepted_request_without_mutation():
    from dpone.adapters.native_generation_mssql_freeze import freeze_body

    sql = freeze_body(
        "native_control",
        read_only=True,
        allow_building=True,
        current_owner_sql="-- current physical owner",
        current_snapshot_sql="-- snapshot",
    )
    assert "UPDATE " not in sql
    assert "revision=@expected_revision" in sql and "revision=@expected_revision+1" in sql
    assert "phase='BUILDING'" in sql and "phase='FROZEN'" in sql
    assert "frozen_payload IS NULL AND frozen_locator IS NULL AND frozen_digest IS NULL" in sql
    assert "completion_payload=@completion" in sql
    assert "DPONE_NATIVE_SOURCE_FREEZE_READBACK_MISMATCH" in sql
    with pytest.raises(ValueError):
        freeze_body(
            "native_control", read_only=False, allow_building=True, current_owner_sql="", current_snapshot_sql=""
        )


class FreezeFixture:
    """Synthetic ports for runtime ordering, never original provenance evidence."""

    def __init__(self, *, outcome="ACTIVE", commit_fault=None):
        from dpone.contracts.native_delivery import GenerationReservation
        from dpone.runtime.native_generation_freeze import NativeGenerationFreeze

        self.positive = completion()
        payload = encode_source_trusted_build_completion(self.positive)
        self.reference = OriginalRef("synthetic/completion", "sha256:" + sha256(payload).hexdigest())
        self.current = replace(snapshot(), revision=4, closure=self.reference, outcome=outcome)
        self.reservation = GenerationReservation(self.current.generation_id, 1, 2, self.current.reservation)
        self.objects = {}
        self.publications = self.mutations = self.current_reads = self.authentications = self.inspections = 0
        self.commit_fault = commit_fault
        self.reject_auth = self.reject_read = False
        self.max_bytes = 1048576
        self.runtime = NativeGenerationFreeze(
            executor=self.positive.executor,
            ledger=self,
            completion_reader=self,
            original_reader=self,
            publish_original=self.publish,
        )

    def require_generation(self, executor):
        assert executor == self.positive.executor

    def read_custody(self, generation_id):
        assert generation_id == self.current.generation_id
        return self.current

    def read_admission_closure(self, generation_id):
        return closure()

    def read_completion(self, reference):
        self.authentications += 1
        assert reference == self.reference
        if self.reject_auth:
            raise ValueError("synthetic missing positive proof")
        return self.positive

    def publish(self, *, kind, locator, payload, max_bytes):
        self.publications += 1
        assert len(payload) <= max_bytes
        reference = OriginalRef(locator, "sha256:" + sha256(payload).hexdigest())
        self.objects[(reference, kind)] = payload
        return reference

    def read(self, reference, kind):
        if self.reject_read:
            raise ValueError("synthetic unreadable original")
        return self.objects[(reference, kind)]

    def inspect_freeze(self, reservation, admission, completion, frozen, *, expected_revision):
        self.inspections += 1
        assert reservation == self.reservation and admission == closure() and completion == self.positive
        assert expected_revision == 4 and frozen.closure.completion == self.reference
        return self.current

    def record_freeze(self, reservation, admission, completion, frozen, *, expected_revision):
        assert reservation == self.reservation and admission == closure() and completion == self.positive
        assert expected_revision == 4 and frozen.closure.completion == self.reference
        self.mutations += 1
        if self.commit_fault != "before":
            self.current = replace(self.current, revision=5, state="FROZEN", outcome="ACTIVE", frozen=frozen.frozen)
        if self.commit_fault:
            raise OSError("synthetic lost acknowledgement")
        return self.current

    def read_freeze(self, reservation, admission, completion, frozen, *, expected_revision):
        self.current_reads += 1
        assert reservation == self.reservation and admission == closure() and completion == self.positive
        if self.current.frozen != frozen.frozen or self.current.revision != expected_revision + 1:
            raise ValueError("synthetic no accepted freeze")
        return self.current

    def freeze(self):
        return self.runtime.freeze(self.reservation, expected_revision=4)


@pytest.mark.parametrize("outcome", ["ACTIVE", "UNKNOWN"])
@pytest.mark.parametrize("fault", [None, "after"])
def test_runtime_freeze_authenticates_publishes_and_independently_reads(outcome, fault):
    fixture = FreezeFixture(outcome=outcome, commit_fault=fault)
    frozen = fixture.freeze()
    assert frozen.revision == 5 and frozen.closure.revision == 4
    assert fixture.freeze() == frozen
    assert fixture.publications == 2 and fixture.mutations == 1
    assert fixture.current_reads == fixture.authentications == 2


def test_runtime_freeze_explicit_metadata_retry_requires_fresh_inspection():
    fixture = FreezeFixture(commit_fault="before")
    for _ in range(2):
        with pytest.raises(ValueError):
            fixture.freeze()
    assert fixture.mutations == fixture.inspections == 2 and fixture.publications == 2
    assert fixture.current.state == "BUILDING"


@pytest.mark.parametrize("accepted", [False, True])
def test_new_runtime_reconciles_metadata_only_after_independent_inspection(accepted):
    from dpone.runtime.native_generation_freeze import NativeGenerationFreeze

    fixture = FreezeFixture()
    frozen = fixture.freeze() if accepted else None
    counts = fixture.publications, fixture.mutations
    recovery = NativeGenerationFreeze(
        executor=fixture.positive.executor,
        ledger=fixture,
        completion_reader=fixture,
        original_reader=fixture,
        publish_original=fixture.publish,
    )
    if accepted:
        assert recovery.freeze(fixture.reservation, expected_revision=4) == frozen
        assert (fixture.publications, fixture.mutations) == counts
    else:
        assert recovery.freeze(fixture.reservation, expected_revision=4).revision == 5
        assert (fixture.publications, fixture.mutations) == (counts[0] + 2, counts[1] + 1)
    assert fixture.inspections == 1 + int(accepted)


def test_runtime_freeze_failed_inspection_never_authorizes_metadata_cas():
    fixture = FreezeFixture()

    def unavailable(*args, **kwargs):
        raise OSError("synthetic current-owner inspection unavailable")

    fixture.inspect_freeze = unavailable
    with pytest.raises(OSError, match="inspection unavailable"):
        fixture.freeze()
    assert fixture.mutations == 0


def test_runtime_freeze_missing_proof_cannot_publish_or_mutate():
    fixture = FreezeFixture()
    fixture.reject_auth = True
    with pytest.raises(ValueError, match="missing positive"):
        fixture.freeze()
    assert fixture.publications == fixture.mutations == 0


def test_runtime_freeze_does_not_accept_cached_success_without_originals():
    fixture = FreezeFixture()
    fixture.freeze()
    fixture.reject_read = True
    with pytest.raises(ValueError, match="unreadable original"):
        fixture.freeze()
    assert fixture.mutations == 1 and fixture.publications == 2


@pytest.mark.parametrize("field,value", [("outcome", "FAILED"), ("revision", 5), ("writer_admission", "OPEN")])
def test_runtime_freeze_rejects_ineligible_custody_before_publication(field, value):
    fixture = FreezeFixture()
    if field == "writer_admission":
        fixture.current = replace(fixture.current, closure=None, writer_admission=value)
    else:
        fixture.current = replace(fixture.current, **{field: value})
    with pytest.raises(ValueError):
        fixture.freeze()
    assert fixture.publications == fixture.mutations == 0


def test_runtime_freeze_never_accepts_historical_success_after_current_read_failure():
    fixture = FreezeFixture()

    def reject_current(*args, **kwargs):
        raise ValueError("synthetic stale physical owner")

    fixture.read_freeze = reject_current
    for _ in range(2):
        with pytest.raises(ValueError, match="independently proved"):
            fixture.freeze()
    assert fixture.current.state == "FROZEN"
    assert fixture.publications == 2 and fixture.mutations == 1


@pytest.mark.parametrize("operation", ["record_freeze", "read_freeze"])
def test_freeze_adapter_serializes_identical_complete_request(operation):
    from dpone.adapters.native_generation_mssql import MssqlNativeGenerationControl
    from dpone.adapters.native_generation_mssql_freeze import freeze_parameters

    fixture = FreezeFixture()
    frozen = fixture.freeze()
    expected = freeze_parameters(fixture.reservation, closure(), fixture.positive, frozen, expected_revision=4)

    class Captured(MssqlNativeGenerationControl):
        def _execute(self, op, parameters):
            assert op == ("freeze" if operation == "record_freeze" else "freeze_read")
            assert parameters == expected
            return fixture.current

    adapter = Captured(
        connection_factory=lambda: pytest.fail("must not connect"),
        control_schema="native_control",
        control_authority=fixture.reference,
    )
    assert (
        getattr(adapter, operation)(fixture.reservation, closure(), fixture.positive, frozen, expected_revision=4)
        == fixture.current
    )


class PositiveControl:
    """In-memory procedure boundary only, never evidence of working SQL."""

    def __init__(self, *, outcome="ACTIVE", lose_ack=False, reject_auth=False, fail_current=False):
        from dpone.adapters.native_generation_mssql import MssqlNativeGenerationControl
        from dpone.contracts.native_delivery import GenerationReservation

        owner = self

        class Verifier:
            def authenticate(self, value, reference):
                owner.authentications += 1
                if reject_auth:
                    raise ValueError("synthetic invalid original")

        class Control(MssqlNativeGenerationControl):
            def read_custody(self, generation_id):
                assert generation_id == owner.reservation.generation_id
                return owner.current

            def read_admission_closure(self, generation_id):
                return closure()

            def _execute(self, operation, parameters):
                assert operation in {"complete", "completion_read"}
                assert parameters == owner.expected_parameters
                if operation == "complete":
                    owner.mutations += 1
                    owner.current = replace(owner.current, revision=4, closure=owner.reference, outcome="ACTIVE")
                    if lose_ack:
                        raise OSError("synthetic lost completion acknowledgement")
                owner.current_reads += operation == "completion_read"
                if operation == "completion_read" and fail_current:
                    raise ValueError("synthetic physical owner no longer current")
                return owner.current

        self.current = replace(snapshot(), outcome=outcome)
        self.value = completion()
        payload = encode_source_trusted_build_completion(self.value)
        self.reference = OriginalRef("synthetic/positive.json", "sha256:" + sha256(payload).hexdigest())
        self.reservation = GenerationReservation(self.current.generation_id, 1, 2, self.current.reservation)
        self.mutations = self.authentications = self.current_reads = 0
        from dpone.contracts.native_source_custody_codec import encode_source_admission_closure

        self.expected_parameters = (
            str(self.current.generation_id),
            3,
            encode_source_admission_closure(closure()),
            payload,
            self.reference.locator.encode(),
            self.reference.sha256.encode(),
        )
        self.control = Control(
            connection_factory=lambda: pytest.fail("synthetic control must not connect"),
            control_schema="native_control",
            control_authority=self.current.reservation,
            completion_verifier=Verifier(),
        )

    def record(self):
        return self.control.record_trusted_build_completion(
            self.reservation,
            closure(),
            self.value,
            self.reference,
            expected_revision=3,
        )


@pytest.mark.parametrize("outcome", ["ACTIVE", "UNKNOWN"])
@pytest.mark.parametrize("lose_ack", [False, True])
def test_durable_positive_completion_reads_exact_current_request_without_replay(outcome, lose_ack):
    fixture = PositiveControl(outcome=outcome, lose_ack=lose_ack)
    result = fixture.record()
    assert result.closure == fixture.reference
    assert result.revision == 4 and result.outcome == "ACTIVE"
    assert fixture.record() == result
    assert fixture.mutations == 1
    assert fixture.authentications == fixture.current_reads == 2


def test_durable_positive_completion_cannot_mutate_after_authentication_failure():
    fixture = PositiveControl(reject_auth=True)
    with pytest.raises(ValueError, match="invalid original"):
        fixture.record()
    assert fixture.mutations == 0


def test_durable_positive_completion_cannot_override_failed_outcome():
    fixture = PositiveControl(outcome="FAILED")
    with pytest.raises(ValueError):
        fixture.record()
    assert fixture.mutations == 0


@pytest.mark.parametrize("lose_ack", [False, True])
def test_durable_positive_completion_cannot_accept_history_when_current_owner_read_fails(lose_ack):
    from dpone.adapters.native_generation_mssql import NativeGenerationAdmissionError

    fixture = PositiveControl(lose_ack=lose_ack, fail_current=True)
    with pytest.raises(NativeGenerationAdmissionError, match="independently proved"):
        fixture.record()
    assert fixture.mutations == 1
    assert fixture.current.closure == fixture.reference
    with pytest.raises(NativeGenerationAdmissionError):
        fixture.record()
    assert fixture.mutations == 1


@pytest.mark.parametrize("revision", [True, False, 0, 2, 9223372036854775807, "3"])
def test_durable_positive_completion_rejects_invalid_revision_before_authentication(revision):
    fixture = PositiveControl()
    with pytest.raises(ValueError):
        fixture.control.record_trusted_build_completion(
            fixture.reservation, closure(), fixture.value, fixture.reference, expected_revision=revision
        )
    assert fixture.authentications == fixture.mutations == 0


@pytest.mark.parametrize("read_only", [False, True])
def test_completion_sql_uses_current_owner_before_row_lock_and_readback_never_updates(read_only):
    from dpone.adapters.native_generation_mssql_completion import completion_body

    sql = completion_body(
        "native_control",
        read_only=read_only,
        current_owner_sql="-- exact current P locks",
        current_snapshot_sql="-- exact new snapshot",
    )
    assert sql.index("-- exact current P locks") < sql.index("WITH (UPDLOCK,HOLDLOCK)")
    assert ("UPDATE [native_control].[native_generations_v1]" in sql) is not read_only
    assert "DATALENGTH(completion_payload)=DATALENGTH(@completion)" in sql
    assert "guard_epoch=@epoch" in sql
    assert "revision=@expected_revision+1" in sql
    assert "-- exact new snapshot" in sql


@pytest.mark.parametrize(
    ("operation", "digest"),
    [
        ("reserve", "28bb5bb934be79d11d6ba4932ae95b749c5e442f1fb1b4cdf2b789df8954c64b"),
        ("bind", "d381e09cbbb1a37c8571369f648c10d3815794f5e6defe6e2b584b48b3026dbc"),
        ("read", "8654f747e09719d957962909994f5587aa8c088c0a37698991a970ec93c8e137"),
        ("close", "0eaa71cce6811c9a063429f2fa9f626480f100137cb94dc793334768df650b45"),
        ("closure_read", "49d1342b09a514ac5f496404d6e217a2eddefcba51389e008a84693ec5286c4a"),
    ],
)
def test_recognized_admission_stage_procedure_bytes_remain_frozen(operation, digest):
    from dpone.adapters.native_generation_mssql_queries import generation_procedure

    # These identify retained admission-stage data, not the new completion layout.
    assert sha256(generation_procedure("native_control", operation).encode()).hexdigest() == digest


@pytest.mark.parametrize("operation", ["complete", "completion_read"])
def test_completion_procedure_keeps_caller_authority_and_extended_snapshot(operation):
    from dpone.adapters.native_generation_mssql_queries import generation_procedure

    sql = generation_procedure("native_control", operation, completed=True)
    assert "native_original_authorities_v1" in sql
    assert "runtime_principal_sid" in sql
    assert "completion_payload, completion_locator, completion_digest" in sql
    assert "frozen_payload, frozen_locator, frozen_digest" in sql
    assert "t.state=N'RUNNING'" in sql
    assert ("UPDATE [native_control].[native_generations_v1]" in sql) == (operation == "complete")


@pytest.mark.parametrize("operation", ["reserve", "bind", "read"])
def test_completed_layout_procedures_return_phase_and_full_original_triples(operation):
    from dpone.adapters.native_generation_mssql_queries import generation_procedure

    sql = generation_procedure("native_control", operation, completed=True)
    assert "admission_closure, phase, completion_payload" in sql
    if operation == "reserve":
        assert ",'RESERVED',NULL,NULL,NULL,NULL,NULL,NULL" in sql
    elif operation == "bind":
        assert "phase='BUILDING'" in sql and "phase='RESERVED'" in sql


@pytest.mark.parametrize("completed", [False, True])
def test_completion_upgrade_recognizes_all_definitions_before_extending_table(completed):
    from dpone.adapters.native_generation_mssql_queries import generation_procedure
    from dpone.adapters.native_generation_mssql_upgrade import upgrade_generation_ledger
    from tests.test_native_source_admission_closure import UpgradeCursor

    old = ("reserve", "bind", "read", "close", "closure_read")
    definitions = [generation_procedure("native_control", op, completed=completed) for op in old]
    definitions += [
        generation_procedure("native_control", op, completed=True) if completed else None
        for op in ("complete", "completion_read")
    ]
    cursor = UpgradeCursor(definitions)
    upgrade_generation_ledger(
        cursor,
        schema="native_control",
        legacy=False,
        fresh=False,
        completed=completed,
        legacy_verification="VERIFY INITIAL",
        current_verification="VERIFY ADMISSION",
        completion_verification="VERIFY COMPLETION",
    )
    ddl = [i for i, value in enumerate(cursor.statements) if "ALTER TABLE" in value]
    reads = [i for i, value in enumerate(cursor.statements) if value.startswith("SELECT OBJECT_DEFINITION")]
    assert len(reads) == 7
    if completed:
        assert not ddl
    else:
        assert max(reads) < min(ddl)
        assert any("SET phase=CASE WHEN executor IS NULL" in value for value in cursor.statements)
        # Installing a procedure containing a future capacity UPDATE does not
        # execute that UPDATE. Inspect only migration data statements here.
        data_updates = [value for value in cursor.statements if value.lstrip().startswith("UPDATE ")]
        assert not any("SET outcome=" in value or "SET charged_bytes=" in value for value in data_updates)


def test_completion_upgrade_rejects_unknown_added_procedure_before_any_ddl():
    from dpone.adapters.native_generation_mssql_queries import generation_procedure
    from dpone.adapters.native_generation_mssql_upgrade import upgrade_generation_ledger
    from tests.test_native_source_admission_closure import UpgradeCursor

    definitions = [
        generation_procedure("native_control", op) for op in ("reserve", "bind", "read", "close", "closure_read")
    ]
    cursor = UpgradeCursor([*definitions, "CREATE PROCEDURE unknown AS SELECT 1", None])
    with pytest.raises(RuntimeError, match="procedure"):
        upgrade_generation_ledger(
            cursor,
            schema="native_control",
            legacy=False,
            fresh=False,
            completed=False,
            legacy_verification="VERIFY INITIAL",
            current_verification="VERIFY ADMISSION",
            completion_verification="VERIFY COMPLETION",
        )
    assert not any("ALTER TABLE" in value for value in cursor.statements)


@pytest.mark.parametrize("installed", [False, True])
def test_freeze_upgrade_inspects_every_procedure_before_installing_pair(installed):
    from dpone.adapters.native_generation_mssql_queries import generation_procedure
    from dpone.adapters.native_generation_mssql_upgrade import COMPLETION_OPERATIONS, upgrade_generation_ledger
    from tests.test_native_source_admission_closure import UpgradeCursor

    definitions = [generation_procedure("native_control", op, completed=True) for op in COMPLETION_OPERATIONS]
    definitions += [
        generation_procedure("native_control", op, completed=True) if installed else None
        for op in ("freeze", "freeze_read")
    ]
    cursor = UpgradeCursor(definitions)
    upgrade_generation_ledger(
        cursor,
        schema="native_control",
        legacy=False,
        fresh=False,
        completed=True,
        legacy_verification="VERIFY INITIAL",
        current_verification="VERIFY ADMISSION",
        completion_verification="VERIFY COMPLETION",
        freeze_enabled=True,
    )
    reads = [i for i, sql in enumerate(cursor.statements) if sql.startswith("SELECT OBJECT_DEFINITION")]
    writes = [i for i, sql in enumerate(cursor.statements) if sql.startswith("CREATE PROCEDURE")]
    assert len(reads) == 9
    assert len(writes) == (0 if installed else 2)
    if writes:
        assert max(reads) < min(writes)
    assert not any(sql.startswith(("ALTER ", "UPDATE ", "DELETE ")) for sql in cursor.statements)


def test_freeze_upgrade_rejects_partial_pair_before_mutation():
    from dpone.adapters.native_generation_mssql_queries import generation_procedure
    from dpone.adapters.native_generation_mssql_upgrade import COMPLETION_OPERATIONS, upgrade_generation_ledger
    from tests.test_native_source_admission_closure import UpgradeCursor

    definitions = [generation_procedure("native_control", op, completed=True) for op in COMPLETION_OPERATIONS]
    cursor = UpgradeCursor([*definitions, generation_procedure("native_control", "freeze", completed=True), None])
    with pytest.raises(RuntimeError, match="partial"):
        upgrade_generation_ledger(
            cursor,
            schema="native_control",
            legacy=False,
            fresh=False,
            completed=True,
            legacy_verification="VERIFY INITIAL",
            current_verification="VERIFY ADMISSION",
            completion_verification="VERIFY COMPLETION",
            freeze_enabled=True,
        )
    assert not any(sql.startswith(("CREATE ", "ALTER ", "UPDATE ")) for sql in cursor.statements)


@pytest.mark.parametrize("installed", [False, True])
def test_inspection_upgrade_preserves_existing_nine_procedures(installed):
    from dpone.adapters.native_generation_mssql_queries import generation_procedure
    from dpone.adapters.native_generation_mssql_upgrade import FREEZE_OPERATIONS, upgrade_generation_ledger
    from tests.test_native_source_admission_closure import UpgradeCursor

    definitions = [generation_procedure("native_control", op, completed=True) for op in FREEZE_OPERATIONS]
    definitions.append(generation_procedure("native_control", "freeze_inspect", completed=True) if installed else None)
    cursor = UpgradeCursor(definitions)
    upgrade_generation_ledger(
        cursor,
        schema="native_control",
        legacy=False,
        fresh=False,
        completed=True,
        legacy_verification="VERIFY INITIAL",
        current_verification="VERIFY ADMISSION",
        completion_verification="VERIFY COMPLETION",
        freeze_enabled=True,
        inspection_enabled=True,
    )
    reads = [i for i, sql in enumerate(cursor.statements) if sql.startswith("SELECT OBJECT_DEFINITION")]
    writes = [i for i, sql in enumerate(cursor.statements) if sql.startswith("CREATE PROCEDURE")]
    assert len(reads) == 10
    assert len(writes) == int(not installed)
    if writes:
        assert max(reads) < min(writes)
        assert "native_source_freeze_inspect_v1" in cursor.statements[writes[0]]
    assert not any(sql.startswith(("ALTER ", "UPDATE ", "DELETE ")) for sql in cursor.statements)


def completed_row():
    from dpone.contracts.native_source_custody_codec import (
        encode_source_admission_closure,
        encode_source_executor_binding,
    )

    value = completion()
    payload = encode_source_trusted_build_completion(value)
    reference = OriginalRef("synthetic/completion.json", "sha256:" + sha256(payload).hexdigest())
    return (
        str(value.executor.generation_id),
        1,
        4,
        value.executor.reservation.locator.encode(),
        value.executor.reservation.sha256.encode(),
        encode_source_executor_binding(value.executor),
        "CLOSED",
        "ACTIVE",
        1,
        encode_source_admission_closure(closure()),
        "BUILDING",
        payload,
        reference.locator.encode(),
        reference.sha256.encode(),
        None,
        None,
        None,
    ), reference


class DetachedRowConnection:
    """One detached DB-API response, with observable transaction cleanup."""

    def __init__(self, row):
        self.rows = iter((row, None))
        self.commits = self.rollbacks = self.closes = 0
        self.autocommit = True

    def cursor(self):
        return self

    def execute(self, *args):
        return self

    def fetchone(self):
        return next(self.rows)

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closes += 1


def read_detached_snapshot(row):
    from dpone.adapters.native_generation_mssql import MssqlNativeGenerationControl

    connection = DetachedRowConnection(row)
    adapter = MssqlNativeGenerationControl(
        connection_factory=lambda: connection, control_schema="native_control", control_authority=completion().command
    )
    return adapter, connection


def test_extended_snapshot_preserves_historical_admission_revision_and_positive_original():
    row, reference = completed_row()
    adapter, connection = read_detached_snapshot(row)
    assert adapter.read_custody(snapshot().generation_id) == replace(snapshot(), revision=4, closure=reference)
    assert (connection.commits, connection.rollbacks, connection.closes) == (1, 0, 2)


@pytest.mark.parametrize(
    "column,value",
    [(11, None), (12, None), (13, None), (13, b"sha256:" + b"f" * 64), (10, "RESERVED"), (10, "FROZEN"), (2, 2)],
)
def test_extended_snapshot_rejects_incomplete_or_inconsistent_positive_columns(column, value):
    from dpone.adapters.native_generation_mssql import NativeGenerationAdmissionError

    row, _ = completed_row()
    changed = list(row)
    changed[column] = value
    adapter, connection = read_detached_snapshot(tuple(changed))
    with pytest.raises(NativeGenerationAdmissionError):
        adapter.read_custody(snapshot().generation_id)
    assert (connection.commits, connection.rollbacks, connection.closes) == (0, 1, 2)


def test_admission_decoder_failure_uses_the_same_transaction_error_boundary():
    from dpone.adapters.native_generation_mssql import NativeGenerationAdmissionError

    adapter, connection = read_detached_snapshot(("invalid admission row",))
    with pytest.raises(NativeGenerationAdmissionError) as failure:
        adapter.read_admission_closure(snapshot().generation_id)
    assert failure.value.__cause__ is not None
    assert (connection.commits, connection.rollbacks, connection.closes) == (0, 1, 2)


def completion():
    executor = closure().executor

    def ref(name):
        return OriginalRef("synthetic/" + name, "sha256:" + "b" * 64)

    return SourceTrustedBuildCompletion(
        executor, executor.command, ref("toolchain"), ref("build"), ref("inventory"), ref("termination")
    )


def test_completion_exact_shape_and_canonical_roundtrip():
    value = completion()
    names = {"executor", "command", "toolchain", "build_evidence", "artifact_inventory", "termination"}
    assert {field.name for field in fields(value)} == names
    encoded = encode_source_trusted_build_completion(value)
    raw = decode_native_delivery_json(encoded)
    assert set(raw) == names | {"schema"}
    assert raw["schema"] == "dpone.native-source-trusted-build-completion.v1"
    assert decode_source_trusted_build_completion(encoded) == value


@pytest.mark.parametrize(
    "field", ["executor", "command", "toolchain", "build_evidence", "artifact_inventory", "termination"]
)
@pytest.mark.parametrize("invalid", [None, True, "success", {}])
def test_completion_requires_all_exact_nested_records(field, invalid):
    with pytest.raises(NativeSourceCustodyError):
        replace(completion(), **{field: invalid})


def test_completion_command_cannot_differ_from_admitted_executor():
    with pytest.raises(NativeSourceCustodyError):
        replace(completion(), command=OriginalRef("other/command", "sha256:" + "c" * 64))


@pytest.mark.parametrize(
    "mutation", ["success", "receipt", "missing", "old_schema", "epoch_bool", "epoch_overflow", "command"]
)
def test_completion_decoder_rejects_false_or_incomplete_claims(mutation):
    raw = decode_native_delivery_json(encode_source_trusted_build_completion(completion()))
    if mutation in {"success", "receipt"}:
        raw[mutation] = True
    elif mutation == "missing":
        del raw["termination"]
    elif mutation == "old_schema":
        raw["schema"] = "dpone.native-source-writer-settlement.v1"
    elif mutation == "command":
        raw["command"]["locator"] = "other/command"
    else:
        raw["executor"]["guard_epoch"] = True if mutation == "epoch_bool" else 9223372036854775808
    with pytest.raises(NativeSourceCustodyError):
        decode_source_trusted_build_completion(json.dumps(raw, sort_keys=True, separators=(",", ":")).encode())


def test_completion_decoder_rejects_noncanonical_bytes():
    payload = encode_source_trusted_build_completion(completion())
    with pytest.raises(NativeSourceCustodyError):
        decode_source_trusted_build_completion(b" " + payload)


def snapshot():
    executor = completion().executor
    return SourceCustodySnapshot(
        executor.generation_id,
        executor.guard_epoch,
        3,
        executor,
        executor.reservation,
        None,
        None,
        None,
        None,
        (),
        "BUILDING",
        "CLOSED",
        "ACTIVE",
    )


@pytest.mark.parametrize("outcome", ["ACTIVE", "UNKNOWN"])
def test_completion_predicate_accepts_closed_current_owner_without_mutating_state(outcome):
    before = replace(snapshot(), outcome=outcome)
    assert require_trusted_build_completion(before, closure(), completion()) is None
    assert before.closure is None
    assert before.outcome == outcome


@pytest.mark.parametrize("mutation", ["open", "failed", "future_revision", "executor", "reservation", "epoch"])
def test_completion_predicate_rejects_inconsistent_custody(mutation):
    current, admission, positive = snapshot(), closure(), completion()
    if mutation == "open":
        current = replace(current, writer_admission="OPEN")
    elif mutation == "failed":
        current = replace(current, outcome="FAILED")
    elif mutation == "future_revision":
        admission = replace(admission, revision=current.revision + 1)
    else:
        executor = positive.executor
        if mutation == "executor":
            executor = replace(executor, invocation_id=type(executor.invocation_id)(int=99))
        elif mutation == "reservation":
            executor = replace(executor, reservation=OriginalRef("different", "sha256:" + "a" * 64))
        else:
            executor = replace(executor, guard_epoch=2)
        positive = replace(positive, executor=executor)
    with pytest.raises(NativeSourceCustodyError):
        require_trusted_build_completion(current, admission, positive)


def closed_source():
    executor = completion().executor
    value = SourceClosureReceipt(
        executor.generation_id,
        executor.guard_epoch,
        4,
        executor.reservation,
        completion().build_evidence,
        executor.reservation,
    )
    digest = "sha256:" + sha256(encode_source_closure_receipt(value)).hexdigest()
    return replace(value, receipt=OriginalRef("synthetic/closure", digest))


def frozen_source():
    closed = closed_source()
    value = FrozenGeneration(closed.generation_id, closed.guard_epoch, 5, closed.reservation, closed, closed.receipt)
    digest = "sha256:" + sha256(encode_frozen_generation(value)).hexdigest()
    return replace(value, frozen=OriginalRef("synthetic/frozen", digest))


def test_prepublication_payloads_match_existing_descriptor_bytes():
    from dpone.contracts.native_delivery_codec import prepare_frozen_generation, prepare_source_closure_receipt

    closed, frozen = closed_source(), frozen_source()
    payload = prepare_source_closure_receipt(
        generation_id=closed.generation_id,
        guard_epoch=closed.guard_epoch,
        revision=closed.revision,
        reservation=closed.reservation,
        completion=closed.completion,
    )
    assert payload == encode_source_closure_receipt(closed)
    assert decode_source_closure_receipt(payload, receipt=closed.receipt) == closed
    payload = prepare_frozen_generation(closed, revision=frozen.revision)
    assert payload == encode_frozen_generation(frozen)
    assert decode_frozen_generation(payload, frozen=frozen.frozen) == frozen


@pytest.mark.parametrize("field", ["guard_epoch", "revision"])
@pytest.mark.parametrize("invalid", [True, 0, -1, 9223372036854775808, "4", 4.0])
def test_prepublication_closure_rejects_invalid_coordinates(field, invalid):
    from dpone.contracts.native_delivery_codec import prepare_source_closure_receipt

    closed = closed_source()
    values = {
        name: getattr(closed, name)
        for name in ("generation_id", "guard_epoch", "revision", "reservation", "completion")
    }
    values[field] = invalid
    with pytest.raises(NativeGenerationContractError):
        prepare_source_closure_receipt(**values)


@pytest.mark.parametrize("revision", [True, 0, 4, 6, 9223372036854775808, "5"])
def test_prepublication_freeze_rejects_invalid_next_revision(revision):
    from dpone.contracts.native_delivery_codec import prepare_frozen_generation

    with pytest.raises(NativeGenerationContractError):
        prepare_frozen_generation(closed_source(), revision=revision)


@pytest.mark.parametrize("field", ["generation_id", "reservation", "completion"])
def test_prepublication_closure_rejects_coerced_identity(field):
    from dpone.contracts.native_delivery_codec import prepare_source_closure_receipt

    closed = closed_source()
    values = {
        name: getattr(closed, name)
        for name in ("generation_id", "guard_epoch", "revision", "reservation", "completion")
    }
    values[field] = str(values[field])
    with pytest.raises(NativeGenerationContractError):
        prepare_source_closure_receipt(**values)


def test_prepublication_freeze_rejects_revision_overflow():
    from dpone.contracts.native_delivery_codec import prepare_frozen_generation

    closed = replace(closed_source(), revision=9223372036854775807)
    payload = encode_source_closure_receipt(closed)
    closed = replace(closed, receipt=OriginalRef("synthetic/max-closure", "sha256:" + sha256(payload).hexdigest()))
    with pytest.raises(NativeGenerationContractError):
        prepare_frozen_generation(closed, revision=closed.revision + 1)


def test_prepublication_freeze_rejects_changed_published_closure():
    from dpone.contracts.native_delivery_codec import prepare_frozen_generation

    changed = replace(closed_source(), completion=OriginalRef("changed", "sha256:" + "c" * 64))
    with pytest.raises(NativeGenerationContractError, match="digest differs"):
        prepare_frozen_generation(changed, revision=5)


def test_closed_and_frozen_canonical_external_descriptors():
    closed, frozen = closed_source(), frozen_source()
    encoded = encode_source_closure_receipt(closed)
    assert "receipt" not in decode_native_delivery_json(encoded)
    assert decode_source_closure_receipt(encoded, receipt=closed.receipt) == closed
    encoded = encode_frozen_generation(frozen)
    assert "frozen" not in decode_native_delivery_json(encoded)
    assert decode_frozen_generation(encoded, frozen=frozen.frozen) == frozen


@pytest.mark.parametrize("field", ["guard_epoch", "revision"])
@pytest.mark.parametrize("invalid", [True, 0, -1, 9223372036854775808, "4", 4.0])
def test_closed_receipt_rejects_invalid_bigints(field, invalid):
    with pytest.raises(NativeGenerationContractError):
        replace(closed_source(), **{field: invalid})


@pytest.mark.parametrize("field", ["generation_id", "guard_epoch", "revision", "reservation"])
def test_frozen_requires_exact_closed_identity_and_next_revision(field):
    value = frozen_source()
    wrong = {
        "generation_id": type(value.generation_id)(int=90),
        "guard_epoch": 2,
        "revision": 6,
        "reservation": OriginalRef("other", "sha256:" + "f" * 64),
    }
    with pytest.raises(NativeGenerationContractError):
        replace(value, **{field: wrong[field]})


@pytest.mark.parametrize("frozen", [False, True])
def test_generation_decoders_reject_substituted_descriptor(frozen):
    bad = OriginalRef("wrong", "sha256:" + "f" * 64)
    with pytest.raises(NativeGenerationContractError):
        if frozen:
            decode_frozen_generation(encode_frozen_generation(frozen_source()), frozen=bad)
        else:
            decode_source_closure_receipt(encode_source_closure_receipt(closed_source()), receipt=bad)


@pytest.mark.parametrize("mutation", ["own_ref", "extra", "old_schema", "nested_payload", "nested_receipt", "bool"])
def test_frozen_rejects_tampering_even_with_rehashed_outer_descriptor(mutation):
    raw = decode_native_delivery_json(encode_frozen_generation(frozen_source()))
    if mutation in {"own_ref", "extra"}:
        raw["frozen" if mutation == "own_ref" else "success"] = True
    elif mutation == "old_schema":
        raw["schema"] = "dpone.native-generation-frozen.v0"
    elif mutation == "nested_payload":
        raw["closure"]["payload"]["completion"]["locator"] = "different/build"
    elif mutation == "nested_receipt":
        raw["closure"]["receipt"]["sha256"] = "sha256:" + "f" * 64
    else:
        raw["revision"] = True
    payload = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
    external = OriginalRef("synthetic/frozen", "sha256:" + sha256(payload).hexdigest())
    with pytest.raises(NativeGenerationContractError):
        decode_frozen_generation(payload, frozen=external)


def test_frozen_encoder_rejects_invalid_nested_descriptor_before_publication():
    value = frozen_source()
    substituted = replace(value.closure, receipt=OriginalRef("synthetic/closure", "sha256:" + "f" * 64))
    with pytest.raises(NativeGenerationContractError):
        encode_frozen_generation(replace(value, closure=substituted))
