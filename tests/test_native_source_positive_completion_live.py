"""Real SQL completion and migration, with explicitly synthetic build originals.

This certifies the ledger boundary only, not actual dbt execution or delivery.
All databases and credentials belong to the isolated disposable test fixture.
"""

from __future__ import annotations

import os
from hashlib import sha256

import pytest

from dpone.adapters.native_generation_mssql import MssqlNativeGenerationControl, NativeGenerationAdmissionError
from dpone.adapters.native_generation_mssql_queries import generation_procedure, generation_procedure_name
from dpone.adapters.native_generation_mssql_upgrade import FREEZE_INSPECTION_OPERATIONS, GENERATION_ADMISSION_CHECK
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import SourceTrustedBuildCompletion
from dpone.contracts.native_source_custody_codec import encode_source_trusted_build_completion
from tests.test_native_generation_admission_live import generations as generations
from tests.test_native_original_bindings import D
from tests.test_native_originals_mssql_live import CommitFault
from tests.test_native_originals_mssql_live import ledger as ledger
from tests.test_native_source_admission_closure_live import bound, charged, retained_row, runtime_principal

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(os.environ.get("DPONE_RUN_NATIVE_ORIGINAL_MSSQL_LIVE") != "1", reason="isolated SQL disabled"),
]


def prepared(generations, outcome="ACTIVE"):
    _, provider, runtime, admin, _, _, _ = generations
    _, reservation, executor, snapshot = bound(generations)
    if outcome != "ACTIVE":
        connection = admin()
        try:
            connection.execute("UPDATE dpone_control.native_generations_v1 SET outcome=?", outcome)
            connection.commit()
        finally:
            connection.close()
    admission = provider().close_writer_admission(reservation, expected_revision=snapshot.revision)
    positive = SourceTrustedBuildCompletion(
        executor,
        executor.command,
        OriginalRef("synthetic/toolchain", D),
        OriginalRef("synthetic/build", D),
        OriginalRef("synthetic/inventory", D),
        OriginalRef("synthetic/termination", D),
    )
    payload = encode_source_trusted_build_completion(positive)
    reference = OriginalRef("synthetic/positive/東京.json", "sha256:" + sha256(payload).hexdigest())

    class SyntheticVerifier:
        def authenticate(self, completion, completion_ref):
            assert completion == positive and completion_ref == reference

    def record(factory=runtime):
        control = MssqlNativeGenerationControl(
            connection_factory=factory,
            control_schema="dpone_control",
            control_authority=OriginalRef("control/authority", D),
            completion_verifier=SyntheticVerifier(),
        )
        return control.record_trusted_build_completion(
            reservation,
            admission,
            positive,
            reference,
            expected_revision=admission.revision,
        )

    return record, reservation, admission, reference


def freeze_request(generations, *, completion_locator=None, substitute=False, receipt_digest=None):
    """Prepare synthetic originals for the real SQL ledger boundary only."""
    import json

    from dpone.contracts.native_delivery_codec import (
        decode_source_closure_receipt,
        prepare_frozen_generation,
        prepare_source_closure_receipt,
    )
    from dpone.contracts.native_delivery_json import encode_native_delivery_json
    from dpone.contracts.native_source_custody_codec import encode_source_admission_closure

    record, reservation, admission, reference = prepared(generations)
    before = record()
    _, provider, _, admin, _, _, _ = generations
    connection = admin()
    try:
        positive_payload = bytes(
            connection.execute("SELECT completion_payload FROM dpone_control.native_generations_v1").fetchone()[0]
        )
        if completion_locator is not None and not substitute:
            connection.execute(
                "UPDATE dpone_control.native_generations_v1 SET completion_locator=?",
                completion_locator.encode("utf-8"),
            )
            connection.commit()
            reference = OriginalRef(completion_locator, reference.sha256)
    finally:
        connection.close()
    closure_reference = OriginalRef(completion_locator, reference.sha256) if substitute else reference
    payload = prepare_source_closure_receipt(
        generation_id=reservation.generation_id,
        guard_epoch=reservation.guard_epoch,
        revision=before.revision,
        reservation=reservation.reservation,
        completion=closure_reference,
    )
    closed = decode_source_closure_receipt(
        payload, receipt=OriginalRef("synthetic/freeze/closure", "sha256:" + sha256(payload).hexdigest())
    )
    frozen = prepare_frozen_generation(closed, revision=before.revision + 1)
    if receipt_digest is not None:
        parsed = json.loads(frozen)
        parsed["closure"]["receipt"]["sha256"] = receipt_digest
        frozen = encode_native_delivery_json(parsed)
    frozen_ref = OriginalRef("synthetic/freeze/frozen", "sha256:" + sha256(frozen).hexdigest())
    parameters = (
        str(reservation.generation_id),
        before.revision,
        encode_source_admission_closure(admission),
        positive_payload,
        reference.locator.encode("utf-8"),
        reference.sha256.encode("ascii"),
        frozen,
        frozen_ref.locator.encode("utf-8"),
        frozen_ref.sha256.encode("ascii"),
    )
    return provider(), parameters, provider().read_custody(reservation.generation_id), frozen_ref


@pytest.mark.parametrize("locator", ["x" * 4001, "x" * 4096, "я" * 2048, "😀" * 1024])
@pytest.mark.parametrize("substitute", [False, True])
def test_live_freeze_long_reference_bounds_match_utf8_contract(generations, locator, substitute):
    control, parameters, before, reference = freeze_request(
        generations,
        completion_locator=locator,
        substitute=substitute,
    )
    admin = generations[3]
    if substitute:
        with pytest.raises(Exception, match="DPONE_NATIVE_SOURCE_FREEZE_COMPLETION_MISMATCH"):
            control._execute("freeze", parameters)
        assert control.read_custody(before.generation_id) == before
    else:
        result = control._execute("freeze", parameters)
        assert (result.state, result.outcome, result.revision, result.frozen) == (
            "FROZEN",
            "ACTIVE",
            before.revision + 1,
            reference,
        )
        assert control._execute("freeze_read", parameters) == result
        assert control._execute("freeze", parameters) == result
    assert charged(admin) == 60


@pytest.mark.parametrize("digest", ["a" * 4001, "sha256:" + "a" * 65, "", None, 71])
def test_live_freeze_rejects_invalid_nested_receipt_without_mutation(generations, digest):
    import json

    from dpone.contracts.native_delivery_json import encode_native_delivery_json

    control, parameters, before, _ = freeze_request(generations)
    parsed = json.loads(parameters[6])
    parsed["closure"]["receipt"]["sha256"] = digest
    payload = encode_native_delivery_json(parsed)
    parameters = (*parameters[:6], payload, parameters[7], ("sha256:" + sha256(payload).hexdigest()).encode())
    with pytest.raises(Exception, match="DPONE_NATIVE_(SOURCE_FREEZE_CLOSURE_INVALID|GENERATION_JSON_SHAPE_INVALID)"):
        control._execute("freeze", parameters)
    assert control.read_custody(before.generation_id) == before
    assert charged(generations[3]) == 60


@pytest.mark.parametrize("outcome", ["UNKNOWN", "FAILED"])
def test_live_freeze_reconciles_only_positive_unknown(generations, outcome):
    control, parameters, before, reference = freeze_request(generations)
    connection = generations[3]()
    try:
        connection.execute("UPDATE dpone_control.native_generations_v1 SET outcome=?", outcome)
        connection.commit()
    finally:
        connection.close()
    if outcome == "UNKNOWN":
        result = control._execute("freeze", parameters)
        assert (result.state, result.outcome, result.revision, result.frozen) == (
            "FROZEN",
            "ACTIVE",
            before.revision + 1,
            reference,
        )
    else:
        with pytest.raises(Exception, match="DPONE_NATIVE_SOURCE_FREEZE_CUSTODY_MISMATCH"):
            control._execute("freeze", parameters)
        after = control.read_custody(before.generation_id)
        assert after.revision == before.revision and after.state == "BUILDING" and after.frozen is None
        assert after.outcome == "FAILED"
    assert charged(generations[3]) == 60


@pytest.mark.parametrize("fault", [None, "before", "after"])
def test_live_runtime_freeze_recovery_uses_actual_current_owner_inspection(generations, fault):
    from dpone.contracts.native_source_custody import NativeSourceCustodyError
    from dpone.contracts.native_source_custody_codec import decode_source_trusted_build_completion
    from dpone.runtime.native_generation_freeze import NativeGenerationFreeze

    record, reservation, admission, reference = prepared(generations)
    before = record()
    _, _, runtime, admin, _, _, _ = generations
    connection = admin()
    try:
        positive = decode_source_trusted_build_completion(
            bytes(
                connection.execute("SELECT completion_payload FROM dpone_control.native_generations_v1").fetchone()[0]
            )
        )
    finally:
        connection.close()

    class SyntheticOriginals:
        """Synthetic authenticated port; only the SQL/runtime boundary is live."""

        max_bytes = 1048576

        def __init__(self):
            self.objects = {}
            self.publications = self.authentications = 0

        def require_generation(self, executor):
            assert executor == positive.executor

        def read_completion(self, ref):
            assert ref == reference
            self.authentications += 1
            return positive

        def publish(self, *, kind, locator, payload, max_bytes):
            assert len(payload) <= max_bytes
            ref = OriginalRef(locator, "sha256:" + sha256(payload).hexdigest())
            if (ref, kind) in self.objects:
                assert self.objects[(ref, kind)] == payload
                return ref
            self.objects[(ref, kind)] = payload
            self.publications += 1
            return ref

        def read(self, ref, kind):
            return self.objects[(ref, kind)]

    originals = SyntheticOriginals()
    calls = []

    def factory():
        calls.append(1)
        connection = runtime()
        # Current custody, admission, inspection precede the sole mutation.
        return CommitFault(connection, fault == "after") if fault and len(calls) == 4 else connection

    def coordinator(factory):
        ledger = MssqlNativeGenerationControl(
            connection_factory=factory,
            control_schema="dpone_control",
            control_authority=OriginalRef("control/authority", D),
        )
        return NativeGenerationFreeze(
            executor=positive.executor,
            ledger=ledger,
            completion_reader=originals,
            original_reader=originals,
            publish_original=originals.publish,
        )

    first = coordinator(factory)
    if fault == "before":
        with pytest.raises(NativeSourceCustodyError, match="independently proved"):
            first.freeze(reservation, expected_revision=before.revision)
        assert generations[1]().read_custody(reservation.generation_id) == before
        assert charged(admin) == 60
        # A new coordinator has no attempted-operation cache. It must inspect
        # the exact current request before this explicit metadata retry.
        frozen = coordinator(runtime).freeze(reservation, expected_revision=before.revision)
    else:
        frozen = first.freeze(reservation, expected_revision=before.revision)
    assert frozen.revision == before.revision + 1
    assert len(calls) == 5
    assert coordinator(runtime).freeze(reservation, expected_revision=before.revision) == frozen
    assert originals.publications == 2
    assert originals.authentications == (3 if fault == "before" else 2)
    assert frozen.closure.revision == before.revision and admission.revision < before.revision
    assert charged(admin) == 60


@pytest.mark.parametrize("committed", [False, True])
def test_live_freeze_lost_commit_ack_uses_separate_read_only_result(generations, committed):
    control, parameters, before, reference = freeze_request(generations)
    runtime = generations[2]
    faulted = MssqlNativeGenerationControl(
        connection_factory=lambda: CommitFault(runtime(), committed),
        control_schema="dpone_control",
        control_authority=OriginalRef("control/authority", D),
    )
    with pytest.raises(OSError, match="synthetic lost commit acknowledgement"):
        faulted._execute("freeze", parameters)
    if committed:
        result = control._execute("freeze_read", parameters)
        assert result.frozen == reference and result.revision == before.revision + 1
    else:
        with pytest.raises(Exception, match="DPONE_NATIVE_SOURCE_FREEZE_READBACK_MISMATCH"):
            control._execute("freeze_read", parameters)
        assert control.read_custody(before.generation_id) == before
    assert charged(generations[3]) == 60


@pytest.mark.parametrize("accepted", [False, True])
def test_live_freeze_stale_owner_cannot_mutate_or_acknowledge_history(generations, accepted):
    control, parameters, before, _ = freeze_request(generations)
    if accepted:
        before = control._execute("freeze", parameters)
    connection = generations[3]()
    try:
        connection.execute("UPDATE dpone_control.semantic_refresh_guards SET fencing_epoch=fencing_epoch+1")
        connection.commit()
    finally:
        connection.close()
    for operation in ("freeze", "freeze_read"):
        with pytest.raises(Exception, match="DPONE_NATIVE_GENERATION_PHYSICAL_OWNER_CHANGED"):
            control._execute(operation, parameters)
    assert control.read_custody(before.generation_id) == before
    assert charged(generations[3]) == 60


@pytest.mark.parametrize("outcome", ["ACTIVE", "UNKNOWN"])
def test_live_completion_reconciles_exact_owner_and_preserves_charge(generations, outcome):
    _, provider, _, admin, migration, _, _ = generations
    record, reservation, admission, reference = prepared(generations, outcome)
    observed = record()
    assert observed.revision == admission.revision + 1
    assert (observed.state, observed.outcome, observed.closure) == ("BUILDING", "ACTIVE", reference)
    assert provider().read_admission_closure(reservation.generation_id) == admission
    assert record() == observed
    migration.apply()
    assert record() == observed
    assert charged(admin) == 60


@pytest.mark.parametrize("committed", [False, True])
def test_live_completion_lost_ack_never_repeats_mutation(generations, committed):
    _, provider, runtime, admin, _, _, _ = generations
    record, reservation, admission, reference = prepared(generations)
    calls = []

    def factory():
        calls.append(1)
        connection = runtime()
        return CommitFault(connection, committed) if len(calls) == 3 else connection

    if committed:
        assert record(factory).closure == reference
    else:
        with pytest.raises(NativeGenerationAdmissionError):
            record(factory)
    assert len(calls) == 4
    observed = provider().read_custody(reservation.generation_id)
    assert observed.revision == admission.revision + int(committed)
    assert observed.closure == (reference if committed else None)
    assert charged(admin) == 60


@pytest.mark.parametrize("recorded", [False, True])
def test_live_completion_rejects_stale_physical_owner_even_after_previous_success(generations, recorded):
    _, _, _, admin, _, _, _ = generations
    record, _, _, _ = prepared(generations)
    if recorded:
        record()
    connection = admin()
    try:
        connection.execute("UPDATE dpone_control.semantic_refresh_guards SET fencing_epoch=fencing_epoch+1")
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(NativeGenerationAdmissionError):
        record()
    assert charged(admin) == 60


def retained_admission_fixture(admin, principal):
    """Disposable fixture downgrade only; production migration never downgrades."""
    connection = admin()
    try:
        connection.execute(
            "ALTER TABLE dpone_control.native_generations_v1 DROP CONSTRAINT CK_native_generation_completion_v1"
        )
        connection.execute(
            "ALTER TABLE dpone_control.native_generations_v1 DROP COLUMN phase,completion_payload,completion_locator,completion_digest,frozen_payload,frozen_locator,frozen_digest"
        )
        connection.execute(
            "ALTER TABLE dpone_control.native_generations_v1 ADD CHECK (" + GENERATION_ADMISSION_CHECK + ")"
        )
        for operation in FREEZE_INSPECTION_OPERATIONS:
            name = "[dpone_control].[" + generation_procedure_name(operation) + "]"
            connection.execute("DROP PROCEDURE " + name)
            if operation not in {"complete", "completion_read", "freeze", "freeze_read", "freeze_inspect"}:
                connection.execute(generation_procedure("dpone_control", operation))
                connection.execute("GRANT EXECUTE ON OBJECT::" + name + " TO [" + principal.replace("]", "]]") + "]")
        connection.commit()
    finally:
        connection.close()


def test_live_populated_admission_upgrade_preserves_originals_and_records_completion(generations):
    _, provider, runtime, admin, migration, _, _ = generations
    record, reservation, admission, reference = prepared(generations)
    before = retained_row(admin)
    retained_admission_fixture(admin, runtime_principal(runtime))
    assert provider().read_admission_closure(reservation.generation_id) == admission
    migration.apply()
    assert retained_row(admin) == before
    assert provider().read_admission_closure(reservation.generation_id) == admission
    assert record().closure == reference
    assert charged(admin) == 60


def test_live_partial_original_triple_is_rejected_by_constraint(generations):
    _, _, _, admin, _, _, _ = generations
    prepared(generations)
    connection = admin()
    try:
        with pytest.raises(Exception, match="CHECK constraint"):
            connection.execute("UPDATE dpone_control.native_generations_v1 SET completion_payload=?", b"{}")
        connection.rollback()
    finally:
        connection.close()
    assert charged(admin) == 60
