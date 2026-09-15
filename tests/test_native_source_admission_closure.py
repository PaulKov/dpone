"""Admission closure is a bounded identity record, not a positive build result."""

from __future__ import annotations

import json
from dataclasses import replace
from hashlib import sha256
from uuid import UUID

import pytest

from dpone.adapters.native_generation_mssql import MssqlNativeGenerationControl, NativeGenerationAdmissionError
from dpone.adapters.native_generation_mssql_queries import generation_procedure
from dpone.contracts.native_delivery import GenerationReservation
from dpone.contracts.native_delivery_json import decode_native_delivery_json
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import (
    NativeSourceCustodyError,
    SourceAdmissionClosure,
    SourceCustodySnapshot,
    SourceExecutorBinding,
)
from dpone.contracts.native_source_custody_codec import (
    decode_source_admission_closure,
    encode_source_admission_closure,
)


def closure() -> SourceAdmissionClosure:
    reference = OriginalRef("synthetic/original.json", "sha256:" + "a" * 64)
    executor = SourceExecutorBinding(UUID(int=1), 1, UUID(int=2), reference, reference, reference)
    value = SourceAdmissionClosure(executor, 1, 3, reference)
    digest = "sha256:" + sha256(encode_source_admission_closure(value)).hexdigest()
    return replace(value, receipt=OriginalRef("synthetic/admission-closure.json", digest))


def test_closure_roundtrip_uses_external_receipt_without_hash_self_reference() -> None:
    expected = closure()
    payload = encode_source_admission_closure(expected)
    document = decode_native_delivery_json(payload)
    assert set(document) == {"schema", "executor", "admission_sequence", "revision"}
    assert document["schema"] == "dpone.native-source-admission-closure.v1"
    assert "receipt" not in document
    assert expected.receipt.locator.encode() not in payload
    assert decode_source_admission_closure(payload, receipt=expected.receipt) == expected


@pytest.mark.parametrize("field", ["admission_sequence", "revision"])
@pytest.mark.parametrize("value", [True, False, 0, -1, 9223372036854775808, 1.0, "1", None])
def test_closure_rejects_nonpositive_or_inexact_bigint(field: str, value: object) -> None:
    with pytest.raises(NativeSourceCustodyError):
        replace(closure(), **{field: value})


@pytest.mark.parametrize("field", ["admission_sequence", "revision"])
def test_closure_accepts_bigint_upper_bound(field: str) -> None:
    value = replace(closure(), **{field: 9223372036854775807})
    assert getattr(value, field) == 9223372036854775807


@pytest.mark.parametrize("field", ["executor", "receipt"])
def test_closure_requires_exact_nested_records(field: str) -> None:
    with pytest.raises(NativeSourceCustodyError):
        replace(closure(), **{field: None})


def test_decoder_rejects_receipt_digest_for_other_payload() -> None:
    expected = closure()
    with pytest.raises(NativeSourceCustodyError):
        decode_source_admission_closure(
            encode_source_admission_closure(expected),
            receipt=OriginalRef(expected.receipt.locator, "sha256:" + "b" * 64),
        )


@pytest.mark.parametrize("mutation", ["receipt", "extra", "missing", "schema", "bool", "float"])
def test_decoder_rejects_unrecognized_payload_even_with_matching_digest(mutation: str) -> None:
    expected = closure()
    document = decode_native_delivery_json(encode_source_admission_closure(expected))
    if mutation == "receipt":
        document["receipt"] = {"locator": expected.receipt.locator, "sha256": expected.receipt.sha256}
    elif mutation == "extra":
        document["success"] = True
    elif mutation == "missing":
        del document["revision"]
    elif mutation == "schema":
        document["schema"] = "dpone.native-source-writer-settlement.v1"
    elif mutation == "bool":
        document["revision"] = True
    else:
        document["revision"] = 3.0
    payload = json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
    receipt = OriginalRef(expected.receipt.locator, "sha256:" + sha256(payload).hexdigest())
    with pytest.raises((NativeSourceCustodyError, ValueError)):
        decode_source_admission_closure(payload, receipt=receipt)


def test_decoder_rejects_noncanonical_bytes_even_with_matching_digest() -> None:
    expected = closure()
    payload = b" " + encode_source_admission_closure(expected)
    receipt = OriginalRef(expected.receipt.locator, "sha256:" + sha256(payload).hexdigest())
    with pytest.raises(NativeSourceCustodyError):
        decode_source_admission_closure(payload, receipt=receipt)


@pytest.mark.parametrize(
    ("operation", "digest"),
    [
        ("reserve", "46149c852abd9c12941beabe727a3041320b70bea6fa4853a3d257075fcbaa0c"),
        ("bind", "21a79b69dfe845ebc09409eecf120a599f2a369f21907774022631fc57d96b3a"),
        ("read", "ff0d2e6d13578d6d38291d9b0193ab9ae284dfe47050645a643777d2a4e30b7d"),
    ],
)
def test_upgrade_recognizes_exact_retained_initial_procedure_bytes(operation: str, digest: str) -> None:
    # Frozen initial-admission definitions from 2c32334. Do not update these
    # identities when adding a new procedure revision: they identify old data.
    definition = generation_procedure("native_control", operation, extended=False)
    assert sha256(definition.encode()).hexdigest() == digest


class ClosureControl(MssqlNativeGenerationControl):
    """Synthetic metadata outcomes; these do not certify a live SQL transition."""

    def __init__(self, *, lose_ack=False, outcome="ACTIVE"):
        self.value = closure()
        self.reserved = GenerationReservation(UUID(int=1), 1, 2, self.value.executor.reservation)
        self.closed = False
        self.mutations = 0
        self.lose_ack = lose_ack
        self.outcome = outcome
        super().__init__(
            connection_factory=lambda: pytest.fail("synthetic control must not connect"),
            control_schema="native_control",
            control_authority=self.reserved.reservation,
        )

    def read_custody(self, generation_id):
        assert generation_id == self.reserved.generation_id
        return SourceCustodySnapshot(
            generation_id=generation_id,
            guard_epoch=1,
            revision=3 if self.closed else 2,
            executor=self.value.executor,
            reservation=self.reserved.reservation,
            closure=None,
            frozen=None,
            quality=None,
            export_plan=None,
            active_reads=(),
            state="BUILDING",
            writer_admission="CLOSED" if self.closed else "OPEN",
            outcome=self.outcome,
        )

    def _query(self, operation, parameters, decoder):
        if operation == "close":
            self.mutations += 1
            assert parameters == (
                str(self.reserved.generation_id),
                2,
                self.reserved.reservation.locator.encode(),
                self.reserved.reservation.sha256.encode(),
            )
            self.closed = True
            if self.lose_ack:
                raise OSError("synthetic lost commit acknowledgement")
        else:
            assert operation == "closure_read"
        assert self.closed
        payload = encode_source_admission_closure(self.value)
        locator = f"generations/{self.reserved.generation_id}/writer-admission-closure.json".encode()
        return decoder((payload, locator, self.value.receipt.sha256.encode()))


@pytest.mark.parametrize("lose_ack", [False, True])
@pytest.mark.parametrize("outcome", ["ACTIVE", "UNKNOWN"])
def test_close_reconciles_metadata_once_without_clearing_unknown_or_granting_completion(lose_ack, outcome):
    control = ClosureControl(lose_ack=lose_ack, outcome=outcome)
    value = control.close_writer_admission(control.reserved, expected_revision=2)
    assert value.executor == control.value.executor
    assert value.revision == 3
    assert control.close_writer_admission(control.reserved, expected_revision=2) == value
    assert control.mutations == 1
    snapshot = control.read_custody(control.reserved.generation_id)
    assert snapshot.writer_admission == "CLOSED"
    assert snapshot.outcome == outcome
    assert snapshot.closure is None


def test_stale_revision_cannot_close_or_replay_another_request():
    control = ClosureControl()
    with pytest.raises(NativeGenerationAdmissionError):
        control.close_writer_admission(control.reserved, expected_revision=3)
    assert control.mutations == 0


def test_different_reservation_cannot_close_admission():
    control = ClosureControl()
    other = replace(control.reserved, reservation=OriginalRef("another/reservation", "sha256:" + "f" * 64))
    with pytest.raises(NativeGenerationAdmissionError):
        control.close_writer_admission(other, expected_revision=2)
    assert control.mutations == 0


class UpgradeCursor:
    """Procedure observations only; real constraint and rollback tests are live."""

    def __init__(self, definitions):
        self.definitions = iter(definitions)
        self.statements = []
        self.pending = None

    def execute(self, sql, *parameters):
        self.statements.append(sql)
        if sql.startswith("SELECT OBJECT_DEFINITION"):
            self.pending = (next(self.definitions),)
        return self

    def fetchone(self):
        result, self.pending = self.pending, None
        return result


def test_unknown_retained_procedure_rejects_before_upgrade_ddl():
    from dpone.adapters.native_generation_mssql_upgrade import upgrade_generation_ledger

    cursor = UpgradeCursor(["CREATE PROCEDURE unexpected AS SELECT 1"])
    with pytest.raises(RuntimeError, match="procedure"):
        upgrade_generation_ledger(
            cursor,
            schema="native_control",
            legacy=True,
            fresh=False,
            legacy_verification="VERIFY LEGACY",
            current_verification="VERIFY CURRENT",
        )
    assert not any("ALTER TABLE" in statement for statement in cursor.statements)


def test_known_current_migration_does_not_rewrite_procedures_or_tables():
    from dpone.adapters.native_generation_mssql_upgrade import upgrade_generation_ledger

    operations = ("reserve", "bind", "read", "close", "closure_read")
    cursor = UpgradeCursor([generation_procedure("native_control", operation) for operation in operations])
    upgrade_generation_ledger(
        cursor,
        schema="native_control",
        legacy=False,
        fresh=False,
        legacy_verification="VERIFY LEGACY",
        current_verification="VERIFY CURRENT",
    )
    assert not any(
        "ALTER TABLE" in statement or statement.startswith("CREATE PROCEDURE") for statement in cursor.statements
    )
