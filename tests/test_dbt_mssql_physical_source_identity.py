"""Closed source facts reject mismatched registrations, roles and executors."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
from dpone.contracts.dbt_mssql_physical_source_identity import PhysicalSourceIdentity, require_source_identity
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import SourceExecutorBinding
from dpone.contracts.native_source_custody_codec import decode_source_executor_binding, encode_source_executor_binding
from tests.support.dbt_mssql_physical_registration import registration_inputs


def source_case():
    registration = replace(
        MssqlPhysicalRuntimeRegistration(**registration_inputs()), registration_id=str(UUID(int=0xABCD))
    )
    generation, invocation = UUID(int=0xABC), UUID(int=0xDEF)
    reservation = OriginalRef("generations/reservation.json", "sha256:" + "7" * 64)
    executor = SourceExecutorBinding(
        generation,
        1,
        invocation,
        reservation,
        registration.trusted_profile.reference,
        OriginalRef("commands/build.json", "sha256:" + "8" * 64),
    )
    facts = PhysicalSourceIdentity(
        1,
        registration.registration_id,
        physical_runtime_registration_digest(registration),
        str(generation),
        str(invocation),
        1,
        2,
        reservation,
        encode_source_executor_binding(executor),
        registration.principals.build.model,
        registration.principals.build.control,
    )
    return registration, facts


def test_exact_source_facts():
    registration, facts = source_case()
    assert require_source_identity(facts, registration, facts.generation_id, facts.executor_invocation_id) is facts


def _source_row(facts):
    return (
        facts.wire_version,
        facts.registration_id,
        facts.registration_digest.encode(),
        facts.generation_id,
        facts.executor_invocation_id,
        facts.guard_epoch,
        facts.source_revision,
        facts.reservation.locator.encode(),
        facts.reservation.sha256.encode(),
        facts.executor_payload,
        facts.observed_model_principal.principal_id,
        bytes.fromhex(facts.observed_model_principal.sid_hex),
        facts.observed_control_principal.principal_id,
        bytes.fromhex(facts.observed_control_principal.sid_hex),
    )


@pytest.mark.parametrize("representation", [bytes, bytearray, memoryview])
def test_source_decoder_detaches_driver_binary_representations(representation):
    from dpone.contracts.dbt_mssql_physical_source_identity import decode_physical_source_row

    _, facts = source_case()
    row = tuple(representation(value) if type(value) is bytes else value for value in _source_row(facts))
    assert decode_physical_source_row(row) == facts


@pytest.mark.parametrize("row", [None, (), (None,) * 13, (None,) * 15])
def test_source_decoder_rejects_nonclosed_rows(row):
    from dpone.contracts.dbt_mssql_physical_source_identity import PhysicalSourceReadError, decode_physical_source_row

    with pytest.raises(PhysicalSourceReadError, match="exactly one closed fact row"):
        decode_physical_source_row(row)


@pytest.mark.parametrize("position,maximum", [(2, 71), (7, 4096), (8, 71), (9, 1048576), (11, 85), (13, 85)])
@pytest.mark.parametrize("damage", ["text", "empty", "oversized"])
def test_source_decoder_preserves_each_binary_field_bound(position, maximum, damage):
    from dpone.contracts.dbt_mssql_physical_source_identity import PhysicalSourceReadError, decode_physical_source_row

    _, facts = source_case()
    row = list(_source_row(facts))
    row[position] = "invalid" if damage == "text" else b"" if damage == "empty" else b"x" * (maximum + 1)
    with pytest.raises(PhysicalSourceReadError, match="binary field"):
        decode_physical_source_row(tuple(row))


def test_existing_reader_imports_and_annotations_retain_canonical_identity():
    from typing import get_type_hints

    from dpone.adapters import dbt_mssql_physical_source as reader
    from dpone.contracts import dbt_mssql_physical_source_identity as identity

    assert reader.PhysicalSourceReadError is identity.PhysicalSourceReadError
    assert reader._bytes is identity._bytes
    assert reader._uuid is identity._uuid
    assert get_type_hints(reader.MssqlPhysicalSourceReader.read)["return"] is PhysicalSourceIdentity
    assert get_type_hints(identity.decode_physical_source_row)["return"] is PhysicalSourceIdentity
    assert get_type_hints(PhysicalSourceIdentity)["reservation"] is OriginalRef


@pytest.mark.parametrize(
    "field,value",
    [
        ("wire_version", True),
        ("wire_version", 2),
        ("guard_epoch", 0),
        ("source_revision", False),
        ("registration_digest", "sha256:" + "0" * 64),
        ("executor_payload", b"{}"),
        ("generation_id", str(UUID(int=103))),
        ("executor_invocation_id", str(UUID(int=104))),
    ],
)
def test_inconsistent_facts_reject(field, value):
    registration, facts = source_case()
    with pytest.raises(ValueError):
        altered = replace(facts, **{field: value})
        require_source_identity(altered, registration, facts.generation_id, facts.executor_invocation_id)


def test_cross_database_role_swap_rejects():
    registration, facts = source_case()
    altered = replace(facts, observed_control_principal=registration.principals.metadata.control)
    with pytest.raises(ValueError):
        require_source_identity(altered, registration, facts.generation_id, facts.executor_invocation_id)


@pytest.mark.parametrize("invalid", [None, "not-a-uuid", "00000000000000000000000000000000"])
def test_reader_rejects_noncanonical_uuid_before_connection(invalid):
    from dpone.adapters.dbt_mssql_physical_source import MssqlPhysicalSourceReader

    registration, facts = source_case()
    reader = MssqlPhysicalSourceReader(
        connection_factory=lambda: pytest.fail("invalid request must not connect"), registration=registration
    )
    with pytest.raises(ValueError):
        reader.read(invalid, facts.executor_invocation_id)


@pytest.mark.parametrize(
    "failure",
    [
        None,
        "uppercase",
        "execute",
        "fetch",
        "commit",
        "empty",
        "extra",
        "wrong_width",
        "oversize",
        "registration",
        "profile",
    ],
)
def test_reader_owns_one_bounded_transaction_and_closes(failure):
    from dpone.adapters.dbt_mssql_physical_source import MssqlPhysicalSourceReader, PhysicalSourceReadError

    registration, facts = source_case()
    row = _source_row(facts)
    calls = []

    class Connection:
        autocommit = True

        def cursor(self):
            return self

        def execute(self, sql, *parameters):
            calls.append((sql, parameters))
            if parameters and failure == "execute":
                raise RuntimeError("unavailable source")
            result = row
            if failure == "uppercase":
                result = (row[0], row[1].upper(), row[2], row[3].upper(), row[4].upper()) + row[5:]
            if failure == "wrong_width":
                result = row + ("unrequested",)
            if failure == "oversize":
                result = row[:9] + (b"x" * 1048577,) + row[10:]
            if failure == "registration":
                result = row[:2] + (("sha256:" + "0" * 64).encode(),) + row[3:]
            if failure == "profile":
                executor = replace(
                    decode_source_executor_binding(facts.executor_payload),
                    profile=OriginalRef("profiles/changed", "sha256:" + "0" * 64),
                )
                result = row[:9] + (encode_source_executor_binding(executor),) + row[10:]
            self.rows = iter([None if failure == "empty" else result, row if failure == "extra" else None])

        def fetchone(self):
            if failure == "fetch":
                raise RuntimeError("response unavailable")
            return next(self.rows)

        def commit(self):
            calls.append("commit")
            if failure == "commit":
                raise RuntimeError("commit acknowledgement unavailable")

        def rollback(self):
            calls.append("rollback")

        def close(self):
            calls.append("close")

    connection = Connection()
    connections = []

    def connect():
        connections.append(connection)
        return connection

    reader = MssqlPhysicalSourceReader(connection_factory=connect, registration=registration)
    succeeded = failure in (None, "uppercase")
    if succeeded:
        assert reader.read(facts.generation_id, facts.executor_invocation_id) == facts
    else:
        with pytest.raises(PhysicalSourceReadError):
            reader.read(facts.generation_id, facts.executor_invocation_id)
    assert connections == [connection]
    assert calls[0][0].startswith("IF @@TRANCOUNT=0 BEGIN TRANSACTION;")
    assert calls[1][1] == (registration.registration_id, facts.generation_id, facts.executor_invocation_id)
    assert calls[-3:] == ["commit" if succeeded else "rollback", "close", "close"]
