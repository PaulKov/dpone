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
    registration = MssqlPhysicalRuntimeRegistration(**registration_inputs())
    generation, invocation = UUID(int=101), UUID(int=102)
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


@pytest.mark.parametrize(
    "failure",
    [None, "execute", "fetch", "commit", "empty", "extra", "wrong_width", "oversize", "registration", "profile"],
)
def test_reader_owns_one_bounded_transaction_and_closes(failure):
    from dpone.adapters.dbt_mssql_physical_source import MssqlPhysicalSourceReader, PhysicalSourceReadError

    registration, facts = source_case()
    row = (
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
    if failure is None:
        assert reader.read(facts.generation_id, facts.executor_invocation_id) == facts
    else:
        with pytest.raises(PhysicalSourceReadError):
            reader.read(facts.generation_id, facts.executor_invocation_id)
    assert connections == [connection]
    assert calls[0][0] == "SET IMPLICIT_TRANSACTIONS OFF; BEGIN TRANSACTION"
    assert calls[1][1] == (registration.registration_id, facts.generation_id, facts.executor_invocation_id)
    assert calls[-3:] == ["commit" if failure is None else "rollback", "close", "close"]
