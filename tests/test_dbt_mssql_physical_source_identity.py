"""Closed source facts reject mismatched registrations, roles and executors."""

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.dbt_mssql_physical_registration import MssqlPhysicalRuntimeRegistration
from dpone.contracts.dbt_mssql_physical_registration_codec import physical_runtime_registration_digest
from dpone.contracts.dbt_mssql_physical_source_identity import PhysicalSourceIdentity, require_source_identity
from dpone.contracts.native_identity import OriginalRef
from dpone.contracts.native_source_custody import SourceExecutorBinding
from dpone.contracts.native_source_custody_codec import encode_source_executor_binding
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
