"""Cohesive internal owners preserve exact shared types and wire discriminators."""

import ast
from dataclasses import fields
from importlib import import_module
from pathlib import Path
from typing import get_type_hints

import pytest

OWNERS = {
    "MssqlR1ResourceKindV1": "dpone.contracts.mssql_r1_v3_physical_descriptor_resources",
    "MssqlR1AccessKindV1": "dpone.contracts.mssql_r1_v3_physical_descriptor_resources",
    "MssqlR1LockKindV1": "dpone.contracts.mssql_r1_v3_physical_descriptor_resources",
    "MssqlR1LockCardinalityV1": "dpone.contracts.mssql_r1_v3_physical_descriptor_resources",
    "MssqlR1ProjectionScalarKindV1": "dpone.contracts.mssql_r1_v3_physical_descriptor_resources",
    "MssqlR1ValueCardinalityV1": "dpone.contracts.mssql_r1_v3_physical_descriptor_resources",
    "MssqlR1LockActionV1": "dpone.contracts.mssql_r1_v3_physical_descriptor_locks",
    "MssqlR1LockMechanismV1": "dpone.contracts.mssql_r1_v3_physical_descriptor_locks",
    "MssqlR1LockModeV1": "dpone.contracts.mssql_r1_v3_physical_descriptor_locks",
    "MssqlR1LockOwnerV1": "dpone.contracts.mssql_r1_v3_physical_descriptor_locks",
    "MssqlR1LockTimeoutPolicyV1": "dpone.contracts.mssql_r1_v3_physical_descriptor_locks",
    "MssqlR1BindingSignerIdentityV2": "dpone.contracts.mssql_r1_v3_binding_permissions",
    "MssqlR1BindingSignatureIntentV2": "dpone.contracts.mssql_r1_v3_binding_permissions",
}

ENUM_MEMBERS = {
    "MssqlR1ResourceKindV1": (
        ("STATIC_OBJECT", "static_object"),
        ("DYNAMIC_STAGE", "dynamic_stage"),
        ("REGISTERED_TARGET", "registered_target"),
        ("CATALOG", "catalog"),
        ("SESSION", "session"),
        ("PERMISSION", "permission"),
        ("SIGNATURE", "signature"),
        ("EXTENDED_PROPERTY", "extended_property"),
    ),
    "MssqlR1AccessKindV1": (
        ("READ", "read"),
        ("INSERT", "insert"),
        ("UPDATE", "update"),
        ("DELETE", "delete"),
        ("DDL", "ddl"),
        ("EXECUTE", "execute"),
        ("GRANT", "grant"),
        ("REVOKE", "revoke"),
        ("SIGN", "sign"),
        ("ADD_PROPERTY", "add_property"),
        ("DROP_PROPERTY", "drop_property"),
    ),
    "MssqlR1LockKindV1": (
        ("SCHEMA", "schema"),
        ("PHYSICAL", "physical"),
        ("BINDING", "binding"),
        ("OPERATION", "operation"),
        ("ARTIFACT", "artifact"),
        ("REGISTRATION", "registration"),
        ("AUTHORITY", "authority"),
    ),
    "MssqlR1LockMechanismV1": (("APPLICATION", "application"), ("GUARDED_ROW", "guarded_row"), ("TABLE", "table")),
    "MssqlR1LockActionV1": (("ACQUIRE", "acquire"), ("ASSERT_HELD", "assert_held")),
    "MssqlR1LockModeV1": (("SHARED", "shared"), ("UPDATE", "update"), ("EXCLUSIVE", "exclusive")),
    "MssqlR1LockOwnerV1": (("TRANSACTION", "transaction"),),
    "MssqlR1LockCardinalityV1": (("ONE", "one"), ("EXACT_REQUEST_SET", "exact_request_set")),
    "MssqlR1LockTimeoutPolicyV1": (("BOUNDED_ENVIRONMENT", "bounded_environment"),),
    "MssqlR1ProjectionScalarKindV1": (
        ("TEXT", "text"),
        ("BINARY", "binary"),
        ("DIGEST", "digest"),
        ("UUID", "uuid"),
        ("INTEGER", "integer"),
        ("BOOLEAN", "boolean"),
        ("UTC", "utc"),
    ),
    "MssqlR1ValueCardinalityV1": (("SCALAR", "scalar"), ("ORDERED_SET", "ordered_set")),
}

SIGNER_FIELDS = {
    "MssqlR1BindingSignerIdentityV2": (
        "contract_version",
        "target_binding_uuid",
        "lifecycle_policy",
        "certificate_name",
        "certificate_user_name",
        "certificate_subject",
        "certificate_owner",
        "start_date_yyyymmdd",
        "expiry_date_yyyymmdd",
        "certificate_creation_profile",
        "signature_algorithm",
        "secret_policy_digest",
    ),
    "MssqlR1BindingSignatureIntentV2": (
        "contract_version",
        "ordinal",
        "module_kind",
        "semantic_module_digest",
        "signer_identity_digest",
    ),
}


@pytest.mark.parametrize(("name", "owner"), OWNERS.items())
def test_definition_lives_in_its_cohesive_owner(name, owner):
    contract = getattr(import_module(owner), name)
    assert contract.__module__ == owner
    root = Path(__file__).resolve().parents[1] / "src/dpone/contracts"
    definitions = []
    for path in root.glob("mssql_r1_v3_*.py"):
        if any(isinstance(node, ast.ClassDef) and node.name == name for node in ast.parse(path.read_text()).body):
            definitions.append(path.stem)
    assert definitions == [owner.rsplit(".", 1)[-1]]


@pytest.mark.parametrize(("name", "members"), ENUM_MEMBERS.items())
def test_moved_discriminator_member_order_and_wire_values_are_frozen(name, members):
    contract = getattr(import_module(OWNERS[name]), name)
    assert tuple((member.name, member.value) for member in contract) == members
    assert all(contract(value) is member for member, (_, value) in zip(contract, members))
    with pytest.raises(ValueError):
        contract("unknown_discriminator")


@pytest.mark.parametrize(("name", "names"), SIGNER_FIELDS.items())
def test_signer_constructor_fields_and_immutability_are_preserved(name, names):
    contract = getattr(import_module(OWNERS[name]), name)
    assert tuple(field.name for field in fields(contract)) == names
    assert contract.__dataclass_params__.frozen is True
    assert tuple(contract.__slots__) == names


def test_consumers_share_the_owner_type_without_duplicate_alias_classes():
    resources = import_module("dpone.contracts.mssql_r1_v3_physical_descriptor_resources")
    locks = import_module("dpone.contracts.mssql_r1_v3_physical_descriptor_locks")
    permissions = import_module("dpone.contracts.mssql_r1_v3_binding_permissions")
    pack = import_module("dpone.contracts.mssql_r1_v3_binding_pack")
    assert locks.MssqlR1LockKindV1 is resources.MssqlR1LockKindV1
    assert locks.MssqlR1LockCardinalityV1 is resources.MssqlR1LockCardinalityV1
    hints = get_type_hints(locks.MssqlR1PhysicalLockStepV1)
    assert hints["lock_kind"] is resources.MssqlR1LockKindV1
    assert hints["cardinality"] is resources.MssqlR1LockCardinalityV1
    assert hints["action"] is locks.MssqlR1LockActionV1
    assert pack.MssqlR1BindingSignerIdentityV2 is permissions.MssqlR1BindingSignerIdentityV2
    assert pack.MssqlR1BindingSignatureIntentV2 is permissions.MssqlR1BindingSignatureIntentV2


def test_retired_owners_do_not_forward_moved_definitions():
    enums = import_module("dpone.contracts.mssql_r1_v3_physical_descriptor_enums")
    assert not any(hasattr(enums, name) for name in ENUM_MEMBERS)
    root = Path(__file__).resolve().parents[1] / "src/dpone/contracts"
    assert not (root / "mssql_r1_v3_binding_signer.py").exists()
