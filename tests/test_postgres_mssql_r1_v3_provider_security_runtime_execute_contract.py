"""Regression contract for binding-owned runtime EXECUTE permission closure."""

from __future__ import annotations

from dataclasses import replace
from uuid import UUID

import pytest

from dpone.contracts.mssql_r1_v3_codec import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1BindingModuleKindV1
from dpone.contracts.mssql_r1_v3_provider_security_enums import MssqlR1PermissionTargetScopeV1
from dpone.contracts.mssql_r1_v3_provider_security_permissions import (
    MssqlR1PermissionClosurePolicyV1,
    MssqlR1ResolvedPermissionPathV2,
    project_schema_permission_rule,
)
from dpone.contracts.mssql_r1_v3_provider_security_principals import (
    MssqlR1BindingSignerInstancePrincipalRefV1,
    MssqlR1DirectPermissionOriginV1,
    MssqlR1EnvironmentPrincipalRefV1,
    MssqlR1NamedDatabaseRoleRefV1,
    MssqlR1PermissionTargetV1,
    MssqlR1PublicPermissionOriginV1,
    MssqlR1RolePermissionOriginV1,
    MssqlR1SharedSignerPrincipalRefV1,
)
from dpone.contracts.mssql_r1_v3_schema_primitives import MssqlR1SignerProfileKindV3
from dpone.contracts.mssql_r1_v3_schema_security import MssqlR1PermissionEffectV3, MssqlR1SubjectRoleV3
from tests.test_postgres_mssql_r1_v3_provider_security_contract import _descriptor


def _owner() -> MssqlR1EnvironmentPrincipalRefV1:
    return MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.PROVISIONER)


def _signer_select() -> MssqlR1ResolvedPermissionPathV2:
    return MssqlR1ResolvedPermissionPathV2(
        MssqlR1BindingSignerInstancePrincipalRefV1(UUID(int=1)),
        _owner(),
        MssqlR1DirectPermissionOriginV1(),
        MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.OBJECT, "dbo", "orders", None, True),
        "SELECT",
        MssqlR1PermissionEffectV3.GRANT,
        False,
    )


def _runtime_execute(
    module_kind: MssqlR1BindingModuleKindV1 = MssqlR1BindingModuleKindV1.BATCH_MUTATE,
    *,
    binding_hex: str = "00000000000000000000000000000001",
    schema_name: str = "dpone_authority",
    object_name: str | None = None,
) -> MssqlR1ResolvedPermissionPathV2:
    return MssqlR1ResolvedPermissionPathV2(
        MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.RUNTIME),
        _owner(),
        MssqlR1DirectPermissionOriginV1(),
        MssqlR1PermissionTargetV1(
            MssqlR1PermissionTargetScopeV1.OBJECT,
            schema_name,
            object_name or f"dpone_b_{binding_hex}_{module_kind.value}_v3",
            None,
            True,
        ),
        "EXECUTE",
        MssqlR1PermissionEffectV3.GRANT,
        False,
    )


def _complete_observation(
    binding_paths: tuple[MssqlR1ResolvedPermissionPathV2, ...],
) -> tuple[MssqlR1ResolvedPermissionPathV2, ...]:
    descriptor = _descriptor()
    shared = tuple(
        project_schema_permission_rule(item) for item in descriptor.expected_schema_contract.ordered_permission_rules
    )
    return tuple(sorted((*shared, *binding_paths), key=lambda item: item.canonical_bytes))


@pytest.mark.parametrize("module_kind", tuple(MssqlR1BindingModuleKindV1))
def test_complete_closure_accepts_every_runtime_module_kind(module_kind: MssqlR1BindingModuleKindV1) -> None:
    descriptor = _descriptor()
    policy = MssqlR1PermissionClosurePolicyV1.create(descriptor)
    binding_paths = tuple(
        sorted((_signer_select(), _runtime_execute(module_kind)), key=lambda item: item.canonical_bytes)
    )

    assert policy.validate_complete(descriptor, binding_paths, _complete_observation(binding_paths)) == (
        _complete_observation(binding_paths)
    )


@pytest.mark.parametrize(
    "invalid_path",
    (
        replace(
            _runtime_execute(),
            beneficiary=MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.LOADER),
        ),
        replace(
            _runtime_execute(),
            beneficiary=MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.PROVISIONER),
        ),
        replace(
            _runtime_execute(),
            beneficiary=MssqlR1EnvironmentPrincipalRefV1(MssqlR1SubjectRoleV3.OBSERVER),
        ),
        replace(
            _runtime_execute(),
            beneficiary=MssqlR1SharedSignerPrincipalRefV1(MssqlR1SignerProfileKindV3.ATTESTOR),
        ),
        replace(_runtime_execute(), permission="SELECT"),
        _runtime_execute(schema_name="dbo"),
        replace(
            _runtime_execute(),
            target=MssqlR1PermissionTargetV1(
                MssqlR1PermissionTargetScopeV1.COLUMN, "dpone_authority", "module", "column", False
            ),
        ),
        replace(
            _runtime_execute(),
            target=MssqlR1PermissionTargetV1(
                MssqlR1PermissionTargetScopeV1.SCHEMA, "dpone_authority", None, None, True
            ),
        ),
        replace(
            _runtime_execute(),
            target=MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.DATABASE, None, None, None, False),
        ),
        replace(
            _runtime_execute(),
            target=replace(_runtime_execute().target, include_descendants=False),
        ),
        replace(
            _runtime_execute(),
            origin=MssqlR1RolePermissionOriginV1(MssqlR1NamedDatabaseRoleRefV1("etl_runtime")),
        ),
        replace(_runtime_execute(), origin=MssqlR1PublicPermissionOriginV1()),
        replace(_runtime_execute(), effect=MssqlR1PermissionEffectV3.DENY),
        replace(_runtime_execute(), grant_option=True),
        _runtime_execute(binding_hex="0000000000000000000000000000000"),
        _runtime_execute(binding_hex="000000000000000000000000000000001"),
        _runtime_execute(binding_hex="0000000000000000000000000000000A"),
        _runtime_execute(object_name="dpone_x_00000000000000000000000000000001_batch_mutate_v3"),
        _runtime_execute(object_name="dpone_b_00000000000000000000000000000001_unknown_v3"),
        _runtime_execute(object_name="dpone_b_00000000000000000000000000000001_batch_mutate_v2"),
    ),
    ids=(
        "foreign-environment-role",
        "provisioner-role",
        "observer-role",
        "shared-signer",
        "wrong-permission",
        "wrong-schema",
        "column-scope",
        "schema-scope",
        "database-scope",
        "descendants-disabled",
        "role-origin",
        "public-origin",
        "deny",
        "grant-option",
        "short-binding-hex",
        "long-binding-hex",
        "uppercase-binding-hex",
        "wrong-prefix",
        "unknown-module-kind",
        "wrong-suffix",
    ),
)
def test_runtime_execute_binding_path_is_closed(invalid_path: MssqlR1ResolvedPermissionPathV2) -> None:
    descriptor = _descriptor()
    policy = MssqlR1PermissionClosurePolicyV1.create(descriptor)
    binding_paths = tuple(sorted((_signer_select(), invalid_path), key=lambda item: item.canonical_bytes))

    assert MssqlR1ResolvedPermissionPathV2.from_canonical_bytes(invalid_path.canonical_bytes) == invalid_path
    with pytest.raises(MssqlR1V3ContractError, match="binding paths are not pre-resolved direct authority"):
        policy.validate_complete(descriptor, binding_paths, _complete_observation(binding_paths))


@pytest.mark.parametrize("permission", ("SELECT", "INSERT", "UPDATE", "DELETE"))
def test_binding_signer_accepts_only_exact_object_dml_permissions(permission: str) -> None:
    descriptor = _descriptor()
    policy = MssqlR1PermissionClosurePolicyV1.create(descriptor)
    path = replace(_signer_select(), permission=permission)
    binding_paths = (path,)

    assert policy.validate_complete(descriptor, binding_paths, _complete_observation(binding_paths))


@pytest.mark.parametrize("permission", ("EXECUTE", "CONTROL", "ALTER"))
def test_binding_signer_rejects_non_dml_permissions(permission: str) -> None:
    descriptor = _descriptor()
    policy = MssqlR1PermissionClosurePolicyV1.create(descriptor)
    path = replace(_signer_select(), permission=permission)

    with pytest.raises(MssqlR1V3ContractError, match="binding paths are not pre-resolved direct authority"):
        policy.validate_complete(descriptor, (path,), _complete_observation((path,)))


@pytest.mark.parametrize(
    "invalid_path",
    (
        replace(
            _signer_select(),
            origin=MssqlR1RolePermissionOriginV1(MssqlR1NamedDatabaseRoleRefV1("etl_signer")),
        ),
        replace(_signer_select(), origin=MssqlR1PublicPermissionOriginV1()),
        replace(_signer_select(), effect=MssqlR1PermissionEffectV3.DENY),
        replace(_signer_select(), grant_option=True),
        replace(_signer_select(), target=replace(_signer_select().target, include_descendants=False)),
        replace(
            _signer_select(),
            target=MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.SCHEMA, "dbo", None, None, True),
        ),
        replace(
            _signer_select(),
            target=MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.DATABASE, None, None, None, False),
        ),
        replace(
            _signer_select(),
            target=MssqlR1PermissionTargetV1(MssqlR1PermissionTargetScopeV1.COLUMN, "dbo", "orders", "id", False),
        ),
    ),
    ids=(
        "role-origin",
        "public-origin",
        "deny",
        "grant-option",
        "descendants-disabled",
        "schema-scope",
        "database-scope",
        "column-scope",
    ),
)
def test_binding_signer_object_dml_path_is_closed(invalid_path: MssqlR1ResolvedPermissionPathV2) -> None:
    descriptor = _descriptor()
    policy = MssqlR1PermissionClosurePolicyV1.create(descriptor)

    assert MssqlR1ResolvedPermissionPathV2.from_canonical_bytes(invalid_path.canonical_bytes) == invalid_path
    with pytest.raises(MssqlR1V3ContractError, match="binding paths are not pre-resolved direct authority"):
        policy.validate_complete(descriptor, (invalid_path,), _complete_observation((invalid_path,)))
