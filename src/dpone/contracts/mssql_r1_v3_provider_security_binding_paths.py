"""Closed validation of binding-owned Security V2 permission paths."""

from __future__ import annotations

from typing import Protocol

from dpone.contracts import mssql_r1_v3_provider_security_principals as security
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1BindingModuleKindV1
from dpone.contracts.mssql_r1_v3_provider_security_enums import MssqlR1PermissionTargetScopeV1
from dpone.contracts.mssql_r1_v3_schema_security import MssqlR1PermissionEffectV3, MssqlR1SubjectRoleV3

_DML_PERMISSIONS = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE"})
_MODULE_KINDS = frozenset(item.value for item in MssqlR1BindingModuleKindV1)


class ResolvedBindingPath(Protocol):
    @property
    def beneficiary(self) -> security.MssqlR1SecurityPrincipalRefV1: ...

    @property
    def origin(self) -> security.MssqlR1PermissionOriginV1: ...

    @property
    def target(self) -> security.MssqlR1PermissionTargetV1: ...

    @property
    def permission(self) -> str: ...

    @property
    def effect(self) -> MssqlR1PermissionEffectV3: ...

    @property
    def grant_option(self) -> bool: ...


def _is_binding_module_name(value: str) -> bool:
    prefix, suffix = "dpone_b_", "_v3"
    if not value.startswith(prefix) or not value.endswith(suffix):
        return False
    identity, separator, module_kind = value[len(prefix) : -len(suffix)].partition("_")
    return (
        separator == "_"
        and len(identity) == 32
        and all(character in "0123456789abcdef" for character in identity)
        and module_kind in _MODULE_KINDS
    )


def is_exact_binding_path(item: ResolvedBindingPath) -> bool:
    """Accept only binding-signer DML or runtime binding-module EXECUTE."""

    target = item.target
    if (
        type(item.origin) is not security.MssqlR1DirectPermissionOriginV1
        or item.effect is not MssqlR1PermissionEffectV3.GRANT
        or item.grant_option
        or target.scope is not MssqlR1PermissionTargetScopeV1.OBJECT
        or not target.include_descendants
    ):
        return False
    if type(item.beneficiary) is security.MssqlR1BindingSignerInstancePrincipalRefV1:
        return item.permission in _DML_PERMISSIONS
    return (
        type(item.beneficiary) is security.MssqlR1EnvironmentPrincipalRefV1
        and item.beneficiary.subject_role is MssqlR1SubjectRoleV3.RUNTIME
        and item.permission == "EXECUTE"
        and target.schema_name == "dpone_authority"
        and target.object_name is not None
        and _is_binding_module_name(target.object_name)
    )
