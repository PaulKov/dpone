"""Closed original document categories shared by storage and invocation contracts.

Kinds discriminate authenticated payloads; they are not an extensible provider
registry. A new category requires its explicit codec and admission consumer.
"""

from typing import Literal, TypeAlias, TypeGuard, get_args

NativeOriginalKind: TypeAlias = Literal[
    "generation_storage_root_v1",
    "generation_stored_file_v1",
    "generation_seal_resolution_v1",
    "trusted_dbt_command_plan_v1",
    "trusted_dbt_invocation_completion_v1",
    "trusted_dbt_toolchain_v1",
    "trusted_dbt_qualification_v1",
    "trusted_dbt_owned_root_v1",
]


def is_native_original_kind(value: object) -> TypeGuard[NativeOriginalKind]:
    """Recognize exact strings only, without coercion or granting authority."""
    return type(value) is str and value in get_args(NativeOriginalKind)
