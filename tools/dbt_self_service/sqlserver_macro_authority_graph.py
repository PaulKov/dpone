"""Pure SQL Server macro graph authority shared by offline producer modes.

No manifest or helper I/O occurs here. Closure membership, dispatch precedence,
record hashing and failure order preserve the approved producer's exact policy.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from typing import Any, TypeAlias, TypeGuard

TRUSTED_PACKAGES = ("dbt", "dbt_sqlserver")
TRUSTED_ROOTS = (
    "macro.dbt_sqlserver.materialization_table_sqlserver",
    "macro.dbt_sqlserver.materialization_view_sqlserver",
    "macro.dbt_sqlserver.materialization_incremental_sqlserver",
    "macro.dbt.get_incremental_append_sql",
    "macro.dbt.get_incremental_merge_sql",
    "macro.dbt.materialization_test_default",
    "macro.dbt.materialization_unit_default",
    # Admitted materializations reach these roots through Python adapter
    # methods; manifest macro dependencies alone omit those execution edges.
    "macro.dbt.drop_relation",
    "macro.dbt.rename_relation",
    "macro.dbt.get_columns_in_relation",
    "macro.dbt.list_relations_without_caching",
)
INVOCATION_EXTENSION_UNIQUE_IDS = (
    "macro.dbt.is_incremental",
    "macro.dbt.test_not_null",
    "macro.dbt.default__test_not_null",
    "macro.dbt.test_unique",
    "macro.dbt.default__test_unique",
    "macro.dbt.test_relationships",
    "macro.dbt.default__test_relationships",
)

MacroRecord: TypeAlias = tuple[str, str, str, str, tuple[str, ...]]


def _framework_records(macros: Mapping[str, Any]) -> tuple[MacroRecord, ...]:
    records: dict[str, MacroRecord] = {}
    visiting: list[str] = []

    def visit(unique_id: str) -> None:
        if unique_id in visiting:
            cycle = " -> ".join((*visiting[visiting.index(unique_id) :], unique_id))
            raise ValueError(f"framework macro dependencies must be acyclic: {cycle}")
        if unique_id in records:
            return
        raw = macros.get(unique_id)
        if raw is None:
            raise ValueError(f"framework macro dependency is missing: {unique_id}")
        record = _macro_record(unique_id, raw)
        if record[1] not in TRUSTED_PACKAGES:
            raise ValueError(f"framework macro belongs to a foreign package: {unique_id}")
        visiting.append(unique_id)
        for dependency in record[4]:
            visit(dependency)
        visiting.pop()
        records[unique_id] = record

    for root in TRUSTED_ROOTS:
        visit(root)
    return tuple(sorted(records.values()))


def _invocation_extension_records(
    macros: Mapping[str, Any],
    framework: tuple[MacroRecord, ...],
) -> tuple[MacroRecord, ...]:
    extension = tuple(
        sorted(_macro_record(unique_id, macros.get(unique_id)) for unique_id in INVOCATION_EXTENSION_UNIQUE_IDS)
    )
    if any(record[1] != "dbt" for record in extension):
        raise ValueError("every invocation extension record must belong to the dbt package")
    authority_ids = {record[0] for record in (*framework, *extension)}
    for record in extension:
        foreign_dependencies = set(record[4]) - authority_ids
        if foreign_dependencies:
            raise ValueError(
                f"invocation macro dependencies must terminate in the authority union: {record[0]} -> "
                f"{sorted(foreign_dependencies)}"
            )
    _assert_acyclic((*framework, *extension))
    return extension


def _macro_record(unique_id: str, raw: object) -> MacroRecord:
    macro = _mapping(raw, unique_id)
    package_name = _text(macro.get("package_name"), f"{unique_id}.package_name")
    name = _text(macro.get("name"), f"{unique_id}.name")
    observed_id = _text(macro.get("unique_id"), f"{unique_id}.unique_id")
    if observed_id != unique_id or unique_id != f"macro.{package_name}.{name}":
        raise ValueError(f"macro identity must match its manifest key: {unique_id}")
    if macro.get("resource_type") != "macro":
        raise ValueError(f"{unique_id}.resource_type must equal 'macro'")
    body = _text(macro.get("macro_sql"), f"{unique_id}.macro_sql", allow_empty=True)
    depends_on = _mapping(macro.get("depends_on"), f"{unique_id}.depends_on")
    raw_dependencies = depends_on.get("macros")
    if not _is_non_empty_string_sequence(raw_dependencies):
        raise ValueError(f"{unique_id}.depends_on.macros must be an array of non-empty strings")
    dependencies = tuple(sorted(raw_dependencies))
    if len(dependencies) != len(set(dependencies)):
        raise ValueError(f"{unique_id}.depends_on.macros must not contain duplicates")
    return (unique_id, package_name, name, _sha256(body.encode("utf-8")), dependencies)


def _dispatch_protection(
    records: tuple[MacroRecord, ...],
    macros: Mapping[str, Any],
) -> tuple[tuple[str, ...], tuple[tuple[str, str], ...], tuple[str, ...]]:
    logical_names = {_logical_name(record[2]) for record in records}
    protected = {candidate for name in logical_names for candidate in (name, f"default__{name}", f"sqlserver__{name}")}
    by_package_and_name = {(record[1], record[2]): record[0] for record in records}
    winners: list[tuple[str, str]] = []
    for name in sorted(logical_names):
        winner = (
            by_package_and_name.get(("dbt_sqlserver", f"sqlserver__{name}"))
            or by_package_and_name.get(("dbt_sqlserver", name))
            or by_package_and_name.get(("dbt", f"default__{name}"))
        )
        if winner is not None:
            winners.append((name, winner))
    candidates: list[str] = []
    for raw_key, raw in sorted(macros.items(), key=lambda item: str(item[0])):
        if not isinstance(raw_key, str) or not isinstance(raw, Mapping):
            continue
        raw_name = raw.get("name")
        if not isinstance(raw_name, str) or raw_name not in protected:
            continue
        record = _macro_record(raw_key, raw)
        if record[1] not in TRUSTED_PACKAGES:
            raise ValueError(f"protected dispatch candidate belongs to a foreign package: {raw_key}")
        candidates.append(raw_key)
    return tuple(sorted(protected)), tuple(winners), tuple(candidates)


def _assert_acyclic(records: tuple[MacroRecord, ...]) -> None:
    dependencies = {record[0]: record[4] for record in records}
    complete: set[str] = set()
    visiting: list[str] = []

    def visit(unique_id: str) -> None:
        if unique_id in visiting:
            raise ValueError(f"macro authority union must be acyclic at {unique_id}")
        if unique_id in complete:
            return
        visiting.append(unique_id)
        for dependency in dependencies[unique_id]:
            visit(dependency)
        visiting.pop()
        complete.add(unique_id)

    for unique_id in sorted(dependencies):
        visit(unique_id)


def _logical_name(name: str) -> str:
    for prefix in ("default__", "sqlserver__"):
        if name.startswith(prefix):
            return name.removeprefix(prefix)
    return name


def _mapping(value: object, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field} must be an object")
    return value


def _text(value: object, field: str, *, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value):
        raise ValueError(f"{field} must be a {'string' if allow_empty else 'non-empty string'}")
    return value


def _is_non_empty_string_sequence(value: object) -> TypeGuard[Sequence[str]]:
    return (
        isinstance(value, Sequence)
        and not isinstance(value, (str, bytes, bytearray))
        and all(isinstance(item, str) and bool(item) for item in value)
    )


def _sha256(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()
