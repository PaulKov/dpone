#!/usr/bin/env python3
"""Generate the immutable SQL Server dbt macro-authority baseline."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any, TypeAlias, TypeGuard

ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = ROOT / "examples" / "dbt-inline-publishing" / "fixtures" / "manifest.v12.json"
HELPER_PATH = ROOT / "packages" / "dbt-dpone" / "macros" / "dpone_publish.sql"
OUTPUT_PATH = ROOT / "src" / "dpone" / "contracts" / "dbt_sqlserver_macro_authority_baseline.py"

GENERATOR_VERSION = 1
DBT_CORE_VERSION = "1.12.3"
DBT_SQLSERVER_VERSION = "1.11.1"
MANIFEST_SCHEMA = "v12"
MANIFEST_SCHEMA_URL = "https://schemas.getdbt.com/dbt/manifest/v12.json"
ADAPTER_TYPE = "sqlserver"
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
HELPER_UNIQUE_ID = "macro.dbt_dpone.dpone_publish"
HELPER_BODY_SHA256 = "sha256:6223548504b32a7670cf7fb8ef7a3f42c9ad144ffae14b8591074de6b0decc83"
AUTHORITY_DIFF_SCHEMA = "dpone.dbt-sqlserver-macro-authority-diff.v1"
EXECUTION_CAPABLE_TOKENS = ("run_query", "statement(", "adapter.", "dispatch(", "{% call")
EXPECTED_FRAMEWORK_RECORD_COUNT = 153
EXPECTED_INVOCATION_EXTENSION_RECORD_COUNT = 7

MacroRecord: TypeAlias = tuple[str, str, str, str, tuple[str, ...]]


def main(argv: Sequence[str] | None = None) -> int:
    """Generate the baseline or fail when ``--check`` observes drift."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Fail instead of writing when the baseline differs")
    parser.add_argument(
        "--candidate-manifest",
        type=Path,
        help="Compare the approved fixture with a candidate dbt manifest instead of regenerating the baseline",
    )
    parser.add_argument(
        "--diff-output",
        type=Path,
        help="Write the deterministic candidate macro-authority diff to this JSON file",
    )
    args = parser.parse_args(argv)
    if args.candidate_manifest is not None:
        if args.check:
            parser.error("--check cannot be combined with --candidate-manifest")
        if args.diff_output is None:
            parser.error("--diff-output is required with --candidate-manifest")
        try:
            diff = generate_candidate_diff(args.candidate_manifest)
            args.diff_output.write_text(
                json.dumps(diff, allow_nan=False, ensure_ascii=True, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
            print(f"macro-authority candidate comparison failed: {exc}", file=sys.stderr)
            return 2
        print(f"wrote {args.diff_output}")
        return 0
    if args.diff_output is not None:
        parser.error("--diff-output requires --candidate-manifest")
    try:
        expected = generate_baseline()
    except (OSError, UnicodeError, ValueError, json.JSONDecodeError) as exc:
        print(f"macro-authority generation failed: {exc}", file=sys.stderr)
        return 2
    if args.check:
        try:
            current = OUTPUT_PATH.read_text(encoding="utf-8")
        except FileNotFoundError:
            current = ""
        if current != expected:
            print(f"macro-authority baseline is stale: {OUTPUT_PATH.relative_to(ROOT)}", file=sys.stderr)
            return 1
        print("macro-authority baseline is current")
        return 0
    OUTPUT_PATH.write_text(expected, encoding="utf-8")
    print(f"wrote {OUTPUT_PATH.relative_to(ROOT)}")
    return 0


def generate_baseline() -> str:
    """Return the complete deterministic generated Python module."""

    manifest = _load_manifest()
    macros = _mapping(manifest.get("macros"), "manifest.macros")
    framework = _framework_records(macros)
    extension = _invocation_extension_records(macros, framework)
    authority = tuple(sorted((*framework, *extension)))
    if len(framework) != EXPECTED_FRAMEWORK_RECORD_COUNT:
        raise ValueError(
            f"framework macro closure must contain {EXPECTED_FRAMEWORK_RECORD_COUNT} records, observed {len(framework)}"
        )
    expected_authority_count = EXPECTED_FRAMEWORK_RECORD_COUNT + EXPECTED_INVOCATION_EXTENSION_RECORD_COUNT
    if (
        len(extension) != EXPECTED_INVOCATION_EXTENSION_RECORD_COUNT
        or len({record[0] for record in authority}) != expected_authority_count
    ):
        raise ValueError(
            "invocation extension must contain "
            f"{EXPECTED_INVOCATION_EXTENSION_RECORD_COUNT} records and form a "
            f"{expected_authority_count}-record authority union"
        )
    protected_names, winners, dispatch_candidates = _dispatch_protection(authority, macros)
    helper = _helper_record(macros)
    baseline_sha256 = _fingerprint(
        {
            "dbt_core": DBT_CORE_VERSION,
            "dbt_sqlserver": DBT_SQLSERVER_VERSION,
            "manifest_schema": MANIFEST_SCHEMA,
            "adapter_type": ADAPTER_TYPE,
            "trusted_packages": list(TRUSTED_PACKAGES),
            "trusted_roots": list(TRUSTED_ROOTS),
            "framework_records": [_jsonable_record(record) for record in framework],
            "invocation_extension_records": [_jsonable_record(record) for record in extension],
            "protected_macro_names": list(protected_names),
            "dispatch_family_winners": [list(item) for item in winners],
            "dispatch_candidate_unique_ids": list(dispatch_candidates),
            "dpone_publish_helper": _jsonable_record(helper),
        }
    )
    return _render_module(
        framework=framework,
        extension=extension,
        protected_names=protected_names,
        winners=winners,
        dispatch_candidates=dispatch_candidates,
        helper=helper,
        baseline_sha256=baseline_sha256,
    )


def generate_candidate_diff(candidate_manifest_path: Path) -> dict[str, object]:
    """Return a deterministic review artifact for a candidate dbt toolchain manifest.

    The artifact deliberately reports any newly reachable execution-capable macro.
    It does not approve a candidate or mutate the checked-in authority baseline.
    """

    baseline = _load_manifest()
    candidate = _load_manifest(candidate_manifest_path, expected_dbt_version=None)
    baseline_inventory = _authority_inventory(baseline)
    candidate_inventory = _authority_inventory(candidate)
    framework_diff = _record_diff(baseline_inventory["framework_records"], candidate_inventory["framework_records"])
    extension_diff = _record_diff(
        baseline_inventory["invocation_extension_records"], candidate_inventory["invocation_extension_records"]
    )
    changed_candidate_ids = {
        entry["unique_id"]
        for diff in (framework_diff, extension_diff)
        for section in ("added", "changed")
        for entry in diff[section]
    }
    candidate_macros = _mapping(candidate.get("macros"), "manifest.macros")
    execution_tokens = {
        unique_id: _execution_capable_tokens(candidate_macros, unique_id) for unique_id in changed_candidate_ids
    }
    execution_capable = [
        {"unique_id": unique_id, "tokens": execution_tokens[unique_id]}
        for unique_id in sorted(execution_tokens)
        if execution_tokens[unique_id]
    ]
    return {
        "schema": AUTHORITY_DIFF_SCHEMA,
        "baseline": _inventory_summary(baseline, baseline_inventory),
        "candidate": _inventory_summary(candidate, candidate_inventory),
        "framework_macro_records": framework_diff,
        "invocation_extension_records": extension_diff,
        "new_or_changed_execution_capable_macros": execution_capable,
        "trusted_root_paths_to_new_or_changed_execution_capable_macros": _trusted_root_paths(
            candidate_inventory["framework_records"],
            set(execution_tokens),
        ),
    }


def _load_manifest(
    path: Path = MANIFEST_PATH,
    *,
    expected_dbt_version: str | None = DBT_CORE_VERSION,
) -> Mapping[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    manifest = _mapping(payload, "manifest")
    metadata = _mapping(manifest.get("metadata"), "manifest.metadata")
    expected = {
        "dbt_schema_version": MANIFEST_SCHEMA_URL,
        "adapter_type": ADAPTER_TYPE,
    }
    if expected_dbt_version is not None:
        expected["dbt_version"] = expected_dbt_version
    for field, value in expected.items():
        if metadata.get(field) != value:
            raise ValueError(f"manifest.metadata.{field} must equal {value!r}")
    if expected_dbt_version is None:
        _text(metadata.get("dbt_version"), "manifest.metadata.dbt_version")
    return manifest


def _authority_inventory(manifest: Mapping[str, Any]) -> dict[str, object]:
    macros = _mapping(manifest.get("macros"), "manifest.macros")
    framework = _framework_records(macros)
    extension = _invocation_extension_records(macros, framework)
    authority = tuple(sorted((*framework, *extension)))
    protected_names, winners, dispatch_candidates = _dispatch_protection(authority, macros)
    return {
        "framework_records": framework,
        "invocation_extension_records": extension,
        "protected_macro_names": protected_names,
        "dispatch_family_winners": winners,
        "dispatch_candidate_unique_ids": dispatch_candidates,
    }


def _inventory_summary(manifest: Mapping[str, Any], inventory: Mapping[str, object]) -> dict[str, object]:
    metadata = _mapping(manifest.get("metadata"), "manifest.metadata")
    return {
        "dbt_core_version": _text(metadata.get("dbt_version"), "manifest.metadata.dbt_version"),
        "manifest_schema": MANIFEST_SCHEMA,
        "adapter_type": ADAPTER_TYPE,
        "framework_macro_record_count": len(inventory["framework_records"]),
        "invocation_extension_record_count": len(inventory["invocation_extension_records"]),
        "protected_macro_names": list(inventory["protected_macro_names"]),
        "dispatch_family_winners": [list(item) for item in inventory["dispatch_family_winners"]],
        "dispatch_candidate_unique_ids": list(inventory["dispatch_candidate_unique_ids"]),
    }


def _record_diff(
    baseline_records: object,
    candidate_records: object,
) -> dict[str, list[dict[str, object]]]:
    baseline_by_id = _records_by_unique_id(baseline_records, "baseline authority records")
    candidate_by_id = _records_by_unique_id(candidate_records, "candidate authority records")
    added = [
        _jsonable_record(candidate_by_id[unique_id])
        for unique_id in sorted(candidate_by_id.keys() - baseline_by_id.keys())
    ]
    removed = [
        _jsonable_record(baseline_by_id[unique_id])
        for unique_id in sorted(baseline_by_id.keys() - candidate_by_id.keys())
    ]
    changed = [
        {
            "unique_id": unique_id,
            "changed_fields": _changed_record_fields(baseline_by_id[unique_id], candidate_by_id[unique_id]),
            "before": _jsonable_record(baseline_by_id[unique_id]),
            "after": _jsonable_record(candidate_by_id[unique_id]),
        }
        for unique_id in sorted(baseline_by_id.keys() & candidate_by_id.keys())
        if baseline_by_id[unique_id] != candidate_by_id[unique_id]
    ]
    return {"added": added, "removed": removed, "changed": changed}


def _records_by_unique_id(records: object, field: str) -> dict[str, MacroRecord]:
    if not isinstance(records, tuple) or not all(isinstance(record, tuple) and len(record) == 5 for record in records):
        raise ValueError(f"{field} must contain macro records")
    result = {record[0]: record for record in records}
    if len(result) != len(records):
        raise ValueError(f"{field} must not contain duplicate unique ids")
    return result


def _changed_record_fields(before: MacroRecord, after: MacroRecord) -> list[str]:
    fields = ("package_name", "name", "macro_sql_sha256", "depends_on_macros")
    return [
        field
        for field, before_value, after_value in zip(fields, before[1:], after[1:], strict=True)
        if before_value != after_value
    ]


def _execution_capable_tokens(macros: Mapping[str, Any], unique_id: str) -> list[str]:
    macro = _mapping(macros.get(unique_id), unique_id)
    body = _text(macro.get("macro_sql"), f"{unique_id}.macro_sql", allow_empty=True)
    return [token for token in EXECUTION_CAPABLE_TOKENS if token in body]


def _trusted_root_paths(records: object, targets: set[object]) -> list[dict[str, object]]:
    """Report one deterministic dependency path per trusted root and changed executable macro."""

    dependencies = {
        record[0]: record[4] for record in _records_by_unique_id(records, "candidate framework records").values()
    }
    executable_targets = sorted(target for target in targets if isinstance(target, str))
    paths: list[dict[str, object]] = []
    for root in TRUSTED_ROOTS:
        for target in executable_targets:
            path = _dependency_path(root, target, dependencies)
            if path is not None:
                paths.append({"trusted_root": root, "target_unique_id": target, "path": list(path)})
    return paths


def _dependency_path(
    root: str,
    target: str,
    dependencies: Mapping[str, tuple[str, ...]],
) -> tuple[str, ...] | None:
    pending: list[tuple[str, tuple[str, ...]]] = [(root, (root,))]
    visited: set[str] = set()
    while pending:
        unique_id, path = pending.pop(0)
        if unique_id in visited:
            continue
        visited.add(unique_id)
        if unique_id == target:
            return path
        pending.extend(
            (dependency, (*path, dependency))
            for dependency in dependencies.get(unique_id, ())
            if dependency not in visited
        )
    return None


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


def _helper_record(macros: Mapping[str, Any]) -> MacroRecord:
    body = HELPER_PATH.read_text(encoding="utf-8")
    body_sha256 = _sha256(body.encode("utf-8"))
    if body_sha256 != HELPER_BODY_SHA256:
        raise ValueError("dpone_publish helper body differs from the approved metadata-only source")
    forbidden = ("run_query", "adapter.", "dispatch(", "statement(", "{% call", "{{")
    if any(token in body for token in forbidden) or "{% do return({'dpone': {'publish': publish}}) %}" not in body:
        raise ValueError("dpone_publish helper must remain metadata-only")
    record = _macro_record(HELPER_UNIQUE_ID, macros.get(HELPER_UNIQUE_ID))
    manifest_body = _text(
        _mapping(macros.get(HELPER_UNIQUE_ID), HELPER_UNIQUE_ID).get("macro_sql"),
        f"{HELPER_UNIQUE_ID}.macro_sql",
        allow_empty=True,
    )
    if manifest_body != body.rstrip("\r\n"):
        raise ValueError("dbt did not preserve the approved dpone_publish helper source")
    return record


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


def _render_module(
    *,
    framework: tuple[MacroRecord, ...],
    extension: tuple[MacroRecord, ...],
    protected_names: tuple[str, ...],
    winners: tuple[tuple[str, str], ...],
    dispatch_candidates: tuple[str, ...],
    helper: MacroRecord,
    baseline_sha256: str,
) -> str:
    lines = [
        '"""Generated SQL Server macro authority. Do not edit by hand."""',
        "",
        "from __future__ import annotations",
        "",
        "# Generated by tools/dbt_self_service/generate_sqlserver_macro_authority.py.",
        "# fmt: off",
        f"DBT_SQLSERVER_MACRO_AUTHORITY_GENERATOR_VERSION = {GENERATOR_VERSION!r}",
        f"DBT_SQLSERVER_DBT_CORE_VERSION = {DBT_CORE_VERSION!r}",
        f"DBT_SQLSERVER_ADAPTER_VERSION = {DBT_SQLSERVER_VERSION!r}",
        f"DBT_SQLSERVER_MANIFEST_SCHEMA = {MANIFEST_SCHEMA!r}",
        f"DBT_SQLSERVER_ADAPTER_TYPE = {ADAPTER_TYPE!r}",
        f"DBT_SQLSERVER_TRUSTED_PACKAGES = {TRUSTED_PACKAGES!r}",
        f"DBT_SQLSERVER_TRUSTED_ROOTS = {TRUSTED_ROOTS!r}",
        "DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS = (",
        *(_record_line(record) for record in framework),
        ")",
        "DBT_SQLSERVER_INVOCATION_EXTENSION_RECORDS = (",
        *(_record_line(record) for record in extension),
        ")",
        f"DBT_SQLSERVER_PROTECTED_MACRO_NAMES = {protected_names!r}",
        f"DBT_SQLSERVER_DISPATCH_FAMILY_WINNERS = {winners!r}",
        f"DBT_SQLSERVER_DISPATCH_CANDIDATE_UNIQUE_IDS = {dispatch_candidates!r}",
        f"DBT_DPONE_PUBLISH_HELPER_RECORD = {helper!r}",
        f"DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256 = {baseline_sha256!r}",
        "# fmt: on",
        "",
        "__all__ = [",
        '    "DBT_DPONE_PUBLISH_HELPER_RECORD",',
        '    "DBT_SQLSERVER_ADAPTER_TYPE",',
        '    "DBT_SQLSERVER_ADAPTER_VERSION",',
        '    "DBT_SQLSERVER_DBT_CORE_VERSION",',
        '    "DBT_SQLSERVER_DISPATCH_CANDIDATE_UNIQUE_IDS",',
        '    "DBT_SQLSERVER_DISPATCH_FAMILY_WINNERS",',
        '    "DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS",',
        '    "DBT_SQLSERVER_INVOCATION_EXTENSION_RECORDS",',
        '    "DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256",',
        '    "DBT_SQLSERVER_MACRO_AUTHORITY_GENERATOR_VERSION",',
        '    "DBT_SQLSERVER_MANIFEST_SCHEMA",',
        '    "DBT_SQLSERVER_PROTECTED_MACRO_NAMES",',
        '    "DBT_SQLSERVER_TRUSTED_PACKAGES",',
        '    "DBT_SQLSERVER_TRUSTED_ROOTS",',
        "]",
        "",
    ]
    return "\n".join(lines)


def _record_line(record: MacroRecord) -> str:
    return f"    {record!r},"


def _jsonable_record(record: MacroRecord) -> dict[str, object]:
    return {
        "unique_id": record[0],
        "package_name": record[1],
        "name": record[2],
        "macro_sql_sha256": record[3],
        "depends_on_macros": list(record[4]),
    }


def _fingerprint(value: Mapping[str, Any]) -> str:
    raw = json.dumps(dict(value), allow_nan=False, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode()
    return _sha256(raw)


def _sha256(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


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


if __name__ == "__main__":
    raise SystemExit(main())
