"""Pure fail-closed authority for pinned dbt SQL Server execution macros."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, TypeAlias, TypeGuard

from dpone.contracts.dbt_contract_validation import canonical_fingerprint
from dpone.contracts.dbt_sqlserver_macro_authority_baseline import (
    DBT_DPONE_PUBLISH_HELPER_RECORD,
    DBT_SQLSERVER_ADAPTER_TYPE,
    DBT_SQLSERVER_DBT_CORE_VERSION,
    DBT_SQLSERVER_DISPATCH_CANDIDATE_UNIQUE_IDS,
    DBT_SQLSERVER_DISPATCH_FAMILY_WINNERS,
    DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS,
    DBT_SQLSERVER_INVOCATION_EXTENSION_RECORDS,
    DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
    DBT_SQLSERVER_MACRO_AUTHORITY_GENERATOR_VERSION,
    DBT_SQLSERVER_MANIFEST_SCHEMA,
    DBT_SQLSERVER_PROTECTED_MACRO_NAMES,
    DBT_SQLSERVER_TRUSTED_PACKAGES,
    DBT_SQLSERVER_TRUSTED_ROOTS,
)

DBT_SQLSERVER_MACRO_AUTHORITY_INVALID = "DPONE_DBT_SQLSERVER_MACRO_AUTHORITY_INVALID"
DBT_SQLSERVER_MACRO_AUTHORITY_POLICY_PAYLOAD: Mapping[str, object] = {
    "generator_version": DBT_SQLSERVER_MACRO_AUTHORITY_GENERATOR_VERSION,
    "baseline_sha256": DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
}

MacroRecord: TypeAlias = tuple[str, str, str, str, tuple[str, ...]]
_EXPECTED_FRAMEWORK = {record[0]: record for record in DBT_SQLSERVER_FRAMEWORK_MACRO_RECORDS}
_EXPECTED_EXTENSION = {record[0]: record for record in DBT_SQLSERVER_INVOCATION_EXTENSION_RECORDS}
_EXPECTED_AUTHORITY = {**_EXPECTED_FRAMEWORK, **_EXPECTED_EXTENSION}
_EXPECTED_HELPER = DBT_DPONE_PUBLISH_HELPER_RECORD
_EXPECTED_DISPATCH_CANDIDATES = frozenset(DBT_SQLSERVER_DISPATCH_CANDIDATE_UNIQUE_IDS)
_EXPECTED_WINNERS = frozenset(winner for _family, winner in DBT_SQLSERVER_DISPATCH_FAMILY_WINNERS)
_MANIFEST_SCHEMA_URL = f"https://schemas.getdbt.com/dbt/manifest/{DBT_SQLSERVER_MANIFEST_SCHEMA}.json"


@dataclass(frozen=True, slots=True)
class DbtSqlServerMacroAuthorityIssue:
    """One stable content-free macro-authority violation."""

    code: str
    unique_id: str
    field: str
    expectation: str


@dataclass(frozen=True, slots=True)
class DbtSqlServerMacroAuthorityReport:
    """Immutable result for one manifest and exact selected graph."""

    baseline_sha256: str
    projection_sha256: str | None
    issues: tuple[DbtSqlServerMacroAuthorityIssue, ...]

    @property
    def passed(self) -> bool:
        """Return whether the manifest and selection match the pinned authority."""

        return not self.issues


def evaluate_dbt_sqlserver_macro_authority(
    manifest: Mapping[str, Any],
    selected_graph_unique_ids: tuple[str, ...],
) -> DbtSqlServerMacroAuthorityReport:
    """Validate pinned macro bodies, dependencies, dispatch and selected usage."""

    issues: list[DbtSqlServerMacroAuthorityIssue] = []
    if not isinstance(manifest, Mapping):
        return _report([_violation("manifest", "manifest", "be an object")], None)
    _metadata_issues(manifest, issues)
    macros = _section(manifest, "macros", issues)
    observed = _authority_records(macros, issues)
    _closure_issues(observed, issues)
    _cycle_issues(observed, issues)
    protected_candidates = _protected_candidate_projection(macros, issues)
    selected_projection, helper_used = _selected_dependency_projection(
        manifest,
        selected_graph_unique_ids,
        issues,
    )
    helper_projection: dict[str, object] | None = None
    if helper_used or _EXPECTED_HELPER[0] in macros:
        helper = _observed_record(_EXPECTED_HELPER[0], macros.get(_EXPECTED_HELPER[0]), issues)
        _record_drift_issues(_EXPECTED_HELPER, helper, issues)
        if helper == _EXPECTED_HELPER:
            helper_projection = _record_projection(helper)
    if issues:
        return _report(issues, None)
    projection_sha256 = canonical_fingerprint(
        {
            "baseline_sha256": DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
            "selected_node_macro_dependencies": selected_projection,
            "protected_dispatch_candidates": protected_candidates,
            "dpone_publish_helper": helper_projection,
        }
    )
    return _report((), projection_sha256)


def _metadata_issues(
    manifest: Mapping[str, Any],
    issues: list[DbtSqlServerMacroAuthorityIssue],
) -> None:
    metadata = manifest.get("metadata")
    if not isinstance(metadata, Mapping):
        issues.append(_violation("manifest", "manifest.metadata", "be an object"))
        return
    expected = {
        "dbt_schema_version": _MANIFEST_SCHEMA_URL,
        "dbt_version": DBT_SQLSERVER_DBT_CORE_VERSION,
        "adapter_type": DBT_SQLSERVER_ADAPTER_TYPE,
    }
    for field, value in expected.items():
        if metadata.get(field) != value:
            issues.append(_violation("manifest", f"metadata.{field}", f"equal the pinned value {value!r}"))


def _section(
    manifest: Mapping[str, Any],
    name: str,
    issues: list[DbtSqlServerMacroAuthorityIssue],
) -> Mapping[str, Any]:
    value = manifest.get(name)
    if isinstance(value, Mapping):
        return value
    issues.append(_violation("manifest", f"manifest.{name}", "be an object"))
    return {}


def _authority_records(
    macros: Mapping[str, Any],
    issues: list[DbtSqlServerMacroAuthorityIssue],
) -> dict[str, MacroRecord]:
    observed: dict[str, MacroRecord] = {}
    for unique_id, expected in sorted(_EXPECTED_AUTHORITY.items()):
        record = _observed_record(unique_id, macros.get(unique_id), issues)
        if record is not None:
            observed[unique_id] = record
        _record_drift_issues(expected, record, issues)
    return observed


def _observed_record(
    unique_id: str,
    raw: object,
    issues: list[DbtSqlServerMacroAuthorityIssue],
) -> MacroRecord | None:
    if not isinstance(raw, Mapping):
        issues.append(_violation(unique_id, "manifest.macros", "contain the pinned macro record"))
        return None
    observed_id = raw.get("unique_id")
    package_name = raw.get("package_name")
    name = raw.get("name")
    body = raw.get("macro_sql")
    if (
        not isinstance(observed_id, str)
        or not isinstance(package_name, str)
        or not isinstance(name, str)
        or not isinstance(body, str)
    ):
        issues.append(_violation(unique_id, "identity", "contain string identity and macro_sql fields"))
        return None
    if raw.get("resource_type") != "macro":
        issues.append(_violation(unique_id, "resource_type", "equal 'macro'"))
    if observed_id != unique_id or unique_id != f"macro.{package_name}.{name}":
        issues.append(_violation(unique_id, "unique_id", "match the manifest key, package and name"))
    depends_on = raw.get("depends_on")
    dependencies = depends_on.get("macros") if isinstance(depends_on, Mapping) else None
    if not _is_non_empty_string_sequence(dependencies):
        issues.append(_violation(unique_id, "depends_on.macros", "be an array of non-empty macro IDs"))
        return None
    normalized_dependencies = tuple(sorted(dependencies))
    if len(normalized_dependencies) != len(set(normalized_dependencies)):
        issues.append(_violation(unique_id, "depends_on.macros", "contain no duplicate macro IDs"))
        return None
    return (
        unique_id,
        package_name,
        name,
        "sha256:" + hashlib.sha256(body.encode("utf-8")).hexdigest(),
        normalized_dependencies,
    )


def _record_drift_issues(
    expected: MacroRecord,
    observed: MacroRecord | None,
    issues: list[DbtSqlServerMacroAuthorityIssue],
) -> None:
    if observed is None:
        return
    fields = ("unique_id", "package_name", "name", "macro_sql_sha256", "depends_on.macros")
    for index, field in enumerate(fields):
        if observed[index] != expected[index]:
            issues.append(_violation(expected[0], field, "equal the checked-in pinned record"))


def _closure_issues(
    observed: Mapping[str, MacroRecord],
    issues: list[DbtSqlServerMacroAuthorityIssue],
) -> None:
    framework_ids = set(_EXPECTED_FRAMEWORK)
    visited: set[str] = set()

    def visit(unique_id: str) -> None:
        if unique_id in visited:
            return
        visited.add(unique_id)
        record = observed.get(unique_id)
        if record is None:
            return
        for dependency in record[4]:
            if dependency not in framework_ids:
                issues.append(_violation(unique_id, "depends_on.macros", "remain inside the pinned framework closure"))
                continue
            visit(dependency)

    for root in DBT_SQLSERVER_TRUSTED_ROOTS:
        visit(root)
    if visited != framework_ids:
        issues.append(_violation("manifest", "manifest.macros", "reconstruct the exact pinned framework closure"))
    authority_ids = set(_EXPECTED_AUTHORITY)
    for unique_id in _EXPECTED_EXTENSION:
        record = observed.get(unique_id)
        if record is not None and any(dependency not in authority_ids for dependency in record[4]):
            issues.append(
                _violation(
                    unique_id,
                    "depends_on.macros",
                    "terminate in the pinned macro authority union",
                )
            )


def _cycle_issues(
    observed: Mapping[str, MacroRecord],
    issues: list[DbtSqlServerMacroAuthorityIssue],
) -> None:
    complete: set[str] = set()
    visiting: list[str] = []

    def visit(unique_id: str) -> None:
        if unique_id in visiting:
            issues.append(_violation(unique_id, "depends_on.macros", "form an acyclic macro authority graph"))
            return
        if unique_id in complete:
            return
        visiting.append(unique_id)
        record = observed.get(unique_id)
        if record is not None:
            for dependency in record[4]:
                if dependency in _EXPECTED_AUTHORITY:
                    visit(dependency)
        visiting.pop()
        complete.add(unique_id)

    for unique_id in sorted(_EXPECTED_AUTHORITY):
        visit(unique_id)


def _protected_candidate_projection(
    macros: Mapping[str, Any],
    issues: list[DbtSqlServerMacroAuthorityIssue],
) -> list[dict[str, str]]:
    projection: list[dict[str, str]] = []
    observed_candidates: set[str] = set()
    for raw_key, raw in sorted(macros.items(), key=lambda item: str(item[0])):
        if not isinstance(raw_key, str) or not isinstance(raw, Mapping):
            continue
        name = raw.get("name")
        if not isinstance(name, str) or name not in DBT_SQLSERVER_PROTECTED_MACRO_NAMES:
            continue
        package_name = raw.get("package_name")
        observed_id = raw.get("unique_id")
        if package_name not in DBT_SQLSERVER_TRUSTED_PACKAGES:
            issues.append(_violation(raw_key, "name", "not shadow a protected dbt dispatch candidate"))
            continue
        if observed_id != raw_key or raw_key != f"macro.{package_name}.{name}":
            issues.append(_violation(raw_key, "unique_id", "match the protected candidate package and name"))
            continue
        observed_candidates.add(raw_key)
        if raw_key not in _EXPECTED_DISPATCH_CANDIDATES:
            issues.append(_violation(raw_key, "name", "belong to the exact checked-in dispatch candidate set"))
            continue
        projection.append({"unique_id": raw_key, "package_name": package_name, "name": name})
    for missing in sorted(_EXPECTED_DISPATCH_CANDIDATES - observed_candidates):
        issues.append(_violation(missing, "manifest.macros", "contain the exact checked-in dispatch candidate"))
    for missing_winner in sorted(_EXPECTED_WINNERS - observed_candidates):
        issues.append(_violation(missing_winner, "manifest.macros", "contain the pinned dispatch-family winner"))
    return projection


def _selected_dependency_projection(
    manifest: Mapping[str, Any],
    selected_graph_unique_ids: object,
    issues: list[DbtSqlServerMacroAuthorityIssue],
) -> tuple[list[dict[str, object]], bool]:
    if (
        not isinstance(selected_graph_unique_ids, tuple)
        or not selected_graph_unique_ids
        or any(not isinstance(item, str) or not item for item in selected_graph_unique_ids)
        or len(selected_graph_unique_ids) != len(set(selected_graph_unique_ids))
    ):
        issues.append(
            _violation(
                "selected_graph_unique_ids",
                "selected_graph_unique_ids",
                "be a non-empty tuple of unique IDs",
            )
        )
        return [], False
    raw_nodes = manifest.get("nodes")
    raw_unit_tests = manifest.get("unit_tests")
    nodes = raw_nodes if isinstance(raw_nodes, Mapping) else {}
    unit_tests = raw_unit_tests if isinstance(raw_unit_tests, Mapping) else {}
    if not isinstance(raw_nodes, Mapping):
        issues.append(_violation("manifest", "manifest.nodes", "be an object"))
    if not isinstance(raw_unit_tests, Mapping):
        issues.append(_violation("manifest", "manifest.unit_tests", "be an object"))
    for collision in sorted(set(nodes) & set(unit_tests), key=str):
        issues.append(
            _violation(
                str(collision),
                "manifest.unit_tests",
                "not duplicate an ID from manifest.nodes",
            )
        )
    allowed = set(_EXPECTED_AUTHORITY)
    helper_used = False
    projection: list[dict[str, object]] = []
    for unique_id in sorted(selected_graph_unique_ids):
        resource_type = unique_id.partition(".")[0]
        section = unit_tests if resource_type == "unit_test" else nodes
        node = section.get(unique_id)
        if (
            not isinstance(node, Mapping)
            or node.get("unique_id") != unique_id
            or node.get("resource_type") != resource_type
        ):
            issues.append(_violation(unique_id, "manifest.nodes", "contain the exact selected executable node"))
            continue
        depends_on = node.get("depends_on")
        dependencies = depends_on.get("macros") if isinstance(depends_on, Mapping) else None
        if not _is_non_empty_string_sequence(dependencies):
            issues.append(_violation(unique_id, "depends_on.macros", "be an array of non-empty macro IDs"))
            continue
        normalized = tuple(sorted(dependencies))
        if len(normalized) != len(set(normalized)):
            issues.append(_violation(unique_id, "depends_on.macros", "contain no duplicate macro IDs"))
            continue
        for dependency in normalized:
            if dependency == _EXPECTED_HELPER[0]:
                helper_used = True
            elif dependency not in allowed:
                issues.append(
                    _violation(
                        unique_id,
                        "depends_on.macros",
                        "reference only the pinned framework, invocation extension or metadata helper",
                    )
                )
        projection.append({"unique_id": unique_id, "depends_on_macros": list(normalized)})
    return projection, helper_used


def _record_projection(record: MacroRecord) -> dict[str, object]:
    return {
        "unique_id": record[0],
        "package_name": record[1],
        "name": record[2],
        "macro_sql_sha256": record[3],
        "depends_on_macros": list(record[4]),
    }


def _report(
    issues: Sequence[DbtSqlServerMacroAuthorityIssue],
    projection_sha256: str | None,
) -> DbtSqlServerMacroAuthorityReport:
    unique = {(issue.unique_id, issue.field, issue.expectation): issue for issue in issues}
    ordered = tuple(unique[key] for key in sorted(unique))
    return DbtSqlServerMacroAuthorityReport(
        baseline_sha256=DBT_SQLSERVER_MACRO_AUTHORITY_BASELINE_SHA256,
        projection_sha256=projection_sha256 if not ordered else None,
        issues=ordered,
    )


def _violation(unique_id: str, field: str, expectation: str) -> DbtSqlServerMacroAuthorityIssue:
    return DbtSqlServerMacroAuthorityIssue(
        code=DBT_SQLSERVER_MACRO_AUTHORITY_INVALID,
        unique_id=unique_id,
        field=field,
        expectation=expectation,
    )


def _is_non_empty_string_sequence(value: object) -> TypeGuard[Sequence[str]]:
    return (
        isinstance(value, Sequence)
        and not isinstance(value, (str, bytes, bytearray))
        and all(isinstance(item, str) and bool(item) for item in value)
    )
