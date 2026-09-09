from __future__ import annotations

import ast
import importlib
import os
from collections.abc import Mapping
from pathlib import Path

import pytest

import dpone.adapters.dbt_sqlserver_project_policy as project_policy
from dpone.adapters.dbt_sqlserver_project_policy import (
    PROJECT_POLICY_INVALID_CODE,
    REQUIRED_SQLSERVER_PROJECT_FLAGS,
    sqlserver_project_policy_input_issue,
    validate_sqlserver_project_policy,
)
from dpone.readiness.dbt_sqlserver_project_policy import (
    DbtSqlserverProjectPolicyValidator,
)


def _project(flags: Mapping[str, object]) -> dict[str, object]:
    return {"name": "analytics", "flags": dict(flags)}


def _valid_project_yaml() -> str:
    rendered_flags = "\n".join(
        f"  {name}: {str(value).lower()}" for name, value in REQUIRED_SQLSERVER_PROJECT_FLAGS.items()
    )
    return f"name: analytics\nflags:\n{rendered_flags}\n"


def test_exact_literal_project_flags_are_accepted() -> None:
    report = validate_sqlserver_project_policy(
        _project(REQUIRED_SQLSERVER_PROJECT_FLAGS),
        project_file="analytics/dbt_project.yml",
    )

    assert report.passed
    assert report.required_project_flags == REQUIRED_SQLSERVER_PROJECT_FLAGS
    assert report.issues == ()


@pytest.mark.parametrize(
    ("flag", "value", "expected_detail"),
    [
        ("dbt_sqlserver_use_native_string_types", None, "is missing"),
        ("dbt_sqlserver_use_dbt_transactions", False, "found false"),
        ("dbt_sqlserver_use_default_schema_concat", 1, "found an integer"),
        ("dbt_sqlserver_enable_safe_type_expansion", True, "found true"),
        (
            "dbt_sqlserver_use_native_string_types",
            "{{ env_var('DBT_NATIVE_TYPES') }}",
            "found a template",
        ),
        ("dbt_sqlserver_use_dbt_transactions", "true", "found a string"),
    ],
)
def test_invalid_flag_values_return_stable_actionable_issues(
    flag: str,
    value: object,
    expected_detail: str,
) -> None:
    flags = dict(REQUIRED_SQLSERVER_PROJECT_FLAGS)
    if value is None:
        del flags[flag]
    else:
        flags[flag] = value

    report = validate_sqlserver_project_policy(
        _project(flags),
        project_file="analytics/dbt_project.yml",
    )

    assert not report.passed
    assert report.required_project_flags is None
    assert len(report.issues) == 1
    issue = report.issues[0]
    assert issue.code == PROJECT_POLICY_INVALID_CODE
    assert issue.path == "analytics/dbt_project.yml"
    assert f"flags.{flag}" in issue.message
    assert expected_detail in issue.message
    assert issue.remediation == (
        f"Set `flags.{flag}: "
        f"{str(REQUIRED_SQLSERVER_PROJECT_FLAGS[flag]).lower()}` "
        "as an unquoted YAML boolean, then retry."
    )


@pytest.mark.parametrize("flags", [None, [], "templated", True])
def test_missing_or_non_mapping_flags_report_every_required_flag_in_canonical_order(
    flags: object,
) -> None:
    project = {"name": "analytics"}
    if flags is not None:
        project["flags"] = flags

    report = validate_sqlserver_project_policy(project)

    assert [issue.code for issue in report.issues] == [
        PROJECT_POLICY_INVALID_CODE,
        PROJECT_POLICY_INVALID_CODE,
        PROJECT_POLICY_INVALID_CODE,
        PROJECT_POLICY_INVALID_CODE,
    ]
    assert [issue.message.split("'")[1] for issue in report.issues] == [
        f"flags.{flag}" for flag in REQUIRED_SQLSERVER_PROJECT_FLAGS
    ]


def test_additional_project_flags_do_not_change_the_required_policy() -> None:
    flags = dict(REQUIRED_SQLSERVER_PROJECT_FLAGS)
    flags["send_anonymous_usage_stats"] = False

    report = validate_sqlserver_project_policy(_project(flags))

    assert report.passed
    assert dict(report.required_project_flags or {}) == dict(REQUIRED_SQLSERVER_PROJECT_FLAGS)


@pytest.mark.parametrize(
    "dispatch",
    [
        [{"macro_namespace": "dbt", "search_order": ["analytics", "dbt"]}],
        {"dbt": ["analytics", "dbt"]},
        "analytics",
    ],
)
def test_project_dispatch_overrides_are_rejected(dispatch: object) -> None:
    project = _project(REQUIRED_SQLSERVER_PROJECT_FLAGS)
    project["dispatch"] = dispatch

    report = validate_sqlserver_project_policy(project)

    assert not report.passed
    assert [issue.code for issue in report.issues] == [
        PROJECT_POLICY_INVALID_CODE,
    ]
    issue = report.issues[0]
    assert "dispatch" in issue.message
    assert issue.path == "dbt_project.yml"
    assert issue.remediation


@pytest.mark.parametrize("dispatch", [None, []])
def test_absent_or_empty_project_dispatch_is_accepted(dispatch: object) -> None:
    project = _project(REQUIRED_SQLSERVER_PROJECT_FLAGS)
    project["dispatch"] = dispatch

    report = validate_sqlserver_project_policy(project)

    assert report.passed


def test_duplicate_yaml_input_maps_to_safe_stable_issue() -> None:
    issue = sqlserver_project_policy_input_issue(
        "duplicate_key",
        project_file="analytics/dbt_project.yml",
    )

    assert issue.code == PROJECT_POLICY_INVALID_CODE
    assert issue.path == "analytics/dbt_project.yml"
    assert "duplicate YAML key" in issue.message
    assert issue.remediation is not None
    assert "exactly once" in issue.remediation
    assert "top-level `flags`" in issue.remediation


def test_unknown_input_failure_does_not_echo_parser_or_file_content() -> None:
    secret_like_reason = "password=should-not-be-rendered"

    issue = sqlserver_project_policy_input_issue(secret_like_reason)

    assert secret_like_reason not in issue.message
    assert secret_like_reason not in (issue.remediation or "")


def test_authoring_validator_reads_the_exact_project_file(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text(
        _valid_project_yaml(),
        encoding="utf-8",
    )

    assert DbtSqlserverProjectPolicyValidator().validate_root(tmp_path) == ()


def test_authoring_validator_rejects_duplicate_yaml_keys(tmp_path: Path) -> None:
    (tmp_path / "dbt_project.yml").write_text(
        _valid_project_yaml() + "\nflags:\n  dbt_sqlserver_use_native_string_types: true\n",
        encoding="utf-8",
    )

    issues = DbtSqlserverProjectPolicyValidator().validate_root(tmp_path)

    assert len(issues) == 1
    assert issues[0].code == PROJECT_POLICY_INVALID_CODE
    assert "duplicate YAML key" in issues[0].message


def test_authoring_validator_rejects_symlinked_project_file(
    tmp_path: Path,
) -> None:
    target = tmp_path / "real-project.yml"
    target.write_text(_valid_project_yaml(), encoding="utf-8")
    (tmp_path / "dbt_project.yml").symlink_to(target)

    issues = DbtSqlserverProjectPolicyValidator().validate_root(tmp_path)

    assert len(issues) == 1
    assert issues[0].code == PROJECT_POLICY_INVALID_CODE
    assert "bounded, unique-key UTF-8 YAML" in issues[0].message


def test_module_has_no_yaml_filesystem_or_forbidden_layer_dependency(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    source = Path(project_policy.__file__).read_text(encoding="utf-8")

    def fail_io(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("project-policy import performed filesystem I/O")

    monkeypatch.setattr(Path, "read_bytes", fail_io)
    monkeypatch.setattr(Path, "read_text", fail_io)
    monkeypatch.setattr(os, "open", fail_io)

    importlib.reload(project_policy)

    imports = {
        alias.name.split(".", 1)[0]
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imports.update(
        (node.module or "").split(".", 1)[0] for node in ast.walk(ast.parse(source)) if isinstance(node, ast.ImportFrom)
    )
    assert imports.isdisjoint({"os", "pathlib", "yaml"})
    assert "dpone.manifest" not in source
    assert "dpone.runtime" not in source
