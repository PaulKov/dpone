"""Pure validation at the SQL Server ``dbt_project.yml`` adapter boundary.

The caller owns bounded, duplicate-rejecting YAML acquisition and passes the
already parsed root mapping here. Keeping acquisition outside this module
avoids filesystem work and layer-crossing imports on the base import path.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass

from dpone.contracts.dbt_publish_models import DbtPublishIssue
from dpone.contracts.dbt_sqlserver_policy import (
    DBT_SQLSERVER_REQUIRED_PROJECT_FLAGS as REQUIRED_SQLSERVER_PROJECT_FLAGS,
)
from dpone.contracts.dbt_sqlserver_policy import (
    MAX_DBT_SQLSERVER_PROJECT_YAML_BYTES,
)

PROJECT_POLICY_INVALID_CODE = "DPONE_DBT_SQLSERVER_PROJECT_POLICY_INVALID"
DEFAULT_DBT_PROJECT_FILE = "dbt_project.yml"

_MISSING = object()
_TEMPLATE_MARKERS = ("{{", "{%", "{#")


@dataclass(frozen=True, slots=True)
class DbtSqlserverProjectPolicyReport:
    """Result of validating the required literal SQL Server adapter flags.

    ``required_project_flags`` is present only after every required flag has
    passed. It is the canonical immutable mapping, rather than a reference to
    caller-owned YAML data, so downstream pack identity cannot be changed by a
    later mutation of the parsed project object.
    """

    required_project_flags: Mapping[str, bool] | None
    issues: tuple[DbtPublishIssue, ...]

    @property
    def passed(self) -> bool:
        """Return whether the project is safe to pass to later dbt stages."""

        return not self.issues


def validate_sqlserver_project_policy(
    project: Mapping[str, object],
    *,
    project_file: str = DEFAULT_DBT_PROJECT_FILE,
) -> DbtSqlserverProjectPolicyReport:
    """Validate SQL Server v1 flags and the pinned macro-dispatch boundary.

    Extra dbt flags remain valid. A non-empty or malformed top-level
    ``dispatch`` block is rejected because macro resolution is part of the
    certified adapter surface. Issues are deterministic and never include a
    raw YAML value, environment name, endpoint, or secret.
    """

    raw_flags = project.get("flags")
    flags = raw_flags if isinstance(raw_flags, Mapping) else {}
    flag_issues = tuple(
        issue
        for flag, expected in REQUIRED_SQLSERVER_PROJECT_FLAGS.items()
        if (
            issue := _flag_issue(
                flag,
                flags.get(flag, _MISSING),
                expected=expected,
                project_file=project_file,
            )
        )
        is not None
    )
    dispatch_issue = _dispatch_issue(
        project.get("dispatch", _MISSING),
        project_file=project_file,
    )
    issues = (
        *flag_issues,
        *((dispatch_issue,) if dispatch_issue is not None else ()),
    )
    return DbtSqlserverProjectPolicyReport(
        required_project_flags=None if issues else REQUIRED_SQLSERVER_PROJECT_FLAGS,
        issues=issues,
    )


def _dispatch_issue(
    value: object,
    *,
    project_file: str,
) -> DbtPublishIssue | None:
    if value is _MISSING or value is None:
        return None
    if isinstance(value, Sequence) and not isinstance(value, str | bytes) and not value:
        return None
    return DbtPublishIssue(
        code=PROJECT_POLICY_INVALID_CODE,
        message=(
            "SQL Server v1 does not admit a non-empty or malformed top-level "
            "`dispatch` configuration because adapter macro resolution must "
            "remain pinned."
        ),
        path=project_file,
        remediation=("Remove top-level `dispatch` overrides and use the pinned dbt-sqlserver macro set, then retry."),
    )


def sqlserver_project_policy_input_issue(
    reason: str,
    *,
    project_file: str = DEFAULT_DBT_PROJECT_FILE,
) -> DbtPublishIssue:
    """Translate a bounded-reader failure into one safe public policy issue.

    ``reason`` is expected to be a machine code such as ``duplicate_key`` from
    the authorized YAML acquisition layer. Unknown values deliberately map to
    a generic message; raw parser diagnostics may contain project content and
    must not cross the public error boundary.
    """

    if reason == "duplicate_key":
        message = (
            f"{project_file} contains a duplicate YAML key, so required "
            "SQL Server adapter flags cannot be validated safely"
        )
        remediation = (
            "Remove the duplicate key, define each required flag exactly once under top-level `flags`, and retry."
        )
    else:
        message = f"{project_file} is not bounded, unique-key UTF-8 YAML with a top-level mapping"
        remediation = (
            "Repair `dbt_project.yml`, define the required literal booleans under top-level `flags`, and retry."
        )
    return DbtPublishIssue(
        code=PROJECT_POLICY_INVALID_CODE,
        message=message,
        path=project_file,
        remediation=remediation,
    )


def _flag_issue(
    flag: str,
    value: object,
    *,
    expected: bool,
    project_file: str,
) -> DbtPublishIssue | None:
    if type(value) is bool and value is expected:
        return None
    expected_literal = str(expected).lower()
    if value is _MISSING:
        detail = "is missing"
    elif type(value) is bool:
        detail = f"found {str(value).lower()}"
    else:
        detail = f"found {_value_kind(value)}"
    return DbtPublishIssue(
        code=PROJECT_POLICY_INVALID_CODE,
        message=(
            f"Required SQL Server dbt project flag 'flags.{flag}' {detail}; "
            f"expected literal YAML boolean {expected_literal}."
        ),
        path=project_file,
        remediation=(f"Set `flags.{flag}: {expected_literal}` as an unquoted YAML boolean, then retry."),
    )


def _value_kind(value: object) -> str:
    if isinstance(value, str):
        return "a template" if any(marker in value for marker in _TEMPLATE_MARKERS) else "a string"
    if value is None:
        return "null"
    if isinstance(value, int):
        return "an integer"
    if isinstance(value, float):
        return "a number"
    if isinstance(value, Mapping):
        return "a mapping"
    if isinstance(value, list | tuple | set | frozenset):
        return "a collection"
    return "an unsupported value"


__all__ = [
    "DEFAULT_DBT_PROJECT_FILE",
    "PROJECT_POLICY_INVALID_CODE",
    "REQUIRED_SQLSERVER_PROJECT_FLAGS",
    "MAX_DBT_SQLSERVER_PROJECT_YAML_BYTES",
    "DbtSqlserverProjectPolicyReport",
    "sqlserver_project_policy_input_issue",
    "validate_sqlserver_project_policy",
]
