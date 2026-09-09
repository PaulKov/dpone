from __future__ import annotations

import pytest

from dpone.contracts.dbt_publishing import DbtPublishingError
from dpone.contracts.dbt_sqlserver_policy import (
    DBT_SQLSERVER_REQUIRED_PROJECT_FLAGS,
    DbtSqlServerAdapterPolicy,
    DbtSqlServerRuntimePolicy,
)


def test_runtime_policy_derives_strict_timeout_hierarchy() -> None:
    policy = DbtSqlServerRuntimePolicy.for_process_timeout(3600)

    assert policy.to_dict() == {
        "schema": "dpone.dbt-sqlserver-runtime-policy.v1",
        "backend": "pyodbc",
        "retries": 1,
        "login_timeout_seconds": 15,
        "query_timeout_seconds": 3300,
        "adapter_runtime_sha256": policy.adapter_runtime_sha256,
    }
    assert policy.airflow_execution_timeout_seconds(3600) == 3900


@pytest.mark.parametrize("timeout_seconds", [True, 0, 599, 86_401])
def test_runtime_policy_rejects_process_timeout_outside_preview_bounds(
    timeout_seconds: object,
) -> None:
    with pytest.raises(
        DbtPublishingError,
        match="dbt process timeout must be an integer from 600 through 86400",
    ):
        DbtSqlServerRuntimePolicy.for_process_timeout(timeout_seconds)


def test_runtime_policy_strict_parser_rejects_retry_tampering() -> None:
    payload = DbtSqlServerRuntimePolicy.for_process_timeout(3600).to_dict()
    payload["retries"] = 2

    with pytest.raises(DbtPublishingError, match="retries must equal 1"):
        DbtSqlServerRuntimePolicy.from_mapping(payload)


@pytest.mark.parametrize("field", ["schema", "backend", "query_timeout_seconds"])
def test_runtime_policy_strict_parser_rejects_missing_fields(field: str) -> None:
    payload = DbtSqlServerRuntimePolicy.for_process_timeout(3600).to_dict()
    del payload[field]

    with pytest.raises(DbtPublishingError, match="adapter_runtime must contain exactly"):
        DbtSqlServerRuntimePolicy.from_mapping(payload)


def test_runtime_policy_strict_parser_rejects_unknown_fields() -> None:
    payload = DbtSqlServerRuntimePolicy.for_process_timeout(3600).to_dict()
    payload["implicit_adapter_default"] = True

    with pytest.raises(DbtPublishingError, match="adapter_runtime must contain exactly"):
        DbtSqlServerRuntimePolicy.from_mapping(payload)


@pytest.mark.parametrize(
    ("field", "value", "message"),
    (
        ("backend", "mssql_python", "backend must equal pyodbc"),
        ("retries", True, "retries must equal 1"),
        ("login_timeout_seconds", 16, "login timeout must equal 15 seconds"),
        ("query_timeout_seconds", 15, "query timeout must exceed login timeout"),
    ),
)
def test_runtime_policy_rejects_effective_adapter_value_mutation(
    field: str,
    value: object,
    message: str,
) -> None:
    payload = DbtSqlServerRuntimePolicy.for_process_timeout(3600).to_dict()
    payload[field] = value

    with pytest.raises(DbtPublishingError, match=message):
        DbtSqlServerRuntimePolicy.from_mapping(payload)


@pytest.mark.parametrize("value", (True, "3300", None, [3300]))
def test_runtime_policy_rejects_malformed_query_timeout_with_stable_error(
    value: object,
) -> None:
    payload = DbtSqlServerRuntimePolicy.for_process_timeout(3600).to_dict()
    payload["query_timeout_seconds"] = value

    with pytest.raises(
        DbtPublishingError,
        match="query timeout must be an integer",
    ):
        DbtSqlServerRuntimePolicy.from_mapping(payload)


def test_runtime_policy_rejects_process_budget_mismatch_at_airflow_projection() -> None:
    policy = DbtSqlServerRuntimePolicy.for_process_timeout(3600)

    with pytest.raises(
        DbtPublishingError,
        match="runtime query timeout differs from the dbt process timeout",
    ):
        policy.airflow_execution_timeout_seconds(600)


def test_adapter_policy_binds_exact_required_project_flags() -> None:
    policy = DbtSqlServerAdapterPolicy.canonical()

    assert policy.required_project_flags == DBT_SQLSERVER_REQUIRED_PROJECT_FLAGS
    assert DbtSqlServerAdapterPolicy.from_mapping(policy.to_dict()) == policy


def test_adapter_policy_rejects_flag_tampering() -> None:
    payload = DbtSqlServerAdapterPolicy.canonical().to_dict()
    flags = dict(payload["required_project_flags"])
    flags["dbt_sqlserver_use_dbt_transactions"] = False
    payload["required_project_flags"] = flags

    with pytest.raises(
        DbtPublishingError,
        match="required project flags differ from the certified policy",
    ):
        DbtSqlServerAdapterPolicy.from_mapping(payload)


@pytest.mark.parametrize("mutation", ["missing", "extra"])
def test_adapter_policy_strict_parser_rejects_shape_mutation(
    mutation: str,
) -> None:
    payload = DbtSqlServerAdapterPolicy.canonical().to_dict()
    if mutation == "missing":
        del payload["required_project_flags"]
    else:
        payload["implicit_default"] = True

    with pytest.raises(DbtPublishingError, match="adapter_policy must contain exactly"):
        DbtSqlServerAdapterPolicy.from_mapping(payload)
