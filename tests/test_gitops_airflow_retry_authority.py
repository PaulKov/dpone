from __future__ import annotations

from copy import deepcopy

import pytest

from dpone.gitops.airflow_retry_authority import certify_airflow_retry_authority

_MSSQL_ALIASES = (
    "mssql",
    "MSSQL",
    "microsoft mssql",
    "microsoft_mssql",
    "odbc",
    "sqlserver",
    "sql_server",
    "sql-server",
)


def _eligible_process() -> dict[str, object]:
    return {
        "source": {
            "type": "postgres",
            "options": {
                "incremental_strategy": "xmin",
                "xmin_execution": {"mode": "initial", "handoff_id": "orders_v1"},
            },
        },
        "sink": {
            "type": "mssql",
            "strategy": {
                "mode": "backfill",
                "unique_key": ["id"],
                "backfill": {
                    "inner_mode": "incremental_merge",
                    "retry_policy": "non_committed",
                    "state": {
                        "backend": "audit_schema",
                        "require_distributed_lock": True,
                    },
                },
            },
        },
        "state": {
            "type": "mssql",
            "atomicity": "target_atomic",
            "provisioning": "external",
        },
    }


def _eligible_shadow_process() -> dict[str, object]:
    process = deepcopy(_eligible_process())
    strategy = process["sink"]["strategy"]  # type: ignore[index]
    strategy["only_new_rows"] = False  # type: ignore[index]
    backfill = strategy["backfill"]  # type: ignore[index]
    backfill["inner_mode"] = "incremental_append"  # type: ignore[index]
    backfill["publication"] = {  # type: ignore[index]
        "mode": "shadow_swap",
        "retain_backup": True,
    }
    return process


def test_exact_xmin_initial_mssql_route_receives_retry_authority() -> None:
    assert certify_airflow_retry_authority((_eligible_process(),)) == {
        "schema": "dpone.airflow-retry-authority.v1",
        "mode": "postgres_xmin_initial_mssql_target_atomic_v1",
        "max_task_retries": 3,
    }


@pytest.mark.parametrize("endpoint_type", _MSSQL_ALIASES)
def test_endpoint_aliases_preserve_exact_retry_authority(endpoint_type: str) -> None:
    process = _eligible_process()
    process["source"]["type"] = "PostgreSQL"  # type: ignore[index]
    process["sink"]["type"] = endpoint_type  # type: ignore[index]
    process["state"]["type"] = endpoint_type  # type: ignore[index]

    assert certify_airflow_retry_authority((process,)) == {
        "schema": "dpone.airflow-retry-authority.v1",
        "mode": "postgres_xmin_initial_mssql_target_atomic_v1",
        "max_task_retries": 3,
    }


def test_exact_shadow_xmin_initial_route_receives_retry_authority() -> None:
    assert certify_airflow_retry_authority((_eligible_shadow_process(),)) == {
        "schema": "dpone.airflow-retry-authority.v1",
        "mode": "postgres_xmin_initial_mssql_target_atomic_v1",
        "max_task_retries": 3,
    }


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("sink", "strategy", "only_new_rows"), True),
        (("sink", "strategy", "only_new_rows"), None),
        (("sink", "strategy", "backfill", "publication"), None),
        (("sink", "strategy", "backfill", "publication", "mode"), "direct"),
        (("sink", "strategy", "backfill", "publication", "retain_backup"), False),
    ],
)
def test_shadow_retry_authority_rejects_incomplete_publication(path: tuple[str, ...], value: object) -> None:
    process = _eligible_shadow_process()
    target = process
    for field in path[:-1]:
        target = target[field]  # type: ignore[assignment,index]
    target[path[-1]] = value  # type: ignore[index]

    assert certify_airflow_retry_authority((process,)) is None


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("source", "type"), "mysql"),
        (("source", "options", "incremental_strategy"), "updated_at"),
        (("source", "options", "xmin_execution", "mode"), "incremental"),
        (("sink", "type"), "postgres"),
        (("sink", "strategy", "mode"), "incremental"),
        (("sink", "strategy", "unique_key"), []),
        (("sink", "strategy", "backfill", "inner_mode"), "full_refresh"),
        (("sink", "strategy", "backfill", "retry_policy"), "failed_only"),
        (("sink", "strategy", "backfill", "state", "backend"), "file"),
        (("sink", "strategy", "backfill", "state", "require_distributed_lock"), False),
        (("state", "type"), "postgres"),
        (("state", "atomicity"), "best_effort"),
        (("state", "provisioning"), "managed"),
    ],
)
def test_near_miss_route_remains_uncertified(path: tuple[str, ...], value: object) -> None:
    process = deepcopy(_eligible_process())
    target = process
    for field in path[:-1]:
        target = target[field]  # type: ignore[assignment,index]
    target[path[-1]] = value  # type: ignore[index]

    assert certify_airflow_retry_authority((process,)) is None


def test_all_processes_must_be_retry_safe() -> None:
    unsafe = deepcopy(_eligible_process())
    unsafe["source"]["options"]["xmin_execution"]["mode"] = "incremental"  # type: ignore[index]

    assert certify_airflow_retry_authority((_eligible_process(), unsafe)) is None
    assert certify_airflow_retry_authority(()) is None
