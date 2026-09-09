"""Interval-aware Airflow environment templating for dpone pods.

Airflow templates ``KubernetesPodOperator.env_vars`` at task-instance render
time, so the generated artifacts stay static while every pod receives the
concrete DAG-run interval. The pod-side counterpart of this contract is
:mod:`dpone.contracts.run_interval`, consumed by ``dpone run``.

Manual DAG runs without a data interval render empty strings, which the
runtime treats as "no interval" — behavior stays identical to non-scheduled
CLI runs.
"""

from __future__ import annotations

from dpone.contracts.run_interval import (
    DAG_ID_ENV,
    DAG_RUN_ID_ENV,
    INTERVAL_END_ENV,
    INTERVAL_START_ENV,
    LOGICAL_DATE_ENV,
    PARTITION_KEY_ENV,
    RUN_INTERVAL_ENV_NAMES,
    TRY_NUMBER_ENV,
    run_interval_from_env,
)

AIRFLOW_INTERVAL_ENV_TEMPLATES: dict[str, str] = {
    DAG_ID_ENV: "{{ dag.dag_id }}",
    DAG_RUN_ID_ENV: "{{ run_id }}",
    TRY_NUMBER_ENV: "{{ ti.try_number | string }}",
    LOGICAL_DATE_ENV: "{{ logical_date | ts if logical_date is defined and logical_date else '' }}",
    INTERVAL_START_ENV: "{{ data_interval_start | ts if data_interval_start is defined and data_interval_start else '' }}",
    INTERVAL_END_ENV: "{{ data_interval_end | ts if data_interval_end is defined and data_interval_end else '' }}",
    PARTITION_KEY_ENV: (
        "{{ dag_run.partition_key if dag_run is defined and dag_run "
        "and dag_run.partition_key is defined and dag_run.partition_key else '' }}"
    ),
}


def airflow_interval_env_vars() -> dict[str, str]:
    """Fresh copy of the templated env contract for KPO ``env_vars``."""

    return dict(AIRFLOW_INTERVAL_ENV_TEMPLATES)


__all__ = [
    "AIRFLOW_INTERVAL_ENV_TEMPLATES",
    "RUN_INTERVAL_ENV_NAMES",
    "airflow_interval_env_vars",
    "run_interval_from_env",
]
