"""Interval-aware dpone DAG with catchup (canonical idempotent pattern).

Every DAG run loads exactly its own data interval:

- the compact pack templates ``DPONE_INTERVAL_START/END`` into the pod via
  ``KubernetesPodOperator.env_vars`` (rendered per task instance);
- ``dpone run`` resolves ``{{ data_interval_start }}`` / ``{{ data_interval_end }}``
  tokens inside the manifest, e.g. a ``mode: backfill`` chunk window with
  ``inner_mode: partition_replace``;
- re-running an interval (task clear, ``airflow dags backfill``, catchup)
  replaces exactly that interval's partition — no duplicates, no manual
  cleanup (functional data engineering).

Manifest counterpart::

    sink:
      strategy:
        mode: backfill
        backfill:
          inner_mode: partition_replace
          chunk:
            column: business_date
            from: "{{ data_interval_start }}"
            to: "{{ data_interval_end }}"
            step: 1d
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from dpone_airflow_pack import build_dpone_gitops_task_group_from_pack

PACK_PATH = "/opt/airflow/dpone-packs/orders_daily/airflow-pack.json"

with DAG(
    dag_id="dpone_orders_daily",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=True,  # historical intervals are loaded idempotently, one pod per interval
    max_active_runs=4,
    default_args={"retries": 2},
    tags=["dpone", "interval-aware"],
) as dag:
    tasks = build_dpone_gitops_task_group_from_pack(
        PACK_PATH,
        dag=dag,
        # Pools/priority stay a DAG-author concern:
        operator_overrides={"pool": "dpone_clickhouse", "retries": 2},
    )
