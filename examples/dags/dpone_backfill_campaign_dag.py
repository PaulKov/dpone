"""Parameterized dpone backfill campaign DAG (manually triggered).

Use this pattern for large one-off historical reloads that should not be
modeled as N scheduler intervals: one DAG run drives one resumable chunked
campaign via ``dpone backfill run --execute``.

- Trigger with config, e.g. ``{"from": "2024-01-01", "to": "2024-12-31", "step": "1w"}``.
- The campaign is resumable: re-triggering with the same window continues
  from the first non-committed chunk (durable ledger in ``.dpone/backfill``).
- Progress and verification land in the XCom summary (``backfill`` section)
  and in the runtime evidence file.
"""

from __future__ import annotations

import pendulum
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator

MANIFEST = "dpone_workloads/manifests/orders_backfill.yaml"
IMAGE = "registry.example.com/dpone-runtime:latest"

with DAG(
    dag_id="dpone_orders_backfill_campaign",
    schedule=None,  # manual campaigns only
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    params={"from": "2024-01-01", "to": "2024-12-31", "step": "1w"},
    tags=["dpone", "backfill"],
) as dag:
    run_campaign = KubernetesPodOperator(
        task_id="dpone_backfill_run",
        name="dpone-backfill-orders",
        image=IMAGE,
        cmds=["/bin/sh", "-ec"],
        arguments=[
            f"dpone backfill run {MANIFEST} --execute "
            "--from {{ params['from'] }} --to {{ params['to'] }} --step {{ params['step'] }} "
            "--format json"
        ],
        get_logs=True,
        do_xcom_push=False,
    )
