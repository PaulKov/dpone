"""Airflow's bounded backfill summary preserves truthful state authority."""

from __future__ import annotations

import jsonschema

from dpone.gitops.schema_airflow_runtime_contracts import airflow_backfill_progress_schema


def test_backfill_xcom_accepts_absent_cache_with_sql_authority() -> None:
    jsonschema.validate(
        {
            "run_key": "campaign-a",
            "state_path": None,
            "state_cache_status": "unavailable",
            "state_authority": {
                "backend": "audit_schema",
                "dialect": "mssql",
                "schema": "DWH_Tech",
                "campaigns_table": "__dpone__backfill_campaigns",
                "chunks_table": "__dpone__backfill_chunks",
                "run_key": "campaign-a",
            },
        },
        airflow_backfill_progress_schema(),
    )
