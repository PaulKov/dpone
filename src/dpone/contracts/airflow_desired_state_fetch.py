"""Compatibility re-export for the desired-state fetch evidence contract."""

from dpone.contracts.airflow_desired_state import (
    AIRFLOW_DESIRED_STATE_FETCH_SCHEMA,
    DesiredStateFetchEvidence,
)

__all__ = ["AIRFLOW_DESIRED_STATE_FETCH_SCHEMA", "DesiredStateFetchEvidence"]
