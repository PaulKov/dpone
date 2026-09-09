"""Compatibility re-export for the desired-state snapshot writer."""

from dpone.adapters.airflow_desired_state_checkpoint import (
    AtomicAirflowDesiredStateSnapshotWriter,
)

__all__ = ["AtomicAirflowDesiredStateSnapshotWriter"]
