"""Compatibility re-exports for Airflow desired-state reconciliation ports."""

from dpone.ports.airflow_desired_state import (
    ActiveDesiredDeployment,
    DesiredDeploymentActivator,
    DesiredDeploymentMaterializer,
    DesiredStateActivationResult,
    DesiredStateCheckpointStore,
    DesiredStateReconcilePortError,
)

__all__ = [
    "ActiveDesiredDeployment",
    "DesiredDeploymentActivator",
    "DesiredDeploymentMaterializer",
    "DesiredStateActivationResult",
    "DesiredStateCheckpointStore",
    "DesiredStateReconcilePortError",
]
