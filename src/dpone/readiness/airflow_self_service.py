"""Backward-compatible self-service Airflow application facade."""

from dpone.ports.project_authoring_lock import AuthoringLockFactory
from dpone.readiness.airflow_self_service_composition import (
    AirflowSelfServiceService,
    build_airflow_self_service_service,
)
from dpone.readiness.airflow_self_service_models import Change, SelfServiceResult

__all__ = [
    "AirflowSelfServiceService",
    "AuthoringLockFactory",
    "Change",
    "SelfServiceResult",
    "build_airflow_self_service_service",
]
