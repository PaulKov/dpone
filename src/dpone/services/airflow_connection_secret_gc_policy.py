"""Compatibility facade for Airflow Connection Secret GC policy."""

from dpone.services.airflow_connection_secret_gc_policy_impl import (
    AirflowConnectionSecretGcCandidate as AirflowConnectionSecretGcCandidate,
)
from dpone.services.airflow_connection_secret_gc_policy_impl import (
    AirflowConnectionSecretGcClassification as AirflowConnectionSecretGcClassification,
)
from dpone.services.airflow_connection_secret_gc_policy_impl import (
    classify_airflow_connection_secret_inventory as classify_airflow_connection_secret_inventory,
)
from dpone.services.airflow_connection_secret_gc_policy_impl import (
    iso_utc as iso_utc,
)
from dpone.services.airflow_connection_secret_gc_policy_impl import (
    require_aware_utc as require_aware_utc,
)

__all__ = [
    "AirflowConnectionSecretGcCandidate",
    "AirflowConnectionSecretGcClassification",
    "classify_airflow_connection_secret_inventory",
    "iso_utc",
    "require_aware_utc",
]
