"""Canonical identity vocabulary and resource budgets for project discovery."""

from __future__ import annotations

DISCOVERY_WORKLOAD_IDENTITY_FIELDS = (
    "pipeline_id",
    "domain",
    "owner",
    "ownership_fingerprint",
    "authoring_source",
    "source_sha256",
    "semantic_fingerprint",
    "connection_refs",
    "dependencies",
    "airflow",
)

MAX_PROJECT_DOMAINS = 500
MAX_PROJECT_WORKLOADS = 5_000
MAX_PROJECT_DISCOVERY_ENTRIES = 10_000
MAX_PROJECT_DISCOVERY_BYTES = 32 * 1024 * 1024

__all__ = [
    "DISCOVERY_WORKLOAD_IDENTITY_FIELDS",
    "MAX_PROJECT_DISCOVERY_BYTES",
    "MAX_PROJECT_DISCOVERY_ENTRIES",
    "MAX_PROJECT_DOMAINS",
    "MAX_PROJECT_WORKLOADS",
]
