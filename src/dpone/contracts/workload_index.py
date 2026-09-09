"""Shared closed vocabulary for the public workload-index contract."""

from __future__ import annotations

from dpone.contracts.project_discovery import (
    DISCOVERY_WORKLOAD_IDENTITY_FIELDS,
    MAX_PROJECT_DISCOVERY_BYTES,
    MAX_PROJECT_DOMAINS,
    MAX_PROJECT_WORKLOADS,
)

WORKLOAD_INDEX_SCHEMA = "dpone.workload-index.v1"
WORKLOAD_INDEX_FIELDS = (
    "schema",
    "project_fingerprint",
    "layout_mode",
    "layout_root",
    "pipeline_id_scope",
    "workloads",
)
WORKLOAD_INDEX_ITEM_FIELDS = (
    "pipeline_id",
    "domain",
    "owner",
    "ownership_fingerprint",
    "authoring_source",
    "source_sha256",
    "semantic_fingerprint",
    "workload_fingerprint",
    "connection_refs",
    "dependencies",
    "airflow",
)
WORKLOAD_IDENTITY_FIELDS = DISCOVERY_WORKLOAD_IDENTITY_FIELDS
WORKLOAD_INDEX_DEPENDENCY_FIELDS = ("kind", "path", "sha256")
WORKLOAD_INDEX_AIRFLOW_FIELDS = ("enabled", "dag_id", "schedule")

MAX_WORKLOAD_INDEX_BYTES = MAX_PROJECT_DISCOVERY_BYTES
MAX_WORKLOAD_INDEX_TOKENS = 2_000_000
MAX_WORKLOAD_INDEX_NODES = MAX_PROJECT_WORKLOADS * 64 + 4_096
MAX_WORKLOAD_INDEX_DEPTH = 32

__all__ = [
    "MAX_PROJECT_DISCOVERY_BYTES",
    "MAX_PROJECT_DOMAINS",
    "MAX_PROJECT_WORKLOADS",
    "MAX_WORKLOAD_INDEX_BYTES",
    "MAX_WORKLOAD_INDEX_DEPTH",
    "MAX_WORKLOAD_INDEX_NODES",
    "MAX_WORKLOAD_INDEX_TOKENS",
    "WORKLOAD_IDENTITY_FIELDS",
    "WORKLOAD_INDEX_AIRFLOW_FIELDS",
    "WORKLOAD_INDEX_DEPENDENCY_FIELDS",
    "WORKLOAD_INDEX_FIELDS",
    "WORKLOAD_INDEX_ITEM_FIELDS",
    "WORKLOAD_INDEX_SCHEMA",
]
