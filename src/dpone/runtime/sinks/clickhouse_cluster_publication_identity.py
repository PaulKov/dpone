"""Stable invocation identity and unique dispatch tokens for cluster publication."""

from __future__ import annotations

import secrets
from typing import Any

from dpone.ports.clickhouse_cluster_publication import contracts
from dpone.runtime.sinks.clickhouse_full_refresh_contract import publication_invocation_id
from dpone.runtime.sinks.clickhouse_full_refresh_publication import SCHEDULER_IDENTITY_OPTION
from dpone.runtime.sinks.clickhouse_table_ddl import ClickHouseTableDesign

ClusterPublicationError = contracts.ClusterPublicationError
digest_payload = contracts.digest_payload


def operation_id(load_config: Any) -> str:
    options = getattr(load_config, "options", {}) or {}
    stable = str(options.get(SCHEDULER_IDENTITY_OPTION) or "") or publication_invocation_id(
        scheduler_run_id="direct", process_id=str(load_config.target_table)
    )
    return digest_payload({"stable": stable, "database": load_config.target_schema, "target": load_config.target_table})


def cluster_name(load_config: Any) -> str:
    if not (name := ClickHouseTableDesign.from_options(getattr(load_config, "options", {}) or {}).cluster.name):
        raise ClusterPublicationError("DPONE_CLICKHOUSE_CLUSTER_TOPOLOGY_UNSUPPORTED", "cluster name is missing")
    return name


def is_cluster_enabled(load_config: Any) -> bool:
    """Identify a configured clustered table without side effects."""

    return ClickHouseTableDesign.from_options(getattr(load_config, "options", {}) or {}).cluster.on_cluster


def correlation_token(operation_id: str, action: str, epoch: int) -> str:
    return f"dpone-v1-{operation_id[:20]}-{action}-{epoch}-{secrets.token_hex(16)}"
