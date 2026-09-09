"""ClickHouse cluster pre-extract safety checks."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.runtime.decision_audit import publish_runtime_decision
from dpone.runtime.sinks.clickhouse_cluster_topology import ClickHouseClusterTopologyProbe

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.ports.clickhouse_connector import ClickHouseConnectorPort


class ClickHouseClusterPreflightMixin:
    """Fail before source IO when cluster target topology is unsafe."""

    connector: ClickHouseConnectorPort

    def preflight_before_extract(self, *, load_config: LoadConfig, load_record: Any | None = None) -> None:
        del load_record
        evidence = ClickHouseClusterTopologyProbe(self.connector).inspect(load_config)
        publish_runtime_decision(
            {
                "requested": "cluster_complete_target",
                "selected": evidence.status,
                "release_gate": "green" if evidence.passed else "blocked",
                "blockers": evidence.blockers,
                "warnings": evidence.warnings,
                "target_schema": load_config.target_schema,
                "target_table": load_config.target_table,
                **evidence.to_dict(),
            },
            decision_id="clickhouse.cluster_target_topology",
            phase="pre_extract",
            component="clickhouse_sink",
            category="cluster_topology",
            fallback_allowed=False,
        )
        if evidence.blockers:
            missing = ", ".join(evidence.missing_hosts) or "unknown"
            raise RuntimeError(
                "ClickHouse cluster target topology blocked: "
                f"{', '.join(evidence.blockers)}; target={evidence.target}; missing_hosts={missing}"
            )


__all__ = ["ClickHouseClusterPreflightMixin"]
