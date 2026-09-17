"""Select local or cluster full-refresh publication without fallback."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.runtime.clickhouse_cluster_publication_composition import (
    build_clickhouse_cluster_publication,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import CLUSTER_RECEIPT_VERSION
from dpone.runtime.sinks.clickhouse_full_refresh_publication import ClickHouseFullRefreshPublicationService


class ClickHouseFullRefreshPublicationRouter:
    """Keep local behavior unchanged and admit clusters through strict services."""

    def __init__(
        self,
        local: ClickHouseFullRefreshPublicationService,
        cluster: Any,
        *,
        cluster_admitted: bool = False,
    ) -> None:
        self._local = local
        self._cluster = cluster
        self._cluster_admitted = cluster_admitted

    @classmethod
    def from_connector(cls, connector: Any) -> ClickHouseFullRefreshPublicationRouter:
        return cls(
            ClickHouseFullRefreshPublicationService.from_connector(connector),
            build_clickhouse_cluster_publication(connector),
        )

    @staticmethod
    def bind_runtime_identity(load_config: Any, *, scheduler_run_id: str, process_id: str) -> Any:
        return ClickHouseFullRefreshPublicationService.bind_runtime_identity(
            load_config, scheduler_run_id=scheduler_run_id, process_id=process_id
        )

    @staticmethod
    def is_enabled(load_config: Any) -> bool:
        return ClickHouseFullRefreshPublicationService.is_enabled(load_config)

    def prepare_admission(self, load_config: Any) -> Any:
        service = self._cluster if self._use_cluster(load_config) else self._local
        return service.prepare_admission(load_config)

    def publish(self, load_config: Any, candidate_config: Any, *, staged_rows: int) -> Any:
        service = self._cluster if self._use_cluster(load_config) else self._local
        return service.publish(load_config, candidate_config, staged_rows=staged_rows)

    def _use_cluster(self, load_config: Any) -> bool:
        return self._cluster_admitted and self._local.is_enabled(load_config) and self._cluster.is_enabled(load_config)

    def cleanup(self, receipt: Any) -> None:
        if isinstance(receipt, Mapping) and receipt.get("schema_version") == CLUSTER_RECEIPT_VERSION:
            self._cluster.cleanup(receipt)
            return
        if getattr(receipt, "schema_version", None) == CLUSTER_RECEIPT_VERSION:
            self._cluster.cleanup(receipt)
            return
        self._local.cleanup(receipt)

    @staticmethod
    def replay_result(load_config: Any) -> Any:
        return ClickHouseFullRefreshPublicationService.replay_result(load_config)
