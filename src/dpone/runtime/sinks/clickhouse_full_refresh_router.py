"""Select local or cluster full-refresh publication without fallback."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.config.load_strategy import SOURCE_BYTE_BUDGET_OPTION
from dpone.ports.clickhouse_cluster_publication import (
    ClickHouseClusterAdmissionError,
    clickhouse_cluster_admission_input,
    evaluate_clickhouse_cluster_admission,
)
from dpone.runtime.clickhouse_cluster_publication_composition import (
    build_clickhouse_cluster_publication,
)
from dpone.runtime.decision_audit import publish_runtime_decision
from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import CLUSTER_RECEIPT_VERSION
from dpone.contracts.clickhouse_external_replication import EXTERNAL_RECEIPT_SCHEMA_VERSION
from dpone.runtime.sinks.clickhouse_full_refresh_publication import ClickHouseFullRefreshPublicationService


class ClickHouseFullRefreshPublicationRouter:
    """Keep local behavior unchanged and admit clusters through strict services."""

    def __init__(
        self,
        local: ClickHouseFullRefreshPublicationService,
        cluster: Any,
        external: Any | None = None,
    ) -> None:
        self._local = local
        self._cluster = cluster
        self._external = external

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
        if ClickHouseFullRefreshPublicationService.is_enabled(load_config):
            return True
        return evaluate_clickhouse_cluster_admission(_admission_input(load_config)).requested

    def prepare_admission(self, load_config: Any) -> Any:
        service = self._service(load_config)
        return service.prepare_admission(load_config)

    def publish(self, load_config: Any, candidate_config: Any, *, staged_rows: int) -> Any:
        service = self._service(load_config)
        return service.publish(load_config, candidate_config, staged_rows=staged_rows)

    def is_external(self, load_config: Any) -> bool:
        return evaluate_clickhouse_cluster_admission(_admission_input(load_config)).mode == "cluster_external"

    def stage_external(self, load_config: Any, payload: Any) -> Any:
        service = self._service(load_config)
        return service.stage(load_config, payload)

    def validate_external(self, context: Any) -> Any:
        if self._external is None:
            raise RuntimeError("clickhouse_cluster_external_publication.runtime_capability_unavailable")
        return self._external.validate(context)

    def publish_external(self, context: Any, validation: Any) -> Any:
        if self._external is None:
            raise RuntimeError("clickhouse_cluster_external_publication.runtime_capability_unavailable")
        return self._external.publish(context, validation)

    def cleanup_external(self, context: Any) -> Any:
        if self._external is None:
            raise RuntimeError("clickhouse_cluster_external_publication.runtime_capability_unavailable")
        return self._external.cleanup(context)

    def abort_external(self, context: Any) -> None:
        if self._external is None:
            raise RuntimeError("clickhouse_cluster_external_publication.runtime_capability_unavailable")
        self._external.abort(context)

    def _service(self, load_config: Any) -> Any:
        decision = evaluate_clickhouse_cluster_admission(_admission_input(load_config))
        if not self._local.is_enabled(load_config) and not decision.requested:
            return self._local
        publish_runtime_decision(
            {
                "requested": "cluster" if decision.requested else "local",
                "selected": decision.mode,
                "replication_mode": decision.replication_mode,
                "blockers": decision.blockers,
                "release_gate": "blocked" if decision.blockers else "green",
                "runtime_admission_required": decision.runtime_admission_required,
                "no_fallback": decision.no_fallback,
            },
            decision_id="clickhouse.full_refresh_publication",
            phase="pre_extract",
            component="clickhouse_sink",
            category="publication",
            fallback_allowed=False,
        )
        if decision.requested and not decision.selected:
            raise ClickHouseClusterAdmissionError(decision)
        if not decision.selected:
            return self._local
        if decision.mode == "cluster_external":
            if self._external is None or not self._external.is_enabled(load_config):
                raise RuntimeError("clickhouse_cluster_external_publication.runtime_capability_unavailable")
            return self._external
        if not self._cluster.is_enabled(load_config):
            raise RuntimeError("clickhouse_cluster_publication.runtime_capability_unavailable")
        return self._cluster

    def cleanup(self, receipt: Any) -> None:
        if isinstance(receipt, Mapping) and receipt.get("schema_version") == EXTERNAL_RECEIPT_SCHEMA_VERSION:
            if self._external is None:
                raise RuntimeError("clickhouse_cluster_external_publication.runtime_capability_unavailable")
            self._external.cleanup_receipt(receipt)
            return
        if getattr(receipt, "schema_version", None) == EXTERNAL_RECEIPT_SCHEMA_VERSION:
            if self._external is None:
                raise RuntimeError("clickhouse_cluster_external_publication.runtime_capability_unavailable")
            self._external.cleanup_receipt(receipt)
            return
        if isinstance(receipt, Mapping) and receipt.get("schema_version") == CLUSTER_RECEIPT_VERSION:
            self._cluster.cleanup(receipt)
            return
        if getattr(receipt, "schema_version", None) == CLUSTER_RECEIPT_VERSION:
            self._cluster.cleanup(receipt)
            return
        self._local.cleanup(receipt)

    def replay_result(self, load_config: Any) -> Any:
        decision = evaluate_clickhouse_cluster_admission(_admission_input(load_config))
        if decision.mode == "cluster_external" and self._external is not None:
            return self._external.replay_result(load_config)
        return ClickHouseFullRefreshPublicationService.replay_result(load_config)


def _admission_input(load_config: Any) -> Any:
    options = getattr(load_config, "options", {}) or {}
    return clickhouse_cluster_admission_input(
        sink_type="clickhouse",
        strategy_mode=str(getattr(getattr(load_config, "load_strategy", None), "value", "")),
        max_source_bytes=options.get(SOURCE_BYTE_BUDGET_OPTION),
        physical_design=options.get("physical_design"),
        target_database=str(getattr(load_config, "target_schema", "") or ""),
        staging_database=str(getattr(load_config, "staging_schema", "") or ""),
    )
