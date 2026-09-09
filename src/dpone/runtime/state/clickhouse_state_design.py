"""ClickHouse state/audit table physical design."""

from __future__ import annotations

import warnings
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Literal, cast

from dpone.runtime.sinks.clickhouse_table_ddl import ClickHouseClusterDesign

ClickHouseStateEngine = Literal["auto", "MergeTree", "ReplacingMergeTree", "ReplicatedReplacingMergeTree"]

_NON_REPLICATED_CLUSTER_WARNING = "clickhouse_audit_non_replicated_engine_on_cluster"


@dataclass(frozen=True, slots=True)
class ClickHouseStateTableDesign:
    """Physical design for ClickHouse runtime-owned state tables."""

    cluster: ClickHouseClusterDesign = ClickHouseClusterDesign()
    engine: ClickHouseStateEngine = "auto"

    @classmethod
    def from_config(cls, *, cluster: Any | None = None, engine: str | None = None) -> ClickHouseStateTableDesign:
        return cls(
            cluster=ClickHouseClusterDesign.from_config(cluster),
            engine=_state_engine(engine),
        )

    @classmethod
    def from_load_config(cls, load_config: Any) -> ClickHouseStateTableDesign:
        options = _mapping(getattr(load_config, "options", {}))
        storage = _clickhouse_storage(options)
        audit = _mapping(_mapping(options.get("load_governance")).get("audit"))
        return cls.from_config(cluster=storage.get("cluster"), engine=_audit_engine(audit))

    @property
    def cluster_clause(self) -> str:
        return self.cluster.ddl_clause

    @property
    def engine_sql(self) -> str:
        engine = self.engine
        if engine == "auto":
            engine = "ReplicatedReplacingMergeTree" if self.cluster.on_cluster else "ReplacingMergeTree"
        if engine == "MergeTree":
            return "MergeTree"
        if engine == "ReplacingMergeTree":
            return "ReplacingMergeTree(__dpone__loaded_at)"
        return "ReplicatedReplacingMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}', __dpone__loaded_at)"

    @property
    def warnings(self) -> tuple[str, ...]:
        if self.cluster.on_cluster and not self.engine_sql.startswith("Replicated"):
            return (_NON_REPLICATED_CLUSTER_WARNING,)
        return ()

    def emit_warnings(self) -> None:
        for code in self.warnings:
            warnings.warn(
                f"{code}: ClickHouse audit/state table is created ON CLUSTER but uses a non-replicated engine.",
                RuntimeWarning,
                stacklevel=3,
            )


def _clickhouse_storage(options: Mapping[str, Any]) -> Mapping[str, Any]:
    physical = _mapping(options.get("physical_design"))
    storage = _mapping(physical.get("storage"))
    return _mapping(storage.get("clickhouse"))


def _audit_engine(audit: Mapping[str, Any]) -> str | None:
    clickhouse = _mapping(audit.get("clickhouse"))
    return _string(clickhouse.get("engine") or audit.get("clickhouse_engine"))


def _state_engine(value: str | None) -> ClickHouseStateEngine:
    if value is None or value == "":
        return "auto"
    normalized = str(value).strip()
    allowed = {"auto", "MergeTree", "ReplacingMergeTree", "ReplicatedReplacingMergeTree"}
    if normalized not in allowed:
        raise ValueError(
            "load_governance.audit.clickhouse.engine must be one of: "
            "auto, MergeTree, ReplacingMergeTree, ReplicatedReplacingMergeTree"
        )
    return cast(ClickHouseStateEngine, normalized)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _string(value: Any) -> str | None:
    return str(value).strip() if value is not None else None


__all__ = ["ClickHouseStateEngine", "ClickHouseStateTableDesign"]
