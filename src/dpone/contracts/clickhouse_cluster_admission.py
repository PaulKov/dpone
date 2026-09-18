"""Pure admission policy for bounded ClickHouse cluster publication."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

CLICKHOUSE_CLUSTER_ENGINE_REQUIRED = "clickhouse_cluster_publication.engine_must_be_replicated_merge_tree"
CLICKHOUSE_CLUSTER_STAGING_DATABASE_UNSUPPORTED = "clickhouse_cluster_publication.staging_database_must_match_target"
CLICKHOUSE_CLUSTER_ACCESS_TABLE_UNSUPPORTED = "clickhouse_cluster_publication.access_table_unsupported"
CLICKHOUSE_CLUSTER_SOURCE_BUDGET_REQUIRED = "clickhouse_cluster_publication.max_source_bytes_required"
CLICKHOUSE_CLUSTER_DDL_SCOPE_REQUIRED = "clickhouse_cluster_publication.ddl_scope_must_be_cluster"
CLICKHOUSE_CLUSTER_NAME_REQUIRED = "clickhouse_cluster_publication.cluster_name_required"
CLICKHOUSE_CLUSTER_EXTERNAL_ENGINE_REQUIRED = (
    "clickhouse_cluster_publication.external_engine_must_be_non_replicated_merge_tree"
)
CLICKHOUSE_CLUSTER_REPLICATION_MODE_INVALID = "clickhouse_cluster_publication.replication_mode_invalid"

_REPLICATED_MERGE_TREE = re.compile(r"^Replicated[A-Za-z0-9_]*MergeTree(?:\s*\(|\s*$)")
_MERGE_TREE = re.compile(r"^[A-Za-z0-9_]*MergeTree(?:\s*\(|\s*$)")


@dataclass(frozen=True, slots=True)
class ClickHouseClusterAdmissionInput:
    """Resolved build/runtime facts needed by the admission policy."""

    sink_type: str
    strategy_mode: str
    max_source_bytes: Any
    engine: str
    cluster_name: str | None
    ddl_scope: str
    access_table_enabled: bool
    target_database: str
    staging_database: str
    replication_mode: str = "internal"


@dataclass(frozen=True, slots=True)
class ClickHouseClusterAdmissionDecision:
    """Deterministic route selection with stable fail-closed blockers."""

    requested: bool
    selected: bool
    blockers: tuple[str, ...]
    mode: str
    runtime_admission_required: bool
    no_fallback: bool
    replication_mode: str = "internal"

    def to_dict(self) -> dict[str, object]:
        return {
            "requested": self.requested,
            "selected": self.selected,
            "mode": self.mode,
            "runtime_admission_required": self.runtime_admission_required,
            "blockers": list(self.blockers),
            "no_fallback": self.no_fallback,
            "replication_mode": self.replication_mode,
        }


class ClickHouseClusterAdmissionError(ValueError):
    """A requested cluster publication contract is statically unsafe."""

    def __init__(self, decision: ClickHouseClusterAdmissionDecision) -> None:
        self.decision = decision
        super().__init__("; ".join(decision.blockers))


def evaluate_clickhouse_cluster_admission(
    request: ClickHouseClusterAdmissionInput,
) -> ClickHouseClusterAdmissionDecision:
    """Select local or cluster publication without an implicit fallback."""

    sink_type = request.sink_type.strip().lower()
    strategy = request.strategy_mode.strip().lower()
    cluster_name = (request.cluster_name or "").strip()
    ddl_scope = request.ddl_scope.strip().lower()
    replication_mode = request.replication_mode.strip().lower()
    requested = (
        sink_type == "clickhouse" and strategy == "full_refresh" and bool(cluster_name or ddl_scope == "cluster")
    )
    if not requested:
        return ClickHouseClusterAdmissionDecision(
            requested=False,
            selected=False,
            blockers=(),
            mode="local" if sink_type == "clickhouse" and strategy == "full_refresh" else "not_applicable",
            runtime_admission_required=False,
            no_fallback=False,
            replication_mode=replication_mode,
        )

    blockers: list[str] = []
    engine = request.engine.strip()
    if replication_mode not in {"internal", "external"}:
        blockers.append(CLICKHOUSE_CLUSTER_REPLICATION_MODE_INVALID)
    elif replication_mode == "internal" and not _REPLICATED_MERGE_TREE.match(engine):
        blockers.append(CLICKHOUSE_CLUSTER_ENGINE_REQUIRED)
    elif replication_mode == "external" and (_REPLICATED_MERGE_TREE.match(engine) or not _MERGE_TREE.match(engine)):
        blockers.append(CLICKHOUSE_CLUSTER_EXTERNAL_ENGINE_REQUIRED)
    if request.staging_database.strip() != request.target_database.strip():
        blockers.append(CLICKHOUSE_CLUSTER_STAGING_DATABASE_UNSUPPORTED)
    if request.access_table_enabled:
        blockers.append(CLICKHOUSE_CLUSTER_ACCESS_TABLE_UNSUPPORTED)
    if (
        isinstance(request.max_source_bytes, bool)
        or not isinstance(request.max_source_bytes, int)
        or request.max_source_bytes <= 0
    ):
        blockers.append(CLICKHOUSE_CLUSTER_SOURCE_BUDGET_REQUIRED)
    if ddl_scope != "cluster":
        blockers.append(CLICKHOUSE_CLUSTER_DDL_SCOPE_REQUIRED)
    if not cluster_name:
        blockers.append(CLICKHOUSE_CLUSTER_NAME_REQUIRED)

    return ClickHouseClusterAdmissionDecision(
        requested=True,
        selected=not blockers,
        blockers=tuple(blockers),
        mode=("cluster_external" if replication_mode == "external" else "cluster") if not blockers else "blocked",
        runtime_admission_required=not blockers,
        no_fallback=True,
        replication_mode=replication_mode,
    )


def clickhouse_cluster_admission_input(
    *,
    sink_type: str,
    strategy_mode: str,
    max_source_bytes: Any,
    physical_design: Mapping[str, Any] | None,
    target_database: str,
    staging_database: str | None,
) -> ClickHouseClusterAdmissionInput:
    """Normalize manifest, dbt, plan, and runtime facts for one evaluator."""

    physical = physical_design if isinstance(physical_design, Mapping) else {}
    storage = _mapping(physical.get("storage"))
    clickhouse = _mapping(storage.get("clickhouse"))
    cluster_name, ddl_scope, replication_mode = _cluster(clickhouse.get("cluster"))
    access = _mapping(clickhouse.get("access_table"))
    configured_staging = str(staging_database or "").strip()
    return ClickHouseClusterAdmissionInput(
        sink_type=sink_type,
        strategy_mode=strategy_mode,
        max_source_bytes=max_source_bytes,
        engine=str(clickhouse.get("engine") or "MergeTree"),
        cluster_name=cluster_name,
        ddl_scope=ddl_scope,
        replication_mode=replication_mode,
        access_table_enabled=bool(str(access.get("name") or "").strip()),
        target_database=str(target_database),
        staging_database=(str(target_database) if configured_staging in {"", "staging"} else configured_staging),
    )


def _cluster(raw: object) -> tuple[str | None, str, str]:
    if isinstance(raw, str):
        name = raw.strip() or None
        return name, "cluster" if name else "local", "internal"
    values = _mapping(raw)
    name = str(values.get("name") or "").strip() or None
    replication_mode = str(values.get("replication_mode") or "internal").strip().lower()
    if "ddl_scope" in values:
        return name, str(values.get("ddl_scope") or "local").strip().lower(), replication_mode
    return name, "cluster" if bool(values.get("on_cluster", False)) else "local", replication_mode


def _mapping(raw: object) -> Mapping[str, Any]:
    return raw if isinstance(raw, Mapping) else {}


def require_clickhouse_cluster_admission(
    request: ClickHouseClusterAdmissionInput,
) -> ClickHouseClusterAdmissionDecision:
    """Return the decision or raise with its complete structured blocker set."""

    decision = evaluate_clickhouse_cluster_admission(request)
    if decision.requested and not decision.selected:
        raise ClickHouseClusterAdmissionError(decision)
    return decision


__all__ = [
    "CLICKHOUSE_CLUSTER_ACCESS_TABLE_UNSUPPORTED",
    "CLICKHOUSE_CLUSTER_DDL_SCOPE_REQUIRED",
    "CLICKHOUSE_CLUSTER_ENGINE_REQUIRED",
    "CLICKHOUSE_CLUSTER_EXTERNAL_ENGINE_REQUIRED",
    "CLICKHOUSE_CLUSTER_NAME_REQUIRED",
    "CLICKHOUSE_CLUSTER_REPLICATION_MODE_INVALID",
    "CLICKHOUSE_CLUSTER_SOURCE_BUDGET_REQUIRED",
    "CLICKHOUSE_CLUSTER_STAGING_DATABASE_UNSUPPORTED",
    "ClickHouseClusterAdmissionDecision",
    "ClickHouseClusterAdmissionError",
    "ClickHouseClusterAdmissionInput",
    "clickhouse_cluster_admission_input",
    "evaluate_clickhouse_cluster_admission",
    "require_clickhouse_cluster_admission",
]
