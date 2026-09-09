"""ClickHouse cluster topology preflight for managed finalization routes."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from dpone.runtime.sinks.clickhouse_table_ddl import ClickHouseTableDesign

SCHEMA_VERSION = "dpone.clickhouse.cluster_topology.v1"

_UNAVAILABLE_MARKERS = (
    "Code: 279",
    "All connection tries failed",
    "While executing Remote",
    "HedgedConnections",
    "Connection refused",
    "Timeout exceeded",
    "NetException",
)


@dataclass(frozen=True, slots=True)
class ClickHouseClusterTopologyEvidence:
    """Result of checking whether a target table exists on all cluster hosts."""

    target: str
    cluster: str | None
    status: str
    expected_hosts: tuple[str, ...] = ()
    actual_hosts: tuple[str, ...] = ()
    missing_hosts: tuple[str, ...] = ()
    unreachable_hosts: tuple[str, ...] = ()
    uuids: tuple[str, ...] = ()
    engines: tuple[str, ...] = ()
    blockers: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()

    @property
    def passed(self) -> bool:
        return not self.blockers

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "target": self.target,
            "cluster": self.cluster,
            "status": self.status,
            "expected_hosts": list(self.expected_hosts),
            "actual_hosts": list(self.actual_hosts),
            "missing_hosts": list(self.missing_hosts),
            "unreachable_hosts": list(self.unreachable_hosts),
            "uuids": list(self.uuids),
            "engines": list(self.engines),
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
        }


class ClickHouseClusterTopologyProbe:
    """Fail fast when cluster DDL would hit an incomplete target topology."""

    def __init__(self, connector: Any) -> None:
        self._connector = connector

    def inspect(self, load_config: Any) -> ClickHouseClusterTopologyEvidence:
        target = _target(load_config)
        design = ClickHouseTableDesign.from_options(getattr(load_config, "options", {}) or {})
        if not design.cluster.on_cluster:
            return ClickHouseClusterTopologyEvidence(target=target, cluster=design.cluster.name, status="not_clustered")

        cluster = str(design.cluster.name or "")
        expected = self._cluster_hosts(cluster)
        if not expected:
            return ClickHouseClusterTopologyEvidence(
                target=target,
                cluster=cluster,
                status="blocked",
                blockers=("clickhouse_cluster_not_found",),
            )
        table_rows, unreachable = self._target_rows(cluster, load_config, expected_hosts=expected)
        actual = tuple(row.host for row in table_rows)
        if not actual:
            return ClickHouseClusterTopologyEvidence(
                target=target,
                cluster=cluster,
                status="target_absent",
                expected_hosts=expected,
                unreachable_hosts=unreachable,
                warnings=_unreachable_warnings(unreachable),
            )

        missing = tuple(host for host in expected if host not in set(actual))
        uuids = tuple(sorted({row.uuid for row in table_rows if row.uuid}))
        engines = tuple(sorted({row.engine_full for row in table_rows if row.engine_full}))
        hard_missing, soft_missing = _partition_missing(missing, unreachable)
        if soft_missing and not hard_missing and _skip_unavailable_shards():
            blockers = _consistency_blockers(uuids, engines)
            warnings = (
                "clickhouse_cluster_unreachable_replicas_skipped",
                "clickhouse_cluster_finalization_would_be_partial",
                *_unreachable_warnings(soft_missing),
            )
            if blockers:
                return ClickHouseClusterTopologyEvidence(
                    target=target,
                    cluster=cluster,
                    status="blocked",
                    expected_hosts=expected,
                    actual_hosts=actual,
                    missing_hosts=missing,
                    unreachable_hosts=soft_missing,
                    uuids=uuids,
                    engines=engines,
                    blockers=blockers,
                    warnings=warnings,
                )
            return ClickHouseClusterTopologyEvidence(
                target=target,
                cluster=cluster,
                status="passed_partial",
                expected_hosts=expected,
                actual_hosts=actual,
                missing_hosts=missing,
                unreachable_hosts=soft_missing,
                uuids=uuids,
                engines=engines,
                warnings=warnings,
            )
        if missing:
            return ClickHouseClusterTopologyEvidence(
                target=target,
                cluster=cluster,
                status="blocked",
                expected_hosts=expected,
                actual_hosts=actual,
                missing_hosts=missing,
                unreachable_hosts=unreachable,
                uuids=uuids,
                engines=engines,
                blockers=("clickhouse_cluster_target_missing_replicas",),
                warnings=("clickhouse_cluster_finalization_would_be_partial",),
            )
        blockers = _consistency_blockers(uuids, engines)
        if blockers:
            return ClickHouseClusterTopologyEvidence(
                target=target,
                cluster=cluster,
                status="blocked",
                expected_hosts=expected,
                actual_hosts=actual,
                unreachable_hosts=unreachable,
                uuids=uuids,
                engines=engines,
                blockers=blockers,
                warnings=("clickhouse_cluster_finalization_would_be_inconsistent",),
            )
        return ClickHouseClusterTopologyEvidence(
            target=target,
            cluster=cluster,
            status="passed",
            expected_hosts=expected,
            actual_hosts=actual,
            unreachable_hosts=unreachable,
            uuids=uuids,
            engines=engines,
        )

    def _cluster_hosts(self, cluster: str) -> tuple[str, ...]:
        rows = self._connector.get_records(
            f"SELECT DISTINCT host_name FROM system.clusters WHERE cluster = {_literal(cluster)} ORDER BY host_name"
        )
        return tuple(str(row[0]) for row in rows)

    def _target_rows(
        self,
        cluster: str,
        load_config: Any,
        *,
        expected_hosts: tuple[str, ...],
    ) -> tuple[tuple[_ClusterTableRow, ...], tuple[str, ...]]:
        query = (
            "SELECT hostName(), toString(uuid), engine_full FROM "
            f"clusterAllReplicas({_literal(cluster)}, system.tables) "
            f"WHERE database = {_literal(load_config.target_schema)} "
            f"AND name = {_literal(load_config.target_table)} "
            "ORDER BY hostName()"
        )
        try:
            rows = self._connector.get_records(query)
        except Exception as exc:
            if not (_skip_unavailable_shards() and _is_host_unavailable_error(exc)):
                raise
            return self._target_rows_per_host(load_config, expected_hosts)

        table_rows = tuple(
            _ClusterTableRow(host=str(row[0]), uuid=str(row[1]), engine_full=str(row[2])) for row in rows
        )
        if not _skip_unavailable_shards():
            return table_rows, ()

        missing = tuple(host for host in expected_hosts if host not in {row.host for row in table_rows})
        if not missing:
            return table_rows, ()
        classified_rows, unreachable = self._classify_missing_hosts(load_config, missing)
        return table_rows + classified_rows, unreachable

    def _target_rows_per_host(
        self,
        load_config: Any,
        expected_hosts: tuple[str, ...],
    ) -> tuple[tuple[_ClusterTableRow, ...], tuple[str, ...]]:
        actual: list[_ClusterTableRow] = []
        unreachable: list[str] = []
        for host in expected_hosts:
            row, host_unreachable = self._probe_host_table(host, load_config)
            if host_unreachable:
                unreachable.append(host)
            elif row is not None:
                actual.append(row)
        return tuple(actual), tuple(unreachable)

    def _classify_missing_hosts(
        self,
        load_config: Any,
        missing_hosts: tuple[str, ...],
    ) -> tuple[tuple[_ClusterTableRow, ...], tuple[str, ...]]:
        found: list[_ClusterTableRow] = []
        unreachable: list[str] = []
        for host in missing_hosts:
            row, host_unreachable = self._probe_host_table(host, load_config)
            if host_unreachable:
                unreachable.append(host)
            elif row is not None:
                found.append(row)
        return tuple(found), tuple(unreachable)

    def _probe_host_table(self, host: str, load_config: Any) -> tuple[_ClusterTableRow | None, bool]:
        query = (
            "SELECT hostName(), toString(uuid), engine_full FROM "
            f"remote({_literal(host)}, system.tables) "
            f"WHERE database = {_literal(load_config.target_schema)} "
            f"AND name = {_literal(load_config.target_table)} "
            "LIMIT 1"
        )
        try:
            rows = self._connector.get_records(query)
        except Exception as exc:
            if _is_host_unavailable_error(exc):
                return None, True
            raise
        if not rows:
            return None, False
        row = rows[0]
        return _ClusterTableRow(host=str(row[0]), uuid=str(row[1]), engine_full=str(row[2])), False


@dataclass(frozen=True, slots=True)
class _ClusterTableRow:
    host: str
    uuid: str
    engine_full: str


def _consistency_blockers(uuids: tuple[str, ...], engines: tuple[str, ...]) -> tuple[str, ...]:
    blockers = []
    if len(uuids) > 1:
        blockers.append("clickhouse_cluster_target_uuid_mismatch")
    if len(engines) > 1:
        blockers.append("clickhouse_cluster_target_engine_mismatch")
    return tuple(blockers)


def _partition_missing(
    missing: tuple[str, ...],
    unreachable: tuple[str, ...],
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    unreachable_set = set(unreachable)
    hard = tuple(host for host in missing if host not in unreachable_set)
    soft = tuple(host for host in missing if host in unreachable_set)
    return hard, soft


def _unreachable_warnings(hosts: tuple[str, ...]) -> tuple[str, ...]:
    if not hosts:
        return ()
    return (f"clickhouse_cluster_unreachable_hosts:{','.join(hosts)}",)


def _skip_unavailable_shards() -> bool:
    raw = os.getenv("DPONE_CLICKHOUSE_SKIP_UNAVAILABLE_SHARDS", "1")
    try:
        return bool(int(raw))
    except ValueError:
        return True


def _is_host_unavailable_error(exc: BaseException) -> bool:
    text = str(exc)
    return any(marker in text for marker in _UNAVAILABLE_MARKERS)


def _target(load_config: Any) -> str:
    return f"`{load_config.target_schema}`.`{load_config.target_table}`"


def _literal(value: str) -> str:
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


__all__ = ["ClickHouseClusterTopologyEvidence", "ClickHouseClusterTopologyProbe", "SCHEMA_VERSION"]
