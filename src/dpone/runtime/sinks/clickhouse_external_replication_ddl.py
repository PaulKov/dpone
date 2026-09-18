"""Fenced distributed DDL for external ClickHouse publication."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from dpone.ports import clickhouse_external_replication as ports
from dpone.ports.clickhouse_external_replication import (
    ExternalAuthorityRecord,
    ExternalContractError,
    cluster_contract,
)

_DDL_SETTINGS = dict(skip_unavailable_shards=0, distributed_ddl_output_mode="throw", distributed_ddl_task_timeout=60)


class ClickHouseExternalClusterDdl:
    """Submit a fenced DDL once and delegate exact queue observation."""

    def __init__(self, connector: Any, catalog: Any, *, member_identity: Callable[[str], str]) -> None:
        self._connector = connector
        self._catalog = catalog
        self._member_identity = member_identity

    def publication_query_digest(self, record: ExternalAuthorityRecord, *, cluster: str) -> str:
        return cluster_contract.ddl_query_digest(_publication_sql(record, cluster))

    def cleanup_query_digest(self, record: ExternalAuthorityRecord, *, cluster: str) -> str:
        return cluster_contract.ddl_query_digest(_cleanup_sql(record, cluster))

    def dispatch_publication(
        self,
        record: ExternalAuthorityRecord,
        permit: ports.ExternalDispatchPermit,
        *,
        cluster: str,
    ) -> None:
        self._require_permit(record, permit)
        token = record.publication_correlation_token
        if not token:
            raise ValueError("external publication correlation token is missing")
        self._execute_once(_publication_sql(record, cluster), token, permit)

    def find_entries(self, cluster: str, correlation_token: str) -> tuple[cluster_contract.QueueEntry, ...]:
        return tuple(
            _redact_entry(entry, self._member_identity)
            for entry in self._catalog.find_entries(cluster, correlation_token)
        )

    def read_entry(self, cluster: str, entry: str) -> cluster_contract.QueueEntry | None:
        observed = self._catalog.read_entry(cluster, entry)
        return None if observed is None else _redact_entry(observed, self._member_identity)

    def drop_predecessor(
        self,
        record: ExternalAuthorityRecord,
        permit: ports.ExternalDispatchPermit,
        *,
        cluster: str,
    ) -> None:
        self._require_permit(record, permit)
        token = record.cleanup_correlation_token
        if not token:
            raise ValueError("external cleanup correlation token is missing")
        self._execute_once(_cleanup_sql(record, cluster), token, permit)

    def _execute_once(self, sql: str, token: str, permit: ports.ExternalDispatchPermit) -> None:
        try:
            self._connector.connection.execute(
                sql,
                settings={**_DDL_SETTINGS, "log_comment": token},
                query_id=f"dpone-external-ddl-{permit.operation_id[:20]}-{permit.dispatch_epoch}",
            )
        except Exception:
            raise ExternalContractError(
                "DDL_DISPATCH_UNKNOWN",
                "distributed DDL outcome requires queue reconciliation",
            ) from None

    @staticmethod
    def _require_permit(record: ExternalAuthorityRecord, permit: ports.ExternalDispatchPermit) -> None:
        if (
            permit.target_key != record.target_key
            or permit.operation_id != record.operation_id
            or permit.fence_token != record.fence_token
            or permit.dispatch_epoch != record.dispatch_epoch
        ):
            raise ValueError("external publication dispatch permit does not match authority")


def _redact_entry(
    entry: cluster_contract.QueueEntry, member_identity: Callable[[str], str]
) -> cluster_contract.QueueEntry:
    return cluster_contract.QueueEntry(
        entry=entry.entry,
        query_digest=entry.query_digest,
        correlation_token=entry.correlation_token,
        hosts=tuple(
            cluster_contract.QueueHostResult(
                host=member_identity(item.host),
                status=item.status,
                exception_code=item.exception_code,
                exception_text=(
                    None if item.exception_text is None else "" if item.exception_text == "" else "redacted"
                ),
            )
            for item in entry.hosts
        ),
    )


def _quote(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _qualified(database: str, table: str) -> str:
    return f"{_quote(database)}.{_quote(table)}"


def _publication_sql(record: ExternalAuthorityRecord, cluster: str) -> str:
    target = _qualified(record.database, record.target)
    candidate = _qualified(record.database, record.candidate)
    predecessors = [member.predecessor for member in record.members]
    if all(item is None for item in predecessors):
        return f"RENAME TABLE {candidate} TO {target} ON CLUSTER {_quote(cluster)}"
    if any(item is None for item in predecessors):
        raise ExternalContractError("GENERATION_DIVERGED", "predecessor presence differs between members")
    return f"EXCHANGE TABLES {target} AND {candidate} ON CLUSTER {_quote(cluster)}"


def _cleanup_sql(record: ExternalAuthorityRecord, cluster: str) -> str:
    return f"DROP TABLE IF EXISTS {_qualified(record.database, record.candidate)} ON CLUSTER {_quote(cluster)}"


__all__ = ["ClickHouseExternalClusterDdl"]
