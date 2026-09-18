"""ClickHouse adapters retaining endpoints only inside direct connection boundaries."""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence
from typing import Any

from dpone.contracts import clickhouse_cluster_publication as cluster_contract
from dpone.contracts.clickhouse_cluster_publication import digest_payload
from dpone.contracts.clickhouse_external_replication import (
    EXTERNAL_AUTHORITY_SCHEMA_VERSION,
    INTERNAL_AUTHORITY_SCHEMA_VERSION,
    ArtifactIdentity,
    ExternalAuthorityPhase,
    ExternalAuthorityRecord,
    ExternalContractError,
    ExternalMember,
    ExternalTopology,
    MemberGenerationObservation,
    PhysicalGeneration,
    ReplicationMode,
)
from dpone.ports import clickhouse_external_replication as ports

_MUTATION_SETTINGS = {"keeper_map_strict_mode": 1, "insert_keeper_max_retries": 0}
_DDL_SETTINGS = dict(skip_unavailable_shards=0, distributed_ddl_output_mode="throw", distributed_ddl_task_timeout=60)
_INVENTORY_SQL = (
    "SELECT host_name, host_address, port, shard_num, replica_num, internal_replication "
    "FROM system.clusters WHERE cluster = %(cluster)s ORDER BY shard_num, replica_num"
)


class ClickHouseExternalTopologyCatalog:
    def __init__(self, connector: Any) -> None:
        self._connector = connector
        self._member_ids_by_host: dict[str, str] = {}
        self._bootstrap_hosts: tuple[str, ...] = ()
        self._inventory_digest: str | None = None

    def inventory(self, cluster: str) -> ExternalTopology:
        rows = _inventory_rows(self._connector, cluster)
        members = tuple(_member(row) for row in rows)
        topology = ExternalTopology(
            cluster=cluster,
            replication_mode=ReplicationMode.EXTERNAL,
            members=members,
        )
        topology.validate()
        self._member_ids_by_host = {
            host: member.member_id
            for row, member in zip(rows, members, strict=True)
            for host in (str(row[0]), str(row[1]))
        }
        self._bootstrap_hosts = tuple(str(row[0]) for row in rows)
        self._inventory_digest = digest_payload(
            [
                {
                    "member_id": member.member_id,
                    "endpoint_digest": digest_payload(
                        {"host": str(row[0]), "address": str(row[1]), "port": int(row[2])}
                    ),
                }
                for row, member in zip(rows, members, strict=True)
            ]
        )
        return topology

    @property
    def bootstrap_hosts(self) -> tuple[str, ...]:
        """Return raw host keys only for the internal authority bootstrap boundary."""

        return self._bootstrap_hosts

    @property
    def inventory_digest(self) -> str:
        if self._inventory_digest is None:
            raise ExternalContractError("INVENTORY_INVALID", "inventory must be observed first")
        return self._inventory_digest

    def member_identity(self, host: str) -> str:
        """Resolve a queue host into its opaque admitted member identity."""

        member_id = self._member_ids_by_host.get(str(host))
        if member_id is None:
            raise ExternalContractError("INVENTORY_INVALID", "queue host is not in admitted inventory")
        return member_id


class ClickHouseExternalReplicaConnectionProvider:
    def __init__(self, connector: Any, *, cluster: str, connect: Callable[..., Any]) -> None:
        self._connector = connector
        self._cluster = cluster
        self._connect = connect
        self._connections: dict[str, Any] = {}
        self._member_ids_by_connection: dict[int, str] = {}

    def connection_for(self, member_id: str) -> Any:
        cached = self._connections.get(member_id)
        if cached is not None:
            return cached
        rows = _inventory_rows(self._connector, self._cluster)
        topology = ExternalTopology(
            cluster=self._cluster,
            replication_mode=ReplicationMode.EXTERNAL,
            members=tuple(_member(row) for row in rows),
        )
        topology.validate()
        matches = [row for row in rows if _member(row).member_id == member_id]
        if len(matches) != 1:
            raise ExternalContractError("INVENTORY_INVALID", "opaque member identity is not uniquely resolvable")
        row = matches[0]
        try:
            connection = self._connect(self._connector, str(row[0]), str(row[1]), int(row[2]))
            self._connections[member_id] = connection
            self._member_ids_by_connection[id(connection)] = member_id
            return connection
        except Exception:
            raise ExternalContractError("INVENTORY_INVALID", "direct member connection is unavailable") from None

    def member_identity(self, connection: Any) -> str:
        """Return the opaque identity bound to a provider-owned connection."""

        return self._member_ids_by_connection.get(id(connection), "")


class ClickHouseExternalArtifactVerifier:
    def __init__(self, *, revalidate: Callable[[ArtifactIdentity], None]) -> None:
        self._revalidate = revalidate

    def revalidate(self, artifact: ArtifactIdentity) -> None:
        artifact.validate()
        self._revalidate(artifact)


class ClickHouseExternalReplicaStaging:
    def __init__(
        self,
        *,
        connection_provider: Any,
        driver: Any,
    ) -> None:
        self._connection_provider = connection_provider
        self._driver = driver

    def observe(self, member_id: str, record: ExternalAuthorityRecord) -> MemberGenerationObservation:
        observation = self._driver.observe(self._connection(member_id), record)
        if observation.member_id != member_id:
            raise ExternalContractError("INVENTORY_INVALID", "staging observation belongs to another member")
        _validate_observation(observation)
        return observation

    def create_candidate(self, member_id: str, record: ExternalAuthorityRecord) -> PhysicalGeneration:
        generation = self._driver.create_candidate(self._connection(member_id), record)
        generation.validate()
        return generation

    def load_candidate(
        self,
        member_id: str,
        record: ExternalAuthorityRecord,
        source: ports.ExternalArtifactSourcePort,
    ) -> None:
        artifact = record.artifact
        if artifact is None or source.binding_id != record.artifact_binding_id:
            raise ExternalContractError("ARTIFACT_UNSUPPORTED", "sealed artifact identity is unavailable")
        source.revalidate(artifact)
        sealed_source = source.open_replay()
        self._driver.load_candidate(self._connection(member_id), record, sealed_source)

    def drop_candidate(
        self,
        member_id: str,
        record: ExternalAuthorityRecord,
        expected: PhysicalGeneration,
    ) -> None:
        expected.validate()
        self._driver.drop_candidate(self._connection(member_id), record, expected)

    def _connection(self, member_id: str) -> Any:
        provider = self._connection_provider
        connection = provider(member_id) if callable(provider) else provider.connection_for(member_id)
        if connection is None:
            raise ExternalContractError("INVENTORY_INVALID", "direct member connection is unavailable")
        return connection


class ClickHouseExternalKeeperMapAuthority:
    """Persist external authority with one mutation and an exact post-read."""

    def __init__(self, connector: Any, database: str, *, table: str = cluster_contract.AUTHORITY_TABLE) -> None:
        self._connector = connector
        self._database = database
        self._table = table
        self._completed_internal: dict[str, tuple[int, str, str, str]] = {}

    def read_versioned(self, target_key: str) -> ports.VersionedExternalAuthorityRecord | None:
        rows = self._connector.get_records(
            f"SELECT operation_id, fence_token, phase, dispatch_epoch, payload, payload_sha256, _version "
            f"FROM {self._qualified} WHERE target_key = %(target_key)s",
            {"target_key": target_key},
        )
        if len(rows) != 1:
            return None
        row = rows[0]
        raw = _text(row[4])
        schema = _authority_schema(raw)
        if schema == INTERNAL_AUTHORITY_SCHEMA_VERSION:
            if str(row[2]) != "COMPLETED":
                raise ExternalContractError("MODE_MISMATCH", "unresolved internal authority blocks external mode")
            self._completed_internal[target_key] = (int(row[6]), str(row[0]), str(row[1]), str(row[2]))
            return None
        if schema != EXTERNAL_AUTHORITY_SCHEMA_VERSION:
            raise ExternalContractError("SCHEMA_UNSUPPORTED", "authority schema is unsupported")
        record = ExternalAuthorityRecord.from_payload(raw)
        if (
            record.target_key != target_key
            or record.operation_id != str(row[0])
            or record.fence_token != str(row[1])
            or record.phase.value != str(row[2])
            or record.dispatch_epoch != int(row[3])
            or record.payload_sha256 != _text(row[5])
        ):
            return None
        return ports.VersionedExternalAuthorityRecord(record=record, version=int(row[6]))

    def create_if_absent(self, record: ExternalAuthorityRecord) -> ports.ExternalAuthorityMutationResult:
        record.validate()
        migration = self._completed_internal.pop(record.target_key, None)
        if migration is not None:
            version, operation_id, fence_token, phase = migration
            sql = (
                f"ALTER TABLE {self._qualified} UPDATE operation_id = %(operation_id)s, "
                "fence_token = %(fence_token)s, phase = %(phase)s, dispatch_epoch = %(dispatch_epoch)s, "
                "payload = %(payload)s, payload_sha256 = %(payload_sha256)s "
                "WHERE target_key = %(target_key)s AND _version = %(version)s "
                "AND operation_id = %(expected_operation)s AND fence_token = %(expected_fence)s "
                "AND phase = %(expected_phase)s"
            )
            params = {
                **_record_params(record),
                "version": version,
                "expected_operation": operation_id,
                "expected_fence": fence_token,
                "expected_phase": phase,
            }
            return self._mutate_once(sql, params, record, prior_version=version)
        sql = (
            f"INSERT INTO {self._qualified} "
            "(target_key, operation_id, fence_token, phase, dispatch_epoch, payload, payload_sha256) "
            "VALUES (%(target_key)s, %(operation_id)s, %(fence_token)s, %(phase)s, %(dispatch_epoch)s, "
            "%(payload)s, %(payload_sha256)s)"
        )
        return self._mutate_once(sql, _record_params(record), record, prior_version=None)

    def compare_and_swap(
        self,
        current: ports.VersionedExternalAuthorityRecord,
        desired: ExternalAuthorityRecord,
    ) -> ports.ExternalAuthorityMutationResult:
        desired.validate()
        before = current.record
        sql = (
            f"ALTER TABLE {self._qualified} UPDATE operation_id = %(operation_id)s, "
            "fence_token = %(fence_token)s, phase = %(phase)s, dispatch_epoch = %(dispatch_epoch)s, "
            "payload = %(payload)s, payload_sha256 = %(payload_sha256)s "
            "WHERE target_key = %(target_key)s AND _version = %(version)s "
            "AND operation_id = %(expected_operation)s AND fence_token = %(expected_fence)s "
            "AND phase = %(expected_phase)s"
        )
        params = {
            **_record_params(desired),
            "version": current.version,
            "expected_operation": before.operation_id,
            "expected_fence": before.fence_token,
            "expected_phase": before.phase.value,
        }
        return self._mutate_once(sql, params, desired, prior_version=current.version)

    def _mutate_once(
        self,
        sql: str,
        params: dict[str, Any],
        desired: ExternalAuthorityRecord,
        *,
        prior_version: int | None,
    ) -> ports.ExternalAuthorityMutationResult:
        query_id = f"dpone-external-authority-{desired.operation_id[:20]}-{desired.dispatch_epoch}"
        try:
            self._connector.connection.execute(
                sql,
                params,
                settings=_MUTATION_SETTINGS,
                query_id=query_id,
            )
        except Exception:
            return ports.ExternalAuthorityMutationResult(ports.ExternalAuthorityMutationStatus.OUTCOME_UNKNOWN)
        observed = self.read_versioned(desired.target_key)
        expected_version = 0 if prior_version is None else prior_version + 1
        if observed is None:
            return ports.ExternalAuthorityMutationResult(ports.ExternalAuthorityMutationStatus.OUTCOME_UNKNOWN)
        if observed.record.payload != desired.payload or observed.version != expected_version:
            return ports.ExternalAuthorityMutationResult(
                ports.ExternalAuthorityMutationStatus.CONFLICT, observed=observed
            )
        permit = _dispatch_permit(desired)
        return ports.ExternalAuthorityMutationResult(
            ports.ExternalAuthorityMutationStatus.VERIFIED,
            observed=observed,
            permit=permit,
        )

    @property
    def _qualified(self) -> str:
        return _qualified(self._database, self._table)


class ClickHouseExternalClusterDdl:
    """Submit a fenced DDL once and delegate exact queue observation."""

    def __init__(
        self,
        connector: Any,
        catalog: Any,
        *,
        member_identity: Callable[[str], str],
    ) -> None:
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


def _inventory_rows(connector: Any, cluster: str) -> list[Any]:
    return connector.get_records(_INVENTORY_SQL, {"cluster": cluster})


def _member(row: Sequence[Any]) -> ExternalMember:
    return ExternalMember.create(
        shard_num=int(row[3]),
        replica_num=int(row[4]),
        internal_replication=bool(row[5]),
    )


def _record_params(record: ExternalAuthorityRecord) -> dict[str, Any]:
    return {
        "target_key": record.target_key,
        "operation_id": record.operation_id,
        "fence_token": record.fence_token,
        "phase": record.phase.value,
        "dispatch_epoch": record.dispatch_epoch,
        "payload": record.payload,
        "payload_sha256": record.payload_sha256,
    }


def _dispatch_permit(record: ExternalAuthorityRecord) -> ports.ExternalDispatchPermit | None:
    if record.phase not in {
        ExternalAuthorityPhase.PUBLICATION_DISPATCHING,
        ExternalAuthorityPhase.CLEANUP_DISPATCHING,
    }:
        return None
    return ports.ExternalDispatchPermit(
        target_key=record.target_key,
        operation_id=record.operation_id,
        fence_token=record.fence_token,
        dispatch_epoch=record.dispatch_epoch,
    )


def _validate_observation(observation: MemberGenerationObservation) -> None:
    for generation in (observation.target, observation.candidate):
        if generation is not None:
            generation.validate()


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


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _authority_schema(raw: str) -> str | None:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None
    return str(value.get("schema_version")) if isinstance(value, dict) else None
