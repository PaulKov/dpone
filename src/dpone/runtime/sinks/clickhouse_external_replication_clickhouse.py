"""ClickHouse adapters retaining endpoints only inside direct connection boundaries."""

from __future__ import annotations

import json
from collections.abc import Callable, Iterator, Sequence
from contextlib import contextmanager
from typing import Any

from dpone.ports import clickhouse_external_replication as ports
from dpone.ports.clickhouse_external_replication import (
    EXTERNAL_AUTHORITY_SCHEMA_VERSION,
    INTERNAL_AUTHORITY_SCHEMA_VERSION,
    ExternalAuthorityPhase,
    ExternalAuthorityRecord,
    ExternalContractError,
    ExternalMember,
    ExternalTopology,
    MemberGenerationObservation,
    PhysicalGeneration,
    ReplicationMode,
    cluster_contract,
    digest_payload,
)
from dpone.runtime.sinks.clickhouse_external_artifact_verifier import ClickHouseExternalArtifactVerifier
from dpone.runtime.sinks.clickhouse_external_replication_connection_provider import (
    ExternalReplicaConnectionProvider,
    managed_member_connection,
)

_MUTATION_SETTINGS = {"keeper_map_strict_mode": 1, "insert_keeper_max_retries": 0}
_INVENTORY_SQL = (
    "SELECT host_name, host_address, port, shard_num, replica_num, internal_replication "
    "FROM system.clusters WHERE cluster = %(cluster)s ORDER BY shard_num, replica_num"
)


class ClickHouseExternalTopologyCatalog:
    def __init__(self, connector: Any, *, resolve_endpoint: Callable[..., tuple[str, str, int]] | None = None) -> None:
        self._connector = connector
        self._resolve_endpoint = resolve_endpoint or _identity_endpoint
        self._member_ids_by_host: dict[str, str] = {}
        self._member_endpoints: dict[str, tuple[str, str, int]] = {}
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
        self._member_endpoints = {
            member.member_id: self._resolve_endpoint(str(row[0]), str(row[1]), int(row[2]))
            for row, member in zip(rows, members, strict=True)
        }
        self._bootstrap_hosts = tuple(str(row[0]) for row in rows)
        self._inventory_digest = digest_payload(
            [
                {
                    "member_id": member.member_id,
                    "endpoint_digest": digest_payload(
                        {"host": endpoint[0], "address": endpoint[1], "port": endpoint[2]}
                    ),
                }
                for member in members
                for endpoint in (self._member_endpoints[member.member_id],)
            ]
        )
        return topology

    @property
    def bootstrap_hosts(self) -> tuple[str, ...]:
        return self._bootstrap_hosts

    @property
    def inventory_digest(self) -> str:
        if self._inventory_digest is None:
            raise ExternalContractError("INVENTORY_INVALID", "inventory must be observed first")
        return self._inventory_digest

    def member_identity(self, host: str) -> str:
        member_id = self._member_ids_by_host.get(str(host))
        if member_id is None:
            raise ExternalContractError("INVENTORY_INVALID", "queue host is not in admitted inventory")
        return member_id

    def member_endpoint(self, member_id: str) -> tuple[str, str, int]:
        try:
            return self._member_endpoints[member_id]
        except KeyError:
            raise ExternalContractError("INVENTORY_INVALID", "member is not in admitted inventory") from None

    def member_ids(self) -> tuple[str, ...]:
        return tuple(sorted(self._member_endpoints))


class ClickHouseExternalReplicaConnectionProvider(ExternalReplicaConnectionProvider):
    def __init__(
        self,
        connector: Any,
        *,
        topology: ClickHouseExternalTopologyCatalog,
        connect: Callable[..., Any],
    ) -> None:
        super().__init__(
            connector,
            topology=topology,
            connect=connect,
            unavailable=lambda: ExternalContractError(
                "INVENTORY_INVALID",
                "direct member connection is unavailable",
            ),
        )


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
        with self._connection(member_id) as connection:
            observation = self._driver.observe(connection, record)
        if observation.member_id != member_id:
            raise ExternalContractError("INVENTORY_INVALID", "staging observation belongs to another member")
        _validate_observation(observation)
        return observation

    def create_candidate(
        self, member_id: str, record: ExternalAuthorityRecord, *, expected_uuid: str
    ) -> PhysicalGeneration:
        with self._connection(member_id) as connection:
            generation = self._driver.create_candidate(connection, record, expected_uuid=expected_uuid)
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
        with self._connection(member_id) as connection:
            self._driver.load_candidate(connection, record, sealed_source)

    def drop_candidate(
        self,
        member_id: str,
        record: ExternalAuthorityRecord,
        expected: PhysicalGeneration,
    ) -> None:
        expected.validate()
        with self._connection(member_id) as connection:
            self._driver.drop_candidate(connection, record, expected)

    @contextmanager
    def _connection(self, member_id: str) -> Iterator[Any]:
        with managed_member_connection(
            self._connection_provider,
            member_id,
            unavailable=lambda: ExternalContractError("INVENTORY_INVALID", "direct member connection is unavailable"),
        ) as connection:
            yield connection


class ClickHouseExternalKeeperMapAuthority:
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


def _inventory_rows(connector: Any, cluster: str) -> list[Any]:
    return connector.get_records(_INVENTORY_SQL, {"cluster": cluster})


def _identity_endpoint(host: str, address: str, port: int) -> tuple[str, str, int]:
    return host, address, port


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


def _quote(value: str) -> str:
    return "`" + value.replace("`", "``") + "`"


def _qualified(database: str, table: str) -> str:
    return f"{_quote(database)}.{_quote(table)}"


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _authority_schema(raw: str) -> str | None:
    try:
        value = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return None
    return str(value.get("schema_version")) if isinstance(value, dict) else None


__all__ = [
    "ClickHouseExternalArtifactVerifier",
    "ClickHouseExternalKeeperMapAuthority",
    "ClickHouseExternalReplicaConnectionProvider",
    "ClickHouseExternalReplicaStaging",
    "ClickHouseExternalTopologyCatalog",
]
