from __future__ import annotations

from dataclasses import replace
from typing import Any

import pytest

from dpone.contracts.clickhouse_cluster_publication import QueueEntry, QueueHostResult
from dpone.contracts.clickhouse_external_replication import (
    ArtifactIdentity,
    ExternalAuthorityPhase,
    ExternalAuthorityRecord,
    ExternalMember,
    ExternalMemberRecord,
    ExternalMemberStageState,
    MemberGenerationObservation,
    PhysicalGeneration,
    derive_generation_id,
)
from dpone.ports.clickhouse_external_replication import (
    ExternalAuthorityMutationStatus,
    ExternalDispatchPermit,
    VersionedExternalAuthorityRecord,
)
from dpone.runtime.sinks.clickhouse_external_replication_clickhouse import (
    ClickHouseExternalArtifactVerifier,
    ClickHouseExternalClusterDdl,
    ClickHouseExternalKeeperMapAuthority,
    ClickHouseExternalReplicaConnectionProvider,
    ClickHouseExternalReplicaStaging,
    ClickHouseExternalTopologyCatalog,
)


class _Connection:
    def __init__(self) -> None:
        self.executions: list[tuple[str, Any, dict[str, Any]]] = []

    def execute(self, sql: str, params: Any = None, **kwargs: Any) -> None:
        self.executions.append((sql, params, kwargs))


class _Connector:
    def __init__(self, rows: list[tuple[Any, ...]] | None = None) -> None:
        self.rows = rows or []
        self.queries: list[tuple[str, Any]] = []
        self.connection = _Connection()

    def get_records(self, query: str, params: Any = None) -> list[tuple[Any, ...]]:
        self.queries.append((query, params))
        return list(self.rows)


class _QueueCatalog:
    def __init__(self, entries: tuple[QueueEntry, ...] = ()) -> None:
        self.entries = entries

    def find_entries(self, cluster: str, token: str) -> tuple[QueueEntry, ...]:
        assert (cluster, token) == ("analytics_cluster", "publish-token")
        return self.entries

    def read_entry(self, cluster: str, entry: str) -> QueueEntry | None:
        assert cluster == "analytics_cluster"
        return next((item for item in self.entries if item.entry == entry), None)


def _digest(character: str) -> str:
    return character * 64


def _generation(uuid: str, content: str = "d") -> PhysicalGeneration:
    return PhysicalGeneration(
        uuid=uuid,
        engine_full="MergeTree ORDER BY tuple()",
        schema_digest=_digest("c"),
        content_digest=_digest(content),
        row_count=2,
    )


def _record() -> ExternalAuthorityRecord:
    members = tuple(
        ExternalMemberRecord(
            member_id=ExternalMember.create(
                shard_num=1,
                replica_num=replica_num,
                internal_replication=False,
            ).member_id,
            stage_state=ExternalMemberStageState.READY,
            predecessor=_generation(f"predecessor-{replica_num}", "e"),
            candidate=_generation(f"candidate-{replica_num}"),
        )
        for replica_num in (1, 2)
    )
    operation_id = _digest("2")
    artifact = ArtifactIdentity(
        sha256=_digest("5"),
        byte_size=128,
        row_count=2,
        schema_digest=_digest("c"),
        wire_digest=_digest("d"),
    )
    return ExternalAuthorityRecord(
        target_key=_digest("1"),
        operation_id=operation_id,
        fence_token="fence-token",
        phase=ExternalAuthorityPhase.PUBLICATION_DISPATCHING,
        dispatch_epoch=1,
        inventory_digest=_digest("3"),
        plan_digest=_digest("4"),
        database="analytics",
        target="target_table",
        candidate="candidate_table",
        members=members,
        artifact=artifact,
        generation_id=derive_generation_id(
            operation_id=operation_id,
            artifact_sha256=artifact.sha256,
            schema_digest=artifact.schema_digest,
            row_count=artifact.row_count,
        ),
        publication_correlation_token="publish-token",
    )


def test_topology_catalog_returns_only_opaque_external_members() -> None:
    connector = _Connector(
        [
            ("node_2", "192.0.2.2", 9000, 1, 2, 0),
            ("node_1", "192.0.2.1", 9000, 1, 1, 0),
        ]
    )

    topology = ClickHouseExternalTopologyCatalog(connector).inventory("analytics_cluster")

    assert topology.cluster == "analytics_cluster"
    assert sorted(member.replica_num for member in topology.ordered_members) == [1, 2]
    assert "node_1" not in repr(topology)
    assert "192.0.2.1" not in repr(topology)


def test_topology_catalog_rejects_internal_replication_without_fallback() -> None:
    connector = _Connector(
        [
            ("node_1", "192.0.2.1", 9000, 1, 1, 1),
            ("node_2", "192.0.2.2", 9000, 1, 2, 1),
        ]
    )

    with pytest.raises(ValueError, match="MODE_MISMATCH"):
        ClickHouseExternalTopologyCatalog(connector).inventory("analytics_cluster")


def test_connection_provider_resolves_an_opaque_member_through_injected_factory() -> None:
    connector = _Connector(
        [
            ("node_1", "192.0.2.1", 9000, 1, 1, 0),
            ("node_2", "192.0.2.2", 9000, 1, 2, 0),
        ]
    )
    member_id = ExternalMember.create(shard_num=1, replica_num=1, internal_replication=False).member_id
    calls: list[tuple[Any, str, str, int]] = []

    def connect(template: Any, host: str, address: str, port: int) -> object:
        calls.append((template, host, address, port))
        return object()

    result = ClickHouseExternalReplicaConnectionProvider(
        connector,
        cluster="analytics_cluster",
        connect=connect,
    ).connection_for(member_id)

    assert result is not None
    assert calls == [(connector, "node_1", "192.0.2.1", 9000)]


def test_authority_create_verifies_exact_post_write_and_returns_no_dispatch_permit() -> None:
    record = replace(_record(), phase=ExternalAuthorityPhase.LOCKED, dispatch_epoch=0)
    connector = _Connector()
    authority = ClickHouseExternalKeeperMapAuthority(connector, "analytics")
    connector.rows = [
        (
            record.operation_id,
            record.fence_token,
            record.phase.value,
            record.dispatch_epoch,
            record.payload,
            record.payload_sha256,
            0,
        )
    ]

    result = authority.create_if_absent(record)

    assert result.status is ExternalAuthorityMutationStatus.VERIFIED
    assert result.permit is None
    sql, _, kwargs = connector.connection.executions[0]
    assert "insert_keeper_max_retries" not in sql
    assert kwargs["settings"]["insert_keeper_max_retries"] == 0


def test_authority_dispatch_cas_returns_bound_permit_only_after_exact_version() -> None:
    desired = _record()
    current_record = replace(desired, phase=ExternalAuthorityPhase.STAGED, dispatch_epoch=0)
    connector = _Connector(
        [
            (
                desired.operation_id,
                desired.fence_token,
                desired.phase.value,
                desired.dispatch_epoch,
                desired.payload,
                desired.payload_sha256,
                8,
            )
        ]
    )
    authority = ClickHouseExternalKeeperMapAuthority(connector, "analytics")
    current = VersionedExternalAuthorityRecord(record=current_record, version=7)

    result = authority.compare_and_swap(current, desired)

    assert result.status is ExternalAuthorityMutationStatus.VERIFIED
    assert result.permit == ExternalDispatchPermit(
        target_key=desired.target_key,
        operation_id=desired.operation_id,
        fence_token=desired.fence_token,
        dispatch_epoch=desired.dispatch_epoch,
    )


def test_authority_transport_error_is_unknown_and_never_retried() -> None:
    record = replace(_record(), phase=ExternalAuthorityPhase.LOCKED, dispatch_epoch=0)
    connector = _Connector()

    def fail_once(*args: Any, **kwargs: Any) -> None:
        connector.connection.executions.append((str(args[0]), args[1], kwargs))
        raise TimeoutError("synthetic timeout")

    connector.connection.execute = fail_once  # type: ignore[method-assign]

    result = ClickHouseExternalKeeperMapAuthority(connector, "analytics").create_if_absent(record)

    assert result.status is ExternalAuthorityMutationStatus.OUTCOME_UNKNOWN
    assert len(connector.connection.executions) == 1
    assert connector.queries == []


def test_artifact_verifier_validates_before_injected_revalidation() -> None:
    observed: list[ArtifactIdentity] = []
    artifact = _record().artifact
    assert artifact is not None

    ClickHouseExternalArtifactVerifier(revalidate=observed.append).revalidate(artifact)

    assert observed == [artifact]


def test_replica_staging_passes_only_a_direct_member_connection_to_driver() -> None:
    record = _record()
    member_id = record.members[0].member_id
    connection = object()
    sealed_source = object()
    calls: list[tuple[str, Any]] = []
    artifact_checks: list[ArtifactIdentity] = []

    class Driver:
        def observe(self, direct: Any, observed_record: ExternalAuthorityRecord) -> MemberGenerationObservation:
            calls.append(("observe", direct))
            return MemberGenerationObservation(member_id, None, observed_record.members[0].candidate)

        def create_candidate(self, direct: Any, observed_record: ExternalAuthorityRecord) -> PhysicalGeneration:
            calls.append(("create", direct))
            candidate = observed_record.members[0].candidate
            assert candidate is not None
            return candidate

        def load_candidate(
            self,
            direct: Any,
            observed_record: ExternalAuthorityRecord,
            source: Any,
        ) -> None:
            assert source is sealed_source
            calls.append(("load", direct))

        def drop_candidate(
            self,
            direct: Any,
            observed_record: ExternalAuthorityRecord,
            expected: PhysicalGeneration,
        ) -> None:
            calls.append(("drop", direct))

    staging = ClickHouseExternalReplicaStaging(
        connection_provider=lambda requested: connection if requested == member_id else None,
        artifact_verifier=ClickHouseExternalArtifactVerifier(revalidate=artifact_checks.append),
        artifact_source=lambda artifact: sealed_source,
        driver=Driver(),
    )

    assert staging.observe(member_id, record).member_id == member_id
    staging.create_candidate(member_id, record)
    staging.load_candidate(member_id, record)
    expected = record.members[0].candidate
    assert expected is not None
    staging.drop_candidate(member_id, record, expected)
    assert calls == [(name, connection) for name in ("observe", "create", "load", "drop")]
    assert artifact_checks == [record.artifact, record.artifact]


def test_replica_staging_fails_before_candidate_mutation_when_artifact_cannot_reopen() -> None:
    record = _record()
    member_id = record.members[0].member_id
    mutations: list[str] = []

    class Driver:
        def observe(self, direct: Any, observed_record: ExternalAuthorityRecord) -> MemberGenerationObservation:
            raise AssertionError("not used")

        def create_candidate(self, direct: Any, observed_record: ExternalAuthorityRecord) -> PhysicalGeneration:
            mutations.append("create")
            return _generation("unexpected")

        def load_candidate(self, direct: Any, observed_record: ExternalAuthorityRecord, source: Any) -> None:
            raise AssertionError("not used")

        def drop_candidate(
            self,
            direct: Any,
            observed_record: ExternalAuthorityRecord,
            expected: PhysicalGeneration,
        ) -> None:
            raise AssertionError("not used")

    staging = ClickHouseExternalReplicaStaging(
        connection_provider=lambda requested: object(),
        artifact_verifier=ClickHouseExternalArtifactVerifier(revalidate=lambda artifact: None),
        artifact_source=lambda artifact: None,
        driver=Driver(),
    )

    with pytest.raises(ValueError, match="ARTIFACT_UNSUPPORTED"):
        staging.create_candidate(member_id, record)

    assert mutations == []


def test_cluster_ddl_requires_exact_permit_and_submits_one_correlated_statement() -> None:
    record = _record()
    connector = _Connector()
    ddl = ClickHouseExternalClusterDdl(
        connector,
        _QueueCatalog(),
        member_identity=lambda host: f"opaque-{host[-1]}",
    )
    permit = ExternalDispatchPermit(
        target_key=record.target_key,
        operation_id=record.operation_id,
        fence_token=record.fence_token,
        dispatch_epoch=record.dispatch_epoch,
    )

    ddl.dispatch_publication(record, permit, cluster="analytics_cluster")

    assert len(connector.connection.executions) == 1
    sql, _, kwargs = connector.connection.executions[0]
    assert (
        sql
        == "EXCHANGE TABLES `analytics`.`target_table` AND `analytics`.`candidate_table` ON CLUSTER `analytics_cluster`"
    )
    assert kwargs["settings"]["log_comment"] == "publish-token"
    assert "node_1" not in repr(kwargs)


def test_cluster_ddl_rejects_unbound_permit_before_execution() -> None:
    record = _record()
    connector = _Connector()
    ddl = ClickHouseExternalClusterDdl(
        connector,
        _QueueCatalog(),
        member_identity=lambda host: f"opaque-{host[-1]}",
    )
    permit = ExternalDispatchPermit(
        target_key=record.target_key,
        operation_id=record.operation_id,
        fence_token="different-fence",
        dispatch_epoch=record.dispatch_epoch,
    )

    with pytest.raises(ValueError, match="permit"):
        ddl.dispatch_publication(record, permit, cluster="analytics_cluster")

    assert connector.connection.executions == []


def test_cluster_ddl_redacts_queue_hosts_and_exception_text() -> None:
    entry = QueueEntry(
        entry="query-1",
        query_digest=_digest("9"),
        correlation_token="publish-token",
        hosts=(
            QueueHostResult(
                host="node_1",
                status="Finished",
                exception_code=42,
                exception_text="endpoint-specific detail",
            ),
        ),
    )
    ddl = ClickHouseExternalClusterDdl(
        _Connector(),
        _QueueCatalog((entry,)),
        member_identity=lambda host: "opaque-member",
    )

    observed = ddl.find_entries("analytics_cluster", "publish-token")

    assert observed[0].hosts[0].host == "opaque-member"
    assert observed[0].hosts[0].exception_text == "redacted"
    assert "node_1" not in repr(observed)
    assert "endpoint-specific" not in repr(observed)
