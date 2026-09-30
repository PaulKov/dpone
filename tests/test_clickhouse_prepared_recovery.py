"""Prepared publication is planned from exact physical evidence, without writes."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from dpone.contracts.clickhouse_cluster_publication import (
    AuthorityPhase,
    AuthorityRecord,
    ClusterInventory,
    ClusterPublicationError,
    ClusterReplica,
    GenerationIdentity,
    ReplicaGeneration,
    VersionedAuthorityRecord,
    digest_payload,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_ddl import ClickHouseClusterPublicationDdl
from dpone.runtime.sinks.clickhouse_prepared_recovery import plan_prepared_recovery


def _identity(name: str, *, schema: str = "schema") -> GenerationIdentity:
    return GenerationIdentity(name, "ReplicatedMergeTree()", schema, "default", f"/tables/{name}")


class Catalog:
    def __init__(self) -> None:
        self.inventory_value = ClusterInventory(
            "cluster",
            (
                ClusterReplica("replica-a", "127.0.0.1", 9000, 1, 1, True),
                ClusterReplica("replica-b", "127.0.0.2", 9000, 1, 2, True),
            ),
        )
        self.target = _identity("old")
        self.candidate = _identity("new")
        self.rows = 2

    def inventory(self, cluster: str) -> ClusterInventory:
        return self.inventory_value

    def require_atomic_database(self, cluster: str, database: str, hosts: tuple[str, ...]) -> None:
        return None

    def candidate_counts(self, cluster: str, database: str, candidate: str) -> dict[str, int]:
        return {host: self.rows for host in self.inventory_value.hosts}

    def generations(
        self, cluster: str, database: str, target: str, candidate: str, hosts: tuple[str, ...]
    ) -> tuple[ReplicaGeneration, ...]:
        return tuple(ReplicaGeneration(host, self.target, self.candidate, row_count=self.rows) for host in hosts)


class Authority:
    def __init__(self, record: AuthorityRecord) -> None:
        self.current = VersionedAuthorityRecord(record, 1)
        self.mutations = 0

    def read_versioned(self, target_key: str) -> VersionedAuthorityRecord:
        return self.current

    def compare_and_swap(self, current: VersionedAuthorityRecord, desired: AuthorityRecord) -> None:
        self.mutations += 1


class Ddl:
    def __init__(self) -> None:
        self.absence_proven = True
        self.dispatches = 0

    def prove_no_prior_publication(
        self, record: AuthorityRecord, *, cluster: str, operation_started_at: datetime
    ) -> bool:
        return self.absence_proven

    def publication_query_digest(self, record: AuthorityRecord, *, cluster: str) -> str:
        return "query-digest"

    def dispatch_publication(self, record: AuthorityRecord, permit: object, *, cluster: str) -> None:
        self.dispatches += 1


def _case() -> tuple[Catalog, Authority, Ddl]:
    catalog = Catalog()
    record = AuthorityRecord(
        target_key=digest_payload({"cluster": "cluster", "database": "analytics", "target": "target"}),
        operation_id="original-operation",
        fence_token="original-fence",
        phase=AuthorityPhase.PREPARED,
        dispatch_epoch=0,
        inventory_digest=catalog.inventory_value.digest,
        plan_digest="original-plan",
        database="analytics",
        target="target",
        candidate="candidate",
        desired=catalog.candidate,
        predecessor=catalog.target,
        staged_rows=2,
        authority_write_id="strict-authority-write",
    )
    return catalog, Authority(record), Ddl()


def _plan(catalog: Catalog, authority: Authority, ddl: Ddl):
    return plan_prepared_recovery(
        catalog,
        authority,
        ddl,
        cluster="cluster",
        database="analytics",
        target="target",
        operation_id="original-operation",
        expected_version=1,
        operation_started_at=datetime(2026, 9, 27, 10, 7, tzinfo=UTC),
    )


def test_plan_accepts_exact_prepared_generation() -> None:
    catalog, authority, ddl = _case()
    plan = _plan(catalog, authority, ddl)
    assert plan.operation_id == "original-operation"
    assert plan.authority_version == 1
    assert plan.candidate_identity == catalog.candidate
    assert plan.predecessor_identity == catalog.target
    assert plan.query_digest == "query-digest"
    assert plan.plan_digest
    public = str(plan.to_public_dict())
    assert "original-operation" not in public and "'new'" not in public and "'old'" not in public
    assert authority.mutations == ddl.dispatches == 0


def test_plan_rejects_schema_digest_drift() -> None:
    catalog, authority, ddl = _case()
    catalog.candidate = _identity("new", schema="changed")
    with pytest.raises(ClusterPublicationError, match="GENERATION_DIVERGED"):
        _plan(catalog, authority, ddl)
    assert authority.mutations == ddl.dispatches == 0


def test_plan_rejects_unknown_ddl_history() -> None:
    catalog, authority, ddl = _case()
    ddl.absence_proven = False
    with pytest.raises(ClusterPublicationError, match="DDL_UNKNOWN"):
        _plan(catalog, authority, ddl)
    assert authority.mutations == ddl.dispatches == 0


def test_plan_rejects_legacy_prepared_origin() -> None:
    catalog, authority, ddl = _case()
    authority.current = replace(authority.current, record=replace(authority.current.record, authority_write_id=None))
    with pytest.raises(ClusterPublicationError, match="AUTHORITY_UNSAFE"):
        _plan(catalog, authority, ddl)
    assert authority.mutations == ddl.dispatches == 0


def test_plan_rejects_foreign_operation() -> None:
    catalog, authority, ddl = _case()
    authority.current = replace(authority.current, record=replace(authority.current.record, operation_id="foreign"))
    with pytest.raises(ClusterPublicationError, match="AUTHORITY_CONFLICT"):
        _plan(catalog, authority, ddl)
    assert authority.mutations == ddl.dispatches == 0


@pytest.mark.parametrize("field", ["ddl_entry", "ddl_correlation_token", "ddl_query_digest"])
def test_plan_rejects_any_prior_ddl_identity(field: str) -> None:
    catalog, authority, ddl = _case()
    authority.current = replace(authority.current, record=replace(authority.current.record, **{field: "observed"}))
    with pytest.raises(ClusterPublicationError, match="PUBLICATION_UNRESOLVED"):
        _plan(catalog, authority, ddl)
    assert authority.mutations == ddl.dispatches == 0


def test_plan_rejects_inventory_drift() -> None:
    catalog, authority, ddl = _case()
    catalog.inventory_value = replace(catalog.inventory_value, replicas=catalog.inventory_value.replicas[:1])
    with pytest.raises(ClusterPublicationError, match="INVENTORY"):
        _plan(catalog, authority, ddl)
    assert authority.mutations == ddl.dispatches == 0


def test_plan_rejects_predecessor_generation_drift() -> None:
    catalog, authority, ddl = _case()
    catalog.target = _identity("unexpected-old")
    with pytest.raises(ClusterPublicationError, match="GENERATION_DIVERGED"):
        _plan(catalog, authority, ddl)
    assert authority.mutations == ddl.dispatches == 0


def test_plan_rejects_replica_row_mismatch() -> None:
    catalog, authority, ddl = _case()
    catalog.rows = 1
    with pytest.raises(ClusterPublicationError, match="CANDIDATE"):
        _plan(catalog, authority, ddl)
    assert authority.mutations == ddl.dispatches == 0


def test_plan_rejects_empty_candidate_without_authored_permission() -> None:
    catalog, authority, ddl = _case()
    catalog.rows = 0
    authority.current = replace(authority.current, record=replace(authority.current.record, staged_rows=0))
    with pytest.raises(ClusterPublicationError, match="EMPTY_CANDIDATE"):
        _plan(catalog, authority, ddl)
    assert authority.mutations == ddl.dispatches == 0


def test_plan_rejects_quality_mismatch() -> None:
    catalog, authority, ddl = _case()
    authority.current = replace(
        authority.current,
        record=replace(authority.current.record, quality_reader="unverified"),
    )
    with pytest.raises(ClusterPublicationError, match="QUALITY"):
        _plan(catalog, authority, ddl)
    assert authority.mutations == ddl.dispatches == 0


class EvidenceConnector:
    def __init__(self, *, history_covered: bool = True, prior_query: bool = False) -> None:
        self.history_covered = history_covered
        self.prior_query = prior_query

    def get_records(self, sql: str, params: dict[str, object]) -> list[tuple[object, ...]]:
        hosts = ("replica-a", "replica-b")
        if "system.settings" in sql:
            return [(host, "1") for host in hosts]
        if "min(event_time)" in sql:
            event_time = datetime(2026, 9, 27, 9 if self.history_covered else 11, tzinfo=UTC)
            return [(host, event_time) for host in hosts]
        if "system.query_log" in sql:
            return [("replica-a", 1)] if self.prior_query else []
        if "system.processes" in sql:
            return []
        if "system.distributed_ddl_queue" in sql:
            return [(0,)]
        raise AssertionError(f"unexpected proof query: {sql}")


def test_real_ddl_proof_requires_history_coverage() -> None:
    catalog, authority, _ = _case()
    connector = EvidenceConnector(history_covered=False)
    ddl = ClickHouseClusterPublicationDdl(connector, catalog)
    assert not ddl.prove_no_prior_publication(
        authority.current.record,
        cluster="cluster",
        operation_started_at=datetime(2026, 9, 27, 10, 7, tzinfo=UTC),
    )


def test_real_ddl_proof_rejects_original_query_id() -> None:
    catalog, authority, _ = _case()
    connector = EvidenceConnector(prior_query=True)
    ddl = ClickHouseClusterPublicationDdl(connector, catalog)
    assert not ddl.prove_no_prior_publication(
        authority.current.record,
        cluster="cluster",
        operation_started_at=datetime(2026, 9, 27, 10, 7, tzinfo=UTC),
    )


def test_real_ddl_proof_rejects_legacy_prepared_record_even_with_negative_history() -> None:
    catalog, authority, _ = _case()
    connector = EvidenceConnector()
    ddl = ClickHouseClusterPublicationDdl(connector, catalog)
    legacy_record = replace(authority.current.record, authority_write_id=None)
    assert not ddl.prove_no_prior_publication(
        legacy_record,
        cluster="cluster",
        operation_started_at=datetime(2026, 9, 27, 10, 7, tzinfo=UTC),
    )


def test_real_ddl_proof_accepts_strict_origin_and_complete_negative_history() -> None:
    catalog, authority, _ = _case()
    connector = EvidenceConnector()
    ddl = ClickHouseClusterPublicationDdl(connector, catalog)
    assert ddl.prove_no_prior_publication(
        authority.current.record,
        cluster="cluster",
        operation_started_at=datetime(2026, 9, 27, 10, 7, tzinfo=UTC),
    )
