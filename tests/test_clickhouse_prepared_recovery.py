"""Prepared publication is planned from exact physical evidence, without writes."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest

from dpone.contracts.clickhouse_cluster_publication import (
    AuthorityMutationResult,
    AuthorityMutationStatus,
    AuthorityPhase,
    AuthorityRecord,
    ClusterInventory,
    ClusterPublicationError,
    ClusterReplica,
    DispatchPermit,
    GenerationIdentity,
    QueueEntry,
    QueueHostResult,
    ReplicaGeneration,
    VersionedAuthorityRecord,
    digest_payload,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_ddl import ClickHouseClusterPublicationDdl
from dpone.runtime.sinks.clickhouse_cluster_publication_receipt import ClusterFullRefreshReceipt
from dpone.runtime.sinks.clickhouse_prepared_recovery import PreparedRecoveryService, plan_prepared_recovery


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
        self.published = False
        self.cleaned = False
        self.missing_host = False

    def inventory(self, cluster: str) -> ClusterInventory:
        return self.inventory_value

    def require_atomic_database(self, cluster: str, database: str, hosts: tuple[str, ...]) -> None:
        return None

    def candidate_counts(self, cluster: str, database: str, candidate: str) -> dict[str, int]:
        return {host: self.rows for host in self.inventory_value.hosts}

    def generations(
        self, cluster: str, database: str, target: str, candidate: str, hosts: tuple[str, ...]
    ) -> tuple[ReplicaGeneration, ...]:
        target_identity, candidate_identity = (
            (self.candidate, None if self.cleaned else self.target) if self.published else (self.target, self.candidate)
        )
        observed_hosts = hosts[:1] if self.missing_host else hosts
        return tuple(
            ReplicaGeneration(host, target_identity, candidate_identity, row_count=self.rows) for host in observed_hosts
        )


class Authority:
    def __init__(self, record: AuthorityRecord) -> None:
        self.current = VersionedAuthorityRecord(record, 0)
        self.mutations = 0

    def supports_linearizable_dispatch_permit(self) -> bool:
        return True

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
        expected_version=0,
        operation_started_at=datetime(2026, 9, 27, 10, 7, tzinfo=UTC),
    )


def test_plan_accepts_exact_prepared_generation() -> None:
    catalog, authority, ddl = _case()
    plan = _plan(catalog, authority, ddl)
    assert plan.operation_id == "original-operation"
    assert plan.authority_version == 0
    assert plan.candidate_identity == catalog.candidate
    assert plan.predecessor_identity == catalog.target
    assert plan.query_digest == "query-digest"
    assert plan.plan_digest
    public_payload = plan.to_public_dict()
    assert public_payload["correlation_id"] == plan.token
    assert public_payload["replica_summary"] == {"expected": 2, "candidate_ready": 2}
    public = str(public_payload)
    assert "original-operation" not in public and "'new'" not in public and "'old'" not in public
    assert authority.mutations == ddl.dispatches == 0


def test_plan_identity_is_stable_across_read_only_preflights() -> None:
    catalog, authority, ddl = _case()
    first = _plan(catalog, authority, ddl)
    second = _plan(catalog, authority, ddl)
    assert first.token == second.token
    assert first.plan_digest == second.plan_digest


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


def test_plan_rejects_noninitial_prepared_version_even_with_negative_logs() -> None:
    catalog, authority, ddl = _case()
    authority.current = replace(authority.current, version=1)
    with pytest.raises(ClusterPublicationError, match="AUTHORITY_UNSAFE"):
        plan_prepared_recovery(
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
    assert ddl.dispatches == 0


def test_plan_rejects_unadmitted_authority_even_with_negative_logs() -> None:
    catalog, authority, ddl = _case()
    authority.supports_linearizable_dispatch_permit = lambda: False  # type: ignore[method-assign]
    with pytest.raises(ClusterPublicationError, match="AUTHORITY_UNSAFE"):
        _plan(catalog, authority, ddl)
    assert ddl.dispatches == 0


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


class MutationAuthority(Authority):
    def __init__(self, record: AuthorityRecord, *, strict: bool = True) -> None:
        super().__init__(record)
        self.strict = strict
        self.status = AuthorityMutationStatus.VERIFIED

    def supports_linearizable_dispatch_permit(self) -> bool:
        return self.strict

    def compare_and_swap(self, current: VersionedAuthorityRecord, desired: AuthorityRecord) -> AuthorityMutationResult:
        self.mutations += 1
        if self.status is not AuthorityMutationStatus.VERIFIED:
            return AuthorityMutationResult(self.status)
        self.current = VersionedAuthorityRecord(desired, current.version + 1)
        permit = DispatchPermit(desired.target_key, desired.operation_id, desired.fence_token, desired.dispatch_epoch)
        return AuthorityMutationResult(AuthorityMutationStatus.VERIFIED, self.current, permit)


class RecoveryDdl(Ddl):
    def __init__(self, catalog: Catalog) -> None:
        super().__init__()
        self.catalog = catalog
        self.raise_after_dispatch = False
        self.queue_failed = False
        self.last_token: str | None = None

    def dispatch_publication(self, record: AuthorityRecord, permit: DispatchPermit, *, cluster: str) -> None:
        self.dispatches += 1
        self.last_token = record.ddl_correlation_token
        self.catalog.published = True
        if self.raise_after_dispatch:
            raise TimeoutError("acknowledgement lost")

    def find_entries(self, cluster: str, token: str) -> tuple[QueueEntry, ...]:
        return (
            QueueEntry(
                "query-1",
                "query-digest",
                token,
                tuple(
                    QueueHostResult(
                        host, "Finished", 1 if self.queue_failed else 0, "failed" if self.queue_failed else ""
                    )
                    for host in self.catalog.inventory_value.hosts
                ),
            ),
        )

    def read_entry(self, cluster: str, entry: str) -> QueueEntry | None:
        return self.find_entries(cluster, self.last_token or "missing")[0] if entry == "query-1" else None


class RecoveryPublication:
    def __init__(self, authority: MutationAuthority, catalog: Catalog) -> None:
        self.authority = authority
        self.catalog = catalog

    def reconcile(self, current: VersionedAuthorityRecord, cluster: str) -> ClusterFullRefreshReceipt:
        committed = replace(current.record, phase=AuthorityPhase.COMMITTED, ddl_entry="query-1")
        self.authority.current = VersionedAuthorityRecord(committed, current.version + 1)
        return ClusterFullRefreshReceipt.from_authority(self.authority.current, cluster)

    def cleanup(self, receipt: ClusterFullRefreshReceipt) -> None:
        self.catalog.cleaned = True
        current = self.authority.current
        self.authority.current = VersionedAuthorityRecord(
            replace(current.record, phase=AuthorityPhase.COMPLETED), current.version + 1
        )


def _execution_case(*, strict: bool = True):
    catalog, initial, _ = _case()
    authority = MutationAuthority(initial.current.record, strict=strict)
    ddl = RecoveryDdl(catalog)
    plan = _plan(catalog, authority, ddl)
    publication = RecoveryPublication(authority, catalog)
    service = PreparedRecoveryService(catalog, authority, ddl, publication.reconcile, publication.cleanup)
    return plan, service, authority, ddl


def test_execute_refuses_legacy_authority() -> None:
    plan, service, authority, ddl = _execution_case()
    authority.strict = False
    with pytest.raises(ClusterPublicationError, match="AUTHORITY_UNSAFE"):
        service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert authority.mutations == ddl.dispatches == 0


def test_execute_rejects_stale_plan() -> None:
    plan, service, authority, ddl = _execution_case()
    authority.current = replace(authority.current, version=authority.current.version + 1)
    with pytest.raises(ClusterPublicationError, match="AUTHORITY_CONFLICT"):
        service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert authority.mutations == ddl.dispatches == 0


@pytest.mark.parametrize("status", [AuthorityMutationStatus.CONFLICT, AuthorityMutationStatus.OUTCOME_UNKNOWN])
def test_only_cas_winner_dispatches(status: AuthorityMutationStatus) -> None:
    plan, service, authority, ddl = _execution_case()
    authority.status = status
    with pytest.raises(ClusterPublicationError, match="CAS_"):
        service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert authority.mutations == 1
    assert ddl.dispatches == 0


def test_execute_strict_winner_dispatches_once_and_returns_original_receipt() -> None:
    plan, service, authority, ddl = _execution_case()
    receipt = service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert ddl.dispatches == 1
    assert receipt.authority.operation_id == plan.operation_id
    assert authority.current.record.phase is AuthorityPhase.COMPLETED


def test_execute_and_replay_reject_terminal_failed_ddl_consistently() -> None:
    plan, service, authority, ddl = _execution_case()
    ddl.queue_failed = True
    with pytest.raises(ClusterPublicationError, match="PUBLICATION_UNKNOWN"):
        service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert authority.current.record.phase is AuthorityPhase.COMPLETED
    with pytest.raises(ClusterPublicationError, match="PUBLICATION_UNKNOWN"):
        service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert ddl.dispatches == 1


def test_crash_after_cas_reconciles_without_redispatch() -> None:
    plan, service, authority, ddl = _execution_case()
    ddl.raise_after_dispatch = True
    receipt = service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert ddl.dispatches == 1
    assert receipt.authority.operation_id == plan.operation_id
    assert authority.current.record.phase is AuthorityPhase.COMPLETED


def test_completed_operation_replays_receipt_without_dispatch() -> None:
    plan, service, authority, ddl = _execution_case()
    original = service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    replay = service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert replay.authority.operation_id == original.authority.operation_id
    assert replay.authority.phase is AuthorityPhase.COMPLETED
    assert ddl.dispatches == 1


def test_completed_replay_accepts_new_strict_cas_write_identity() -> None:
    plan, service, authority, ddl = _execution_case()
    service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    authority.current = replace(
        authority.current,
        record=replace(authority.current.record, authority_write_id="final-strict-cas-write"),
    )
    receipt = service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert receipt.authority.phase is AuthorityPhase.COMPLETED
    assert ddl.dispatches == 1


def test_completed_replay_rejects_missing_replica() -> None:
    plan, service, authority, ddl = _execution_case()
    service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    service.catalog.missing_host = True
    with pytest.raises(ClusterPublicationError, match="PUBLICATION_UNKNOWN"):
        service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert authority.current.record.phase is AuthorityPhase.COMPLETED
    assert ddl.dispatches == 1


def test_completed_replay_rejects_residual_predecessor() -> None:
    plan, service, authority, ddl = _execution_case()
    service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    service.catalog.cleaned = False
    with pytest.raises(ClusterPublicationError, match="PUBLICATION_UNKNOWN"):
        service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert authority.current.record.phase is AuthorityPhase.COMPLETED
    assert ddl.dispatches == 1


def test_completed_replay_rejects_foreign_ddl_identity() -> None:
    plan, service, authority, ddl = _execution_case()
    service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    authority.current = replace(
        authority.current,
        record=replace(authority.current.record, ddl_correlation_token="foreign-token"),
    )
    with pytest.raises(ClusterPublicationError, match="AUTHORITY_CONFLICT"):
        service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert ddl.dispatches == 1


def test_reentry_after_dispatch_reconciles_without_second_ddl() -> None:
    plan, service, authority, ddl = _execution_case()
    dispatched = plan.record.dispatching(token=plan.token, query_digest=plan.query_digest)
    authority.current = VersionedAuthorityRecord(dispatched, plan.authority_version + 1)
    ddl.catalog.published = True
    ddl.last_token = plan.token
    ddl.dispatches = 1
    receipt = service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert receipt.authority.phase is AuthorityPhase.COMPLETED
    assert ddl.dispatches == 1


def test_reentry_during_cleanup_does_not_repeat_publication() -> None:
    plan, service, authority, ddl = _execution_case()
    dispatched = plan.record.dispatching(token=plan.token, query_digest=plan.query_digest)
    authority.current = VersionedAuthorityRecord(
        replace(dispatched, phase=AuthorityPhase.CLEANUP_DISPATCHING, dispatch_epoch=2, ddl_entry="query-1"),
        plan.authority_version + 2,
    )
    ddl.catalog.published = True
    ddl.last_token = plan.token
    ddl.dispatches = 1
    receipt = service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert receipt.authority.phase is AuthorityPhase.COMPLETED
    assert ddl.dispatches == 1


def test_execute_rejects_changed_target_after_plan() -> None:
    plan, service, authority, ddl = _execution_case()
    service.catalog.candidate = _identity("other")
    with pytest.raises(ClusterPublicationError, match="GENERATION_DIVERGED"):
        service.execute_prepared_recovery(plan, confirmation_digest=plan.plan_digest)
    assert authority.mutations == ddl.dispatches == 0
