"""Source-free recovery through shared lifecycle, strict CAS and held observations."""

from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from importlib import import_module
from types import SimpleNamespace
from uuid import uuid4

import pytest

from dpone.contracts.clickhouse_cluster_publication import (
    AuthorityMutationResult,
    DispatchPermit,
    QueueEntry,
    QueueHostResult,
    VersionedAuthorityRecord,
    ddl_query_digest,
)
from dpone.contracts.clickhouse_cluster_publication import (
    AuthorityMutationStatus as Status,
)
from dpone.contracts.clickhouse_cluster_publication import (
    AuthorityPhase as Phase,
)
from dpone.contracts.publication_preparation import NativePublicationPreparation
from dpone.contracts.publication_retirement import PublicationFreezeObservation
from dpone.runtime.sinks.clickhouse_cluster_publication_ddl import ClickHouseClusterPublicationDdl
from dpone.runtime.state.mssql_publication_envelope import require_transition
from tests.test_clickhouse_cluster_full_refresh_publication import _Catalog
from tests.test_mssql_publication_authority import record


class Authority:
    def __init__(self, catalog):
        value = replace(
            record(),
            database="analytics",
            target="target",
            candidate="candidate",
            desired=catalog.new,
            predecessor=catalog.old,
            inventory_digest=catalog.inventory("cluster").digest,
            authority_write_id=uuid4().hex,
        )
        from dpone.contracts.clickhouse_cluster_publication import digest_payload

        value = replace(
            value, target_key=digest_payload({"cluster": "cluster", "database": "analytics", "target": "target"})
        )
        self.prepared = self.current = VersionedAuthorityRecord(value, 1)
        self.binding = "a" * 64
        self.mutations = 0
        self.reads = 0
        self.outcome = Status.VERIFIED

    def read_native_preparation(self, target_key, operation_id):
        self.reads += 1
        assert (target_key, operation_id) == (self.current.record.target_key, self.current.record.operation_id)
        return NativePublicationPreparation(self.binding, self.prepared, self.current, datetime.fromtimestamp(100, UTC))

    def read_versioned(self, target_key):
        assert target_key == self.current.record.target_key
        return self.current

    def read_for_operation(self, target_key, operation_id):
        return self.read_native_preparation(target_key, operation_id).current

    def compare_and_swap(self, current, desired):
        self.mutations += 1
        if self.outcome is not Status.VERIFIED:
            return AuthorityMutationResult(
                self.outcome, observed=self.current if self.outcome is Status.CONFLICT else None
            )
        assert current == self.current
        dispatch = require_transition(current, desired)
        written = replace(desired, authority_write_id=uuid4().hex)
        self.current = VersionedAuthorityRecord(written, current.version + 1)
        return AuthorityMutationResult(
            Status.VERIFIED, self.current, DispatchPermit.for_record(written) if dispatch else None
        )


class Catalog(_Catalog):
    def __init__(self):
        super().__init__()
        self.entries = {}
        self.calls = []
        self.lose_reply = False
        self.connection = self

    def execute(self, query, settings, query_id):
        self.calls.append(query)
        if query.startswith("EXCHANGE"):
            self.committed = True
        elif query.startswith("DROP"):
            self.candidate_removed = True
        else:
            raise AssertionError("unexpected effect")
        entry = QueueEntry(
            str(len(self.calls)),
            ddl_query_digest(query),
            settings["log_comment"],
            tuple(QueueHostResult(host, "Finished", 0, "") for host in self.inventory("cluster").hosts),
        )
        self.entries[entry.entry] = entry
        if self.lose_reply:
            raise TimeoutError("synthetic loss after effect")

    def find_entries(self, cluster, token):
        return tuple(entry for entry in self.entries.values() if entry.correlation_token == token)

    def read_entry(self, cluster, entry):
        return self.entries.get(entry)


class Provider:
    def __init__(self, authority):
        self.authority = authority
        self.ensures = []
        self.databases = []

    def ensure(self, cluster, database, hosts):
        self.ensures.append((cluster, database, hosts))

    def for_database(self, database):
        self.databases.append(database)
        return self.authority


class Safety:
    def __init__(self, catalog):
        self.catalog = catalog
        self.held = False
        self.negative_reads = 0
        self.checks = 0
        self.fail_at = None
        self.alter = lambda value: value

    @contextmanager
    def hold(self, *, cluster, preparation):
        self.held = True
        try:
            yield self
        finally:
            self.held = False

    def require_held(self):
        self.checks += 1
        assert self.held
        if self.checks == self.fail_at:
            raise ValueError("held exclusion lost")

    def observe_unpublished(self, preparation):
        self.negative_reads += 1
        module = import_module("dpone.contracts.prepared_recovery")
        inventory = self.catalog.inventory("cluster")
        freeze = PublicationFreezeObservation(
            "trusted-test-issuer",
            "b" * 64,
            preparation.prepared.record.target_key,
            preparation.binding_digest,
            inventory.digest,
            ("other-writer",),
            ("other-writer",),
            110,
            120,
            200,
        )
        value = module.PreparedRecoverySafetyObservation(
            preparation.binding_digest,
            preparation.prepared.record.target_key,
            preparation.prepared.record.operation_id,
            preparation.prepared.version,
            preparation.prepared.record.payload_sha256,
            inventory,
            freeze,
            tuple(module.RecoveryReplicaHistory(host, 99, 130, "c" * 64) for host in inventory.hosts),
        )
        return self.alter(value)


def rig(*, binding="a" * 64):
    module = import_module("dpone.runtime.sinks.clickhouse_prepared_recovery")
    catalog = Catalog()
    authority = Authority(catalog)
    authority.binding = binding
    provider, safety = Provider(authority), Safety(catalog)
    ddl = ClickHouseClusterPublicationDdl(catalog, catalog)
    service = module.PreparedRecoveryService(
        catalog=catalog,
        provider=provider,
        ddl=ddl,
        safety=safety,
        binding_digest=binding,
        cluster="cluster",
        database="analytics",
        clock=lambda: 130,
    )
    return SimpleNamespace(
        service=service, authority=authority, provider=provider, catalog=catalog, safety=safety, ddl=ddl
    )


def plan(r):
    return r.service.plan(target="target", operation_id=r.authority.current.record.operation_id, expected_version=1)


def test_plan_is_read_only_and_execute_uses_one_existing_publication_lifecycle():
    r = rig()
    value = plan(r)
    assert r.authority.mutations == 0 and r.catalog.calls == []
    result = r.service.execute(value, confirmation_digest=value.digest)
    assert result.authority.phase is Phase.COMPLETED
    assert len(r.catalog.calls) == 2 and r.catalog.candidate_removed
    assert r.provider.databases and set(r.provider.databases) == {"analytics"}
    assert r.provider.ensures and not r.safety.held
    before = (r.authority.mutations, len(r.catalog.calls), r.safety.negative_reads)
    again = r.service.execute(value, confirmation_digest=value.digest)
    assert again == result
    assert (r.authority.mutations, len(r.catalog.calls), r.safety.negative_reads) == before


def test_confirmation_mismatch_has_no_additional_io():
    r = rig()
    value = plan(r)
    before = (r.authority.reads, len(r.provider.ensures), r.safety.checks)
    with pytest.raises(ValueError, match="confirmation"):
        r.service.execute(value, confirmation_digest="bad")
    assert (r.authority.reads, len(r.provider.ensures), r.safety.checks) == before
    assert r.catalog.calls == [] and r.authority.mutations == 0


@pytest.mark.parametrize("outcome", [Status.OUTCOME_UNKNOWN, Status.CONFLICT])
def test_unacknowledged_cas_does_not_dispatch_or_retry(outcome):
    r = rig()
    value = plan(r)
    r.authority.outcome = outcome
    with pytest.raises(Exception, match="CAS_UNKNOWN|CAS_CONFLICT"):
        r.service.execute(value, confirmation_digest=value.digest)
    assert r.authority.mutations == 1 and r.catalog.calls == []


def test_lost_ddl_reply_reconciles_and_never_redispatches():
    r = rig()
    value = plan(r)
    r.catalog.lose_reply = True
    result = r.service.execute(value, confirmation_digest=value.digest)
    assert result.authority.phase is Phase.COMPLETED and len(r.catalog.calls) == 2


@pytest.mark.parametrize("change", ["binding", "operation", "preparation", "candidate", "inventory"])
def test_changed_plan_or_observed_scope_causes_no_effect(change):
    r = rig()
    value = plan(r)
    if change == "binding":
        r.authority.binding = "e" * 64
    elif change == "operation":
        r.authority.current = replace(
            r.authority.current, record=replace(r.authority.current.record, operation_id="other")
        )
    elif change == "preparation":
        r.authority.prepared = replace(r.authority.prepared, version=2)
    elif change == "candidate":
        r.catalog.new = replace(r.catalog.new, uuid="other")
    else:
        r.catalog.inventory_address = "127.0.0.9"
    with pytest.raises(Exception):
        r.service.execute(value, confirmation_digest=value.digest)
    assert r.catalog.calls == [] and r.authority.mutations == 0


@pytest.mark.parametrize(
    "defect", ["missing", "duplicate", "gap", "late-start", "short", "ddl", "pending", "expired", "writers"]
)
def test_incomplete_held_history_blocks_prepared_mutation(defect):
    r = rig()
    value = plan(r)

    def alter(observed):
        histories = observed.histories
        if defect == "missing":
            histories = histories[:-1]
        elif defect == "duplicate":
            histories = histories + histories[:1]
        elif defect in {"gap", "late-start", "short", "ddl", "pending"}:
            changes = {
                "gap": {"gaps": ((110, 111),)},
                "late-start": {"history_from": 101},
                "short": {"history_through": 129},
                "ddl": {"matching_ddl_entries": ("prior",)},
                "pending": {"pending_requests": ("pending",)},
            }
            histories = (replace(histories[0], **changes[defect]),) + histories[1:]
        freeze = observed.freeze
        if defect == "expired":
            freeze = replace(freeze, expires_at=130)
        elif defect == "writers":
            freeze = replace(freeze, excluded_writers=())
        return replace(observed, histories=histories, freeze=freeze)

    r.safety.alter = alter
    with pytest.raises(ValueError):
        r.service.execute(value, confirmation_digest=value.digest)
    assert r.catalog.calls == [] and r.authority.mutations == 0


@pytest.mark.parametrize("change", ["missing", "unhealthy", "candidate-left"])
def test_completed_marker_needs_fresh_physical_proof(change):
    r = rig()
    value = plan(r)
    r.service.execute(value, confirmation_digest=value.digest)
    if change == "missing":
        r.catalog.target_missing = True
    elif change == "unhealthy":
        r.catalog.target_healthy = False
    else:
        r.catalog.candidate_removed = False
    before = len(r.catalog.calls)
    with pytest.raises(Exception):
        r.service.execute(value, confirmation_digest=value.digest)
    assert len(r.catalog.calls) == before


@pytest.mark.parametrize("phase", [Phase.DISPATCHING, Phase.COMMITTED, Phase.CLEANUP_DISPATCHING])
def test_resume_observes_original_effects_without_repeating_them(phase):
    from dpone.runtime.sinks.clickhouse_cluster_publication_recovery import reconcile_existing

    r = rig()
    value = plan(r)
    dispatching = r.authority.current.record.dispatching(
        token=value.dispatch_token, query_digest=value.dispatch_query_digest
    )
    mutation = r.authority.compare_and_swap(r.authority.current, dispatching)
    r.ddl.dispatch_publication(dispatching, mutation.permit, cluster="cluster")
    if phase is not Phase.DISPATCHING:
        reconcile_existing(r.catalog, r.ddl, r.authority, r.authority.current, "cluster")
    if phase is Phase.CLEANUP_DISPATCHING:
        cleanup = replace(
            r.authority.current.record,
            phase=phase,
            dispatch_epoch=r.authority.current.record.dispatch_epoch + 1,
            cleanup_correlation_token="cleanup-original",
        )
        cleanup = replace(cleanup, cleanup_query_digest=r.ddl.cleanup_query_digest(cleanup, cluster="cluster"))
        mutation = r.authority.compare_and_swap(r.authority.current, cleanup)
        r.ddl.drop_predecessor(cleanup, mutation.permit, cluster="cluster")
    before = r.safety.negative_reads
    result = r.service.execute(value, confirmation_digest=value.digest)
    assert result.authority.phase is Phase.COMPLETED
    assert len(r.catalog.calls) == 2 and r.safety.negative_reads == before


def test_unknown_cas_that_committed_cannot_mint_a_second_dispatch_permit():
    r = rig()
    value = plan(r)
    compare = r.authority.compare_and_swap

    def lose_ack(current, desired):
        compare(current, desired)
        return AuthorityMutationResult(Status.OUTCOME_UNKNOWN)

    r.authority.compare_and_swap = lose_ack
    with pytest.raises(Exception, match="CAS_UNKNOWN"):
        r.service.execute(value, confirmation_digest=value.digest)
    r.authority.compare_and_swap = compare
    with pytest.raises(Exception, match="DDL_UNKNOWN"):
        r.service.execute(value, confirmation_digest=value.digest)
    assert r.catalog.calls == [] and r.authority.mutations == 1


@pytest.mark.parametrize("phase", [Phase.DISPATCHING, Phase.CLEANUP_DISPATCHING])
def test_hold_loss_after_winning_cas_never_dispatches(phase):
    r = rig()
    value = plan(r)
    compare = r.authority.compare_and_swap

    def lose_hold(current, desired):
        result = compare(current, desired)
        if desired.phase is phase:
            r.safety.fail_at = r.safety.checks + 1
        return result

    r.authority.compare_and_swap = lose_hold
    with pytest.raises(Exception):
        r.service.execute(value, confirmation_digest=value.digest)
    assert len(r.catalog.calls) == (0 if phase is Phase.DISPATCHING else 1)
    assert r.authority.current.record.phase is phase
    assert not r.safety.held


def test_unobserved_dispatch_is_not_repeated_on_second_execute():
    r = rig()
    value = plan(r)

    def fail_transport(*args, **kwargs):
        raise TimeoutError("no effect observable")

    r.catalog.execute = fail_transport
    for _ in range(2):
        with pytest.raises(Exception, match="DDL_UNKNOWN"):
            r.service.execute(value, confirmation_digest=value.digest)
    assert r.catalog.calls == [] and r.authority.mutations == 1


@pytest.mark.parametrize("change", ["zero", "quality", "reader", "revision", "current"])
def test_unsupported_preparation_cannot_even_be_planned(change):
    r = rig()
    if change == "revision":
        r.authority.prepared = r.authority.current = replace(r.authority.current, version=2)
    elif change == "current":
        r.authority.current = replace(r.authority.current, version=2)
    else:
        fields = {"zero": {"staged_rows": 0}, "quality": {"quality_evidence": "{}"}, "reader": {"quality_reader": "{}"}}
        r.authority.prepared = r.authority.current = replace(
            r.authority.current, record=replace(r.authority.current.record, **fields[change])
        )
    with pytest.raises(ValueError):
        plan(r)
    assert r.catalog.calls == [] and r.authority.mutations == 0


def test_later_native_preparation_revision_uses_original_nonzero_epoch():
    r = rig()
    r.authority.prepared = r.authority.current = replace(
        r.authority.current,
        version=14,
        record=replace(r.authority.current.record, dispatch_epoch=8),
    )
    value = r.service.plan(target="target", operation_id="operation", expected_version=14)
    result = r.service.execute(value, confirmation_digest=value.digest)
    assert result.authority.dispatch_epoch == 10
    assert result.authority_version == 18 and len(r.catalog.calls) == 2


@pytest.mark.parametrize("defect", ["counts", "inventory", "duplicate", "ddl-active", "ddl-missing", "ddl-digest"])
def test_completed_requires_exact_counts_inventory_and_terminal_ddl(defect):
    r = rig()
    value = plan(r)
    r.service.execute(value, confirmation_digest=value.digest)
    if defect == "counts":
        r.catalog.candidate_counts = lambda *args: {"node-1": 2, "node-2": 1}
    elif defect == "inventory":
        r.catalog.inventory_address = "127.0.0.3"
    elif defect == "duplicate":
        observe = r.catalog.generations
        r.catalog.generations = lambda *args: observe(*args)[:1] * 2
    elif defect == "ddl-missing":
        r.catalog.entries.pop("1")
    else:
        entry = r.catalog.entries["1"]
        r.catalog.entries["1"] = replace(
            entry,
            **(
                {"query_digest": "other"}
                if defect == "ddl-digest"
                else {"hosts": tuple(QueueHostResult(host, "Active") for host in r.catalog.inventory("cluster").hosts)}
            ),
        )
    before = r.authority.mutations
    with pytest.raises(Exception):
        r.service.execute(value, confirmation_digest=value.digest)
    assert r.authority.mutations == before and len(r.catalog.calls) == 2


def test_completed_terminal_failure_with_physical_proof_keeps_normal_semantics():
    r = rig()
    value = plan(r)
    result = r.service.execute(value, confirmation_digest=value.digest)
    for key, entry in r.catalog.entries.items():
        r.catalog.entries[key] = replace(
            entry,
            hosts=tuple(
                QueueHostResult(host, "Finished", 1, "synthetic timeout")
                for host in r.catalog.inventory("cluster").hosts
            ),
        )
    assert r.service.execute(value, confirmation_digest=value.digest) == result
    assert len(r.catalog.calls) == 2


@pytest.mark.parametrize("phase", [Phase.DISPATCHING, Phase.COMMITTED])
@pytest.mark.parametrize("coverage", ["missing", "duplicate", "extra"])
def test_incomplete_replica_observation_never_authorizes_cleanup(phase, coverage):
    from dpone.runtime.sinks.clickhouse_cluster_publication_recovery import reconcile_existing

    r = rig()
    value = plan(r)
    dispatching = r.authority.current.record.dispatching(
        token=value.dispatch_token, query_digest=value.dispatch_query_digest
    )
    mutation = r.authority.compare_and_swap(r.authority.current, dispatching)
    r.ddl.dispatch_publication(dispatching, mutation.permit, cluster="cluster")
    if phase is Phase.COMMITTED:
        reconcile_existing(r.catalog, r.ddl, r.authority, r.authority.current, "cluster")
    observe = r.catalog.generations

    def incomplete(*args):
        result = observe(*args)
        return {
            "missing": result[:1],
            "duplicate": result[:1] * 2,
            "extra": result + (replace(result[0], host="unadmitted-host"),),
        }[coverage]

    r.catalog.generations = incomplete
    with pytest.raises(Exception):
        r.service.execute(value, confirmation_digest=value.digest)
    assert len(r.catalog.calls) == 1
    assert r.authority.current.record.phase is phase


@pytest.mark.parametrize("phase", [Phase.DISPATCHING, Phase.COMMITTED])
@pytest.mark.parametrize("counts", [{"node-1": 0, "node-2": 0}, {"node-1": 2}, {"node-1": True, "node-2": 2}])
def test_target_row_loss_or_unknown_counts_preserve_predecessor(phase, counts):
    from dpone.runtime.sinks.clickhouse_cluster_publication_recovery import reconcile_existing

    r = rig()
    value = plan(r)
    dispatching = r.authority.current.record.dispatching(
        token=value.dispatch_token, query_digest=value.dispatch_query_digest
    )
    mutation = r.authority.compare_and_swap(r.authority.current, dispatching)
    r.ddl.dispatch_publication(dispatching, mutation.permit, cluster="cluster")
    if phase is Phase.COMMITTED:
        reconcile_existing(r.catalog, r.ddl, r.authority, r.authority.current, "cluster")
    r.catalog.candidate_counts = lambda *args: counts
    with pytest.raises(Exception):
        r.service.execute(value, confirmation_digest=value.digest)
    assert len(r.catalog.calls) == 1 and not r.catalog.candidate_removed
    assert r.authority.current.record.phase is not Phase.COMPLETED
