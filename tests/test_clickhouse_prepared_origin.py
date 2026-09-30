"""Operation provenance, not a slot's initial version, admits recovery."""

import json
from dataclasses import replace
from datetime import UTC, datetime

import pytest

from dpone.ports.clickhouse_cluster_publication import contracts, require_verified_mutation
from dpone.runtime.sinks.clickhouse_prepared_recovery import plan_prepared_recovery
from tests.test_clickhouse_prepared_recovery import _case
from tests.test_clickhouse_quality_authority import AtomicConnector, ready, record


def _complete(authority, current):
    current = require_verified_mutation(
        authority.compare_and_swap(current, current.record.dispatching(token="token", query_digest="query")),
        permit=True,
    )
    current = require_verified_mutation(
        authority.compare_and_swap(
            current, replace(current.record, phase=contracts.AuthorityPhase.COMMITTED, ddl_entry="entry")
        ),
        permit=False,
    )
    return require_verified_mutation(
        authority.compare_and_swap(current, replace(current.record, phase=contracts.AuthorityPhase.COMPLETED)),
        permit=False,
    )


def _plan(catalog, authority, ddl, current):
    return plan_prepared_recovery(
        catalog,
        authority,
        ddl,
        cluster="cluster",
        database="analytics",
        target="target",
        operation_id=current.record.operation_id,
        expected_version=current.version,
        operation_started_at=datetime(2026, 9, 27, tzinfo=UTC),
    )


def test_strict_reused_slots_admit_second_and_third_prepared_operations():
    catalog, original, ddl = _case()
    connector = AtomicConnector()
    authority = ready(connector)
    fresh = replace(original.current.record, authority_write_id=None)
    current = require_verified_mutation(authority.create_if_absent(fresh), permit=False)
    for operation, version, epoch in (("second", 4, 2), ("third", 8, 4)):
        completed = _complete(authority, current)
        desired = replace(fresh, operation_id=operation, fence_token=operation, dispatch_epoch=epoch)
        current = require_verified_mutation(authority.compare_and_swap(completed, desired), permit=False)
        plan = _plan(catalog, authority, ddl, current)
        assert plan.authority_version == version
        assert plan.record.dispatch_epoch == epoch
        assert plan.operation_id == operation
    assert ddl.dispatches == 0


def test_strict_acquisition_stamps_digest_bound_operation_origin():
    connector = AtomicConnector()
    authority = ready(connector)
    current = require_verified_mutation(authority.create_if_absent(record()), permit=False)
    origin = json.loads(current.record.prepared_origin)
    assert origin["kind"] == "strict-prepared-v1"
    assert origin["prepared_version"] == origin["prepared_epoch"] == 0
    assert origin["prepared_payload_sha256"] == record().payload_sha256
    advanced = _complete(authority, current)
    assert advanced.record.prepared_origin == current.record.prepared_origin


def test_same_operation_cannot_regress_to_prepared():
    connector = AtomicConnector()
    authority = ready(connector)
    prepared = require_verified_mutation(authority.create_if_absent(record()), permit=False)
    dispatched = require_verified_mutation(
        authority.compare_and_swap(prepared, prepared.record.dispatching(token="token", query_digest="query")),
        permit=True,
    )
    writes = len(connector.calls)
    with pytest.raises(contracts.ClusterPublicationError, match="INVALID"):
        authority.compare_and_swap(dispatched, prepared.record)
    assert len(connector.calls) == writes


def test_caller_cannot_forge_prepared_origin_on_acquisition():
    connector = AtomicConnector()
    authority = ready(connector)
    with pytest.raises(contracts.ClusterPublicationError, match="INVALID"):
        authority.create_if_absent(replace(record(), prepared_origin='{"kind":"forged"}'))
    assert connector.calls == []


def test_cas_binds_exact_observed_payload_not_only_version_and_phase():
    connector = AtomicConnector()
    authority = ready(connector)
    prepared = require_verified_mutation(authority.create_if_absent(record()), permit=False)
    authority.compare_and_swap(prepared, prepared.record.dispatching(token="token", query_digest="query"))
    sql, params, _ = connector.calls[-1]
    assert "payload_sha256 = %(expected_payload_sha256)s" in sql
    assert params["expected_payload_sha256"] == prepared.record.payload_sha256


def test_fabricated_current_payload_cannot_obtain_a_permit():
    connector = AtomicConnector()
    authority = ready(connector)
    prepared = require_verified_mutation(authority.create_if_absent(record()), permit=False)
    fabricated = replace(prepared, record=replace(prepared.record, staged_rows=99))
    result = authority.compare_and_swap(
        fabricated,
        fabricated.record.dispatching(token="forged", query_digest="query"),
    )
    assert result.permit is None
    assert result.status is contracts.AuthorityMutationStatus.CONFLICT
    assert authority.read_versioned(prepared.record.target_key) == prepared


@pytest.mark.parametrize("change", ["missing", "replaced"])
def test_transition_cannot_discard_or_replace_origin(change):
    connector = AtomicConnector()
    authority = ready(connector)
    prepared = require_verified_mutation(authority.create_if_absent(record()), permit=False)
    desired = prepared.record.dispatching(token="token", query_digest="query")
    desired = replace(desired, prepared_origin=None if change == "missing" else "{}")
    writes = len(connector.calls)
    with pytest.raises(contracts.ClusterPublicationError, match="INVALID"):
        authority.compare_and_swap(prepared, desired)
    assert len(connector.calls) == writes


def test_cleanup_governance_write_never_reissues_dispatch_permit():
    connector = AtomicConnector()
    authority = ready(connector)
    current = require_verified_mutation(authority.create_if_absent(record()), permit=False)
    current = require_verified_mutation(
        authority.compare_and_swap(current, current.record.dispatching(token="publish", query_digest="query")),
        permit=True,
    )
    current = require_verified_mutation(
        authority.compare_and_swap(current, replace(current.record, phase=contracts.AuthorityPhase.COMMITTED)),
        permit=False,
    )
    cleanup = authority.compare_and_swap(
        current,
        replace(
            current.record,
            phase=contracts.AuthorityPhase.CLEANUP_DISPATCHING,
            dispatch_epoch=current.record.dispatch_epoch + 1,
            cleanup_correlation_token="cleanup",
            cleanup_query_digest="cleanup-query",
        ),
    )
    current = require_verified_mutation(cleanup, permit=True)
    governance = authority.compare_and_swap(current, replace(current.record, quality_reader="guard"))
    assert governance.status is contracts.AuthorityMutationStatus.VERIFIED
    assert governance.permit is None


@pytest.mark.parametrize("field,value", [("staged_rows", 7), ("dispatch_epoch", 19)])
def test_prepared_origin_rejects_changed_record_before_dispatch(field, value):
    catalog, initial, ddl = _case()
    connector = AtomicConnector()
    authority = ready(connector)
    prepared = require_verified_mutation(
        authority.create_if_absent(replace(initial.current.record, authority_write_id=None)),
        permit=False,
    )
    changed = replace(prepared, record=replace(prepared.record, **{field: value}))
    connector.observe(changed.record, changed.version)
    with pytest.raises(contracts.ClusterPublicationError, match="AUTHORITY_UNSAFE"):
        _plan(catalog, authority, ddl, changed)
    assert ddl.dispatches == 0


def test_prepared_with_residual_cleanup_digest_is_not_undispatched():
    catalog, authority, ddl = _case()
    authority.current = replace(
        authority.current,
        record=replace(authority.current.record, cleanup_query_digest="old-cleanup"),
    )
    with pytest.raises(contracts.ClusterPublicationError, match="PUBLICATION_UNRESOLVED"):
        _plan(catalog, authority, ddl, authority.current)
    assert ddl.dispatches == 0
