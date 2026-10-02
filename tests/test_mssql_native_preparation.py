"""Operation-specific immutable SQL history, never a minted dispatch permit."""

from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from dpone.contracts.clickhouse_cluster_publication import (
    QUALITY_SCHEMA_VERSION,
    ClusterPublicationError,
    VersionedAuthorityRecord,
)
from dpone.contracts.clickhouse_cluster_publication import AuthorityPhase as Phase
from dpone.contracts.quality_replay import QualityReplayCapsule
from tests.test_mssql_publication_authority import BINDING_DIGEST, WRITE_ID, Transport, authority, receipt, record
from tests.test_mssql_publication_retirement import retired_receipt, retired_record
from tests.test_publication_retirement import plan
from tests.test_quality_replay_capsule import core

CREATED = datetime(2026, 1, 2, 3, 4, 5)


class HistoryTransport(Transport):
    def __init__(self, current_rows, preparation_rows, failure=None):
        super().__init__(current_rows, failure)
        self.rowsets = [current_rows, preparation_rows]
        self.commits = 0
        self.closes = 0

    def execute(self, sql, params):
        super().execute(sql, params)
        self.rows = self.rowsets[len(self.calls) - 1]

    def commit(self):
        self.commits += 1
        super().commit()

    def close(self):
        self.closes += 1


def fixture(kind="initial", phase=Phase.PREPARED):
    first = replace(record(), authority_write_id=WRITE_ID.hex)
    root = receipt(first, 1, won=0)
    previous, previous_row, version = None, (None,) * 13, 1
    if kind == "later":
        previous = replace(first, phase=Phase.COMPLETED, dispatch_epoch=1, ddl_entry="query-old")
        previous_row, version = receipt(previous, 4, won=0), 5
        first = replace(
            first,
            operation_id="later-op",
            fence_token="later-fence",
            dispatch_epoch=2,
            candidate="later-candidate",
            desired=replace(first.desired, uuid="later", keeper_path="/later"),
            predecessor=previous.desired,
            authority_write_id=uuid4().hex,
        )
    if kind == "retired":
        retirement = plan()
        previous = retired_record(retirement)
        root = previous_row = retired_receipt(retirement, won=0)
        version = 2
        first = replace(
            previous,
            phase=Phase.PREPARED,
            operation_id="fresh-op",
            fence_token="fresh-fence",
            candidate="fresh-candidate",
            desired=replace(previous.desired, uuid="fresh", keeper_path="/fresh"),
            dispatch_epoch=1,
            authority_write_id=uuid4().hex,
        )
    current = first
    current_version = version
    if phase != Phase.PREPARED:
        current = replace(
            first.dispatching(token="publish-one", query_digest="e" * 64),
            phase=phase,
            ddl_entry=None if phase == Phase.DISPATCHING else "query-one",
            authority_write_id=uuid4().hex,
        )
        current_version += 1 if phase == Phase.DISPATCHING else 3
    return (
        first,
        version,
        current,
        current_version,
        HistoryTransport(
            [receipt(current, current_version, won=0) + root],
            [receipt(first, version, won=0) + previous_row + (CREATED,)],
        ),
    )


@pytest.mark.parametrize("kind", ["initial", "later", "retired"])
@pytest.mark.parametrize("phase", [Phase.PREPARED, Phase.DISPATCHING, Phase.COMMITTED, Phase.COMPLETED])
def test_admission_binds_actual_operation_origin_not_fixed_revision(kind, phase):
    prepared, version, current, current_version, transport = fixture(kind, phase)
    result = authority(transport).read_native_preparation(current.target_key, current.operation_id)
    assert result.binding_digest == BINDING_DIGEST
    assert result.prepared == VersionedAuthorityRecord(prepared, version)
    assert result.current == VersionedAuthorityRecord(current, current_version)
    assert result.prepared_at == CREATED.replace(tzinfo=UTC)
    assert not hasattr(result, "permit")
    assert transport.commits == 1 and transport.closes == 2
    assert len(transport.calls) == 2
    assert all("INSERT" not in sql and "UPDATE" not in sql for sql, _ in transport.calls)
    assert transport.calls[1][1][-1] == current.operation_id


@pytest.mark.parametrize("position", [0, 1])
@pytest.mark.parametrize(
    "column,value",
    [
        (1, 0),
        (2, b"{}"),
        (3, b"x" * 32),
        (4, "not-a-uuid"),
        (5, b"x" * 32),
        (6, 0),
        (7, 0),
        (8, "legacy_adopted"),
        (9, b"forged"),
        (10, b"x" * 32),
        (11, "other-operation"),
        (12, "DISPATCHING"),
    ],
)
def test_invalid_preparation_or_predecessor_receipt_cannot_admit(position, column, value):
    _, _, current, _, transport = fixture("later")
    raw = list(transport.rowsets[1][0])
    raw[position * 13 + column] = value
    transport.rowsets[1] = [tuple(raw)]
    with pytest.raises(ClusterPublicationError, match="PREPARATION_UNVERIFIED"):
        authority(transport).read_native_preparation(current.target_key, current.operation_id)


@pytest.mark.parametrize("defect", ["absent", "duplicate", "initial-predecessor", "timestamp", "future-origin"])
def test_incomplete_or_ambiguous_preparation_is_not_a_proof(defect):
    _, _, current, _, transport = fixture()
    rows = transport.rowsets[1]
    if defect == "absent":
        rows.clear()
    elif defect == "duplicate":
        rows.append(rows[0])
    else:
        raw = list(rows[0])
        if defect == "initial-predecessor":
            raw[13:26] = receipt(current, 1, won=0)
        elif defect == "timestamp":
            raw[-1] = "2026-01-02T03:04:05"
        else:
            raw[1] = 2
        rows[0] = tuple(raw)
    with pytest.raises(ClusterPublicationError, match="PREPARATION_UNVERIFIED"):
        authority(transport).read_native_preparation(current.target_key, current.operation_id)


@pytest.mark.parametrize(
    "field,value",
    [
        ("operation_id", "different"),
        ("fence_token", "different"),
        ("staged_rows", 999),
        ("candidate", "different"),
        ("target", "different"),
        ("dispatch_epoch", 17),
    ],
)
def test_current_operation_cannot_substitute_preparation_identity(field, value):
    _, _, current, _, transport = fixture("later", Phase.COMPLETED)
    raw = transport.rowsets[0][0]
    altered = replace(current, **{field: value})
    transport.rowsets[0] = [receipt(altered, 8, won=0) + raw[13:]]
    with pytest.raises(ClusterPublicationError, match="PREPARATION_UNVERIFIED"):
        authority(transport).read_native_preparation(current.target_key, current.operation_id)


def test_retired_operation_remains_prohibited_after_fresh_completion():
    _, _, current, _, transport = fixture("retired", Phase.COMPLETED)
    with pytest.raises(ClusterPublicationError, match="RETIRED_OPERATION"):
        authority(transport).read_native_preparation(current.target_key, plan().original.record.operation_id)
    assert len(transport.calls) == 1


@pytest.mark.parametrize("failure", ["commit", "after_output"])
def test_ambiguous_read_does_not_return_native_origin_or_retry(failure):
    _, _, current, _, transport = fixture()
    transport.failure = failure
    with pytest.raises(ClusterPublicationError, match="PREPARATION_UNVERIFIED"):
        authority(transport).read_native_preparation(current.target_key, current.operation_id)
    assert len(transport.calls) <= 2


@pytest.mark.parametrize("phase", [Phase.COMMITTED, Phase.CLEANUP_DISPATCHING, Phase.COMPLETED])
def test_current_revision_cannot_skip_required_native_transitions(phase):
    _, version, current, _, transport = fixture("later", Phase.COMMITTED)
    current = replace(current, phase=phase)
    if phase is Phase.CLEANUP_DISPATCHING:
        current = replace(
            current,
            dispatch_epoch=current.dispatch_epoch + 1,
            cleanup_correlation_token="cleanup",
            cleanup_query_digest="f" * 64,
        )
    transport.rowsets[0] = [receipt(current, version + 1, won=0) + transport.rowsets[0][0][13:]]
    with pytest.raises(ClusterPublicationError, match="PREPARATION_UNVERIFIED"):
        authority(transport).read_native_preparation(current.target_key, current.operation_id)


@pytest.mark.parametrize("phase", [Phase.CLEANUP_DISPATCHING, Phase.COMPLETED])
@pytest.mark.parametrize("defect", [None, "token", "digest", "entry", "epoch"])
def test_cleanup_observation_requires_complete_phase_consistent_intent(phase, defect):
    _, _, current, _, transport = fixture("later", Phase.COMMITTED)
    current = replace(
        current,
        phase=phase,
        dispatch_epoch=current.dispatch_epoch + 1,
        cleanup_correlation_token="cleanup",
        cleanup_query_digest="f" * 64,
        cleanup_entry="cleanup-entry" if phase is Phase.COMPLETED else None,
    )
    if defect:
        changes = {
            "token": {"cleanup_correlation_token": None},
            "digest": {"cleanup_query_digest": None},
            "entry": {"cleanup_entry": None if phase is Phase.COMPLETED else "unexpected"},
            "epoch": {"dispatch_epoch": current.dispatch_epoch - 1},
        }
        current = replace(current, **changes[defect])
    transport.rowsets[0] = [receipt(current, 10, won=0) + transport.rowsets[0][0][13:]]
    if defect:
        with pytest.raises(ClusterPublicationError, match="PREPARATION_UNVERIFIED"):
            authority(transport).read_native_preparation(current.target_key, current.operation_id)
    else:
        assert (
            authority(transport).read_native_preparation(current.target_key, current.operation_id).current.record
            == current
        )


@pytest.mark.parametrize("defect", [None, "core", "malformed", "added", "removed"])
def test_quality_evolution_preserves_core_but_does_not_authorize_it(defect):
    prepared, _, current, version, transport = fixture(phase=Phase.COMPLETED)
    capsule = QualityReplayCapsule.prepare(core())
    prepared = replace(prepared, schema_version=QUALITY_SCHEMA_VERSION, quality_evidence=capsule.payload)
    current = replace(
        current,
        schema_version=QUALITY_SCHEMA_VERSION,
        quality_evidence=capsule.advance("COMPLETE", authority_version=version).payload,
    )
    if defect == "core":
        current = replace(
            current, quality_evidence=QualityReplayCapsule.prepare({**core(), "load_id": "other"}).payload
        )
    elif defect == "malformed":
        current = replace(current, quality_evidence="{}")
    elif defect == "added":
        prepared = replace(prepared, schema_version="dpone.clickhouse.cluster-full-refresh.v1", quality_evidence=None)
    elif defect == "removed":
        current = replace(current, schema_version="dpone.clickhouse.cluster-full-refresh.v1", quality_evidence=None)
    root = receipt(prepared, 1, won=0)
    transport.rowsets = [[receipt(current, version, won=0) + root], [root + (None,) * 13 + (CREATED,)]]
    if defect:
        with pytest.raises(ClusterPublicationError, match="PREPARATION_UNVERIFIED"):
            authority(transport).read_native_preparation(current.target_key, current.operation_id)
    else:
        result = authority(transport).read_native_preparation(current.target_key, current.operation_id)
        assert result.prepared.record == prepared and result.current.record == current


@pytest.mark.parametrize("created", [None, 1, CREATED.replace(tzinfo=UTC)])
def test_non_sql_timestamp_cannot_be_an_origin(created):
    _, _, current, _, transport = fixture()
    transport.rowsets[1] = [transport.rowsets[1][0][:-1] + (created,)]
    with pytest.raises(ClusterPublicationError, match="PREPARATION_UNVERIFIED"):
        authority(transport).read_native_preparation(current.target_key, current.operation_id)


@pytest.mark.parametrize("defect", ["missing-current", "late-origin-error", "cursor-close", "session-close"])
def test_incomplete_transaction_cannot_release_observation(defect):
    _, _, current, _, transport = fixture()
    if defect == "missing-current":
        transport.rowsets[0].clear()
    if defect == "late-origin-error":

        def nextset():
            if len(transport.calls) == 2:
                raise RuntimeError("synthetic late error")
            return None

        transport.nextset = nextset
    if defect in {"cursor-close", "session-close"}:

        def close():
            transport.closes += 1
            if transport.closes == (1 if defect == "cursor-close" else 2):
                raise RuntimeError("synthetic close error")

        transport.close = close
    with pytest.raises(ClusterPublicationError, match="PREPARATION_UNVERIFIED"):
        authority(transport).read_native_preparation(current.target_key, current.operation_id)


@pytest.mark.parametrize("invalid", ["", False, 0, [], {}])
@pytest.mark.parametrize("location", ["prepared", "current", "both"])
def test_falsy_malformed_capsule_is_never_admitted(invalid, location):
    prepared, _, current, version, transport = fixture(phase=Phase.COMPLETED)
    valid = QualityReplayCapsule.prepare(core()).payload
    prepared = replace(
        prepared,
        schema_version=QUALITY_SCHEMA_VERSION,
        quality_evidence=invalid if location in {"prepared", "both"} else valid,
    )
    current = replace(
        current,
        schema_version=QUALITY_SCHEMA_VERSION,
        quality_evidence=invalid if location in {"current", "both"} else valid,
    )
    # One or both sides have an invalid capsule; two falsy values must not
    # normalize to the same nonexistent core.
    root = receipt(prepared, 1, won=0)
    transport.rowsets = [[receipt(current, version, won=0) + root], [root + (None,) * 13 + (CREATED,)]]
    with pytest.raises(ClusterPublicationError, match="PREPARATION_UNVERIFIED"):
        authority(transport).read_native_preparation(current.target_key, current.operation_id)
