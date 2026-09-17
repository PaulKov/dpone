"""Pure marker and UUID-state tests for ClickHouse full-refresh publication."""

from __future__ import annotations

import pytest

from dpone.runtime.sinks.clickhouse_full_refresh_contract import (
    ClickHouseFullRefreshPublicationError,
    FullRefreshPublicationMarker,
    PublicationState,
    classify_publication,
)

_OLD = "11111111-1111-1111-1111-111111111111"
_NEW = "22222222-2222-2222-2222-222222222222"


def _marker() -> FullRefreshPublicationMarker:
    return FullRefreshPublicationMarker.create(
        operation_id="operation",
        database="analytics",
        target="target",
        candidate="candidate",
        predecessor_uuid=_OLD,
        desired_uuid=_NEW,
        staged_rows=9,
    )


def test_marker_round_trip_and_exact_uuid_classification() -> None:
    marker = _marker()

    assert FullRefreshPublicationMarker.from_json(marker.to_json()) == marker
    assert classify_publication(marker, target_uuid=_OLD, candidate_uuid=_NEW) is PublicationState.PENDING
    assert classify_publication(marker, target_uuid=_NEW, candidate_uuid=_OLD) is PublicationState.COMMITTED
    assert classify_publication(marker, target_uuid=_NEW, candidate_uuid=None) is PublicationState.CLEANUP_PENDING
    assert classify_publication(marker, target_uuid=_NEW, candidate_uuid=_NEW) is PublicationState.UNKNOWN


def test_marker_digest_tampering_fails_closed() -> None:
    marker = _marker()

    with pytest.raises(ClickHouseFullRefreshPublicationError, match="plan digest mismatch"):
        FullRefreshPublicationMarker.from_json(marker.to_json().replace('"staged_rows":9', '"staged_rows":8'))
