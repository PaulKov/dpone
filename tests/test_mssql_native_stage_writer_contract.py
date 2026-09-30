from __future__ import annotations

from pathlib import Path

import pytest

from dpone.contracts.mssql_native_stage_writer import (
    NativeStageColumnMapping,
    NativeStageWriteMetrics,
    NativeStageWriteObservation,
    NativeStageWriteRequest,
    OperationDeadline,
)
from dpone.contracts.mssql_native_writer import SQLCLIENT_SESSION_PROOF


def _request(tmp_path: Path, **changes: object) -> NativeStageWriteRequest:
    values: dict[str, object] = {
        "attempt_id": "attempt-1",
        "qualified_stage": "[db].[stage].[raw_1]",
        "stage_id_sha256": "1" * 64,
        "owner_binding_sha256": "2" * 64,
        "object_id": 42,
        "schema_sha256": "3" * 64,
        "file_path": tmp_path / "chunk.bcp",
        "expected_rows": 7,
        "encoded_bytes": 11,
        "max_row_bytes": 11,
        "file_sha256": "4" * 64,
        "grant_token_sha256": "5" * 64,
        "proof_capability": SQLCLIENT_SESSION_PROOF,
        "wire_layout_sha256": "6" * 64,
        "columns": (
            NativeStageColumnMapping(0, "event_id", "bigint", False),
            NativeStageColumnMapping(1, "payload", "nvarchar(max)", True),
        ),
    }
    values.update(changes)
    return NativeStageWriteRequest(**values)  # type: ignore[arg-type]


def test_request_binds_exact_stage_file_layout_and_ordered_columns(tmp_path: Path) -> None:
    request = _request(tmp_path)

    assert request.columns[0].ordinal == 0
    assert request.columns[1].target_name == "payload"
    assert request.proof_capability == SQLCLIENT_SESSION_PROOF
    assert request.layout_version == 1


def test_sqlclient_request_explicitly_selects_persisted_hash_layout_v2(tmp_path: Path) -> None:
    request = _request(tmp_path, layout_version=2)

    assert request.layout_version == 2


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("qualified_stage", "stage.raw_1"),
        ("stage_id_sha256", "x" * 64),
        ("object_id", 0),
        ("file_path", "chunk.bcp"),
        ("expected_rows", -1),
        ("max_row_bytes", 0),
        ("wire_layout_sha256", "0" * 63),
        ("proof_capability", "unknown"),
        ("layout_version", 3),
        ("columns", ()),
        ("columns", [NativeStageColumnMapping(0, "event_id", "bigint", False)]),
        (
            "columns",
            (
                NativeStageColumnMapping(0, "EventId", "bigint", False),
                NativeStageColumnMapping(1, "eventid", "bigint", False),
            ),
        ),
        (
            "columns",
            (
                NativeStageColumnMapping(1, "event_id", "bigint", False),
                NativeStageColumnMapping(0, "payload", "nvarchar(max)", True),
            ),
        ),
    ],
)
def test_request_rejects_incomplete_or_noncanonical_authority(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    with pytest.raises(ValueError, match="mssql_native.invalid_stage_write_request"):
        _request(tmp_path, **{field: value})


def test_operation_deadline_uses_one_absolute_monotonic_bound() -> None:
    deadline = OperationDeadline(expires_at_monotonic=12.5, clock=lambda: 10.0)

    assert deadline.remaining_seconds() == 2.5

    expired = OperationDeadline(expires_at_monotonic=12.5, clock=lambda: 12.5)
    with pytest.raises(TimeoutError, match="mssql_native.writer_deadline_expired"):
        expired.remaining_seconds()


def test_observation_is_non_authoritative_and_contains_only_bounded_metrics() -> None:
    observation = NativeStageWriteObservation(
        attempt_id="attempt-1",
        input_rows_consumed=7,
        classification="success",
        writer_identity_sha256="7" * 64,
        runtime_identity_sha256="8" * 64,
        protocol="dpone.mssql-sqlclient.ipc.v1",
        metrics=NativeStageWriteMetrics(launch_seconds=0.1, write_seconds=1.25, dispose_seconds=0.05),
    )

    assert observation.positive_terminal is True
    assert observation.metrics.total_seconds == pytest.approx(1.4)


def test_unreached_phase_metrics_remain_unavailable() -> None:
    metrics = NativeStageWriteMetrics(launch_seconds=None, write_seconds=None, dispose_seconds=None)

    assert metrics.total_seconds is None


def test_observation_rejects_unknown_protocol() -> None:
    with pytest.raises(ValueError, match="mssql_native.invalid_stage_write_observation"):
        NativeStageWriteObservation(
            attempt_id="attempt-1",
            input_rows_consumed=1,
            classification="success",
            writer_identity_sha256="7" * 64,
            runtime_identity_sha256="8" * 64,
            protocol="unknown",
            metrics=NativeStageWriteMetrics(0.1, 0.2, 0.1),
        )


@pytest.mark.parametrize("value", [-1.0, float("inf"), float("nan"), True])
def test_metrics_reject_invalid_seconds(value: object) -> None:
    with pytest.raises(ValueError, match="mssql_native.invalid_stage_write_metrics"):
        NativeStageWriteMetrics(launch_seconds=value, write_seconds=0.0, dispose_seconds=0.0)  # type: ignore[arg-type]
