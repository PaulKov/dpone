from __future__ import annotations

from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_native_stage_writer import (
    NativeStageColumnMapping,
    NativeStageWriteRequest,
    OperationDeadline,
)
from dpone.contracts.mssql_native_writer import BCP_STAGE_PROOF
from dpone.runtime.sinks.mssql_native_bcp_stage_writer import MssqlNativeBcpStageWriter


class _DeadlineAwareLaunch:
    def __init__(self, proof: object | None = None, error: Exception | None = None) -> None:
        self.proof = proof or SimpleNamespace(classification="success", acknowledged=True, reaped=True, rows_copied=7)
        self.error = error
        self.calls: list[tuple[object, Path, OperationDeadline]] = []

    def __call__(self, request: object, rejects_path: Path, deadline: OperationDeadline) -> object:
        self.calls.append((request, rejects_path, deadline))
        if self.error is not None:
            raise self.error
        return self.proof


def _request(tmp_path: Path, *, proof_capability: str = BCP_STAGE_PROOF) -> NativeStageWriteRequest:
    payload = b"hello world"
    path = tmp_path / "chunk.bcp"
    path.write_bytes(payload)
    return NativeStageWriteRequest(
        attempt_id="attempt-1",
        qualified_stage="[db].[stage].[raw_1]",
        stage_id_sha256="1" * 64,
        owner_binding_sha256="2" * 64,
        object_id=42,
        schema_sha256="3" * 64,
        file_path=path,
        expected_rows=7,
        encoded_bytes=11,
        max_row_bytes=11,
        file_sha256=sha256(payload).hexdigest(),
        grant_token_sha256="5" * 64,
        proof_capability=proof_capability,
        wire_layout_sha256="6" * 64,
        columns=(NativeStageColumnMapping(0, "event_id", "bigint", False),),
    )


def test_adapter_passes_one_deadline_to_supervised_launch_and_maps_positive_outcome(tmp_path: Path) -> None:
    launch = _DeadlineAwareLaunch()
    adapter = MssqlNativeBcpStageWriter(
        launch,
        rejects_root=tmp_path / "rejects",
        writer_identity_sha256="7" * 64,
        runtime_identity_sha256="8" * 64,
        clock=lambda: 1.0,
    )

    observation = adapter.write(_request(tmp_path), deadline=OperationDeadline(10.0, clock=lambda: 1.0))

    assert observation.input_rows_consumed == 7
    assert observation.classification == "success"
    assert observation.protocol == "dpone.mssql-bcp.canonical.v1"
    request, rejects_path, observed_deadline = launch.calls[0]
    assert request.attempt_id == "attempt-1"
    assert rejects_path.parent == tmp_path / "rejects"
    assert observed_deadline.expires_at_monotonic == 10.0


def test_adapter_rejects_elapsed_deadline_before_launch(tmp_path: Path) -> None:
    launch = _DeadlineAwareLaunch()
    adapter = MssqlNativeBcpStageWriter(
        launch,
        rejects_root=tmp_path / "rejects",
        writer_identity_sha256="7" * 64,
        runtime_identity_sha256="8" * 64,
        clock=lambda: 2.0,
    )

    observation = adapter.write(_request(tmp_path), deadline=OperationDeadline(2.0, clock=lambda: 2.0))

    assert observation.classification == "timeout"
    assert observation.input_rows_consumed is None
    assert launch.calls == []


def test_adapter_never_accepts_sqlclient_proof(tmp_path: Path) -> None:
    request = _request(tmp_path, proof_capability="sqlclient-session-applock-v1")
    launch = _DeadlineAwareLaunch()
    adapter = MssqlNativeBcpStageWriter(
        launch,
        rejects_root=tmp_path / "rejects",
        writer_identity_sha256="7" * 64,
        runtime_identity_sha256="8" * 64,
    )

    with pytest.raises(ValueError, match="mssql_native.writer_proof_capability_mismatch"):
        adapter.write(request, deadline=OperationDeadline(2.0, clock=lambda: 1.0))


def test_adapter_maps_launcher_exception_to_custody_lost_observation(tmp_path: Path) -> None:
    launch = _DeadlineAwareLaunch(error=RuntimeError("private vendor detail"))
    adapter = MssqlNativeBcpStageWriter(
        launch,
        rejects_root=tmp_path / "rejects",
        writer_identity_sha256="7" * 64,
        runtime_identity_sha256="8" * 64,
    )

    observation = adapter.write(_request(tmp_path), deadline=OperationDeadline(2.0, clock=lambda: 1.0))

    assert observation.classification == "custody_lost"
    assert observation.input_rows_consumed is None


def test_adapter_converts_deadline_expiry_after_launch_to_timeout(tmp_path: Path) -> None:
    ticks = iter((1.0, 1.0, 10.0))
    deadline = OperationDeadline(5.0, clock=lambda: next(ticks))
    launch = _DeadlineAwareLaunch()
    adapter = MssqlNativeBcpStageWriter(
        launch,
        rejects_root=tmp_path / "rejects",
        writer_identity_sha256="7" * 64,
        runtime_identity_sha256="8" * 64,
    )

    observation = adapter.write(_request(tmp_path), deadline=deadline)

    assert observation.classification == "timeout"
    assert launch.calls[0][2] is deadline


def test_adapter_rejects_file_identity_drift_without_launch(tmp_path: Path) -> None:
    request = _request(tmp_path)
    request.file_path.write_bytes(b"changed")
    launch = _DeadlineAwareLaunch()
    adapter = MssqlNativeBcpStageWriter(
        launch,
        rejects_root=tmp_path / "rejects",
        writer_identity_sha256="7" * 64,
        runtime_identity_sha256="8" * 64,
    )

    observation = adapter.write(request, deadline=OperationDeadline(2.0, clock=lambda: 1.0))

    assert observation.classification == "custody_lost"
    assert launch.calls == []
