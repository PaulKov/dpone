from __future__ import annotations

import os
import sys
import textwrap
import time
from pathlib import Path

import pytest

from dpone.contracts.mssql_native_stage_writer import (
    NativeStageColumnMapping,
    NativeStageWriteRequest,
    OperationDeadline,
)
from dpone.contracts.mssql_native_writer import SQLCLIENT_SESSION_PROOF
from dpone.contracts.mssql_sqlclient_ipc import MssqlSqlClientCredentials
from dpone.runtime.sinks.mssql_sqlclient_stage_writer import MssqlSqlClientStageWriter

WRITER_ID = "7" * 64
RUNTIME_ID = "8" * 64
SECRET = "not-for-argv-env-or-stdin"


def _request(
    tmp_path: Path,
    *,
    attempt_id: str = "attempt-1",
    layout_version: int = 1,
) -> NativeStageWriteRequest:
    payload = tmp_path / "sealed.bcp"
    payload.write_bytes(b"sealed-input")
    import hashlib

    return NativeStageWriteRequest(
        attempt_id=attempt_id,
        qualified_stage="[DWH].[test].[stage]",
        stage_id_sha256="1" * 64,
        owner_binding_sha256="2" * 64,
        object_id=42,
        schema_sha256="3" * 64,
        file_path=payload,
        expected_rows=7,
        encoded_bytes=payload.stat().st_size,
        max_row_bytes=payload.stat().st_size,
        file_sha256=hashlib.sha256(payload.read_bytes()).hexdigest(),
        grant_token_sha256="4" * 64,
        proof_capability=SQLCLIENT_SESSION_PROOF,
        wire_layout_sha256="5" * 64,
        columns=(NativeStageColumnMapping(0, "value", "bigint", False),),
        layout_version=layout_version,
    )


def _credentials() -> MssqlSqlClientCredentials:
    return MssqlSqlClientCredentials(
        host="db.internal",
        port=1433,
        database="warehouse",
        username="loader",
        password=SECRET,
    )


def _companion(tmp_path: Path) -> Path:
    script = tmp_path / "fake_companion.py"
    script.write_text(
        textwrap.dedent(
            f"""
            import json, os, struct, subprocess, sys, time

            def read_frame(stream):
                header = stream.read(4)
                if len(header) != 4:
                    raise RuntimeError('missing frame')
                size = struct.unpack('>I', header)[0]
                payload = stream.read(size)
                if len(payload) != size or stream.read(1):
                    raise RuntimeError('invalid frame')
                return payload

            def write_frame(document):
                payload = json.dumps(document, sort_keys=True, separators=(',', ':')).encode()
                sys.stdout.buffer.write(struct.pack('>I', len(payload)) + payload)
                sys.stdout.buffer.flush()

            mode = sys.argv[sys.argv.index('--mode') + 1]
            secret_fd = int(sys.argv[sys.argv.index('--secret-fd') + 1])
            request_payload = read_frame(sys.stdin.buffer)
            with os.fdopen(secret_fd, 'rb', closefd=True) as stream:
                secret_payload = read_frame(stream)
            request = json.loads(request_payload)
            credentials = json.loads(secret_payload)
            if mode == 'success':
                assert credentials['password'] == {SECRET!r}
                assert {SECRET!r} not in ' '.join(sys.argv)
                assert {SECRET!r} not in '\\n'.join(os.environ.values())
                assert {SECRET!r}.encode() not in request_payload
                write_frame({{
                    'schema_version': 1,
                    'protocol': 'dpone.mssql-sqlclient.ipc.v1',
                    'attempt_id': request['attempt_id'],
                    'classification': 'success',
                    'input_rows_consumed': request['expected_rows'],
                    'writer_identity_sha256': {WRITER_ID!r},
                    'runtime_identity_sha256': {RUNTIME_ID!r},
                    'metrics': {{'launch_seconds': 0.01, 'write_seconds': 0.02, 'dispose_seconds': 0.01}},
                }})
            elif mode == 'failure':
                write_frame({{
                    'schema_version': 1,
                    'protocol': 'dpone.mssql-sqlclient.ipc.v1',
                    'attempt_id': request['attempt_id'],
                    'classification': 'failure',
                    'input_rows_consumed': None,
                    'writer_identity_sha256': {WRITER_ID!r},
                    'runtime_identity_sha256': {RUNTIME_ID!r},
                    'metrics': {{'launch_seconds': 0.01, 'write_seconds': 0.01, 'dispose_seconds': 0.01}},
                }})
            elif mode == 'malformed':
                sys.stdout.buffer.write(b'not-a-frame')
                sys.stdout.buffer.flush()
            elif mode == 'stable-diagnostic':
                sys.stderr.write('mssql_sqlclient.bulk_copy_failed')
                write_frame({{
                    'schema_version': 1,
                    'protocol': 'dpone.mssql-sqlclient.ipc.v1',
                    'attempt_id': request['attempt_id'],
                    'classification': 'failure',
                    'input_rows_consumed': None,
                    'writer_identity_sha256': {WRITER_ID!r},
                    'runtime_identity_sha256': {RUNTIME_ID!r},
                    'metrics': {{'launch_seconds': None, 'write_seconds': None, 'dispose_seconds': None}},
                }})
            elif mode == 'unsafe-diagnostic':
                sys.stderr.write('mssql_sqlclient.bulk_copy_failed password=secret')
                write_frame({{
                    'schema_version': 1,
                    'protocol': 'dpone.mssql-sqlclient.ipc.v1',
                    'attempt_id': request['attempt_id'],
                    'classification': 'failure',
                    'input_rows_consumed': None,
                    'writer_identity_sha256': {WRITER_ID!r},
                    'runtime_identity_sha256': {RUNTIME_ID!r},
                    'metrics': {{'launch_seconds': None, 'write_seconds': None, 'dispose_seconds': None}},
                }})
            elif mode == 'oversize':
                sys.stdout.buffer.write(b'x' * (70 * 1024))
                sys.stderr.buffer.write(b'y' * (70 * 1024))
                sys.stdout.buffer.flush(); sys.stderr.buffer.flush()
            elif mode == 'sleep':
                time.sleep(60)
            elif mode == 'descendant':
                child = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)'])
                marker = sys.argv[sys.argv.index('--marker') + 1]
                open(marker, 'w').write(str(child.pid))
                time.sleep(60)
            """
        ),
        encoding="utf-8",
    )
    return script


def _writer(tmp_path: Path, mode: str, *extra: str) -> MssqlSqlClientStageWriter:
    return MssqlSqlClientStageWriter(
        (sys.executable, str(_companion(tmp_path)), "--mode", mode, *extra),
        credentials_provider=lambda _request: _credentials(),
        writer_identity_sha256=WRITER_ID,
        runtime_identity_sha256=RUNTIME_ID,
        cleanup_reserve_seconds=0.25,
    )


def _deadline(seconds: float = 5.0) -> OperationDeadline:
    return OperationDeadline(float(time.monotonic() + seconds))


def test_supervisor_projects_secret_only_through_inherited_pipe(tmp_path: Path) -> None:
    observation = _writer(tmp_path, "success").write(_request(tmp_path), deadline=_deadline())

    assert observation.classification == "success"
    assert observation.input_rows_consumed == 7
    assert observation.writer_identity_sha256 == WRITER_ID
    assert observation.runtime_identity_sha256 == RUNTIME_ID


def test_supervisor_reports_started_process_for_external_liveness_observation(tmp_path: Path) -> None:
    started = []
    request = _request(tmp_path)
    writer = MssqlSqlClientStageWriter(
        (sys.executable, str(_companion(tmp_path)), "--mode", "success"),
        credentials_provider=lambda _request: _credentials(),
        writer_identity_sha256=WRITER_ID,
        runtime_identity_sha256=RUNTIME_ID,
        cleanup_reserve_seconds=0.25,
        process_started=lambda process_id, observed_request: started.append((process_id, observed_request)),
    )

    assert writer.write(request, deadline=_deadline()).positive_terminal
    assert len(started) == 1 and type(started[0][0]) is int and started[0][0] > 0
    assert started[0][1] is request


def test_supervisor_reports_closed_observation_to_injected_sink(tmp_path: Path) -> None:
    observed = []
    writer = MssqlSqlClientStageWriter(
        (sys.executable, str(_companion(tmp_path)), "--mode", "success"),
        credentials_provider=lambda _request: _credentials(),
        writer_identity_sha256=WRITER_ID,
        runtime_identity_sha256=RUNTIME_ID,
        cleanup_reserve_seconds=0.25,
        observation_sink=observed.append,
    )

    result = writer.write(_request(tmp_path), deadline=_deadline())

    assert observed == [result]


def test_supervisor_projects_only_stable_child_diagnostic_to_injected_sink(tmp_path: Path) -> None:
    diagnostics = []
    writer = MssqlSqlClientStageWriter(
        (sys.executable, str(_companion(tmp_path)), "--mode", "stable-diagnostic"),
        credentials_provider=lambda _request: _credentials(),
        writer_identity_sha256=WRITER_ID,
        runtime_identity_sha256=RUNTIME_ID,
        cleanup_reserve_seconds=0.25,
        diagnostic_sink=diagnostics.append,
    )

    result = writer.write(_request(tmp_path), deadline=_deadline())

    assert result.classification == "failure"
    assert diagnostics == ["mssql_sqlclient.bulk_copy_failed"]


def test_supervisor_discards_untrusted_child_diagnostic(tmp_path: Path) -> None:
    diagnostics = []
    writer = MssqlSqlClientStageWriter(
        (sys.executable, str(_companion(tmp_path)), "--mode", "unsafe-diagnostic"),
        credentials_provider=lambda _request: _credentials(),
        writer_identity_sha256=WRITER_ID,
        runtime_identity_sha256=RUNTIME_ID,
        cleanup_reserve_seconds=0.25,
        diagnostic_sink=diagnostics.append,
    )

    assert writer.write(_request(tmp_path), deadline=_deadline()).classification == "failure"
    assert diagnostics == []


def test_observation_sink_failure_cannot_change_delivery_result(tmp_path: Path) -> None:
    def unavailable(_observation) -> None:
        raise RuntimeError("telemetry unavailable")

    writer = MssqlSqlClientStageWriter(
        (sys.executable, str(_companion(tmp_path)), "--mode", "success"),
        credentials_provider=lambda _request: _credentials(),
        writer_identity_sha256=WRITER_ID,
        runtime_identity_sha256=RUNTIME_ID,
        cleanup_reserve_seconds=0.25,
        observation_sink=unavailable,
    )

    assert writer.write(_request(tmp_path), deadline=_deadline()).positive_terminal


def test_supervisor_preserves_closed_failure_result(tmp_path: Path) -> None:
    observation = _writer(tmp_path, "failure").write(_request(tmp_path), deadline=_deadline())

    assert observation.classification == "failure"
    assert observation.input_rows_consumed is None


@pytest.mark.parametrize("mode", ["malformed", "oversize"])
def test_supervisor_maps_invalid_or_oversized_output_to_lost_ack(tmp_path: Path, mode: str) -> None:
    observation = _writer(tmp_path, mode).write(_request(tmp_path), deadline=_deadline())

    assert observation.classification == "lost_ack"
    assert observation.input_rows_consumed is None


def test_supervisor_times_out_and_reaps_process(tmp_path: Path) -> None:
    observation = _writer(tmp_path, "sleep").write(_request(tmp_path), deadline=_deadline(0.6))

    assert observation.classification == "timeout"


@pytest.mark.skipif(os.name != "posix", reason="POSIX process-group proof")
def test_supervisor_terminates_descendant_process_group(tmp_path: Path) -> None:
    marker = tmp_path / "descendant.pid"
    observation = _writer(tmp_path, "descendant", "--marker", str(marker)).write(
        _request(tmp_path), deadline=_deadline(0.8)
    )

    assert observation.classification == "timeout"
    descendant = int(marker.read_text())
    with pytest.raises(ProcessLookupError):
        os.kill(descendant, 0)


def test_supervisor_rejects_elapsed_deadline_without_credentials_or_launch(tmp_path: Path) -> None:
    called = False

    def credentials(_request: NativeStageWriteRequest) -> MssqlSqlClientCredentials:
        nonlocal called
        called = True
        return _credentials()

    writer = MssqlSqlClientStageWriter(
        (sys.executable, str(_companion(tmp_path)), "--mode", "success"),
        credentials_provider=credentials,
        writer_identity_sha256=WRITER_ID,
        runtime_identity_sha256=RUNTIME_ID,
    )
    observation = writer.write(_request(tmp_path), deadline=OperationDeadline(float(time.monotonic() - 1)))

    assert observation.classification == "timeout"
    assert called is False


def test_supervisor_enforces_once_per_attempt_before_credential_projection(tmp_path: Path) -> None:
    writer = _writer(tmp_path, "success")
    request = _request(tmp_path)
    assert writer.write(request, deadline=_deadline()).positive_terminal

    with pytest.raises(ValueError, match="mssql_native.grant_already_launched"):
        writer.write(request, deadline=_deadline())


def test_supervisor_rejects_file_drift_without_launch(tmp_path: Path) -> None:
    request = _request(tmp_path)
    request.file_path.write_bytes(b"changed")

    observation = _writer(tmp_path, "success").write(request, deadline=_deadline())

    assert observation.classification == "custody_lost"


def test_supervisor_closes_v2_attempt_with_v2_protocol(tmp_path: Path) -> None:
    request = _request(tmp_path, layout_version=2)
    request.file_path.write_bytes(b"changed")

    observation = _writer(tmp_path, "success").write(request, deadline=_deadline())

    assert observation.classification == "custody_lost"
    assert observation.protocol == "dpone.mssql-sqlclient.ipc.v2"


def test_supervisor_rejects_bcp_proof_capability(tmp_path: Path) -> None:
    request = _request(tmp_path)
    object.__setattr__(request, "proof_capability", "bcp-supervised-stage-barrier-v1")

    with pytest.raises(ValueError, match="mssql_native.writer_proof_capability_mismatch"):
        _writer(tmp_path, "success").write(request, deadline=_deadline())


def test_supervisor_never_exposes_secret_in_repr(tmp_path: Path) -> None:
    writer = _writer(tmp_path, "success")

    assert SECRET not in repr(writer)
    assert SECRET not in repr(_credentials())
