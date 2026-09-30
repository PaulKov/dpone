"""Canonical SqlClient implementation of the native stage-writer port."""

from __future__ import annotations

import re
import time
from collections.abc import Callable, Sequence
from hashlib import sha256
from threading import Lock

from dpone.ports.mssql_native import (
    PROTOCOL,
    PROTOCOL_V2,
    SQLCLIENT_SESSION_PROOF,
    MssqlSqlClientCredentials,
    NativeStageWriteMetrics,
    NativeStageWriteObservation,
    NativeStageWriteRequest,
    OperationDeadline,
    decode_observation_frame,
    encode_credentials_frame,
)
from dpone.runtime.mssql_sqlclient_process import run_sqlclient_companion

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


class MssqlSqlClientStageWriter:
    """Project credentials once and supervise one exact SqlClient companion."""

    def __init__(
        self,
        command: Sequence[str],
        *,
        credentials_provider: Callable[[NativeStageWriteRequest], MssqlSqlClientCredentials],
        writer_identity_sha256: str,
        runtime_identity_sha256: str,
        cleanup_reserve_seconds: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
        process_started: Callable[[int, NativeStageWriteRequest], None] | None = None,
        observation_sink: Callable[[NativeStageWriteObservation], None] | None = None,
        diagnostic_sink: Callable[[str], None] | None = None,
    ) -> None:
        if (
            not command
            or any(type(part) is not str or not part for part in command)
            or _SHA256.fullmatch(writer_identity_sha256) is None
            or _SHA256.fullmatch(runtime_identity_sha256) is None
            or type(cleanup_reserve_seconds) is not float
            or cleanup_reserve_seconds <= 0.0
        ):
            raise ValueError("mssql_sqlclient.invalid_stage_writer")
        self._command = tuple(command)
        self._credentials_provider = credentials_provider
        self._writer_identity_sha256 = writer_identity_sha256
        self._runtime_identity_sha256 = runtime_identity_sha256
        self._cleanup_reserve_seconds = cleanup_reserve_seconds
        self._clock = clock
        self._process_started = process_started
        self._observation_sink = observation_sink
        self._diagnostic_sink = diagnostic_sink
        self._launched: set[str] = set()
        self._lock = Lock()

    def write(
        self,
        request: NativeStageWriteRequest,
        *,
        deadline: OperationDeadline,
    ) -> NativeStageWriteObservation:
        """Return one closed observation and never expose credentials or child output."""
        if request.proof_capability != SQLCLIENT_SESSION_PROOF:
            raise ValueError("mssql_native.writer_proof_capability_mismatch")
        if not self._has_time(deadline):
            return self._observed(self._closed(request, "timeout"))
        if not _file_matches(request):
            return self._observed(self._closed(request, "custody_lost"))
        if not self._has_time(deadline):
            return self._observed(self._closed(request, "timeout"))
        with self._lock:
            if request.attempt_id in self._launched:
                raise ValueError("mssql_native.grant_already_launched")
            self._launched.add(request.attempt_id)
        try:
            credentials = self._credentials_provider(request)
            credential_frame = encode_credentials_frame(credentials)
        except Exception:
            return self._observed(self._closed(request, "custody_lost"))
        process_result = run_sqlclient_companion(
            self._command,
            request=request,
            credential_frame=credential_frame,
            deadline=deadline,
            cleanup_reserve_seconds=self._cleanup_reserve_seconds,
            clock=self._clock,
            process_started=self._process_started,
        )
        if process_result.diagnostic_code is not None and self._diagnostic_sink is not None:
            try:
                self._diagnostic_sink(process_result.diagnostic_code)
            except Exception:
                pass
        if process_result.classification != "completed":
            return self._observed(self._closed(request, process_result.classification))
        if not _file_matches(request):
            return self._observed(self._closed(request, "custody_lost"))
        if not self._has_time(deadline):
            return self._observed(self._closed(request, "timeout"))
        try:
            return self._observed(
                decode_observation_frame(
                    process_result.stdout,
                    request=request,
                    writer_identity_sha256=self._writer_identity_sha256,
                    runtime_identity_sha256=self._runtime_identity_sha256,
                )
            )
        except ValueError:
            return self._observed(self._closed(request, "lost_ack"))

    def _observed(self, observation: NativeStageWriteObservation) -> NativeStageWriteObservation:
        """Publish non-authoritative telemetry without changing delivery authority."""
        if self._observation_sink is not None:
            try:
                self._observation_sink(observation)
            except Exception:
                pass
        return observation

    def _has_time(self, deadline: OperationDeadline) -> bool:
        try:
            deadline.remaining_seconds()
        except TimeoutError:
            return False
        return True

    def _closed(self, request: NativeStageWriteRequest, classification: str) -> NativeStageWriteObservation:
        return NativeStageWriteObservation(
            attempt_id=request.attempt_id,
            input_rows_consumed=None,
            classification=classification,
            writer_identity_sha256=self._writer_identity_sha256,
            runtime_identity_sha256=self._runtime_identity_sha256,
            protocol=PROTOCOL if request.layout_version == 1 else PROTOCOL_V2,
            metrics=NativeStageWriteMetrics(None, None, None),
        )


def _file_matches(request: NativeStageWriteRequest) -> bool:
    digest = sha256()
    size = 0
    try:
        with request.file_path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
                size += len(block)
    except OSError:
        return False
    return size == request.encoded_bytes and digest.hexdigest() == request.file_sha256


__all__ = ["MssqlSqlClientStageWriter"]
