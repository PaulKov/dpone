"""Canonical deadline-aware BCP implementation of the native stage-writer port."""

from __future__ import annotations

import time
from collections.abc import Callable
from hashlib import sha256
from pathlib import Path
from threading import Lock
from typing import Protocol

from dpone.ports.mssql_native import (
    BCP_STAGE_PROOF,
    NativeStageProcessProof,
    NativeStageWriteMetrics,
    NativeStageWriteObservation,
    NativeStageWriteRequest,
    OperationDeadline,
)


class DeadlineAwareBcpLaunch(Protocol):
    """Launch and reap BCP while deriving every wait from one deadline."""

    def __call__(
        self,
        request: NativeStageWriteRequest,
        rejects_path: Path,
        deadline: OperationDeadline,
    ) -> NativeStageProcessProof: ...


class MssqlNativeBcpStageWriter:
    """Validate one sealed input around a deadline-aware supervised BCP launch."""

    protocol = "dpone.mssql-bcp.canonical.v1"

    def __init__(
        self,
        launch: DeadlineAwareBcpLaunch,
        *,
        rejects_root: Path,
        writer_identity_sha256: str,
        runtime_identity_sha256: str,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._launch = launch
        self._rejects_root = rejects_root
        self._writer_identity_sha256 = writer_identity_sha256
        self._runtime_identity_sha256 = runtime_identity_sha256
        self._clock = clock
        self._launched: set[str] = set()
        self._lock = Lock()

    @staticmethod
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

    def _observation(
        self,
        request: NativeStageWriteRequest,
        classification: str,
        *,
        rows: int | None = None,
        write_seconds: float | None = None,
    ) -> NativeStageWriteObservation:
        return NativeStageWriteObservation(
            attempt_id=request.attempt_id,
            input_rows_consumed=rows if classification == "success" else None,
            classification=classification,
            writer_identity_sha256=self._writer_identity_sha256,
            runtime_identity_sha256=self._runtime_identity_sha256,
            protocol=self.protocol,
            metrics=NativeStageWriteMetrics(None, write_seconds, None),
        )

    def write(
        self,
        request: NativeStageWriteRequest,
        *,
        deadline: OperationDeadline,
    ) -> NativeStageWriteObservation:
        """Return only closed observations after launch; never leak vendor errors."""
        if request.proof_capability != BCP_STAGE_PROOF:
            raise ValueError("mssql_native.writer_proof_capability_mismatch")
        try:
            deadline.remaining_seconds()
        except TimeoutError:
            return self._observation(request, "timeout")
        if not self._file_matches(request):
            return self._observation(request, "custody_lost")
        try:
            deadline.remaining_seconds()
        except TimeoutError:
            return self._observation(request, "timeout")
        with self._lock:
            if request.attempt_id in self._launched:
                raise ValueError("mssql_native.grant_already_launched")
            self._launched.add(request.attempt_id)
        self._rejects_root.mkdir(parents=True, exist_ok=True)
        rejects_path = self._rejects_root / f"{sha256(request.attempt_id.encode()).hexdigest()}.rejects"
        started = self._clock()
        try:
            proof = self._launch(request, rejects_path, deadline)
        except TimeoutError:
            return self._observation(request, "timeout", write_seconds=max(0.0, self._clock() - started))
        except Exception:
            return self._observation(request, "custody_lost", write_seconds=max(0.0, self._clock() - started))
        elapsed = max(0.0, self._clock() - started)
        try:
            deadline.remaining_seconds()
        except TimeoutError:
            return self._observation(request, "timeout", write_seconds=elapsed)
        if not self._file_matches(request):
            return self._observation(request, "custody_lost", write_seconds=elapsed)
        try:
            deadline.remaining_seconds()
        except TimeoutError:
            return self._observation(request, "timeout", write_seconds=elapsed)
        try:
            valid_proof = _valid_proof(proof)
        except Exception:
            valid_proof = False
        if not valid_proof:
            return self._observation(request, "custody_lost", write_seconds=elapsed)
        if proof.classification != "success":
            return self._observation(request, proof.classification, write_seconds=elapsed)
        if not proof.acknowledged:
            return self._observation(request, "lost_ack", write_seconds=elapsed)
        if not proof.reaped:
            return self._observation(request, "cleanup_failed", write_seconds=elapsed)
        if proof.rows_copied != request.expected_rows:
            return self._observation(request, "failure", write_seconds=elapsed)
        try:
            if rejects_path.exists() and rejects_path.stat().st_size:
                return self._observation(request, "failure", write_seconds=elapsed)
            if rejects_path.exists():
                rejects_path.unlink()
        except OSError:
            return self._observation(request, "cleanup_failed", write_seconds=elapsed)
        return self._observation(request, "success", rows=proof.rows_copied, write_seconds=elapsed)


def _valid_proof(proof: object) -> bool:
    return (
        getattr(proof, "classification", None)
        in {"success", "failure", "timeout", "lost_ack", "cleanup_failed", "custody_lost"}
        and type(getattr(proof, "acknowledged", None)) is bool
        and type(getattr(proof, "reaped", None)) is bool
        and (
            getattr(proof, "rows_copied", None) is None
            or type(getattr(proof, "rows_copied", None)) is int
            and getattr(proof, "rows_copied") >= 0
        )
    )


__all__ = ["DeadlineAwareBcpLaunch", "MssqlNativeBcpStageWriter"]
