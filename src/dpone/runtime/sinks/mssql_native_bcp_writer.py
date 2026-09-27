"""Supervised BCP adapter that refuses ambiguous process and file evidence."""

from __future__ import annotations

from collections.abc import Callable
from hashlib import sha256
from pathlib import Path
from threading import Lock

from dpone.ports.mssql_native_writer import (
    BCP_STAGE_PROOF,
    NativeStageProcessProof,
    NativeStageWriteGrant,
    NativeStageWriteOutcome,
)


class MssqlNativeBcpWriter:
    """Own one launch per grant in this process; journal owns durable launch fencing."""

    def __init__(self, launch: Callable[[NativeStageWriteGrant, Path], NativeStageProcessProof]) -> None:
        self._launch = launch
        self._launched: set[str] = set()
        self._lock = Lock()

    @staticmethod
    def _require_file(grant: NativeStageWriteGrant) -> None:
        digest = sha256()
        size = 0
        with grant.file_path.open("rb") as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
                size += len(block)
        if size != grant.encoded_bytes or digest.hexdigest() != grant.file_sha256:
            raise ValueError("mssql_native.file_identity_changed")

    def write(self, grant: NativeStageWriteGrant, *, rejects_path: Path) -> NativeStageWriteOutcome:
        """Validate both sides of one launch before returning positive process proof."""
        if grant.proof_capability != BCP_STAGE_PROOF:
            raise ValueError("mssql_native.writer_proof_capability_mismatch")
        self._require_file(grant)
        key = grant.attempt_id
        with self._lock:
            if key in self._launched:
                raise ValueError("mssql_native.grant_already_launched")
            self._launched.add(key)
        try:
            result = self._launch(grant, rejects_path)
        except Exception:
            return NativeStageWriteOutcome(grant.attempt_id, False, None, "custody_lost")
        if result.classification != "success" or not result.acknowledged or not result.reaped:
            return NativeStageWriteOutcome(grant.attempt_id, False, result.rows_copied, result.classification)
        if type(result.rows_copied) is not int or result.rows_copied != grant.expected_rows:
            raise ValueError("mssql_native.vendor_count_mismatch")
        if rejects_path.exists() and rejects_path.stat().st_size:
            raise ValueError("mssql_native.rejects_not_empty")
        self._require_file(grant)
        return NativeStageWriteOutcome(grant.attempt_id, True, result.rows_copied, "success")
