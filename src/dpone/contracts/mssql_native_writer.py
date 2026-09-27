"""Immutable authority for one native stage writer launch."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BRACKETED_PART = r"\[(?:[^\]\x00-\x1f]|\]\])+\]"
_QUALIFIED_STAGE = re.compile(rf"{_BRACKETED_PART}(?:\.{_BRACKETED_PART}){{1,2}}\Z")
BCP_STAGE_PROOF = "bcp-supervised-stage-barrier-v1"
SQLCLIENT_SESSION_PROOF = "sqlclient-session-applock-v1"


def is_qualified_native_stage(value: object) -> bool:
    """Admit only prequoted two- or three-part SQL Server stage identifiers."""
    return type(value) is str and _QUALIFIED_STAGE.fullmatch(value) is not None


@dataclass(frozen=True, slots=True)
class NativeStageWriteGrant:
    """Bind a sealed file to one owned attempt; the token never enters evidence."""

    attempt_id: str
    qualified_stage: str
    file_path: Path
    expected_rows: int
    encoded_bytes: int
    file_sha256: str
    grant_token_sha256: str
    proof_capability: str

    def __post_init__(self) -> None:
        if (
            not self.attempt_id
            or not is_qualified_native_stage(self.qualified_stage)
            or not isinstance(self.file_path, Path)
            or type(self.expected_rows) is not int
            or self.expected_rows < 0
            or type(self.encoded_bytes) is not int
            or self.encoded_bytes < 0
            or _SHA256.fullmatch(self.file_sha256) is None
            or _SHA256.fullmatch(self.grant_token_sha256) is None
            or self.proof_capability not in {BCP_STAGE_PROOF, SQLCLIENT_SESSION_PROOF}
        ):
            raise ValueError("mssql_native.invalid_writer_grant")


@dataclass(frozen=True, slots=True)
class NativeStageWriteOutcome:
    """Writer terminal proof only; stage contents require separate verification."""

    attempt_id: str
    positive_terminal: bool
    rows_consumed: int | None
    classification: str

    def __post_init__(self) -> None:
        if (
            not self.attempt_id
            or type(self.positive_terminal) is not bool
            or (self.rows_consumed is not None and (type(self.rows_consumed) is not int or self.rows_consumed < 0))
            or self.classification
            not in {"success", "failure", "timeout", "lost_ack", "cleanup_failed", "custody_lost"}
            or (self.classification == "success") != self.positive_terminal
            or (self.positive_terminal and self.rows_consumed is None)
        ):
            raise ValueError("mssql_native.invalid_writer_outcome")
