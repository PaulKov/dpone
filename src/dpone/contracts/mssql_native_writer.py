"""Immutable authority for one native stage writer launch."""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_BRACKETED_PART = r"\[(?:[^\]\x00-\x1f]|\]\])+\]"
_QUALIFIED_STAGE = re.compile(rf"{_BRACKETED_PART}(?:\.{_BRACKETED_PART}){{1,2}}\Z")
BCP_STAGE_PROOF = "bcp-supervised-stage-barrier-v1"
SQLCLIENT_SESSION_PROOF = "sqlclient-session-applock-v1"
WRITER_OUTCOMES = frozenset({"success", "failure", "timeout", "lost_ack", "cleanup_failed", "custody_lost"})


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
            or self.classification not in WRITER_OUTCOMES
            or (self.classification == "success") != self.positive_terminal
            or (self.positive_terminal and self.rows_consumed is None)
        ):
            raise ValueError("mssql_native.invalid_writer_outcome")


def is_nonnegative_int(value: object) -> bool:
    return type(value) is int and value >= 0


def valid_native_writer_observation(value: dict[str, Any]) -> bool:
    limbs = value["limbs"]
    return (
        value["writer_outcome"] in WRITER_OUTCOMES
        and (value["input_rows_consumed"] is None or is_nonnegative_int(value["input_rows_consumed"]))
        and (value["row_count"] is None or is_nonnegative_int(value["row_count"]))
        and (value["count_overflow"] is None or type(value["count_overflow"]) is bool)
        and (
            limbs is None
            or isinstance(limbs, list)
            and len(limbs) == 8
            and all(type(limb) is str and re.fullmatch(r"0|[1-9][0-9]*", limb) is not None for limb in limbs)
        )
        and value["quiescence"] in {"unverified", "proved", "failed"}
        and isinstance(value["diagnostic_code"], str)
        and re.fullmatch(r"[a-z][a-z0-9_]*\.[a-z][a-z0-9_]*", value["diagnostic_code"]) is not None
    )


def validate_bcp_writer_event(event: dict[str, Any], previous: dict[str, Any] | None, expected_rows: int) -> None:
    """Require positive terminal authority before BCP stage verification."""
    name, observation = event["event"], event["observation"]
    if name == "PARTIAL_PROVED":
        raise ValueError("bcp partial proof forbidden")
    if name in {"QUIESCENT", "VERIFIED"} and observation["writer_outcome"] != "success":
        raise ValueError("bcp terminal success required")
    if (
        name == "QUIESCENT"
        and previous is not None
        and previous["event"] == "WRITER_TERMINAL"
        and previous["observation"]["writer_outcome"] != "success"
    ):
        raise ValueError("bcp terminal success required")
    if name == "WRITER_TERMINAL" and observation["writer_outcome"] == "success":
        if observation["input_rows_consumed"] != expected_rows:
            raise ValueError("bcp vendor count")
    if name == "UNKNOWN" and observation["writer_outcome"] == "success":
        if (
            previous is None
            or previous["event"] not in {"WRITER_TERMINAL", "QUIESCENT"}
            or previous["observation"]["writer_outcome"] != "success"
        ):
            raise ValueError("bcp success authority missing")
