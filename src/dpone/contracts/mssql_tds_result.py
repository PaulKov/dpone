"""Bounded worker completion messages, never SQL or process settlement proofs.

The supervisor must feed bytes from the dedicated result channel and call finish
only after observing its EOF. It must independently require zero exit, reaping
and SQL verification. A valid error result never permits automatic retry.
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from hashlib import sha256

from dpone.contracts.mssql_native_chunks import TdsInputReceipt
from dpone.contracts.mssql_tds_frames import TdsMessageFrame, encode_message
from dpone.contracts.mssql_tds_worker import TdsAttemptError, TdsAttemptIdentity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

_MAX_PAYLOAD = 16 * 1024
_ERROR = "mssql_native.tds_result_protocol"
_KEYS = {"schema_version", "status", "attempt_sha256", "input_eof", "receipt", "error"}


def attempt_identity_digest(identity: TdsAttemptIdentity) -> str:
    """Bind every immutable attempt field using the canonical JSON representation."""
    if type(identity) is not TdsAttemptIdentity:
        raise ValueError(_ERROR)
    return sha256(canonical_json_bytes(asdict(identity))).hexdigest()


@dataclass(frozen=True)
class TdsWorkerResult:
    """Exactly one complete input receipt or one closed diagnostic enum.

    attempt_sha256 binds the canonical full TdsAttemptIdentity, including policy,
    implementation, file, target name and ownership binding, not merely a PID.
    SQL object_id belongs to the separate lifecycle object identity and still
    requires independent coordinator verification.
    Failure results intentionally carry no partial-input authority or free text.
    """

    attempt_sha256: str
    receipt: TdsInputReceipt | None
    error: TdsAttemptError | None

    def __post_init__(self) -> None:
        if type(self.attempt_sha256) is not str or re.fullmatch("[0-9a-f]{64}", self.attempt_sha256) is None:
            raise ValueError(_ERROR)
        if self.receipt is not None:
            if type(self.receipt) is not TdsInputReceipt or self.error is not None:
                raise ValueError(_ERROR)
        elif type(self.error) is not TdsAttemptError:
            raise ValueError(_ERROR)


def encode_result_payload(result: TdsWorkerResult) -> bytes:
    """Produce the canonical closed body for standalone or embedded results."""
    if type(result) is not TdsWorkerResult:
        raise ValueError(_ERROR)
    body = canonical_json_bytes(
        {
            "schema_version": 1,
            "status": "success" if result.receipt is not None else "error",
            "attempt_sha256": result.attempt_sha256,
            "input_eof": result.receipt is not None,
            "receipt": None if result.receipt is None else asdict(result.receipt),
            "error": None if result.error is None else result.error.value,
        }
    )
    if not 0 < len(body) <= _MAX_PAYLOAD:
        raise ValueError(_ERROR)
    return body


def encode_result(result: TdsWorkerResult) -> bytes:
    """Produce one closed result frame, without SDK diagnostic text."""
    return encode_message(encode_result_payload(result), max_payload=_MAX_PAYLOAD)


class TdsResultFrame:
    """Incremental one-frame reader with a fixed allocation bound.

    Header admission precedes buffering payload. Malformed, oversized, extra or
    truncated input poisons the reader. No partial message can yield a result.
    Blocking I/O and absolute deadlines belong to the process adapter.
    """

    def __init__(self) -> None:
        self._frame = TdsMessageFrame(max_payload=_MAX_PAYLOAD)

    def feed(self, data: bytes) -> None:
        """Accept one fragment without truncating excess bytes."""
        self._frame.feed(data)

    def finish(self, *, expected_attempt_sha256: str, expected_input: TdsInputReceipt) -> TdsWorkerResult:
        """Validate after actual channel EOF, binding success to expected input."""
        return decode_result_payload(
            self._frame.finish(), expected_attempt_sha256=expected_attempt_sha256, expected_input=expected_input
        )


def decode_result_payload(
    body: bytes, *, expected_attempt_sha256: str, expected_input: TdsInputReceipt
) -> TdsWorkerResult:
    """Validate an already framed payload; this function does not observe EOF."""
    try:
        if type(body) is not bytes or not 0 < len(body) <= _MAX_PAYLOAD:
            raise ValueError(_ERROR)
        if type(expected_input) is not TdsInputReceipt:
            raise ValueError(_ERROR)
        data = strict_json_object(body)
        if set(data) != _KEYS or type(data["schema_version"]) is not int or data["schema_version"] != 1:
            raise ValueError(_ERROR)
        raw_receipt = data["receipt"]
        if raw_receipt is not None:
            if type(raw_receipt) is not dict or set(raw_receipt) != {"rows", "encoded_bytes", "file_sha256"}:
                raise ValueError(_ERROR)
            receipt = TdsInputReceipt(**raw_receipt)
        else:
            receipt = None
        error = None if data["error"] is None else TdsAttemptError(data["error"])
        result = TdsWorkerResult(data["attempt_sha256"], receipt, error)
        if data["status"] != ("success" if receipt is not None else "error"):
            raise ValueError(_ERROR)
        if type(data["input_eof"]) is not bool or data["input_eof"] != (receipt is not None):
            raise ValueError(_ERROR)
        if result.attempt_sha256 != expected_attempt_sha256 or (receipt is not None and receipt != expected_input):
            raise ValueError(_ERROR)
        return result
    except Exception:
        raise ValueError(_ERROR) from None
